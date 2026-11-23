//! Filtered per-work MCP sessions. Codex sees a loopback server with only the
//! admitted tools. Upstream credentials and processes belong to Argentic.
use super::{approvals::CommandApprovals030, AgentWork030};
use crate::{
    agsdl_host_call_executor030::{agent_mcp_grant, mcp_server, remote_mcp, NoHuman},
    agsdl_host_calls::{raw_mcp, CallIdentity, Control, PhysicalEvidence},
    agsdl_host_flow030::FlowWorkAttempt030,
    host::HostContext,
    permissions::{evaluate_effective, may_equip_mcp, ActionRequest, Effect, ExecutionPurpose},
    persistence::{failure, Result},
};
use axum::{
    body::to_bytes,
    extract::{Path, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub(crate) const TOKEN_ENV: &str = "ARGENTIC_MCP_BRIDGE_TOKEN";

#[derive(Clone)]
pub(crate) struct SessionConfig {
    pub(crate) servers: Value,
    token: String,
}

impl SessionConfig {
    pub(crate) fn token(&self) -> &str {
        &self.token
    }
}

pub(super) struct Equipment {
    state: Arc<Bridge>,
    server: Option<JoinHandle<std::io::Result<()>>>,
    config: Option<SessionConfig>,
}

struct Bridge {
    stop: CancellationToken,
    auth: String,
    work: FlowWorkAttempt030,
    approvals: Option<Arc<dyn CommandApprovals030>>,
    admission: crate::agsdl_host_flow030::operator030::AgentOperator030,
    upstreams: BTreeMap<String, Upstream>,
}

struct Upstream {
    session: Mutex<Option<raw_mcp::Session>>,
    tools: BTreeMap<String, (Effect, Value)>,
    identity: CallIdentity,
}

impl Equipment {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn start(
        context: &HostContext,
        work: &AgentWork030,
        document: &Value,
        agent: &Value,
        scope: &Value,
        read_only: bool,
        approvals: Option<Arc<dyn CommandApprovals030>>,
        cancel: &CancellationToken,
    ) -> Result<Self> {
        let token = uuid::Uuid::new_v4().to_string();
        let mut state = Bridge::new(work, approvals, cancel, format!("Bearer {token}"));
        let result = async {
            for name in agent["mcpServers"].as_array().into_iter().flatten() {
                let name = name.as_str().ok_or_else(|| {
                    mcp_error("MCP_CORRELATION", "Invalid captured MCP server name")
                })?;
                if !may_equip_mcp(&work.policy, name)
                    .map_err(|_| mcp_error("MCP_POLICY", "Invalid MCP permission ceiling"))?
                {
                    continue;
                }
                let server = &document["mcpServers"][name];
                let control = state.control();
                drop(state.admit(&control).await?);
                let session = if let Some(url) = server["url"].as_str() {
                    remote_mcp(context, &work.capture, name, url)
                        .map_err(transport_error)?
                        .start(&control)
                        .await
                        .map_err(transport_error)?
                } else {
                    if !cfg!(all(target_os = "macos", target_arch = "aarch64")) {
                        return Err(mcp_error(
                            "MCP_STDIO_UNAVAILABLE",
                            "Confined local MCP requires macOS ARM64",
                        ));
                    }
                    let grant = agent_mcp_grant(context, &work.capture, scope, name, read_only)?;
                    let server =
                        mcp_server(context, grant, &work.capture, name).map_err(transport_error)?;
                    raw_mcp::start(server).map_err(transport_error)?
                };
                let upstream = Upstream::new(session, &work.attempt);
                let upstream = state.upstreams.entry(name.to_owned()).or_insert(upstream);
                let mut session = upstream.session.lock().await;
                let session = session.as_mut().expect("owned discovery session");
                session
                    .initialize(Arc::new(NoHuman), &upstream.identity, &control)
                    .await
                    .map_err(transport_error)?;
                let listed = session
                    .list_tools(&NoHuman, &upstream.identity, &control)
                    .await
                    .map_err(transport_error)?;
                let mut seen = std::collections::BTreeSet::new();
                for tool in listed {
                    let invalid =
                        || mcp_error("MCP_TOOL_LIST", "MCP tool name or schema is invalid");
                    let tool_name = tool["name"].as_str().ok_or_else(invalid)?;
                    if !seen.insert(tool_name.to_owned()) || !tool["inputSchema"].is_object() {
                        return Err(invalid());
                    }
                    let request = ActionRequest::Mcp {
                        server: name.to_owned(),
                        tool: Some(tool_name.to_owned()),
                    };
                    let decision =
                        evaluate_effective(&work.policy, &request, ExecutionPurpose::Agent)
                            .map_err(|_| invalid())?;
                    if decision.effect == Effect::Ask && state.approvals.is_none() {
                        return Err(mcp_error(
                            "MCP_APPROVAL_UNAVAILABLE",
                            "Host MCP action approvals are unavailable",
                        ));
                    }
                    if decision.effect != Effect::Deny {
                        upstream
                            .tools
                            .insert(tool_name.to_owned(), (decision.effect, tool));
                    }
                }
            }
            if state
                .upstreams
                .values()
                .all(|upstream| upstream.tools.is_empty())
            {
                return Ok(None);
            }
            let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .map_err(|_| {
                    mcp_error("MCP_BRIDGE_UNAVAILABLE", "Cannot bind private MCP listener")
                })?;
            let addr = listener.local_addr().map_err(|_| {
                mcp_error(
                    "MCP_BRIDGE_UNAVAILABLE",
                    "Cannot read private MCP listener address",
                )
            })?;
            let servers: BTreeMap<_, _> = state
                .upstreams
                .iter()
                .filter(|(_, upstream)| !upstream.tools.is_empty())
                .map(|(name, upstream)| {
                    (
                        name,
                        json!({
                            "url": format!("http://{addr}/{name}"),
                            "bearer_token_env_var": TOKEN_ENV,
                            "enabled_tools": upstream.tools.keys().collect::<Vec<_>>(),
                            "default_tools_approval_mode": "approve",
                            "required": true,
                            "startup_timeout_sec": 10.0,
                            // Codex requires a finite duration. The engine's durable
                            // clock and human waits govern this work.
                            "tool_timeout_sec": f64::from(u32::MAX),
                        }),
                    )
                })
                .collect();
            Ok(Some((
                listener,
                SessionConfig {
                    servers: json!(servers),
                    token,
                },
            )))
        }
        .await;
        let mut equipment = Self {
            state: Arc::new(state),
            server: None,
            config: None,
        };
        match result {
            Ok(Some((listener, config))) => {
                let stop = equipment.state.stop.clone();
                let router = Router::new()
                    .route("/{server}", post(handle))
                    .with_state(equipment.state.clone());
                equipment.server = Some(tokio::spawn(async move {
                    axum::serve(listener, router)
                        .with_graceful_shutdown(stop.cancelled_owned())
                        .await
                }));
                equipment.config = Some(config);
                Ok(equipment)
            }
            Ok(None) => Ok(equipment),
            Err(error) => {
                equipment.close().await?;
                Err(error)
            }
        }
    }

    pub(super) fn config(&self) -> Option<&SessionConfig> {
        self.config.as_ref()
    }

    pub(super) async fn close(mut self) -> Result<()> {
        self.state.stop.cancel();
        let mut uncertain = false;
        for upstream in self.state.upstreams.values() {
            let Some(session) = upstream.session.lock().await.take() else {
                continue;
            };
            uncertain |= session.is_remote() && session.tool_in_flight();
            let (evidence, error) = session.shutdown(false).await;
            uncertain |= error.is_some()
                || matches!(
                    evidence,
                    PhysicalEvidence::Local {
                        leader_reaped: false,
                        ..
                    } | PhysicalEvidence::Local {
                        tree_termination_succeeded: false,
                        ..
                    } | PhysicalEvidence::Local {
                        streams_joined: false,
                        ..
                    }
                );
        }
        if let Some(mut server) = self.server.take() {
            if tokio::time::timeout(Duration::from_secs(5), &mut server)
                .await
                .is_err()
            {
                server.abort();
                let _ = server.await;
            }
        }
        if uncertain {
            return Err(mcp_error(
                "MCP_TERMINATION_UNCERTAIN",
                "Agent MCP tool termination could not be confirmed",
            ));
        }
        Ok(())
    }
}

impl Drop for Equipment {
    fn drop(&mut self) {
        self.state.stop.cancel();
        if let Some(server) = self.server.as_ref() {
            server.abort();
        }
    }
}

impl Upstream {
    fn new(session: raw_mcp::Session, attempt: &FlowWorkAttempt030) -> Self {
        let identity = CallIdentity {
            system_id: attempt.identity().system_start_id.clone(),
            run_id: attempt.identity().flow_invocation_id.clone(),
            group_id: String::new(),
            work_id: attempt.work().activation_id.clone(),
            epoch: attempt.owner().owner_epoch.clone(),
            effect_id: attempt.start_effect_id().to_owned(),
            provider_attempt_id: None,
            native_call_id: uuid::Uuid::new_v4().to_string(),
        };
        Self {
            session: Mutex::new(Some(session)),
            tools: BTreeMap::new(),
            identity,
        }
    }
}

impl Bridge {
    fn new(
        work: &AgentWork030,
        approvals: Option<Arc<dyn CommandApprovals030>>,
        cancel: &CancellationToken,
        auth: String,
    ) -> Self {
        Self {
            stop: cancel.child_token(),
            auth,
            work: work.attempt.clone(),
            approvals,
            admission: work.operator.clone(),
            upstreams: BTreeMap::new(),
        }
    }

    fn control(&self) -> Control {
        Control {
            stop: self.stop.clone(),
            machine_timeout: CancellationToken::new(),
        }
    }

    async fn admit(&self, control: &Control) -> Result<tokio::sync::OwnedMutexGuard<()>> {
        let admission =
            self.admission.admit_turn().await?.ok_or_else(|| {
                mcp_error("STOP_PENDING", "Agent work cannot admit an MCP request")
            })?;
        control.check().map_err(transport_error)?;
        Ok(admission)
    }

    async fn dispatch(
        &self,
        session: &mut raw_mcp::Session,
        tool: &str,
        args: &str,
        control: &Control,
    ) -> Result<raw_mcp::PendingToolCall> {
        #[cfg(test)]
        self.admission.test_yield("mcp.dispatch.before").await;
        let pending = session
            .dispatch_tool(tool, args, control)
            .await
            .map_err(|error| mcp_error(&error.code, "Cannot deliver the admitted MCP tool call"))?;
        #[cfg(test)]
        self.admission.test_yield("mcp.dispatch.handed_off").await;
        Ok(pending)
    }

    async fn call(&self, upstream: &Upstream, server: &str, params: &Value) -> Option<Value> {
        let control = self.control();
        control.check().ok()?;
        let tool = params["name"].as_str()?;
        let (effect, _) = upstream.tools.get(tool)?;
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        if !arguments.is_object() {
            return None;
        }
        let arguments_json = arguments.to_string();
        let mut session = tokio::select! {
            biased;
            _ = self.stop.cancelled() => return None,
            lock = upstream.session.lock() => lock,
        };
        let session = session.as_mut()?;
        if session.tool_in_flight() || session.guard_secrets(&arguments_json).is_err() {
            return None;
        }
        let pending = if *effect == Effect::Ask {
            let approvals = self.approvals.as_ref()?;
            let mut pending = None;
            let dispatched = &mut pending;
            let wire = &mut *session;
            let args = arguments_json.as_str();
            let control = &control;
            let payload = json!({"nativeRequest":{
                "id": uuid::Uuid::new_v4().to_string(),
                "method":"mcp/tool/requestApproval",
                "params":{"reason":format!("Allow MCP tool {server}/{tool}?"),"server":server,"tool":tool,"arguments":arguments}
            }});
            approvals
                .answer(
                    &self.work,
                    payload,
                    &self.stop,
                    Box::new(move |yes| {
                        Box::pin(async move {
                            // The broker already holds the policy lease and
                            // Stop's admission gate while delivering this answer.
                            if yes {
                                *dispatched = Some(self.dispatch(wire, tool, args, control).await?);
                            }
                            Ok(())
                        })
                    }),
                )
                .await
                .ok()?;
            pending?
        } else {
            let _admission = self.admit(&control).await.ok()?;
            self.dispatch(session, tool, &arguments_json, &control)
                .await
                .ok()?
        };
        #[cfg(test)]
        self.admission.test_yield("mcp.dispatch.wait").await;
        let receipt = session
            .finish_tool_call(pending, &NoHuman, &upstream.identity, &control)
            .await
            .ok()?;
        serde_json::from_str(&receipt.raw_result_json).ok()
    }
}

async fn handle(
    State(state): State<Arc<Bridge>>,
    Path(server): Path<String>,
    request: Request,
) -> Response {
    if request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some(&state.auth)
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Some(upstream) = state
        .upstreams
        .get(&server)
        .filter(|upstream| !upstream.tools.is_empty())
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let body = tokio::select! {
        biased;
        _ = state.stop.cancelled() => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
        body = to_bytes(request.into_body(), raw_mcp::MAX_MCP_MESSAGE_BYTES) => body,
    };
    let Some(message) = body
        .as_deref()
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(bytes).ok())
    else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if message["jsonrpc"] != "2.0" {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let method = message["method"].as_str().unwrap_or("");
    let Some(id) = message
        .get("id")
        .filter(|id| id.is_number() || id.is_string())
    else {
        return StatusCode::ACCEPTED.into_response();
    };
    let result = match method {
        "initialize" => {
            let requested = message["params"]["protocolVersion"].as_str().unwrap_or("");
            let version = match requested {
                "2025-03-26" | "2025-06-18" | "2025-11-25" => requested,
                _ => "2025-11-25",
            };
            Some(json!({"protocolVersion":version,"capabilities":{"tools":{}},"serverInfo":{"name":"Argentic","version":"1"}}))
        }
        "ping" => Some(json!({})),
        "tools/list" => Some(json!({"tools":upstream.tools.values().map(|(_,tool)| tool).collect::<Vec<_>>()})),
        "resources/list" => Some(json!({"resources":[]})),
        "resources/templates/list" => Some(json!({"resourceTemplates":[]})),
        "prompts/list" => Some(json!({"prompts":[]})),
        "tools/call" => Some(state.call(upstream, &server, &message["params"]).await
            .unwrap_or_else(|| json!({"isError":true,"content":[{"type":"text","text":"MCP call refused or unavailable"}]}))),
        _ => None,
    };
    let reply = match result {
        Some(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
        None => {
            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"Method unavailable"}})
        }
    };
    Json(reply).into_response()
}

fn transport_error(
    error: crate::agsdl_host_calls::CallError,
) -> crate::persistence::dto::EngineFailure {
    mcp_error(
        &error.code,
        "Cannot connect or discover tools for the admitted MCP server",
    )
}

fn mcp_error(code: &str, message: &str) -> crate::persistence::dto::EngineFailure {
    failure(code, "/mcpServers", message)
}

#[cfg(all(test, unix))]
pub(crate) async fn call_test_session(
    work: &AgentWork030,
    approvals: Arc<dyn CommandApprovals030>,
    cancel: &CancellationToken,
    session: raw_mcp::Session,
) -> Result<Option<Value>> {
    let mut state = Bridge::new(work, Some(approvals), cancel, String::new());
    let mut upstream = Upstream::new(session, &work.attempt);
    upstream
        .tools
        .insert("check".into(), (Effect::Ask, Value::Null));
    state.upstreams.insert("issues".into(), upstream);
    let equipment = Equipment {
        state: Arc::new(state),
        server: None,
        config: None,
    };
    let upstream = &equipment.state.upstreams["issues"];
    upstream
        .session
        .lock()
        .await
        .as_mut()
        .unwrap()
        .initialize(
            Arc::new(NoHuman),
            &upstream.identity,
            &equipment.state.control(),
        )
        .await
        .map_err(transport_error)?;
    let result = equipment
        .state
        .call(upstream, "issues", &json!({"name":"check"}))
        .await;
    equipment.close().await?;
    Ok(result)
}
