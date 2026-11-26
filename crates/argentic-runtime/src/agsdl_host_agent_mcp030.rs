//! Filtered per-work MCP sessions. The Codex adapter also uses a private loopback listener.
use crate::{agsdl_host_action_approval030::CommandApprovals030, harness::AgentWork030};
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
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex},
    task::JoinHandle,
    time::Instant,
};
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

pub(crate) struct Equipment {
    state: Arc<Bridge>,
    server: Option<JoinHandle<std::io::Result<()>>>,
    config: Option<SessionConfig>,
}

struct Bridge {
    stop: CancellationToken,
    termination_uncertain: AtomicBool,
    auth: String,
    work: FlowWorkAttempt030,
    remote_tool_budget: Option<Duration>,
    approvals: Option<Arc<dyn CommandApprovals030>>,
    admission: crate::agsdl_host_flow030::operator030::AgentOperator030,
    upstreams: BTreeMap<String, Upstream>,
}

struct Upstream {
    session: Mutex<Option<raw_mcp::Session>>,
    tools: BTreeMap<String, (Effect, Value)>,
    identity: CallIdentity,
}

enum Dispatch {
    HandedOff(raw_mcp::PendingToolCall, Option<Instant>),
    BudgetExpired(Value),
}

impl Equipment {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn discover(
        context: &HostContext,
        work: &AgentWork030,
        document: &Value,
        agent: &Value,
        scope: &Value,
        read_only: bool,
        authored_budget: bool,
        approvals: Option<Arc<dyn CommandApprovals030>>,
        cancel: &CancellationToken,
    ) -> Result<Self> {
        let mut state = Bridge::new(
            work,
            (!authored_budget)
                .then(|| Duration::from_millis(context.remote_mcp_tool_budget_ms().get())),
            approvals,
            cancel,
            String::new(),
        );
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
                    raw_mcp::require_stdio().map_err(transport_error)?;
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
                    if decision.effect == Effect::Allow
                        || (decision.effect == Effect::Ask && state.approvals.is_some())
                    {
                        upstream
                            .tools
                            .insert(tool_name.to_owned(), (decision.effect, tool));
                    }
                }
            }
            Ok(())
        }
        .await;
        let equipment = Self {
            state: Arc::new(state),
            server: None,
            config: None,
        };
        if let Err(error) = result {
            equipment.close().await?;
            return Err(error);
        }
        Ok(equipment)
    }

    pub(crate) fn tools(&self) -> impl Iterator<Item = (&str, &str, &Value)> {
        self.state.upstreams.iter().flat_map(|(server, upstream)| {
            upstream
                .tools
                .iter()
                .map(move |(name, (_, tool))| (server.as_str(), name.as_str(), tool))
        })
    }

    pub(crate) async fn call(&self, server: &str, tool: &str, arguments: Value) -> Option<Value> {
        let upstream = self.state.upstreams.get(server)?;
        self.state
            .call(
                upstream,
                server,
                &json!({"name":tool,"arguments":arguments}),
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start(
        context: &HostContext,
        work: &AgentWork030,
        document: &Value,
        agent: &Value,
        scope: &Value,
        read_only: bool,
        authored_budget: bool,
        approvals: Option<Arc<dyn CommandApprovals030>>,
        cancel: &CancellationToken,
    ) -> Result<Self> {
        let token = uuid::Uuid::new_v4().to_string();
        let mut equipment = Self::discover(
            context,
            work,
            document,
            agent,
            scope,
            read_only,
            authored_budget,
            approvals,
            cancel,
        )
        .await?;
        let state = Arc::get_mut(&mut equipment.state).expect("new MCP equipment");
        state.auth = format!("Bearer {token}");
        let result = async {
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
                            // Codex requires a finite duration. Argentic owns the
                            // work clock, human waits and remote tool budget.
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

    pub(crate) fn config(&self) -> Option<&SessionConfig> {
        self.config.as_ref()
    }

    pub(crate) fn turn_cancel(&self) -> &CancellationToken {
        &self.state.stop
    }

    pub(crate) async fn close(mut self) -> Result<()> {
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
        if uncertain || self.state.termination_uncertain.load(Ordering::Relaxed) {
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
        remote_tool_budget: Option<Duration>,
        approvals: Option<Arc<dyn CommandApprovals030>>,
        cancel: &CancellationToken,
        auth: String,
    ) -> Self {
        Self {
            stop: cancel.child_token(),
            termination_uncertain: AtomicBool::new(false),
            auth,
            work: work.attempt.clone(),
            remote_tool_budget,
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
    ) -> Result<Dispatch> {
        let deadline = self
            .remote_tool_budget
            .filter(|_| session.is_remote())
            .and_then(|budget| Instant::now().checked_add(budget));
        #[cfg(test)]
        self.admission.test_yield("mcp.dispatch.before").await;
        let pending = match session.dispatch_tool(tool, args, control, deadline).await {
            Ok(pending) => pending,
            Err(error) if error.code == raw_mcp::TOOL_BUDGET_EXPIRED => {
                return Ok(Dispatch::BudgetExpired(self.budget_expired(session)));
            }
            Err(error) => {
                self.stop_if_remote_uncertain(session);
                return Err(mcp_error(
                    &error.code,
                    "Cannot deliver the admitted MCP tool call",
                ));
            }
        };
        #[cfg(test)]
        self.admission.test_yield("mcp.dispatch.handed_off").await;
        Ok(Dispatch::HandedOff(pending, deadline))
    }

    fn stop_if_remote_uncertain(&self, session: &raw_mcp::Session) -> bool {
        let uncertain = session.is_remote() && session.tool_in_flight();
        if uncertain {
            self.termination_uncertain.store(true, Ordering::Relaxed);
            // Close bridge admission and wake the Codex driver's interrupt path.
            self.stop.cancel();
        }
        uncertain
    }

    fn budget_expired(&self, session: &raw_mcp::Session) -> Value {
        let message = if self.stop_if_remote_uncertain(session) {
            "MCP_TERMINATION_UNCERTAIN: remote MCP tool budget expired before termination could be confirmed"
        } else {
            "Remote MCP tool budget expired"
        };
        json!({"isError":true,"content":[{"type":"text","text":message}]})
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
        let wire = session.as_mut()?;
        if wire.tool_in_flight() || wire.guard_secrets(&arguments_json).is_err() {
            return None;
        }
        if *effect == Effect::Ask {
            let approvals = self.approvals.as_ref()?;
            let (handed_off, dispatched) = oneshot::channel();
            let args = arguments_json.as_str();
            let control = &control;
            let payload = json!({"nativeRequest":{
                "id": uuid::Uuid::new_v4().to_string(),
                "method":"mcp/tool/requestApproval",
                "params":{"reason":format!("Allow MCP tool {server}/{tool}?"),"server":server,"tool":tool,"arguments":arguments}
            }});
            let answer = approvals.answer(
                &self.work,
                payload,
                &self.stop,
                Box::new(move |yes| {
                    Box::pin(async move {
                        // The broker holds the policy lease and Stop's gate
                        // while handing off the approved request.
                        if yes {
                            let wire = session.as_mut().expect("locked MCP session");
                            let dispatch = self.dispatch(wire, tool, args, control).await?;
                            let _ = handed_off.send((session, dispatch));
                        }
                        Ok(())
                    })
                }),
            );
            #[cfg(test)]
            let answer = async {
                let result = answer.await;
                self.admission.test_yield("mcp.approval.acknowledged").await;
                result
            };
            let reply = async {
                let (mut session, dispatch) = dispatched.await.ok()?;
                let reply = self
                    .finish(session.as_mut()?, upstream, dispatch, control)
                    .await;
                Some((session, reply))
            };
            // Enforce the tool deadline even while the broker records its
            // acknowledgement. Hold the session lock until both futures settle.
            let (answer, received) = tokio::join!(answer, reply);
            answer.ok()?;
            let (_session, reply) = received?;
            return reply;
        }

        let admission = self.admit(&control).await.ok()?;
        #[cfg(test)]
        let admission = self.admission.track_mcp_gate(admission);
        let sent = self.dispatch(wire, tool, &arguments_json, &control).await;
        drop(admission);
        self.finish(wire, upstream, sent.ok()?, &control).await
    }

    async fn finish(
        &self,
        session: &mut raw_mcp::Session,
        upstream: &Upstream,
        dispatch: Dispatch,
        control: &Control,
    ) -> Option<Value> {
        let (pending, deadline) = match dispatch {
            Dispatch::HandedOff(pending, deadline) => (pending, deadline),
            Dispatch::BudgetExpired(error) => return Some(error),
        };
        #[cfg(test)]
        self.admission.test_yield("mcp.dispatch.wait").await;
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            return Some(self.budget_expired(session));
        }
        let result = session.finish_tool_call(pending, &NoHuman, &upstream.identity, control);
        let receipt = if let Some(deadline) = deadline {
            match tokio::time::timeout_at(deadline, result).await {
                Ok(result) if Instant::now() < deadline => result,
                _ => return Some(self.budget_expired(session)),
            }
        } else {
            result.await
        };
        if receipt.is_err() {
            self.stop_if_remote_uncertain(session);
        }
        serde_json::from_str(&receipt.ok()?.raw_result_json).ok()
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
    approvals: Option<Arc<dyn CommandApprovals030>>,
    cancel: &CancellationToken,
    session: raw_mcp::Session,
    budget: Option<Duration>,
) -> Result<(Option<Value>, Result<()>)> {
    let effect = if approvals.is_some() {
        Effect::Ask
    } else {
        Effect::Allow
    };
    let mut state = Bridge::new(work, budget, approvals, cancel, String::new());
    let mut upstream = Upstream::new(session, &work.attempt);
    upstream.tools.insert("check".into(), (effect, Value::Null));
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
    Ok((result, equipment.close().await))
}
