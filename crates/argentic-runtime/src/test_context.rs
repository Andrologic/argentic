//! Synthetic host policies for native fixtures. No application configuration store.
use crate::{host::*, persistence::Result};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};
#[derive(Default)]
pub(crate) struct Policy {
    ceiling: Arc<tokio::sync::RwLock<bool>>,
}
impl Policy {
    pub(crate) async fn workspace(&self) {
        *self.ceiling.write().await = true;
    }
    fn permissions(allowed: bool) -> crate::permissions::RequestedPermissions {
        serde_json::from_value(json!(if allowed { "workspace" } else { "read-only" })).unwrap()
    }
}
impl PolicySource for Policy {
    fn lock_project<'a>(&'a self, _: &'a str) -> PolicyFuture<'a, ProjectPolicyLease> {
        Box::pin(async move {
            let guard = self.ceiling.clone().read_owned().await;
            Ok(ProjectPolicyLease::new(
                json!({"riskLevel":"balanced"}),
                Self::permissions(*guard),
                guard,
            ))
        })
    }
    fn lock_ceiling(&self) -> PolicyFuture<'_, HostPolicyLease> {
        Box::pin(async move {
            let guard = self.ceiling.clone().read_owned().await;
            Ok(HostPolicyLease::new(
                Self::permissions(*guard),
                format!("synthetic-ceiling:{}", *guard),
                guard,
            ))
        })
    }
}
pub(crate) fn context(root: PathBuf) -> HostContext {
    context_with_policy(root).0
}
pub(crate) fn context_with_policy(root: PathBuf) -> (HostContext, Arc<Policy>) {
    std::fs::create_dir_all(&root).unwrap();
    let policy = Arc::new(Policy::default());
    (HostContext::new(root, policy.clone()), policy)
}
/// Explicit captured tool projection used by the old native fixtures. Macro's
/// projection/configuration behavior remains tested in Macro.
pub(crate) fn captured_permissions() -> Value {
    json!({"mode":"Implement","readOnly":false,"riskLevel":"balanced","allowedToolIds":["list","read","glob","grep","ast_grep","write","edit","delete","apply_patch","git_status","git_log","git_branch_list","git_diff","git_get_tree"]})
}

use crate::persistence::failure;
use std::path::Path;
pub(crate) fn codex_registry030(
    document: &Value,
    source_sha256: &str,
    scope: &Value,
    observed: &crate::agsdl_host_codex::CodexObservation030,
) -> Result<(Value, Vec<Value>)> {
    crate::agsdl_host_codex030::admission::validate_codex_document030(document)?;
    if document["agents"].as_object().is_some_and(|agents| {
        agents.values().any(|agent| {
            agent["skills"]
                .as_array()
                .is_some_and(|skills| !skills.is_empty())
        })
    }) {
        crate::api::qualify_skill_folders(
            document,
            Path::new(scope["root"].as_str().ok_or_else(|| {
                failure("CORRELATION", "skills", "Native project scope is absent")
            })?),
        )?;
    }
    if !crate::agsdl_host_codex::qualified_version030(&observed.version)
        || observed.models.is_empty()
    {
        return Err(failure(
            "CODEX_VERSION_UNQUALIFIED",
            "/agents",
            "Codex observation is outside the qualified version or catalogue",
        ));
    }
    let adapter = crate::agsdl_host_codex::ADAPTER;
    let evidence = json!({"adapterId":adapter,"version":observed.version,"profileDigest":observed.profile.digest,"accountDigest":observed.account_digest,"kind":"native-codex"}).to_string();
    let capabilities = ["result-submission", "continuity", "permission-enforcement", "contained-stop", "response-attribution", "instructions", "resources", "model.name", "model.effort", "instance.new", "delivery.queue", "delivery.steer", "answer-timeout", "reviewer.person", "planFirst.auto", "completion-check", "skills", "mcpServers", "harness-settings"]
        .into_iter().filter(|name| (!matches!(*name,"completion-check"|"skills") || cfg!(all(target_os="macos",target_arch="aarch64"))) && (*name!="skills" || observed.version=="0.167.0" && crate::content_io::file_helper::qualified_platform())).map(|name| json!({"name":name,"support":if matches!(name, "resources" | "planFirst.auto") {"approximate"} else {"native"},"evidenceJson":evidence})).collect::<Vec<_>>();
    let default = observed
        .models
        .iter()
        .find(|model| model["isDefault"] == true)
        .and_then(|model| model["name"].as_str());
    let models = observed.models.iter().map(|model| json!({"name":model["name"],"efforts":model["efforts"],"defaultEffort":model["defaultEffort"]})).collect::<Vec<_>>();
    let agents = document["agents"].as_object().into_iter().flatten().filter(|(_, agent)| agent["harness"] == "codex").map(|(name, agent)| {
        json!({"agentName":name,"agentDeclarationJson":agent.to_string(),"settingsJson":agent.get("harnessSettings").cloned().unwrap_or_else(|| json!({})).to_string(),
            "permissionsJson":json!({"requested":agent["permissions"],"authority":"native-03-at-handoff"}).to_string(),
            "scopeJson":scope.to_string(),"evidenceJson":evidence,"provenanceJson":json!({"sourceSha256":source_sha256,"agent":name}).to_string()})
    }).collect();
    Ok((
        json!({"harness":"codex","version":observed.version,"installation":"available","adapterId":adapter,"qualified":true,"capabilities":capabilities,"models":models,"defaultModel":default}),
        agents,
    ))
}

#[cfg(unix)]
pub(crate) mod mcp {
    use super::*;
    use axum::{
        extract::{Path, State},
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
        Json, Router,
    };
    use std::{collections::BTreeMap, sync::Mutex};

    pub const FIRST: &str = "argentic-fixture-mcp-first-secret";
    pub const SECOND: &str = "argentic-fixture-mcp-second-secret";
    pub const STDIO_TOOL_SCRIPT: &str = r#"
IFS= read -r frame || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":"1","result":{"protocolVersion":"2025-06-18"}}'
IFS= read -r frame || exit 1
IFS= read -r frame || exit 0
id=2
case "$frame" in
    *'"method":"tools/list"'*)
        printf '%s\n' '{"jsonrpc":"2.0","id":"2","result":{"tools":[{"name":"check","inputSchema":{"type":"object"}}]}}'
        IFS= read -r frame || exit 0
        id=3
        ;;
esac
case "$frame" in
    *'"method":"tools/call"'*) printf sent > "$1" ;;
    *) exit 1 ;;
esac
while [ ! -f "$2" ]; do /bin/sleep .01; done
printf '{"jsonrpc":"2.0","id":"%s","result":{"content":[]}}\n' "$id"
"#;
    pub const REQUIREMENTS: &str = r#"{"first":["FIRST_TOKEN"],"second":["SECOND_TOKEN"]}"#;
    type Calls = Arc<Mutex<Vec<(String, String)>>>;

    pub struct Peer {
        pub addr: std::net::SocketAddr,
        calls: Calls,
        task: tokio::task::JoinHandle<std::io::Result<()>>,
    }

    impl Peer {
        pub async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let calls = Calls::default();
            let app = Router::new()
                .route("/{server}", post(reply))
                .with_state(calls.clone());
            let task = tokio::spawn(async move { axum::serve(listener, app).await });
            Self { addr, calls, task }
        }

        pub fn servers(&self) -> Value {
            json!({"first":{"url":format!("http://{}/first", self.addr)}, "second":{"url":format!("http://{}/second", self.addr)}})
        }

        pub fn authorize(&self) -> Arc<McpHttpAuthorizer> {
            let addr = self.addr;
            Arc::new(move |source, server, url, addresses, names| {
                assert_eq!(source.owner.project_id, "project");
                assert_eq!(addresses, [addr]);
                assert_eq!(url.path(), format!("/{server}"));
                let (name, destination) = match server {
                    "first" => ("FIRST_TOKEN", McpHttpSecret::Bearer),
                    "second" => ("SECOND_TOKEN", McpHttpSecret::Header("X-Mcp-Secret".into())),
                    _ => unreachable!(),
                };
                assert_eq!(names, [name]);
                Some(BTreeMap::from([(name.into(), destination)]))
            })
        }

        pub fn configure(&self, config: HostContext) -> HostContext {
            config
                .with_mcp_http(self.authorize())
                .with_mcp_secrets(Arc::new(|_, server, names| {
                    let (name, value) = match server {
                        "first" => ("FIRST_TOKEN", FIRST),
                        "second" => ("SECOND_TOKEN", SECOND),
                        _ => unreachable!(),
                    };
                    assert_eq!(names, [name]);
                    Some(BTreeMap::from([(name.into(), value.into())]))
                }))
        }

        pub fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl Drop for Peer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn tools() -> Vec<Value> {
        ["check", "approve", "blocked", "echo"]
            .into_iter()
            .map(|name| json!({"name":name,"inputSchema":{"type":"object"}}))
            .collect()
    }

    async fn reply(
        State(calls): State<Calls>,
        Path(server): Path<String>,
        headers: HeaderMap,
        Json(request): Json<Value>,
    ) -> Response {
        let authorized = match server.as_str() {
            "first" => {
                headers
                    .get("authorization")
                    .is_some_and(|h| h == &format!("Bearer {FIRST}"))
                    && !headers.contains_key("x-mcp-secret")
            }
            "second" => {
                headers.get("x-mcp-secret").is_some_and(|h| h == SECOND)
                    && !headers.contains_key("authorization")
            }
            _ => false,
        };
        if !authorized {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if request["method"].is_null() && request["id"] == "synthetic-form" {
            assert_eq!(request["result"]["action"], "accept");
            return StatusCode::ACCEPTED.into_response();
        }
        if request["method"].is_null() && request["id"] == "synthetic-form-lost" {
            // The answer may have reached the server; its response is lost.
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        let result = match request["method"].as_str().unwrap() {
            "initialize" => {
                json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}},"serverInfo":{"name":"fixture","version":"1"}})
            }
            "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
            "tools/list" => json!({"tools":tools()}),
            "tools/call" => {
                let tool = request["params"]["name"].as_str().unwrap();
                calls.lock().unwrap().push((server.clone(), tool.into()));
                if tool == "blocked" {
                    std::future::pending::<()>().await;
                }
                if tool == "form" || tool == "form-lost" {
                    use futures::StreamExt;
                    let id = if tool == "form" {
                        "synthetic-form"
                    } else {
                        "synthetic-form-lost"
                    };
                    let form = json!({"jsonrpc":"2.0","id":id,
                        "method":"elicitation/create",
                        "params":{"message":"Continue?","requestedSchema":{"type":"object","properties":{}}}
                    });
                    let frame = Ok::<_, std::io::Error>(format!("data: {form}\n\n"));
                    let stream = futures::stream::once(async move { frame })
                        .chain(futures::stream::pending());
                    return (
                        [("content-type", "text/event-stream")],
                        axum::body::Body::from_stream(stream),
                    )
                        .into_response();
                }
                if tool == "echo" {
                    json!({"structuredContent":{"secret":FIRST},"content":[]})
                } else {
                    json!({"structuredContent":{"checked":server,"value":request["params"]["arguments"]["value"]},"content":[]})
                }
            }
            _ => return StatusCode::BAD_REQUEST.into_response(),
        };
        Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response()
    }

    pub fn assert_no_secrets(path: &std::path::Path) {
        let bytes = std::fs::read(path).unwrap();
        for secret in [FIRST, SECOND] {
            assert!(!bytes
                .windows(secret.len())
                .any(|window| window == secret.as_bytes()));
        }
    }
}
