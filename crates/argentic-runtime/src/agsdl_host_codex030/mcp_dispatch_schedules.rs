use super::*;
use crate::{
    agsdl_host_action_approval030::AgentActionHumans030,
    agsdl_host_calls::raw_mcp,
    host::{ActionPolicyLease, ActionPolicySource, PolicyFuture},
};
use std::{path::PathBuf, sync::Mutex as StdMutex};
use tokio::sync::RwLock;

struct CurrentPolicy(Arc<RwLock<bool>>);

impl ActionPolicySource for CurrentPolicy {
    fn lock_current<'a>(
        &'a self,
        _: &'a StartCapture030,
        payload: &'a Value,
    ) -> PolicyFuture<'a, ActionPolicyLease> {
        Box::pin(async move {
            assert_eq!(payload["nativeRequest"]["params"]["server"], "issues");
            assert_eq!(payload["nativeRequest"]["params"]["tool"], "check");
            let lease = self.0.clone().read_owned().await;
            Ok(ActionPolicyLease::new(*lease, lease))
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum Transport {
    Http,
    Stdio,
}

#[derive(Clone, Copy, Debug)]
enum Contender {
    Revocation,
    Stop(StopPath),
}

#[derive(Clone)]
struct Peer {
    sent: PathBuf,
    reply: PathBuf,
}

impl Peer {
    async fn http(self) -> (raw_mcp::ServerTransport, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = axum::Router::new()
            .route("/", axum::routing::post(http_reply))
            .with_state(self);
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let http = raw_mcp::HttpServer {
            url: format!("http://{addr}/").parse().unwrap(),
            addresses: vec![addr],
            headers: Default::default(),
            secret_values: vec![],
            tool_schema: None,
        };
        (raw_mcp::ServerTransport::Http(http), task)
    }

    fn stdio(&self) -> raw_mcp::ServerTransport {
        let workspace = self.sent.parent().unwrap();
        let script = r#"
IFS= read -r frame || exit 1
printf '%s\n' '{"jsonrpc":"2.0","id":"1","result":{"protocolVersion":"2025-06-18"}}'
IFS= read -r frame || exit 1
IFS= read -r frame || exit 0
case "$frame" in
    *'"method":"tools/call"'*) printf sent > "$1" ;;
    *) exit 1 ;;
esac
while [ ! -f "$2" ]; do /bin/sleep .01; done
printf '%s\n' '{"jsonrpc":"2.0","id":"2","result":{"content":[]}}'
"#;
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        let grant = crate::agsdl_host_calls::tests::grant(workspace);
        #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
        let grant = crate::agsdl_host_calls::CommandGrant {
            workspace: workspace.to_owned(),
            environment: Default::default(),
            sandbox: crate::agsdl_host_confinement::SandboxPolicy::from_authorized_ceilings(
                "mcp-stdio-fixture".into(),
                &[Default::default()],
            )
            .unwrap(),
        };
        raw_mcp::ServerTransport::Stdio(raw_mcp::StdioServer {
            grant,
            program: "/bin/sh".into(),
            arguments: vec![
                "-c".into(),
                script.into(),
                "mcp-fixture".into(),
                self.sent.to_str().unwrap().into(),
                self.reply.to_str().unwrap().into(),
            ],
            secret_values: vec![],
        })
    }

    async fn start(
        &self,
        transport: Transport,
    ) -> (raw_mcp::Session, Option<tokio::task::JoinHandle<()>>) {
        let (wire, http) = match transport {
            Transport::Http => {
                let (wire, task) = self.clone().http().await;
                (wire, Some(task))
            }
            Transport::Stdio => (self.stdio(), None),
        };
        let session = raw_mcp::start_test_stdio(raw_mcp::CapturedServer {
            document_name: "issues".into(),
            capture_digest: "test".into(),
            protocol: raw_mcp::Protocol::Legacy,
            transport: wire,
        })
        .unwrap();
        (session, http)
    }

    async fn sent(&self) {
        operator_wait(async || self.sent.exists().then_some(())).await;
    }
}

async fn http_reply(
    axum::extract::State(peer): axum::extract::State<Peer>,
    axum::Json(message): axum::Json<Value>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let result = match message["method"].as_str().unwrap() {
        "initialize" => json!({"protocolVersion":"2025-06-18"}),
        "notifications/initialized" => return axum::http::StatusCode::ACCEPTED.into_response(),
        "tools/call" => {
            assert_eq!(message["params"]["name"], "check");
            std::fs::write(&peer.sent, b"sent").unwrap();
            tokio::time::timeout(WAIT, async move {
                while !peer.reply.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            json!({"content":[]})
        }
        method => panic!("Unexpected request {method}"),
    };
    axum::Json(json!({"jsonrpc":"2.0","id":message["id"],"result":result})).into_response()
}

struct Agent {
    approvals: Option<Arc<AgentActionHumans030>>,
    budget: Option<std::time::Duration>,
    expect_budget_expiration: bool,
    session: StdMutex<Option<raw_mcp::Session>>,
    expected_reply: Option<bool>,
}

impl AgentExecutor030 for Agent {
    fn prepare(
        &self,
        work: crate::agsdl_host_work_admission030::PreparedEffectiveAgentWork030,
    ) -> HostResult<crate::agsdl_host_work_admission030::AttestedWorkPolicy030> {
        AttestedWorkPolicy030::from_codex(work)
    }

    fn run(
        &self,
        work: AgentWork030,
        events: WorkEventSink030,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, HostResult<()>> {
        let approvals = self.approvals.clone();
        let session = self.session.lock().unwrap().take().unwrap();
        let expected_reply = self.expected_reply;
        let budget = self.budget;
        let expect_budget_expiration = self.expect_budget_expiration;
        Box::pin(async move {
            let (reply, closed) = crate::agsdl_host_codex030::mcp::call_test_session(
                &work,
                approvals.map(|port| port as _),
                &cancel,
                session,
                budget,
            )
            .await?;
            if expect_budget_expiration {
                let reply = reply.as_ref().expect("Codex receives an MCP tool error");
                assert_eq!(reply["isError"], true);
                assert!(reply["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("MCP_TERMINATION_UNCERTAIN"));
            }
            closed?;
            if let Some(expected_reply) = expected_reply {
                assert_eq!(reply, expected_reply.then(|| json!({"content":[]})));
                events
                    .emit(
                        json!({"kind":"submit","callId":"mcp","argumentsJson":"{\"results\":{}}"}),
                        &cancel,
                    )
                    .await?;
            } else {
                cancel.cancelled().await;
            }
            Ok(())
        })
    }
}

async fn submit_yes(f: &Fixture) {
    let request = operator_wait(async || {
        sqlx::query_scalar::<_, String>(
            "SELECT request_id FROM engine_flow030_human_requests WHERE kind='ask'",
        )
        .fetch_optional(&f.pool)
        .await
        .unwrap()
    })
    .await;
    let source = operator_scope(f).await.source;
    f.host
        .submit_person(
            &f.pool,
            crate::api::HumanSubmission {
                scope: crate::persistence::public030::HumanScope030 {
                    system_start_id: f.identity.system_start_id.clone(),
                    source,
                },
                request_id: request,
                expected_revision: "0".into(),
                answer: crate::api::HumanAnswer030::Decision {
                    decision: "yes".into(),
                    comment: String::new(),
                },
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn operator_schedule_mcp_http_ask_handoff() {
    handoff_schedules(Transport::Http, true).await;
}

#[tokio::test]
async fn operator_schedule_mcp_stdio_ask_handoff() {
    handoff_schedules(Transport::Stdio, true).await;
}

#[tokio::test]
async fn operator_schedule_mcp_http_allow_handoff() {
    handoff_schedules(Transport::Http, false).await;
}

#[tokio::test]
async fn operator_schedule_mcp_stdio_allow_handoff() {
    handoff_schedules(Transport::Stdio, false).await;
}

async fn handoff_schedules(transport: Transport, ask: bool) {
    let contenders: &[_] = if exhaustive() {
        &[
            Contender::Stop(StopPath::Public),
            Contender::Stop(StopPath::Internal),
            Contender::Revocation,
        ]
    } else {
        &[Contender::Stop(StopPath::Public), Contender::Revocation]
    };
    for &contender in contenders {
        if ask {
            for point in ["mcp.approval.resolved", "mcp.approval.policy"] {
                schedule(transport, contender, point, ask).await;
            }
        } else if matches!(contender, Contender::Revocation) {
            continue;
        }
        for point in [
            "mcp.dispatch.before",
            "mcp.dispatch.handed_off",
            "mcp.dispatch.wait",
        ] {
            schedule(transport, contender, point, ask).await;
        }
    }
}

#[tokio::test]
async fn remote_mcp_bridge_reports_budget_expiration_and_uncertain_termination() {
    let f = Arc::new(
        Fixture::with_document(document(
            json!({"work":{"type":"agent","agent":"worker"}}),
            "work",
        ))
        .await,
    );
    let workspace = f._temp.path().join("workspace").canonicalize().unwrap();
    let peer = Peer {
        sent: workspace.join("mcp-sent"),
        reply: workspace.join("mcp-reply"),
    };
    let (session, http) = peer.start(Transport::Http).await;
    let tasks = StreamTasks::default();
    let gap = arm(&f, "mcp.dispatch.wait");
    start(
        &f,
        &tasks,
        Arc::new(Agent {
            approvals: None,
            budget: Some(std::time::Duration::ZERO),
            expect_budget_expiration: true,
            session: StdMutex::new(Some(session)),
            expected_reply: None,
        }),
    )
    .await;
    reached(&gap, "mcp.dispatch.wait").await;
    peer.sent().await;
    gap.release();
    let error = tokio::time::timeout(
        WAIT,
        f.host
            .flow030()
            .supervisors
            .wait(&f.identity.system_start_id),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code, "MCP_TERMINATION_UNCERTAIN");
    http.unwrap().abort();
}

async fn schedule(transport: Transport, contender: Contender, point: &'static str, ask: bool) {
    let f = Arc::new(
        Fixture::with_document(document(
            json!({"work":{"type":"agent","agent":"worker"}}),
            "work",
        ))
        .await,
    );
    let workspace = f._temp.path().join("workspace").canonicalize().unwrap();
    let peer = Peer {
        sent: workspace.join("mcp-sent"),
        reply: workspace.join("mcp-reply"),
    };
    let (session, http) = peer.start(transport).await;
    let policy = Arc::new(RwLock::new(true));
    let approval = Arc::new(AgentActionHumans030 {
        host: f.host.as_ref().clone(),
        pool: f.pool.clone(),
        policy: Arc::new(CurrentPolicy(policy.clone())),
    });
    let tasks = StreamTasks::default();
    let gate_held = matches!(point, "mcp.dispatch.before" | "mcp.dispatch.handed_off");
    let policy_held = gate_held || point == "mcp.approval.policy";
    let sent_before = matches!(point, "mcp.dispatch.handed_off" | "mcp.dispatch.wait");
    let will_send = sent_before
        || match contender {
            Contender::Revocation => policy_held,
            Contender::Stop(_) => gate_held,
        };
    let gap = arm(&f, point);
    start(
        &f,
        &tasks,
        Arc::new(Agent {
            approvals: ask.then_some(approval),
            budget: matches!(transport, Transport::Stdio).then_some(std::time::Duration::ZERO),
            expect_budget_expiration: false,
            session: StdMutex::new(Some(session)),
            expected_reply: matches!(contender, Contender::Revocation).then_some(will_send),
        }),
    )
    .await;
    if ask {
        submit_yes(&f).await;
    }
    reached(&gap, point).await;
    assert_eq!(
        f.host.flow030().mcp_dispatch_holds_gate(),
        gate_held,
        "{transport:?} {contender:?} {point}, ask={ask}"
    );
    if sent_before {
        peer.sent().await;
    } else {
        assert!(!peer.sent.exists(), "{transport:?} {point}");
    }

    let change = match contender {
        Contender::Revocation => {
            assert_eq!(
                policy.try_write().is_err(),
                policy_held,
                "{transport:?} {point}"
            );
            tokio::spawn(async move { *policy.write().await = false })
        }
        Contender::Stop(path) => {
            let admission = arm(&f, "stop.admission");
            let change = tokio::spawn({
                let (f, tasks) = (f.clone(), tasks.clone());
                async move { stop(&f, &tasks, path).await }
            });
            reached(&admission, "stop.admission").await;
            admission.release();
            change
        }
    };
    let blocked = match contender {
        Contender::Revocation => policy_held,
        Contender::Stop(_) => gate_held,
    };
    // The winning transition must commit before the schedule resumes.
    let mut change = Some(change);
    if !blocked {
        tokio::time::timeout(WAIT, change.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
    if point == "mcp.dispatch.before" {
        let sent = arm(&f, "mcp.dispatch.handed_off");
        gap.release();
        reached(&sent, "mcp.dispatch.handed_off").await;
        peer.sent().await;
        assert!(
            f.host.flow030().mcp_dispatch_holds_gate(),
            "{transport:?} {contender:?} at handoff, ask={ask}"
        );
        sent.release();
    } else {
        gap.release();
    }
    if let Some(change) = change {
        tokio::time::timeout(WAIT, change).await.unwrap().unwrap();
    }

    match contender {
        Contender::Revocation => {
            std::fs::write(&peer.reply, b"reply").unwrap();
            tokio::time::timeout(
                WAIT,
                f.host
                    .flow030()
                    .supervisors
                    .wait(&f.identity.system_start_id),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                inspect(&f).await.state.verdict.as_deref(),
                Some("succeeded")
            );
            journal(&f).await;
        }
        Contender::Stop(_) if will_send && matches!(transport, Transport::Http) => {
            let error = tokio::time::timeout(
                WAIT,
                f.host
                    .flow030()
                    .supervisors
                    .wait(&f.identity.system_start_id),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert_eq!(error.code, "MCP_TERMINATION_UNCERTAIN", "{point}");
            let stopping: bool = sqlx::query_scalar(
                "SELECT json_extract(state_json, '$.stopping') FROM engine_flow030_runs",
            )
            .fetch_one(&f.pool)
            .await
            .unwrap();
            assert!(stopping);
            journal(&f).await;
        }
        Contender::Stop(_) => stopped(&f).await,
    }
    assert_eq!(
        peer.sent.exists(),
        will_send,
        "{transport:?} {contender:?} {point}"
    );
    if let Some(http) = http {
        http.abort();
        assert!(http.await.unwrap_err().is_cancelled());
    }
}
