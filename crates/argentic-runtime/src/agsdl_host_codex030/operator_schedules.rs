use super::*;
use crate::agsdl_host_flow030::CommitTestHook030;
use crate::api::OperatorAction as Action;

#[path = "operator_boundary_schedules.rs"]
mod boundaries;

const WAIT: std::time::Duration = std::time::Duration::from_secs(15);

fn exhaustive() -> bool {
    std::env::var_os("ARGENTIC_OPERATOR_EXHAUSTIVE").as_deref() == Some(std::ffi::OsStr::new("1"))
}

async fn reached(hook: &CommitTestHook030, name: &str) {
    tokio::time::timeout(WAIT, hook.reached())
        .await
        .unwrap_or_else(|_| panic!("actor did not reach {name}"));
}

fn arm(f: &Fixture, point: &'static str) -> Arc<CommitTestHook030> {
    f.host.flow030().install_operator_test_hook(point)
}

async fn journal(f: &Fixture) {
    let result = crate::persistence::replay_verify030::verify_path030(
        &f._temp.path().join("run.db"),
        &f.identity.system_start_id,
        &f.identity.flow_invocation_id,
    )
    .await;
    assert!(result.ok, "{:?}", result.divergence);
}

async fn inspect(f: &Fixture) -> crate::persistence::flow_store030::FlowView030 {
    flow::inspect_run(f.host.flow030(), &f.host, &f.pool, &f.owner)
        .await
        .unwrap()
        .flow
}

#[derive(Clone, Copy, Debug)]
enum StopPath {
    Internal,
    Public,
}

const STOP_PATHS: [StopPath; 2] = [StopPath::Internal, StopPath::Public];

async fn stop(f: &Fixture, tasks: &StreamTasks, path: StopPath) {
    let runtime = f.host.flow030();
    let request = EngineStopRequest {
        system_start_id: f.identity.system_start_id.clone(),
        request_id: "scheduled-stop".into(),
        activation_id: None,
    };
    if matches!(path, StopPath::Public) {
        for _ in 0..2 {
            assert!(matches!(
                f.host.request_stop(&f.pool, tasks, &request).await.unwrap(),
                crate::persistence::public::PublicStopState::Requested { request_id }
                    if request_id == request.request_id
            ));
        }
        return;
    }
    let first = flow::request_system_stop030(runtime, &f.pool, &request)
        .await
        .unwrap();
    let retry = flow::request_system_stop030(runtime, &f.pool, &request)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(first.receipt()).unwrap(),
        serde_json::to_value(retry.receipt()).unwrap()
    );
    runtime.supervisors.wake(&f.identity.system_start_id);
}

async fn stopped(f: &Fixture) {
    let system = &f.identity.system_start_id;
    match tokio::time::timeout(WAIT, f.host.flow030().supervisors.wait(system)).await {
        Ok(outcome) => outcome.expect("supervisor lost authority during Stop"),
        Err(_) => {
            let effects: Vec<(String, String)> =
                sqlx::query_as("SELECT effect_id,status FROM engine_flow030_outbox ORDER BY rowid")
                    .fetch_all(&f.pool)
                    .await
                    .unwrap();
            panic!("supervisor did not exit after Stop: {effects:?}");
        }
    }
    let view = inspect(f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("cancelled"));
    assert!(view.state.activations.iter().all(|a| !a.physical));
    for row in &view.effects {
        assert_ne!(row["status"], "uncertain", "{row}");
        let effect = serde_json::from_value(row["effect"].clone()).unwrap();
        assert!(
            row["status"] != "pending"
                || !crate::persistence::flow_store030::eligible(&view.state, &effect),
            "{row}"
        );
    }
    journal(f).await;
}

async fn start(f: &Fixture, tasks: &StreamTasks, agent: Arc<dyn AgentExecutor030>) {
    let runtime = f.host.flow030();
    let capture = flow::inspect_run(runtime, &f.host, &f.pool, &f.owner)
        .await
        .unwrap()
        .capture;
    *runtime.fixture_agents.lock().unwrap() = Some(agent);
    runtime
        .supervisors
        .reserve(&f.identity.system_start_id)
        .unwrap();
    f.host
        .inspect_start_session()
        .await
        .unwrap()
        .attach_committed_start(
            &f.pool,
            f.config.clone(),
            tasks.clone(),
            &f.identity,
            &capture,
            Arc::new(UnusedActionPolicy),
        )
        .await
        .unwrap();
}

fn document(steps: Value, entry: &str) -> Value {
    json!({
        "contract":"agsdl-0.4.0","id":"operator-interleaving",
        "agents":{"worker":{"harness":"codex","permissions":"read-only"}},
        "flow":{"entry":entry,"steps":steps}
    })
}

#[derive(Default)]
struct ScheduledAgent {
    running: Arc<Notify>,
    fail: Arc<Notify>,
}
impl AgentExecutor030 for ScheduledAgent {
    fn supports_operator_hold(&self) -> bool {
        true
    }
    fn prepare(
        &self,
        work: crate::agsdl_host_work_admission030::PreparedEffectiveAgentWork030,
    ) -> HostResult<crate::agsdl_host_work_admission030::AttestedWorkPolicy030> {
        PausableAgent::default().prepare(work)
    }
    fn run(
        &self,
        work: AgentWork030,
        events: WorkEventSink030,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, HostResult<()>> {
        if work.attempt.work().step.as_deref() != Some(&["work".into()]) {
            return PausableAgent::default().run(work, events, cancel);
        }
        self.running.notify_one();
        let fail = self.fail.clone();
        Box::pin(async move {
            tokio::select! {
                _ = cancel.cancelled() => Ok(()),
                _ = fail.notified() => Err(crate::persistence::failure("CODEX_RUNNER_EXITED", "turn", "Scripted provider failure")),
            }
        })
    }
}

#[tokio::test]
async fn operator_schedule_join_after_pause_ack_loss() {
    for stop_path in STOP_PATHS {
        for boundary in [
            "supervisor.joined",
            "work.join.reserved",
            "work.join.evaluated",
        ] {
            let f = Arc::new(
            Fixture::with_document(document(
                json!({
                    "work":{"type":"agent","agent":"worker","timeoutMs":10000,"onError":"handler"},
                    "handler":{"type":"agent","agent":"worker"}
                }),
                "work",
            ))
            .await,
        );
            let scope = operator_scope(&f).await;
            let tasks = StreamTasks::default();
            let agent = Arc::new(ScheduledAgent::default());
            let joining = arm(&f, boundary);
            start(&f, &tasks, agent.clone()).await;
            agent.fail.notify_one();
            reached(&joining, boundary).await;
            let ack = arm(&f, "operator.ack");
            let pausing = tokio::spawn({
                let (f, scope) = (f.clone(), scope.clone());
                async move { operator_control(&f, &scope, "lost-pause", Action::PauseRun).await }
            });
            reached(&ack, "operator.ack").await;
            let recorded: String = sqlx::query_scalar(
                "SELECT receipt_json FROM engine_flow030_events WHERE event_id LIKE 'operator:%'",
            )
            .fetch_one(&f.pool)
            .await
            .unwrap();
            let recorded: crate::persistence::flow030::FlowReceipt030 =
                serde_json::from_str(&recorded).unwrap();
            pausing.abort();
            assert!(pausing.await.unwrap_err().is_cancelled());
            joining.release();

            // Finalization has to succeed on its own. A caller retry would otherwise
            // clear the marker and hide the race.
            operator_wait(async || {
                assert!(
                    f.host
                        .flow030()
                        .supervisors
                        .outcome(&scope.system_start_id)
                        .is_none(),
                    "{boundary}: {:?}",
                    f.host.flow030().supervisors.outcome(&scope.system_start_id)
                );
                inspect(&f).await.state.activations[0]
                    .result
                    .as_ref()
                    .map(|_| ())
            })
            .await;
            assert_eq!(
                operator_control(&f, &scope, "lost-pause", Action::PauseRun)
                    .await
                    .unwrap(),
                recorded
            );
            stop(&f, &tasks, stop_path).await;
            stopped(&f).await;
        }
    }
}

#[tokio::test]
async fn operator_schedule_stop_between_start_selection_and_claim() {
    let cases = [
        (
            "agent",
            json!({"type":"agent","agent":"worker"}),
            &["supervisor.agent.selected"][..],
        ),
        (
            "approval",
            json!({"type":"approval","next":{"yes":[],"no":[]}}),
            &["supervisor.host.selected", "approval.claim"],
        ),
        (
            "call.command",
            json!({"type":"call","command":"true"}),
            &["supervisor.host.selected", "call.reserved", "call.claim"],
        ),
        (
            "call.mcp",
            json!({"type":"call","mcp":{"server":"local","tool":"check"}}),
            &["supervisor.host.selected", "call.reserved", "call.claim"],
        ),
    ];
    // Compose and join run inside the reducer and never emit a Start. Check
    // both editions so new step types and call variants require a schedule.
    for schema in [
        include_str!("../../../argentic-core/assets/schema/agsdl-030.schema.json"),
        include_str!("../../../argentic-core/assets/schema/agsdl.schema.json"),
    ] {
        let schema: Value = serde_json::from_str(schema).unwrap();
        let defs = &schema["$defs"];
        let mut kinds = std::collections::BTreeSet::new();
        for kind in defs["step"]["properties"]["type"]["enum"]
            .as_array()
            .unwrap()
        {
            match kind.as_str().unwrap() {
                "compose" | "join" => (),
                "call" => {
                    kinds.extend(defs["callStep"]["oneOf"].as_array().unwrap().iter().map(
                        |variant| format!("call.{}", variant["required"][0].as_str().unwrap()),
                    ))
                }
                kind => {
                    kinds.insert(kind.to_owned());
                }
            }
        }
        assert_eq!(
            kinds,
            cases
                .iter()
                .map(|(kind, _, _)| (*kind).to_owned())
                .collect()
        );
    }

    for stop_path in STOP_PATHS {
        for (kind, step, boundaries) in &cases {
            for &boundary in *boundaries {
                let mut doc = if *kind == "agent" {
                    document(json!({"check":step}), "check")
                } else {
                    document(
                        json!({
                            "seed":{"type":"agent","agent":"worker","next":["work","check"]},
                            "work":{"type":"agent","agent":"worker"},
                            "check":step
                        }),
                        "seed",
                    )
                };
                let capabilities = match *kind {
                    "call.command" => &["call.command"][..],
                    "call.mcp" => {
                        doc["mcpServers"] = json!({"local":{"command":["true"]}});
                        &["call.mcp"]
                    }
                    _ => &[],
                };
                let f = Fixture::with_capabilities(doc, "system", capabilities).await;
                let tasks = StreamTasks::default();
                let claim = arm(&f, boundary);
                start(&f, &tasks, Arc::new(ScheduledAgent::default())).await;
                reached(&claim, boundary).await;
                if *kind != "agent" {
                    assert!(inspect(&f)
                        .await
                        .state
                        .activations
                        .iter()
                        .any(|a| a.step.as_deref() == Some(&["work".into()]) && a.physical));
                }
                stop(&f, &tasks, stop_path).await;
                claim.release();
                stopped(&f).await;
                let reservation: Option<String> =
                    sqlx::query_scalar("SELECT status FROM engine_flow030_calls")
                        .fetch_optional(&f.pool)
                        .await
                        .unwrap();
                let expected = match boundary {
                    "call.reserved" => Some("reserved"),
                    "call.claim" => Some("prepared"),
                    _ => None,
                };
                assert_eq!(
                    reservation.as_deref(),
                    expected,
                    "{kind} at {boundary} via {stop_path:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn operator_schedule_stop_after_deadline() {
    for stop_path in STOP_PATHS {
        stop_before_clock_retirement(true, stop_path).await;
    }
}

#[tokio::test]
async fn operator_schedule_stop_after_failure() {
    for stop_path in STOP_PATHS {
        stop_before_clock_retirement(false, stop_path).await;
    }
}

async fn stop_before_clock_retirement(timeout: bool, stop_path: StopPath) {
    let f = Fixture::with_document(document(
        json!({
            "work":{"type":"agent","agent":"worker","timeoutMs":10000,"onError":"handler"},
            "handler":{"type":"agent","agent":"worker"}
        }),
        "work",
    ))
    .await;
    let runtime = f.host.flow030();
    let clock = runtime.human_runtime().virtual_clock().await.unwrap();
    let tasks = StreamTasks::default();
    let agent = Arc::new(ScheduledAgent::default());
    start(&f, &tasks, agent.clone()).await;
    tokio::time::timeout(WAIT, agent.running.notified())
        .await
        .unwrap();
    let boundary = if timeout {
        "supervisor.pass"
    } else {
        "supervisor.retirement"
    };
    let gap = arm(&f, boundary);
    if timeout {
        runtime.supervisors.wake(&f.identity.system_start_id);
        reached(&gap, boundary).await;
        let broker = runtime
            .humans
            .broker(&f.host, &f.pool, &f.identity)
            .await
            .unwrap();
        let _operation = broker.operation.lock().await;
        clock.advance_to(10001).unwrap();
        broker.tick(&f.host).await.unwrap();
        broker
            .pump(&f.host, &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(broker.timeout_controls().await.unwrap().len(), 1);
    } else {
        agent.fail.notify_one();
        reached(&gap, boundary).await;
        assert!(inspect(&f).await.state.activations[0].result.is_some());
    }
    stop(&f, &tasks, stop_path).await;
    gap.release();
    stopped(&f).await;
}

#[tokio::test]
async fn operator_schedule_recover_paused_approval_at_clock_boundaries() {
    let boundaries: &[_] = if exhaustive() {
        &[
            "clock.reserved",
            "clock.evaluated",
            "clock.commit",
            "clock.ack",
        ]
    } else {
        &["clock.reserved", "clock.ack"]
    };
    for &boundary in boundaries {
        let f = Arc::new(
            Fixture::with_document(document(
                json!({
                    "approval":{"type":"approval","timeoutMs":1000,"next":{"yes":[],"no":[]}}
                }),
                "approval",
            ))
            .await,
        );
        let scope = operator_scope(&f).await;
        let runtime = f.host.flow030();
        let clock = runtime.human_runtime().virtual_clock().await.unwrap();
        let tasks = StreamTasks::default();
        start(&f, &tasks, Arc::new(ScheduledAgent::default())).await;
        operator_wait(async || {
            sqlx::query_scalar::<_, String>(
                "SELECT request_id FROM engine_flow030_human_requests WHERE kind='approval'",
            )
            .fetch_optional(&f.pool)
            .await
            .unwrap()
        })
        .await;
        operator_control(&f, &scope, "pause", Action::PauseRun)
            .await
            .unwrap();
        let dispatch = arm(&f, "supervisor.pass");
        runtime.supervisors.wake(&scope.system_start_id);
        reached(&dispatch, "supervisor.pass").await;
        let gap = arm(&f, boundary);
        clock.advance_to(1001).unwrap();
        let ticking = tokio::spawn({
            let f = f.clone();
            async move {
                let broker = f
                    .host
                    .flow030()
                    .humans
                    .broker(&f.host, &f.pool, &f.identity)
                    .await
                    .unwrap();
                let _operation = broker.operation.lock().await;
                broker.tick(&f.host).await.unwrap();
                broker
                    .pump(&f.host, &CancellationToken::new())
                    .await
                    .unwrap();
            }
        });
        reached(&gap, boundary).await;
        ticking.abort();
        assert!(ticking.await.unwrap_err().is_cancelled());
        let reopened = crate::sqlite::open_file(&f._temp.path().join("run.db"))
            .await
            .unwrap();
        crate::api::interrupt_previous_runs(&reopened)
            .await
            .unwrap();
        assert_eq!(
            operator_view(&f, &scope).await.verdict.as_deref(),
            Some("interrupted")
        );
        journal(&f).await;
        runtime.supervisors.cancel(&scope.system_start_id);
        dispatch.release();
        assert!(
            tokio::time::timeout(WAIT, runtime.supervisors.wait(&scope.system_start_id))
                .await
                .unwrap()
                .is_err()
        );
        journal(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_confirmation_on_either_side_of_pause() {
    for stop_path in STOP_PATHS {
        let schedules: &[_] = if exhaustive() {
            &[(false, false), (true, false), (false, true), (true, true)]
        } else {
            &[(false, false), (true, false), (true, true)]
        };
        for &(hold_first, interrupt) in schedules {
            let f = Fixture::with_document(document(
                json!({
                    "seed":{"type":"agent","agent":"worker","next":["work","steer"]},
                    "work":{"type":"agent","agent":"worker"},
                    "steer":{"type":"agent","agent":"worker","delivery":"steer"}
                }),
                "seed",
            ))
            .await;
            let scope = operator_scope(&f).await;
            let tasks = StreamTasks::default();
            let steer = Arc::new(DeferredSteer::default());
            let agent = Arc::new(PausableAgent {
                deferred_steer: Some(steer.clone()),
                confirm_deferred_steer: true,
                ..Default::default()
            });
            let gap = arm(&f, "steer.receipt");
            start(&f, &tasks, agent).await;
            tokio::time::timeout(WAIT, steer.sent.notified())
                .await
                .unwrap();
            let active = inspect(&f)
                .await
                .state
                .activations
                .into_iter()
                .find(|a| a.step.as_deref() == Some(&["work".into()]))
                .unwrap()
                .id;
            let action = if interrupt {
                Action::InterruptWork {
                    activation_id: active,
                }
            } else {
                Action::PauseRun
            };
            steer.report.notify_one();
            reached(&gap, "steer.receipt").await;
            if !hold_first {
                gap.release();
                wait_for_consumption(&f).await;
            }
            let receipt = operator_control(&f, &scope, "hold", action.clone())
                .await
                .unwrap();
            assert_eq!(
                operator_control(&f, &scope, "hold", action).await.unwrap(),
                receipt
            );
            if hold_first {
                gap.release();
                wait_for_consumption(&f).await;
            }
            stop(&f, &tasks, stop_path).await;
            stopped(&f).await;
        }
    }
}

async fn wait_for_consumption(f: &Fixture) {
    operator_wait(async || {
        inspect(f)
            .await
            .state
            .steer_deliveries
            .iter()
            .any(|d| d.status == "consumed")
            .then_some(())
    })
    .await;
}

struct QueuedAgent {
    started: Arc<std::sync::Mutex<Vec<String>>>,
}
impl AgentExecutor030 for QueuedAgent {
    fn supports_operator_hold(&self) -> bool {
        true
    }

    fn prepare(
        &self,
        work: crate::agsdl_host_work_admission030::PreparedEffectiveAgentWork030,
    ) -> HostResult<crate::agsdl_host_work_admission030::AttestedWorkPolicy030> {
        PausableAgent::default().prepare(work)
    }

    fn run(
        &self,
        work: AgentWork030,
        events: WorkEventSink030,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, HostResult<()>> {
        let crate::persistence::flow030::FlowEffectKind030::Start { message_json, .. } =
            &work.effect.kind
        else {
            unreachable!()
        };
        let message: Value = serde_json::from_str(message_json.as_deref().unwrap()).unwrap();
        self.started
            .lock()
            .unwrap()
            .push(message["prompt"].as_str().unwrap().into());
        PausableAgent::default().run(work, events, cancel)
    }
}

#[tokio::test]
async fn operator_schedule_preserves_delivery_order_after_ack_loss() {
    let schedules: &[_] = if exhaustive() {
        &[
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ]
    } else {
        &[[0, 1, 2], [2, 1, 0]]
    };
    for order in schedules {
        let f = Arc::new(
            Fixture::with_document(json!({
                "contract":"agsdl-0.4.0","id":"scheduled-deliveries",
                "agents":{"worker":{"harness":"codex","permissions":"read-only"}}
            }))
            .await,
        );
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let names = ["one", "two", "three"];
        let started = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dispatch = arm(&f, "supervisor.pass");
        start(
            &f,
            &tasks,
            Arc::new(QueuedAgent {
                started: started.clone(),
            }),
        )
        .await;
        reached(&dispatch, "supervisor.pass").await;
        operator_control(&f, &scope, "pause", Action::PauseRun)
            .await
            .unwrap();
        let ack = arm(&f, "delivery.ack");
        let delivery = tokio::spawn({
            let (f, scope, id) = (f.clone(), scope.clone(), names[order[0]]);
            async move { deliver_operator(&f, &scope, id, "queue").await }
        });
        reached(&ack, "delivery.ack").await;
        let event: (String, String) = sqlx::query_as("SELECT event_id,receipt_json ->> '$.revision' FROM engine_flow030_events WHERE event_id LIKE 'deliver030:%'")
            .fetch_one(&f.pool).await.unwrap();
        delivery.abort();
        assert!(delivery.await.unwrap_err().is_cancelled());
        for &i in &order[1..] {
            deliver_operator(&f, &scope, names[i], "queue")
                .await
                .unwrap();
        }
        operator_control(&f, &scope, "resume", Action::ResumeRun)
            .await
            .unwrap();
        let retry = deliver_operator(&f, &scope, names[order[0]], "queue")
            .await
            .unwrap();
        assert_eq!((retry.event_id, retry.revision), event);
        journal(&f).await;
        dispatch.release();
        tokio::time::timeout(
            WAIT,
            f.host.flow030().supervisors.wait(&scope.system_start_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(*started.lock().unwrap(), order.map(|i| names[i]));
        assert_eq!(
            operator_view(&f, &scope).await.verdict.as_deref(),
            Some("succeeded")
        );
        journal(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_stop_reads_durable_pause_after_ack_loss() {
    for stop_path in STOP_PATHS {
        let f = Arc::new(
            Fixture::with_document(document(
                json!({
                    "work":{"type":"agent","agent":"worker"}
                }),
                "work",
            ))
            .await,
        );
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let dispatch = arm(&f, "supervisor.pass");
        start(&f, &tasks, Arc::new(ScheduledAgent::default())).await;
        reached(&dispatch, "supervisor.pass").await;
        let ack = arm(&f, "operator.ack");
        let pausing = tokio::spawn({
            let (f, scope) = (f.clone(), scope.clone());
            async move { operator_control(&f, &scope, "pause", Action::PauseRun).await }
        });
        reached(&ack, "operator.ack").await;
        pausing.abort();
        assert!(pausing.await.unwrap_err().is_cancelled());

        // Stop must read through the lost Pause acknowledgement without depending
        // on either an operator retry or a fresh supervisor pass.
        let stop_ack = arm(&f, "stop.ack");
        let stopping = tokio::spawn({
            let (f, tasks) = (f.clone(), tasks.clone());
            async move { stop(&f, &tasks, stop_path).await }
        });
        reached(&stop_ack, "stop.ack").await;
        stopping.abort();
        assert!(stopping.await.unwrap_err().is_cancelled());
        stop(&f, &tasks, stop_path).await;
        dispatch.release();
        stopped(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_crash_during_approval_start() {
    let boundaries: &[_] = if exhaustive() {
        &[
            "supervisor.host.selected",
            "approval.claim",
            "effect.admission",
            "effect.claim.commit",
            "effect.claim.ack",
            "effect.handoff.commit",
            "effect.handoff.ack",
            "approval.admission",
            "approval.clock.reserve",
            "approval.clock.ack",
            "clock.reserved",
            "clock.commit",
            "clock.ack",
        ]
    } else {
        &["effect.claim.ack", "approval.clock.ack", "clock.ack"]
    };
    for &boundary in boundaries {
        let f = Fixture::with_document(document(
            json!({
                "approval":{"type":"approval","timeoutMs":1000,"next":{"yes":[],"no":[]}}
            }),
            "approval",
        ))
        .await;
        let scope = operator_scope(&f).await;
        let runtime = f.host.flow030();
        runtime.human_runtime().virtual_clock().await.unwrap();
        let tasks = StreamTasks::default();
        let gap = arm(&f, boundary);
        start(&f, &tasks, Arc::new(ScheduledAgent::default())).await;
        reached(&gap, boundary).await;
        runtime
            .supervisors
            .crash_operator_fixture(&scope.system_start_id)
            .await;

        let db = f._temp.path().join("run.db");
        let reopened = crate::sqlite::open_file(&db).await.unwrap();
        crate::api::interrupt_previous_runs(&reopened)
            .await
            .unwrap();
        assert_eq!(
            operator_view(&f, &scope).await.verdict.as_deref(),
            Some("interrupted"),
            "{boundary}"
        );
        journal(&f).await;
        // The same durable interruption and its binding are visible on the
        // next restart, regardless of what happened before revocation.
        let reopened_again = crate::sqlite::open_file(&db).await.unwrap();
        crate::api::interrupt_previous_runs(&reopened_again)
            .await
            .unwrap();
        journal(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_codex_plan_reserved_before_pause() {
    for stop_path in STOP_PATHS {
        let mut doc = document(json!({"work":{"type":"agent","agent":"worker"}}), "work");
        doc["agents"]["worker"]["planFirst"] = json!("auto");
        let f = Fixture::with_document(doc).await;
        let scope = operator_scope(&f).await;
        let trace = f._temp.path().join("trace.jsonl");
        let mut first = envelope(json!({"results":{}}));
        first["_plan"] = json!("current");
        std::fs::write(
            trace.with_extension("jsonl.script"),
            json!([first]).to_string(),
        )
        .unwrap();

        let plan = arm(&f, "work.plan.reserved");
        let tasks = StreamTasks::default();
        start(&f, &tasks, Arc::new(executor(&f, &trace, None))).await;
        reached(&plan, "work.plan.reserved").await;
        operator_control(&f, &scope, "pause", Action::PauseRun)
            .await
            .unwrap();
        plan.release();
        operator_wait(async || {
        let accepted: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM engine_flow030_work_events
             WHERE json_extract(decision_json ->> '$.receiptJson', '$.decision.kind')='planAccepted')",
        ).fetch_one(&f.pool).await.unwrap();
        accepted.then_some(())
    }).await;
        stop(&f, &tasks, stop_path).await;
        stopped(&f).await;
        assert_eq!(requests(&trace, "turn/start").len(), 1);
    }
}

#[tokio::test]
async fn operator_schedule_crash_during_input_commit() {
    let exhaustive = exhaustive();
    let schedule = [
        ("operator.dispatch", true),
        ("operator.admission", true),
        ("operator.commit", true),
        ("operator.ack", true),
        ("operator.return", true),
        ("delivery.dispatch", false),
        ("delivery.admission", false),
        ("delivery.commit", false),
        ("delivery.ack", false),
    ];
    for (boundary, pause) in schedule {
        if !exhaustive && !boundary.ends_with(".ack") {
            continue;
        }
        let f = Arc::new(
            Fixture::with_document(json!({
                "contract":"agsdl-0.4.0","id":"crash-during-input",
                "agents":{"worker":{"harness":"codex","permissions":"read-only"}}
            }))
            .await,
        );
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let dispatch = arm(&f, "supervisor.pass");
        start(&f, &tasks, Arc::new(ScheduledAgent::default())).await;
        reached(&dispatch, "supervisor.pass").await;
        let gate = arm(&f, boundary);
        let caller = tokio::spawn({
            let (f, scope) = (f.clone(), scope.clone());
            async move {
                if pause {
                    operator_control(&f, &scope, "pause", Action::PauseRun)
                        .await
                        .unwrap();
                } else {
                    deliver_operator(&f, &scope, "delivery", "queue")
                        .await
                        .unwrap();
                }
            }
        });
        reached(&gate, boundary).await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let runtime = f.host.flow030();
        runtime
            .supervisors
            .crash_operator_fixture(&scope.system_start_id)
            .await;
        let db = f._temp.path().join("run.db");
        let reopened = crate::sqlite::open_file(&db).await.unwrap();
        crate::api::interrupt_previous_runs(&reopened)
            .await
            .unwrap();
        let view = operator_view(&f, &scope).await;
        assert_eq!(view.verdict.as_deref(), Some("interrupted"));
        assert_eq!(
            view.interrupted_while_paused,
            pause && matches!(boundary, "operator.ack" | "operator.return")
        );
        journal(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_crash_during_stop() {
    for stop_path in STOP_PATHS {
        let exhaustive = exhaustive();
        for boundary in [
            "stop.admission",
            "stop.commit",
            "stop.ack",
            "stopWork.admission",
            "stopWork.commit",
            "stopWork.ack",
        ] {
            if !exhaustive && !boundary.ends_with(".ack") {
                continue;
            }
            let f = Arc::new(
                Fixture::with_document(document(
                    json!({
                        "work":{"type":"agent","agent":"worker","timeoutMs":10000}
                    }),
                    "work",
                ))
                .await,
            );
            let scope = operator_scope(&f).await;
            let tasks = StreamTasks::default();
            let agent = Arc::new(ScheduledAgent::default());
            start(&f, &tasks, agent.clone()).await;
            tokio::time::timeout(WAIT, agent.running.notified())
                .await
                .unwrap();

            let gap = arm(&f, boundary);
            let request = tokio::spawn({
                let (f, tasks) = (f.clone(), tasks.clone());
                async move { stop(&f, &tasks, stop_path).await }
            });
            reached(&gap, boundary).await;
            if boundary.starts_with("stop.") {
                request.abort();
                assert!(request.await.unwrap_err().is_cancelled());
            } else {
                request.await.unwrap();
            }
            f.host
                .flow030()
                .supervisors
                .crash_operator_fixture(&scope.system_start_id)
                .await;
            let db = f._temp.path().join("run.db");
            let reopened = crate::sqlite::open_file(&db).await.unwrap();
            crate::api::interrupt_previous_runs(&reopened)
                .await
                .unwrap();
            let view = operator_view(&f, &scope).await;
            let verdict = match boundary {
                "stop.admission" | "stop.commit" => Some("interrupted"),
                "stopWork.ack" => Some("cancelled"),
                _ => None,
            };
            assert_eq!(view.verdict.as_deref(), verdict, "{boundary}");
            assert_eq!(view.stopping, verdict.is_none(), "{boundary}");
            journal(&f).await;
        }
    }
}

#[tokio::test]
async fn operator_schedule_stop_before_agent_failure_publication() {
    for stop_path in STOP_PATHS {
        let f = Fixture::with_document(document(
            json!({
                "work":{"type":"agent","agent":"worker","timeoutMs":10000}
            }),
            "work",
        ))
        .await;
        let tasks = StreamTasks::default();
        let agent = Arc::new(ScheduledAgent::default());
        start(&f, &tasks, agent.clone()).await;
        tokio::time::timeout(WAIT, agent.running.notified())
            .await
            .unwrap();

        let runtime = f.host.flow030();
        let pass = arm(&f, "supervisor.pass");
        runtime.supervisors.wake(&f.identity.system_start_id);
        reached(&pass, "supervisor.pass").await;
        stop(&f, &tasks, stop_path).await;

        let rejected = arm(&f, "work.failure.rejected");
        agent.fail.notify_one();
        reached(&rejected, "work.failure.rejected").await;
        rejected.release();
        pass.release();
        stopped(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_recovery_commit_is_idempotent() {
    for boundary in ["recovery.commit", "recovery.ack"] {
        let f = Arc::new(
            Fixture::with_document(document(
                json!({
                    "approval":{"type":"approval","next":{"yes":[],"no":[]}}
                }),
                "approval",
            ))
            .await,
        );
        let scope = operator_scope(&f).await;
        let db = f._temp.path().join("run.db");
        let gap = arm(&f, boundary);
        let recovering = tokio::spawn({
            let f = f.clone();
            let db = db.clone();
            async move {
                let reopened = crate::sqlite::open_file(&db).await.unwrap();
                flow::RECOVERY_TEST_HOST030
                    .scope(
                        f.host.as_ref().clone(),
                        crate::api::interrupt_previous_runs(&reopened),
                    )
                    .await
                    .unwrap();
            }
        });
        reached(&gap, boundary).await;
        recovering.abort();
        assert!(recovering.await.unwrap_err().is_cancelled());
        let reopened = crate::sqlite::open_file(&db).await.unwrap();
        crate::api::interrupt_previous_runs(&reopened)
            .await
            .unwrap();
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM engine_flow030_interruption_clock")
                .fetch_one(&reopened)
                .await
                .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            operator_view(&f, &scope).await.verdict.as_deref(),
            Some("interrupted")
        );
        journal(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_crash_at_work_and_retirement_boundaries() {
    if !exhaustive() {
        return;
    }
    for boundary in [
        "work.reserve.commit",
        "work.ack",
        "supervisor.joined",
        "work.join.reserved",
        "work.join.evaluated",
        "supervisor.retirement",
        "supervisor.clock.return",
    ] {
        let f = Fixture::with_document(document(
            json!({
                "work":{"type":"agent","agent":"worker","timeoutMs":10000,"onError":"handler"},
                "handler":{"type":"agent","agent":"worker"}
            }),
            "work",
        ))
        .await;
        let scope = operator_scope(&f).await;
        let runtime = f.host.flow030();
        runtime.human_runtime().virtual_clock().await.unwrap();
        let tasks = StreamTasks::default();
        let agent = Arc::new(ScheduledAgent::default());
        start(&f, &tasks, agent.clone()).await;
        tokio::time::timeout(WAIT, agent.running.notified())
            .await
            .unwrap();

        let activation = inspect(&f).await.state.activations.remove(0);
        let parent = format!(
            "flow030:{}:{}",
            scope.system_start_id,
            activation.start_effect_id.unwrap()
        );
        let gap = arm(&f, boundary);
        if boundary == "supervisor.clock.return" {
            runtime.supervisors.wake(&scope.system_start_id);
        } else {
            agent.fail.notify_one();
        }
        reached(&gap, boundary).await;
        runtime
            .supervisors
            .crash_operator_fixture(&scope.system_start_id)
            .await;
        // The simulated process loss also drops the synthetic owned executor.
        if matches!(
            boundary,
            "work.reserve.commit" | "work.ack" | "supervisor.clock.return"
        ) {
            tasks.cancel_owned(&parent).await.unwrap();
        }
        let db = f._temp.path().join("run.db");
        let reopened = crate::sqlite::open_file(&db).await.unwrap();
        crate::api::interrupt_previous_runs(&reopened)
            .await
            .unwrap();
        assert_eq!(
            operator_view(&f, &scope).await.verdict.as_deref(),
            Some("interrupted"),
            "{boundary}"
        );
        journal(&f).await;
    }
}
