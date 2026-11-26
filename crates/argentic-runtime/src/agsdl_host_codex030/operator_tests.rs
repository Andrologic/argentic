//! Operator transitions through the native supervisor and scripted Codex peer.
use super::*;
use crate::persistence::dto::EngineStopRequest;

#[path = "operator_schedules.rs"]
mod schedules;

async fn operator_scope(f: &Fixture) -> crate::api::RuntimeProjectionScope030 {
    let capture = flow::inspect_run(f.host.flow030(), &f.host, &f.pool, &f.owner)
        .await
        .unwrap()
        .capture;
    crate::api::RuntimeProjectionScope030 {
        system_start_id: f.owner.system_start_id.clone(),
        source: capture.source,
        snapshot_id: f.identity.snapshot_id.clone(),
    }
}

async fn operator_control(
    f: &Fixture,
    scope: &crate::api::RuntimeProjectionScope030,
    request_id: &str,
    action: crate::api::OperatorAction,
) -> HostResult<crate::persistence::flow030::FlowReceipt030> {
    f.host
        .control_run(
            &f.pool,
            crate::api::OperatorRequest {
                scope: scope.clone(),
                request_id: request_id.into(),
                action,
            },
        )
        .await
}

async fn operator_view(
    f: &Fixture,
    scope: &crate::api::RuntimeProjectionScope030,
) -> crate::api::RuntimeProjectionPage030 {
    f.host
        .runtime_projection(
            &f.pool,
            crate::api::RuntimeProjectionQuery030 {
                system_start_id: scope.system_start_id.clone(),
                source: scope.source.clone(),
                snapshot_id: Some(scope.snapshot_id.clone()),
            },
            None,
        )
        .await
        .unwrap()
}

async fn operator_wait<T>(mut check: impl AsyncFnMut() -> Option<T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if let Some(value) = check().await {
                return value;
            }
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        }
    })
    .await
    .expect("operator state did not arrive")
}

#[tokio::test]
async fn operator_and_message_retries_clear_only_their_exact_committed_event() {
    use crate::api::OperatorAction as Action;
    use crate::persistence::flow030::{
        FlowEvent030, FlowEventKind030, Instance030, OperatorEvent030,
    };
    let f = Fixture::with_document(json!({
        "contract":"agsdl-0.4.0","id":"operator-commit-retry",
        "agents":{"worker":{"harness":"codex","permissions":"read-only"}}
    }))
    .await;
    let scope = operator_scope(&f).await;
    let runtime = f.host.flow030();
    runtime
        .supervisor_registry()
        .reserve(&scope.system_start_id)
        .unwrap();
    let revision = async || {
        crate::agsdl_host_flow030::inspect_run(runtime, &f.host, &f.pool, &f.owner)
            .await
            .unwrap()
            .flow
            .state
            .revision
    };

    let expected_revision = revision().await;
    let paused = operator_control(&f, &scope, "pause", Action::PauseRun)
        .await
        .unwrap();
    let marker = FlowEvent030 {
        identity: f.identity.clone(),
        event_id: paused.event_id.clone(),
        expected_revision,
        kind: FlowEventKind030::Operator(OperatorEvent030::Pause {
            activation_ids: vec![],
        }),
    };
    // Recreate the exact state left by cancellation between COMMIT and ack.
    runtime.begin_event_commit(&marker).unwrap();
    assert!(runtime.available(&f.identity).is_err());
    assert_eq!(
        operator_control(&f, &scope, "pause", Action::ResumeRun)
            .await
            .unwrap_err()
            .code,
        "CONFLICT"
    );
    assert!(runtime.available(&f.identity).is_err());
    assert_eq!(
        operator_control(&f, &scope, "pause", Action::PauseRun)
            .await
            .unwrap(),
        paused
    );
    runtime.available(&f.identity).unwrap();

    let expected_revision = revision().await;
    let delivered = deliver_operator(&f, &scope, "queued", "queue")
        .await
        .unwrap();
    runtime
        .begin_event_commit(&FlowEvent030 {
            identity: f.identity.clone(),
            event_id: delivered.event_id.clone(),
            expected_revision,
            kind: FlowEventKind030::Deliver {
                agent: "worker".into(),
                instance: Instance030::Shared,
                delivery: "queue".into(),
                message_json: json!({"prompt":"queued"}).to_string(),
                sender: "host".into(),
            },
        })
        .unwrap();
    assert!(deliver_operator(&f, &scope, "queued", "steer")
        .await
        .is_err());
    assert!(runtime.available(&f.identity).is_err());
    assert_eq!(
        deliver_operator(&f, &scope, "queued", "queue")
            .await
            .unwrap()
            .event_id,
        delivered.event_id
    );
    runtime.available(&f.identity).unwrap();
    operator_control(&f, &scope, "resume", Action::ResumeRun)
        .await
        .unwrap();
    assert!(!operator_view(&f, &scope).await.paused);
}

#[tokio::test]
async fn supervisor_reconciles_lost_operator_and_delivery_commit_acks_before_retry() {
    use crate::api::OperatorAction as Action;
    use crate::persistence::flow030::{
        FlowEvent030, FlowEventKind030, Instance030, OperatorEvent030,
    };

    for lose_resume_ack in [false, true] {
        let f = Fixture::with_document(json!({
            "contract":"agsdl-0.4.0","id":"supervisor-commit-readback",
            "agents":{"worker":{"harness":"codex","permissions":"read-only"}}
        }))
        .await;
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let runtime = f.host.flow030();
        let registry = runtime.supervisor_registry();
        registry.reserve(&scope.system_start_id).unwrap();
        let trace = f._temp.path().join("trace.jsonl");
        std::fs::write(
            trace.with_extension("jsonl.script"),
            json!([envelope(json!({"results":{}}))]).to_string(),
        )
        .unwrap();
        let (expected_revision, event_id, kind) = if lose_resume_ack {
            operator_control(&f, &scope, "pause", Action::PauseRun)
                .await
                .unwrap();
            let delivery = deliver_operator(&f, &scope, "queued", "queue")
                .await
                .unwrap();
            let resume = operator_control(&f, &scope, "resume", Action::ResumeRun)
                .await
                .unwrap();
            (
                delivery.revision,
                resume.event_id,
                FlowEventKind030::Operator(OperatorEvent030::ResumeRun),
            )
        } else {
            let before = flow::inspect_run(runtime, &f.host, &f.pool, &f.owner)
                .await
                .unwrap();
            let delivery = deliver_operator(&f, &scope, "queued", "queue")
                .await
                .unwrap();
            (
                before.flow.state.revision,
                delivery.event_id,
                FlowEventKind030::Deliver {
                    agent: "worker".into(),
                    instance: Instance030::Shared,
                    delivery: "queue".into(),
                    message_json: json!({"prompt":"queued"}).to_string(),
                    sender: "host".into(),
                },
            )
        };
        runtime
            .begin_event_commit(&FlowEvent030 {
                identity: f.identity.clone(),
                event_id: event_id.clone(),
                expected_revision,
                kind,
            })
            .unwrap();
        // Dispatch has to start and complete the work before either caller retries.
        start_operator_supervisor(&f, Arc::new(executor(&f, &trace, None)), &tasks);
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            registry.wait(&scope.system_start_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            operator_view(&f, &scope).await.verdict.as_deref(),
            Some("succeeded")
        );
        let retried = if lose_resume_ack {
            operator_control(&f, &scope, "resume", Action::ResumeRun)
                .await
                .unwrap()
                .event_id
        } else {
            deliver_operator(&f, &scope, "queued", "queue")
                .await
                .unwrap()
                .event_id
        };
        assert_eq!(retried, event_id);
        assert_eq!(requests(&trace, "turn/start").len(), 1);
    }
}

fn start_operator_supervisor(f: &Fixture, agents: Arc<dyn AgentExecutor030>, tasks: &StreamTasks) {
    let (pool, app, identity) = (f.pool.clone(), f.host.as_ref().clone(), f.identity.clone());
    let executors = Executors030 {
        agents: BTreeMap::from([("codex".into(), agents)]),
        calls: None,
        humans: Some(Arc::new(
            crate::agsdl_host_human_executor030::HumanExecutor030 {
                pool: pool.clone(),
                tasks: tasks.clone(),
            },
        )),
        clock: Some(Arc::new(
            crate::agsdl_host_human_executor030::HumanClockRuntime030 {
                app: app.clone(),
                pool,
                tasks: tasks.clone(),
                identity,
            },
        )),
    };
    spawn_supervisor030(
        app,
        f.pool.clone(),
        f.config.clone(),
        tasks.clone(),
        f.owner.clone(),
        executors,
    )
    .unwrap();
}

async fn deliver_operator(
    f: &Fixture,
    scope: &crate::api::RuntimeProjectionScope030,
    id: &str,
    mode: &str,
) -> HostResult<crate::api::DeliveryReceipt030> {
    f.host
        .deliver_message(
            &f.pool,
            crate::api::DeliveryRequest030 {
                scope: scope.clone(),
                delivery_id: id.into(),
                agent_name: "worker".into(),
                instance: None,
                delivery: mode.into(),
                message_json: json!({"prompt":id}).to_string(),
            },
        )
        .await
}

#[tokio::test]
async fn operator_interrupt_keeps_a_codex_work_open_across_all_resume_paths() {
    use crate::api::{OperatorAction as Action, OperatorWorkState};
    for mode in ["steer", "queue", "explicit", "recover", "stop"] {
        let document = json!({"contract":"agsdl-0.4.0","id":"operator-hold",
            "agents":{"worker":{"harness":"codex","permissions":"read-only"}}});
        let f = Fixture::with_document(document).await;
        let scope = operator_scope(&f).await;
        let trace = f._temp.path().join("trace.jsonl");
        let mut continuation = envelope(json!({"results":{}}));
        if mode == "steer" {
            continuation["_waitForSteer"] = json!(true);
        }
        std::fs::write(
            trace.with_extension("jsonl.script"),
            json!([null, continuation, envelope(json!({"results":{}}))]).to_string(),
        )
        .unwrap();
        let clock = f
            .host
            .flow030()
            .human_runtime()
            .virtual_clock()
            .await
            .unwrap();
        let tasks = StreamTasks::default();
        let registry = f.host.flow030().supervisor_registry();
        registry.reserve(&f.owner.system_start_id).unwrap();
        start_operator_supervisor(&f, Arc::new(executor(&f, &trace, None)), &tasks);
        let initial = deliver_operator(&f, &scope, "initial", "queue")
            .await
            .unwrap();
        let activation_id = operator_wait(async || {
            if requests(&trace, "turn/start").is_empty() {
                return None;
            }
            operator_view(&f, &scope)
                .await
                .items
                .into_iter()
                .find_map(|item| match item {
                    crate::api::RuntimeProjectionItem030::Activation { value }
                        if value.phase == "running" =>
                    {
                        Some(value.id)
                    }
                    _ => None,
                })
        })
        .await;
        clock.advance_to(250).unwrap();
        let action = Action::InterruptWork {
            activation_id: activation_id.clone(),
        };
        let receipt = operator_control(&f, &scope, "interrupt", action.clone())
            .await
            .unwrap();
        assert_eq!(
            operator_control(&f, &scope, "interrupt", action)
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(
            operator_control(&f, &scope, "interrupt", Action::PauseRun)
                .await
                .unwrap_err()
                .code,
            "CONFLICT"
        );
        let held = operator_wait(async || {
            let page = operator_view(&f, &scope).await;
            (page.operator_works.get(&activation_id) == Some(&OperatorWorkState::Held))
                .then_some(page)
        })
        .await;
        assert!(held.verdict.is_none());
        assert_eq!(requests(&trace, "turn/interrupt").len(), 1);
        assert_eq!(requests(&trace, "turn/start").len(), 1);

        let machine_snapshot = async || -> Value {
            let raw: String = sqlx::query_scalar(
                "SELECT snapshot_json FROM engine_flow030_clock_works WHERE activation_id=?",
            )
            .bind(&activation_id)
            .fetch_one(&f.pool)
            .await
            .unwrap();
            serde_json::from_str(&raw).unwrap()
        };
        let elapsed = machine_snapshot().await["elapsedMachineMs"].clone();
        assert_eq!(elapsed, "250");
        clock.advance_to(2000).unwrap();
        let broker = f
            .host
            .flow030()
            .human_runtime()
            .broker(&f.host, &f.pool, &f.identity)
            .await
            .unwrap();
        {
            let _op = broker.operation.lock().await;
            broker.tick(&f.host).await.unwrap();
            broker
                .pump(&f.host, &CancellationToken::new())
                .await
                .unwrap();
        }
        let snapshot = machine_snapshot().await;
        assert_eq!(snapshot["operatorHeld"], true);
        assert_eq!(snapshot["elapsedMachineMs"], elapsed);
        assert_eq!(
            deliver_operator(&f, &scope, "initial", "queue")
                .await
                .unwrap()
                .event_id,
            initial.event_id
        );
        assert_eq!(
            operator_view(&f, &scope)
                .await
                .operator_works
                .get(&activation_id),
            Some(&OperatorWorkState::Held)
        );

        if mode == "recover" {
            let reopened = crate::sqlite::open_file(&f._temp.path().join("run.db"))
                .await
                .unwrap();
            crate::api::interrupt_previous_runs(&reopened)
                .await
                .unwrap();
            let view = f
                .host
                .runtime_projection(
                    &reopened,
                    crate::api::RuntimeProjectionQuery030 {
                        system_start_id: scope.system_start_id.clone(),
                        source: scope.source.clone(),
                        snapshot_id: Some(scope.snapshot_id.clone()),
                    },
                    None,
                )
                .await
                .unwrap();
            assert_eq!(view.verdict.as_deref(), Some("interrupted"));
            assert_eq!(
                view.operator_works.get(&activation_id),
                Some(&OperatorWorkState::Interrupted {
                    code: "INTERRUPTED".into()
                })
            );
            assert!(operator_control(
                &f,
                &scope,
                "recover-resume",
                Action::ResumeWork { activation_id }
            )
            .await
            .is_err());
            assert!(registry.wait(&scope.system_start_id).await.is_err());
            assert_eq!(requests(&trace, "turn/start").len(), 1);
            continue;
        }
        if mode == "stop" {
            f.host
                .request_stop(
                    &f.pool,
                    &tasks,
                    &EngineStopRequest {
                        system_start_id: scope.system_start_id.clone(),
                        request_id: "stop".into(),
                        activation_id: None,
                    },
                )
                .await
                .unwrap();
        } else if mode == "explicit" {
            operator_control(
                &f,
                &scope,
                "resume",
                Action::ResumeWork {
                    activation_id: activation_id.clone(),
                },
            )
            .await
            .unwrap();
        } else {
            deliver_operator(&f, &scope, "operator reply", mode)
                .await
                .unwrap();
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            registry.wait(&scope.system_start_id),
        )
        .await
        .unwrap()
        .unwrap_or_else(|error| panic!("operator branch did not drain: {error:?}"));
        let page = operator_view(&f, &scope).await;
        assert_eq!(
            page.verdict.as_deref(),
            Some(if mode == "stop" {
                "cancelled"
            } else {
                "succeeded"
            })
        );
        assert!(page.operator_works.is_empty());
        let starts = requests(&trace, "turn/start");
        assert_eq!(
            starts.len(),
            match mode {
                "stop" => 1,
                "queue" => 3,
                _ => 2,
            }
        );
        if starts.len() >= 2 {
            assert_eq!(starts[0]["threadId"], starts[1]["threadId"]);
            assert!(starts[1]["input"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("Continue the current work"));
        }
        if mode == "queue" {
            assert!(starts[2]["input"][0]["text"]
                .as_str()
                .unwrap()
                .contains("operator reply"));
        }
        if mode == "steer" {
            assert_eq!(requests(&trace, "turn/steer").len(), 1);
            assert!(page.items.iter().any(|item| matches!(item,
                crate::api::RuntimeProjectionItem030::Activation { value }
                    if value.message.as_ref().is_some_and(|message| message.statuses.iter().any(|status| status == "delivered")) && value.id != activation_id
            )));
        }
    }
}

struct UnusedActionPolicy;
impl crate::host::ActionPolicySource for UnusedActionPolicy {
    fn lock_current<'a>(
        &'a self,
        _: &'a StartCapture030,
        _: &'a Value,
    ) -> crate::host::PolicyFuture<'a, crate::host::ActionPolicyLease> {
        panic!("operator interrupt must close the question without consulting the permission grant")
    }
}

struct WaitingActionPolicy(Arc<Notify>);
impl crate::host::ActionPolicySource for WaitingActionPolicy {
    fn lock_current<'a>(
        &'a self,
        _: &'a StartCapture030,
        _: &'a Value,
    ) -> crate::host::PolicyFuture<'a, crate::host::ActionPolicyLease> {
        self.0.notify_one();
        Box::pin(std::future::pending())
    }
}

#[tokio::test]
async fn operator_interrupt_closes_a_codex_tool_approval_without_authorizing_it() {
    interrupt_codex_ask(None, false, false).await;
}

#[tokio::test]
async fn operator_interrupt_preserves_an_observed_plan_until_the_controller_accepts_it() {
    interrupt_codex_ask(Some("current"), false, false).await;
}

#[tokio::test]
async fn operator_interrupt_retains_the_first_plan_reported_during_interrupt() {
    interrupt_codex_ask(Some("interrupt"), false, false).await;
}

#[tokio::test]
async fn operator_interrupt_journals_an_accepted_answer_closed_before_delivery() {
    interrupt_codex_ask(None, true, false).await;
}

#[tokio::test]
async fn operator_interrupt_closes_an_action_approval_that_expires_in_its_clock_event() {
    interrupt_codex_ask(None, false, true).await;
}

async fn interrupt_codex_ask(plan: Option<&str>, answer_first: bool, expire_on_hold: bool) {
    use crate::api::{OperatorAction as Action, OperatorWorkState};
    let mut document = json!({"contract":"agsdl-0.4.0","id":"operator-approval","agents":{"worker":{"harness":"codex","permissions":{"preset":"workspace","reviewer":"person","rules":[{"action":"write","effect":"ask"}]}}},"flow":{"entry":"work","steps":{"work":{"type":"agent","agent":"worker","timeoutMs":1000}}}});
    if plan.is_some() {
        document["agents"]["worker"]["planFirst"] = json!("auto");
    }
    if expire_on_hold {
        document["agents"]["worker"]["answerTimeoutMs"] = json!(100);
    }
    let f = Fixture::with_document(document).await;
    approve_fixture_workspace(&f).await;
    let scope = operator_scope(&f).await;
    let root = f._temp.path().canonicalize().unwrap().join("workspace");
    let path = root.join("sentinel");
    std::fs::write(&path, "before").unwrap();
    let trace = f._temp.path().join("trace.jsonl");
    let mut result = envelope(json!({"results":{}}));
    if let Some(plan) = plan {
        result["_plan"] = json!(plan);
    }
    result["_approval"] = json!({"method":"item/commandExecution/requestApproval","id":"native-approval","params":{"kind":"command","environmentId":"local","itemId":"command","command":"synthetic command","cwd":root,"additionalPermissions":{"network":null,"fileSystem":{"read":null,"write":[path],"entries":[{"path":{"type":"path","path":path},"access":"write"}]}}}});
    std::fs::write(
        trace.with_extension("jsonl.script"),
        json!([result, envelope(json!({"results":{}}))]).to_string(),
    )
    .unwrap();
    let clock = f
        .host
        .flow030()
        .human_runtime()
        .virtual_clock()
        .await
        .unwrap();
    let tasks = StreamTasks::default();
    let registry = f.host.flow030().supervisor_registry();
    registry.reserve(&scope.system_start_id).unwrap();
    let policy_entered = Arc::new(Notify::new());
    let policy: Arc<dyn crate::host::ActionPolicySource> = if answer_first {
        Arc::new(WaitingActionPolicy(policy_entered.clone()))
    } else {
        Arc::new(UnusedActionPolicy)
    };
    let agent = executor(&f, &trace, None).with_approvals(Arc::new(
        crate::agsdl_host_action_approval030::AgentActionHumans030 {
            host: f.host.as_ref().clone(),
            pool: f.pool.clone(),
            policy,
        },
    ));
    start_operator_supervisor(&f, Arc::new(agent), &tasks);
    let (id, activation_id) = operator_wait(async || {
        sqlx::query_as::<_, (String, String)>("SELECT request_id, work_ids_json ->> '$[0]' FROM engine_flow030_human_requests WHERE kind='ask'")
            .fetch_optional(&f.pool).await.unwrap()
    }).await;
    if answer_first {
        let receipt = f
            .host
            .submit_person(
                &f.pool,
                crate::api::HumanSubmission {
                    scope: crate::persistence::public030::HumanScope030 {
                        system_start_id: scope.system_start_id.clone(),
                        source: scope.source.clone(),
                    },
                    request_id: id.clone(),
                    expected_revision: "0".into(),
                    answer: crate::agsdl_host_human030::HumanAnswer030::Decision {
                        decision: "yes".into(),
                        comment: "Approved before interruption".into(),
                    },
                },
            )
            .await
            .unwrap();
        let accepted: bool = sqlx::query_scalar(
            "SELECT accepted FROM engine_flow030_human_submissions WHERE receipt_id=?",
        )
        .bind(receipt)
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert!(accepted);
        tokio::time::timeout(std::time::Duration::from_secs(5), policy_entered.notified())
            .await
            .unwrap();
    }
    let hold = operator_control(
        &f,
        &scope,
        "hold",
        Action::InterruptWork {
            activation_id: activation_id.clone(),
        },
    );
    tokio::pin!(hold);
    if expire_on_hold {
        let (dispatch, _) = registry.dispatcher(&scope.system_start_id).unwrap();
        let gate = dispatch.lock().await;
        assert!(futures::poll!(&mut hold).is_pending());
        clock.advance_to(100).unwrap();
        drop(gate);
    }
    tokio::time::timeout(std::time::Duration::from_secs(5), hold)
        .await
        .unwrap()
        .unwrap();
    operator_wait(async || {
        (operator_view(&f, &scope)
            .await
            .operator_works
            .get(&activation_id)
            == Some(&OperatorWorkState::Held))
        .then_some(())
    })
    .await;
    let (resolution, delivery): (String, String) = sqlx::query_as(
        "SELECT resolution_json,delivery FROM engine_flow030_human_requests WHERE request_id=?",
    )
    .bind(&id)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    let resolution: Value = serde_json::from_str(&resolution).unwrap();
    if answer_first {
        assert_eq!(resolution["reason"], "answer");
        assert_eq!(resolution["answer"]["decision"], "yes");
    } else if expire_on_hold {
        assert_eq!(resolution["expired"], true);
        assert_eq!(resolution["resolvedAtMs"], "100");
        assert_eq!(resolution["reason"], "answerTimeout");
        assert_eq!(delivery, "reserved");
        let kind: String = sqlx::query_scalar(
            "SELECT json_extract(e.ticket_json ->> '$.eventJson', '$.event.kind')
             FROM engine_flow030_human_resolution_proofs p
             JOIN engine_flow030_clock_events e USING(system_id,invocation_id,event_id)
             WHERE p.request_id=?",
        )
        .bind(&id)
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(kind, "operatorHold");
    } else {
        assert_eq!(resolution["operatorCancelled"], true);
    }
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "before");
    clock.advance_to(5000).unwrap();
    let broker = f
        .host
        .flow030()
        .human_runtime()
        .broker(&f.host, &f.pool, &f.identity)
        .await
        .unwrap();
    {
        let _guard = broker.operation.lock().await;
        broker.tick(&f.host).await.unwrap();
        broker
            .pump(&f.host, &CancellationToken::new())
            .await
            .unwrap();
    }
    assert!(broker.timeout_controls().await.unwrap().is_empty());
    operator_control(&f, &scope, "resume", Action::ResumeWork { activation_id })
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        registry.wait(&scope.system_start_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        operator_view(&f, &scope).await.verdict.as_deref(),
        Some("succeeded")
    );
    assert_eq!(std::fs::read_to_string(path).unwrap(), "before");
    let replay = crate::persistence::replay_verify030::verify_path030(
        &f._temp.path().join("run.db"),
        &scope.system_start_id,
        &f.identity.flow_invocation_id,
    )
    .await;
    assert!(replay.ok, "{:?}", replay.divergence);
    if plan.is_some() {
        let plans: Vec<String> = sqlx::query_scalar(
            "SELECT json_extract(decision_json ->> '$.receiptJson', '$.decision.planId') FROM engine_flow030_work_events
             WHERE json_extract(decision_json ->> '$.receiptJson', '$.decision.kind')='planAccepted'",
        ).fetch_all(&f.pool).await.unwrap();
        assert_eq!(plans, ["codex:turn1"]);
    }
    if answer_first {
        sqlx::query(
            "UPDATE engine_flow030_human_requests SET delivery='acknowledged' WHERE request_id=?",
        )
        .bind(id)
        .execute(&f.pool)
        .await
        .unwrap();
        let forged = crate::persistence::replay_verify030::verify_path030(
            &f._temp.path().join("run.db"),
            &scope.system_start_id,
            &f.identity.flow_invocation_id,
        )
        .await;
        assert!(
            !forged.ok,
            "delivery without an acknowledgment was accepted"
        );
    }
}

#[tokio::test]
async fn operator_pause_defers_a_pending_start_and_stop_withdraws_it() {
    use crate::api::OperatorAction as Action;
    for stop in [false, true] {
        let document = json!({"contract":"agsdl-0.4.0","id":"operator-pending-start",
            "agents":{"worker":{"harness":"codex","permissions":"read-only"}},
            "flow":{"entry":"work","steps":{"work":{"type":"agent","agent":"worker"}}}});
        let f = Fixture::with_document(document).await;
        let scope = operator_scope(&f).await;
        let trace = f._temp.path().join("trace.jsonl");
        std::fs::write(
            trace.with_extension("jsonl.script"),
            json!([envelope(json!({"results":{}}))]).to_string(),
        )
        .unwrap();
        let tasks = StreamTasks::default();
        let registry = f.host.flow030().supervisor_registry();
        registry.reserve(&scope.system_start_id).unwrap();
        let paused = operator_control(&f, &scope, "pause", Action::PauseRun)
            .await
            .unwrap();
        assert_eq!(
            operator_control(&f, &scope, "pause", Action::PauseRun)
                .await
                .unwrap(),
            paused
        );
        start_operator_supervisor(&f, Arc::new(executor(&f, &trace, None)), &tasks);
        let view = operator_view(&f, &scope).await;
        assert!(view.paused);
        assert!(view.operator_works.is_empty());
        assert!(requests(&trace, "turn/start").is_empty());
        if stop {
            f.host
                .request_stop(
                    &f.pool,
                    &tasks,
                    &EngineStopRequest {
                        system_start_id: scope.system_start_id.clone(),
                        request_id: "stop".into(),
                        activation_id: None,
                    },
                )
                .await
                .unwrap();
        } else {
            operator_control(&f, &scope, "resume", Action::ResumeRun)
                .await
                .unwrap();
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            registry.wait(&scope.system_start_id),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(!operator_view(&f, &scope).await.paused);
        assert_eq!(
            operator_view(&f, &scope).await.verdict.as_deref(),
            Some(if stop { "cancelled" } else { "succeeded" })
        );
        assert_eq!(requests(&trace, "turn/start").len(), usize::from(!stop));
    }
}

#[derive(Default)]
struct DeferredSteer {
    sent: Notify,
    report: Notify,
}

#[derive(Default)]
struct PausableAgent {
    starts: std::sync::Mutex<Vec<String>>,
    finish_work: Arc<Notify>,
    disconnect_on_hold: bool,
    deferred_steer: Option<Arc<DeferredSteer>>,
    confirm_deferred_steer: bool,
}
impl AgentExecutor030 for PausableAgent {
    fn supports_operator_hold(&self) -> bool {
        true
    }

    fn prepare(
        &self,
        work: crate::agsdl_host_work_admission030::PreparedEffectiveAgentWork030,
    ) -> HostResult<crate::agsdl_host_work_admission030::AttestedWorkPolicy030> {
        crate::agsdl_host_work_admission030::AttestedWorkPolicy030::from_codex(work)
    }

    fn run(
        &self,
        mut work: AgentWork030,
        events: WorkEventSink030,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, HostResult<()>> {
        let step = work
            .attempt
            .work()
            .step
            .as_ref()
            .and_then(|s| s.last())
            .cloned()
            .unwrap_or_else(|| "message".into());
        self.starts.lock().unwrap().push(step.clone());
        let finish = self.finish_work.clone();
        let disconnect = self.disconnect_on_hold;
        let deferred_steer = self.deferred_steer.clone();
        let confirm = self.confirm_deferred_steer;
        Box::pin(async move {
            let sibling = disconnect && step == "sibling";
            if step == "work" || sibling {
                if let Some(deferred) = deferred_steer {
                    let steer = work.steer.receive().await.unwrap();
                    drop(steer.wire_admission().await?.unwrap());
                    let report = deferred.clone();
                    steer.handoff_observation(&work.tasks, &work.attempt, async move {
                        report.report.notified().await;
                        if confirm {
                            return Ok(Some(true));
                        }
                        Err(crate::persistence::failure(
                            "CORRELATION",
                            "steer",
                            "Scripted receipt named another work",
                        ))
                    })?;
                    deferred.sent.notify_one();
                }
                let hold = tokio::select! {
                    id = work.operator.requested() => id,
                    _ = cancel.cancelled() => return Ok(()),
                };
                if disconnect && !sibling {
                    return Err(crate::persistence::failure(
                        "CODEX_RUNNER_EXITED",
                        "turn",
                        "Scripted disconnect before interruption confirmation",
                    ));
                }
                tokio::select! {
                    result = work.operator.park(hold) => result?,
                    _ = cancel.cancelled() => return Ok(()),
                }
                if sibling {
                    cancel.cancelled().await;
                    return Ok(());
                }
                tokio::select! {
                    _ = finish.notified() => (),
                    _ = cancel.cancelled() => return Ok(()),
                }
            }
            events
                .emit(
                    json!({"kind":"submit","callId":"script","argumentsJson":"{}"}),
                    &cancel,
                )
                .await?;
            Ok(())
        })
    }
}

#[tokio::test]
async fn operator_pause_defers_delivery_and_routing_while_approval_expires() {
    check_operator_pause_approval(false).await;
}

#[tokio::test]
async fn operator_pause_settles_its_clock_ticket_before_a_parallel_approval_answer() {
    check_operator_pause_approval(true).await;
}

async fn check_operator_pause_approval(answer: bool) {
    use crate::api::{OperatorAction as Action, OperatorWorkState, RuntimeProjectionItem030};
    for stop in [false, true].into_iter().filter(|stop| !answer || !stop) {
        let document = json!({"contract":"agsdl-0.4.0","id":"operator-approval-deadline",
        "agents":{"seed":{"harness":"codex","permissions":"read-only"},"worker":{"harness":"codex","permissions":"read-only"}},
        "flow":{"entry":"seed","steps":{
            "seed":{"type":"agent","agent":"seed","next":["work","queued","approval"]},
            "work":{"type":"agent","agent":"worker","delivery":"queue"},
            "queued":{"type":"agent","agent":"worker","delivery":"queue"},
            "approval":{"type":"approval","timeoutMs":800,"next":{"yes":[],"no":[]},"onError":"expired"},
            "expired":{"type":"agent","agent":"seed"}
        }}});
        let f = Fixture::with_document(document).await;
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let clock = f
            .host
            .flow030()
            .human_runtime()
            .virtual_clock()
            .await
            .unwrap();
        let agent = Arc::new(PausableAgent::default());
        let registry = f.host.flow030().supervisor_registry();
        registry.reserve(&scope.system_start_id).unwrap();
        start_operator_supervisor(&f, agent.clone(), &tasks);
        let (approval_id, _) = operator_wait(async || {
            sqlx::query_as::<_, (String, String)>("SELECT request_id,deadline_json FROM engine_flow030_human_requests WHERE kind='approval' AND resolution_json IS NULL").fetch_optional(&f.pool).await.unwrap()
        }).await;
        let held_id = operator_view(&f, &scope)
            .await
            .items
            .into_iter()
            .find_map(|item| match item {
                RuntimeProjectionItem030::Activation { value }
                    if value.step == Some(vec!["work".into()]) =>
                {
                    Some(value.id)
                }
                _ => None,
            })
            .unwrap();
        if answer {
            // Queue the pause, then reacquire dispatch before the supervisor
            // can pump the clock ticket. The product entry point must settle it.
            let (dispatch, _) = registry.dispatcher(&scope.system_start_id).unwrap();
            let gate = dispatch.lock().await;
            let pause = operator_control(&f, &scope, "pause", Action::PauseRun);
            tokio::pin!(pause);
            assert!(futures::poll!(&mut pause).is_pending());
            let block = dispatch.lock();
            tokio::pin!(block);
            assert!(futures::poll!(&mut block).is_pending());
            drop(gate);
            pause.await.unwrap();
            let _block = block.await;
            let pending: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM engine_flow030_clock_events WHERE output_json IS NULL
                 AND json_extract(ticket_json ->> '$.eventJson', '$.event.kind')='operatorHold'",
            )
            .fetch_one(&f.pool)
            .await
            .unwrap();
            assert_eq!(pending, 1);
            clock.advance_to(799).unwrap();
            let evaluation_clock = clock.clone();
            f.host
                .start_lease()
                .await
                .unwrap()
                .clock_jobs030()
                .approval_check_probe(Arc::new(move |raw, _| {
                    let job: crate::agsdl_host_route030::HostRouteJob030 =
                        serde_json::from_str(raw).unwrap();
                    let request: Value = serde_json::from_str(&job.input_json).unwrap();
                    if request["operation"] != "humanClock" {
                        return;
                    }
                    let clock: crate::agsdl_host_clock030::HostClockInput030 =
                        serde_json::from_value(request["input"].clone()).unwrap();
                    let event: Value = serde_json::from_str(&clock.event_json).unwrap();
                    if event["event"]["kind"] == "operatorHold" {
                        evaluation_clock.advance_to(801).unwrap();
                    }
                }));
            let receipt = f
                .host
                .submit_person(
                    &f.pool,
                    crate::api::HumanSubmission {
                        scope: crate::persistence::public030::HumanScope030 {
                            system_start_id: scope.system_start_id.clone(),
                            source: scope.source.clone(),
                        },
                        request_id: approval_id.clone(),
                        expected_revision: "0".into(),
                        answer: crate::agsdl_host_human030::HumanAnswer030::Approval {
                            choice: "yes".into(),
                            comment: "Before the deadline".into(),
                        },
                    },
                )
                .await
                .unwrap();
            let accepted: bool = sqlx::query_scalar(
                "SELECT accepted FROM engine_flow030_human_submissions WHERE receipt_id=?",
            )
            .bind(receipt)
            .fetch_one(&f.pool)
            .await
            .unwrap();
            assert!(accepted);
            assert_eq!(
                crate::agsdl_host_clock030::Clock030::now_ms(clock.as_ref()),
                801
            );
        } else {
            operator_control(&f, &scope, "pause", Action::PauseRun)
                .await
                .unwrap();
        }
        operator_wait(async || {
            (operator_view(&f, &scope).await.operator_works.get(&held_id)
                == Some(&OperatorWorkState::Held))
            .then_some(())
        })
        .await;
        assert_eq!(
            operator_control(
                &f,
                &scope,
                "bad-resume",
                Action::ResumeWork {
                    activation_id: held_id
                }
            )
            .await
            .unwrap_err()
            .code,
            "OPERATOR_TRANSITION"
        );
        deliver_operator(&f, &scope, "delayed", "queue")
            .await
            .unwrap();

        let human_scope = crate::persistence::public030::HumanScope030 {
            system_start_id: scope.system_start_id.clone(),
            source: scope.source.clone(),
        };
        let humans = f.host.human_page(&f.pool, human_scope, None).await.unwrap();
        if !answer {
            let human = humans
                .requests
                .iter()
                .find(|r| r.id == approval_id)
                .unwrap();
            let facts: Value = serde_json::from_str(human.flow_json.as_deref().unwrap()).unwrap();
            assert_eq!(facts["clock"]["value"]["timeoutMs"], "800");
        }
        clock.advance_to(801).unwrap();
        registry.wake(&scope.system_start_id);
        operator_wait(async || {
            let page = operator_view(&f, &scope).await;
            page.items
                .iter()
                .any(|item| {
                    matches!(item, RuntimeProjectionItem030::Activation {value}
                if value.step == Some(vec!["approval".into()]) && value.phase == "deferred")
                })
                .then_some(())
        })
        .await;
        assert_eq!(agent.starts.lock().unwrap().as_slice(), ["seed", "work"]);
        let view = operator_view(&f, &scope).await;
        assert!(view.paused && view.verdict.is_none());
        assert!(view.items.iter().any(|item| matches!(item, RuntimeProjectionItem030::Activation {value}
            if value.phase == "deferred" && value.message.as_ref().is_some_and(|m| m.message_json.contains("delayed")))));
        let code: Option<String> = sqlx::query_scalar("SELECT resolution_json ->> '$.failure' FROM engine_flow030_human_requests WHERE request_id=?")
            .bind(approval_id).fetch_one(&f.pool).await.unwrap();
        assert_eq!(code.as_deref(), (!answer).then_some("APPROVAL_TIMEOUT"));

        if stop {
            f.host
                .request_stop(
                    &f.pool,
                    &tasks,
                    &EngineStopRequest {
                        system_start_id: scope.system_start_id.clone(),
                        request_id: "stop".into(),
                        activation_id: None,
                    },
                )
                .await
                .unwrap();
        } else {
            operator_control(&f, &scope, "resume-run", Action::ResumeRun)
                .await
                .unwrap();
            if !answer {
                operator_wait(async || {
                    agent
                        .starts
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|step| step == "expired")
                        .then_some(())
                })
                .await;
                assert_eq!(
                    agent.starts.lock().unwrap().as_slice(),
                    ["seed", "work", "expired"]
                );
            }
            agent.finish_work.notify_one();
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            registry.wait(&scope.system_start_id),
        )
        .await
        .unwrap()
        .unwrap();
        let page = operator_view(&f, &scope).await;
        assert_eq!(
            page.verdict.as_deref(),
            Some(if stop { "cancelled" } else { "succeeded" })
        );
        assert!(!page.paused && page.operator_works.is_empty());
        assert_eq!(
            agent.starts.lock().unwrap().as_slice(),
            if stop {
                &["seed", "work"][..]
            } else if answer {
                &["seed", "work", "queued", "message"][..]
            } else {
                &["seed", "work", "expired", "queued", "message"][..]
            }
        );
    }
}

#[tokio::test]
async fn operator_resume_routes_a_join_failure_with_the_original_result_evidence() {
    use crate::api::OperatorAction as Action;
    use crate::persistence::flow030::FlowEffectKind030;
    for approval in [false, true] {
        let work = if approval {
            json!({"type":"approval","timeoutMs":800,"next":{"yes":"join","no":"join"}})
        } else {
            json!({"type":"agent","agent":"worker","instance":"new","next":"join"})
        };
        let f = Fixture::with_document(json!({
            "contract":"agsdl-0.4.0","id":"deferred-branch-failure",
            "agents":{"worker":{"harness":"codex","permissions":"read-only"}},
            "flow":{"entry":"seed","steps":{
                "seed":{"type":"agent","agent":"worker","next":["work","sibling"]},
                "work":work,
                "sibling":{"type":"agent","agent":"worker","instance":"new","next":"join"},
                "join":{"type":"join","next":[],"onError":"handler"},
                "handler":{"type":"agent","agent":"worker"}
            }}
        }))
        .await;
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let registry = f.host.flow030().supervisor_registry();
        registry.reserve(&scope.system_start_id).unwrap();
        let agent = Arc::new(PausableAgent {
            disconnect_on_hold: true,
            ..Default::default()
        });
        let clock = f
            .host
            .flow030()
            .human_runtime()
            .virtual_clock()
            .await
            .unwrap();
        start_operator_supervisor(&f, agent.clone(), &tasks);
        let inspect = async || {
            crate::agsdl_host_flow030::inspect_run(f.host.flow030(), &f.host, &f.pool, &f.owner)
                .await
                .unwrap()
                .flow
        };
        let activation_id = if approval {
            operator_wait(async || sqlx::query_scalar::<_, String>(
                "SELECT work_ids_json ->> '$[0]' FROM engine_flow030_human_requests WHERE kind='approval'",
            ).fetch_optional(&f.pool).await.unwrap()).await
        } else {
            operator_wait(async || {
                inspect()
                    .await
                    .state
                    .activations
                    .into_iter()
                    .find(|a| {
                        a.step.as_deref() == Some(&["work".into()])
                            && a.phase == "running"
                            && a.physical
                    })
                    .map(|a| a.id)
            })
            .await
        };
        operator_wait(async || {
            agent
                .starts
                .lock()
                .unwrap()
                .iter()
                .any(|step| step == "sibling")
                .then_some(())
        })
        .await;
        operator_control(&f, &scope, "pause", Action::PauseRun)
            .await
            .unwrap();
        if approval {
            clock.advance_to(801).unwrap();
            registry.wake(&scope.system_start_id);
        }
        let origin = operator_wait(async || {
            let state = inspect().await.state;
            let deferred = state
                .activations
                .iter()
                .any(|a| a.id == activation_id && a.phase == "deferred");
            (deferred && state.operator_holds.values().all(|hold| hold.ready)).then(|| {
                state
                    .events
                    .into_iter()
                    .find_map(|(id, record)| {
                        let event: Value = serde_json::from_str(&record.event_json).unwrap();
                        (event.pointer("/kind/WorkResult/work/activationId")
                            == Some(&json!(activation_id))
                            && event.pointer("/kind/WorkResult/result/Failed").is_some())
                        .then_some(id)
                    })
                    .expect("original result event")
            })
        })
        .await;
        assert!(!agent.starts.lock().unwrap().contains(&"handler".into()));
        let resumed = operator_control(&f, &scope, "resume", Action::ResumeRun)
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            registry.wait(&scope.system_start_id),
        )
        .await
        .unwrap()
        .unwrap();
        let done = inspect().await;
        assert_eq!(done.state.verdict.as_deref(), Some("succeeded"));
        assert!(agent.starts.lock().unwrap().contains(&"handler".into()));
        assert!(done.effects.iter().any(|row| {
            let effect: crate::persistence::flow030::FlowEffect030 = serde_json::from_value(row["effect"].clone()).unwrap();
            resumed.effect_ids.contains(&effect.id) && matches!(effect.kind,
                FlowEffectKind030::Failure {activation_id: id, origin_event_id: Some(event), counted: true, ..}
                    if id == activation_id && event == origin)
        }));
        let replay = crate::persistence::replay_verify030::verify_path030(
            &f._temp.path().join("run.db"),
            &scope.system_start_id,
            &f.identity.flow_invocation_id,
        )
        .await;
        assert!(replay.ok, "{:?}", replay.divergence);
    }
}

#[tokio::test]
async fn operator_resume_routes_join_failure_from_a_steer_settled_during_pause() {
    use crate::api::OperatorAction as Action;
    use crate::persistence::flow030::FlowEffectKind030;

    let f = Fixture::with_document(json!({
        "contract":"agsdl-0.4.0","id":"deferred-steer-failure",
        "agents":{"worker":{"harness":"codex","permissions":"read-only"}},
        "flow":{"entry":"seed","steps":{
            "seed":{"type":"agent","agent":"worker","next":["work","nudge","sibling"]},
            "work":{"type":"agent","agent":"worker"},
            "nudge":{"type":"agent","agent":"worker","delivery":"steer","next":"join"},
            "sibling":{"type":"agent","agent":"worker","instance":"new","next":"join"},
            "join":{"type":"join","next":[],"onError":"handler"},
            "handler":{"type":"agent","agent":"worker"}
        }}
    }))
    .await;
    let scope = operator_scope(&f).await;
    let tasks = StreamTasks::default();
    let registry = f.host.flow030().supervisor_registry();
    registry.reserve(&scope.system_start_id).unwrap();
    let steer = Arc::new(DeferredSteer::default());
    let agent = Arc::new(PausableAgent {
        deferred_steer: Some(steer.clone()),
        ..Default::default()
    });
    start_operator_supervisor(&f, agent.clone(), &tasks);
    tokio::time::timeout(std::time::Duration::from_secs(10), steer.sent.notified())
        .await
        .unwrap();
    operator_wait(async || {
        let view = flow::inspect_run(f.host.flow030(), &f.host, &f.pool, &f.owner)
            .await
            .unwrap();
        view.flow
            .state
            .activations
            .iter()
            .any(|activation| {
                activation.step.as_deref() == Some(&["sibling".into()])
                    && matches!(
                        activation.result,
                        Some(crate::persistence::flow030::WorkResultRecord030::Completed { .. })
                    )
            })
            .then_some(())
    })
    .await;
    let (dispatch, _) = registry.dispatcher(&scope.system_start_id).unwrap();
    let gate = dispatch.lock().await;
    let pause = operator_control(&f, &scope, "pause", Action::PauseRun);
    tokio::pin!(pause);
    assert!(futures::poll!(&mut pause).is_pending());
    let block = dispatch.lock();
    tokio::pin!(block);
    assert!(futures::poll!(&mut block).is_pending());
    drop(gate);
    let paused = pause.await.unwrap();
    let block = block.await;
    let runtime = f.host.flow030();
    let view = flow::inspect_run(runtime, &f.host, &f.pool, &f.owner)
        .await
        .unwrap();
    let pause_event: Value =
        serde_json::from_str(&view.flow.state.events[&paused.event_id].event_json).unwrap();
    runtime
        .begin_event_commit(&crate::persistence::flow030::FlowEvent030 {
            identity: f.identity.clone(),
            event_id: paused.event_id.clone(),
            expected_revision: pause_event["expectedRevision"].as_str().unwrap().into(),
            kind: crate::persistence::flow030::FlowEventKind030::Operator(
                serde_json::from_value(pause_event["kind"]["Operator"].clone()).unwrap(),
            ),
        })
        .unwrap();
    steer.report.notify_one();
    // The native steer settles independently of the dispatcher. It must
    // reconcile the durable Pause before committing its own evidence.
    operator_wait(async || {
        let view = flow::inspect_run(runtime, &f.host, &f.pool, &f.owner)
            .await
            .unwrap();
        (view.flow.state.steer_deliveries[0].status == "failed").then_some(())
    })
    .await;
    runtime.available(&f.identity).unwrap();
    drop(block);
    let (activation_id, origin_event_id) = operator_wait(async || {
        let view = flow::inspect_run(f.host.flow030(), &f.host, &f.pool, &f.owner)
            .await
            .unwrap();
        let state = &view.flow.state;
        let delivery = state.steer_deliveries.first()?;
        (delivery.status == "failed" && state.operator_holds.values().all(|hold| hold.ready)).then(
            || {
                assert_eq!(delivery.reason.as_deref(), Some("HARNESS_REFUSED"));
                (
                    delivery.delivery_activation_id.clone(),
                    delivery.resolved_event_id.clone().unwrap(),
                )
            },
        )
    })
    .await;
    assert!(!agent
        .starts
        .lock()
        .unwrap()
        .iter()
        .any(|step| step == "handler"));
    let resume = operator_control(&f, &scope, "resume", Action::ResumeRun)
        .await
        .unwrap();
    agent.finish_work.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        registry.wait(&scope.system_start_id),
    )
    .await
    .unwrap()
    .unwrap();
    let done = flow::inspect_run(f.host.flow030(), &f.host, &f.pool, &f.owner)
        .await
        .unwrap()
        .flow;
    assert_eq!(done.state.verdict.as_deref(), Some("succeeded"));
    assert!(agent
        .starts
        .lock()
        .unwrap()
        .iter()
        .any(|step| step == "handler"));
    assert!(done.effects.iter().any(|row| {
        let effect: crate::persistence::flow030::FlowEffect030 = serde_json::from_value(row["effect"].clone()).unwrap();
        resume.effect_ids.contains(&effect.id) && matches!(effect.kind,
            FlowEffectKind030::Failure {activation_id: id, origin_event_id: Some(origin), counted: true, ..}
                if id == activation_id && origin == origin_event_id)
    }));
    let replay = crate::persistence::replay_verify030::verify_path030(
        &f._temp.path().join("run.db"),
        &scope.system_start_id,
        &f.identity.flow_invocation_id,
    )
    .await;
    assert!(replay.ok, "{:?}", replay.divergence);
}

#[tokio::test]
async fn operator_stop_supersedes_the_exact_pending_pause_clock_ticket() {
    use crate::api::OperatorAction as Action;
    let f = Fixture::with_document(json!({
        "contract":"agsdl-0.4.0","id":"stop-before-pause-clock",
        "agents":{"worker":{"harness":"codex","permissions":"read-only"}},
        "flow":{"entry":"work","steps":{"work":{"type":"agent","agent":"worker","timeoutMs":1000}}}
    }))
    .await;
    let scope = operator_scope(&f).await;
    let tasks = StreamTasks::default();
    let registry = f.host.flow030().supervisor_registry();
    registry.reserve(&scope.system_start_id).unwrap();
    let agent = Arc::new(PausableAgent::default());
    start_operator_supervisor(&f, agent.clone(), &tasks);
    operator_wait(async || (!agent.starts.lock().unwrap().is_empty()).then_some(())).await;

    // After Pause commits, another waiter holds dispatch before the supervisor
    // can evaluate the reserved ticket. Stop is independent of dispatch.
    let (dispatch, _) = registry.dispatcher(&scope.system_start_id).unwrap();
    let gate = dispatch.lock().await;
    let pause = operator_control(&f, &scope, "pause", Action::PauseRun);
    tokio::pin!(pause);
    assert!(futures::poll!(&mut pause).is_pending());
    let block = dispatch.lock();
    tokio::pin!(block);
    assert!(futures::poll!(&mut block).is_pending());
    drop(gate);
    pause.await.unwrap();
    let block = block.await;
    let (ticket, raw): (String, String) = sqlx::query_as(
        "SELECT event_id,ticket_json FROM engine_flow030_clock_events WHERE output_json IS NULL
         AND json_extract(ticket_json ->> '$.eventJson', '$.event.kind')='operatorHold'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    f.host
        .request_stop(
            &f.pool,
            &tasks,
            &EngineStopRequest {
                system_start_id: scope.system_start_id.clone(),
                request_id: "stop".into(),
                activation_id: None,
            },
        )
        .await
        .unwrap();
    drop(block);
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        registry.wait(&scope.system_start_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        operator_view(&f, &scope).await.verdict.as_deref(),
        Some("cancelled")
    );
    let stop_receipt: String = sqlx::query_scalar(
        "SELECT receipt_json FROM engine_flow030_system_stop_requests WHERE request_id='stop'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    let stop_receipt: Value = serde_json::from_str(&stop_receipt).unwrap();
    assert_eq!(
        stop_receipt["supersededClockTicket"],
        json!(crate::agsdl_host_flow030::SupersededClockTicket030::new(
            &ticket, &raw
        ))
    );
    let verify = async || {
        crate::persistence::replay_verify030::verify_path030(
            &f._temp.path().join("run.db"),
            &scope.system_start_id,
            &f.identity.flow_invocation_id,
        )
        .await
    };
    let replay = verify().await;
    assert!(replay.ok, "{:?}", replay.divergence);

    sqlx::query("DROP TRIGGER engine_flow030_clock_ticket_immutable")
        .execute(&f.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE engine_flow030_clock_events SET ticket_json=? WHERE event_id=?")
        .bind(format!("{raw} "))
        .bind(ticket)
        .execute(&f.pool)
        .await
        .unwrap();
    let replay = verify().await;
    assert!(!replay.ok, "Replay accepted a different clock ticket");
}

#[tokio::test]
async fn operator_stop_preserves_an_accepted_approval_that_has_not_routed() {
    let f = Fixture::with_document(json!({
        "contract":"agsdl-0.4.0","id":"stop-before-approval-result",
        "agents":{"worker":{"harness":"codex","permissions":"read-only"}},
        "flow":{"entry":"approval","steps":{
            "approval":{"type":"approval","next":{"yes":"handler","no":"handler"}},
            "handler":{"type":"agent","agent":"worker"}
        }}
    }))
    .await;
    let scope = operator_scope(&f).await;
    let tasks = StreamTasks::default();
    let registry = f.host.flow030().supervisor_registry();
    registry.reserve(&scope.system_start_id).unwrap();
    let agent = Arc::new(PausableAgent::default());
    start_operator_supervisor(&f, agent.clone(), &tasks);
    let id = operator_wait(async || {
        sqlx::query_scalar::<_, String>(
            "SELECT request_id FROM engine_flow030_human_requests WHERE kind='approval'",
        )
        .fetch_optional(&f.pool)
        .await
        .unwrap()
    })
    .await;
    let (dispatch, _) = registry.dispatcher(&scope.system_start_id).unwrap();
    let block = dispatch.lock().await;
    let receipt = f
        .host
        .submit_person(
            &f.pool,
            crate::api::HumanSubmission {
                scope: crate::persistence::public030::HumanScope030 {
                    system_start_id: scope.system_start_id.clone(),
                    source: scope.source.clone(),
                },
                request_id: id,
                expected_revision: "0".into(),
                answer: crate::agsdl_host_human030::HumanAnswer030::Approval {
                    choice: "yes".into(),
                    comment: "Reviewed".into(),
                },
            },
        )
        .await
        .unwrap();
    let accepted: bool = sqlx::query_scalar(
        "SELECT accepted FROM engine_flow030_human_submissions WHERE receipt_id=?",
    )
    .bind(receipt)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert!(accepted);
    f.host
        .request_stop(
            &f.pool,
            &tasks,
            &EngineStopRequest {
                system_start_id: scope.system_start_id.clone(),
                request_id: "stop".into(),
                activation_id: None,
            },
        )
        .await
        .unwrap();
    drop(block);
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        registry.wait(&scope.system_start_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        operator_view(&f, &scope).await.verdict.as_deref(),
        Some("cancelled")
    );
    assert!(agent.starts.lock().unwrap().is_empty());
    let replay = crate::persistence::replay_verify030::verify_path030(
        &f._temp.path().join("run.db"),
        &scope.system_start_id,
        &f.identity.flow_invocation_id,
    )
    .await;
    assert!(replay.ok, "{:?}", replay.divergence);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operator_stop_during_clock_retirement_preserves_the_prior_result() {
    let f = Fixture::with_document(json!({
        "contract":"agsdl-0.4.0","id":"stop-during-clock-retirement",
        "agents":{"worker":{"harness":"codex","permissions":"read-only"}},
        "flow":{"entry":"work","steps":{
            "work":{"type":"agent","agent":"worker","timeoutMs":10000,"next":"handler"},
            "handler":{"type":"agent","agent":"worker"}
        }}
    }))
    .await;
    let scope = operator_scope(&f).await;
    let tasks = StreamTasks::default();
    let registry = f.host.flow030().supervisor_registry();
    registry.reserve(&scope.system_start_id).unwrap();
    let trace = f._temp.path().join("trace.jsonl");
    std::fs::write(
        trace.with_extension("jsonl.script"),
        json!([envelope(json!({"results":{}}))]).to_string(),
    )
    .unwrap();

    let (retiring, wait_for_retirement) = tokio::sync::oneshot::channel();
    let retiring = std::sync::Mutex::new(Some(retiring));
    let (release, stop_committed) = std::sync::mpsc::sync_channel(1);
    let stop_committed = std::sync::Mutex::new(stop_committed);
    f.host
        .start_lease()
        .await
        .unwrap()
        .clock_jobs030()
        .approval_check_probe(Arc::new(move |raw, _| {
            let job: crate::agsdl_host_route030::HostRouteJob030 =
                serde_json::from_str(raw).unwrap();
            let request: Value = serde_json::from_str(&job.input_json).unwrap();
            if request["operation"] == "humanClock" {
                let clock: crate::agsdl_host_clock030::HostClockInput030 =
                    serde_json::from_value(request["input"].clone()).unwrap();
                let event: Value = serde_json::from_str(&clock.event_json).unwrap();
                if event["event"]["kind"] == "retire" {
                    if let Some(signal) = retiring.lock().unwrap().take() {
                        signal.send(()).unwrap();
                        stop_committed
                            .lock()
                            .unwrap()
                            .recv_timeout(std::time::Duration::from_secs(10))
                            .unwrap();
                    }
                }
            }
        }));
    start_operator_supervisor(&f, Arc::new(executor(&f, &trace, None)), &tasks);
    tokio::time::timeout(std::time::Duration::from_secs(10), wait_for_retirement)
        .await
        .unwrap()
        .unwrap();
    f.host
        .request_stop(
            &f.pool,
            &tasks,
            &EngineStopRequest {
                system_start_id: scope.system_start_id.clone(),
                request_id: "stop".into(),
                activation_id: None,
            },
        )
        .await
        .unwrap();
    release.send(()).unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        registry.wait(&scope.system_start_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        operator_view(&f, &scope).await.verdict.as_deref(),
        Some("cancelled")
    );
    assert_eq!(requests(&trace, "turn/start").len(), 1);
    let replay = crate::persistence::replay_verify030::verify_path030(
        &f._temp.path().join("run.db"),
        &scope.system_start_id,
        &f.identity.flow_invocation_id,
    )
    .await;
    assert!(replay.ok, "{:?}", replay.divergence);
}

#[tokio::test]
async fn supervisor_cancels_an_unadmitted_approval_when_stop_wins_before_dispatch() {
    let f = Fixture::with_document(json!({
        "contract":"agsdl-0.4.0","id":"stop-before-approval-admission",
        "agents":{},
        "flow":{"entry":"approval","steps":{
            "approval":{"type":"approval","next":{"yes":[],"no":[]}}
        }}
    }))
    .await;
    let scope = operator_scope(&f).await;
    let tasks = StreamTasks::default();
    let runtime = f.host.flow030();
    let registry = runtime.supervisor_registry();
    registry.reserve(&scope.system_start_id).unwrap();
    let stopped = flow::request_system_stop030(
        runtime,
        &f.pool,
        &EngineStopRequest {
            system_start_id: scope.system_start_id.clone(),
            request_id: "stop".into(),
            activation_id: None,
        },
    )
    .await
    .unwrap();
    // Dispatch sees the durable Stop before the caller's cleanup can run.
    assert!(
        stopped
            .control()
            .read(&f.pool)
            .await
            .unwrap()
            .state
            .activations[0]
            .physical
    );

    let clock = crate::agsdl_host_human_executor030::HumanClockRuntime030 {
        app: f.host.as_ref().clone(),
        pool: f.pool.clone(),
        tasks: tasks.clone(),
        identity: f.identity.clone(),
    };
    assert!(
        crate::agsdl_host_runtime030::RuntimeClock030::next_wake(&clock)
            .await
            .unwrap()
            .is_none()
    );

    start_operator_supervisor(&f, Arc::new(PausableAgent::default()), &tasks);
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        registry.wait(&scope.system_start_id),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        operator_view(&f, &scope).await.verdict.as_deref(),
        Some("cancelled")
    );
    let replay = crate::persistence::replay_verify030::verify_path030(
        &f._temp.path().join("run.db"),
        &scope.system_start_id,
        &f.identity.flow_invocation_id,
    )
    .await;
    assert!(replay.ok, "{:?}", replay.divergence);
}
