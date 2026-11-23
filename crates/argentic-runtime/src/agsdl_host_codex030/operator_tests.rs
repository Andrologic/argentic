//! Operator transitions through the native supervisor and scripted Codex peer.
use super::*;
use crate::persistence::dto::EngineStopRequest;

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

fn start_operator_supervisor(f: &Fixture, agents: Arc<dyn AgentExecutor030>, tasks: &StreamTasks) {
    let (pool, app, identity) = (f.pool.clone(), f.host.as_ref().clone(), f.identity.clone());
    let executors = Executors030 {
        agents,
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

#[tokio::test]
async fn operator_interrupt_closes_a_codex_tool_approval_without_authorizing_it() {
    use crate::api::{OperatorAction as Action, OperatorWorkState};
    let document = json!({"contract":"agsdl-0.4.0","id":"operator-approval","agents":{"worker":{"harness":"codex","permissions":{"preset":"workspace","reviewer":"person","rules":[{"action":"write","effect":"ask"}]}}},"flow":{"entry":"work","steps":{"work":{"type":"agent","agent":"worker","timeoutMs":1000}}}});
    let f = Fixture::with_document(document).await;
    approve_fixture_workspace(&f).await;
    let scope = operator_scope(&f).await;
    let root = f._temp.path().canonicalize().unwrap().join("workspace");
    let path = root.join("sentinel");
    std::fs::write(&path, "before").unwrap();
    let trace = f._temp.path().join("trace.jsonl");
    let mut result = envelope(json!({"results":{}}));
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
    let agent = executor(&f, &trace, None).with_approvals(Arc::new(
        crate::agsdl_host_action_approval030::AgentActionHumans030 {
            host: f.host.as_ref().clone(),
            pool: f.pool.clone(),
            policy: Arc::new(UnusedActionPolicy),
        },
    ));
    start_operator_supervisor(&f, Arc::new(agent), &tasks);
    let (id, activation_id) = operator_wait(async || {
        sqlx::query_as::<_, (String, String)>("SELECT request_id, work_ids_json ->> '$[0]' FROM engine_flow030_human_requests WHERE kind='ask'")
            .fetch_optional(&f.pool).await.unwrap()
    }).await;
    operator_control(
        &f,
        &scope,
        "hold",
        Action::InterruptWork {
            activation_id: activation_id.clone(),
        },
    )
    .await
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
    let resolution: String = sqlx::query_scalar(
        "SELECT resolution_json FROM engine_flow030_human_requests WHERE request_id=?",
    )
    .bind(id)
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&resolution).unwrap()["operatorCancelled"],
        true
    );
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
struct PausableAgent {
    starts: std::sync::Mutex<Vec<String>>,
    finish_work: Arc<Notify>,
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
        Box::pin(async move {
            if step == "work" {
                let hold = tokio::select! {
                    id = work.operator.requested() => id,
                    _ = cancel.cancelled() => return Ok(()),
                };
                tokio::select! {
                    result = work.operator.park(hold) => result?,
                    _ = cancel.cancelled() => return Ok(()),
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
    use crate::api::{OperatorAction as Action, OperatorWorkState, RuntimeProjectionItem030};
    for stop in [false, true] {
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
        operator_control(&f, &scope, "pause", Action::PauseRun)
            .await
            .unwrap();
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
        let human = humans
            .requests
            .iter()
            .find(|r| r.id == approval_id)
            .unwrap();
        let facts: Value = serde_json::from_str(human.flow_json.as_deref().unwrap()).unwrap();
        assert_eq!(facts["clock"]["value"]["timeoutMs"], "800");
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
        let code: String = sqlx::query_scalar("SELECT resolution_json ->> '$.failure' FROM engine_flow030_human_requests WHERE request_id=?")
            .bind(approval_id).fetch_one(&f.pool).await.unwrap();
        assert_eq!(code, "APPROVAL_TIMEOUT");

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
            } else {
                &["seed", "work", "expired", "queued", "message"][..]
            }
        );
    }
}
