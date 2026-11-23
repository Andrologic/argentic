use super::*;

#[derive(Clone, Copy, Debug)]
enum Command {
    Interrupt,
    ResumeWork,
    Pause,
    ResumeRun,
}

impl Command {
    fn action(self, id: &str) -> Action {
        match self {
            Self::Interrupt => Action::InterruptWork {
                activation_id: id.into(),
            },
            Self::ResumeWork => Action::ResumeWork {
                activation_id: id.into(),
            },
            Self::Pause => Action::PauseRun,
            Self::ResumeRun => Action::ResumeRun,
        }
    }

    fn prerequisite(self) -> Option<Self> {
        match self {
            Self::ResumeWork => Some(Self::Interrupt),
            Self::ResumeRun => Some(Self::Pause),
            _ => None,
        }
    }
}

const COMMANDS: [Command; 4] = [
    Command::Interrupt,
    Command::ResumeWork,
    Command::Pause,
    Command::ResumeRun,
];

async fn wait_held(f: &Fixture, id: &str) {
    operator_wait(async || {
        inspect(f)
            .await
            .state
            .operator_holds
            .get(id)
            .is_some_and(|hold| hold.ready)
            .then_some(())
    })
    .await;
}

async fn freeze_pass(f: &Fixture) -> Arc<CommitTestHook030> {
    let pass = arm(f, "supervisor.pass");
    f.host
        .flow030()
        .supervisors
        .wake(&f.identity.system_start_id);
    reached(&pass, "supervisor.pass").await;
    pass
}

async fn timed_work(
    command: Command,
    tasks: &StreamTasks,
) -> (
    Arc<Fixture>,
    crate::api::RuntimeProjectionScope030,
    Arc<PausableAgent>,
    String,
) {
    let f = Arc::new(
        Fixture::with_document(document(
            json!({"work":{"type":"agent","agent":"worker","timeoutMs":10000}}),
            "work",
        ))
        .await,
    );
    let scope = operator_scope(&f).await;
    f.host
        .flow030()
        .human_runtime()
        .virtual_clock()
        .await
        .unwrap();
    let agent = Arc::new(PausableAgent::default());
    start(&f, tasks, agent.clone());
    let id = operator_wait(async || {
        sqlx::query_scalar::<_, String>(
            "SELECT activation_id FROM engine_flow030_clock_works WHERE retired=0",
        )
        .fetch_optional(&f.pool)
        .await
        .unwrap()
    })
    .await;
    if let Some(prerequisite) = command.prerequisite() {
        operator_control(&f, &scope, "prerequisite", prerequisite.action(&id))
            .await
            .unwrap();
        wait_held(&f, &id).await;
    }
    (f, scope, agent, id)
}

async fn cancel_command(
    f: &Arc<Fixture>,
    scope: &crate::api::RuntimeProjectionScope030,
    request_id: &'static str,
    action: Action,
    boundary: &'static str,
) {
    let gap = arm(f, boundary);
    let mut request = tokio::spawn({
        let (f, scope) = (f.clone(), scope.clone());
        async move { operator_control(&f, &scope, request_id, action).await }
    });
    tokio::select! {
        result = &mut request => panic!("Command returned before {boundary}: {result:?}"),
        _ = reached(&gap, boundary) => {}
    }
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
}

async fn last_operator_receipt(f: &Fixture) -> crate::persistence::flow030::FlowReceipt030 {
    let raw: String = sqlx::query_scalar(
        "SELECT receipt_json FROM engine_flow030_events WHERE event_id LIKE 'operator:%' ORDER BY rowid DESC LIMIT 1",
    ).fetch_one(&f.pool).await.unwrap();
    serde_json::from_str(&raw).unwrap()
}

async fn finished(f: &Fixture, verdict: &str) {
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
    let view = inspect(f).await;
    assert_eq!(view.state.verdict.as_deref(), Some(verdict));
    assert!(view.state.activations.iter().all(|a| !a.physical));
    journal(f).await;
}

async fn interrupt_reopened(f: &Fixture, scope: &crate::api::RuntimeProjectionScope030) {
    let reopened = crate::sqlite::open_file(&f._temp.path().join("run.db"))
        .await
        .unwrap();
    crate::api::interrupt_previous_runs(&reopened)
        .await
        .unwrap();
    assert_eq!(
        operator_view(f, scope).await.verdict.as_deref(),
        Some("interrupted")
    );
    journal(f).await;
}

#[tokio::test]
async fn operator_schedule_lost_control_ack_before_supervisor_pass_and_retry() {
    for command in COMMANDS {
        let tasks = StreamTasks::default();
        let (f, scope, agent, id) = timed_work(command, &tasks).await;
        let pass = freeze_pass(&f).await;
        cancel_command(&f, &scope, "scheduled", command.action(&id), "operator.ack").await;
        let receipt = last_operator_receipt(&f).await;
        let pending = || {
            f.host
                .flow030()
                .pending_event_commit(&f.identity, &receipt.event_id)
                .unwrap()
                .is_some()
        };
        assert!(pending(), "{command:?} lost its marker before the pass");

        // Let the blocked pass run once before the caller retries.
        let next = arm(&f, "supervisor.pass");
        f.host.flow030().supervisors.wake(&scope.system_start_id);
        pass.release();
        reached(&next, "supervisor.pass").await;
        assert!(!pending(), "{command:?} was not reconciled in one pass");
        assert_eq!(
            operator_control(&f, &scope, "scheduled", command.action(&id))
                .await
                .unwrap(),
            receipt,
            "{command:?}"
        );
        next.release();
        if let Some(resume) = match command {
            Command::Interrupt => Some(Command::ResumeWork),
            Command::Pause => Some(Command::ResumeRun),
            _ => None,
        } {
            wait_held(&f, &id).await;
            operator_control(&f, &scope, "release", resume.action(&id))
                .await
                .unwrap();
        }
        agent.finish_work.notify_one();
        finished(&f, "succeeded").await;
    }
}

#[tokio::test]
async fn operator_schedule_stop_after_lost_control_ack_without_retry() {
    for command in COMMANDS {
        let tasks = StreamTasks::default();
        let (f, scope, _, id) = timed_work(command, &tasks).await;
        let pass = freeze_pass(&f).await;
        cancel_command(&f, &scope, "scheduled", command.action(&id), "operator.ack").await;
        stop(&f).await;
        pass.release();
        stopped(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_restart_with_timed_work_at_control_boundaries() {
    let boundaries: &[_] = if exhaustive() {
        &[
            "operator.admission",
            "operator.commit",
            "operator.ack",
            "operator.return",
        ]
    } else {
        &["operator.ack"]
    };
    for command in COMMANDS {
        for &boundary in boundaries {
            let tasks = StreamTasks::default();
            let (f, scope, _, id) = timed_work(command, &tasks).await;
            let _pass = freeze_pass(&f).await;
            cancel_command(&f, &scope, "scheduled", command.action(&id), boundary).await;
            let pending: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM engine_flow030_clock_events WHERE output_json IS NULL
                 AND json_extract(ticket_json ->> '$.eventJson', '$.event.kind')='operatorHold'",
            )
            .fetch_one(&f.pool)
            .await
            .unwrap();
            assert_eq!(
                pending,
                i64::from(matches!(boundary, "operator.ack" | "operator.return")),
                "{command:?} at {boundary}"
            );
            let effect = inspect(&f).await.state.activations[0]
                .start_effect_id
                .clone()
                .unwrap();
            f.host
                .flow030()
                .supervisors
                .crash_operator_fixture(&scope.system_start_id)
                .await;
            tasks
                .cancel_owned(&format!("flow030:{}:{effect}", scope.system_start_id))
                .await
                .unwrap();
            interrupt_reopened(&f, &scope).await;
        }
    }
}

#[tokio::test]
async fn operator_schedule_stop_during_timed_agent_tick() {
    let boundaries: &[_] = if exhaustive() {
        &["clock.reserved", "clock.evaluated", "clock.commit"]
    } else {
        &["clock.evaluated"]
    };
    for &boundary in boundaries {
        let f = Arc::new(
            Fixture::with_document(document(
                json!({"work":{"type":"agent","agent":"worker","timeoutMs":10000}}),
                "work",
            ))
            .await,
        );
        let runtime = f.host.flow030();
        let clock = runtime.human_runtime().virtual_clock().await.unwrap();
        let tasks = StreamTasks::default();
        let agent = Arc::new(ScheduledAgent::default());
        start(&f, &tasks, agent.clone());
        tokio::time::timeout(WAIT, agent.running.notified())
            .await
            .unwrap();
        let pass = freeze_pass(&f).await;
        let tick = arm(&f, boundary);
        clock.advance_to(10001).unwrap();
        pass.release();
        reached(&tick, boundary).await;
        stop_at(&f, tick, boundary == "clock.commit").await;
        stopped(&f).await;
    }
}

async fn stop_at(f: &Arc<Fixture>, gap: Arc<CommitTestHook030>, holds_admission: bool) {
    if holds_admission {
        // Poll Stop while the other actor owns admission, then release it.
        let stopping = stop(f);
        tokio::pin!(stopping);
        assert!(futures::poll!(&mut stopping).is_pending());
        gap.release();
        stopping.await;
    } else {
        stop(f).await;
        gap.release();
    }
}

#[tokio::test]
async fn operator_schedule_stop_during_joined_finalization() {
    let boundaries: &[_] = if exhaustive() {
        &[
            "supervisor.joined",
            "work.join.reserved",
            "work.join.evaluated",
            "work.reserve.commit",
            "work.ack",
        ]
    } else {
        &["supervisor.joined", "work.ack"]
    };
    for &boundary in boundaries {
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
        let tasks = StreamTasks::default();
        let agent = Arc::new(ScheduledAgent::default());
        start(&f, &tasks, agent.clone());
        tokio::time::timeout(WAIT, agent.running.notified())
            .await
            .unwrap();
        let joined = arm(&f, "supervisor.joined");
        agent.fail.notify_one();
        reached(&joined, "supervisor.joined").await;
        let gap = if boundary == "supervisor.joined" {
            joined
        } else {
            let gap = arm(&f, boundary);
            joined.release();
            reached(&gap, boundary).await;
            gap
        };
        stop_at(
            &f,
            gap,
            matches!(boundary, "work.reserve.commit" | "work.ack"),
        )
        .await;
        stopped(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_stop_and_restart_at_agent_start() {
    let boundaries: &[_] = if exhaustive() {
        &[
            "effect.admission",
            "effect.claim.commit",
            "effect.claim.ack",
            "effect.handoff.commit",
            "effect.handoff.ack",
        ]
    } else {
        &["effect.claim.ack"]
    };
    for &boundary in boundaries {
        for restart in [false, true] {
            let f = Arc::new(
                Fixture::with_document(document(
                    json!({"work":{"type":"agent","agent":"worker","timeoutMs":10000}}),
                    "work",
                ))
                .await,
            );
            let scope = operator_scope(&f).await;
            let tasks = StreamTasks::default();
            let gap = arm(&f, boundary);
            start(&f, &tasks, Arc::new(ScheduledAgent::default()));
            reached(&gap, boundary).await;
            if restart {
                f.host
                    .flow030()
                    .supervisors
                    .crash_operator_fixture(&f.identity.system_start_id)
                    .await;
                interrupt_reopened(&f, &scope).await;
            } else {
                stop_at(&f, gap, boundary != "effect.admission").await;
                stopped(&f).await;
            }
        }
    }
}

#[tokio::test]
async fn operator_schedule_steer_receipt_with_stop_restart_or_lost_hold_ack() {
    let endings: &[_] = if exhaustive() {
        &["stop", "restart", "pause", "interrupt"]
    } else {
        &["pause", "stop"]
    };
    for &ending in endings {
        let f = Arc::new(
            Fixture::with_document(document(
                json!({
                    "seed":{"type":"agent","agent":"worker","next":["work","steer"]},
                    "work":{"type":"agent","agent":"worker"},
                    "steer":{"type":"agent","agent":"worker","delivery":"steer"}
                }),
                "seed",
            ))
            .await,
        );
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let steer = Arc::new(DeferredSteer::default());
        let agent = Arc::new(PausableAgent {
            deferred_steer: Some(steer.clone()),
            confirm_deferred_steer: true,
            ..Default::default()
        });
        let gap = arm(&f, "steer.receipt");
        start(&f, &tasks, agent.clone());
        tokio::time::timeout(WAIT, steer.sent.notified())
            .await
            .unwrap();
        let work = inspect(&f)
            .await
            .state
            .activations
            .into_iter()
            .find(|a| a.step.as_deref() == Some(&["work".into()]))
            .unwrap();
        steer.report.notify_one();
        reached(&gap, "steer.receipt").await;
        match ending {
            "stop" => {
                stop(&f).await;
                gap.release();
                stopped(&f).await;
                assert!(inspect(&f)
                    .await
                    .state
                    .steer_deliveries
                    .iter()
                    .all(|d| d.status != "pending"));
            }
            "restart" => {
                f.host
                    .flow030()
                    .supervisors
                    .crash_operator_fixture(&scope.system_start_id)
                    .await;
                tasks
                    .cancel_owned(&format!(
                        "flow030:{}:{}",
                        scope.system_start_id,
                        work.start_effect_id.unwrap()
                    ))
                    .await
                    .unwrap();
                interrupt_reopened(&f, &scope).await;
                gap.release();
            }
            _ => {
                let (hold, resume) = if ending == "pause" {
                    (Command::Pause, Command::ResumeRun)
                } else {
                    (Command::Interrupt, Command::ResumeWork)
                };
                cancel_command(
                    &f,
                    &scope,
                    "scheduled",
                    hold.action(&work.id),
                    "operator.ack",
                )
                .await;
                gap.release();
                wait_for_consumption(&f).await;
                wait_held(&f, &work.id).await;
                operator_control(&f, &scope, "release", resume.action(&work.id))
                    .await
                    .unwrap();
                agent.finish_work.notify_one();
                finished(&f, "succeeded").await;
            }
        }
    }
}

#[tokio::test]
async fn operator_schedule_lost_delivery_ack_with_held_recipient() {
    for mode in ["steer", "queue"] {
        let tasks = StreamTasks::default();
        let (f, scope, agent, id) = timed_work(Command::ResumeRun, &tasks).await;
        let pass = freeze_pass(&f).await;
        let gap = arm(&f, "delivery.ack");
        let delivery = tokio::spawn({
            let (f, scope) = (f.clone(), scope.clone());
            async move { deliver_operator(&f, &scope, "held-message", mode).await }
        });
        reached(&gap, "delivery.ack").await;
        let durable: String = sqlx::query_scalar(
            "SELECT receipt_json FROM engine_flow030_events WHERE event_id LIKE 'deliver030:%'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        let durable: crate::persistence::flow030::FlowReceipt030 =
            serde_json::from_str(&durable).unwrap();
        delivery.abort();
        assert!(delivery.await.unwrap_err().is_cancelled());
        pass.release();
        f.host.flow030().supervisors.wake(&scope.system_start_id);
        operator_wait(async || f.host.flow030().available(&f.identity).ok()).await;
        let retry = deliver_operator(&f, &scope, "held-message", mode)
            .await
            .unwrap();
        assert_eq!(
            (retry.event_id, retry.revision),
            (durable.event_id, durable.revision)
        );
        assert!(inspect(&f).await.state.operator_holds[&id].ready);
        operator_control(&f, &scope, "resume", Action::ResumeRun)
            .await
            .unwrap();
        agent.finish_work.notify_one();
        finished(&f, "succeeded").await;
        let view = inspect(&f).await;
        assert_eq!(view.state.activations.len(), 2);
    }
}

#[derive(Default)]
struct GatedAgent {
    finish: Arc<Notify>,
    lose_held_ack: Option<Arc<Notify>>,
}
impl AgentExecutor030 for GatedAgent {
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
        mut work: AgentWork030,
        events: WorkEventSink030,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, HostResult<()>> {
        let step = work
            .attempt
            .work()
            .step
            .as_ref()
            .unwrap()
            .last()
            .unwrap()
            .clone();
        let finish = self.finish.clone();
        let lose_held_ack = self.lose_held_ack.clone();
        Box::pin(async move {
            if step == "work" {
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return Ok(()),
                        _ = finish.notified() => break,
                        hold = work.operator.requested() => {
                            if let Some(lose) = &lose_held_ack {
                                tokio::select! {
                                    _ = lose.notified() => (),
                                    result = work.operator.park(hold.clone()) => { result?; continue; },
                                    _ = cancel.cancelled() => return Ok(()),
                                }
                            }
                            tokio::select! {
                                result = work.operator.park(hold) => result?,
                                _ = cancel.cancelled() => return Ok(()),
                            }
                        }
                    }
                }
            }
            let submission = if step == "list" {
                json!({"results":{"items":["a","b","c"]}})
            } else {
                json!({})
            };
            events.emit(json!({"kind":"submit","callId":"script","argumentsJson":submission.to_string()}), &cancel).await?;
            Ok(())
        })
    }
}

#[tokio::test]
async fn operator_schedule_lost_executor_held_ack() {
    let f = Fixture::with_document(document(
        json!({"work":{"type":"agent","agent":"worker","timeoutMs":10000}}),
        "work",
    ))
    .await;
    let scope = operator_scope(&f).await;
    let tasks = StreamTasks::default();
    let lose = Arc::new(Notify::new());
    let agent = Arc::new(GatedAgent {
        lose_held_ack: Some(lose.clone()),
        ..Default::default()
    });
    start(&f, &tasks, agent.clone());
    let id = operator_wait(async || {
        sqlx::query_scalar::<_, String>("SELECT activation_id FROM engine_flow030_clock_works")
            .fetch_optional(&f.pool)
            .await
            .unwrap()
    })
    .await;
    let gap = arm(&f, "operator.held.ack");
    operator_control(&f, &scope, "interrupt", Command::Interrupt.action(&id))
        .await
        .unwrap();
    reached(&gap, "operator.held.ack").await;
    let ready: bool = sqlx::query_scalar(
        "SELECT value ->> '$.ready' FROM engine_flow030_runs, json_each(state_json, '$.operatorHolds') WHERE key=?",
    ).bind(&id).fetch_one(&f.pool).await.unwrap();
    assert!(ready);
    assert!(f.host.flow030().available(&f.identity).is_err());
    lose.notify_one();
    f.host.flow030().supervisors.wake(&scope.system_start_id);
    operator_wait(async || f.host.flow030().available(&f.identity).ok()).await;
    operator_control(&f, &scope, "resume", Command::ResumeWork.action(&id))
        .await
        .unwrap();
    agent.finish.notify_one();
    finished(&f, "succeeded").await;
    let confirms: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM engine_flow030_events WHERE event_id LIKE 'operator-held:%'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(confirms, 1);
}

#[tokio::test]
async fn operator_schedule_join_and_compose_with_lost_pause_and_resume_acks() {
    let boundaries: &[_] = if exhaustive() {
        &[
            "supervisor.joined",
            "work.join.reserved",
            "work.join.evaluated",
        ]
    } else {
        &["supervisor.joined"]
    };
    for &boundary in boundaries {
        for compose in [false, true] {
            let mut doc = document(
                json!({
                    "work":{"type":"agent","agent":"worker","next":{"ok":{"output":"ok"}}}
                }),
                "work",
            );
            if compose {
                doc["compositions"] =
                    json!({"inner":{"entry":"work","outputs":["ok"],"steps":doc["flow"]["steps"]}});
                // A default route lets the submitted empty result take the sole output.
                doc["compositions"]["inner"]["steps"]["work"]["next"] = json!({"output":"ok"});
                doc["flow"] = json!({"entry":"compose","steps":{"compose":{"type":"compose","composition":"inner","next":{"ok":[]}}}});
            } else {
                doc["flow"] = json!({"entry":"seed","steps":{
                    "seed":{"type":"agent","agent":"worker","next":["work","other"]},
                    "work":{"type":"agent","agent":"worker","instance":"new","next":"join"},
                    "other":{"type":"agent","agent":"worker","instance":"new","next":"join"},
                    "join":{"type":"join","next":[]}
                }});
            }
            let f = Arc::new(Fixture::with_document(doc).await);
            let scope = operator_scope(&f).await;
            let tasks = StreamTasks::default();
            let agent = Arc::new(GatedAgent::default());
            start(&f, &tasks, agent.clone());
            let id = operator_wait(async || {
                let view = inspect(&f).await;
                view.state
                    .activations
                    .iter()
                    .find(|a| {
                        a.physical
                            && a.step
                                .as_ref()
                                .and_then(|s| s.last())
                                .is_some_and(|s| s == "work")
                    })
                    .map(|a| a.id.clone())
                    .filter(|_| {
                        compose
                            || view.state.activations.iter().any(|a| {
                                a.step.as_deref() == Some(&["other".into()]) && a.result.is_some()
                            })
                    })
            })
            .await;
            let gap = arm(&f, boundary);
            agent.finish.notify_one();
            reached(&gap, boundary).await;
            cancel_command(
                &f,
                &scope,
                "scheduled-pause",
                Action::PauseRun,
                "operator.ack",
            )
            .await;
            gap.release();
            operator_wait(async || {
                inspect(&f)
                    .await
                    .state
                    .activations
                    .iter()
                    .any(|a| a.id == id && a.result.is_some())
                    .then_some(())
            })
            .await;
            assert!(inspect(&f).await.state.paused);
            let pass = freeze_pass(&f).await;
            cancel_command(
                &f,
                &scope,
                "scheduled-resume",
                Action::ResumeRun,
                "operator.ack",
            )
            .await;
            pass.release();
            f.host.flow030().supervisors.wake(&scope.system_start_id);
            finished(&f, "succeeded").await;
        }
    }
}

#[tokio::test]
async fn operator_schedule_fanout_with_pause_interrupt_and_stop() {
    for action in ["pause", "interrupt", "stop"] {
        let mut doc = document(
            json!({
                "list":{"type":"agent","agent":"lead","next":{"step":"work","each":["results","items"],"concurrency":1}},
                "work":{"type":"agent","agent":"worker","instance":"new"}
            }),
            "list",
        );
        doc["agents"]["lead"] = json!({"harness":"codex","permissions":"read-only","results":{"items":{"schema":{"type":"array","items":{"type":"string"}}}}});
        let f = Fixture::with_document(doc).await;
        let scope = operator_scope(&f).await;
        let tasks = StreamTasks::default();
        let agent = Arc::new(GatedAgent::default());
        let second = f
            .host
            .flow030()
            .install_nth_operator_test_hook("supervisor.pass", 2);
        start(&f, &tasks, agent.clone());
        reached(&second, "supervisor.pass").await;
        assert_eq!(
            inspect(&f).await.state.activations[0].step.as_deref(),
            Some(["list".into()].as_slice())
        );
        second.release();

        for index in ["0", "1", "2"] {
            let id = operator_wait(async || {
                let view = inspect(&f).await;
                assert!(
                    view.state.verdict.is_none(),
                    "item {index}: {:?}",
                    view.state.activations
                );
                let id = &view.state.activations.iter().find(|a| {
                    a.physical && a.item_json.is_some()
                        && a.occurrence.as_ref().is_some_and(|o| o.item_index == index)
                })?.id;
                sqlx::query_scalar::<_, String>("SELECT activation_id FROM engine_flow030_clock_works WHERE activation_id=? AND retired=0")
                    .bind(id).fetch_optional(&f.pool).await.unwrap()
            })
            .await;
            if index == "1" && action == "stop" {
                stop(&f).await;
                stopped(&f).await;
                assert!(inspect(&f).await.state.activations.iter().all(|a| a
                    .occurrence
                    .as_ref()
                    .is_none_or(|o| o.item_index != "2")
                    || !a.physical));
                break;
            }
            if index == "1" {
                let (hold, resume) = if action == "pause" {
                    (Command::Pause, Command::ResumeRun)
                } else {
                    (Command::Interrupt, Command::ResumeWork)
                };
                operator_control(&f, &scope, "hold-item", hold.action(&id))
                    .await
                    .unwrap();
                wait_held(&f, &id).await;
                assert_eq!(
                    inspect(&f)
                        .await
                        .state
                        .activations
                        .iter()
                        .filter(|a| a.physical)
                        .count(),
                    1
                );
                operator_control(&f, &scope, "resume-item", resume.action(&id))
                    .await
                    .unwrap();
            }
            agent.finish.notify_one();
        }
        if action != "stop" {
            finished(&f, "succeeded").await;
        }
    }
}

#[tokio::test]
async fn operator_schedule_approval_result_at_generic_flow_commit() {
    for boundary in ["flow.commit", "flow.ack"] {
        let f = Arc::new(
            Fixture::with_document(document(
                json!({"approval":{"type":"approval","timeoutMs":1000,"next":{"yes":[],"no":[]}}}),
                "approval",
            ))
            .await,
        );
        let runtime = f.host.flow030();
        let clock = runtime.human_runtime().virtual_clock().await.unwrap();
        let tasks = StreamTasks::default();
        start(&f, &tasks, Arc::new(ScheduledAgent::default()));
        operator_wait(async || {
            sqlx::query_scalar::<_, String>(
                "SELECT request_id FROM engine_flow030_human_requests WHERE kind='approval'",
            )
            .fetch_optional(&f.pool)
            .await
            .unwrap()
        })
        .await;
        let pass = freeze_pass(&f).await;
        let gap = arm(&f, boundary);
        clock.advance_to(1001).unwrap();
        pass.release();
        reached(&gap, boundary).await;
        let result: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM engine_flow030_events WHERE event_id LIKE 'human:%:terminal'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!(result, i64::from(boundary == "flow.ack"));
        runtime
            .supervisors
            .crash_operator_fixture(&f.identity.system_start_id)
            .await;
        let reopened = crate::sqlite::open_file(&f._temp.path().join("run.db"))
            .await
            .unwrap();
        crate::api::interrupt_previous_runs(&reopened)
            .await
            .unwrap();
        journal(&f).await;
    }
}

#[tokio::test]
async fn operator_schedule_steer_failure_withdraws_selected_approval() {
    let f = Fixture::with_document(document(
        json!({
            "seed":{"type":"agent","agent":"worker","next":["work","steer","approval"]},
            "work":{"type":"agent","agent":"worker"},
            "steer":{"type":"agent","agent":"worker","delivery":"steer","next":"join"},
            "approval":{"type":"approval","next":{"approve":"join"}},
            "join":{"type":"join","next":[],"onError":"handler"},
            "handler":{"type":"agent","agent":"worker","instance":"new"}
        }),
        "seed",
    ))
    .await;
    let tasks = StreamTasks::default();
    let steer = Arc::new(DeferredSteer::default());
    let selected = arm(&f, "approval.claim");
    start(
        &f,
        &tasks,
        Arc::new(PausableAgent {
            deferred_steer: Some(steer.clone()),
            ..Default::default()
        }),
    );
    reached(&selected, "approval.claim").await;
    tokio::time::timeout(WAIT, steer.sent.notified())
        .await
        .unwrap();
    steer.report.notify_one();
    operator_wait(async || {
        let view = inspect(&f).await;
        let approval = view.effects.iter().find_map(|row| {
            let effect: crate::persistence::flow030::FlowEffect030 =
                serde_json::from_value(row["effect"].clone()).unwrap();
            matches!(&effect.kind, crate::persistence::flow030::FlowEffectKind030::Start {step:Some(step),..} if step == &["approval"])
                .then_some(effect)
        })?;
        (!crate::persistence::flow_store030::eligible(&view.state, &approval)).then_some(())
    }).await;
    assert!(!inspect(&f).await.state.stopping);
    selected.release();
    operator_wait(async || {
        if let Some(outcome) = f
            .host
            .flow030()
            .supervisors
            .outcome(&f.identity.system_start_id)
        {
            panic!("Supervisor stopped after the approval changed: {outcome:?}");
        }
        inspect(&f)
            .await
            .state
            .activations
            .iter()
            .any(|a| a.step.as_deref() == Some(&["handler".into()]) && a.result.is_some())
            .then_some(())
    })
    .await;
    let approvals: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM engine_flow030_human_requests WHERE kind='approval'",
    )
    .fetch_one(&f.pool)
    .await
    .unwrap();
    assert_eq!(approvals, 0);
    stop(&f).await;
    stopped(&f).await;
}
