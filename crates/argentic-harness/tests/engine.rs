use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use argentic_harness::{
    config::Settings,
    policy::{Action, Effect, Preset, ReadPolicy, Rule},
    providers::{
        Chunk, Finish, Message, Provider, ProviderError, ProviderStream, Request, ScriptedProvider,
        ToolCall, ToolChoice, Usage,
    },
    r#loop::{Event, Failure, Harness, Hook, ReviewBudget, Steer, Work, WorkPort},
    tools::{Approval, Authorizer, Extensions, Page, Registry, ToolOutput},
};
use async_trait::async_trait;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_util::sync::CancellationToken;

fn call(id: &str, name: &str, arguments: Value) -> Chunk {
    Chunk::ToolCall(ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: arguments.to_string(),
    })
}

fn submit(id: &str, results: Value) -> Chunk {
    call(id, "agsdl_submit_result", json!({ "results": results }))
}

fn end(finish: Finish) -> Chunk {
    Chunk::End {
        finish,
        usage: Some(Usage {
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
            prompt_tokens: 10,
            completion_tokens: 5,
        }),
    }
}

fn fixture(turns: Vec<Vec<Chunk>>, project: Value) -> (tempfile::TempDir, Harness) {
    fixture_with_provider(Arc::new(scripted(turns)), project)
}

fn scripted(turns: Vec<Vec<Chunk>>) -> ScriptedProvider {
    ScriptedProvider::new(
        turns
            .into_iter()
            .map(|turn| turn.into_iter().map(Ok).collect()),
    )
}

fn fixture_with_provider(
    provider: Arc<dyn Provider>,
    project: Value,
) -> (tempfile::TempDir, Harness) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("notes.txt"), "result is 42\n").unwrap();
    let root = dir.path().canonicalize().unwrap();
    let policy = ReadPolicy::compile(&root, None, Some(Preset::ReadOnly), None, &[]).unwrap();
    let registry = Registry::new(
        &[root],
        policy,
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["results"],
            "properties": {"results": {
                "type": "object", "additionalProperties": false, "required": ["answer"],
                "properties": { "answer": {"type": "integer", "enum": [42]} }
            }}
        }),
    )
    .unwrap();
    let settings = Settings::resolve(&json!({}), &json!({}), &project).unwrap();
    (dir, Harness::new(provider, Arc::new(registry), settings))
}

fn work(submission_required: bool) -> Work {
    Work {
        model: "script/test".parse().unwrap(),
        reasoning_effort: None,
        messages: vec![Message::User("Read notes.txt, then answer.".into())],
        submission_required,
        max_corrections: 2,
    }
}

#[derive(Default)]
struct Trace(Mutex<Vec<String>>);

#[async_trait]
impl WorkPort for Trace {
    async fn event(&self, event: Event<'_>, _: &CancellationToken) -> Result<(), Failure> {
        let entry = match event {
            Event::VisibleText { text, .. } => text.into(),
            Event::Reasoning { .. } => "reasoning".into(),
            Event::ToolResult { call, content, .. } => format!("{} {content}", call.id),
            Event::SteerConsumed { id } => format!("consumed {id}"),
            Event::Compaction {
                summarized_messages,
                truncated_tool_results,
                ..
            } => {
                format!("compacted {summarized_messages} messages and truncated {truncated_tool_results} tool results")
            }
            Event::HistoryPruned {
                before_bytes,
                after_bytes,
                dropped_messages,
            } => {
                assert!(before_bytes > 32 * 1024 * 1024);
                assert!(after_bytes <= 32 * 1024 * 1024);
                format!("pruned {dropped_messages} messages")
            }
        };
        self.0.lock().unwrap().push(entry);
        Ok(())
    }
}

#[tokio::test]
async fn reads_and_submits_a_valid_result() {
    let (_dir, harness) = fixture(
        vec![
            vec![
                call("read-1", "read", json!({"path":"notes.txt"})),
                call("list-1", "list", json!({})),
                end(Finish::ToolCalls),
            ],
            vec![
                Chunk::Reasoning("hidden".into()),
                Chunk::Text("I found the answer.".into()),
                submit("s1", json!({"answer":42.0})),
                end(Finish::ToolCalls),
            ],
        ],
        json!({}),
    );
    let trace = Trace::default();
    let (_, steer) = mpsc::channel(4);
    let report = harness
        .run(work(true), &trace, steer, CancellationToken::new())
        .await;
    let outcome = report.outcome.unwrap();
    assert_eq!(
        outcome.submission.unwrap()["results"]["answer"].as_f64(),
        Some(42.0)
    );
    assert_eq!(outcome.text, "I found the answer.");
    let events = trace.0.lock().unwrap();
    assert!(events[0].contains("result is 42"));
    assert!(events[1].contains("notes.txt"));
    assert!(!report.physical_end.forced());
}

#[derive(Default)]
struct Requests(Mutex<Vec<Vec<Message>>>);

#[async_trait]
impl Hook for Requests {
    async fn prepare_request(
        &self,
        _: u32,
        messages: &mut std::borrow::Cow<'_, [Message]>,
        _: &CancellationToken,
    ) -> Result<(), Failure> {
        self.0.lock().unwrap().push(messages.to_vec());
        Ok(())
    }
}

#[tokio::test]
async fn invalid_submission_is_corrected_in_the_same_instance() {
    let (_dir, harness) = fixture(
        vec![
            vec![
                submit("bad", json!({"answer":"42"})),
                end(Finish::ToolCalls),
            ],
            vec![submit("good", json!({"answer":42})), end(Finish::ToolCalls)],
        ],
        json!({}),
    );
    let requests = Arc::new(Requests::default());
    let harness = harness.with_hook(requests.clone());
    let (_, steer) = mpsc::channel(4);
    let report = harness
        .run(work(true), &(), steer, CancellationToken::new())
        .await;
    assert_eq!(
        report.outcome.unwrap().submission.unwrap()["results"]["answer"],
        42
    );
    let requests = requests.0.lock().unwrap();
    assert_eq!(requests[0][0], requests[1][0]);
    assert!(matches!(&requests[1][1], Message::Assistant { calls, .. } if calls[0].id == "bad"));
    assert!(
        matches!(&requests[1][2], Message::Tool { call_id, content } if call_id == "bad" && content.contains("/results/answer"))
    );
}

struct PauseTool {
    entered: Notify,
    released: Notify,
    dropped: Notify,
}

impl PauseTool {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            released: Notify::new(),
            dropped: Notify::new(),
        })
    }
}

struct NotifyOnDrop<'a>(&'a Notify);
impl Drop for NotifyOnDrop<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[async_trait]
impl Hook for PauseTool {
    async fn after_tool_call(
        &self,
        _: u32,
        call: &ToolCall,
        _: &str,
        _: &CancellationToken,
    ) -> Result<(), Failure> {
        if call.name == "read" {
            let _guard = NotifyOnDrop(&self.dropped);
            self.entered.notify_one();
            self.released.notified().await;
        }
        Ok(())
    }
}

struct RecordingProvider {
    requests: Arc<Requests>,
    provider: ScriptedProvider,
}

#[async_trait]
impl Provider for RecordingProvider {
    async fn stream(
        &self,
        request: Request<'_>,
        cancel: CancellationToken,
    ) -> Result<ProviderStream, ProviderError> {
        self.requests
            .0
            .lock()
            .unwrap()
            .push(request.messages.to_vec());
        self.provider.stream(request, cancel).await
    }
}

fn recorded_tool_turn(id: &str, output: String) -> [Message; 2] {
    [
        Message::Assistant {
            text: String::new(),
            reasoning: String::new(),
            replay: None,
            calls: vec![ToolCall {
                id: id.into(),
                name: "read".into(),
                arguments: "{}".into(),
            }],
        },
        Message::Tool {
            call_id: id.into(),
            content: output,
        },
    ]
}

#[tokio::test]
async fn compaction_can_truncate_an_old_tool_result_without_a_summary_request() {
    let (_dir, harness) = fixture(
        vec![vec![
            submit("done", json!({"answer":42})),
            end(Finish::ToolCalls),
        ]],
        json!({"compaction":{"keepRecentTurns":1}}),
    );
    let mut work = work(true);
    work.messages
        .extend(recorded_tool_turn("old", "€".repeat(7000)));
    let recent = recorded_tool_turn("recent", "Recent tool result".into());
    work.messages.extend(recent.clone());
    let trace = Trace::default();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_context_window(Some(8192))
        .run(work, &trace, steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    assert!(
        matches!(&report.messages[2], Message::Tool { call_id, content }
        if call_id == "old" && content.len() <= 4096 && content.ends_with("[truncated]"))
    );
    assert_eq!(&report.messages[3..5], &recent);
    assert_eq!(
        trace.0.lock().unwrap()[0],
        "compacted 0 messages and truncated 1 tool results"
    );
}

#[tokio::test]
async fn compaction_leaves_pending_submission_feedback_in_the_next_request() {
    let requests = Arc::new(Requests::default());
    let provider = Arc::new(RecordingProvider {
        requests: requests.clone(),
        provider: scripted(vec![
            vec![
                Chunk::Text("earlier evidence ".repeat(730)),
                call("read", "list", json!({})),
                end(Finish::ToolCalls),
            ],
            vec![
                Chunk::Text("recent attempt ".repeat(825)),
                submit("invalid", json!({"answer":"wrong"})),
                end(Finish::ToolCalls),
            ],
            vec![
                Chunk::Text("Remember the earlier evidence.".into()),
                end(Finish::Stop),
            ],
            vec![submit("done", json!({"answer":42})), end(Finish::ToolCalls)],
        ]),
    });
    let (_dir, harness) = fixture_with_provider(
        provider,
        json!({
            "maxOutputTokens":256, "compaction":{"threshold":0.60,"keepRecentTurns":1}
        }),
    );
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_context_window(Some(8192))
        .run(work(true), &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    let requests = requests.0.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(matches!(&requests[2][0], Message::System(prompt) if prompt.contains("Summarize")));
    assert_eq!(&requests[3][0], &requests[0][0]);
    assert!(
        matches!(&requests[3][1], Message::User(summary) if summary.contains("Remember the earlier evidence."))
    );
    assert!(
        matches!(&requests[3][2], Message::Assistant { calls, .. } if calls[0].id == "invalid")
    );
    assert!(matches!(&requests[3][3], Message::Tool { call_id, content }
        if call_id == "invalid" && content.contains("/results/answer")));
}

#[tokio::test]
async fn steer_arriving_during_summary_reaches_the_next_agent_request() {
    struct SteerDuringSummary {
        recording: RecordingProvider,
        send: mpsc::Sender<Steer>,
        late: Mutex<Option<Steer>>,
    }

    #[async_trait]
    impl Provider for SteerDuringSummary {
        async fn stream(
            &self,
            request: Request<'_>,
            cancel: CancellationToken,
        ) -> Result<ProviderStream, ProviderError> {
            if request.tools.is_empty() {
                self.send
                    .try_send(self.late.lock().unwrap().take().unwrap())
                    .unwrap();
            }
            self.recording.stream(request, cancel).await
        }
    }

    let (send, steer) = mpsc::channel(2);
    let (early, early_receipt) = oneshot::channel();
    let (late, late_receipt) = oneshot::channel();
    send.try_send(Steer {
        reference: None,
        id: "early".into(),
        message: "Earlier steer".into(),
        consumed: early,
    })
    .unwrap();
    let requests = Arc::new(Requests::default());
    let provider = Arc::new(SteerDuringSummary {
        recording: RecordingProvider {
            requests: requests.clone(),
            provider: scripted(vec![
                vec![
                    Chunk::Text("recent ".repeat(1800)),
                    call("read", "list", json!({})),
                    end(Finish::ToolCalls),
                ],
                vec![Chunk::Text("Earlier evidence".into()), end(Finish::Stop)],
                vec![submit("done", json!({"answer":42})), end(Finish::ToolCalls)],
            ]),
        },
        send,
        late: Mutex::new(Some(Steer {
            reference: None,
            id: "late".into(),
            message: "Steer during summary".into(),
            consumed: late,
        })),
    });
    let (_dir, harness) = fixture_with_provider(
        provider,
        json!({"maxOutputTokens":256,"compaction":{"threshold":0.60,"keepRecentTurns":1}}),
    );
    let mut work = work(true);
    let mut older = recorded_tool_turn("old", "earlier evidence".into());
    if let Message::Assistant { text, .. } = &mut older[0] {
        *text = "old ".repeat(3000);
    }
    work.messages.extend(older);
    let report = harness
        .with_context_window(Some(8192))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    assert!(requests.0.lock().unwrap()[2].ends_with(&[
        Message::User("Earlier steer".into()),
        Message::User("Steer during summary".into()),
    ]));
    early_receipt.await.unwrap();
    late_receipt.await.unwrap();
}

#[tokio::test]
async fn compaction_reserves_tokens_for_the_next_agent_request() {
    struct BudgetProvider;

    #[async_trait]
    impl Provider for BudgetProvider {
        fn min_output_tokens(&self) -> u32 {
            16
        }

        async fn stream(
            &self,
            request: Request<'_>,
            _: CancellationToken,
        ) -> Result<ProviderStream, ProviderError> {
            let (chunk, finish, prompt_tokens, completion_tokens) = if request.tools.is_empty() {
                (
                    Chunk::Text("Earlier evidence".into()),
                    Finish::Stop,
                    200,
                    10,
                )
            } else {
                (
                    submit("done", json!({"answer":42})),
                    Finish::ToolCalls,
                    3960,
                    255,
                )
            };
            Ok(Box::pin(futures::stream::iter([
                Ok(chunk),
                Ok(Chunk::End {
                    finish,
                    usage: Some(Usage {
                        prompt_tokens,
                        completion_tokens,
                        cache_read_tokens: 0,
                        cache_creation_tokens: 0,
                    }),
                }),
            ])))
        }
    }

    let (_dir, harness) = fixture_with_provider(
        Arc::new(BudgetProvider),
        json!({"maxTotalTokens":4400,"maxOutputTokens":256,
            "compaction":{"threshold":0.60,"keepRecentTurns":1}}),
    );
    let mut work = work(true);
    work.messages
        .extend(recorded_tool_turn("old", "x".repeat(3000)));
    work.messages
        .extend(recorded_tool_turn("recent", "x".repeat(12_000)));
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_context_window(Some(6000))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    assert!(matches!(&report.messages[2], Message::Tool { call_id, .. } if call_id == "old"));
}

#[tokio::test]
async fn compaction_rechecks_the_input_budget_after_summarizing_each_old_range() {
    let requests = Arc::new(Requests::default());
    let provider = Arc::new(RecordingProvider {
        requests: requests.clone(),
        provider: scripted(vec![
            vec![
                Chunk::Text("first ".repeat(670)),
                call("first", "list", json!({})),
                end(Finish::ToolCalls),
            ],
            vec![
                Chunk::Text("recent ".repeat(2450)),
                call("recent", "list", json!({})),
                end(Finish::ToolCalls),
            ],
            vec![Chunk::Text("Prior work".into()), end(Finish::Stop)],
            vec![Chunk::Text("Earlier tool turn".into()), end(Finish::Stop)],
            vec![submit("done", json!({"answer":42})), end(Finish::ToolCalls)],
        ]),
    });
    let (_dir, harness) = fixture_with_provider(
        provider,
        json!({"maxOutputTokens":256,"compaction":{"threshold":0.99,"keepRecentTurns":1}}),
    );
    let mut work = work(true);
    let mut old = recorded_tool_turn("old", "Old tool result".into());
    if let Message::Assistant { text, .. } = &mut old[0] {
        *text = "old ".repeat(3000);
    }
    work.messages.extend(old);
    let mission = Message::User("mission ".repeat(1500));
    work.messages.push(mission.clone());
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_context_window(Some(8192))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    let requests = requests.0.lock().unwrap();
    assert_eq!(requests.len(), 5);
    for request in &requests[2..4] {
        assert!(matches!(&request[0], Message::System(prompt) if prompt.contains("Summarize")));
    }
    assert_eq!(requests[4][2], mission);
    assert!(matches!(&requests[4][1], Message::User(summary) if summary.ends_with("Prior work")));
    assert!(
        matches!(&requests[4][3], Message::User(summary) if summary.ends_with("Earlier tool turn"))
    );
    assert!(matches!(&requests[4][5], Message::Tool { call_id, .. } if call_id == "recent"));
}

#[tokio::test]
async fn compaction_leaves_history_intact_when_the_summary_finishes_at_its_limit() {
    let (_dir, harness) = fixture(
        vec![
            vec![Chunk::Text("A partial summary".into()), end(Finish::Length)],
            vec![submit("done", json!({"answer":42})), end(Finish::ToolCalls)],
        ],
        json!({"compaction":{"keepRecentTurns":1}}),
    );
    let mut work = work(true);
    work.messages
        .extend(recorded_tool_turn("old", "earlier evidence".into()));
    work.messages
        .extend(recorded_tool_turn("recent", "x".repeat(40_000)));
    let initial = work.messages.clone();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_context_window(Some(8192))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    assert_eq!(&report.messages[..initial.len()], &initial);
}

#[tokio::test]
async fn compaction_skips_an_incomplete_tool_exchange() {
    let (_dir, harness) = fixture(
        vec![vec![
            submit("done", json!({"answer":42})),
            end(Finish::ToolCalls),
        ]],
        json!({"compaction":{"keepRecentTurns":1}}),
    );
    let mut work = work(true);
    let [unanswered, _] = recorded_tool_turn("unanswered", String::new());
    work.messages.push(unanswered);
    work.messages
        .extend(recorded_tool_turn("recent", "x".repeat(40_000)));
    let initial = work.messages.clone();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_context_window(Some(8192))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    assert_eq!(&report.messages[..initial.len()], &initial);
}

#[tokio::test]
async fn starting_work_prunes_complete_old_exchanges_to_make_room_for_the_mission() {
    let requests = Arc::new(Requests::default());
    let provider = Arc::new(RecordingProvider {
        requests: requests.clone(),
        provider: scripted(vec![vec![end(Finish::Stop)]]),
    });
    let (_dir, harness) = fixture_with_provider(provider, json!({"compaction":{"enabled":false}}));
    let mut work = work(false);
    work.messages
        .insert(0, Message::System("Instructions".into()));
    let mut expected = work.messages.clone();
    work.messages
        .push(Message::User("x".repeat(32 * 1024 * 1024 - 4096)));
    work.messages
        .extend(recorded_tool_turn("old", "Discarded together".into()));
    let recent = recorded_tool_turn("recent", "Retained together".into());
    work.messages.extend(recent.clone());
    expected.extend(recent);
    let mission = Message::User("m".repeat(4096));
    work.messages.push(mission.clone());
    expected.push(mission);
    let (_, steer) = mpsc::channel(1);
    let trace = Trace::default();
    let report = harness
        .run(work, &trace, steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    assert_eq!(requests.0.lock().unwrap().as_slice(), &[expected]);
    assert_eq!(trace.0.lock().unwrap().as_slice(), &["pruned 3 messages"]);
}

#[tokio::test]
async fn mid_tool_steer_waits_for_results() {
    let requests = Arc::new(Requests::default());
    let provider = Arc::new(RecordingProvider {
        requests: requests.clone(),
        provider: scripted(vec![
            vec![
                call("read", "read", json!({"path":"notes.txt"})),
                end(Finish::ToolCalls),
            ],
            vec![submit("s1", json!({"answer":42})), end(Finish::ToolCalls)],
        ]),
    });
    let (_dir, harness) = fixture_with_provider(provider, json!({}));
    let pause = PauseTool::new();
    let harness = harness.with_hook(pause.clone());
    let (send, steer) = mpsc::channel(4);
    let trace = Trace::default();
    let dispatch = async {
        pause.entered.notified().await;
        let (consumed, mut receipt) = oneshot::channel();
        send.send(Steer {
            reference: None,
            id: "s1".into(),
            message: "From reviewer: include units".into(),
            consumed,
        })
        .await
        .unwrap();
        assert!(matches!(
            receipt.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        pause.released.notify_one();
        receipt.await.unwrap();
    };
    let (report, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(
            harness.run(work(true), &trace, steer, CancellationToken::new()),
            dispatch
        )
    })
    .await
    .unwrap();
    assert!(report.outcome.is_ok());
    let requests = requests.0.lock().unwrap();
    assert!(matches!(&requests[1][2], Message::Tool { .. }));
    assert_eq!(
        requests[1][3],
        Message::User("From reviewer: include units".into())
    );
    let events = trace.0.lock().unwrap();
    assert!(events[0].starts_with("read "));
    assert_eq!(events[1], "consumed s1");
}

#[tokio::test]
async fn cancellation_during_tool_call_physically_ends_before_return() {
    let (_dir, harness) = fixture(
        vec![vec![
            call("read", "read", json!({"path":"notes.txt"})),
            end(Finish::ToolCalls),
        ]],
        json!({"cancelGraceMs": 1}),
    );
    let pause = PauseTool::new();
    let harness = harness.with_hook(pause.clone());
    let (_, steer) = mpsc::channel(4);
    let parent = CancellationToken::new();
    let cancel = async {
        pause.entered.notified().await;
        parent.cancel();
    };
    let (report, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(harness.run(work(true), &(), steer, parent.clone()), cancel)
    })
    .await
    .unwrap();
    assert!(matches!(report.outcome, Err(Failure::Cancelled)));
    assert!(report.physical_end.forced());
    assert!(
        matches!(&report.messages[1], Message::Assistant { calls, .. } if calls[0].id == "read")
    );
    assert!(
        matches!(&report.messages[2], Message::Tool { call_id, content }
        if call_id == "read" && content.contains("stopped before a tool result"))
    );
    tokio::time::timeout(Duration::from_millis(100), pause.dropped.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn max_turns_stops_even_if_provider_can_continue() {
    let (_dir, harness) = fixture(
        vec![
            vec![
                call("read", "read", json!({"path":"notes.txt"})),
                end(Finish::ToolCalls),
            ],
            vec![submit("s1", json!({"answer":42})), end(Finish::ToolCalls)],
        ],
        json!({"maxTurns":1}),
    );
    let (_, steer) = mpsc::channel(4);
    let report = harness
        .run(work(true), &(), steer, CancellationToken::new())
        .await;
    assert!(matches!(report.outcome, Err(Failure::TurnLimit)));
}

#[tokio::test]
async fn token_limit_is_checked_before_tools_execute() {
    let (_dir, harness) = fixture(
        vec![vec![
            call("read", "read", json!({"path":"notes.txt"})),
            end(Finish::ToolCalls),
        ]],
        json!({"maxTotalTokens":14}),
    );
    let (_, steer) = mpsc::channel(4);
    let trace = Trace::default();
    let report = harness
        .run(work(true), &trace, steer, CancellationToken::new())
        .await;
    assert!(matches!(report.outcome, Err(Failure::TokenLimit)));
    assert!(trace.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_and_refused_submissions_share_the_correction_budget() {
    let (_dir, harness) = fixture(
        vec![
            vec![submit("bad", json!({})), end(Finish::ToolCalls)],
            vec![Chunk::Text("Missing result".into()), end(Finish::Stop)],
            vec![
                submit("bad-again", json!({"answer":0})),
                end(Finish::ToolCalls),
            ],
        ],
        json!({}),
    );
    let (_, steer) = mpsc::channel(4);
    let report = harness
        .run(work(true), &(), steer, CancellationToken::new())
        .await;
    assert!(matches!(report.outcome, Err(Failure::OutputInvalid(_))));
}

#[tokio::test]
async fn text_only_submission_gets_a_tool_call_correction() {
    struct CorrectionScript {
        provider: ScriptedProvider,
        expected: ToolChoice<'static>,
    }

    #[async_trait]
    impl Provider for CorrectionScript {
        async fn stream(
            &self,
            request: Request<'_>,
            cancel: CancellationToken,
        ) -> Result<ProviderStream, ProviderError> {
            let Some(Message::User(last)) = request.messages.last() else {
                panic!("expected a user message");
            };
            if request.messages.len() == 1 {
                assert_eq!(request.tool_choice, ToolChoice::Auto);
            } else {
                assert_eq!(request.tool_choice, self.expected);
                let (instruction, details) = last.split_once("\n\n").unwrap();
                assert!(instruction
                    .starts_with("Your previous reply did not call the agsdl_submit_result tool."));
                assert!(instruction.contains("argument schema"));
                assert!(instruction.contains("leave the assistant text empty."));
                assert!(instruction.contains(r#"required keys in results are ["answer"]"#));
                let details: Value = serde_json::from_str(details).unwrap();
                assert_eq!(details["diagnostics"][0]["rule"], "SUBMISSION_REQUIRED");
            }
            self.provider.stream(request, cancel).await
        }
    }

    for (project, expected) in [
        (json!({}), ToolChoice::Named("agsdl_submit_result")),
        (
            json!({"forceSubmissionOnCorrection":false}),
            ToolChoice::Auto,
        ),
    ] {
        let provider = CorrectionScript {
            provider: scripted(vec![
                vec![
                    Chunk::Text(r#"agsdl_submit_result({"results":{"answer":42}})"#.into()),
                    end(Finish::Stop),
                ],
                vec![submit("s1", json!({"answer":42})), end(Finish::ToolCalls)],
            ]),
            expected,
        };
        let (_dir, harness) = fixture_with_provider(Arc::new(provider), project);
        let (_, steer) = mpsc::channel(1);
        let report = harness
            .run(work(true), &(), steer, CancellationToken::new())
            .await;
        assert_eq!(
            report.outcome.unwrap().submission,
            Some(json!({"results":{"answer":42}}))
        );
    }
}

#[tokio::test]
async fn short_keys_do_not_trigger_screening() {
    let key = "42";
    let work = work(true);
    let replay = serde_json::from_value(json!({
        "format": "Responses",
        "model": work.model,
        "items": [{"type":"function_call","id":key,"call_id":key,
                   "name":"agsdl_submit_result","arguments":"{\"results\":{\"answer\":42}}"}]
    }))
    .unwrap();
    let (_dir, harness) = fixture(
        vec![vec![
            submit(key, json!({"answer":42})),
            Chunk::Replay(replay),
            end(Finish::ToolCalls),
        ]],
        json!({}),
    );
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_provider_credential(Some(key.into()))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert_eq!(
        report.outcome.unwrap().submission,
        Some(json!({"results":{"answer":42}}))
    );
}

#[tokio::test]
async fn credential_in_tool_metadata_rejects_the_entire_batch() {
    const KEY: &str = "provider-fixture-key";
    for (id, name) in [(KEY, "read"), ("second", KEY)] {
        let (_dir, harness) = fixture(
            vec![vec![
                call("first", "read", json!({"path":"notes.txt"})),
                call(id, name, json!({"path":"notes.txt"})),
                end(Finish::ToolCalls),
            ]],
            json!({}),
        );
        let trace = Trace::default();
        let (_, steer) = mpsc::channel(1);
        let report = harness
            .with_provider_credential(Some(KEY.into()))
            .run(work(false), &trace, steer, CancellationToken::new())
            .await;
        assert!(matches!(
            report.outcome,
            Err(Failure::Provider(ProviderError::Invalid(_)))
        ));
        assert!(trace.0.lock().unwrap().is_empty());
        assert!(!serde_json::to_string(&report.messages)
            .unwrap()
            .contains(KEY));
    }
}

#[tokio::test]
async fn credential_in_native_tool_item_id_rejects_the_reply() {
    const KEY: &str = "provider-fixture-key";
    let work = work(true);
    let replay = serde_json::from_value(json!({
        "format": "Responses",
        "model": work.model,
        "items": [{"type":"function_call","id":KEY,"call_id":"safe",
                   "name":"agsdl_submit_result","arguments":"{\"results\":{\"answer\":42}}"}]
    }))
    .unwrap();
    let (_dir, harness) = fixture(
        vec![vec![
            submit("safe", json!({"answer":42})),
            Chunk::Replay(replay),
            end(Finish::ToolCalls),
        ]],
        json!({}),
    );
    let trace = Trace::default();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_provider_credential(Some(KEY.into()))
        .run(work, &trace, steer, CancellationToken::new())
        .await;
    assert!(matches!(
        report.outcome,
        Err(Failure::Provider(ProviderError::Invalid(_)))
    ));
    assert!(trace.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn tool_artifacts_share_a_per_work_byte_limit() {
    let reads = (0..11).map(|index| {
        call(
            &format!("read-{index}"),
            "read",
            json!({"path":"notes.txt","limit":1}),
        )
    });
    let (dir, harness) = fixture(
        vec![
            reads.chain([end(Finish::ToolCalls)]).collect(),
            vec![end(Finish::Stop)],
        ],
        json!({"maxParallelReads":4}),
    );
    std::fs::write(dir.path().join("notes.txt"), "x\n".repeat(4 * 1024 * 1024)).unwrap();
    let trace = Trace::default();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .run(work(false), &trace, steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok());
    assert_eq!(report.artifacts.len(), 8);
    assert_eq!(
        trace
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.contains("Artifact byte budget exceeded"))
            .count(),
        3,
    );
}

#[tokio::test]
async fn standalone_extension_pages_share_the_work_artifact_budget() {
    struct Export(String);

    #[async_trait]
    impl Extensions for Export {
        async fn execute(
            &self,
            call: &ToolCall,
            _: &CancellationToken,
            _: &ToolOutput,
        ) -> Result<Page, String> {
            Page::one_shot(&self.0, &call.id).map_err(|error| error.to_string())
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let policy = ReadPolicy::compile(&root, None, Some(Preset::ReadOnly), None, &[]).unwrap();
    let data = "x".repeat(8 * 1024 * 1024);
    std::fs::write(root.join("export.txt"), &data).unwrap();
    let registry = Registry::new(&[root], policy, json!(true))
        .unwrap()
        .with_extensions(
            vec![argentic_harness::providers::ToolDefinition {
                name: "export".into(),
                description: "Export data".into(),
                parameters: json!({"type":"object"}),
            }],
            Arc::new(Export(data)),
        )
        .unwrap();
    let batch = std::iter::once(call("read", "read", json!({"path":"export.txt"})))
        .chain((0..8).map(|index| call(&format!("export-{index}"), "export", json!({}))))
        .chain([end(Finish::ToolCalls)])
        .collect();
    let harness = Harness::new(
        Arc::new(scripted(vec![batch, vec![end(Finish::Stop)]])),
        Arc::new(registry),
        Settings::resolve(&json!({}), &json!({}), &json!({})).unwrap(),
    );
    let trace = Trace::default();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .run(work(false), &trace, steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok());
    assert_eq!(report.artifacts.len(), 8);
    assert!(trace.0.lock().unwrap().iter().any(|event| {
        event.starts_with("export-7 ") && event.contains("Artifact byte budget exceeded")
    }));
}

#[tokio::test]
async fn screened_tool_arguments_receive_same_work_corrections() {
    const KEY: &str = "provider-fixture-key";
    let work = work(true);
    let replay = serde_json::from_value(json!({
        "format": "Responses",
        "model": work.model,
        "items": [{"type":"function_call","call_id":"unsafe-submit",
                   "name":"agsdl_submit_result","arguments":format!(r#"{{"results":{{"answer":"\u0070{}"}}}}"#, &KEY[1..])}]
    }))
    .unwrap();
    let (_dir, harness) = fixture(
        vec![
            vec![
                call("unsafe-read", "read", json!({"path":KEY})),
                submit("unsafe-submit", json!({"answer":KEY})),
                Chunk::Replay(replay),
                end(Finish::ToolCalls),
            ],
            vec![
                submit("fixed-submit", json!({"answer":42})),
                end(Finish::ToolCalls),
            ],
        ],
        json!({}),
    );
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_provider_credential(Some(KEY.into()))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert_eq!(
        report.outcome.unwrap().submission,
        Some(json!({"results":{"answer":42}}))
    );
    let messages = serde_json::to_string(&report.messages).unwrap();
    assert!(messages.contains("configured provider credential"));
    assert!(messages.contains("Correct the result arguments"));
    assert!(!messages.contains(KEY));
    assert!(!messages.contains(&KEY[1..]));
}

#[tokio::test]
async fn tool_overflow_artifact_is_screened_before_pagination() {
    const KEY: &str = "provider-fixture-key";
    let (dir, harness) = fixture(
        vec![
            vec![
                call("page", "read", json!({"path":"notes.txt"})),
                end(Finish::ToolCalls),
            ],
            vec![end(Finish::Stop)],
        ],
        json!({}),
    );
    std::fs::write(
        dir.path().join("notes.txt"),
        format!("{}{}\n", "x".repeat(70_000), KEY),
    )
    .unwrap();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_provider_credential(Some(KEY.into()))
        .run(work(false), &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok());
    assert_eq!(report.artifacts.len(), 1);
    let text = std::fs::read_to_string(report.artifacts[0].file.path()).unwrap();
    assert!(text.ends_with("[redacted]\n"));
    assert!(!text.contains(KEY));
}

#[tokio::test]
async fn response_budget_counts_screened_stream_bytes() {
    let key = format!("{}z", "a".repeat(2048));
    let repeated = key.repeat(8 * 1024 * 1024 / key.len() + 1);
    let expected = "[redacted]".repeat(repeated.len() / key.len());
    let (_dir, harness) = fixture(
        vec![vec![
            Chunk::Text(repeated[..1024].into()),
            Chunk::Text(repeated[1024..].into()),
            end(Finish::Stop),
        ]],
        json!({}),
    );
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_provider_credential(Some(key))
        .run(work(false), &(), steer, CancellationToken::new())
        .await;
    assert_eq!(report.outcome.unwrap().text, expected);
}

#[tokio::test]
async fn native_replay_rejects_credential_member_names() {
    const KEY: &str = "provider-fixture-key";
    for (format, item) in [
        (
            "Anthropic",
            json!({"type":"thinking",(KEY):"opaque metadata"}),
        ),
        (
            "Anthropic",
            json!({"type":"thinking","extra":[{(KEY):"opaque metadata"}]}),
        ),
        ("Anthropic", json!({"type":"thinking","id":KEY})),
        (
            "Responses",
            json!({"type":"function_call","status":{(KEY):"malformed metadata"}}),
        ),
        (
            "Responses",
            json!({"type":"function_call","arguments":{(KEY):"malformed arguments"}}),
        ),
    ] {
        let work = work(false);
        let replay = serde_json::from_value(json!({
            "format": format,
            "model": work.model,
            "items": [item],
        }))
        .unwrap();
        let (_dir, harness) = fixture(
            vec![vec![Chunk::Replay(replay), end(Finish::Stop)]],
            json!({}),
        );
        let (_, steer) = mpsc::channel(1);
        let report = harness
            .with_provider_credential(Some(KEY.into()))
            .run(work, &(), steer, CancellationToken::new())
            .await;
        assert!(matches!(
            report.outcome,
            Err(Failure::Provider(ProviderError::Invalid(_)))
        ));
        assert_eq!(report.messages.len(), 1);
    }
}

#[tokio::test]
async fn editable_native_replay_masks_assistant_text() {
    const KEY: &str = "provider-fixture-key";
    for (format, item, screened) in [
        (
            "Anthropic",
            json!({"type":"text","text":KEY}),
            json!({"type":"text","text":"[redacted]"}),
        ),
        (
            "Responses",
            json!({"type":"message","role":"assistant","id":"msg_1","content":[
                {"type":"output_text","text":KEY,"annotations":[]},
                {"type":"refusal","refusal":KEY}
            ]}),
            json!({"type":"message","role":"assistant","id":"msg_1","content":[
                {"type":"output_text","text":"[redacted]","annotations":[]},
                {"type":"refusal","refusal":"[redacted]"}
            ]}),
        ),
    ] {
        let work = work(false);
        let replay = serde_json::from_value(json!({
            "format": format,
            "model": work.model,
            "items": [item],
        }))
        .unwrap();
        let (_dir, harness) = fixture(
            vec![vec![
                Chunk::Text(KEY.into()),
                Chunk::Replay(replay),
                end(Finish::Stop),
            ]],
            json!({}),
        );
        let (_, steer) = mpsc::channel(1);
        let report = harness
            .with_provider_credential(Some(KEY.into()))
            .run(work, &(), steer, CancellationToken::new())
            .await;
        assert_eq!(report.outcome.unwrap().text, "[redacted]");
        assert_eq!(
            serde_json::to_value(&report.messages[1]).unwrap()["Assistant"]["replay"]["items"],
            json!([screened])
        );
    }
}

fn delegation_fixture(turns: Vec<Vec<Chunk>>, limits: Value) -> (tempfile::TempDir, Harness) {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let policy = ReadPolicy::compile_rules(
        &root,
        None,
        Some(Preset::Workspace),
        None,
        &[Rule {
            action: Action::Delegate,
            pattern: None,
            effect: Effect::Allow,
        }],
    )
    .unwrap();
    let registry = Registry::new(&[root], policy, json!(true))
        .unwrap()
        .with_writes();
    let settings = Settings::resolve(&json!({}), &json!({}), &limits).unwrap();
    let harness =
        Harness::new(Arc::new(scripted(turns)), Arc::new(registry), settings).with_delegation();
    (dir, harness)
}

#[tokio::test]
async fn child_budget_exhaustion_stops_the_parent_tool_batch() {
    for token_limit in [29, 30] {
        let (_dir, harness) = delegation_fixture(
            vec![
                vec![
                    call("child", "delegate", json!({"task":"Inspect the workspace"})),
                    submit("submit", json!({"answer":42})),
                    end(Finish::ToolCalls),
                ],
                vec![Chunk::Text("Child reply".into()), end(Finish::Stop)],
            ],
            json!({"maxTotalTokens":token_limit}),
        );
        let trace = Trace::default();
        let (_, steer) = mpsc::channel(1);
        let report = harness
            .run(work(true), &trace, steer, CancellationToken::new())
            .await;
        assert!(matches!(report.outcome, Err(Failure::TokenLimit)));
        assert!(trace.0.lock().unwrap().is_empty());
    }
}

#[derive(Default)]
struct FixtureReview(Mutex<Option<ReviewBudget>>);

#[async_trait]
impl Hook for FixtureReview {
    fn begin_tool_batch(&self, budget: &ReviewBudget) {
        *self.0.lock().unwrap() = Some(budget.clone());
    }
}

#[async_trait]
impl Authorizer for FixtureReview {
    async fn approve(
        &self,
        _: &ToolCall,
        action: Approval<'_>,
        _: &CancellationToken,
    ) -> Result<bool, String> {
        assert!(matches!(action, Approval::Delegate));
        let budget = self.0.lock().unwrap().clone().unwrap();
        Ok(budget.reserve(5, 5).unwrap().settle(Some(Usage {
            prompt_tokens: 45,
            completion_tokens: 5,
            cache_read_tokens: 0,
            cache_creation_tokens: 0,
        })))
    }
}

#[tokio::test]
async fn review_total_overrun_stops_the_batch_after_a_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    let policy = ReadPolicy::compile_rules(
        &root,
        None,
        Some(Preset::ReadOnly),
        None,
        &[Rule {
            action: Action::Delegate,
            pattern: None,
            effect: Effect::Ask,
        }],
    )
    .unwrap();
    let review = Arc::new(FixtureReview::default());
    let registry = Arc::new(
        Registry::new(&[root], policy, json!(true))
            .unwrap()
            .with_authorizer(review.clone()),
    );
    for (limit, exceeded) in [(60, true), (65, false)] {
        let harness = Harness::new(
            Arc::new(scripted(vec![vec![
                call("reviewed", "delegate", json!({"task":"Inspect a fixture"})),
                submit("submit", json!({"answer":42})),
                end(Finish::ToolCalls),
            ]])),
            registry.clone(),
            Settings::resolve(&json!({}), &json!({}), &json!({"maxTotalTokens":limit})).unwrap(),
        )
        .with_delegation()
        .with_hook(review.clone());
        let trace = Trace::default();
        let (_, steer) = mpsc::channel(1);
        let report = harness
            .run(work(true), &trace, steer, CancellationToken::new())
            .await;
        if exceeded {
            assert!(matches!(report.outcome, Err(Failure::TokenLimit)));
        } else {
            assert!(report.outcome.unwrap().submission.is_some());
        }
        assert_eq!(
            trace
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry.starts_with("submit ")),
            !exceeded,
        );
    }
}

#[tokio::test]
async fn failed_batch_retains_completed_tool_results() {
    let (dir, harness) = delegation_fixture(
        vec![
            vec![
                call(
                    "first",
                    "write",
                    json!({"path":"notes.txt","content":"fixture contents"}),
                ),
                call("child", "delegate", json!({"task":"Inspect the workspace"})),
                submit("submit", json!({"answer":42})),
                end(Finish::ToolCalls),
            ],
            vec![Chunk::Text("Child reply".into()), end(Finish::Stop)],
        ],
        json!({"maxTotalTokens":30}),
    );
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .run(work(true), &(), steer, CancellationToken::new())
        .await;
    assert!(matches!(report.outcome, Err(Failure::TokenLimit)));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("notes.txt")).unwrap(),
        "fixture contents"
    );
    assert!(matches!(
        &report.messages[1..],
        [Message::Assistant { calls, .. }, Message::Tool { call_id, content },
         Message::Tool { call_id: child, content: child_result },
         Message::Tool { call_id: submit, content: submit_result }]
        if calls.len() == 3 && call_id == "first" && content.contains("Applied 1 file change")
            && child == "child" && submit == "submit"
            && child_result.contains("before a tool result") && submit_result.contains("before a tool result")
    ));
}

#[tokio::test]
async fn child_local_turn_limit_is_a_recoverable_tool_error() {
    let mut turns = vec![vec![
        call("child", "delegate", json!({"task":"Inspect the workspace"})),
        end(Finish::ToolCalls),
    ]];
    for turn in 1..=8 {
        turns.push(vec![
            call(&format!("read-{turn}"), "list", json!({})),
            end(Finish::ToolCalls),
        ]);
    }
    turns.push(vec![
        submit("submit", json!({"answer":42})),
        end(Finish::ToolCalls),
    ]);
    let (_dir, harness) = delegation_fixture(turns, json!({"maxTurns":10, "maxTotalTokens":200}));
    let trace = Trace::default();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .run(work(true), &trace, steer, CancellationToken::new())
        .await;
    assert_eq!(
        report.outcome.unwrap().submission,
        Some(json!({"results":{"answer":42}}))
    );
    assert_eq!(
        *trace.0.lock().unwrap(),
        [
            r#"child {"error":"Sub-agent failed"}"#,
            "submit Result accepted.",
        ]
    );
}

#[tokio::test]
async fn child_provider_failure_that_exhausts_tokens_blocks_parent_submission() {
    let (_dir, harness) = delegation_fixture(
        vec![
            vec![
                call("child", "delegate", json!({"task":"Inspect the workspace"})),
                submit("submit", json!({"answer":42})),
                end(Finish::ToolCalls),
            ],
            vec![end(Finish::Filtered)],
        ],
        json!({"maxTotalTokens":100}),
    );
    let trace = Trace::default();
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .run(work(true), &trace, steer, CancellationToken::new())
        .await;
    assert!(matches!(report.outcome, Err(Failure::TokenLimit)));
    assert!(trace.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn credential_in_signed_replay_rejects_the_reply() {
    const KEY: &str = "provider-fixture-key";
    let work = work(false);
    let replay = serde_json::from_value(json!({
        "format": "Anthropic",
        "model": work.model,
        "items": [{"type":"thinking","thinking":"private reasoning","signature":KEY}]
    }))
    .unwrap();
    let (_dir, harness) = fixture(
        vec![vec![
            Chunk::Replay(replay),
            submit("submit", json!({"answer":42})),
            end(Finish::ToolCalls),
        ]],
        json!({}),
    );
    let (_, steer) = mpsc::channel(1);
    let report = harness
        .with_provider_credential(Some(KEY.into()))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert!(matches!(
        report.outcome,
        Err(Failure::Provider(ProviderError::Invalid(_)))
    ));
    assert_eq!(report.messages.len(), 1);
    assert!(!serde_json::to_string(&report.messages)
        .unwrap()
        .contains(KEY));
}

#[tokio::test(start_paused = true)]
async fn stop_bounds_pending_steer_settlement() {
    struct PendingSettlement(CancellationToken);

    #[async_trait]
    impl WorkPort for PendingSettlement {
        async fn settle_steer(
            &self,
            id: &str,
            status: argentic_harness::providers::DeliveryStatus,
        ) -> Result<(), Failure> {
            assert_eq!(id, "offer");
            assert_eq!(
                status,
                argentic_harness::providers::DeliveryStatus::Accepted
            );
            self.0.cancel();
            std::future::pending().await
        }
    }

    let (_dir, harness) = fixture(
        vec![vec![end(Finish::Stop)], vec![end(Finish::Stop)]],
        json!({"cancelGraceMs": 10}),
    );
    let (send, steer) = mpsc::channel(1);
    let (consumed, receipt) = oneshot::channel();
    send.try_send(Steer {
        id: "offer".into(),
        reference: None,
        message: "accepted steer".into(),
        consumed,
    })
    .unwrap();
    let cancel = CancellationToken::new();
    let port = PendingSettlement(cancel.clone());
    let report = tokio::time::timeout(
        Duration::from_millis(100),
        harness.run(work(false), &port, steer, cancel),
    )
    .await
    .expect("Stop must return within the cancellation grace period");
    assert!(matches!(report.outcome, Err(Failure::Cancelled)));
    assert!(report.physical_end.forced());
    assert!(receipt.await.is_err());
    assert!(report
        .messages
        .contains(&Message::User("accepted steer".into())));
}

#[tokio::test]
async fn steer_is_not_acknowledged_or_retained_when_the_next_request_fails() {
    let (_dir, harness) = fixture(vec![vec![end(Finish::Stop)]], json!({}));
    let (send, steer) = mpsc::channel(1);
    let (consumed, receipt) = oneshot::channel();
    send.send(Steer {
        reference: None,
        id: "offer".into(),
        message: "steer to retry".into(),
        consumed,
    })
    .await
    .unwrap();
    let trace = Trace::default();
    let report = harness
        .run(work(false), &trace, steer, CancellationToken::new())
        .await;
    assert!(matches!(
        report.outcome,
        Err(Failure::Provider(ProviderError::Invalid(_)))
    ));
    assert!(receipt.await.is_err());
    assert!(!report
        .messages
        .contains(&Message::User("steer to retry".into())));
    assert!(trace.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn steer_admitted_after_missing_submission_can_request_a_tool() {
    struct SteerDuringSummary {
        recording: RecordingProvider,
        send: mpsc::Sender<Steer>,
        late: Mutex<Option<Steer>>,
    }

    #[async_trait]
    impl Provider for SteerDuringSummary {
        async fn stream(
            &self,
            request: Request<'_>,
            cancel: CancellationToken,
        ) -> Result<ProviderStream, ProviderError> {
            if request.tools.is_empty() {
                assert_eq!(request.tool_choice, ToolChoice::Auto);
                self.send
                    .try_send(self.late.lock().unwrap().take().unwrap())
                    .unwrap();
            }
            if request.messages.last()
                == Some(&Message::User("Read notes.txt before you submit.".into()))
            {
                assert_eq!(request.tool_choice, ToolChoice::Auto);
                assert!(request.tools.iter().any(|tool| tool.name == "read"));
            } else if let Some(Message::Tool { call_id, content }) = request.messages.last() {
                if call_id == "read-notes" {
                    assert!(content.contains("result is 42"));
                    assert_eq!(request.tool_choice, ToolChoice::Auto);
                }
            } else if matches!(request.messages.last(), Some(Message::User(text))
                if text.contains("SUBMISSION_REQUIRED"))
            {
                assert_eq!(
                    request.tool_choice,
                    ToolChoice::Named("agsdl_submit_result")
                );
            }
            self.recording.stream(request, cancel).await
        }
    }

    let (send, steer) = mpsc::channel(1);
    let (late, late_receipt) = oneshot::channel();
    let provider = Arc::new(SteerDuringSummary {
        recording: RecordingProvider {
            requests: Arc::new(Requests::default()),
            provider: scripted(vec![
                vec![Chunk::Text("recent ".repeat(1800)), end(Finish::Stop)],
                vec![Chunk::Text("Earlier evidence".into()), end(Finish::Stop)],
                vec![
                    call("read-notes", "read", json!({"path":"notes.txt"})),
                    end(Finish::ToolCalls),
                ],
                vec![end(Finish::Stop)],
                vec![submit("done", json!({"answer":42})), end(Finish::ToolCalls)],
            ]),
        },
        send,
        late: Mutex::new(Some(Steer {
            id: "late".into(),
            message: "Read notes.txt before you submit.".into(),
            reference: None,
            consumed: late,
        })),
    });
    let (_dir, harness) = fixture_with_provider(
        provider,
        json!({"maxOutputTokens":256,"compaction":{"threshold":0.60,"keepRecentTurns":1}}),
    );
    let mut work = work(true);
    let mut older = recorded_tool_turn("old", "earlier evidence".into());
    if let Message::Assistant { text, .. } = &mut older[0] {
        *text = "old ".repeat(3000);
    }
    work.messages.extend(older);
    let report = harness
        .with_context_window(Some(8192))
        .run(work, &(), steer, CancellationToken::new())
        .await;
    assert!(report.outcome.is_ok(), "{:?}", report.outcome);
    late_receipt.await.unwrap();
}

#[tokio::test]
#[ignore = "explicit performance measurement"]
async fn scripted_turn_overhead() {
    const TURNS: usize = 256;
    let (_dir, harness) = fixture(
        vec![vec![Chunk::Text("done".into()), end(Finish::Stop)]; TURNS],
        json!({}),
    );
    let mut samples = Vec::with_capacity(TURNS);
    for _ in 0..TURNS {
        let work = work(false);
        let (_sender, steers) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        let start = std::time::Instant::now();
        let report = harness.run(work, &(), steers, cancel).await;
        samples.push(start.elapsed());
        assert!(report.outcome.is_ok());
    }
    samples.sort_unstable();
    let median = samples[TURNS / 2];
    println!("scripted harness turn: {median:?} median over {TURNS} turns");
    // Hosted CI runners only report timings; controlled runners enforce them.
    if std::env::var_os("ARGENTIC_PERF_REPORT_ONLY").is_none() {
        assert!(median < Duration::from_millis(5));
    }
}
