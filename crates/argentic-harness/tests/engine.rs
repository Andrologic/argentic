use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use argentic_harness::{
    config::Settings,
    policy::{Preset, ReadPolicy},
    providers::{
        Chunk, Finish, Message, Provider, ProviderError, ProviderStream, Request, ScriptedProvider,
        ToolCall, Usage,
    },
    r#loop::{Event, Failure, Harness, Hook, Steer, Work, WorkPort},
    tools::Registry,
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
async fn steer_is_not_acknowledged_or_retained_when_the_next_request_fails() {
    let (_dir, harness) = fixture(vec![vec![end(Finish::Stop)]], json!({}));
    let (send, steer) = mpsc::channel(1);
    let (consumed, receipt) = oneshot::channel();
    send.send(Steer {
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
