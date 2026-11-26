use std::{
    collections::VecDeque,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use argentic_harness::providers::{
    AnthropicMessages, Chunk, DeliveryProgress, DeliveryStatus, Finish, Message, OpenAiResponses,
    Provider, ProviderError, Request, ToolDefinition, TurnLimits, Usage,
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
    Json, Router,
};
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
enum Protocol {
    Messages,
    Responses,
}

impl Protocol {
    fn path(self) -> &'static str {
        match self {
            Self::Messages => "/v1/messages",
            Self::Responses => "/v1/responses",
        }
    }

    fn model(self) -> &'static str {
        match self {
            Self::Messages => "fixture/claude-sonnet-4-6#variant",
            Self::Responses => "fixture/gpt-5.4#variant",
        }
    }

    fn events(self, events: impl IntoIterator<Item = Value>) -> String {
        events
            .into_iter()
            .map(|event| {
                format!(
                    "event: {}\r\ndata: {event}\r\n\r\n",
                    event["type"].as_str().unwrap()
                )
            })
            .collect()
    }

    fn text(self) -> Vec<Value> {
        match self {
            Self::Messages => vec![
                json!({"type":"message_start","message":{"id":"msg_1","content":[],"usage":{
                    "input_tokens":11,"output_tokens":1,"cache_creation_input_tokens":3,"cache_read_input_tokens":5}}}),
                json!({"type":"ping"}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"café"}}),
                json!({"type":"content_block_stop","index":0}),
            ],
            Self::Responses => vec![
                json!({"type":"response.created","response":{"id":"resp_1","status":"in_progress","output":[]}}),
                json!({"type":"response.output_item.added","output_index":0,
                    "item":{"type":"message","id":"msg_1","role":"assistant","status":"in_progress","content":[]}}),
                json!({"type":"response.output_text.delta","output_index":0,"item_id":"msg_1","content_index":0,"delta":"café"}),
                json!({"type":"response.output_item.done","output_index":0,
                    "item":{"type":"message","id":"msg_1","role":"assistant","status":"completed",
                        "content":[{"type":"output_text","text":"café","annotations":[]}]}}),
            ],
        }
    }

    fn end(self, tools: bool) -> Vec<Value> {
        match self {
            Self::Messages => vec![
                json!({"type":"message_delta","delta":{"stop_reason":if tools {"tool_use"} else {"end_turn"},"stop_sequence":null},"usage":{"output_tokens":7}}),
                json!({"type":"message_stop"}),
            ],
            Self::Responses => vec![
                json!({"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{
                    "input_tokens":19,"output_tokens":7,"input_tokens_details":{"cached_tokens":5}}}}),
            ],
        }
    }

    fn success(self) -> Response {
        sse(self.events(self.text().into_iter().chain(self.end(false))))
    }

    fn failure(self) -> Value {
        match self {
            Self::Messages => {
                json!({"type":"error","error":{"type":"overloaded_error","message":"private-response"}})
            }
            Self::Responses => json!({"type":"response.failed","response":{"status":"failed",
                "error":{"code":"server_error","message":"private-response"}}}),
        }
    }

    fn reasoning(self) -> Value {
        match self {
            Self::Messages => {
                json!({"type":"thinking","thinking":"plan","signature":"private-signature"})
            }
            Self::Responses => json!({"type":"reasoning","id":"rs_1",
                "summary":[{"type":"summary_text","text":"plan"}],"encrypted_content":"private-encrypted"}),
        }
    }

    fn tool_turn(self) -> Response {
        let mut events = match self {
            Self::Messages => vec![
                json!({"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":"","signature":""}}),
                json!({"type":"content_block_delta","index":1,"delta":{"type":"thinking_delta","thinking":"plan"}}),
                json!({"type":"content_block_delta","index":1,"delta":{"type":"signature_delta","signature":"private-signature"}}),
                json!({"type":"content_block_stop","index":1}),
            ],
            Self::Responses => vec![
                json!({"type":"response.output_item.added","output_index":1,
                    "item":{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":null}}),
                json!({"type":"response.reasoning_summary_text.delta","output_index":1,
                    "item_id":"rs_1","summary_index":0,"delta":"plan"}),
                json!({"type":"response.output_item.done","output_index":1,"item":self.reasoning()}),
            ],
        };
        for (index, id) in [(2, "call_1"), (3, "call_2")] {
            events.extend(match self {
                Self::Messages => vec![
                    json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":id,"name":"read","input":{}}}),
                    json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}),
                    json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":"\"memo.txt\"}"}}),
                    json!({"type":"content_block_stop","index":index}),
                ],
                Self::Responses => vec![
                    json!({"type":"response.output_item.added","output_index":index,
                        "item":{"type":"function_call","id":format!("fc_{index}"),"call_id":id,"name":"read","arguments":"","status":"in_progress"}}),
                    json!({"type":"response.function_call_arguments.delta","output_index":index,"delta":"{\"path\":"}),
                    json!({"type":"response.function_call_arguments.delta","output_index":index,"delta":"\"memo.txt\"}"}),
                    json!({"type":"response.function_call_arguments.done","output_index":index,"name":"read","arguments":"{\"path\":\"memo.txt\"}"}),
                    json!({"type":"response.output_item.done","output_index":index,
                        "item":{"type":"function_call","id":format!("fc_{index}"),"call_id":id,"name":"read","arguments":"{\"path\":\"memo.txt\"}","status":"completed"}}),
                ],
            });
        }
        sse(self.events(self.text().into_iter().chain(events).chain(self.end(true))))
    }
}

fn fragments(data: String) -> impl futures::Stream<Item = Result<Bytes, Infallible>> {
    // Split CRLF and the UTF-8 bytes in "café", independently of event boundaries.
    let bytes = Bytes::from(data);
    stream::iter(
        (0..bytes.len())
            .step_by(3)
            .map(move |start| Ok(bytes.slice(start..(start + 3).min(bytes.len())))),
    )
}

fn sse(data: String) -> Response {
    let mut response = Response::new(Body::from_stream(fragments(data)));
    response
        .headers_mut()
        .insert("content-type", "text/event-stream".parse().unwrap());
    response
}

type Requests = Arc<Mutex<Vec<(HeaderMap, Value, tokio::time::Instant)>>>;
type Script = Arc<Mutex<VecDeque<Response>>>;

struct Mock {
    provider: Box<dyn Provider>,
    requests: Requests,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Mock {
    async fn start(protocol: Protocol, replies: Vec<Response>) -> Self {
        async fn send(
            State((requests, replies)): State<(Requests, Script)>,
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Response {
            requests
                .lock()
                .unwrap()
                .push((headers, body, tokio::time::Instant::now()));
            replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected request")
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}{}",
            listener.local_addr().unwrap(),
            protocol.path()
        )
        .parse()
        .unwrap();
        let requests = Requests::default();
        let app = Router::new()
            .route(protocol.path(), post(send))
            .with_state((requests.clone(), Arc::new(Mutex::new(replies.into()))));
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let provider: Box<dyn Provider> = match protocol {
            Protocol::Messages => Box::new(
                AnthropicMessages::new("fixture", url, Some("fixture-key"))
                    .unwrap()
                    .with_prompt_caching(true)
                    .with_attempt_effort_fallback(),
            ),
            Protocol::Responses => Box::new(
                OpenAiResponses::new("fixture", url, Some("fixture-key"))
                    .unwrap()
                    .with_attempt_effort_fallback(),
            ),
        };
        Self {
            provider,
            requests,
            task,
        }
    }
}

async fn run(
    mock: &Mock,
    protocol: Protocol,
    messages: &[Message],
    tools: &[ToolDefinition],
) -> Vec<Chunk> {
    mock.provider
        .stream(
            Request {
                model: &protocol.model().parse().unwrap(),
                messages,
                tools,
                max_output_tokens: 4096,
                reasoning_effort: Some("medium"),
                delivery: DeliveryProgress::default(),
                limits: TurnLimits::default(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap()
}

async fn round_trip(protocol: Protocol) {
    let mock = Mock::start(protocol, vec![protocol.tool_turn(), protocol.success()]).await;
    let mut history = vec![
        Message::System("stable".into()),
        Message::User("Read the memo".into()),
    ];
    let tools = vec![ToolDefinition {
        name: "read".into(),
        description: "Read a file".into(),
        parameters: json!({"type":"object","properties":{"path":{"type":"string"}}}),
    }];
    let chunks = run(&mock, protocol, &history, &tools).await;
    assert_eq!(chunks[0], Chunk::Text("café".into()));
    assert_eq!(chunks[1], Chunk::Reasoning("plan".into()));
    let calls = chunks[2..4]
        .iter()
        .map(|chunk| {
            let Chunk::ToolCall(call) = chunk else {
                panic!("{chunk:?}")
            };
            assert_eq!(call.arguments, "{\"path\":\"memo.txt\"}");
            call.clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        calls.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(),
        ["call_1", "call_2"]
    );
    let Chunk::Replay(replay) = &chunks[4] else {
        panic!("missing replay")
    };
    assert_eq!(format!("{replay:?}"), "Replay([redacted])");
    assert_eq!(
        chunks[5],
        Chunk::End {
            finish: Finish::ToolCalls,
            usage: Some(Usage {
                prompt_tokens: 19,
                completion_tokens: 7,
                cache_read_tokens: 5,
                cache_creation_tokens: if matches!(protocol, Protocol::Messages) {
                    3
                } else {
                    0
                }
            }),
        }
    );
    history.push(Message::Assistant {
        text: "café".into(),
        reasoning: "plan".into(),
        calls: calls.clone(),
        replay: Some(replay.clone()),
    });
    history.extend(calls.into_iter().map(|call| Message::Tool {
        call_id: call.id,
        content: "memo".into(),
    }));
    history.push(Message::User("continue".into()));
    let next = run(&mock, protocol, &history, &tools).await;
    assert_eq!(next[0], Chunk::Text("café".into()));
    let requests = mock.requests.lock().unwrap();
    let (headers, first, _) = &requests[0];
    let (_, next, _) = &requests[1];
    match protocol {
        Protocol::Messages => {
            assert_eq!(headers["x-api-key"], "fixture-key");
            assert_eq!(headers["anthropic-version"], "2023-06-01");
            assert_eq!(first["model"], "claude-sonnet-4-6");
            assert_eq!(first["max_tokens"], 4096);
            assert_eq!(
                first["thinking"],
                json!({"type":"enabled","budget_tokens":2048})
            );
            assert_eq!(
                first["system"],
                json!([{"type":"text","text":"stable","cache_control":{"type":"ephemeral"}}])
            );
            assert_eq!(first["system"], next["system"]);
            assert_eq!(first["tools"][0]["input_schema"], tools[0].parameters);
            assert_eq!(next["messages"][1]["content"][1], protocol.reasoning());
            assert_eq!(
                next["messages"][1]["content"][2]["input"],
                json!({"path":"memo.txt"})
            );
            assert_eq!(
                next["messages"][2]["content"],
                json!([
                    {"type":"tool_result","tool_use_id":"call_1","content":"memo"},
                    {"type":"tool_result","tool_use_id":"call_2","content":"memo"},
                    {"type":"text","text":"continue"}
                ])
            );
        }
        Protocol::Responses => {
            assert_eq!(headers["authorization"], "Bearer fixture-key");
            assert_eq!(first["model"], "gpt-5.4");
            assert_eq!(first["max_output_tokens"], 4096);
            assert_eq!(first["store"], false);
            assert_eq!(first["include"], json!(["reasoning.encrypted_content"]));
            assert_eq!(first["reasoning"], json!({"effort":"medium"}));
            assert_eq!(
                first["input"][0],
                json!({"role":"system","content":"stable"})
            );
            assert_eq!(first["tools"][0]["parameters"], tools[0].parameters);
            assert_eq!(first["tools"][0]["type"], "function");
            assert_eq!(next["input"][3], protocol.reasoning());
            assert_eq!(next["input"][4]["call_id"], "call_1");
            assert_eq!(next["input"][4]["id"], "fc_2");
            assert_eq!(
                next["input"][7],
                json!({"type":"function_call_output","call_id":"call_2","output":"memo"})
            );
        }
    }
}

#[tokio::test]
async fn messages_round_trip() {
    round_trip(Protocol::Messages).await;
}

#[tokio::test]
async fn responses_round_trip() {
    round_trip(Protocol::Responses).await;
}

fn http_error(status: u16, retry_after: Option<&str>, body: &'static str) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::from_u16(status).unwrap();
    if let Some(value) = retry_after {
        response
            .headers_mut()
            .insert("retry-after", value.parse().unwrap());
    }
    response
}

async fn retries(protocol: Protocol) {
    let busy = http_error(529, Some("0"), "private-overload");
    let throttle = http_error(429, Some("1"), "private-rate-limit");
    let stream_overload = sse(protocol.events([protocol.failure()]));
    let effort = http_error(
        400,
        None,
        r#"{"error":{"message":"unsupported reasoning effort, private-response"}}"#,
    );
    let mock = Mock::start(
        protocol,
        vec![
            throttle,
            busy,
            stream_overload,
            effort,
            protocol.success(),
            protocol.success(),
        ],
    )
    .await;
    run(&mock, protocol, &[], &[]).await;
    run(&mock, protocol, &[], &[]).await;
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 6);
    assert!(requests[1].2 - requests[0].2 >= Duration::from_secs(1));
    let field = match protocol {
        Protocol::Messages => "thinking",
        Protocol::Responses => "reasoning",
    };
    assert!(requests[..4]
        .iter()
        .all(|(_, body, _)| body[field].is_object()));
    assert!(requests[4..]
        .iter()
        .all(|(_, body, _)| body.get(field).is_none()));
}

#[tokio::test]
async fn messages_retry() {
    retries(Protocol::Messages).await;
}

#[tokio::test]
async fn responses_retry() {
    retries(Protocol::Responses).await;
}

async fn bounds_and_errors(protocol: Protocol) {
    let mut hanging = Response::new(Body::from_stream(
        fragments(protocol.events(protocol.text())).chain(stream::pending()),
    ));
    hanging
        .headers_mut()
        .insert("content-type", "text/event-stream".parse().unwrap());
    let remote_failure =
        sse(protocol.events(protocol.text().into_iter().chain([protocol.failure()])));
    let truncated = sse(protocol.events(protocol.text()));
    let mock = Mock::start(
        protocol,
        vec![
            remote_failure,
            http_error(401, None, "private-response fixture-key"),
            hanging,
            protocol.success(),
            truncated,
            Response::new(Body::from_stream(stream::pending::<
                Result<Bytes, Infallible>,
            >())),
        ],
    )
    .await;
    let model = protocol.model().parse().unwrap();
    let delivery = DeliveryProgress::default();
    let request = |limits| Request {
        model: &model,
        messages: &[],
        tools: &[],
        max_output_tokens: 4096,
        reasoning_effort: None,
        delivery: delivery.clone(),
        limits,
    };
    let cancel = CancellationToken::new();
    let mut output = mock
        .provider
        .stream(request(TurnLimits::default()), cancel.clone())
        .await
        .unwrap();
    assert_eq!(delivery.status(), DeliveryStatus::Accepted);
    assert_eq!(
        output.next().await.unwrap().unwrap(),
        Chunk::Text("café".into())
    );
    let error = output.next().await.unwrap().unwrap_err();
    assert!(matches!(error, ProviderError::Stream { status: 200, .. }));
    assert!(!format!("{error} {error:?}").contains("private-response"));
    assert_eq!(delivery.status(), DeliveryStatus::Accepted);
    let refusal_delivery = DeliveryProgress::default();
    let refused = Request {
        delivery: refusal_delivery.clone(),
        ..request(TurnLimits::default())
    };
    let error = match mock.provider.stream(refused, cancel.clone()).await {
        Ok(_) => panic!("expected HTTP refusal"),
        Err(error) => error,
    };
    assert!(matches!(error, ProviderError::Http { status: 401, .. }));
    assert_eq!(refusal_delivery.status(), DeliveryStatus::NotDelivered);
    let display = format!("{error} {error:?}");
    assert!(!display.contains("private-response") && !display.contains("fixture-key"));

    let mut output = mock
        .provider
        .stream(request(TurnLimits::default()), cancel.clone())
        .await
        .unwrap();
    assert_eq!(
        output.next().await.unwrap().unwrap(),
        Chunk::Text("café".into())
    );
    cancel.cancel();
    assert!(matches!(
        output.next().await,
        Some(Err(ProviderError::Cancelled))
    ));

    let limits = TurnLimits {
        response_bytes: 32,
        ..TurnLimits::default()
    };
    let error = mock
        .provider
        .stream(request(limits), CancellationToken::new())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::ResponseTooLarge);
    let error = mock
        .provider
        .stream(request(TurnLimits::default()), CancellationToken::new())
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(matches!(error, ProviderError::Invalid(_)));

    let header_delivery = DeliveryProgress::default();
    let limits = TurnLimits {
        timeout: Duration::from_millis(250),
        ..TurnLimits::default()
    };
    let without_body = Request {
        delivery: header_delivery.clone(),
        ..request(limits)
    };
    let (response, ()) = tokio::join!(
        mock.provider.stream(without_body, CancellationToken::new()),
        async {
            tokio::time::timeout(Duration::from_secs(2), async {
                while header_delivery.status() != DeliveryStatus::Accepted {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("2xx headers must settle delivery while the body is pending");
        }
    );
    let error = match response {
        Ok(stream) => stream.try_collect::<Vec<_>>().await.unwrap_err(),
        Err(error) => error,
    };
    assert_eq!(error, ProviderError::TurnTimeout);
    assert_eq!(mock.requests.lock().unwrap().len(), 6);
}

#[tokio::test]
async fn messages_bounds_and_errors() {
    bounds_and_errors(Protocol::Messages).await;
}

#[tokio::test]
async fn responses_bounds_and_errors() {
    bounds_and_errors(Protocol::Responses).await;
}

#[tokio::test]
async fn responses_incomplete_has_no_replay_or_tool_calls() {
    let protocol = Protocol::Responses;
    let replies = ["max_output_tokens", "content_filter"].map(|reason| {
        sse(protocol.events(protocol.text().into_iter().chain([
            json!({"type":"response.output_item.added","output_index":1,
                "item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"read","arguments":""}}),
            json!({"type":"response.incomplete","response":{"status":"incomplete",
                "incomplete_details":{"reason":reason}}}),
        ])))
    });
    let mock = Mock::start(protocol, replies.into()).await;
    for finish in [Finish::Length, Finish::Filtered] {
        assert_eq!(
            run(&mock, protocol, &[], &[]).await,
            [
                Chunk::Text("café".into()),
                Chunk::End {
                    finish,
                    usage: None
                }
            ]
        );
    }
}
