use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use argentic_harness::providers::{
    ChatCompletions, Chunk, Finish, Message, ModelName, Provider, ProviderError, Request,
    TurnLimits,
};
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderValue, StatusCode},
    response::Response,
    routing::post,
    Json, Router,
};
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

type Reply = (StatusCode, Option<&'static str>, Vec<&'static [u8]>);
type Log = Arc<Mutex<Vec<(tokio::time::Instant, Value)>>>;

struct Mock {
    client: ChatCompletions,
    requests: Log,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Mock {
    async fn start(replies: Vec<Reply>) -> Self {
        type StateData = (Log, Arc<Mutex<VecDeque<Reply>>>);
        async fn reply(
            State((requests, replies)): State<StateData>,
            Json(body): Json<Value>,
        ) -> Response {
            requests
                .lock()
                .unwrap()
                .push((tokio::time::Instant::now(), body));
            let reply = replies.lock().unwrap().pop_front();
            let (status, retry_after, fragments) = reply.expect("unexpected HTTP request");
            let chunks = stream::iter(fragments).then(|fragment| async move {
                tokio::task::yield_now().await;
                Ok::<_, std::convert::Infallible>(Bytes::from_static(fragment))
            });
            let mut response = Response::new(Body::from_stream(chunks));
            *response.status_mut() = status;
            response.headers_mut().insert(
                "content-type",
                HeaderValue::from_static(if status.is_success() {
                    "text/event-stream"
                } else {
                    "application/json"
                }),
            );
            if let Some(value) = retry_after {
                response
                    .headers_mut()
                    .insert("retry-after", HeaderValue::from_static(value));
            }
            response
        }
        let requests = Log::default();
        let app = Router::new()
            .route("/v1/chat/completions", post(reply))
            .with_state((requests.clone(), Arc::new(Mutex::new(replies.into()))));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binding the mock SSE server");
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = ChatCompletions::new(
            "openai",
            format!("http://{addr}/v1/chat/completions")
                .parse()
                .unwrap(),
            None,
        )
        .unwrap();
        Self {
            client,
            requests,
            server,
        }
    }

    async fn request(&self, effort: Option<&str>) -> Result<Vec<Chunk>, ProviderError> {
        self.request_with_limits(effort, TurnLimits::default())
            .await
    }

    async fn request_with_limits(
        &self,
        effort: Option<&str>,
        limits: TurnLimits,
    ) -> Result<Vec<Chunk>, ProviderError> {
        let model: ModelName = "openai/vendor/model#fast".parse().unwrap();
        assert_eq!(model.variant(), Some("fast"));
        self.client
            .stream(
                Request {
                    model: &model,
                    messages: &[Message::User("hello".into())],
                    reasoning_effort: effort,
                    tools: &[],
                    max_output_tokens: 256,
                    limits,
                },
                CancellationToken::new(),
            )
            .await?
            .try_collect()
            .await
    }
}

const STOP: &[u8] = b"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";

#[tokio::test]
async fn assembles_tool_deltas_and_accepts_unknown_frame_fields() {
    let mock = Mock::start(vec![(StatusCode::OK, None, vec![
        b"\xef\xbb\xbf: heartbeat\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"caf\xc3",
        b"\xa9\",\"tool_calls\":[{\"index\":0,\"id\":\"call_",
        b"\",\"function\":{\"name\":\"re\",\"arguments\":\"{\\\"pa\"}}]}}], \"unknown\":true}\r",
        b"\n\r\ndata: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"17\",\"function\":{\"name\":\"ad\",\"arguments\":\"th\\\":\\\"x.txt\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\r\r",
        b"data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":9},\n",
        b"data: \"ignored\":null}\n\ndata: [DONE]\n\n",
    ])]).await;
    let output = mock.request(Some("medium")).await.unwrap();
    assert_eq!(output[0], Chunk::Text("café".into()));
    assert!(
        matches!(&output[1], Chunk::ToolCall(call) if call.id == "call_17" && call.name == "read" && call.arguments == r#"{"path":"x.txt"}"#)
    );
    assert!(
        matches!(output[2], Chunk::End { finish: Finish::ToolCalls, usage: Some(usage) } if usage.prompt_tokens == 7 && usage.completion_tokens == 9)
    );
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests[0].1["model"], "vendor/model");
    assert_eq!(requests[0].1["reasoning_effort"], "medium");
    assert_eq!(requests[0].1["max_completion_tokens"], 256);
}

#[tokio::test]
async fn effort_rejection_only_changes_the_current_request() {
    let mock = Mock::start(vec![
        (
            StatusCode::BAD_REQUEST,
            None,
            vec![br#"{"error":{"message":"Unknown parameter: reasoning_effort"}}"#],
        ),
        (StatusCode::OK, None, vec![STOP]),
        (StatusCode::OK, None, vec![STOP]),
    ])
    .await;
    assert!(mock.request(Some("high")).await.is_ok());
    assert!(mock.request(Some("high")).await.is_ok());
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].1["reasoning_effort"], "high");
    assert!(requests[1].1.get("reasoning_effort").is_none());
    assert_eq!(requests[2].1["reasoning_effort"], "high");
}

#[tokio::test]
async fn retries_429_after_retry_after_and_transient_5xx() {
    let mock = Mock::start(vec![
        (
            StatusCode::TOO_MANY_REQUESTS,
            Some("1"),
            vec![br#"{"error":"slow down"}"#],
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            None,
            vec![br#"{"error":"try again"}"#],
        ),
        (StatusCode::OK, None, vec![STOP]),
    ])
    .await;
    assert!(mock.request(None).await.is_ok());
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].0.duration_since(requests[0].0) >= Duration::from_secs(1));
    assert!(requests[2].0.duration_since(requests[1].0) >= Duration::from_millis(500));
}

#[tokio::test]
async fn a_truncated_stream_cannot_return_a_completed_tool_call() {
    let mock = Mock::start(vec![(StatusCode::OK, None, vec![
        b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call\",\"function\":{\"name\":\"read\",\"arguments\":\"{}\"}}]}}]}\n\n",
    ])]).await;
    let result = mock.request(None).await;
    assert!(
        matches!(result, Err(ProviderError::Invalid(message)) if message.contains("finish reason"))
    );
}

#[tokio::test]
async fn effort_error_after_visible_text_does_not_replay_the_turn() {
    let mock = Mock::start(vec![(StatusCode::OK, None, vec![
        b"data: {\"choices\":[{\"delta\":{\"content\":\"started\"}}]}\n\ndata: {\"error\":{\"message\":\"Unsupported parameter: reasoning_effort\"}}\n\n"
    ])]).await;
    let model = "openai/model".parse().unwrap();
    let mut output = mock
        .client
        .stream(
            Request {
                model: &model,
                messages: &[],
                reasoning_effort: Some("high"),
                tools: &[],
                max_output_tokens: 100,
                limits: TurnLimits::default(),
            },
            CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(
        output.next().await.unwrap().unwrap(),
        Chunk::Text("started".into())
    );
    assert!(output.next().await.unwrap().is_err());
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn comment_bytes_count_toward_the_turn_budget() {
    let mock = Mock::start(vec![(
        StatusCode::OK,
        None,
        vec![b": keepalive\n\n", b": keepalive\n\n", b": keepalive\n\n"],
    )])
    .await;
    let error = mock
        .request_with_limits(
            None,
            TurnLimits {
                response_bytes: 25,
                ..TurnLimits::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::ResponseTooLarge);
}

#[tokio::test]
async fn turn_deadline_covers_retry_delays_and_keepalive_bodies() {
    let mock = Mock::start(vec![(
        StatusCode::TOO_MANY_REQUESTS,
        Some("2"),
        vec![b"retry later"],
    )])
    .await;
    let error = mock
        .request_with_limits(
            None,
            TurnLimits {
                timeout: Duration::from_millis(300),
                ..TurnLimits::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::TurnTimeout);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async {
                Response::new(Body::from_stream(stream::repeat(()).then(|()| async {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok::<_, std::convert::Infallible>(Bytes::from_static(b": keepalive\n\n"))
                })))
            }),
        );
        axum::serve(listener, app).await.unwrap();
    });
    let mock = Mock {
        client: ChatCompletions::new(
            "openai",
            format!("http://{addr}/v1/chat/completions")
                .parse()
                .unwrap(),
            None,
        )
        .unwrap(),
        requests: Log::default(),
        server,
    };
    let error = tokio::time::timeout(
        Duration::from_secs(3),
        mock.request_with_limits(
            None,
            TurnLimits {
                timeout: Duration::from_millis(300),
                ..TurnLimits::default()
            },
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error, ProviderError::TurnTimeout);
}
