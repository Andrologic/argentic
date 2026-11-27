use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use argentic_harness::providers::{
    ChatCompletions, Chunk, DeliveryProgress, DeliveryStatus, Finish, HttpClient, Message,
    ModelName, Provider, ProviderError, Request, TurnLimits,
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
        self.request_with_delivery(effort, limits, DeliveryProgress::default())
            .await
    }

    async fn request_with_delivery(
        &self,
        effort: Option<&str>,
        limits: TurnLimits,
        delivery: DeliveryProgress,
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
                    tool_choice: Default::default(),
                    tool_choice_rejected: None,
                    max_output_tokens: 256,
                    limits,
                    delivery,
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
async fn completed_sse_event_precedes_a_framing_error() {
    let mock = Mock::start(vec![(
        StatusCode::OK,
        None,
        vec![b"data: {\"choices\":[{\"delta\":{\"content\":\"started\"}}]}\n\ndata: \xff\n\n"],
    )])
    .await;
    let model = "openai/model".parse().unwrap();
    let mut output = mock
        .client
        .stream(
            Request {
                model: &model,
                messages: &[],
                reasoning_effort: Some("high"),
                tools: &[],
                tool_choice: Default::default(),
                tool_choice_rejected: None,
                max_output_tokens: 100,
                limits: TurnLimits::default(),
                delivery: Default::default(),
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
                tool_choice: Default::default(),
                tool_choice_rejected: None,
                max_output_tokens: 100,
                limits: TurnLimits::default(),
                delivery: Default::default(),
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

#[tokio::test]
async fn http_and_sse_errors_redact_provider_supplied_content() {
    const SECRET: &str = "private-provider-marker";
    let mock = Mock::start(vec![
        (
            StatusCode::UNAUTHORIZED,
            None,
            vec![br#"{"error":"private-provider-marker"}"#],
        ),
        (
            StatusCode::OK,
            None,
            vec![b"data: {\"error\":\"private-provider-marker\"}\n\n"],
        ),
        (
            StatusCode::OK,
            None,
            vec![b"data: {\"choices\":[{\"finish_reason\":\"private-provider-marker\"}]}\n\n"],
        ),
    ])
    .await;
    let http = mock.request(None).await.unwrap_err();
    let sse = mock.request(None).await.unwrap_err();
    let invalid = mock.request(None).await.unwrap_err();
    for error in [&http, &sse, &invalid] {
        assert!(!format!("{error} {error:?}").contains(SECRET));
    }
    assert!(matches!(http, ProviderError::Http { status: 401, .. }));
    assert!(matches!(sse, ProviderError::Stream { status: 200, .. }));
}

#[tokio::test]
async fn distinguishes_explicit_refusal_from_missing_headers_after_send() {
    let mock = Mock::start(vec![(StatusCode::FORBIDDEN, None, vec![b"refused"])]).await;
    let delivery = DeliveryProgress::default();
    let error = mock
        .request_with_delivery(None, TurnLimits::default(), delivery.clone())
        .await
        .unwrap_err();
    assert!(matches!(error, ProviderError::Http { status: 403, .. }));
    assert_eq!(delivery.status(), DeliveryStatus::NotDelivered);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|Json(_): Json<Value>| async { std::future::pending::<Response>().await }),
        );
        axum::serve(listener, app).await.unwrap();
    });
    let stalled = Mock {
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
    let delivery = DeliveryProgress::default();
    let error = stalled
        .request_with_delivery(
            None,
            TurnLimits {
                timeout: Duration::from_millis(300),
                ..TurnLimits::default()
            },
            delivery.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::TurnTimeout);
    assert_eq!(delivery.status(), DeliveryStatus::Uncertain);
}

#[test]
fn work_providers_share_connections_only_within_one_runtime() {
    use axum::{extract::ConnectInfo, http::HeaderMap};
    use std::net::SocketAddr;

    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let service = Router::new().route(
        "/chat",
        post(move |ConnectInfo(peer): ConnectInfo<SocketAddr>, headers: HeaderMap| {
            let observed = observed.clone();
            async move {
                observed.lock().unwrap().push((
                    peer,
                    headers["authorization"].to_str().unwrap().to_owned(),
                ));
                (
                    [("content-type", "text/event-stream")],
                    concat!(
                        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n",
                        "data: [DONE]\n\n"
                    ),
                )
            }
        }),
    );
    let server = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let endpoint = server.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/chat", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(
                listener,
                service.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });
        endpoint
    });
    let model = "test/model".parse().unwrap();
    let request = async |pool: HttpClient, key: &str| {
        let work =
            ChatCompletions::with_http_client("test", endpoint.parse().unwrap(), Some(key), pool)
                .unwrap();
        let chunks: Vec<_> = work
            .stream(
                Request {
                    model: &model,
                    reasoning_effort: None,
                    messages: &[],
                    tools: &[],
                    tool_choice: Default::default(),
                    tool_choice_rejected: None,
                    max_output_tokens: 128,
                    limits: TurnLimits {
                        timeout: Duration::from_secs(3),
                        ..TurnLimits::default()
                    },
                    delivery: DeliveryProgress::default(),
                },
                CancellationToken::new(),
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(matches!(
            chunks.last(),
            Some(Chunk::End {
                finish: Finish::Stop,
                ..
            })
        ));
    };
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    };
    let first = runtime();
    let first_pool = HttpClient::new().unwrap();
    first.block_on(request(first_pool.clone(), "first"));
    first.block_on(request(first_pool.clone(), "second"));

    // The first runtime and its pooled connection stay alive while paused.
    let second = runtime();
    let second_pool = HttpClient::new().unwrap();
    second.block_on(request(second_pool, "third"));

    let observed = requests.lock().unwrap();
    assert_eq!(observed.len(), 3);
    assert_eq!(observed[0].0, observed[1].0);
    assert_ne!(observed[1].0, observed[2].0);
    assert_eq!(observed[0].1, "Bearer first");
    assert_eq!(observed[1].1, "Bearer second");
    assert_eq!(observed[2].1, "Bearer third");
}
