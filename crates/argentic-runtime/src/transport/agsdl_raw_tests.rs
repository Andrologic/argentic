use super::*;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use crate::agsdl_host_calls::tests::grant;
use crate::agsdl_host_calls::{
    execute,
    tests::{identity, TestHost},
    CallRequest, Invocation, LaunchGrant,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[test]
fn raw_results_preserve_numeric_lexemes_null_and_original_error_marker() {
    let raw = r#"{"content":[{"type":"text","text":"plain"},{"type":"image","data":"ignored"}],"isError":false,"structuredContent":{"large":9007199254740993,"exp":1e999999,"zero":-0}}"#;
    let r = receipt(raw.into()).unwrap();
    assert!(!r.is_error);
    assert_eq!(r.text, "plain");
    assert_eq!(r.raw_result_json, raw);
    assert_eq!(
        r.structured_content_json.unwrap(),
        r#"{"large":9007199254740993,"exp":1e999999,"zero":-0}"#
    );
    assert_eq!(
        receipt(r#"{"structuredContent":null,"isError":true}"#.into())
            .unwrap()
            .structured_content_json,
        Some("null".into())
    );
    assert!(receipt(r#"{"content":[],"isError":false,"isError":true}"#.into()).is_err());
    let text = "x".repeat(9 * 1024 * 1024);
    let raw = format!("{{\"content\":[{{\"type\":\"text\",\"text\":\"{text}\"}}]}}");
    assert_eq!(receipt(raw).unwrap().text.len(), text.len());
}

struct HttpFixture {
    requests: tokio::sync::Mutex<Vec<String>>,
    calls: AtomicUsize,
    mode: &'static str,
    answer_seen: tokio::sync::Notify,
}
async fn http_handler(
    axum::extract::State(state): axum::extract::State<Arc<HttpFixture>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let value: serde_json::Value = serde_json::from_str(&body).unwrap();
    state.requests.lock().await.push(body.clone());
    let id = value.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = value.get("method").and_then(|v| v.as_str()).unwrap_or("");
    if state.mode == "legacy-2025-06-18" {
        let version = headers
            .get("mcp-protocol-version")
            .and_then(|h| h.to_str().ok());
        if method == "initialize" {
            assert_eq!(value["params"]["protocolVersion"], "2025-11-25");
            if version.is_some() {
                return axum::http::StatusCode::BAD_REQUEST.into_response();
            }
        } else if version != Some("2025-06-18")
            || headers.get("mcp-session-id").and_then(|h| h.to_str().ok()) != Some("captured")
        {
            return axum::http::StatusCode::BAD_REQUEST.into_response();
        }
    }
    if method.is_empty() || method == "notifications/initialized" {
        if method.is_empty() {
            if state.mode == "rejected-human-response" {
                return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            state.answer_seen.notify_one();
        }
        return axum::http::StatusCode::ACCEPTED.into_response();
    }
    let result: String = match method {
        "initialize" if state.mode == "initialize-stall" => std::future::pending().await,
        "initialize" if state.mode == "invalid-version" => r#"{"protocolVersion":"unknown"}"#.into(),
        "tools/list" if state.mode == "list-stall" => std::future::pending().await,
        "initialize" if state.mode == "legacy-2025-06-18" => {
            r#"{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"synthetic","version":"1"}}"#.into()
        }
        "initialize" => r#"{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"synthetic","version":"1"}}"#.into(),
        "server/discover" => {
            assert!(value["params"]["_meta"].is_object());
            assert!(value["params"].get("protocolVersions").is_none());
            r#"{"resultType":"complete","supportedVersions":["2026-07-28"],"capabilities":{},"ttlMs":0,"cacheScope":"private"}"#.into()
        }
        "tools/call" => {
            let call = state.calls.fetch_add(1, Ordering::SeqCst);
            assert!(body.contains("9007199254740993"));
            if state.mode == "get-blocking"
                && tokio::time::timeout(std::time::Duration::from_secs(2), state.answer_seen.notified()).await.is_err() {
                    return axum::http::StatusCode::GATEWAY_TIMEOUT.into_response();
                }
            if state.mode == "sse-line-endings" {
                let chunks = [
                    "id: cursor-1\r".to_owned(),
                    "\nretry: 10\r: heartbeat\r".to_owned(),
                    "\ndata: {\r".to_owned(),
                    format!("data: \"jsonrpc\":\"2.0\",\"id\":{id},\n"),
                    "data: \"result\":{\"content\":[{\"type\":\"text\",\"text\":\"complete\"}]}}\r"
                        .to_owned(),
                    "\r".to_owned(),
                ];
                let body = axum::body::Body::from_stream(futures::stream::iter(
                    chunks.into_iter().map(Ok::<_, std::convert::Infallible>),
                ));
                return ([("content-type", "text/event-stream")], body).into_response();
            }
            if state.mode == "modern-push" {
                let raw = format!("data: {{\"jsonrpc\":\"2.0\",\"id\":\"server-human\",\"method\":\"elicitation/create\",\"params\":{{\"message\":\"Continue?\",\"requestedSchema\":{{\"type\":\"object\"}}}}}}\n\ndata: {{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"resultType\":\"complete\",\"content\":[]}}}}\n\n");
                return ([("content-type", "text/event-stream")], raw).into_response();
            }
            if matches!(state.mode, "resume" | "resume-partial") {
                let stream = if state.mode == "resume-partial" {
                    "id: cursor-1\nretry: 0\ndata:\n\nid: incomplete\ndata: {"
                } else {
                    "id: cursor-1\nretry: 0\ndata:\n\n"
                };
                return ([("content-type", "text/event-stream"), ("mcp-session-id", "captured")], stream).into_response();
            }
            let error = match state.mode {
                "rpc-error-null" => Some("null"),
                "rpc-error-string-code" => Some(r#"{"code":"-32603","message":"failed"}"#),
                "rpc-error-no-message" => Some(r#"{"code":-32603}"#),
                "rpc-error-valid" | "rpc-error-with-result" => {
                    Some(r#"{"code":-32603,"message":"failed"}"#)
                }
                _ => None,
            };
            if let Some(error) = error {
                let result = if state.mode == "rpc-error-with-result" {
                    r#","result":{"content":[]}"#
                } else {
                    ""
                };
                return (
                    [("content-type", "application/json")],
                    format!(r#"{{"jsonrpc":"2.0","id":{id},"error":{error}{result}}}"#),
                ).into_response();
            }
            if state.mode == "redirect" { return (axum::http::StatusCode::TEMPORARY_REDIRECT, [("location", "/other")]).into_response(); }
            if state.mode == "lost" { return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response(); }
            if matches!(state.mode, "modern" | "modern-bad-state" | "modern-rejected-input" | "modern-null-input" | "modern-invalid-complete" | "modern-untyped-block" | "modern-invalid-next") {
                assert_eq!(headers.get("mcp-protocol-version").unwrap(), "2026-07-28");
                assert_eq!(headers.get("mcp-method").unwrap(), "tools/call");
                assert_eq!(headers.get("mcp-name").unwrap(), "synthetic");
                if state.mode == "modern-bad-state" { return ([("content-type", "application/json")], format!("{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"resultType\":\"input_required\",\"requestState\":123}}}}")).into_response(); }
                if call == 0 { r#"{"resultType":"input_required","requestState":"opaque-9007199254740993","inputRequests":{"ask":{"method":"elicitation/create","params":{"message":"Continue?","requestedSchema":{"type":"object"}}}}}"#.into() }
                else {
                    assert!(body.contains("\"requestState\":\"opaque-9007199254740993\""));
                    assert!(value["params"].get("inputResponses").is_some());
                    if state.mode == "modern-rejected-input" {
                        return (
                            [("content-type", "application/json")],
                            format!(r#"{{"jsonrpc":"2.0","id":{id},"error":{{"code":-32602,"message":"Input rejected"}}}}"#),
                        ).into_response();
                    }
                    match state.mode {
                        "modern-null-input" => "null".into(),
                        "modern-invalid-complete" => r#"{"resultType":"complete","content":null}"#.into(),
                        "modern-untyped-block" => r#"{"resultType":"complete","content":[{}]}"#.into(),
                        "modern-invalid-next" => r#"{"resultType":"input_required","requestState":123}"#.into(),
                        _ => r#"{"resultType":"complete","content":[],"structuredContent":{"large":9007199254740993},"isError":true}"#.into(),
                    }
                }
            } else if matches!(state.mode, "sse" | "rejected-human-response") {
                let raw = format!("data: {{\"jsonrpc\":\"2.0\",\"id\":\"human\",\"method\":\"elicitation/create\",\"params\":{{\"message\":\"Continue?\",\"requestedSchema\":{{\"type\":\"object\"}}}}}}\r\n\r\ndata: {{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{{\"content\":[{{\"type\":\"text\",\"text\":\"done\"}}],\"isError\":false}}}}\r\n\r\n");
                return ([("content-type", "text/event-stream"), ("mcp-session-id", "captured")], raw).into_response();
            } else { r#"{"content":[{"type":"text","text":"complete"}],"structuredContent":{"n":9007199254740993,"exp":1e999999},"isError":false}"#.into() }
        }
        _ => panic!("unexpected method {method}"),
    };
    (
        [
            ("content-type", "application/json"),
            ("mcp-session-id", "captured"),
        ],
        format!("{{\"jsonrpc\":\"2.0\",\"id\":{id},\"result\":{result}}}"),
    )
        .into_response()
}

async fn resume_handler(
    axum::extract::State(state): axum::extract::State<Arc<HttpFixture>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if !headers.contains_key("last-event-id") {
        if state.mode == "get-blocking" {
            return ([("content-type", "text/event-stream")], "data: {\"jsonrpc\":\"2.0\",\"id\":\"push-human\",\"method\":\"elicitation/create\",\"params\":{\"message\":\"continue?\",\"requestedSchema\":{\"type\":\"object\"}}}\n\n").into_response();
        }
        return axum::http::StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    assert_eq!(headers.get("last-event-id").unwrap(), "cursor-1");
    assert_eq!(headers.get("mcp-session-id").unwrap(), "captured");
    state.requests.lock().await.push("GET resume".into());
    ([("content-type", "text/event-stream")], "id: cursor-2\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"2\",\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"resumed\"}]}}\n\n").into_response()
}

async fn delete_session(
    axum::extract::State(state): axum::extract::State<Arc<HttpFixture>>,
    headers: axum::http::HeaderMap,
) -> axum::http::StatusCode {
    assert_eq!(headers["mcp-session-id"], "captured");
    let expected = match state.mode {
        "invalid-version" => None,
        "legacy-2025-06-18" => Some("2025-06-18"),
        _ => Some("2025-11-25"),
    };
    assert_eq!(
        headers
            .get("mcp-protocol-version")
            .map(|h| h.to_str().unwrap()),
        expected
    );
    state.requests.lock().await.push("DELETE captured".into());
    // Servers may refuse session deletion.
    axum::http::StatusCode::METHOD_NOT_ALLOWED
}

async fn http_fixture(
    mode: &'static str,
) -> (
    CapturedServer,
    Arc<HttpFixture>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    http_fixture_at(mode, listener, "127.0.0.1").await
}

async fn http_fixture_at(
    mode: &'static str,
    listener: tokio::net::TcpListener,
    host: &str,
) -> (
    CapturedServer,
    Arc<HttpFixture>,
    tokio::task::JoinHandle<()>,
) {
    let state = Arc::new(HttpFixture {
        requests: tokio::sync::Mutex::new(vec![]),
        calls: AtomicUsize::new(0),
        mode,
        answer_seen: tokio::sync::Notify::new(),
    });
    let address = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route(
            "/mcp",
            axum::routing::post(http_handler)
                .get(resume_handler)
                .delete(delete_session),
        )
        .with_state(state.clone());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (
        CapturedServer {
            document_name: "document-server".into(),
            capture_digest: "capture".into(),
            protocol: if mode.starts_with("modern") {
                Protocol::Modern
            } else {
                Protocol::Legacy
            },
            transport: ServerTransport::Http(HttpServer {
                secret_values: Vec::new(),
                url: format!("http://{host}:{}/mcp", address.port())
                    .parse()
                    .unwrap(),
                addresses: vec![address],
                headers: Default::default(),
                tool_schema: Some(("synthetic".into(), "{}".into())),
            }),
        },
        state,
        task,
    )
}
fn request() -> CallRequest {
    CallRequest {
        identity: identity(),
        invocation: Invocation::Mcp {
            server: "document-server".into(),
            tool: "synthetic".into(),
            arguments_json: "{\"n\":9007199254740993}".into(),
        },
    }
}

fn dummy_host() -> Arc<TestHost> {
    Arc::new(TestHost::new(LaunchGrant::Mcp(Box::new(CapturedServer {
        document_name: "unused".into(),
        capture_digest: "unused".into(),
        protocol: Protocol::Legacy,
        transport: ServerTransport::Http(HttpServer {
            secret_values: Vec::new(),
            url: "http://127.0.0.1:1/mcp".parse().unwrap(),
            addresses: vec!["127.0.0.1:1".parse().unwrap()],
            headers: Default::default(),
            tool_schema: None,
        }),
    }))))
}

// Exercises transport only. Product dispatch remains closed pending the parent fence.
async fn isolated(
    host: &Arc<TestHost>,
    server: CapturedServer,
) -> (Result<McpReceipt>, PhysicalEvidence) {
    let mut session = start(server).unwrap();
    let result = session
        .call(
            "synthetic",
            "{\"n\":9007199254740993}",
            host.clone(),
            &identity(),
            &host.control,
        )
        .await;
    let (evidence, cleanup) = session.shutdown(result.is_ok()).await;
    assert!(cleanup.is_none(), "{cleanup:?}");
    (result, evidence)
}

#[tokio::test]
async fn sse_frames_mixed_line_endings_across_chunks() {
    let (server, _, task) = http_fixture("sse-line-endings").await;
    let (result, _) = isolated(&dummy_host(), server).await;
    task.abort();
    assert_eq!(result.unwrap().text, "complete");
}

#[tokio::test]
async fn http_exact_receipts_human_waits_modern_continuation_and_no_replay() {
    for mode in [
        "json",
        "legacy-2025-06-18",
        "sse",
        "resume",
        "resume-partial",
        "modern",
        "modern-bad-state",
        "redirect",
        "lost",
    ] {
        let (server, fixture, task) = http_fixture(mode).await;
        let host = dummy_host();
        let (result, evidence) = isolated(&host, server).await;
        match mode {
            "redirect" | "lost" | "modern-bad-state" => {
                assert!(result.is_err());
                assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            }
            _ => {
                let receipt = result.unwrap_or_else(|error| panic!("{mode}: {error:?}"));
                if matches!(mode, "sse" | "resume" | "resume-partial") {
                    assert_eq!(
                        receipt.text,
                        if mode.starts_with("resume") {
                            "resumed"
                        } else {
                            "done"
                        }
                    );
                } else {
                    assert!(receipt
                        .structured_content_json
                        .unwrap()
                        .contains("9007199254740993"));
                }
                assert_eq!(receipt.is_error, mode == "modern");
                if mode == "sse" || mode == "modern" {
                    assert_eq!(host.human_open.load(Ordering::SeqCst), 1);
                    assert_eq!(host.human_closed.load(Ordering::SeqCst), 1);
                    assert_eq!(host.human_delivered.load(Ordering::SeqCst), 1);
                }
            }
        }
        if mode.starts_with("resume") {
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
            assert!(fixture
                .requests
                .lock()
                .await
                .iter()
                .any(|r| r == "GET resume"));
        }
        assert_eq!(
            fixture
                .requests
                .lock()
                .await
                .iter()
                .filter(|value| *value == "DELETE captured")
                .count(),
            usize::from(!mode.starts_with("modern")),
            "{mode}",
        );
        assert!(matches!(
            evidence,
            PhysicalEvidence::Remote {
                termination_proven: false,
                ..
            }
        ));
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn malformed_rpc_error_keeps_the_remote_tool_in_flight() {
    for (mode, in_flight) in [
        ("rpc-error-null", true),
        ("rpc-error-string-code", true),
        ("rpc-error-no-message", true),
        ("rpc-error-with-result", true),
        ("rpc-error-valid", false),
    ] {
        let (server, fixture, task) = http_fixture(mode).await;
        let host = dummy_host();
        let mut session = start(server).unwrap();
        let result = session
            .call(
                "synthetic",
                r#"{"n":9007199254740993}"#,
                host.clone(),
                &identity(),
                &host.control,
            )
            .await;
        assert!(result.is_err(), "{mode}");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(session.tool_in_flight(), in_flight, "{mode}");
        assert!(session.shutdown(false).await.1.is_none(), "{mode}");
        task.abort();
    }
}

#[tokio::test]
async fn deletes_http_session_without_a_protocol_version_when_negotiation_fails() {
    let (server, fixture, task) = http_fixture("invalid-version").await;
    let mut session = start(server).unwrap();
    let host = dummy_host();
    assert!(session
        .initialize(host.clone(), &identity(), &host.control)
        .await
        .is_err());
    let _ = session.shutdown(false).await;
    assert!(fixture
        .requests
        .lock()
        .await
        .iter()
        .any(|r| r == "DELETE captured"));
    task.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn http_connect_preserves_pinned_ipv6_link_local_scope() {
    use std::net::{Ipv6Addr, SocketAddrV6};

    let mut interfaces = std::ptr::null_mut();
    assert_eq!(unsafe { libc::getifaddrs(&mut interfaces) }, 0);
    let mut current = interfaces;
    let mut address = None;
    // The list and its socket addresses remain valid until freeifaddrs.
    while let Some(entry) = unsafe { current.as_ref() } {
        if let Some(raw) = unsafe { entry.ifa_addr.as_ref() } {
            if raw.sa_family as i32 == libc::AF_INET6 {
                let raw = unsafe { &*entry.ifa_addr.cast::<libc::sockaddr_in6>() };
                let ip = Ipv6Addr::from(raw.sin6_addr.s6_addr);
                if ip.is_unicast_link_local() && entry.ifa_flags & libc::IFF_UP as u32 != 0 {
                    let scope = unsafe { libc::if_nametoindex(entry.ifa_name) };
                    if scope != 0 {
                        address = Some(SocketAddrV6::new(ip, 0, 0, scope));
                        break;
                    }
                }
            }
        }
        current = entry.ifa_next;
    }
    if !interfaces.is_null() {
        unsafe { libc::freeifaddrs(interfaces) };
    }
    let Some(address) = address else {
        eprintln!("Skipping scoped MCP HTTP: no IPv6 link-local address");
        return;
    };
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let destination = SocketAddrV6::new(
        *address.ip(),
        listener.local_addr().unwrap().port(),
        0,
        address.scope_id(),
    );
    #[cfg(target_os = "macos")]
    {
        // A bound link-local address can complete connect without delivering a
        // connection to the listener on macOS. Check the local fixture before
        // exercising the HTTP connector so that failure is attributable.
        let direct = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::try_join!(
                tokio::net::TcpStream::connect(destination),
                listener.accept()
            )
        })
        .await;
        if !matches!(direct, Ok(Ok(_))) {
            eprintln!(
                "Skipping scoped MCP HTTP: macOS did not accept the direct link-local TCP probe"
            );
            return;
        }
    }
    let (mut server, fixture, task) =
        http_fixture_at("legacy-2025-06-18", listener, "mcp.invalid").await;
    let ServerTransport::Http(http) = &mut server.transport else {
        unreachable!()
    };
    // Some platforms omit the scope when reporting the bound address.
    http.addresses = vec![destination.into()];
    let (result, _) = isolated(&dummy_host(), server).await;
    result.unwrap();
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn http_mcp_discovery_bounds_initialize_and_tools_list() {
    async fn assert_deadline(mode: &'static str) {
        let (server, fixture, task) = http_fixture(mode).await;
        let mut session = start(server).unwrap();
        let host = dummy_host();
        let error = tokio::time::timeout(HTTP_DISCOVERY_TIMEOUT + Duration::from_secs(3), async {
            session
                .initialize(host.clone(), &identity(), &host.control)
                .await?;
            session
                .list_tools(host.as_ref(), &identity(), &host.control)
                .await
        })
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(error.code, "TIMEOUT");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        let expected_method = if mode == "initialize-stall" {
            "initialize"
        } else {
            "tools/list"
        };
        assert!(fixture.requests.lock().await.iter().any(|body| {
            serde_json::from_str::<serde_json::Value>(body).unwrap()["method"] == expected_method
        }));
        let _ = session.shutdown(false).await;
        task.abort();
    }
    tokio::join!(
        assert_deadline("initialize-stall"),
        assert_deadline("list-stall")
    );
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
#[ignore = "Real confined stdio MCP process; run outside the agent sandbox"]
async fn darwin_stdio_exact_result_and_legacy_elicitation() {
    let root = tempfile::tempdir().unwrap();
    let script = r#"
IFS= read -r init
printf '%s\n' '{"jsonrpc":"2.0","id":"1","result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}'
IFS= read -r initialized
IFS= read -r call
case "$call" in *9007199254740993*) ;; *) exit 9;; esac
printf '%s\n' '{"jsonrpc":"2.0","id":"ask","method":"elicitation/create","params":{"message":"Continue?","requestedSchema":{"type":"object"}}}'
IFS= read -r answer
case "$answer" in *decline*) ;; *) exit 10;; esac
printf '%s\n' '{"jsonrpc":"2.0","id":"2","result":{"structuredContent":{"n":9007199254740993,"e":1e999999},"content":[],"isError":true}}'
"#;
    let server = CapturedServer {
        document_name: "document-server".into(),
        capture_digest: "capture".into(),
        protocol: Protocol::Legacy,
        transport: ServerTransport::Stdio(StdioServer {
            secret_values: Vec::new(),
            program: "/bin/sh".into(),
            arguments: vec!["-c".into(), script.into()],
            grant: grant(root.path()),
        }),
    };
    let host = dummy_host();
    let (result, _) = isolated(&host, server).await;
    let receipt = result.unwrap();
    assert!(receipt.is_error);
    assert_eq!(
        receipt.structured_content_json.unwrap(),
        "{\"n\":9007199254740993,\"e\":1e999999}"
    );
    assert_eq!(host.human_closed.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn literal_endpoint_must_match_every_captured_address_and_port() {
    fn server(url: &str, addresses: &[&str]) -> CapturedServer {
        CapturedServer {
            document_name: "document-server".into(),
            capture_digest: "native-capture".into(),
            protocol: Protocol::Legacy,
            transport: ServerTransport::Http(HttpServer {
                secret_values: Vec::new(),
                url: url.parse().unwrap(),
                addresses: addresses
                    .iter()
                    .map(|address| address.parse().unwrap())
                    .collect(),
                headers: Default::default(),
                tool_schema: Some(("synthetic".into(), "{}".into())),
            }),
        }
    }
    assert!(start(server("http://127.0.0.1:3000/mcp", &["127.0.0.1:3000"])).is_ok());
    assert!(start(server("http://127.0.0.1:3000/mcp", &["127.0.0.2:3000"])).is_err());
    assert!(start(server("http://127.0.0.1:3000/mcp", &["127.0.0.1:3001"])).is_err());
    assert!(start(server(
        "http://127.0.0.1:3000/mcp",
        &["127.0.0.1:3000", "127.0.0.2:3000"]
    ))
    .is_err());
    assert!(start(server("http://[::1]:3000/mcp", &["[::1]:3000"])).is_ok());
}

#[test]
fn annotated_parameter_headers_are_bound_to_schema_and_exact_nested_path() {
    let schema = r#"{"type":"object","properties":{"nested":{"type":"object","properties":{"region":{"type":"string","x-mcp-header":"Region"}}},"flag":{"type":"boolean","x-mcp-header":"Flag"},"number":{"type":"integer","x-mcp-header":"Count"}}}"#;
    let headers = parameter_headers(
        schema,
        r#"{"nested":{"region":"été"},"flag":true,"number":9007199254740991}"#,
    )
    .unwrap();
    assert_eq!(headers["mcp-param-region"], "=?base64?w6l0w6k=?=");
    assert_eq!(headers["mcp-param-flag"], "true");
    assert_eq!(headers["mcp-param-count"], "9007199254740991");
    assert!(parameter_headers(schema, r#"{"number":9007199254740992}"#).is_err());
    assert!(parameter_headers(r#"{"properties":{"a":{"type":"string","x-mcp-header":"A"},"b":{"type":"string","x-mcp-header":"a"}}}"#, "{}").is_err());
    assert!(parameter_headers(r#"{"items":{"type":"string","x-mcp-header":"A"}}"#, "{}").is_err());
}

#[tokio::test]
async fn product_mcp_dispatch_stays_closed_before_fence() {
    let (server, fixture, task) = http_fixture("json").await;
    let host = Arc::new(TestHost::new(LaunchGrant::Mcp(Box::new(server))));
    let completion = execute(host, request()).await.unwrap();
    assert_eq!(completion.failure.unwrap().code, "HOST_CAPABILITY");
    assert!(matches!(completion.evidence, PhysicalEvidence::NotStarted));
    assert!(fixture.requests.lock().await.is_empty());
    task.abort();
}

#[tokio::test]
async fn modern_push_is_rejected_before_human_or_response_post() {
    let (server, fixture, task) = http_fixture("modern-push").await;
    let host = dummy_host();
    let (result, evidence) = isolated(&host, server).await;
    assert!(result.is_err());
    assert_eq!(host.human_open.load(Ordering::SeqCst), 0);
    assert_eq!(host.human_closed.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.requests.lock().await.len(), 2);
    assert!(matches!(
        evidence,
        PhysicalEvidence::Remote {
            termination_proven: false,
            ..
        }
    ));
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn legacy_get_request_is_answered_before_post_response_headers() {
    let (server, fixture, task) = http_fixture("get-blocking").await;
    let host = dummy_host();
    let (result, evidence) =
        tokio::time::timeout(std::time::Duration::from_secs(5), isolated(&host, server))
            .await
            .unwrap();
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert_eq!(host.human_open.load(Ordering::SeqCst), 1);
    assert_eq!(host.human_closed.load(Ordering::SeqCst), 1);
    assert!(matches!(
        evidence,
        PhysicalEvidence::Remote {
            termination_proven: false,
            ..
        }
    ));
    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn rejected_modern_continuation_does_not_acknowledge_delivery() {
    let (server, fixture, task) = http_fixture("modern-rejected-input").await;
    let host = dummy_host();
    let (result, _) = isolated(&host, server).await;
    assert!(result.is_err());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 2);
    assert_eq!(host.human_closed.load(Ordering::SeqCst), 1);
    assert_eq!(host.human_delivered.load(Ordering::SeqCst), 0);
    task.abort();
}

#[tokio::test]
async fn malformed_modern_continuation_results_do_not_acknowledge_delivery() {
    for mode in [
        "modern-null-input",
        "modern-invalid-complete",
        "modern-untyped-block",
        "modern-invalid-next",
    ] {
        let (server, fixture, task) = http_fixture(mode).await;
        let host = dummy_host();
        let (result, evidence) = isolated(&host, server).await;
        assert!(result.is_err(), "{mode}");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 2, "{mode}");
        assert_eq!(host.human_closed.load(Ordering::SeqCst), 1, "{mode}");
        assert_eq!(host.human_delivered.load(Ordering::SeqCst), 0, "{mode}");
        assert!(
            matches!(
                evidence,
                PhysicalEvidence::Remote {
                    response_received: false,
                    termination_proven: false
                }
            ),
            "{mode}"
        );
        task.abort();
    }
}

#[tokio::test]
async fn rejected_human_response_does_not_acknowledge_delivery() {
    let (server, _, task) = http_fixture("rejected-human-response").await;
    let host = dummy_host();
    let (result, _) = isolated(&host, server).await;
    assert!(result.is_err());
    assert_eq!(host.human_closed.load(Ordering::SeqCst), 1);
    assert_eq!(host.human_delivered.load(Ordering::SeqCst), 0);
    task.abort();
}

#[tokio::test]
async fn stop_during_legacy_get_human_closes_request_without_call_replay() {
    let (server, fixture, task) = http_fixture("get-blocking").await;
    let dummy = CapturedServer {
        document_name: "unused".into(),
        capture_digest: "unused".into(),
        protocol: Protocol::Legacy,
        transport: ServerTransport::Http(HttpServer {
            secret_values: Vec::new(),
            url: "http://127.0.0.1:1/mcp".parse().unwrap(),
            addresses: vec!["127.0.0.1:1".parse().unwrap()],
            headers: Default::default(),
            tool_schema: None,
        }),
    };
    let mut host = TestHost::new(LaunchGrant::Mcp(Box::new(dummy)));
    host.human_delay = std::time::Duration::from_secs(30);
    let host = Arc::new(host);
    let running = {
        let host = host.clone();
        tokio::spawn(async move { isolated(&host, server).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while host.human_open.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    host.control.stop.cancel();
    let (result, evidence) = tokio::time::timeout(std::time::Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert_eq!(host.human_closed.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        evidence,
        PhysicalEvidence::Remote {
            termination_proven: false,
            ..
        }
    ));
    task.abort();
    let _ = task.await;
}

#[test]
fn secret_echoes_in_raw_and_decoded_structured_values_are_refused() {
    let secrets = vec!["secret-marker".to_owned()];
    assert!(reject_secret_echo(r#"{"value":"secret-marker"}"#, &secrets).is_err());
    assert!(reject_secret_echo(r#"{"value":"secret\u002dmarker"}"#, &secrets).is_err());
    assert!(reject_secret_echo(r#"{"secret\u002dmarker":true}"#, &secrets).is_err());
    assert!(reject_secret_echo(r#"{"value":"normal response"}"#, &secrets).is_ok());
}
