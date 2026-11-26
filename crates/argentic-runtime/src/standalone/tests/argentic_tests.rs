use super::*;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::post,
    Json, Router,
};
use futures::StreamExt;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const WAIT_FOR: Duration = Duration::from_secs(15);

struct Request {
    body: Value,
    headers: HeaderMap,
    respond: oneshot::Sender<Response>,
}

impl Request {
    fn send(self, status: StatusCode, body: impl Into<Body>, content_type: &str) {
        assert!(self
            .respond
            .send(
                Response::builder()
                    .status(status)
                    .header("content-type", content_type)
                    .body(body.into())
                    .unwrap()
            )
            .is_ok());
    }

    fn tool_response(call: &str, args: Value) -> String {
        let chunk = json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"same-wire-id",
            "function":{"name":call,"arguments":args.to_string()}}]},"finish_reason":"tool_calls"}]});
        format!("data: {chunk}\n\ndata: [DONE]\n\n")
    }

    fn tool(self, call: &str, args: Value) {
        self.send(
            StatusCode::OK,
            Self::tool_response(call, args),
            "text/event-stream",
        );
    }
}

struct Server {
    base_url: String,
    requests: mpsc::Receiver<Request>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start() -> Self {
        async fn respond(
            State(send): State<mpsc::Sender<Request>>,
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Response {
            let (respond, receive) = oneshot::channel();
            send.send(Request {
                body,
                headers,
                respond,
            })
            .await
            .unwrap();
            receive.await.unwrap()
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        let (send, requests) = mpsc::channel(4);
        let app = Router::new()
            .route("/v1/chat/completions", post(respond))
            .with_state(send);
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Self {
            base_url,
            requests,
            task,
        }
    }

    async fn next(&mut self) -> Request {
        tokio::time::timeout(WAIT_FOR, self.requests.recv())
            .await
            .expect("provider request timed out")
            .unwrap()
    }

    fn config(&self, key_env: Option<&str>) -> crate::harness::ArgenticConfig030 {
        serde_json::from_value(config(&self.base_url, key_env)).unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn config(url: &str, key_env: Option<&str>) -> Value {
    json!({"providers":{"local":{"protocol":"chat-completions","baseUrl":url,
        "models":["gpt-5.5#fixture"],"reasoningCatalog":"openai","apiKeyEnv":key_env}}})
}

fn document() -> Value {
    json!({"contract":"agsdl-0.4.0","id":"argentic-fixture",
        "agents":{"worker":{"harness":"argentic","model":{"name":"local/gpt-5.5#fixture","effort":"high"},
            "permissions":"read-only","harnessSettings":{"maxOutputTokens":256},
            "results":{"report":{"required":true,"schema":{"type":"object","required":["body"],
                "properties":{"body":{"type":"string"}}}}}}},
        "flow":{"entry":"read","steps":{"read":{"type":"agent","agent":"worker","instance":"new",
            "prompt":"Read memo.txt and submit its content.","next":[]}}}})
}

fn opt_in() -> BTreeSet<String> {
    BTreeSet::from(["argentic".into()])
}

async fn open(f: &Fixture, memory: bool) -> Host {
    let mut host = f.open().await.unwrap();
    if memory {
        host.pool.close().await;
        host.pool = crate::sqlite::memory().await.unwrap();
    }
    host
}

async fn start(host: &Host) -> RetainedStart030 {
    host.start_with_harnesses("start".into(), entry(), &opt_in())
        .await
        .unwrap_or_else(|error| {
            if let Ok(report) = serde_json::from_str::<Value>(&error.message) {
                panic!(
                    "{} {}: static {}, runtime {}",
                    error.code,
                    error.pointer,
                    report["static"]["diagnostics"],
                    report["runtime"]["diagnostics"]
                );
            }
            panic!("{} {}: {}", error.code, error.pointer, error.message)
        })
}

async fn until(mut predicate: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(WAIT_FOR, async {
        while !predicate().await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("engine did not reach the expected state");
}

async fn finish(host: &Host, run: &RetainedStart030) -> Value {
    let identity = &run.receipt.identity;
    tokio::time::timeout(
        WAIT_FOR,
        host.engine
            .flow030()
            .supervisors
            .wait(&identity.system_start_id),
    )
    .await
    .expect("supervisor did not join")
    .unwrap();
    let state: String =
        sqlx::query_scalar("SELECT state_json FROM engine_flow030_runs WHERE system_id=?")
            .bind(&identity.system_start_id)
            .fetch_one(&host.pool)
            .await
            .unwrap();
    serde_json::from_str(&state).unwrap()
}

async fn close(host: &Host) {
    host.shutdown(&[]).await.unwrap();
    host.close().await.unwrap();
}

fn completed(state: &Value) -> Value {
    let activation = &state["activations"][0];
    assert_eq!(activation["result"]["kind"], "completed", "{state}");
    serde_json::from_str(activation["result"]["outputJson"].as_str().unwrap()).unwrap()
}

async fn decisions(host: &Host) -> Vec<String> {
    sqlx::query_scalar("SELECT decision_json FROM engine_flow030_work_events WHERE decision_json IS NOT NULL ORDER BY rowid")
        .fetch_all(&host.pool).await.unwrap()
}

#[tokio::test]
async fn reads_and_submits_schema_valid_result() {
    for memory in [false, true] {
        let mut server = Server::start().await;
        let f = Fixture::new(&document().to_string());
        std::fs::write(f.workspace.join("memo.txt"), "Argentic read fixture").unwrap();
        let host = open(&f, memory)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        let first = server.next().await;
        assert_eq!(first.body["model"], "gpt-5.5");
        assert_eq!(first.body["max_completion_tokens"], 256);
        assert_eq!(first.body["reasoning_effort"], "high");
        assert!(first.body["messages"].to_string().contains("Read memo.txt"));
        first.tool("read", json!({"path":"memo.txt"}));
        let next = server.next().await;
        assert!(next.body["messages"]
            .to_string()
            .contains("Argentic read fixture"));
        next.tool(
            "agsdl_submit_result",
            json!({"results":{"report":{"body":"Argentic read fixture"}}}),
        );
        let output = completed(&finish(&host, &run).await);
        assert!(
            output.to_string().contains("Argentic read fixture"),
            "{output}"
        );
        close(&host).await;
    }
}

#[tokio::test]
async fn corrects_in_same_instance_and_remembers_effort_rejection_for_the_attempt() {
    for memory in [false, true] {
        let mut server = Server::start().await;
        let f = Fixture::new(&document().to_string());
        let host = open(&f, memory)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        let request = server.next().await;
        assert_eq!(request.body["reasoning_effort"], "high");
        request.send(
            StatusCode::BAD_REQUEST,
            r#"{"error":{"message":"Unknown parameter: reasoning_effort"}}"#,
            "application/json",
        );
        let retry = server.next().await;
        assert!(retry.body.get("reasoning_effort").is_none());
        retry.tool(
            "agsdl_submit_result",
            json!({"results":{"report":{"body":5}}}),
        );
        let request = server.next().await;
        assert!(request.body.get("reasoning_effort").is_none());
        let messages = request.body["messages"].to_string();
        assert!(
            messages.contains("same-wire-id") && messages.contains("Correct the result"),
            "{messages}"
        );
        request.tool(
            "agsdl_submit_result",
            json!({"results":{"report":{"body":"corrected"}}}),
        );
        let output = completed(&finish(&host, &run).await);
        assert!(output.to_string().contains("corrected"), "{output}");
        let mut kinds = Vec::new();
        for stored in decisions(&host).await {
            let stored: Value = serde_json::from_str(&stored).unwrap();
            let receipt: Value =
                serde_json::from_str(stored["receiptJson"].as_str().unwrap()).unwrap();
            kinds.push(receipt["decision"]["kind"].as_str().unwrap().to_owned());
        }
        assert_eq!(kinds.iter().filter(|kind| *kind == "correction").count(), 1);
        assert_eq!(kinds.iter().filter(|kind| *kind == "accepted").count(), 1);
        close(&host).await;
    }
}

#[tokio::test]
async fn denies_paths_under_either_read_ceiling() {
    for memory in [false, true] {
        let mut server = Server::start().await;
        let mut doc = document();
        doc["agents"]["worker"]["permissions"] = json!({"preset":"read-only","rules":[
            {"action":"read","match":"memo.txt","effect": if memory {"allow"} else {"deny"}}]});
        let mut f = Fixture::new(&doc.to_string());
        if memory {
            let mut policy: Value =
                serde_json::from_slice(&std::fs::read(&f.policy).unwrap()).unwrap();
            policy["permissions"] = json!({"preset":"read-only","rules":[
                {"action":"read","match":"memo.txt","effect":"deny"}]});
            let bytes = serde_json::to_vec(&policy).unwrap();
            std::fs::write(&f.policy, &bytes).unwrap();
            f.digest = digest(bytes);
        }
        std::fs::write(f.workspace.join("memo.txt"), "never disclosed").unwrap();
        let host = open(&f, memory)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        server.next().await.tool("read", json!({"path":"memo.txt"}));
        let request = server.next().await;
        let messages = request.body["messages"].to_string();
        assert!(!messages.contains("never disclosed"));
        assert!(messages.contains("denied"), "{messages}");
        request.tool(
            "agsdl_submit_result",
            json!({"results":{"report":{"body":"unavailable"}}}),
        );
        completed(&finish(&host, &run).await);
        close(&host).await;
    }
}

#[tokio::test]
async fn stop_joins_the_stream_and_records_replayable_physical_end() {
    for memory in [false, true] {
        let mut server = Server::start().await;
        let f = Fixture::new(&document().to_string());
        let host = open(&f, memory)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        let (dropped, did_drop) = oneshot::channel::<()>();
        let (streaming, did_stream) = oneshot::channel();
        let body = futures::stream::unfold(
            (Some(streaming), dropped),
            |(streaming, marker)| async move {
                match streaming {
                    Some(streaming) => {
                        let _ = streaming.send(());
                        Some((
                        Ok::<_, std::io::Error>(
                            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Streaming\"}}]}\n\n",
                        ),
                        (None, marker),
                    ))
                    }
                    None => std::future::pending().await,
                }
            },
        );
        server
            .next()
            .await
            .send(StatusCode::OK, Body::from_stream(body), "text/event-stream");
        tokio::time::timeout(WAIT_FOR, did_stream)
            .await
            .unwrap()
            .unwrap();
        let id = &run.receipt.identity;
        host.stop(EngineStopRequest {
            request_id: "stop".into(),
            system_start_id: id.system_start_id.clone(),
            activation_id: None,
        })
        .await
        .unwrap();
        let state = finish(&host, &run).await;
        assert_eq!(state["verdict"], "cancelled", "{state}");
        let physical: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM engine_flow030_events WHERE
                json_extract(event_json,'$.kind.PhysicalEnded.evidence.descendants_joined')=1
                AND receipt_json IS NOT NULL",
        )
        .fetch_one(&host.pool)
        .await
        .unwrap();
        assert_eq!(physical, 1);
        assert!(tokio::time::timeout(WAIT_FOR, did_drop)
            .await
            .unwrap()
            .is_err());
        close(&host).await;
        if !memory {
            let replay = persistence::replay_verify030::verify_path030(
                &f.data.join("engine.sqlite"),
                &id.system_start_id,
                &id.flow_invocation_id,
            )
            .await;
            assert!(replay.ok, "{:?}", replay.divergence);
        }
    }
}

#[tokio::test]
async fn steers_active_shared_work_and_preserves_the_conversation() {
    for memory in [false, true] {
        let mut server = Server::start().await;
        let mut doc = document();
        doc["agents"]["watcher"] = doc["agents"]["worker"].clone();
        doc["flow"] = json!({"entry":"lead","steps":{
            "lead":{"type":"agent","agent":"watcher","instance":"new","prompt":"Lead mission","next":["work","watch"]},
            "work":{"type":"agent","agent":"worker","delivery":"queue","prompt":"Worker mission","next":"gather"},
            "watch":{"type":"agent","agent":"watcher","instance":"new","prompt":"Watcher mission",
                "next":{"step":"signal","message":{"prompt":"Steer mission fixture"}}},
            "signal":{"type":"agent","agent":"worker","delivery":"steer","next":"gather"},
            "gather":{"type":"join","next":"followup"},
            "followup":{"type":"agent","agent":"worker","prompt":"Follow-up mission","next":[]}
        }});
        let f = Fixture::new(&doc.to_string());
        let host = open(&f, memory)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        let submission = json!({"results":{"report":{"body":"done"}}});
        server
            .next()
            .await
            .tool("agsdl_submit_result", submission.clone());
        let first = server.next().await;
        let second = server.next().await;
        let (mut request, watch) = if first.body["messages"]
            .to_string()
            .contains("Worker mission")
        {
            (first, second)
        } else {
            (second, first)
        };
        assert!(watch.body["messages"]
            .to_string()
            .contains("Watcher mission"));
        watch.tool("agsdl_submit_result", submission.clone());
        let pending = async {
            loop {
                let count: i64 = sqlx::query_scalar("SELECT count(*) FROM engine_flow030_outbox WHERE json_extract(effect_json,'$.kind.kind')='steer'")
                    .fetch_one(&host.pool).await.unwrap();
                if count > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            () = pending => (),
            request = server.next() => panic!("Unexpected provider request before the steer was offered: {}", request.body["messages"]),
        }
        // Keep the current work live until its next safe point receives the offer.
        let mut count = 0;
        while !request.body["messages"]
            .to_string()
            .contains("Steer mission fixture")
        {
            if count >= 10 {
                let raw: String = sqlx::query_scalar("SELECT state_json FROM engine_flow030_runs")
                    .fetch_one(&host.pool)
                    .await
                    .unwrap();
                let state: Value = serde_json::from_str(&raw).unwrap();
                let activations: Vec<_> = state["activations"].as_array().unwrap().iter().map(|activation| {
                    json!({"step":activation["step"],"phase":activation["phase"],"result":activation["result"]})
                }).collect();
                panic!(
                    "No steer reached the conversation. Activations {activations:?}, deliveries {}",
                    state["steerDeliveries"]
                );
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
            request.tool("list", json!({}));
            request = server.next().await;
            count += 1;
        }
        assert!(request.body["messages"]
            .to_string()
            .contains("Message from watch"));
        async fn consumed(host: &Host) -> bool {
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM engine_flow030_runs, json_each(state_json, '$.steerDeliveries')
                 WHERE json_extract(value, '$.status') = 'consumed'",
            ).fetch_one(&host.pool).await.unwrap();
            count > 0
        }
        assert!(
            !consumed(&host).await,
            "steer was consumed before response headers"
        );
        let (release, wait) = oneshot::channel::<()>();
        let accepted = Request::tool_response("agsdl_submit_result", submission.clone());
        let body = futures::stream::iter([Ok::<_, std::io::Error>(": accepted\n\n".to_owned())])
            .chain(futures::stream::once(async move {
                wait.await.unwrap();
                Ok(accepted)
            }));
        request.send(StatusCode::OK, Body::from_stream(body), "text/event-stream");
        until(async || consumed(&host).await).await;
        release.send(()).unwrap();
        let followup = server.next().await;
        let messages = followup.body["messages"].to_string();
        assert!(
            messages.contains("Steer mission fixture") && messages.contains("Follow-up mission"),
            "{messages}"
        );
        followup.tool("agsdl_submit_result", submission);
        let state = finish(&host, &run).await;
        assert_eq!(state["verdict"], "succeeded", "{state}");
        assert_eq!(state["steerDeliveries"][0]["status"], "consumed", "{state}");
        close(&host).await;
    }
}

#[tokio::test]
async fn provider_failure_does_not_persist_the_echoed_credential() {
    // Cargo supplies this fixture value to the test process without mutating
    // the environment of concurrently running tests.
    const KEY_ENV: &str = "CARGO_PKG_DESCRIPTION";
    let credential = std::env::var(KEY_ENV).unwrap();
    let mut server = Server::start().await;
    let f = Fixture::new(&document().to_string());
    let host = f
        .open()
        .await
        .unwrap()
        .with_argentic(server.config(Some(KEY_ENV)))
        .unwrap();
    let run = start(&host).await;
    let request = server.next().await;
    assert!(request.headers["authorization"].to_str().unwrap() == format!("Bearer {credential}"));
    let (prefix, suffix) = credential.split_at(credential.len() / 2);
    let text_chunk = |part: &str| json!({"choices":[{"index":0,"delta":{"content":part}}]});
    let tool = json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"read",
        "function":{"name":"list","arguments":"{}"}}]},"finish_reason":"tool_calls"}]});
    let stream = format!(
        "data: {}\n\ndata: {}\n\ndata: {tool}\n\ndata: [DONE]\n\n",
        text_chunk(prefix),
        text_chunk(suffix)
    );
    request.send(StatusCode::OK, stream, "text/event-stream");
    let request = server.next().await;
    request.send(
        StatusCode::UNAUTHORIZED,
        json!({"error":{"message":credential}}).to_string(),
        "application/json",
    );
    let state = finish(&host, &run).await;
    assert_eq!(
        state["activations"][0]["result"]["error"]["code"], "AGENT_FAILED",
        "{state}"
    );
    assert!(!serde_json::to_string(&run.capture)
        .unwrap()
        .contains(&credential));
    let events = decisions(&host).await.join("");
    assert!(!events.contains(&credential));
    assert!(events.contains("[redacted]"));
    close(&host).await;
    let bytes = std::fs::read(f.data.join("engine.sqlite")).unwrap();
    assert!(!bytes
        .windows(credential.len())
        .any(|window| window == credential.as_bytes()));
}

#[tokio::test]
async fn qualification_refuses_unregistered_opted_out_and_unsupported_permissions() {
    let f = Fixture::new(&document().to_string());
    let host = f.open().await.unwrap();
    let error = host
        .preflight_with_harnesses(entry().as_ref(), &opt_in())
        .await
        .unwrap_err();
    assert_eq!(error.pointer, "/agents/worker/harness");
    let host = host
        .with_argentic(serde_json::from_value(config("http://127.0.0.1:1/v1", None)).unwrap())
        .unwrap();
    let error = host.preflight(entry().as_ref(), false).await.unwrap_err();
    assert_eq!(error.code, "HARNESS_OPT_IN_REQUIRED");
    close(&host).await;
    for action in ["write", "shell", "network", "mcp", "delegate"] {
        let mut doc = document();
        doc["agents"]["worker"]["permissions"] = json!({"preset":"read-only",
            "rules":[{"action":action,"effect":"allow"}]});
        let f = Fixture::new(&doc.to_string());
        let host = f
            .open()
            .await
            .unwrap()
            .with_argentic(serde_json::from_value(config("http://127.0.0.1:1/v1", None)).unwrap())
            .unwrap();
        let error = host
            .preflight_with_harnesses(entry().as_ref(), &opt_in())
            .await
            .unwrap_err();
        assert_eq!(
            error.pointer, "/agents/worker/permissions/rules/0/effect",
            "{action}: {}",
            error.message
        );
        close(&host).await;
    }
}

#[tokio::test]
async fn persists_one_assistant_message_regardless_of_sse_chunking() {
    for parts in [&["bonjour"][..], &["bon", "jour"][..]] {
        let mut server = Server::start().await;
        let f = Fixture::new(&document().to_string());
        let host = open(&f, false)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        let frames = parts
            .iter()
            .map(|part| {
                let frame = json!({"choices":[{"delta":{"content":part}}]});
                format!("data: {frame}\n\n")
            })
            .collect::<String>();
        let arguments = json!({"results":{"report":{"body":"done"}}}).to_string();
        let final_frame = json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"submit",
            "function":{"name":"agsdl_submit_result","arguments":arguments}}]},"finish_reason":"tool_calls"}]});
        server.next().await.send(
            StatusCode::OK,
            format!("{frames}data: {final_frame}\n\ndata: [DONE]\n\n"),
            "text/event-stream",
        );
        let output = completed(&finish(&host, &run).await);
        assert_eq!(output["text"], "bonjour", "{output}");
        close(&host).await;
    }
}

#[tokio::test]
async fn uses_the_configured_default_model_when_the_agent_omits_name() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["model"]
        .as_object_mut()
        .unwrap()
        .remove("name");
    let f = Fixture::new(&doc.to_string());
    let unconfigured = open(&f, false)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let error = unconfigured
        .preflight_with_harnesses(entry().as_ref(), &opt_in())
        .await
        .unwrap_err();
    assert_eq!(error.pointer, "/agents/worker/model/name");
    close(&unconfigured).await;

    let mut config = config(&server.base_url, None);
    config["defaultModel"] = json!("local/gpt-5.5#fixture");
    let host = open(&f, false)
        .await
        .with_argentic(serde_json::from_value(config).unwrap())
        .unwrap();
    let run = start(&host).await;
    let request = server.next().await;
    assert_eq!(request.body["model"], "gpt-5.5");
    assert_eq!(request.body["reasoning_effort"], "high");
    request.tool(
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"done"}}}),
    );
    completed(&finish(&host, &run).await);
    close(&host).await;
}
