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
        Self::start_at("/v1/chat/completions").await
    }

    async fn start_at(path: &str) -> Self {
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
        let app = Router::new().route(path, post(respond)).with_state(send);
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
        assert!(first.body.get("tool_choice").is_none());
        assert!(first.body["messages"].to_string().contains("Read memo.txt"));
        first.tool("read", json!({"path":"memo.txt"}));
        let next = server.next().await;
        assert!(next.body.get("tool_choice").is_none());
        assert!(next.body["messages"]
            .to_string()
            .contains("Argentic read fixture"));
        let text =
            json!({"choices":[{"delta":{"content":"The memo is ready."},"finish_reason":"stop"}]});
        next.send(
            StatusCode::OK,
            format!("data: {text}\n\ndata: [DONE]\n\n"),
            "text/event-stream",
        );
        let correction = server.next().await;
        assert_eq!(
            correction.body["tool_choice"],
            json!({
                "type":"function","function":{"name":"agsdl_submit_result"}
            })
        );
        let messages = correction.body["messages"].to_string();
        assert!(messages.contains("Your previous reply did not call the agsdl_submit_result tool."));
        assert!(messages.contains("SUBMISSION_REQUIRED"));
        correction.tool(
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

#[derive(Clone, Copy)]
enum NativeProvider {
    Messages,
    Responses,
}

impl NativeProvider {
    fn tool(self, request: Request, tool: &str, arguments: Value) {
        let reasoning = self.reasoning();
        let events = match self {
            Self::Messages => vec![
                json!({"type":"message_start","message":{"content":[],"usage":{"input_tokens":8,"output_tokens":1}}}),
                json!({"type":"content_block_start","index":0,"content_block":reasoning}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"content_block_start","index":1,"content_block":{
                    "type":"tool_use","id":"toolu_fixture","name":tool,"input":{}}}),
                json!({"type":"content_block_delta","index":1,"delta":{
                    "type":"input_json_delta","partial_json":arguments.to_string()}}),
                json!({"type":"content_block_stop","index":1}),
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":4}}),
                json!({"type":"message_stop"}),
            ],
            Self::Responses => vec![
                json!({"type":"response.created","response":{"id":"resp_fixture","status":"in_progress"}}),
                json!({"type":"response.output_item.done","output_index":0,"item":reasoning}),
                json!({"type":"response.output_item.done","output_index":1,"item":{
                    "type":"function_call","id":"fc_fixture","call_id":"call_fixture","name":tool,
                    "arguments":arguments.to_string(),"status":"completed"}}),
                json!({"type":"response.completed","response":{"status":"completed",
                    "usage":{"input_tokens":8,"output_tokens":4}}}),
            ],
        };
        let body: String = events
            .into_iter()
            .map(|event| {
                format!(
                    "event: {}\ndata: {event}\n\n",
                    event["type"].as_str().unwrap()
                )
            })
            .collect();
        request.send(StatusCode::OK, body, "text/event-stream");
    }

    fn reasoning(self) -> Value {
        match self {
            Self::Messages => {
                json!({"type":"thinking","thinking":"Read the memo","signature":"signed-fixture"})
            }
            Self::Responses => {
                json!({"type":"reasoning","id":"rs_fixture","summary":[],"encrypted_content":"encrypted-fixture"})
            }
        }
    }
}

async fn native_provider_reads_and_submits(provider: NativeProvider) {
    let (path, protocol, model) = match provider {
        NativeProvider::Messages => ("/v1/messages", "anthropic-messages", "claude-sonnet-4-6"),
        NativeProvider::Responses => ("/v1/responses", "openai-responses", "gpt-5.4"),
    };
    let mut server = Server::start_at(path).await;
    let base_url = match provider {
        NativeProvider::Messages => server.base_url.strip_suffix("/v1").unwrap(),
        NativeProvider::Responses => &server.base_url,
    };
    let mut doc = document();
    doc["agents"]["worker"]["model"]["name"] = json!(format!("local/{model}#fixture"));
    doc["agents"]["worker"]["harnessSettings"]["maxOutputTokens"] = json!(4096);
    let mut config = json!({"providers":{"local":{"protocol":protocol,"baseUrl":base_url,
        "models":[format!("{model}#fixture")]}}});
    if matches!(provider, NativeProvider::Messages) {
        config["providers"]["local"]["promptCaching"] = json!(true);
    }
    let f = Fixture::new(&doc.to_string());
    std::fs::write(f.workspace.join("memo.txt"), "Native provider memo").unwrap();
    let host = open(&f, false)
        .await
        .with_argentic(serde_json::from_value(config).unwrap())
        .unwrap();
    let run = start(&host).await;
    let first = server.next().await;
    assert_eq!(first.body["model"], model);
    let history_field = match provider {
        NativeProvider::Messages => {
            assert_eq!(first.headers["anthropic-version"], "2023-06-01");
            assert_eq!(first.body["thinking"]["budget_tokens"], 4095);
            let system = first.body["system"].as_array().unwrap();
            assert_eq!(system.last().unwrap()["cache_control"]["type"], "ephemeral");
            "messages"
        }
        NativeProvider::Responses => {
            assert_eq!(first.body["store"], false);
            assert_eq!(first.body["reasoning"]["effort"], "high");
            "input"
        }
    };
    assert!(first.body[history_field]
        .to_string()
        .contains("Read memo.txt"));
    provider.tool(first, "read", json!({"path":"memo.txt"}));
    let next = server.next().await;
    let history = next.body[history_field].as_array().unwrap();
    match provider {
        NativeProvider::Messages => {
            let assistant = history.iter().find(|m| m["role"] == "assistant").unwrap();
            assert_eq!(assistant["content"][0], provider.reasoning());
            assert_eq!(
                history.last().unwrap()["content"][0]["tool_use_id"],
                "toolu_fixture"
            );
        }
        NativeProvider::Responses => {
            assert!(history.contains(&provider.reasoning()));
            assert_eq!(history.last().unwrap()["call_id"], "call_fixture");
        }
    }
    assert!(history
        .last()
        .unwrap()
        .to_string()
        .contains("Native provider memo"));
    provider.tool(
        next,
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"Native provider memo"}}}),
    );
    let output = completed(&finish(&host, &run).await);
    assert!(
        output.to_string().contains("Native provider memo"),
        "{output}"
    );
    close(&host).await;
}

#[tokio::test]
async fn anthropic_messages_reads_file_and_submits() {
    native_provider_reads_and_submits(NativeProvider::Messages).await;
}

#[tokio::test]
async fn openai_responses_reads_file_and_submits() {
    native_provider_reads_and_submits(NativeProvider::Responses).await;
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
        assert!(request.body.get("tool_choice").is_none());
        let tool_reply = request.body["messages"].as_array().unwrap().last().unwrap();
        assert_eq!(tool_reply["tool_call_id"], "same-wire-id");
        let content = tool_reply["content"].as_str().unwrap();
        let (instruction, rest) = content.split_once("\n\n").unwrap();
        assert!(instruction.starts_with("Correct the result arguments"));
        let (quoted_errors, details) = rest.split_once("\n\n").unwrap();
        let decision: Value = serde_json::from_str(details).unwrap();
        let diagnostic = decision["diagnostics"][0]["message"].as_str().unwrap();
        assert!(quoted_errors
            .lines()
            .any(|line| line == format!("{diagnostic:?}")));
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
async fn queues_works_on_one_named_instance_and_isolates_other_names() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["flow"] = json!({"entry":"lead","steps":{
        "lead":{"type":"agent","agent":"worker","instance":"new","prompt":"Lead work","next":["one","two"]},
        "one":{"type":"agent","agent":"worker","instance":"alpha","delivery":"queue","prompt":"Alpha one","next":"join"},
        "two":{"type":"agent","agent":"worker","instance":"alpha","delivery":"queue","prompt":"Alpha two","next":"join"},
        "join":{"type":"join","next":"other"},
        "other":{"type":"agent","agent":"worker","instance":"beta","prompt":"Beta work","next":[]}
    }});
    let f = Fixture::new(&doc.to_string());
    let host = open(&f, false)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    let result = json!({"results":{"report":{"body":"first alpha result"}}});
    server.next().await.tool(
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"lead"}}}),
    );
    let first = server.next().await;
    assert!(first.body["messages"].to_string().contains("Alpha one"));
    assert!(!first.body["messages"].to_string().contains("Lead work"));
    first.tool("agsdl_submit_result", result.clone());
    let second = server.next().await;
    let messages = second.body["messages"].to_string();
    for expected in [
        "Alpha one",
        "Alpha two",
        "first alpha result",
        "Result accepted.",
    ] {
        assert!(messages.contains(expected), "{messages}");
    }
    second.tool("agsdl_submit_result", result.clone());
    let other = server.next().await;
    assert!(other.body["messages"].to_string().contains("Beta work"));
    assert!(!other.body["messages"].to_string().contains("Alpha"));
    other.tool("agsdl_submit_result", result);
    let state = finish(&host, &run).await;
    assert_eq!(state["verdict"], "succeeded", "{state}");
    close(&host).await;
}

#[tokio::test]
async fn remaining_stop_joins_a_work_and_keeps_its_shared_conversation() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["winner"] = doc["agents"]["worker"].clone();
    doc["flow"] = json!({"entry":"lead","steps":{
        "lead":{"type":"agent","agent":"winner","instance":"new","prompt":"Lead","next":["slow","fast"]},
        "slow":{"type":"agent","agent":"worker","delivery":"queue","prompt":"Stopped mission","next":{"approve":"join"}},
        "fast":{"type":"agent","agent":"winner","instance":"new","prompt":"Winning mission","next":{"approve":"join"}},
        "join":{"type":"join","quorum":{"count":1,"accept":["approve"]},"remaining":"stop",
            "next":{"reached":"resume","missed":[]}},
        "resume":{"type":"agent","agent":"worker","prompt":"Resumed mission","next":[]}
    }});
    let f = Fixture::new(&doc.to_string());
    let host = open(&f, false)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    let result = json!({"results":{"report":{"body":"done"}}});
    server
        .next()
        .await
        .tool("agsdl_submit_result", result.clone());
    let a = server.next().await;
    let b = server.next().await;
    let (slow, fast) = if a.body["messages"].to_string().contains("Stopped mission") {
        (a, b)
    } else {
        (b, a)
    };
    slow.tool("list", json!({}));
    let inflight = server.next().await;
    assert_eq!(
        inflight.body["messages"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["role"],
        "tool"
    );
    let (dropped, did_drop) = oneshot::channel::<()>();
    let body = futures::stream::once(async move {
        let _held = dropped;
        std::future::pending::<std::io::Result<String>>().await
    });
    inflight.send(StatusCode::OK, Body::from_stream(body), "text/event-stream");
    let mut winning = result.clone();
    winning["choice"] = json!("approve");
    fast.tool("agsdl_submit_result", winning);
    let resumed = server.next().await;
    let messages = resumed.body["messages"].to_string();
    for expected in ["Stopped mission", "Resumed mission", "\"role\":\"tool\""] {
        assert!(messages.contains(expected), "{messages}");
    }
    assert!(tokio::time::timeout(WAIT_FOR, did_drop)
        .await
        .unwrap()
        .is_err());
    resumed.tool("agsdl_submit_result", result);
    let state = finish(&host, &run).await;
    assert_eq!(state["verdict"], "succeeded", "{state}");
    close(&host).await;
}

#[tokio::test]
async fn a_host_restart_interrupts_argentic_runs_and_new_runs_start_fresh() {
    const ROOT: &str = "ARGENTIC_INSTANCE_CRASH_ROOT";
    const URL: &str = "ARGENTIC_INSTANCE_CRASH_URL";
    if let Some(root) = std::env::var_os(ROOT) {
        let root = PathBuf::from(root);
        let bytes = std::fs::read(root.join("policy.json")).unwrap();
        let host = Host::open(
            &root.join("data"),
            &root.join("workspace"),
            &root.join("source.json"),
            &root.join("policy.json"),
            &digest(bytes),
        )
        .await
        .unwrap()
        .with_argentic(serde_json::from_value(config(&std::env::var(URL).unwrap(), None)).unwrap())
        .unwrap();
        start(&host).await;
        std::future::pending::<()>().await;
        return;
    }
    let mut server = Server::start().await;
    let mut doc = document();
    doc["flow"] = json!({"entry":"one","steps":{
        "one":{"type":"agent","agent":"worker","prompt":"First shared work","next":"two"},
        "two":{"type":"agent","agent":"worker","prompt":"Second shared work","next":[]}
    }});
    let f = Fixture::new(&doc.to_string());
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "standalone::tests::argentic_tests::a_host_restart_interrupts_argentic_runs_and_new_runs_start_fresh"])
        .env(ROOT, f._temp.path())
        .env(URL, &server.base_url)
        .kill_on_drop(true)
        .spawn().unwrap();
    let old = json!({"results":{"report":{"body":"before restart"}}});
    server.next().await.tool("agsdl_submit_result", old);
    let active = server.next().await;
    assert!(active.body["messages"]
        .to_string()
        .contains("before restart"));
    child.kill().await.unwrap();
    drop(active);

    let host = open(&f, false)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    assert_eq!(host.runs().await.unwrap()[0]["interrupted"], true);
    assert!(host.active_runs().await.unwrap().is_empty());
    let mut fresh_entry = entry().unwrap();
    fresh_entry.delivery_id = "after-restart-entry".into();
    let fresh = host
        .start_with_harnesses("after-restart".into(), Some(fresh_entry), &opt_in())
        .await
        .unwrap();
    let request = server.next().await;
    let messages = request.body["messages"].to_string();
    assert!(messages.contains("First shared work"), "{messages}");
    assert!(!messages.contains("before restart"), "{messages}");
    let result = json!({"results":{"report":{"body":"after restart"}}});
    request.tool("agsdl_submit_result", result.clone());
    server.next().await.tool("agsdl_submit_result", result);
    assert_eq!(finish(&host, &fresh).await["verdict"], "succeeded");
    close(&host).await;
}

#[tokio::test]
async fn compaction_summarizes_old_turns_and_preserves_the_active_wire_prefix() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["instructions"] = json!("Stable rules for this run.");
    doc["agents"]["worker"]["harnessSettings"]["compaction"] =
        json!({"threshold":0.60,"keepRecentTurns":1});
    let mut provider = config(&server.base_url, None);
    provider["providers"]["local"]["contextWindowTokens"] = json!(8192);
    let f = Fixture::new(&doc.to_string());
    let host = open(&f, false)
        .await
        .with_argentic(serde_json::from_value(provider).unwrap())
        .unwrap();
    let run = start(&host).await;

    let first = server.next().await;
    let prefix = first.body["messages"].clone();
    let tools = first.body["tools"].clone();
    let long_tool_turn = |request: Request, marker: &str, padding: char| {
        let content = format!("{marker} {}", padding.to_string().repeat(12_000));
        let chunk = json!({"choices":[{"index":0,"delta":{"content":content}}]});
        let tool = Request::tool_response("list", json!({}));
        request.send(
            StatusCode::OK,
            format!("data: {chunk}\n\n{tool}"),
            "text/event-stream",
        );
    };
    long_tool_turn(first, "Old turn marker", 'a');
    long_tool_turn(server.next().await, "Recent turn marker", 'b');
    let summary = server.next().await;
    assert!(summary.body.get("tools").is_none(), "{}", summary.body);
    let transcript = summary.body["messages"].to_string();
    assert!(transcript.contains("Summarize this older conversation"));
    assert!(transcript.contains("Old turn marker"));
    assert!(!transcript.contains("Recent turn marker"));
    let output = json!({"choices":[{"index":0,"delta":{"content":"Remember the old directory listing."},"finish_reason":"stop"}]});
    summary.send(
        StatusCode::OK,
        format!("data: {output}\n\ndata: [DONE]\n\n"),
        "text/event-stream",
    );
    let compacted = server.next().await;
    let messages = compacted.body["messages"].as_array().unwrap();
    assert_eq!(&messages[..2], prefix.as_array().unwrap());
    assert_eq!(compacted.body["tools"], tools);
    assert!(messages[2]["content"]
        .as_str()
        .unwrap()
        .contains("Remember the old directory listing."));
    assert!(messages[3]["content"]
        .as_str()
        .unwrap()
        .contains("Recent turn marker"));
    assert_eq!(
        messages[3]["tool_calls"][0]["id"],
        messages[4]["tool_call_id"]
    );
    assert_eq!(messages.len(), 5);
    compacted.tool(
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"compacted work"}}}),
    );
    assert_eq!(finish(&host, &run).await["verdict"], "succeeded");
    close(&host).await;
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
    for action in ["mcp", "delegate"] {
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

fn forbid_shell() -> Value {
    json!({"action":"shell","effect":"deny"})
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn qualification_rejects_read_restrictions_on_shell_runtime_paths() {
    for (restriction, suffix) in [
        (
            json!({"rules":[{"action":"read","match":"/proc/cmdline","effect":"deny"}]}),
            "rules/0/match",
        ),
        (
            json!({"rules":[{"action":"read","match":"/etc/**","effect":"ask"}]}),
            "rules/0/match",
        ),
        (json!({"outside":"deny"}), "outside"),
    ] {
        for shell in [true, false] {
            let mut doc = document();
            let mut permissions = restriction.clone();
            permissions["preset"] = json!("workspace");
            permissions["reviewer"] = json!("person");
            if !shell {
                if permissions["rules"].is_null() {
                    permissions["rules"] = json!([]);
                }
                permissions["rules"]
                    .as_array_mut()
                    .unwrap()
                    .push(forbid_shell());
            }
            doc["agents"]["worker"]["permissions"] = permissions;
            let f = Fixture::new(&doc.to_string());
            let host = f
                .open()
                .await
                .unwrap()
                .with_argentic(
                    serde_json::from_value(config("http://127.0.0.1:1/v1", None)).unwrap(),
                )
                .unwrap();
            let report = host
                .preflight_with_harnesses(entry().as_ref(), &opt_in())
                .await;
            if shell {
                assert_eq!(
                    report.unwrap_err().pointer,
                    format!("/agents/worker/permissions/{suffix}")
                );
            } else {
                report.unwrap();
            }
            close(&host).await;
        }
    }
}

#[tokio::test]
async fn workspace_edits_and_denied_writes_go_through_the_host() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["permissions"] = json!({"preset":"workspace","rules":[
        forbid_shell(), {"action":"write","match":"protected","effect":"deny"}
    ]});
    let f = Fixture::new(&doc.to_string());
    std::fs::write(f.workspace.join("memo.txt"), "before").unwrap();
    let host = f
        .open()
        .await
        .unwrap()
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    server.next().await.tool(
        "edit",
        json!({"path":"memo.txt","old_text":"before","new_text":"after"}),
    );
    let request = server.next().await;
    assert!(request.body["messages"]
        .to_string()
        .contains("Applied 1 file change"));
    request.tool("write", json!({"path":"protected","content":"denied"}));
    let request = server.next().await;
    assert!(request.body["messages"]
        .to_string()
        .contains("Write permission denied"));
    request.tool(
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"done"}}}),
    );
    completed(&finish(&host, &run).await);
    assert_eq!(
        std::fs::read_to_string(f.workspace.join("memo.txt")).unwrap(),
        "after"
    );
    assert!(!f.workspace.join("protected").exists());
    close(&host).await;
}

async fn pending_ask(
    host: &Host,
    scope: &persistence::public030::HumanScope030,
) -> persistence::public030::PublicHumanRequest030 {
    tokio::time::timeout(WAIT_FOR, async {
        loop {
            let page = host.humans(scope.clone(), None).await.unwrap();
            if let Some(request) = page
                .requests
                .into_iter()
                .find(|request| request.status == "open")
            {
                return request;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Argentic action approval did not open")
}

#[tokio::test]
async fn an_argentic_project_ceiling_can_supply_the_reviewer() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["permissions"] = json!({"preset":"read-only"});
    let mut f = Fixture::new(&doc.to_string());
    let mut policy: Value = serde_json::from_slice(&std::fs::read(&f.policy).unwrap()).unwrap();
    policy["permissions"] = json!({"preset":"read-only","reviewer":"person","rules":[
        {"action":"read","match":"reviewed.txt","effect":"ask"}
    ]});
    let bytes = serde_json::to_vec(&policy).unwrap();
    std::fs::write(&f.policy, &bytes).unwrap();
    f.digest = digest(bytes);
    std::fs::write(f.workspace.join("reviewed.txt"), "approved text").unwrap();
    let host = open(&f, true)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    let scope = persistence::public030::HumanScope030 {
        system_start_id: run.receipt.identity.system_start_id.clone(),
        source: run.capture.source.clone(),
    };
    server
        .next()
        .await
        .tool("read", json!({"path":"reviewed.txt"}));
    let ask = pending_ask(&host, &scope).await;
    host.submit_person(HumanSubmission {
        scope,
        request_id: ask.id,
        expected_revision: ask.revision,
        answer: api::HumanAnswer030::Decision {
            decision: "yes".into(),
            comment: String::new(),
        },
    })
    .await
    .unwrap();
    let result = server.next().await;
    assert!(result.body["messages"]
        .to_string()
        .contains("approved text"));
    result.tool(
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"done"}}}),
    );
    completed(&finish(&host, &run).await);
    close(&host).await;
}

#[tokio::test]
async fn a_write_approval_is_one_shot_and_stop_cancels_the_next_ask() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["permissions"] = json!({"preset":"workspace","reviewer":"person","rules":[
        forbid_shell(), {"action":"write","match":"reviewed","effect":"ask"}
    ]});
    let f = Fixture::new(&doc.to_string());
    let host = f
        .open()
        .await
        .unwrap()
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    let scope = persistence::public030::HumanScope030 {
        system_start_id: run.receipt.identity.system_start_id.clone(),
        source: run.capture.source.clone(),
    };
    server
        .next()
        .await
        .tool("write", json!({"path":"reviewed","content":"first"}));
    let first = pending_ask(&host, &scope).await;
    assert!(!f.workspace.join("reviewed").exists());
    host.submit_person(HumanSubmission {
        scope: scope.clone(),
        request_id: first.id.clone(),
        expected_revision: first.revision,
        answer: api::HumanAnswer030::Decision {
            decision: "yes".into(),
            comment: String::new(),
        },
    })
    .await
    .unwrap();
    let next = server.next().await;
    assert_eq!(
        std::fs::read_to_string(f.workspace.join("reviewed")).unwrap(),
        "first"
    );
    next.tool("write", json!({"path":"reviewed","content":"second"}));
    let second = pending_ask(&host, &scope).await;
    assert_ne!(first.id, second.id);
    host.stop(EngineStopRequest {
        request_id: "stop-pending-review".into(),
        system_start_id: scope.system_start_id.clone(),
        activation_id: None,
    })
    .await
    .unwrap();
    let state = finish(&host, &run).await;
    assert_eq!(state["verdict"], "cancelled", "{state}");
    assert_eq!(
        std::fs::read_to_string(f.workspace.join("reviewed")).unwrap(),
        "first"
    );
    let page = host.humans(scope, None).await.unwrap();
    assert!(page.requests.iter().all(|request| request.status != "open"));
    close(&host).await;
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[tokio::test]
async fn shell_executes_and_stop_reaps_the_namespace_before_physical_end() {
    if ["/usr/bin/bwrap", "/bin/bwrap", "/usr/local/bin/bwrap"]
        .into_iter()
        .all(|path| !std::path::Path::new(path).is_file())
    {
        assert_ne!(
            std::env::var("ARGENTIC_REQUIRE_BWRAP").as_deref(),
            Ok("1"),
            "Bubblewrap is required for the Linux standalone shell test"
        );
        eprintln!("Skipping Linux standalone shell test because system Bubblewrap is absent");
        return;
    }
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["permissions"] = json!("full");
    let mut f = Fixture::new(&doc.to_string());
    let staging = f.workspace.parent().unwrap().join("staging");
    std::fs::create_dir(&staging).unwrap();
    let mut config = config(&server.base_url, None);
    config["shellStagingDirectory"] = json!(staging);
    let mut policy: Value = serde_json::from_slice(&std::fs::read(&f.policy).unwrap()).unwrap();
    policy["permissions"] = json!("full");
    let bytes = serde_json::to_vec(&policy).unwrap();
    std::fs::write(&f.policy, &bytes).unwrap();
    f.digest = digest(bytes);
    let host = f
        .open()
        .await
        .unwrap()
        .with_argentic(serde_json::from_value(config).unwrap())
        .unwrap();
    let run = start(&host).await;
    server
        .next()
        .await
        .tool("shell", json!({"command":"printf sandboxed > output; pwd"}));
    let request = server.next().await;
    assert!(request.body["messages"].to_string().contains("/workspace"));
    assert_eq!(
        std::fs::read_to_string(f.workspace.join("output")).unwrap(),
        "sandboxed"
    );

    // Keep a stream open in a grandchild. EOF after Stop proves the kernel
    // closed its descriptor, independent of the work's journal marker.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    request.tool(
        "shell",
        json!({"command":format!(
            "( (sleep 30) ) > /dev/tcp/127.0.0.1/{port} & wait"
        )}),
    );
    let (stream, _) = tokio::time::timeout(WAIT_FOR, listener.accept())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 1);
    host.stop(EngineStopRequest {
        request_id: "stop-shell".into(),
        system_start_id: run.receipt.identity.system_start_id.clone(),
        activation_id: None,
    })
    .await
    .unwrap();
    let state = finish(&host, &run).await;
    assert_eq!(state["verdict"], "cancelled", "{state}");
    tokio::time::timeout(WAIT_FOR, stream.readable())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stream.try_read(&mut [0]).unwrap(), 0);
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 0);
    let physical: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM engine_flow030_events WHERE
            json_extract(event_json,'$.kind.PhysicalEnded.evidence.descendants_joined')=1
            AND receipt_json IS NOT NULL",
    )
    .fetch_one(&host.pool)
    .await
    .unwrap();
    assert_eq!(physical, 1);
    close(&host).await;
}
