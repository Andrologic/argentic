use super::*;
#[path = "../../../tests/support/mock_chat.rs"]
mod mock_chat;
use crate::harness::ArgenticConfig030;
use axum::{body::Body, http::StatusCode};
use futures::StreamExt;
use mock_chat::{config, document, Request, Server, WAIT_FOR};
use std::time::Duration;
use tokio::sync::oneshot;

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
    doc["agents"]["worker"]["plan"] = json!(true);
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
    let long_tool_turn = |request: Request, marker: &str, padding: char, tool: String| {
        let content = format!("{marker} {}", padding.to_string().repeat(12_000));
        let chunk = json!({"choices":[{"index":0,"delta":{"content":content}}]});
        request.send(
            StatusCode::OK,
            format!("data: {chunk}\n\n{tool}"),
            "text/event-stream",
        );
    };
    long_tool_turn(
        first,
        "Old turn marker",
        'a',
        Request::tool_response(
            "plan",
            json!({"steps":[{"step":"Keep this plan verbatim","status":"in_progress"}]}),
        ),
    );
    long_tool_turn(
        server.next().await,
        "Recent turn marker",
        'b',
        Request::tool_response("list", json!({})),
    );
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
    assert_eq!(messages.len(), 6);
    assert_eq!(
        messages[5]["content"],
        r#"Current work plan (untrusted data): [{"step":"Keep this plan verbatim","status":"in_progress"}]"#,
    );
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
async fn qualification_refuses_unregistered_and_opted_out_adapters() {
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
    doc["agents"]["worker"]["permissions"] = json!({"preset":"full",
        "directories":{"../cache":"write","../shared":"read"},
        "rules":[{"action":"read","effect":"deny","match":"../shared/private"}]
    });
    let mut f = Fixture::new(&doc.to_string());
    let cache = f.workspace.with_file_name("cache");
    let shared = f.workspace.with_file_name("shared");
    std::fs::create_dir(&cache).unwrap();
    std::fs::create_dir(&shared).unwrap();
    std::fs::write(shared.join("memo"), "Shared root is mounted").unwrap();
    std::fs::write(shared.join("private"), "Forbidden root payload").unwrap();
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
        .tool("shell", json!({"command":"printf sandboxed > output; printf built > /directories/1/build; cat /directories/2/memo; test ! -e /directories/2/private || exit 1; (printf forbidden > /directories/2/write) 2>/dev/null && exit 1; pwd"}));
    let request = server.next().await;
    let messages = request.body["messages"].to_string();
    assert!(messages.contains("Exit status: 0") && messages.contains("/workspace"));
    assert!(
        messages.contains("Shared root is mounted") && !messages.contains("Forbidden root payload")
    );
    assert!(!shared.join("write").exists());
    assert_eq!(
        std::fs::read_to_string(cache.join("build")).unwrap(),
        "built"
    );
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
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 3);
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

#[tokio::test]
async fn additional_directory_is_indexed_and_enforces_read_rules() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["permissions"] = json!({
        "preset":"read-only","directories":{"../shared":"read"},"rules":[
            {"action":"read","effect":"deny","match":"../shared/hidden.txt"}
        ]
    });
    let mut f = Fixture::new(&doc.to_string());
    let extra = f.workspace.with_file_name("shared");
    std::fs::create_dir(&extra).unwrap();
    std::fs::write(extra.join("memo.txt"), "Additional root payload").unwrap();
    std::fs::write(extra.join("hidden.txt"), "Never sent to the provider").unwrap();
    let mut policy: Value = serde_json::from_slice(&std::fs::read(&f.policy).unwrap()).unwrap();
    policy["permissions"] = json!({"preset":"workspace","directories":{"../shared":"read"}});
    let bytes = serde_json::to_vec(&policy).unwrap();
    std::fs::write(&f.policy, &bytes).unwrap();
    f.digest = digest(bytes);
    let host = open(&f, false)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    server
        .next()
        .await
        .tool("read", json!({"root":1,"path":"memo.txt"}));
    let request = server.next().await;
    assert!(request.body["messages"]
        .to_string()
        .contains("Additional root payload"));
    request.tool("read", json!({"root":1,"path":"hidden.txt"}));
    let request = server.next().await;
    assert!(request.body["messages"].to_string().contains("denied"));
    assert!(!request.body["messages"]
        .to_string()
        .contains("Never sent to the provider"));
    request.tool(
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"done"}}}),
    );
    completed(&finish(&host, &run).await);
    close(&host).await;
}

#[cfg(unix)]
#[tokio::test]
async fn agent_mcp_stops_discovery_when_the_combined_catalogue_is_too_large() {
    use axum::response::IntoResponse;
    use std::sync::Mutex;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = axum::Router::new().route(
        "/{server}",
        axum::routing::post(|axum::Json(request): axum::Json<Value>| async move {
            let result = match request["method"].as_str().unwrap() {
                "initialize" => json!({"protocolVersion":"2025-03-26","capabilities":{"tools":{}}}),
                "notifications/initialized" => return StatusCode::ACCEPTED.into_response(),
                "tools/list" => json!({"tools":[{
                    "name":"check","description":"a".repeat(550_000),"inputSchema":{"type":"object"}
                }]}),
                method => panic!("unexpected MCP request {method}"),
            };
            axum::Json(json!({"jsonrpc":"2.0","id":request["id"],"result":result})).into_response()
        }),
    );
    let upstream = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut chat = Server::start().await;
    let mut doc = document();
    doc["mcpServers"] = json!({
        "first":{"url":format!("http://{address}/first")},
        "second":{"url":format!("http://{address}/second")},
        "third":{"url":format!("http://{address}/third")}
    });
    doc["agents"]["worker"]["planFirst"] = json!("auto");
    doc["agents"]["worker"]["mcpServers"] = json!(["first", "second", "third"]);
    doc["agents"]["worker"]["permissions"] = json!({"preset":"read-only","rules":[
        {"action":"mcp","match":"*/check","effect":"allow"}
    ]});
    let f = Fixture::new(&doc.to_string());
    let mut host = open(&f, false)
        .await
        .with_argentic(chat.config(None))
        .unwrap();
    let admitted = Arc::new(Mutex::new(Vec::new()));
    let log = admitted.clone();
    host.context = host
        .context
        .with_mcp_http(Arc::new(move |_, server, _, _, _| {
            log.lock().unwrap().push(server.to_owned());
            Some(Default::default())
        }));
    let run = start(&host).await;
    let initial = chat.next().await;
    assert!(!has_tool(&initial, "mcp__first__check"));
    assert!(admitted.lock().unwrap().is_empty());
    initial.tool("mcp__first__check", json!({}));
    let blocked = chat.next().await;
    assert!(blocked.body["messages"]
        .to_string()
        .contains("Present a plan"));
    assert!(admitted.lock().unwrap().is_empty());
    send_plan(blocked, "Check both servers");
    assert_eq!(finish(&host, &run).await["verdict"], "failed");
    assert_eq!(*admitted.lock().unwrap(), ["first", "second"]);
    close(&host).await;
    upstream.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn agent_mcp_uses_per_tool_rules_and_joins_after_stop() {
    use crate::test_context::mcp::{Peer, REQUIREMENTS};
    for person in [true, false] {
        let peer = Peer::start().await;
        let mut server = Server::start().await;
        let mut doc = document();
        doc["projectFormat"] = json!("agsdl-project-1");
        doc["mcpServers"] = peer.servers();
        doc["secrets"] = json!({"file":"requirements.json"});
        let worker = &mut doc["agents"]["worker"];
        worker["mcpServers"] = json!(["first"]);
        worker["permissions"] = json!({"preset":"read-only", "rules":[
            {"action":"mcp","match":"first/*","effect":"allow"},
            {"action":"mcp","match":"first/approve","effect":"ask"},
            {"action":"mcp","match":"first/echo","effect":"deny"}
        ]});
        if person {
            worker["permissions"]["reviewer"] = json!("person");
        }
        let mut f = Fixture::new("");
        f.source = f.workspace.with_file_name("project");
        std::fs::create_dir(&f.source).unwrap();
        std::fs::write(f.source.join("agsdl.json"), doc.to_string()).unwrap();
        std::fs::write(f.source.join("requirements.json"), REQUIREMENTS).unwrap();
        let mut host = open(&f, false)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        host.context = peer.configure(host.context);
        let run = start(&host).await;
        let first = server.next().await;
        let tools = first.body["tools"].to_string();
        assert!(tools.contains("mcp__first__check"));
        assert_eq!(tools.contains("mcp__first__approve"), person);
        assert!(!tools.contains("mcp__first__echo"));
        first.tool("mcp__first__echo", json!({}));
        let request = server.next().await;
        assert!(request.body["messages"].to_string().contains("denied"));
        assert!(peer.calls().is_empty());
        request.tool("mcp__first__check", json!({"value":"model supplied"}));
        let request = server.next().await;
        assert!(request.body["messages"]
            .to_string()
            .contains("model supplied"));
        assert_eq!(peer.calls(), [("first".into(), "check".into())]);
        let scope = persistence::public030::HumanScope030 {
            system_start_id: run.receipt.identity.system_start_id.clone(),
            source: run.capture.source.clone(),
        };
        request.tool("mcp__first__approve", json!({}));
        if person {
            let ask = pending_ask(&host, &scope).await;
            assert_eq!(peer.calls().len(), 1);
            host.submit_person(HumanSubmission {
                scope: scope.clone(),
                request_id: ask.id,
                expected_revision: ask.revision,
                answer: api::HumanAnswer030::Decision {
                    decision: "yes".into(),
                    comment: String::new(),
                },
            })
            .await
            .unwrap();
        }
        let request = server.next().await;
        assert_eq!(peer.calls().len(), if person { 2 } else { 1 });
        if person {
            request.tool("mcp__first__blocked", json!({}));
            until(async || peer.calls().iter().any(|(_, tool)| tool == "blocked")).await;
            host.stop(EngineStopRequest {
                request_id: "stop-mcp".into(),
                system_start_id: scope.system_start_id,
                activation_id: None,
            })
            .await
            .unwrap();
            let error = tokio::time::timeout(
                WAIT_FOR,
                host.engine
                    .flow030()
                    .supervisors
                    .wait(&run.receipt.identity.system_start_id),
            )
            .await
            .unwrap()
            .unwrap_err();
            assert_eq!(error.code, "MCP_TERMINATION_UNCERTAIN");
            let projection = host
                .projection(
                    api::RuntimeProjectionQuery030 {
                        system_start_id: run.receipt.identity.system_start_id.clone(),
                        source: run.capture.source.clone(),
                        snapshot_id: None,
                    },
                    None,
                )
                .await
                .unwrap();
            assert!(projection.stopping && projection.termination_uncertain);
            host.pool.close().await;
        } else {
            request.tool(
                "agsdl_submit_result",
                json!({"results":{"report":{"body":"done"}}}),
            );
            completed(&finish(&host, &run).await);
            close(&host).await;
        }
        crate::test_context::mcp::assert_no_secrets(&f.data.join("engine.sqlite"));
    }
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn stop_committed_during_mcp_discovery_prevents_stdio_spawn() {
    let server = Server::start().await;
    let mut doc = document();
    doc["mcpServers"] = json!({"local":{"command":["/bin/sh","-c","exit 0"]}});
    doc["agents"]["worker"]["mcpServers"] = json!(["local"]);
    doc["agents"]["worker"]["permissions"] = json!({"preset":"read-only","rules":[
        {"action":"mcp","match":"local/check","effect":"allow"}
    ]});
    let f = Fixture::new(&doc.to_string());
    let host = open(&f, false)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let flow = host.engine.flow030();
    let prepared = flow.install_operator_test_hook("mcp.discovery.prepared");
    let spawn = flow.install_operator_test_hook("mcp.discovery.spawn");
    let run = start(&host).await;
    tokio::time::timeout(WAIT_FOR, prepared.reached())
        .await
        .unwrap();
    tokio::time::timeout(
        WAIT_FOR,
        host.stop(EngineStopRequest {
            request_id: "stop-before-stdio-spawn".into(),
            system_start_id: run.receipt.identity.system_start_id.clone(),
            activation_id: None,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    prepared.release();
    tokio::select! {
        _ = spawn.reached() => panic!("MCP discovery reached spawn after Stop committed"),
        state = finish(&host, &run) => assert_eq!(state["verdict"], "cancelled"),
    }
    close(&host).await;
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn confined_stdio_mcp_lists_and_calls_a_tool_then_joins_on_stop() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["mcpServers"] = json!({"local":{"command":["/bin/sh","-c",
        crate::test_context::mcp::STDIO_TOOL_SCRIPT,"mcp-fixture","sent","release"]}});
    doc["agents"]["worker"]["mcpServers"] = json!(["local"]);
    doc["agents"]["worker"]["permissions"] = json!({"preset":"read-only","rules":[
        {"action":"mcp","match":"local/check","effect":"allow"}
    ]});
    let f = Fixture::new(&doc.to_string());
    let host = open(&f, false)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    let request = server.next().await;
    assert!(request.body["tools"]
        .to_string()
        .contains("mcp__local__check"));
    request.tool("mcp__local__check", json!({}));
    until(async || f.workspace.join("sent").is_file()).await;
    host.stop(EngineStopRequest {
        request_id: "stop-stdio".into(),
        system_start_id: run.receipt.identity.system_start_id.clone(),
        activation_id: None,
    })
    .await
    .unwrap();
    assert_eq!(finish(&host, &run).await["verdict"], "cancelled");
    close(&host).await;
}

#[tokio::test]
async fn plan_tool_reports_structured_progress() {
    let mut server = Server::start().await;
    let mut doc = document();
    doc["agents"]["worker"]["plan"] = json!(true);
    let f = Fixture::new(&doc.to_string());
    let host = open(&f, true)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    let first = server.next().await;
    assert!(first.body["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tool| tool["function"]["name"] == "plan"));
    let step = json!({"step":"Inspect the memo","status":"in_progress"});
    first.tool("plan", json!({"steps":[step]}));
    let next = server.next().await;
    assert!(next.body["messages"]
        .to_string()
        .contains("Current work plan"));
    let bodies: Vec<String> =
        sqlx::query_scalar("SELECT ticket_json FROM engine_flow030_work_events WHERE decision_json IS NOT NULL ORDER BY rowid")
            .fetch_all(&host.pool)
            .await
            .unwrap();
    assert!(
        bodies
            .iter()
            .any(|body| body.contains("planPresented") && body.contains("in_progress")),
        "{bodies:?}"
    );
    submit_done(next);
    completed(&finish(&host, &run).await);
    close(&host).await;
}

fn submit_done(request: Request) {
    request.tool(
        "agsdl_submit_result",
        json!({"results":{"report":{"body":"done"}}}),
    );
}

fn plan_write_document(mode: &str) -> Value {
    let mut doc = document();
    doc["agents"]["worker"]["planFirst"] = json!(mode);
    doc["agents"]["worker"]["permissions"] = json!({"preset":"workspace","rules":[forbid_shell()]});
    doc
}

fn write_memo(request: Request) {
    request.tool("write", json!({"path":"memo.txt","content":"approved"}));
}

fn send_plan(request: Request, step: &str) {
    request.tool("plan", json!({"steps":[{"step":step,"status":"pending"}]}));
}

#[tokio::test]
async fn plan_first_auto_blocks_writes_until_a_plan() {
    let mut server = Server::start().await;
    let f = Fixture::new(&plan_write_document("auto").to_string());
    let host = open(&f, true)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    write_memo(server.next().await);
    let blocked = server.next().await;
    assert!(blocked.body["messages"]
        .to_string()
        .contains("Present a plan"));
    assert!(!f.workspace.join("memo.txt").exists());
    send_plan(blocked, "Write the memo");
    write_memo(server.next().await);
    submit_done(server.next().await);
    completed(&finish(&host, &run).await);
    assert_eq!(
        std::fs::read_to_string(f.workspace.join("memo.txt")).unwrap(),
        "approved"
    );
    close(&host).await;
}

#[tokio::test]
async fn plan_first_ask_stays_locked_until_human_accepts_a_revision() {
    for answer in ["accept revision", "reject", "timeout", "no person"] {
        let mut server = Server::start().await;
        let mut doc = plan_write_document("ask");
        if answer == "timeout" {
            doc["agents"]["worker"]["answerTimeoutMs"] = json!(250);
        }
        let f = Fixture::new(&doc.to_string());
        let mut cfg = config(&server.base_url, None);
        if answer == "no person" {
            cfg["personReviewAvailable"] = json!(false);
        }
        let host = open(&f, true)
            .await
            .with_argentic(serde_json::from_value(cfg).unwrap())
            .unwrap();
        let run = start(&host).await;
        let scope = persistence::public030::HumanScope030 {
            system_start_id: run.receipt.identity.system_start_id.clone(),
            source: run.capture.source.clone(),
        };
        let first = server.next().await;
        if answer == "accept revision" {
            let tools = [
                (
                    "plan",
                    json!({"steps":[{"step":"Write the memo","status":"pending"}]}),
                ),
                (
                    "plan",
                    json!({"steps":[{"step":"Bypass the comment","status":"pending"}]}),
                ),
                (
                    "agsdl_submit_result",
                    json!({"results":{"report":{"body":"done"}}}),
                ),
            ];
            let calls: Vec<_> = tools
                .into_iter()
                .enumerate()
                .map(|(index, (name, args))| {
                    json!({"index":index,"id":format!("plan-batch-{index}"),
                    "function":{"name":name,"arguments":args.to_string()}})
                })
                .collect();
            let chunk = json!({"choices":[{"index":0,"delta":{"tool_calls":calls},"finish_reason":"tool_calls"}]});
            first.send(
                StatusCode::OK,
                format!("data: {chunk}\n\ndata: [DONE]\n\n"),
                "text/event-stream",
            );
        } else {
            send_plan(first, "Write the memo");
        }
        if answer == "no person" {
            let state = finish(&host, &run).await;
            assert!(state.to_string().contains("PLAN_REJECTED"), "{state}");
            assert!(host.humans(scope, None).await.unwrap().requests.is_empty());
            close(&host).await;
            continue;
        }
        let ask = pending_ask(&host, &scope).await;
        assert!(matches!(
            ask.content,
            persistence::public030::HumanContent030::Plan { .. }
        ));
        assert!(!f.workspace.join("memo.txt").exists());
        if answer != "timeout" {
            answer_person(
                &host,
                &scope,
                ask,
                "no",
                if answer == "reject" {
                    ""
                } else {
                    "Describe the output first"
                },
            )
            .await;
        }
        if answer != "accept revision" {
            let state = finish(&host, &run).await;
            assert!(state.to_string().contains("PLAN_REJECTED"), "{state}");
            assert!(!f.workspace.join("memo.txt").exists());
        } else {
            let revision = server.next().await;
            assert!(revision.body["messages"]
                .to_string()
                .contains("Describe the output first"));
            for index in 1..=2 {
                let id = format!("plan-batch-{index}");
                assert!(revision.body["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|msg| {
                        msg["tool_call_id"] == id
                            && msg["content"].as_str().is_some_and(|text| {
                                text.contains("not executed because the plan must be revised")
                            })
                    }));
            }
            write_memo(revision);
            let blocked = server.next().await;
            assert!(blocked.body["messages"]
                .to_string()
                .contains("Present a plan"));
            assert!(!f.workspace.join("memo.txt").exists());
            send_plan(blocked, "Write a short memo about the result");
            let ask = pending_ask(&host, &scope).await;
            answer_person(&host, &scope, ask, "yes", "").await;
            write_memo(server.next().await);
            submit_done(server.next().await);
            completed(&finish(&host, &run).await);
            assert_eq!(
                std::fs::read_to_string(f.workspace.join("memo.txt")).unwrap(),
                "approved"
            );
        }
        close(&host).await;
    }
}

async fn answer_person(
    host: &Host,
    scope: &persistence::public030::HumanScope030,
    request: persistence::public030::PublicHumanRequest030,
    decision: &str,
    comment: &str,
) {
    host.submit_person(HumanSubmission {
        scope: scope.clone(),
        request_id: request.id,
        expected_revision: request.revision,
        answer: api::HumanAnswer030::Decision {
            decision: decision.into(),
            comment: comment.into(),
        },
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn model_reviewer_decides_exact_actions_and_fails_closed() {
    for (reply, person, approved, verdict, source) in [
        ("APPROVE", false, true, "APPROVE", "model"),
        ("DENY", true, false, "DENY", "model"),
        ("ESCALATE", true, true, "ESCALATE", "model"),
        ("ESCALATE", false, false, "ESCALATE", "model"),
        ("malformed", true, false, "DENY", "reviewUnavailable"),
        ("network", true, false, "DENY", "reviewUnavailable"),
    ] {
        let mut server = Server::start().await;
        let mut doc = document();
        doc["agents"]["worker"]["permissions"] = json!({"preset":"workspace","reviewer":"model","rules":[
            forbid_shell(), {"action":"write","match":"memo.txt","effect":"ask"}
        ]});
        let f = Fixture::new(&doc.to_string());
        let mut cfg = config(&server.base_url, None);
        cfg["providers"]["local"]["models"]
            .as_array_mut()
            .unwrap()
            .push(json!("reviewer"));
        cfg["reviewerModel"] = json!("local/reviewer");
        cfg["personReviewAvailable"] = json!(person);
        let host = open(&f, true)
            .await
            .with_argentic(serde_json::from_value(cfg).unwrap())
            .unwrap();
        let run = start(&host).await;
        let scope = persistence::public030::HumanScope030 {
            system_start_id: run.receipt.identity.system_start_id.clone(),
            source: run.capture.source.clone(),
        };
        write_memo(server.next().await);
        let review = server.next().await;
        assert_eq!(review.body["model"], "reviewer");
        assert!(review.body["tools"].as_array().is_none_or(Vec::is_empty));
        let context: Value =
            serde_json::from_str(review.body["messages"][1]["content"].as_str().unwrap()).unwrap();
        let args: Value = serde_json::from_str(
            context["action"]["nativeRequest"]["params"]["reviewArguments"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(args, json!({"path":"memo.txt","content":"approved"}));
        assert_eq!(context["permissionEffect"], "ask");
        assert!(context["recentConversation"]
            .to_string()
            .contains("Read memo.txt"));
        if reply == "network" {
            review.send(StatusCode::UNAUTHORIZED, "unavailable", "text/plain");
        } else {
            let text = if reply == "malformed" {
                "APPROVE!".into()
            } else {
                json!({"verdict":reply}).to_string()
            };
            send_text(review, &text);
        }
        if reply == "ESCALATE" && person {
            let ask = pending_ask(&host, &scope).await;
            assert!(!f.workspace.join("memo.txt").exists());
            answer_person(&host, &scope, ask, "yes", "").await;
        }
        submit_done(server.next().await);
        completed(&finish(&host, &run).await);
        assert_eq!(f.workspace.join("memo.txt").exists(), approved, "{reply}");
        let events: Vec<String> = sqlx::query_scalar(
            "SELECT ticket_json -> 'input' ->> 'eventJson' FROM engine_flow030_work_events WHERE decision_json IS NOT NULL ORDER BY rowid",
        ).fetch_all(&host.pool).await.unwrap();
        assert!(
            events.iter().any(|event| {
                let event: Value = serde_json::from_str(event).unwrap();
                event["event"]["kind"] == "reviewerDecision"
                    && event["event"]["verdict"] == verdict
                    && event["event"]["source"] == source
                    && !event.to_string().contains("reviewArguments")
            }),
            "{events:?}"
        );
        if reply != "ESCALATE" || !person {
            assert!(host.humans(scope, None).await.unwrap().requests.is_empty());
        }
        close(&host).await;
    }
}

fn send_text(request: Request, text: &str) {
    let chunk = json!({"choices":[{"index":0,"delta":{"content":text},"finish_reason":"stop"}]});
    request.send(
        StatusCode::OK,
        format!("data: {chunk}\n\ndata: [DONE]\n\n"),
        "text/event-stream",
    );
}

fn delegate_document(effect: &str) -> Value {
    let mut doc = document();
    doc["agents"]["worker"]["permissions"] = json!({
        "preset":"read-only",
        "reviewer":"person",
        "rules":[{"action":"delegate","effect":effect}, {"action":"read","match":"private.txt","effect":"deny"}]
    });
    doc
}

fn has_tool(request: &Request, name: &str) -> bool {
    request.body["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tool| tool["function"]["name"] == name)
}

fn delegate_task(request: Request) {
    request.tool("delegate", json!({"task":"Inspect the fixture"}));
}

#[tokio::test]
async fn delegate_opens_a_fresh_conversation_with_narrowed_permissions() {
    let mut server = Server::start().await;
    let f = Fixture::new(&delegate_document("allow").to_string());
    std::fs::write(f.workspace.join("private.txt"), "unreadable fixture data").unwrap();
    let host = open(&f, true)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    let parent = server.next().await;
    assert!(has_tool(&parent, "delegate"));
    delegate_task(parent);
    let child = server.next().await;
    assert!(has_tool(&child, "read"));
    for tool in ["write", "delegate", "shell", "agsdl_submit_result"] {
        assert!(!has_tool(&child, tool));
    }
    let messages = child.body["messages"].to_string();
    assert!(messages.contains("Inspect the fixture"));
    assert!(!messages.contains("Read memo.txt"));
    write_memo(child);
    let child = server.next().await;
    assert!(child.body["messages"]
        .to_string()
        .contains("Mutation tools are disabled"));
    child.tool("read", json!({"path":"private.txt"}));
    let child = server.next().await;
    let messages = child.body["messages"].to_string();
    assert!(messages.contains("Read permission denied"));
    assert!(!messages.contains("unreadable fixture data"));
    send_text(child, "The child finished its investigation.");
    let parent = server.next().await;
    let messages = parent.body["messages"].to_string();
    assert!(messages.contains("The child finished its investigation."));
    assert!(!messages.contains("Mutation tools are disabled"));
    submit_done(parent);
    completed(&finish(&host, &run).await);
    assert!(!f.workspace.join("memo.txt").exists());
    close(&host).await;
}

#[tokio::test]
async fn delegate_enforces_deny_and_one_shot_person_approval() {
    for (effect, accepted) in [("deny", false), ("ask", false), ("ask", true)] {
        let mut server = Server::start().await;
        let f = Fixture::new(&delegate_document(effect).to_string());
        let host = open(&f, true)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        let parent = server.next().await;
        assert_eq!(has_tool(&parent, "delegate"), effect == "ask");
        delegate_task(parent);
        if effect == "ask" {
            let scope = persistence::public030::HumanScope030 {
                system_start_id: run.receipt.identity.system_start_id.clone(),
                source: run.capture.source.clone(),
            };
            let ask = pending_ask(&host, &scope).await;
            answer_person(&host, &scope, ask, if accepted { "yes" } else { "no" }, "").await;
        }
        if accepted {
            send_text(server.next().await, "Completed.");
        }
        let parent = server.next().await;
        assert_eq!(
            parent.body["messages"]
                .to_string()
                .contains("Delegation permission denied"),
            !accepted,
        );
        submit_done(parent);
        completed(&finish(&host, &run).await);
        close(&host).await;
    }
}

#[tokio::test]
async fn delegation_charges_parent_turns_and_tokens() {
    for max_turns in [2, 3] {
        let mut server = Server::start().await;
        let mut doc = delegate_document("allow");
        doc["agents"]["worker"]["harnessSettings"]["maxTurns"] = json!(max_turns);
        doc["agents"]["worker"]["harnessSettings"]["maxTotalTokens"] = json!(100);
        let f = Fixture::new(&doc.to_string());
        let host = open(&f, true)
            .await
            .with_argentic(server.config(None))
            .unwrap();
        let run = start(&host).await;
        let parent = server.next().await;
        assert_eq!(parent.body["max_completion_tokens"], 100);
        let task = Request::tool_response("delegate", json!({"task":"Report success"}));
        let parent_usage = json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":5}});
        parent.send(
            StatusCode::OK,
            task.replace(
                "data: [DONE]",
                &format!("data: {parent_usage}\n\ndata: [DONE]"),
            ),
            "text/event-stream",
        );
        let child = server.next().await;
        assert_eq!(child.body["max_completion_tokens"], 85);
        let child_reply = json!({"choices":[{"index":0,"delta":{"content":"Child done"},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":20,"completion_tokens":5}});
        child.send(
            StatusCode::OK,
            format!("data: {child_reply}\n\ndata: [DONE]\n\n"),
            "text/event-stream",
        );
        if max_turns == 3 {
            let next = server.next().await;
            assert_eq!(next.body["max_completion_tokens"], 60);
            let result = Request::tool_response(
                "agsdl_submit_result",
                json!({"results":{"report":{"body":"done"}}}),
            );
            let usage = json!({"choices":[],"usage":{"prompt_tokens":15,"completion_tokens":5}});
            next.send(
                StatusCode::OK,
                result.replace("data: [DONE]", &format!("data: {usage}\n\ndata: [DONE]")),
                "text/event-stream",
            );
            completed(&finish(&host, &run).await);
        } else {
            let state = finish(&host, &run).await;
            assert_eq!(state["verdict"], "failed", "{state}");
        }
        close(&host).await;
    }
}

#[tokio::test]
async fn stop_joins_the_child_before_recording_physical_end() {
    let mut server = Server::start().await;
    let f = Fixture::new(&delegate_document("allow").to_string());
    let host = open(&f, true)
        .await
        .with_argentic(server.config(None))
        .unwrap();
    let run = start(&host).await;
    delegate_task(server.next().await);
    let (dropped, did_drop) = oneshot::channel::<()>();
    let (streaming, did_stream) = oneshot::channel();
    let body = futures::stream::unfold(
        (Some(streaming), dropped),
        |(streaming, marker)| async move {
            if let Some(streaming) = streaming {
                let _ = streaming.send(());
                Some((Ok::<_, std::io::Error>("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"child is active\"}}]}\n\n"), (None, marker)))
            } else {
                std::future::pending().await
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
    host.stop(EngineStopRequest {
        request_id: "stop".into(),
        system_start_id: run.receipt.identity.system_start_id.clone(),
        activation_id: None,
    })
    .await
    .unwrap();
    assert_eq!(finish(&host, &run).await["verdict"], "cancelled");
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM engine_flow030_events WHERE json_extract(event_json,'$.kind.PhysicalEnded.evidence.descendants_joined')=1 AND receipt_json IS NOT NULL",
    ).fetch_one(&host.pool).await.unwrap();
    assert_eq!(count, 1);
    assert!(tokio::time::timeout(WAIT_FOR, did_drop)
        .await
        .unwrap()
        .is_err());
    close(&host).await;
}
