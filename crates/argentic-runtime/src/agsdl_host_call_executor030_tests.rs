//! Production call adapter, real confined children, native supervisor and Rust adapters.
use super::*;

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
async fn host_fixture(document: Value) -> (Fixture, crate::tasks::StreamTasks) {
    host_fixture_with_entry(document, json!({})).await
}
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
async fn host_fixture_with_entry(
    document: Value,
    entry: Value,
) -> (Fixture, crate::tasks::StreamTasks) {
    let f =
        Fixture::new(&json!({"document":document,"entry":entry,"timeline":[]}).to_string()).await;
    let transport = Arc::new(crate::agsdl_host_call_executor030::NativeCallTransport030 {
        config: f.config.clone(),
        humans: Some(Arc::new(
            crate::agsdl_host_human_executor030::CallHumans030 {
                app: f.host.clone(),
                pool: f.pool.clone(),
                identity: f.identity.clone(),
            },
        )),
    });
    let tasks = start_calls(&f, f.config.clone(), transport);
    (f, tasks)
}

#[cfg(unix)]
fn start_calls(
    f: &Fixture,
    config: crate::host::HostContext,
    transport: Arc<dyn crate::agsdl_host_call_executor030::CallTransport030>,
) -> crate::tasks::StreamTasks {
    let tasks = crate::tasks::StreamTasks::default();
    let host = &f.host;
    host.flow030()
        .supervisors
        .reserve(&f.owner.system_start_id)
        .unwrap();
    spawn_supervisor030(
        f.host.clone(),
        f.pool.clone(),
        config.clone(),
        tasks.clone(),
        f.owner.clone(),
        Executors030 {
            agents: Arc::new(Executor {
                whole_stop: Default::default(),
                steer_script: Default::default(),
                deferred: Default::default(),
                plans: Default::default(),
                running: Arc::new(Mutex::new(Vec::new())),
                wake: watch::channel(0).0,
            }),
            calls: Some(Arc::new(
                crate::agsdl_host_call_executor030::CallExecutor030 {
                    config,
                    pool: f.pool.clone(),
                    tasks: tasks.clone(),
                    owner: f.owner.clone(),
                    transport,
                },
            )),
            humans: Some(Arc::new(
                crate::agsdl_host_human_executor030::HumanExecutor030 {
                    pool: f.pool.clone(),
                    tasks: tasks.clone(),
                },
            )),
            clock: Some(Arc::new(
                crate::agsdl_host_human_executor030::HumanClockRuntime030 {
                    app: f.host.clone(),
                    pool: f.pool.clone(),
                    tasks: tasks.clone(),
                    identity: f.identity.clone(),
                },
            )),
        },
    )
    .unwrap();
    tasks
}
#[cfg(unix)]
async fn finish(f: &Fixture) -> crate::persistence::flow_store030::FlowView030 {
    let host = &f.host;
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(25),
        host.flow030().supervisors.wait(&f.owner.system_start_id),
    )
    .await;
    if result.is_err() {
        host.flow030().supervisors.cancel(&f.owner.system_start_id);
    }
    let cleanup = host
        .flow030()
        .supervisors
        .wait(&f.owner.system_start_id)
        .await;
    assert!(result.is_ok(), "Native call did not terminate");
    cleanup.unwrap();
    let view = flow::inspect_run(host.flow030(), &host, &f.pool, &f.owner)
        .await
        .unwrap()
        .flow;
    assert!(view.state.activations.iter().all(|a| !a.physical));
    let page = crate::agsdl_host_projection030::read(
        &host,
        &f.pool,
        crate::agsdl_host_projection030::RuntimeProjectionQuery030 {
            system_start_id: f.owner.system_start_id.clone(),
            source: serde_json::from_value(f.plan.source.clone()).unwrap(),
            snapshot_id: Some(f.identity.snapshot_id.clone()),
        },
        None,
    )
    .await
    .unwrap();
    for item in page.items {
        if let crate::agsdl_host_projection030::RuntimeProjectionItem030::Activation { value } =
            item
        {
            let activation = view
                .state
                .activations
                .iter()
                .find(|a| a.id == value.id)
                .unwrap();
            assert_eq!(
                value.call_result_json,
                activation
                    .result
                    .as_ref()
                    .filter(|_| {
                        f.plan.steps.iter().any(|s| {
                            Some(&s.address) == activation.step.as_ref() && s.r#type == "call"
                        })
                    })
                    .map(|r| serde_json::to_string(r).unwrap())
            );
        }
    }
    f.pool.close().await;
    view
}
fn document(step: Value) -> Value {
    json!({"contract":"agsdl-0.3.0","id":"native-calls","agents":{},"flow":{"entry":"check","steps":{"check":step}}})
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_commands_normalize_and_route_without_git_or_agents() {
    for (step, expected) in [
        (
            json!({"type":"call","command":"printf '{\"n\":9007199254740993}'; printf diagnostic >&2; exit 7","parse":"json","next":{"passed":[],"failed":[]}}),
            json!({"outcome":"failed","data":{"exitCode":7,"output":"{\"n\":9007199254740993}diagnostic","value":{"n":9007199254740993u64}}}),
        ),
        (
            json!({"type":"call","command":"printf 'a\\n\\nb\\r\\n'","parse":"lines"}),
            json!({"outcome":"passed","data":{"exitCode":0,"output":"a\n\nb\r\n","value":["a","b"]}}),
        ),
    ] {
        let (f, _) = host_fixture(document(step)).await;
        assert!(!f._temp.path().join("workspace/.git").exists());
        let view = finish(&f).await;
        assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
        let output: Value = serde_json::from_str(
            serde_json::to_value(&view.state.activations[0].result).unwrap()["outputJson"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(output, expected);
    }
    let mut doc = document(
        json!({"type":"call","command":"printf invalid; exit 3","parse":"json","onError":"recover"}),
    );
    doc["flow"]["steps"]["recover"] = json!({"type":"call","command":"printf recovered"});
    let (f, _) = host_fixture(doc).await;
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    assert!(
        matches!(&view.state.activations[0].result, Some(WorkResultRecord030::Failed { error }) if error.code == "OUTPUT_INVALID")
    );
    assert_eq!(view.state.activations.len(), 2);
    let mut doc = document(
        json!({"type":"call","command":"printf '\\377'","parse":"json","onError":"recover"}),
    );
    doc["flow"]["steps"]["recover"] = json!({"type":"call","command":"true"});
    let (f, _) = host_fixture(doc).await;
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    assert!(
        matches!(&view.state.activations[0].result, Some(WorkResultRecord030::Failed { error }) if error.code == "OUTPUT_INVALID")
    );
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_call_stop_joins_real_child_and_never_starts_successor() {
    let doc = document(
        json!({"type":"call","command":"sleep 120 & printf '%s %s' \"$$\" \"$!\" > pids; wait","next":"after","onError":"after"}),
    );
    stop_and_assert(doc, false).await;
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_shutdown_joins_real_call_and_descendant_before_revocation() {
    let doc = document(
        json!({"type":"call","command":"sleep 120 & printf '%s %s' \"$$\" \"$!\" > pids; wait","next":"after","onError":"after"}),
    );
    stop_and_assert(doc, true).await;
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_mcp_stop_joins_real_server_and_child() {
    let server = r#"while IFS= read -r line; do case "$line" in *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":"1","result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}' ;; *'"method":"tools/call"'*) sleep 120 & printf '%s %s' "$$" "$!" > pids; wait ;; esac; done"#;
    let mut doc = document(
        json!({"type":"call","mcp":{"server":"local","tool":"check"},"next":"after","onError":"after"}),
    );
    doc["mcpServers"] = json!({"local":{"command":["/bin/sh","-c",server]}});
    stop_and_assert(doc, false).await;
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
async fn stop_and_assert(mut doc: Value, shutdown: bool) {
    doc["flow"]["steps"]["after"] = json!({"type":"call","command":"printf forbidden > successor"});
    let (f, tasks) = host_fixture(doc).await;
    let path = f._temp.path().join("workspace/pids");
    let observed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(raw) = std::fs::read_to_string(&path) {
                let pids = raw
                    .split_whitespace()
                    .map(|p| p.parse::<i32>().unwrap())
                    .collect::<Vec<_>>();
                if pids.len() == 2 {
                    return pids;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    let host = &f.host;
    let view = if shutdown {
        let generation = host.start_lease().await.unwrap().generation().to_owned();
        let control = host
            .flow030()
            .retiring_controls(&generation)
            .unwrap()
            .into_iter()
            .find(|control| control.identity().system_start_id == f.owner.system_start_id)
            .unwrap();
        let approved = host.shutdown_active_runs(&f.pool).await.unwrap();
        assert_eq!(approved, [f.owner.system_start_id.clone()]);
        tokio::time::timeout(
            std::time::Duration::from_secs(20),
            host.prepare_shutdown(&f.pool, &tasks, &approved),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(host.start_lease().await.is_err());
        control.read(&f.pool).await.unwrap()
    } else {
        crate::agsdl_host_stop030::request(
            host.flow030(),
            &f.pool,
            &tasks,
            &crate::persistence::dto::EngineStopRequest {
                system_start_id: f.owner.system_start_id.clone(),
                request_id: "stop-command".into(),
                activation_id: None,
            },
        )
        .await
        .unwrap();
        finish(&f).await
    };
    let pids = observed.expect("Confined command did not start");
    assert_eq!(view.state.verdict.as_deref(), Some("cancelled"));
    assert!(!view.state.activations.iter().any(|work| work.physical));
    assert_eq!(view.state.activations.len(), 1);
    assert!(!f._temp.path().join("workspace/successor").exists());
    for pid in pids {
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "Child {pid} survived Stop"
        );
    }
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_stdio_mcp_preserves_structured_error_result_and_operands() {
    // The server checks the exact argument bytes before replying, like a real
    // stdio server. No model-side result shortcuts or global MCP clients.
    let server = r#"while IFS= read -r line; do case "$line" in *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":"1","result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}}' ;; *'"method":"tools/call"'*) case "$line" in *'9007199254740993'*) printf '%s\n' '{"jsonrpc":"2.0","id":"2","result":{"content":[{"type":"text","text":"failed check"}],"structuredContent":{"n":9007199254740993},"isError":true}}' ;; *) exit 12 ;; esac ;; esac; done"#;
    let mut doc = document(
        json!({"type":"call","mcp":{"server":"local","tool":"check"},"arguments":{"n":{"value":9007199254740993u64}},"next":{"passed":[],"failed":"after"}}),
    );
    doc["mcpServers"] = json!({"local":{"command":["/bin/sh","-c",server]}});
    doc["flow"]["steps"]["after"] = json!({"type":"call","command":"printf after"});
    let (f, _) = host_fixture(doc).await;
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    assert_eq!(view.state.activations.len(), 2);
    assert_eq!(
        serde_json::to_value(&view.state.activations[0].result).unwrap()["outputJson"].as_str(),
        Some(r#"{"outcome":"failed","data":{"n":9007199254740993}}"#)
    );
}

#[test]
fn native_preflight_opens_humans_timeouts_and_parallelism_but_keeps_http_closed() {
    let base = document(json!({"type":"call","command":"true"}));
    assert!(
        crate::agsdl_host_qualification030::product_slice030(&base.to_string(), None).is_none()
    );
    let mut steer = base.clone();
    steer["agents"] = json!({"worker":{"harness":"codex","permissions":"read-only"}});
    steer["flow"]["steps"]["check"] = json!({"type":"agent","agent":"worker","delivery":"steer"});
    assert!(
        crate::agsdl_host_qualification030::product_slice030(&steer.to_string(), None).is_none()
    );
    let mut doc = base.clone();
    doc["mcpServers"] = json!({"remote":{"url":"https://example.invalid/mcp"}});
    let diagnostic =
        crate::agsdl_host_qualification030::product_slice030(&doc.to_string(), None).unwrap();
    assert_eq!(diagnostic.pointer, "/mcpServers/remote/url");
    assert!(diagnostic.message.contains("physical cancellation"));
    let mut doc = base.clone();
    doc["flow"]["steps"]["check"]["type"] = json!("approval");
    assert!(crate::agsdl_host_qualification030::product_slice030(&doc.to_string(), None).is_none());
    doc["flow"]["steps"]["check"]["timeoutMs"] = json!(10);
    assert!(crate::agsdl_host_qualification030::product_slice030(&doc.to_string(), None).is_none());
    let plan = json!({"steps":[],"routes":[{"targets":[{},{}]}]});
    assert!(crate::agsdl_host_qualification030::product_slice030(
        &base.to_string(),
        Some(&plan.to_string())
    )
    .is_none());
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_mcp_operand_failure_routes_without_launching_server() {
    let mut doc = document(
        json!({"type":"call","mcp":{"server":"local","tool":"check"},"arguments":{"missing":{"path":["missing"]}},"onError":"recover"}),
    );
    doc["mcpServers"] =
        json!({"local":{"command":["/bin/sh","-c","printf forbidden > server-started"]}});
    doc["flow"]["steps"]["recover"] = json!({"type":"call","command":"printf recovered"});
    let (f, _) = host_fixture(doc).await;
    let workspace = f._temp.path().join("workspace");
    let server_started = workspace.join("server-started");
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    assert!(
        matches!(&view.state.activations[0].result, Some(WorkResultRecord030::Failed { error }) if error.code == "OPERAND")
    );
    assert!(view.effects.iter().all(|row| row["status"] == "settled"));
    assert!(!server_started.exists());
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_command_deadline_waits_for_the_real_child_before_routing_timeout() {
    let mut doc = document(
        json!({"type":"call","command":"sleep 120 & child=$!; printf '%s' \"$child\" > timeout-child; wait","timeoutMs":700,"onError":"recover"}),
    );
    doc["flow"]["steps"]["recover"] =
        json!({"type":"call","command":"test ! -e timeout-forbidden; printf recovered"});
    let (f, _) = host_fixture(doc).await;
    let file = f._temp.path().join("workspace/timeout-child");
    let pid = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Ok(raw) = std::fs::read_to_string(&file) {
                if let Ok(pid) = raw.parse::<i32>() {
                    break pid;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    assert!(
        matches!(&view.state.activations[0].result,Some(WorkResultRecord030::Failed{error}) if error.code=="TIMEOUT")
    );
    assert_eq!(
        unsafe { libc::kill(pid, 0) },
        -1,
        "Timed-out child must be absent before the route"
    );
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn native_mcp_question_answers_and_expires_while_machine_time_is_paused() {
    let server = r#"while IFS= read -r line; do case "$line" in *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":"1","result":{"protocolVersion":"2025-11-25","capabilities":{},"serverInfo":{"name":"question-fixture","version":"1"}}}' ;; *'"method":"tools/call"'*) printf '%s\n' '{"jsonrpc":"2.0","id":"person","method":"elicitation/create","params":{"message":"Release note?","requestedSchema":{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}}}' ;; *'"id":"person"'*) printf '%s\n' '{"jsonrpc":"2.0","id":"2","result":{"content":[],"structuredContent":{"answered":true},"isError":false}}' ;; esac; done"#;
    for expire in [false, true] {
        let mut step =
            json!({"type":"call","mcp":{"server":"local","tool":"check"},"timeoutMs":1500});
        if expire {
            step["answerTimeoutMs"] = json!(200);
        }
        let mut doc = document(step);
        doc["mcpServers"] = json!({"local":{"command":["/bin/sh","-c",server]}});
        let (f, _) = host_fixture(doc).await;
        let scope = crate::persistence::public030::HumanScope030 {
            system_start_id: f.identity.system_start_id.clone(),
            source: serde_json::from_value(f.plan.source.clone()).unwrap(),
        };
        let page = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let page =
                    crate::agsdl_host_human_projection030::read(&f.pool, scope.clone(), None)
                        .await
                        .unwrap();
                if !page.requests.is_empty() {
                    break page;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        if !expire {
            tokio::time::sleep(std::time::Duration::from_millis(1600)).await;
            let host = &f.host;
            let broker = host
                .flow030()
                .humans
                .broker(&host, &f.pool, &f.identity)
                .await
                .unwrap();
            let _operation = broker.operation.lock().await;
            broker.submit(&host,crate::agsdl_host_human030::HumanSubmission030 {scope:scope.clone(),request_id:page.requests[0].id.clone(),expected_revision:"0".into(),answer:crate::agsdl_host_human030::HumanAnswer030::Question {answer_json:r#"{"action":"accept","content":{"text":"Écrit par une personne."}}"#.into()}},crate::agsdl_host_human030::HumanOrigin030::Person).await.unwrap();
            broker.pump(&host, &CancellationToken::new()).await.unwrap();
        }
        let view = finish(&f).await;
        assert_eq!(
            view.state.verdict.as_deref(),
            Some("succeeded"),
            "Machine timeout must pause during the human wait"
        );
    }
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn nested_composition_call_outputs_use_the_actual_joined_call_owner() {
    let doc = json!({"contract":"agsdl-0.3.0","id":"nested-native-calls","agents":{},
    "compositions":{
        "inner":{"entry":"call","outputs":["done"],"steps":{"call":{"type":"call","command":"printf nested","next":{"output":"done"}}}},
        "outer":{"entry":"inner","outputs":["done"],"steps":{"inner":{"type":"compose","composition":"inner","next":{"done":{"output":"done"}}}}}
    },
    "flow":{"entry":"first","steps":{
        "first":{"type":"compose","composition":"outer","next":{"done":"second"}},
        "second":{"type":"compose","composition":"outer","next":{"done":"after"}},
        "after":{"type":"call","command":"printf after"}
    }}});
    let (f, _) = host_fixture(doc).await;
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    let calls = view
        .state
        .activations
        .iter()
        .filter(|a| a.start_effect_id.is_some())
        .map(|a| a.step.as_ref().unwrap().join("/"))
        .collect::<Vec<_>>();
    assert_eq!(calls, ["first/inner/call", "second/inner/call", "after"]);
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[tokio::test]
async fn nested_entry_approval_keeps_host_prompt_provenance_through_its_output() {
    let doc = json!({"contract":"agsdl-0.3.0","id":"nested-entry-approval","agents":{},
    "compositions":{
        "inner":{"entry":"approve","outputs":["done"],"steps":{"approve":{"type":"approval","next":{"yes":{"output":"done"}}}}},
        "outer":{"entry":"inner","outputs":["done"],"steps":{"inner":{"type":"compose","composition":"inner","next":{"done":{"output":"done"}}}}}
    },
    "flow":{"entry":"outer","steps":{
        "outer":{"type":"compose","composition":"outer","next":{"done":"after"}},
        "after":{"type":"call","command":"printf after"}
    }}});
    let (f, _) = host_fixture_with_entry(doc, json!({"prompt":"Synthetic entry"})).await;
    let scope = crate::persistence::public030::HumanScope030 {
        system_start_id: f.identity.system_start_id.clone(),
        source: serde_json::from_value(f.plan.source.clone()).unwrap(),
    };
    let page = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let page = crate::agsdl_host_human_projection030::read(&f.pool, scope.clone(), None)
                .await
                .unwrap();
            if !page.requests.is_empty() {
                break page;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let host = &f.host;
    let broker = host
        .flow030()
        .humans
        .broker(&host, &f.pool, &f.identity)
        .await
        .unwrap();
    {
        let _operation = broker.operation.lock().await;
        broker
            .submit(
                &host,
                crate::agsdl_host_human030::HumanSubmission030 {
                    scope,
                    request_id: page.requests[0].id.clone(),
                    expected_revision: "0".into(),
                    answer: crate::agsdl_host_human030::HumanAnswer030::Approval {
                        choice: "yes".into(),
                        comment: "Synthetic comment".into(),
                    },
                },
                crate::agsdl_host_human030::HumanOrigin030::Person,
            )
            .await
            .unwrap();
        broker.pump(&host, &CancellationToken::new()).await.unwrap();
    }
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    let approval = view
        .state
        .activations
        .iter()
        .find(|a| a.step == Some(vec!["outer".into(), "inner".into(), "approve".into()]))
        .unwrap();
    let Some(WorkResultRecord030::Completed { output_json, .. }) = &approval.result else {
        panic!("approval result missing")
    };
    let output: Value = serde_json::from_str(output_json).unwrap();
    assert_eq!(output["message"]["prompt"], "Synthetic entry");
}

#[cfg(unix)]
#[tokio::test]
async fn retained_stdio_refuses_unverified_programs_and_reserved_secrets_before_vault_access() {
    use crate::{
        agsdl_host_call_executor030::mcp_server,
        agsdl_host_calls::CommandGrant,
        agsdl_host_confinement::{OsCeiling, SandboxPolicy},
    };
    use std::{fs, os::unix::fs::PermissionsExt};

    let project = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().canonicalize().unwrap();
    let mutable = root.join("server");
    fs::write(&mutable, "#!/bin/sh\nexit 0\n").unwrap();
    fs::set_permissions(&mutable, fs::Permissions::from_mode(0o700)).unwrap();
    let sandbox =
        SandboxPolicy::from_authorized_ceilings("mcp-config-test".into(), &[OsCeiling::default()])
            .unwrap();
    let doc = json!({
        "projectFormat":"agsdl-project-1","contract":"agsdl-0.4.0",
        "id":"retained-mcp-refusal","agents":{},
        "mcpServers":{
            "absolute":{"command":[mutable]},
            "shell":{"command":["/bin/sh",mutable]},
            "inline":{"command":["sh","-c","exit 0"]},
            "system":{"command":["/bin/echo"]},
            "relative":{"command":["relative.sh"]},
            "path":{"command":["sh","server.sh"]},
            "loader":{"command":["sh","server.sh"]}
        },
        "mcpFiles":{"relative":["relative.sh"],"path":["server.sh"],"loader":["server.sh"]},
        "secrets":{"file":"requirements.json"}
    });
    fs::write(project.path().join("agsdl.json"), doc.to_string()).unwrap();
    fs::write(project.path().join("server.sh"), "#!/bin/sh\nexit 0\n").unwrap();
    let relative = project.path().join("relative.sh");
    fs::write(&relative, "#!  tools/interp\nexit 0\n").unwrap();
    fs::set_permissions(&relative, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(
        project.path().join("requirements.json"),
        r#"{"absolute":["TOKEN"],"relative":["TOKEN"]}"#,
    )
    .unwrap();
    let f = Fixture::new_with_project(r#"{"entry":{},"timeline":[]}"#, false, Some(project.path()))
        .await;
    let mut capture = flow::inspect_run(f.host.flow030(), &f.host, &f.pool, &f.owner)
        .await
        .unwrap()
        .capture;
    // Older retained captures could declare reserved names.
    let mut private: Value = serde_json::from_str(&capture.private_capture_json).unwrap();
    let mut scope: Value =
        serde_json::from_str(private["callScopeJson"].as_str().unwrap()).unwrap();
    scope["projectMcp"]["secrets"]["path"] = json!(["PATH"]);
    scope["projectMcp"]["secrets"]["loader"] = json!(["DYLD_INSERT_LIBRARIES"]);
    private["callScopeJson"] = json!(scope.to_string());
    capture.private_capture_json = private.to_string();
    let config = f.config.clone().with_mcp_secrets(Arc::new(|_, _, _| {
        panic!("Refused MCP configuration must not query the vault")
    }));
    for (server, expected) in [
        ("absolute", "CALL_FAILED"),
        ("shell", "CALL_FAILED"),
        ("inline", "CALL_FAILED"),
        ("system", "CALL_FAILED"),
        ("relative", "CALL_FAILED"),
        ("path", "MCP_SECRET_RESERVED"),
        ("loader", "MCP_SECRET_RESERVED"),
    ] {
        let grant = CommandGrant {
            workspace: root.clone(),
            environment: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            sandbox: sandbox.clone(),
        };
        let error = mcp_server(&config, grant, &capture, server)
            .err()
            .unwrap_or_else(|| panic!("MCP config for {server} was accepted"));
        assert_eq!(error.code, expected, "{server}: {error:?}");
        if server == "relative" {
            assert!(
                error.detail.contains("absolute shebang interpreter"),
                "{error:?}"
            );
        }
    }
}

#[cfg(unix)]
struct RetainedStdio(crate::host::HostContext);

#[cfg(unix)]
impl crate::agsdl_host_call_executor030::CallTransport030 for RetainedStdio {
    fn start(
        &self,
        grant: crate::agsdl_host_calls::CommandGrant,
        prepared: crate::agsdl_host_calls030::PreparedCall030,
        capture: StartCapture030,
        attempt: crate::agsdl_host_calls030::CallAttempt030,
        control: crate::agsdl_host_calls::Control,
    ) -> crate::agsdl_host_calls::Result<
        BoxFuture<'static, Result<crate::persistence::call_protocol030::CallEnd030>>,
    > {
        use crate::agsdl_host_call_executor030::{
            mcp_server, run_transport, NoHuman, StartedTransport,
        };
        let crate::agsdl_host_calls030::PreparedCall030::Mcp { server, .. } = &prepared else {
            unreachable!();
        };
        let config = mcp_server(&self.0, grant, &capture, server)?;
        let started = crate::agsdl_host_calls::raw_mcp::start_test_stdio(config)?;
        Ok(Box::pin(async move {
            run_transport(
                StartedTransport::Mcp(started),
                &prepared,
                &attempt,
                &control,
                Arc::new(NoHuman),
            )
            .await
        }))
    }
}

#[cfg(unix)]
#[tokio::test]
async fn retained_stdio_uses_workspace_and_isolates_secrets_across_servers_and_reopen() {
    use std::{fs, os::unix::fs::PermissionsExt};
    const FIRST: &str = "runtime-test-first-secret";
    const SECOND: &str = "runtime-test-second-secret";
    let project = tempfile::tempdir().unwrap();
    fs::create_dir(project.path().join("mcp")).unwrap();
    let doc = json!({
        "projectFormat":"agsdl-project-1","contract":"agsdl-0.4.0","id":"retained-mcp","agents":{},
        "mcpServers":{"first":{"command":["./mcp/first.sh"]},"second":{"command":["sh","mcp/second.sh"]}},
        "mcpFiles":{"first":["mcp/first.sh"],"second":["mcp/second.sh"]},
        "secrets":{"file":"requirements.json"},
        "flow":{"entry":"first","steps":{
            "first":{"type":"call","mcp":{"server":"first","tool":"check"},"next":"second"},
            "second":{"type":"call","mcp":{"server":"second","tool":"check"}}
        }}
    });
    fs::write(project.path().join("agsdl.json"), doc.to_string()).unwrap();
    fs::write(
        project.path().join("requirements.json"),
        r#"{"first":["FIRST_TOKEN"],"second":["SECOND_TOKEN"]}"#,
    )
    .unwrap();
    for (server, name, other, size) in [
        ("first", "FIRST_TOKEN", "SECOND_TOKEN", FIRST.len()),
        ("second", "SECOND_TOKEN", "FIRST_TOKEN", SECOND.len()),
    ] {
        let script = format!(
            r#"#!/bin/sh
set -eu
test -f cwd-marker
test "${{#{name}}}" = "{size}"
test -z "${{{other}+set}}"
printf '%s\n' "${{{name}}}" >&2
IFS= read -r init
printf '%s\n' '{{"jsonrpc":"2.0","id":"1","result":{{"protocolVersion":"2025-11-25","capabilities":{{}},"serverInfo":{{"name":"fixture","version":"1"}}}}}}'
IFS= read -r initialized
IFS= read -r call
printf '%s\n' '{{"jsonrpc":"2.0","id":"2","result":{{"structuredContent":{{"checked":"{server}"}},"content":[]}}}}'
"#
        );
        let path = project.path().join(format!("mcp/{server}.sh"));
        fs::write(&path, script).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let f = Fixture::new_with_project(r#"{"entry":{},"timeline":[]}"#, false, Some(project.path()))
        .await;
    let unstarted =
        Fixture::new_with_project(r#"{"entry":{},"timeline":[]}"#, false, Some(project.path()))
            .await;
    project.close().unwrap();
    fs::write(f._temp.path().join("workspace/cwd-marker"), "").unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let observed = seen.clone();
    let config = f
        .config
        .clone()
        .with_mcp_secrets(Arc::new(move |source, server, names| {
            assert_eq!(source.owner.project_id, "project");
            observed.lock().unwrap().push(server.to_owned());
            let (key, value) = match server {
                "first" => ("FIRST_TOKEN", FIRST),
                "second" => ("SECOND_TOKEN", SECOND),
                _ => unreachable!(),
            };
            assert_eq!(names, [key]);
            Some(BTreeMap::from([(key.into(), value.into())]))
        }));
    let _tasks = start_calls(&f, config.clone(), Arc::new(RetainedStdio(config)));
    let view = finish(&f).await;
    assert_eq!(view.state.verdict.as_deref(), Some("succeeded"));
    assert_eq!(*seen.lock().unwrap(), ["first", "second"]);
    for (index, name) in ["first", "second"].into_iter().enumerate() {
        assert_eq!(
            serde_json::from_str::<Value>(
                serde_json::to_value(&view.state.activations[index].result).unwrap()["outputJson"]
                    .as_str()
                    .unwrap()
            )
            .unwrap(),
            json!({"outcome":"passed","data":{"checked":name}}),
        );
    }
    for (fixture, expected) in [(&f, "succeeded"), (&unstarted, "interrupted")] {
        fixture.pool.close().await;
        let path = fixture._temp.path().join("run.db");
        let options = sqlx::sqlite::SqliteConnectOptions::new().filename(&path);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        crate::api::interrupt_previous_runs(&pool).await.unwrap();
        let projection = fixture
            .host
            .runtime_projection(
                &pool,
                crate::api::RuntimeProjectionQuery030 {
                    system_start_id: fixture.identity.system_start_id.clone(),
                    source: serde_json::from_value(fixture.plan.source.clone()).unwrap(),
                    snapshot_id: Some(fixture.identity.snapshot_id.clone()),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(projection.verdict.as_deref(), Some(expected));
        pool.close().await;
        let db = fs::read(path).unwrap();
        for secret in [FIRST, SECOND] {
            assert!(!db
                .windows(secret.len())
                .any(|window| window == secret.as_bytes()));
        }
    }
    assert_eq!(*seen.lock().unwrap(), ["first", "second"]);
}
