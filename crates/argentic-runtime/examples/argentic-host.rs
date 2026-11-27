//! Explicit JSON-lines operator for the standalone host. See ../HOST.md.
use argentic_runtime::{
    api::*,
    persistence::{self, dto::*, public030::*},
    standalone::Host,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeSet, future::Future, io, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt},
    sync::{mpsc, oneshot},
};

#[derive(Deserialize)]
#[serde(
    tag = "op",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum Command {
    Preflight {
        entry: Option<EngineEntry>,
    },
    Start {
        request_id: String,
        entry: Option<EngineEntry>,
    },
    Runs,
    Inspect {
        query: RuntimeProjectionQuery030,
        continuation: Option<RuntimeProjectionContinuation030>,
    },
    Humans {
        scope: HumanScope030,
        continuation: Option<HumanContinuation030>,
    },
    Answer {
        input: HumanSubmission,
    },
    ControlRun {
        input: OperatorRequest,
    },
    Deliver {
        input: DeliveryRequest030,
    },
    Stop {
        system_start_id: String,
        request_id: String,
    },
    Shutdown {
        approved: Vec<String>,
    },
    Quit {
        approved: Vec<String>,
    },
}
async fn retire(host: &Host, approved: &[String]) -> persistence::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(10), host.shutdown(approved))
        .await
        .map_err(|_| {
            persistence::failure(
                "SHUTDOWN_PENDING",
                "host",
                "Retirement is incomplete. Inspect and retry the same shutdown.",
            )
        })?
}
async fn execute(
    host: &Host,
    command: Command,
    allowed: &BTreeSet<String>,
) -> persistence::Result<(Value, bool)> {
    let result = match command {
        Command::Preflight { entry } => {
            host.preflight_with_harnesses(entry.as_ref(), allowed)
                .await?
        }
        Command::Start { request_id, entry } => {
            let run = host
                .start_with_harnesses(request_id, entry, allowed)
                .await?;
            json!({"receipt":run.receipt,"source":run.capture.source})
        }
        Command::Runs => json!(host.runs().await?),
        Command::Inspect {
            query,
            continuation,
        } => serde_json::to_value(host.projection(query, continuation).await?)?,
        Command::Humans {
            scope,
            continuation,
        } => serde_json::to_value(host.humans(scope, continuation).await?)?,
        Command::Answer { input } => json!({"receipt":host.submit_person(input).await?}),
        Command::ControlRun { input } => serde_json::to_value(host.control_run(input).await?)?,
        Command::Deliver { input } => serde_json::to_value(host.deliver(input).await?)?,
        Command::Stop {
            system_start_id,
            request_id,
        } => {
            let stopped=tokio::time::timeout(std::time::Duration::from_secs(10),host.stop(EngineStopRequest {system_start_id,request_id,activation_id:None})).await
                .map_err(|_|persistence::failure("STOP_PENDING","host","Stop is committed or unresolved. Inspect and retry; no physical receipt is implied."))??;
            serde_json::to_value(stopped)?
        }
        Command::Shutdown { approved } => {
            retire(host, &approved).await?;
            json!({"drained":true})
        }
        Command::Quit { approved } => {
            retire(host, &approved).await?;
            host.close().await?;
            return Ok((json!({"drained":true}), true));
        }
    };
    Ok((result, false))
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() || args.iter().any(|a| a == "--help") {
        println!("argentic-host --data DIR --workspace DIR --source FILE_OR_PROJECT --policy FILE --approve-policy SHA256 --stop-on-exit [--codex] [--codex-auth-file FILE] [--argentic-config FILE]\nJSON-lines commands: preflight, start, runs, inspect, humans, answer, controlRun, deliver, stop, shutdown, quit.\nUse --codex or --argentic-config to opt into that harness for preflight and start.\n--codex-auth-file opts into native, memory-only reuse of an existing ChatGPT file session, without its configuration or refresh writes.\n--stop-on-exit consents to native Stop/drain of this process's runs on EOF or Ctrl-C.");
        return Ok(());
    }
    let mut data = None;
    let mut workspace = None;
    let mut source = None;
    let mut policy = None;
    let mut approved = None;
    let mut allowed = BTreeSet::new();
    let mut argentic_config = None;
    let mut stop_on_exit = false;
    let mut auth_file = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--codex" => {
                allowed.insert("codex".into());
            }
            "--stop-on-exit" => stop_on_exit = true,
            name @ ("--data" | "--workspace" | "--source" | "--policy" | "--approve-policy"
            | "--codex-auth-file" | "--argentic-config") => {
                i += 1;
                let value = args.get(i).ok_or("Missing option value")?.clone();
                match name {
                    "--data" => data = Some(PathBuf::from(value)),
                    "--workspace" => workspace = Some(PathBuf::from(value)),
                    "--source" => source = Some(PathBuf::from(value)),
                    "--policy" => policy = Some(PathBuf::from(value)),
                    "--codex-auth-file" => auth_file = Some(PathBuf::from(value)),
                    "--argentic-config" => argentic_config = Some(PathBuf::from(value)),
                    _ => approved = Some(value),
                }
            }
            _ => return Err(format!("Unknown option: {}", args[i]).into()),
        }
        i += 1;
    }
    if !stop_on_exit {
        return Err("Explicit --stop-on-exit consent is required".into());
    }
    let host = Host::open(
        &data.ok_or("Missing --data")?,
        &workspace.ok_or("Missing --workspace")?,
        &source.ok_or("Missing --source")?,
        &policy.ok_or("Missing --policy")?,
        &approved.ok_or("Missing --approve-policy")?,
    )
    .await
    .map_err(|e| format!("{}: {}", e.code, e.message))?;
    let host = match auth_file {
        Some(path) => host.with_codex_auth_file(path),
        None => host,
    };
    let host = if let Some(path) = argentic_config {
        let bytes = std::fs::read(path)?;
        let config = serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "Invalid Argentic provider configuration at line {}, column {}",
                error.line(),
                error.column()
            )
        })?;
        allowed.insert("argentic".into());
        host.with_argentic(config)
            .map_err(|e| format!("{}: {}", e.code, e.message))?
    } else {
        host
    };
    serve(
        &host,
        &allowed,
        tokio::io::BufReader::new(tokio::io::stdin()),
        move |value| {
            use std::io::Write;
            writeln!(io::stdout().lock(), "{value}").is_ok()
        },
        tokio::signal::ctrl_c(),
    )
    .await
}

type Output = mpsc::Sender<(Value, oneshot::Sender<bool>)>;

async fn write_output(output: &Output, value: Value) -> bool {
    let (written, receipt) = oneshot::channel();
    output.send((value, written)).await.is_ok() && receipt.await.unwrap_or(false)
}

async fn write_final_output(output: &Output, value: Value) {
    let _ = tokio::time::timeout(Duration::from_secs(1), write_output(output, value)).await;
}

async fn serve(
    host: &Host,
    allowed: &BTreeSet<String>,
    mut input: impl AsyncBufRead + Unpin,
    mut write: impl FnMut(Value) -> bool + Send + 'static,
    interrupt: impl Future<Output = io::Result<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (output, mut pending) = mpsc::channel::<(Value, oneshot::Sender<bool>)>(1);
    // Keep the writer outside Tokio's blocking pool. It may stay in write()
    // until the process exits. If the thread cannot start, the channel closes.
    let _writer = std::thread::Builder::new()
        .name("argentic-host-stdout".into())
        .spawn(move || {
            while let Some((value, written)) = pending.blocking_recv() {
                let ok = write(value);
                let _ = written.send(ok);
                if !ok {
                    break;
                }
            }
        });
    let interrupt = async {
        if let Err(error) = interrupt.await {
            eprintln!("Signal listener failed: {error}");
        }
    };
    tokio::pin!(interrupt);
    let mut response = json!({
        "ready":true,"source":host.source().await,"runs":host.runs().await.map_err(|e|e.code)?
    });
    loop {
        let writable = tokio::select! {
            biased;
            _ = &mut interrupt => break,
            written = write_output(&output, response) => written,
        };
        if !writable {
            break;
        }
        let mut line = String::new();
        let mut bounded = (&mut input).take(8 * 1024 * 1024 + 1);
        let read = tokio::select! {
            biased;
            _ = &mut interrupt => break,
            read = bounded.read_line(&mut line) => read,
        };
        match read {
            Ok(0) => break,
            Ok(_) => (),
            Err(e) => {
                eprintln!("Input error: {e}");
                break;
            }
        }
        if line.len() > 8 * 1024 * 1024 {
            let _ = output.try_send((
                json!({"error":{"code":"INPUT_LIMIT","message":"Command exceeds 8 MiB; closing input and draining owned runs."}}),
                oneshot::channel().0,
            ));
            break;
        }
        response = match serde_json::from_str::<Command>(&line) {
            Err(e) => {
                json!({"error":{"code":"INVALID_COMMAND","message":e.to_string()},"shutdownStarted":host.shutdown_started()})
            }
            Ok(command) => match execute(host, command, allowed).await {
                Ok((value, true)) => {
                    write_final_output(&output, json!({"result":value})).await;
                    return Ok(());
                }
                Ok((value, false)) => json!({"result":value}),
                Err(e) => json!({"error":e,"shutdownStarted":host.shutdown_started()}),
            },
        };
    }
    // EOF/Ctrl-C/output loss never abandons physical owners. The explicit startup
    // consent covers these runs. Retry the same retirement until it is joined.
    loop {
        let result = async {
            let active = host.active_runs().await?;
            host.shutdown(&active).await?;
            host.close().await
        }
        .await;
        match result {
            Ok(()) => {
                write_final_output(&output, json!({"drained":true})).await;
                return Ok(());
            }
            Err(e) => {
                let _ = output.try_send((
                    json!({"error":e,"shutdownStarted":host.shutdown_started(),"retryingDrain":true}),
                    oneshot::channel().0,
                ));
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
fn main() {
    match argentic_runtime::content_io::file_helper::initialize() {
        Ok(Some(code)) => std::process::exit(code),
        Ok(None) => (),
        Err(e) => {
            eprintln!("Content reader initialization failed: {e}");
            std::process::exit(1);
        }
    }
    if let Err(e) = run_on_runtime(run()) {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run_on_runtime<F: std::future::Future>(future: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Tokio runtime");
    let result = runtime.block_on(future);
    // Tokio's blocking stdin read may outlive an interrupted command loop.
    // The host has already drained its owned resources by this point.
    runtime.shutdown_background();
    result
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Stdio;

    #[tokio::test]
    async fn interrupt_exits_with_stdin_still_open() {
        const CHILD: &str = "ARGENTIC_TEST_BLOCKED_HOST_STDIN";
        if let Some(ready) = std::env::var_os(CHILD) {
            std::thread::spawn(move || {
                run_on_runtime(async {
                    let mut input = tokio::io::stdin();
                    let mut byte = [0];
                    let mut read = Box::pin(input.read(&mut byte));
                    let mut interrupt = Box::pin(tokio::signal::ctrl_c());
                    assert!(futures::poll!(&mut interrupt).is_pending());
                    assert!(futures::poll!(&mut read).is_pending());
                    std::fs::write(ready, []).unwrap();
                    interrupt.await.unwrap();
                });
            })
            .join()
            .unwrap();
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let ready = temp.path().join("stdin-ready");
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::interrupt_exits_with_stdin_still_open",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, &ready)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let _input = child.stdin.take().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !ready.exists() {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "Child exited before readiness"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Child did not install the signal handler");
        // The child has installed its signal handler and submitted the stdin read.
        assert_eq!(
            unsafe { libc::kill(child.id().unwrap() as libc::pid_t, libc::SIGINT) },
            0
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3), child.wait())
                .await
                .expect("the runtime kept waiting for stdin after drain")
                .unwrap()
                .success()
        );
    }
}

#[cfg(test)]
mod output_tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncWriteExt;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn interrupt_drains_host_while_output_is_stalled() {
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        let source = dir.path().join("source.json");
        let policy = dir.path().join("policy.json");
        let data = dir.path().join("data");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::write(
            &source,
            br#"{"contract":"agsdl-0.3.0","id":"stdio","agents":{},"flow":{"entry":"wait","steps":{"wait":{"type":"approval","title":"Continue?","timeoutMs":86400000,"next":{"yes":[],"no":[]}}}}}"#,
        )
        .unwrap();
        let bytes = br#"{"owner":{"projectId":"project","planId":"plan","branch":"standalone"},"sourceRevision":"1","permissions":"workspace","readOnly":false}"#;
        std::fs::write(&policy, bytes).unwrap();
        let digest = format!("{:x}", Sha256::digest(bytes));
        let host = Host::open(&data, &workspace, &source, &policy, &digest)
            .await
            .unwrap();
        let (read, mut controller) = tokio::io::duplex(1024);
        controller
            .write_all(
                b"{\"op\":\"start\",\"requestId\":\"start-1\",\"entry\":{\"deliveryId\":\"entry\",\"messageJson\":\"{}\"}}\n",
            )
            .await
            .unwrap();
        let (interrupt, interrupted) = oneshot::channel();
        let (writing, stalled) = oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let mut writing = Some(writing);
        let mut serve = tokio::spawn(async move {
            super::serve(
                &host,
                &BTreeSet::new(),
                tokio::io::BufReader::new(read),
                move |value| {
                    if value["ready"] != true {
                        if let Some(writing) = writing.take() {
                            let _ = writing.send(value);
                            let _ = blocked.recv();
                        }
                    }
                    true
                },
                async { interrupted.await.map_err(io::Error::other) },
            )
            .await
            .map_err(|e| e.to_string())
        });
        let response = tokio::time::timeout(Duration::from_secs(10), stalled)
            .await
            .expect("Start must produce a response")
            .unwrap();
        assert!(response.pointer("/result/receipt").is_some(), "{response}");
        interrupt.send(()).unwrap();
        let drained = tokio::time::timeout(Duration::from_secs(10), &mut serve).await;
        release.send(()).unwrap();
        drained
            .expect("shutdown must finish while the writer is blocked")
            .unwrap()
            .unwrap();
        let reopened = Host::open(&data, &workspace, &source, &policy, &digest)
            .await
            .unwrap();
        assert!(reopened.active_runs().await.unwrap().is_empty());
        reopened.shutdown(&[]).await.unwrap();
        reopened.close().await.unwrap();
    }
}
