use argentic_harness::{
    policy::{subcommands, Action, Effect, Policy, Preset, Rule, Zone},
    providers::ToolCall,
    tools::Registry,
};
use serde_json::{json, Value};
use std::{fs, path::Path, sync::Arc};
use tokio_util::sync::CancellationToken;

fn registry(path: &Path, preset: Option<Preset>, rules: &[Rule]) -> Registry {
    let root = path.canonicalize().unwrap();
    let policy = Policy::compile_rules(&root, None, preset, None, rules).unwrap();
    Registry::new(&[root], policy, json!({"type":"object"}))
        .unwrap()
        .with_writes()
}

async fn call(registry: &Arc<Registry>, name: &str, args: Value) -> Result<String, String> {
    registry
        .read_tool(
            ToolCall {
                name: name.into(),
                id: "test".into(),
                arguments: args.to_string(),
            },
            CancellationToken::new(),
        )
        .await
        .map(|page| page.text)
}

#[test]
fn permissions_matrix() {
    let root = std::env::current_dir().unwrap();
    let zones = [
        Zone::Workspace,
        Zone::ReadDirectory,
        Zone::WriteDirectory,
        Zone::Outside,
    ];
    let cases = [
        (None, Action::Read, [false, false, false, false]),
        (None, Action::Write, [false, false, false, false]),
        (None, Action::Shell, [false; 4]),
        (None, Action::Network, [false; 4]),
        (
            Some(Preset::ReadOnly),
            Action::Read,
            [true, true, true, false],
        ),
        (Some(Preset::ReadOnly), Action::Write, [false; 4]),
        (Some(Preset::ReadOnly), Action::Shell, [false; 4]),
        (Some(Preset::ReadOnly), Action::Network, [false; 4]),
        (
            Some(Preset::Workspace),
            Action::Read,
            [true, true, true, false],
        ),
        (
            Some(Preset::Workspace),
            Action::Write,
            [true, false, true, false],
        ),
        (Some(Preset::Workspace), Action::Shell, [true; 4]),
        (Some(Preset::Workspace), Action::Network, [false; 4]),
        (Some(Preset::Full), Action::Read, [true; 4]),
        (Some(Preset::Full), Action::Write, [true, false, true, true]),
        (Some(Preset::Full), Action::Shell, [true; 4]),
        (Some(Preset::Full), Action::Network, [true; 4]),
    ];
    for (preset, action, defaults) in cases {
        for (zone, allowed) in zones.into_iter().zip(defaults) {
            let policy = Policy::compile_rules(&root, None, preset, None, &[]).unwrap();
            assert_eq!(
                policy.decide(action, "example", zone),
                if allowed { Effect::Allow } else { Effect::Deny },
                "{preset:?} {action:?} {zone:?}"
            );
            for effect in [Effect::Allow, Effect::Ask, Effect::Deny] {
                let rules = [Rule {
                    action,
                    pattern: None,
                    effect,
                }];
                let policy = Policy::compile_rules(&root, None, preset, None, &rules).unwrap();
                assert_eq!(policy.decide(action, "example", zone), effect);
            }
        }
    }
    for (preset, outside, result) in [
        (None, true, Effect::Deny),
        (Some(Preset::ReadOnly), true, Effect::Allow),
        (Some(Preset::Workspace), true, Effect::Allow),
        (Some(Preset::Full), false, Effect::Deny),
    ] {
        let policy = Policy::compile_rules(&root, None, preset, Some(outside), &[]).unwrap();
        assert_eq!(
            policy.decide(Action::Read, "example", Zone::Outside),
            result
        );
    }
}

#[test]
fn bash_subcommands_use_syntax_and_original_text() {
    for (source, expected) in [
        (
            "echo one | cat && echo two; echo three",
            vec!["echo one", "cat", "echo two", "echo three"],
        ),
        (
            "(echo one; echo two) || false",
            vec!["echo one", "echo two", "false"],
        ),
        (
            "printf '%s;' 'echo two && true'; echo \"$HOME\"",
            vec!["printf '%s;' 'echo two && true'", "echo \"$HOME\""],
        ),
        (
            "cat <<'EOF'\na; b && pretend\nEOF\necho done",
            vec!["cat <<'EOF'", "echo done"],
        ),
        (
            "cat <<EOF >out\ndata\nEOF\necho done",
            vec!["cat <<EOF >out", "echo done"],
        ),
        (
            "cat <<EOF file\ndata\nEOF\necho done",
            vec!["cat <<EOF file", "echo done"],
        ),
        ("echo left | cat >out", vec!["echo left", "cat >out"]),
        (
            "echo left >out && echo right",
            vec!["echo left >out", "echo right"],
        ),
        (
            "cat <<EOF | head\n$(echo data)\nEOF\necho done",
            vec!["cat <<EOF", "head", "echo data", "echo done"],
        ),
        (
            "echo \"$(printf hi)\"",
            vec!["echo \"$(printf hi)\"", "printf hi"],
        ),
    ] {
        assert_eq!(subcommands(source).unwrap(), expected, "{source}");
    }
    for invalid in [
        "",
        "echo &&",
        "echo '",
        "(echo one",
        "cat <<EOF\nmissing end\n",
    ] {
        assert!(subcommands(invalid).is_err(), "{invalid}");
    }
    let dir = tempfile::tempdir().unwrap();
    let policy = Policy::compile_rules(
        dir.path(),
        None,
        Some(Preset::Workspace),
        None,
        &[
            Rule {
                action: Action::Shell,
                pattern: Some("git push *".into()),
                effect: Effect::Ask,
            },
            Rule {
                action: Action::Shell,
                pattern: Some("git push * --force".into()),
                effect: Effect::Deny,
            },
        ],
    )
    .unwrap();
    for (source, effect) in [
        ("git push", Effect::Ask),
        ("echo start && git push origin main", Effect::Ask),
        ("(git push origin --force)", Effect::Deny),
        ("git pushx", Effect::Allow),
        ("echo \"git push\"", Effect::Allow),
        ("echo &&", Effect::Deny),
    ] {
        assert_eq!(policy.shell_effect(source), effect, "{source}");
    }
}

#[tokio::test]
async fn opaque_shell_commands_use_the_strictest_shell_rule() {
    let dir = tempfile::tempdir().unwrap();
    let commands = [
        "eval 'printf hello'",
        "name=printf; $name hello",
        "$(printf printf) hello",
        "`printf printf` hello",
        "bash -c 'printf hello'",
        "sh -c 'printf hello'",
        "time printf hello",
        "coproc printf hello",
        "source helper",
        ". helper",
        "exec printf hello",
        "xargs printf",
        "find . -exec printf hello {} +",
        "alias greet='printf hello'; greet",
        "greet() { printf hello; }; greet",
        "pri\\\nntf hello",
    ];
    for effect in [Effect::Ask, Effect::Deny] {
        let policy = Policy::compile_rules(
            dir.path(),
            None,
            Some(Preset::Workspace),
            None,
            &[Rule {
                action: Action::Shell,
                pattern: Some("printf review *".into()),
                effect,
            }],
        )
        .unwrap();
        for command in commands {
            assert_eq!(policy.shell_effect(command), effect, "{command}");
        }
        assert_eq!(policy.shell_effect("printf hello"), Effect::Allow);
        let registry = Arc::new(
            Registry::new(&[dir.path().to_owned()], policy, json!({}))
                .unwrap()
                .with_shell(),
        );
        let error = call(&registry, "shell", json!({"command":commands[0]}))
            .await
            .unwrap_err();
        assert_eq!(error, "Shell permission denied");
    }
}

#[tokio::test]
async fn mutation_tools_apply_exact_edits_and_patches() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(registry(dir.path(), Some(Preset::Workspace), &[]));
    call(
        &registry,
        "write",
        json!({"path":"a","content":"old\nold\n"}),
    )
    .await
    .unwrap();
    assert!(call(
        &registry,
        "edit",
        json!({"path":"a","old_text":"old","new_text":"new"})
    )
    .await
    .is_err());
    call(
        &registry,
        "edit",
        json!({"path":"a","old_text":"old","new_text":"new","replace_all":true}),
    )
    .await
    .unwrap();
    fs::write(dir.path().join("delete"), "obsolete").unwrap();
    let patch = "*** Begin Patch\n*** Update File: a\n*** Move to: renamed\n@@\n-new\n+updated\n new\n*** Add File: added\n+first\n+second\n*** Delete File: delete\n*** End Patch";
    call(&registry, "apply_patch", json!({"patch_text":patch}))
        .await
        .unwrap();
    assert!(!dir.path().join("a").exists());
    assert!(!dir.path().join("delete").exists());
    assert_eq!(
        fs::read_to_string(dir.path().join("renamed")).unwrap(),
        "updated\nnew\n"
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("added")).unwrap(),
        "first\nsecond\n"
    );
    let invalid = "*** Begin Patch\n*** Add File: side-effect\n+new file\n*** Update File: renamed\n@@\n-unmatched\n+change\n*** End Patch";
    assert!(
        call(&registry, "apply_patch", json!({"patch_text":invalid}))
            .await
            .is_err()
    );
    assert!(!dir.path().join("side-effect").exists());
}

#[tokio::test]
async fn patches_use_normalized_context_and_preserve_line_endings() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(registry(dir.path(), Some(Preset::Workspace), &[]));
    let path = dir.path().join("memo");
    fs::write(&path, "first\r\ncontext\nold\r\nlast").unwrap();
    let patch = "*** Begin Patch\n*** Update File: memo\n@@\n first\n context\n-old\n+new\n last\n*** End Patch";
    call(&registry, "apply_patch", json!({"patch_text":patch}))
        .await
        .unwrap();
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "first\r\ncontext\nnew\r\nlast"
    );
    fs::write(&path, "").unwrap();
    let patch = "*** Begin Patch\n*** Update File: memo\n@@\n-\n+new\n*** End Patch";
    call(&registry, "apply_patch", json!({"patch_text":patch}))
        .await
        .unwrap();
    assert_eq!(fs::read_to_string(path).unwrap(), "new");
}

#[tokio::test]
async fn refused_or_unreviewed_patch_has_no_effects() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(registry(
        dir.path(),
        Some(Preset::Workspace),
        &[
            Rule {
                action: Action::Write,
                pattern: Some("review".into()),
                effect: Effect::Ask,
            },
            Rule {
                action: Action::Write,
                pattern: Some("hidden".into()),
                effect: Effect::Deny,
            },
        ],
    ));
    for path in ["review", "hidden", "../escape"] {
        let patch = format!("*** Begin Patch\n*** Add File: ordinary\n+first\n*** Add File: {path}\n+second\n*** End Patch");
        assert!(call(&registry, "apply_patch", json!({"patch_text":patch}))
            .await
            .is_err());
        assert!(!dir.path().join("ordinary").exists());
    }
}

#[cfg(unix)]
#[tokio::test]
async fn writes_refuse_symlink_and_hardlink_aliases() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("work");
    fs::create_dir(&workspace).unwrap();
    let outside = dir.path().join("secret");
    fs::write(&outside, "safe").unwrap();
    symlink(&outside, workspace.join("link")).unwrap();
    symlink(dir.path(), workspace.join("dir")).unwrap();
    fs::hard_link(&outside, workspace.join("hard")).unwrap();
    let registry = Arc::new(registry(&workspace, Some(Preset::Workspace), &[]));
    for path in ["link", "dir/secret", "hard", "../secret"] {
        assert!(
            call(&registry, "write", json!({"path":path,"content":"unsafe"}))
                .await
                .is_err(),
            "{path}"
        );
        assert_eq!(fs::read_to_string(&outside).unwrap(), "safe");
    }
}

#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
fn sandbox_present() -> bool {
    #[cfg(target_os = "linux")]
    if ["/usr/bin/bwrap", "/bin/bwrap", "/usr/local/bin/bwrap"]
        .into_iter()
        .all(|path| !Path::new(path).is_file())
    {
        assert_ne!(
            std::env::var("ARGENTIC_REQUIRE_BWRAP").as_deref(),
            Ok("1"),
            "Bubblewrap is required for the Linux sandbox integration tests"
        );
        eprintln!("Skipping Linux sandbox integration because system Bubblewrap is absent");
        return false;
    }
    argentic_harness::tools::shell_preflight().unwrap();
    true
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[tokio::test]
async fn shell_isolates_host_files_network_and_denied_reads() {
    if !sandbox_present() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("work");
    fs::create_dir(&workspace).unwrap();
    let outside = dir.path().join("outside");
    fs::write(&outside, "host data").unwrap();
    fs::write(workspace.join("hidden"), "private").unwrap();
    fs::write(workspace.join("visible"), "workspace data").unwrap();
    fs::create_dir(workspace.join(".git")).unwrap();
    fs::write(workspace.join(".git/config"), "private host metadata").unwrap();
    let registry = Arc::new(
        registry(
            &workspace,
            Some(Preset::Workspace),
            &[Rule {
                action: Action::Read,
                pattern: Some("hidden".into()),
                effect: Effect::Deny,
            }],
        )
        .with_shell(),
    );
    let command = format!(
        "test ! -e '{}' && test ! -e hidden && test ! -e .git && cat visible && \
         ! (printf no > '{}') && ! (printf no > visible) && \
         ! (printf no > /dev/tcp/127.0.0.1/9)",
        outside.display(),
        outside.display()
    );
    let output = call(&registry, "shell", json!({"command":command}))
        .await
        .unwrap();
    assert!(output.starts_with("Exit status: 0"), "{output}");
    assert!(output.contains("workspace data"));
    assert!(output.contains("Operation not permitted"), "{output}");
    let mutation_error = call(
        &registry,
        "write",
        json!({"path":".git/config","content":"changed"}),
    )
    .await
    .unwrap_err();
    assert_eq!(mutation_error, "Write permission denied");
    assert_eq!(
        fs::read_to_string(workspace.join(".git/config")).unwrap(),
        "private host metadata"
    );
    assert_eq!(fs::read_to_string(outside).unwrap(), "host data");
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[tokio::test]
async fn shell_publishes_regular_files_and_times_out_with_background_processes() {
    if !sandbox_present() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(registry(dir.path(), Some(Preset::Workspace), &[]).with_shell());
    let output = call(
        &registry,
        "shell",
        json!({"command":"printf hello > shell.txt; cat shell.txt"}),
    )
    .await
    .unwrap();
    assert!(output.starts_with("Exit status: 0"), "{output}");
    assert_eq!(
        fs::read_to_string(dir.path().join("shell.txt")).unwrap(),
        "hello"
    );
    let start = std::time::Instant::now();
    let error = call(
        &registry,
        "shell",
        json!({"command":"(sleep 30; printf bad > late) & wait","timeout_ms":500}),
    )
    .await
    .unwrap_err();
    assert!(error.contains("timed out"), "{error}");
    assert!(start.elapsed() < std::time::Duration::from_secs(5));
    assert!(!dir.path().join("late").exists());
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[tokio::test]
async fn shell_stops_on_staging_budget_during_execution() {
    if !sandbox_present() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(registry(dir.path(), Some(Preset::Workspace), &[]).with_shell());
    let output = call(
        &registry,
        "shell",
        json!({"command": "test \"$(ulimit -n)\" -le 256 && test \"$(ulimit -u)\" -le 1024"}),
    )
    .await
    .unwrap();
    assert!(output.starts_with("Exit status: 0"), "{output}");
    for (command, expected) in [
        (
            "for i in {1..19}; do truncate -s 14M part-$i; done; sleep 15",
            "byte budget",
        ),
        ("touch item-{1..50001}; sleep 15", "entry or depth budget"),
    ] {
        let error = call(
            &registry,
            "shell",
            json!({"command":command,"timeout_ms":10000}),
        )
        .await
        .unwrap_err();
        assert!(error.contains(expected), "{error}");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[tokio::test]
async fn shell_refuses_private_publication_before_applying_other_changes() {
    if !sandbox_present() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("auth.json"), "private").unwrap();
    let registry = Arc::new(registry(dir.path(), Some(Preset::Workspace), &[]).with_shell());
    let error = call(
        &registry,
        "shell",
        json!({"command": "test ! -e auth.json && printf new > auth.json && printf new > visible"}),
    )
    .await
    .unwrap_err();
    assert!(error.starts_with("Shell workspace publication:"), "{error}");
    assert_eq!(
        fs::read_to_string(dir.path().join("auth.json")).unwrap(),
        "private"
    );
    assert!(!dir.path().join("visible").exists());
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
#[tokio::test]
async fn shell_validates_additional_roots_before_publishing_workspace_edits() {
    if !sandbox_present() {
        return;
    }
    let workspace = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let workspace = workspace.path().canonicalize().unwrap();
    let extra = extra.path().canonicalize().unwrap();
    fs::write(workspace.join("visible"), "original").unwrap();
    fs::write(extra.join("auth.json"), "private").unwrap();
    let policy = Policy::compile_rules(&workspace, None, Some(Preset::Workspace), None, &[])
        .unwrap()
        .with_directories(vec![(extra.clone(), Zone::WriteDirectory)]);
    let registry = Arc::new(
        Registry::new(
            &[workspace.clone(), extra.clone()],
            policy,
            json!({"type":"object"}),
        )
        .unwrap()
        .with_writes()
        .with_shell(),
    );
    let error = call(
        &registry,
        "shell",
        json!({"command":"printf changed > visible && printf changed > /directories/1/auth.json"}),
    )
    .await
    .unwrap_err();
    assert!(error.starts_with("Shell workspace publication:"), "{error}");
    assert_eq!(
        fs::read_to_string(workspace.join("visible")).unwrap(),
        "original"
    );
    assert_eq!(
        fs::read_to_string(extra.join("auth.json")).unwrap(),
        "private"
    );
}

#[tokio::test]
async fn shell_asks_and_denials_are_checked_before_launch() {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(
        registry(
            dir.path(),
            Some(Preset::Workspace),
            &[
                Rule {
                    action: Action::Shell,
                    pattern: Some("echo review *".into()),
                    effect: Effect::Ask,
                },
                Rule {
                    action: Action::Shell,
                    pattern: Some("printf blocked >out".into()),
                    effect: Effect::Deny,
                },
            ],
        )
        .with_shell(),
    );
    for command in ["true && echo review", "true | printf blocked >out"] {
        let error = call(&registry, "shell", json!({"command":command}))
            .await
            .unwrap_err();
        assert_eq!(error, "Shell permission denied");
    }
    assert!(!dir.path().join("out").exists());
}

#[test]
fn network_globs_match_hosts_and_ip_aliases_by_most_restrictive_effect() {
    let path = std::env::current_dir().unwrap();
    let rules = [
        network_rule("*.Example.COM.", Effect::Allow),
        network_rule("review.example.com", Effect::Ask),
        network_rule("hidden.example.com", Effect::Deny),
        network_rule("0:0:0:*:ffff:7f00:0001", Effect::Deny),
    ];
    let policy = Policy::compile_rules(&path, None, Some(Preset::Workspace), None, &rules).unwrap();
    for (host, effect) in [
        ("nested.sub.example.com", Effect::Allow),
        ("WWW.Example.Com.", Effect::Allow),
        ("review.example.com", Effect::Ask),
        ("hidden.example.com", Effect::Deny),
        ("example.com", Effect::Deny),
        ("example.com.invalid", Effect::Deny),
        ("127.0.0.1", Effect::Deny),
        ("::ffff:127.0.0.1", Effect::Deny),
    ] {
        assert_eq!(
            policy.decide(Action::Network, host, Zone::Workspace),
            effect,
            "{host}"
        );
    }
    let full = Policy::compile_rules(&path, None, Some(Preset::Full), None, &rules).unwrap();
    assert_eq!(
        full.decide(Action::Network, "127.0.0.1", Zone::Workspace),
        Effect::Deny
    );
    assert_eq!(
        full.decide(Action::Network, "other.example", Zone::Outside),
        Effect::Allow
    );
}

fn network_rule(pattern: &str, effect: Effect) -> Rule {
    Rule {
        action: Action::Network,
        pattern: Some(pattern.into()),
        effect,
    }
}

#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "aarch64", target_arch = "x86_64")
    ),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod shell_proxy_tests {
    use super::*;
    use argentic_harness::tools::ShellNetwork;
    use axum::{routing::get, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn server() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let routes = Router::new()
            .route(
                "/",
                get(|headers: axum::http::HeaderMap| async move {
                    assert!(!headers.contains_key("proxy-authorization"));
                    format!("reached {}", headers["host"].to_str().unwrap())
                }),
            )
            .route(
                "/redirect",
                get(|| async {
                    axum::response::Redirect::temporary("http://blocked.invalid/never")
                }),
            );
        (
            port,
            tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() }),
        )
    }

    async fn shell_text(registry: &Arc<Registry>, command: String) -> String {
        call(
            registry,
            "shell",
            json!({"command":command,"timeout_ms":15_000}),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn shell_proxy_allows_http_and_blocks_opaque_connect_and_direct_network() {
        if !sandbox_present() {
            return;
        }
        let (port, server) = server().await;
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(
            registry(
                dir.path(),
                Some(Preset::Workspace),
                &[network_rule("127.0.0.1", Effect::Allow)],
            )
            .with_shell()
            .with_shell_network(ShellNetwork::Intercept),
        );
        let output = shell_text(
            &registry,
            format!(
                "curl -sSf --max-time 5 http://127.0.0.1:{port}/ && \\
             ! curl -sSf --max-time 5 --proxytunnel http://127.0.0.1:{port}/ && \\
             ! (printf x > /dev/tcp/127.0.0.1/{port})"
            ),
        )
        .await;
        assert!(output.starts_with("Exit status: 0\n"), "{output}");
        assert_eq!(
            output.matches(&format!("reached 127.0.0.1:{port}")).count(),
            1,
            "{output}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn shell_proxy_refuses_denied_hosts_redirects_and_implicit_private_addresses() {
        if !sandbox_present() {
            return;
        }
        let (port, server) = server().await;
        let dir = tempfile::tempdir().unwrap();
        let allowed = Arc::new(
            registry(
                dir.path(),
                Some(Preset::Workspace),
                &[
                    network_rule("127.0.0.1", Effect::Allow),
                    network_rule("blocked.invalid", Effect::Deny),
                ],
            )
            .with_shell()
            .with_shell_network(ShellNetwork::Intercept),
        );
        for url in [
            "http://blocked.invalid/".into(),
            format!("http://127.0.0.1:{port}/redirect"),
        ] {
            let output = shell_text(
                &allowed,
                format!("curl -sS --max-time 5 -L --fail-with-body {url}"),
            )
            .await;
            assert!(output.starts_with("Exit status: 22\n"), "{output}");
            assert!(
                output.contains("Argentic network policy denied this destination"),
                "{output}"
            );
        }
        let implicit = Arc::new(
            registry(dir.path(), Some(Preset::Full), &[])
                .with_shell()
                .with_shell_network(ShellNetwork::Intercept),
        );
        let output = shell_text(
            &implicit,
            format!("curl -sS --max-time 5 --fail-with-body http://127.0.0.1:{port}/"),
        )
        .await;
        assert!(output.starts_with("Exit status: 22\n"), "{output}");
        assert!(
            output.contains("Argentic network policy denied this destination"),
            "{output}"
        );
        server.abort();
    }

    #[derive(Default)]
    struct Reviewer(AtomicUsize);

    #[async_trait::async_trait]
    impl argentic_harness::tools::Authorizer for Reviewer {
        async fn approve(
            &self,
            call: &ToolCall,
            action: argentic_harness::tools::Approval<'_>,
            _: &CancellationToken,
        ) -> Result<bool, String> {
            assert_eq!(call.name, "shell");
            assert!(matches!(
                action,
                argentic_harness::tools::Approval::Network("127.0.0.1")
            ));
            Ok(self.0.fetch_add(1, Ordering::Relaxed) == 0)
        }
    }

    #[tokio::test]
    async fn shell_proxy_remembers_refusal_for_the_invocation_and_requires_a_reviewer() {
        if !sandbox_present() {
            return;
        }
        let (port, server) = server().await;
        let dir = tempfile::tempdir().unwrap();
        let rules = [network_rule("127.0.0.1", Effect::Ask)];
        let reviewer = Arc::new(Reviewer::default());
        let reviewed = Arc::new(
            registry(dir.path(), Some(Preset::Workspace), &rules)
                .with_shell()
                .with_shell_network(ShellNetwork::Intercept)
                .with_authorizer(reviewer.clone()),
        );
        let command = format!("curl -sS --max-time 5 --fail-with-body http://127.0.0.1:{port}/");
        let output = shell_text(&reviewed, command.clone()).await;
        assert!(output.contains("reached"), "{output}");
        let output = shell_text(&reviewed, format!("{command}; {command}")).await;
        assert!(output.starts_with("Exit status: 22\n"), "{output}");
        assert_eq!(
            output.matches("Argentic network policy denied").count(),
            2,
            "{output}"
        );
        assert_eq!(reviewer.0.load(Ordering::Relaxed), 2);

        let unreviewed = Arc::new(
            registry(dir.path(), Some(Preset::Workspace), &rules)
                .with_shell()
                .with_shell_network(ShellNetwork::Intercept),
        );
        let output = shell_text(&unreviewed, command).await;
        assert!(output.starts_with("Exit status: 22\n"), "{output}");
        server.abort();
    }

    struct SlowReviewer;

    #[async_trait::async_trait]
    impl argentic_harness::tools::Authorizer for SlowReviewer {
        async fn approve(
            &self,
            _: &ToolCall,
            _: argentic_harness::tools::Approval<'_>,
            _: &CancellationToken,
        ) -> Result<bool, String> {
            tokio::time::sleep(std::time::Duration::from_millis(1700)).await;
            Ok(true)
        }
    }

    #[tokio::test]
    async fn shell_deadline_excludes_network_approval_wait() {
        if !sandbox_present() {
            return;
        }
        let (port, server) = server().await;
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(
            registry(
                dir.path(),
                Some(Preset::Workspace),
                &[network_rule("127.0.0.1", Effect::Ask)],
            )
            .with_shell()
            .with_shell_network(ShellNetwork::Intercept)
            .with_authorizer(Arc::new(SlowReviewer)),
        );
        let output = call(
            &registry,
            "shell",
            json!({
                "command": format!("curl -sSf --max-time 10 http://127.0.0.1:{port}/"),
                "timeout_ms": 1500,
            }),
        )
        .await
        .unwrap();
        assert!(
            output.starts_with("Exit status: 0\n") && output.contains("reached"),
            "{output}"
        );
        server.abort();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_network_preflight_runs_the_relay() {
        if !sandbox_present() {
            return;
        }
        argentic_harness::tools::shell_network_preflight(true).unwrap();
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[tokio::test]
    async fn seatbelt_publishes_workspace_writes_and_refuses_other_writes() {
        assert!(sandbox_present());
        let dir = tempfile::tempdir().unwrap();
        let workspace = dir.path().join("workspace");
        fs::create_dir(&workspace).unwrap();
        let extra = dir.path().join("extra");
        fs::create_dir(&extra).unwrap();
        fs::write(extra.join("memo"), "extra root").unwrap();
        let outside = dir.path().canonicalize().unwrap().join("private");
        let workspace = workspace.canonicalize().unwrap();
        let extra = extra.canonicalize().unwrap();
        let policy = Policy::compile_rules(&workspace, None, Some(Preset::Workspace), None, &[])
            .unwrap()
            .with_directories(vec![(extra.clone(), Zone::ReadDirectory)]);
        let roots = [&workspace, &extra]
            .into_iter()
            .map(|path| {
                let directory =
                    cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority()).unwrap();
                (path.clone(), directory)
            })
            .collect();
        let registry = Arc::new(
            Registry::with_open_roots(roots, vec![policy], json!({"type":"object"}))
                .unwrap()
                .with_writes()
                .with_shell(),
        );
        let output = shell_text(
            &registry,
            format!(
                r#"printf staged > workspace.txt && ! (printf leak > '{}') && \
                 cat "$ARGENTIC_DIRECTORY_1/memo" && ! (printf leak > "$ARGENTIC_DIRECTORY_1/write")"#,
                outside.display()
            ),
        ).await;
        assert!(output.starts_with("Exit status: 0\n"), "{output}");
        assert_eq!(
            fs::read_to_string(workspace.join("workspace.txt")).unwrap(),
            "staged"
        );
        assert!(output.contains("extra root"), "{output}");
        assert!(!extra.join("write").exists());
        assert!(!outside.exists());
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[tokio::test]
    async fn seatbelt_joins_background_jobs_in_other_process_groups() {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(registry(dir.path(), Some(Preset::Workspace), &[]).with_shell());
        let output = shell_text(
            &registry,
            "set -m; (sleep 10; printf late > late) & printf '%s\\n' \"$!\"".into(),
        )
        .await;
        assert!(output.starts_with("Exit status: 0\n"), "{output}");
        let pid = output.lines().nth(2).unwrap().parse::<i32>().unwrap();
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "{output}");
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
        assert!(!dir.path().join("late").exists());
    }
}

#[test]
fn network_patterns_and_requests_use_the_same_idna_mapping() {
    let rules = [
        network_rule("*.BÜCHER。Example.", Effect::Deny),
        network_rule("straße.example", Effect::Ask),
    ];
    let work = tempfile::tempdir().unwrap();
    let policy =
        Policy::compile_rules(work.path(), None, Some(Preset::Full), None, &rules).unwrap();
    for host in ["api.xn--bcher-kva.example", "API.bücher.example."] {
        assert_eq!(
            policy.decide(Action::Network, host, Zone::Workspace),
            Effect::Deny
        );
    }
    assert_eq!(
        policy.decide(Action::Network, "xn--strae-oqa.example", Zone::Workspace),
        Effect::Ask
    );
    for pattern in [
        "example.com:443",
        "*.example.com:443",
        "[::1]:443",
        "x..test",
        "büch*.test",
    ] {
        let rules = [network_rule(pattern, Effect::Deny)];
        assert!(
            Policy::compile_rules(work.path(), None, None, None, &rules).is_err(),
            "{pattern}"
        );
    }
}

#[tokio::test]
async fn blanket_shell_refuses_a_host_rule_before_launching_the_command() {
    let work = tempfile::tempdir().unwrap();
    let shell = Arc::new(
        registry(
            work.path(),
            Some(Preset::Full),
            &[network_rule("blocked.example", Effect::Deny)],
        )
        .with_shell(),
    );
    let error = call(&shell, "shell", json!({"command":":"}))
        .await
        .unwrap_err();
    assert!(
        error.contains("shellNetwork") && error.contains("intercept"),
        "{error}"
    );
}
