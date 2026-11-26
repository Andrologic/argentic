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
        fs::read_to_string(path).unwrap(),
        "first\r\ncontext\nnew\r\nlast"
    );
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

#[cfg(all(
    target_os = "linux",
    any(target_arch = "aarch64", target_arch = "x86_64")
))]
fn sandbox_present() -> bool {
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
