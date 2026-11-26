use std::{path::Path, sync::Arc};

use argentic_harness::{
    config::{Layer, Settings},
    policy::{Effect, PathGlob, Preset, ReadPolicy, ReadRule},
    providers::ToolCall,
    tools::{Page, Registry},
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

fn registry(dir: &Path, rules: &[ReadRule]) -> Arc<Registry> {
    let workspace = dir.canonicalize().unwrap();
    let policy =
        ReadPolicy::compile(&workspace, None, Some(Preset::ReadOnly), None, rules).unwrap();
    Arc::new(Registry::new(&[workspace], policy, json!({"type":"object"})).unwrap())
}

async fn tool(registry: &Arc<Registry>, name: &str, arguments: Value) -> Result<Page, String> {
    registry
        .read_tool(
            ToolCall {
                id: "call".into(),
                name: name.into(),
                arguments: arguments.to_string(),
            },
            CancellationToken::new(),
        )
        .await
}

#[test]
fn glob_stars_respect_slash_and_other_characters_are_literal() {
    let single = PathGlob::compile("src/*.rs").unwrap();
    assert!(single.matches("src/main.rs"));
    assert!(!single.matches("src/sub/main.rs"));
    assert!(PathGlob::compile("src/**.rs")
        .unwrap()
        .matches("src/sub/main.rs"));
    assert!(PathGlob::compile("a[?]").unwrap().matches("a[?]"));
    assert!(!PathGlob::compile("*").unwrap().matches("a/b"));
    assert!(PathGlob::compile("**").unwrap().matches("a/b"));
}

#[test]
fn policy_takes_most_restrictive_rule_then_preset_and_zone() {
    let root = std::env::current_dir().unwrap();
    let rules = [
        ReadRule {
            pattern: Some("private/**".into()),
            effect: Effect::Allow,
        },
        ReadRule {
            pattern: Some("private/*.txt".into()),
            effect: Effect::Ask,
        },
        ReadRule {
            pattern: Some("private/blocked.txt".into()),
            effect: Effect::Deny,
        },
    ];
    let policy = ReadPolicy::compile(&root, None, Some(Preset::ReadOnly), None, &rules).unwrap();
    assert_eq!(
        policy.evaluate(&root.join("private/blocked.txt"), true),
        Effect::Deny
    );
    assert_eq!(
        policy.evaluate(&root.join("private/review.txt"), true),
        Effect::Ask
    );
    assert!(!policy.allows(&root.join("private/review.txt"), true));
    assert!(policy.allows(&root.join("private/pages/review.txt"), true));
    assert!(policy.allows(&root.join("other"), true));
    assert!(!policy.allows(&root.join("other"), false));
    let denied = ReadPolicy::compile(&root, None, None, Some(true), &[]).unwrap();
    assert!(!denied.allows(&root.join("other"), true));
    assert!(!denied.allows(&root.join("other"), false));
}

#[test]
fn settings_reject_unknown_fields_and_project_expansion() {
    let host = json!({"maxTurns":10.0});
    let user = json!({"maxParallelReads": 2});
    let settings = Settings::resolve(&host, &user, &json!({"maxTurns":3})).unwrap();
    assert_eq!(settings.get("maxTurns"), Some((3, Layer::Project)));
    assert_eq!(settings.get("maxParallelReads"), Some((2, Layer::User)));
    assert_eq!(
        settings.get("maxOutputTokens"),
        Some((4096, Layer::Default))
    );
    assert!(Settings::resolve(&host, &user, &json!({"maxTurns":11})).is_err());
    assert!(Settings::resolve(&host, &user, &json!({"maxTurnz":3})).is_err());
    assert!(Settings::resolve(&host, &user, &json!({"cancelGraceMs":-1})).is_err());
    assert!(Settings::resolve(&host, &user, &json!({"maxTurns":0})).is_err());
}

#[tokio::test]
async fn relative_tool_paths_cannot_escape() {
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let outside = dir.path().join("secret.txt");
    std::fs::write(&outside, "secret").unwrap();
    let registry = registry(&workspace, &[]);
    for path in [
        "../secret.txt",
        "sub/../secret.txt",
        "C:/secret.txt",
        "/secret.txt",
    ] {
        assert!(
            tool(&registry, "read", json!({"path":path})).await.is_err(),
            "{path}"
        );
    }
    assert!(tool(&registry, "read", json!({"path":outside}))
        .await
        .is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn read_and_scan_refuse_symlink_and_hard_link_aliases() {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir().unwrap();
    let workspace = dir.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let outside = dir.path().join("secret.txt");
    std::fs::write(&outside, "secret").unwrap();
    symlink(&outside, workspace.join("file-link")).unwrap();
    symlink(dir.path(), workspace.join("directory-link")).unwrap();
    std::fs::hard_link(&outside, workspace.join("hard-link")).unwrap();
    let registry = registry(&workspace, &[]);
    for path in ["file-link", "directory-link/secret.txt", "hard-link"] {
        assert!(
            tool(&registry, "read", json!({"path":path})).await.is_err(),
            "{path}"
        );
    }
    assert!(tool(&registry, "grep", json!({"pattern":"secret"}))
        .await
        .unwrap()
        .text
        .is_empty());
}

#[tokio::test]
async fn scan_does_not_expose_denied_names_or_content() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("sub")).unwrap();
    std::fs::write(dir.path().join("sub/yes.txt"), "first\nneedle\n").unwrap();
    std::fs::write(dir.path().join("sub/no.txt"), "needle\n").unwrap();
    let registry = registry(
        dir.path(),
        &[ReadRule {
            pattern: Some("sub/no.txt".into()),
            effect: Effect::Deny,
        }],
    );
    assert_eq!(
        tool(&registry, "list", json!({"path":"sub"}))
            .await
            .unwrap()
            .text,
        "sub/yes.txt\n"
    );
    assert_eq!(
        tool(&registry, "glob", json!({"pattern":"**.txt"}))
            .await
            .unwrap()
            .text,
        "sub/yes.txt\n"
    );
    assert_eq!(
        tool(&registry, "grep", json!({"pattern":"needle"}))
            .await
            .unwrap()
            .text,
        "sub/yes.txt:2:needle\n"
    );
}

#[tokio::test]
async fn output_caps_spill_and_paginate_with_utf8_progress() {
    let dir = tempfile::tempdir().unwrap();
    let content = format!("{}\n{}", "€".repeat(20_000), "x\n".repeat(3000));
    std::fs::write(dir.path().join("long.txt"), &content).unwrap();
    let registry = registry(dir.path(), &[]);
    let mut page = tool(&registry, "read", json!({"path":"long.txt"}))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(page.overflow.take().unwrap().path()).unwrap(),
        content
    );
    assert!(page.text.len() <= 50 * 1024);
    let mut reassembled = page.text;
    while let Some(cursor) = page.next_cursor {
        assert!(tool(&registry, "list", json!({"cursor":cursor}))
            .await
            .is_err());
        page = tool(
            &registry,
            "read",
            json!({"path":"long.txt","cursor":cursor}),
        )
        .await
        .unwrap();
        assert!(page.text.lines().count() <= 2000);
        reassembled.push_str(&page.text);
    }
    assert_eq!(reassembled, content);
}

#[test]
fn submission_rejects_duplicate_members_at_any_depth() {
    let dir = tempfile::tempdir().unwrap();
    let registry = registry(dir.path(), &[]);
    assert!(registry
        .validate_submission(r#"{"results":{"a":{"x":1,"\u0078":2}}}"#)
        .is_err());
    assert!(registry
        .validate_submission(r#"{"results":{"a":[1.234,9223372036854775808,null,true]}}"#)
        .is_ok());
}

#[tokio::test]
async fn private_project_metadata_is_invisible_to_every_read_tool() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let private = [
        ".MaCrO",
        ".git",
        ".aws",
        ".codex",
        "auth.json",
        "credentials",
    ];
    std::fs::create_dir(root.join("nested")).unwrap();
    for name in private {
        let relative = format!("nested/{name}");
        std::fs::write(root.join(&relative), "shared marker").unwrap();
    }
    std::fs::create_dir_all(root.join(".GIT")).unwrap();
    std::fs::write(root.join(".GIT/secret"), "shared marker").unwrap();
    std::fs::write(root.join("nested/public.txt"), "shared marker").unwrap();
    let registry = registry(root, &[]);
    for name in private {
        let error = tool(&registry, "read", json!({"path":format!("nested/{name}")}))
            .await
            .unwrap_err();
        assert_eq!(error, "Read permission denied");
    }
    for (name, args) in [
        ("list", json!({"path":"nested"})),
        ("glob", json!({"pattern":"**"})),
        ("grep", json!({"pattern":"shared marker"})),
    ] {
        let output = tool(&registry, name, args).await.unwrap().text;
        assert!(output.contains("nested/public.txt"), "{output}");
        assert!(!output.contains("secret"), "{output}");
        for name in private {
            assert!(
                !output
                    .to_ascii_lowercase()
                    .contains(&name.to_ascii_lowercase()),
                "{output}"
            );
        }
    }
}
