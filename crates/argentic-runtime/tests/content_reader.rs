use argentic_runtime::content::{
    discover_skills, load_skill, resolve_content, ContentContext, ContentPayload, ContentUsage,
    ContentUse,
};
use argentic_runtime::content_io::file_helper::{self, MAX_BYTES};
use argentic_runtime::permissions::{
    normalize_requested, EffectivePolicy, ExecutionPurpose, PathContext, Preset,
    RequestedPermissions,
};
use cap_std::{ambient_authority, fs::Dir};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, path::Path, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use url::Url;

fn main() {
    // The Linux and macOS backends launch this test executable as the reader.
    if let Some(status) = file_helper::initialize().unwrap() {
        std::process::exit(status);
    }
    if file_helper::qualified_platform() {
        #[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
        file_helper::enable_qa_chain().unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(15), exercise_reader())
                    .await
                    .expect("content-reader integration test timed out");
            });
        println!("Native content reads passed on {}", std::env::consts::OS);
    }
}

async fn exercise_reader() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let plans = root.join("plans");
    let source = plans.join("document with spaces.json");
    let instructions = plans.join("mission text.txt");
    let skill = plans.join("review");
    let document = r#"{"agents":{"worker":{"skills":["review"]}},"skills":{"review":{"uri":"review/SKILL.md"}}}"#;
    fs::create_dir_all(&skill).unwrap();
    fs::write(&source, document).unwrap();
    fs::write(&instructions, "Read the mission.\n").unwrap();
    fs::write(
        skill.join("SKILL.md"),
        "---\nname: review\ndescription: Review a draft\n---\nReview the draft.\n",
    )
    .unwrap();

    let policy = normalize_requested(
        RequestedPermissions::Preset(Preset::ReadOnly),
        PathContext {
            workspace: root.clone(),
            home: root.clone(),
        },
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let origin = Url::from_file_path(&source).unwrap();
    let context = ContentContext::capture(
        "content-integration".into(),
        format!("{:x}", Sha256::digest(document.as_bytes())),
        document.into(),
        Some(origin.as_str()),
        "worker".into(),
        EffectivePolicy::new(policy.clone(), policy.clone(), policy),
        ExecutionPurpose::Agent,
        cancel.clone(),
        Arc::new(|_| Box::pin(async { panic!("read-only content must not require approval") })),
    )
    .unwrap();

    let absolute = Url::from_file_path(&instructions).unwrap();
    for (uri, usage) in [
        ("mission%20text.txt", ContentUsage::Instructions),
        (absolute.as_str(), ContentUsage::Resource),
    ] {
        let receipt = resolve_content(
            &context,
            ContentUse {
                source_json: json!({"uri":uri,"mediaType":"text/plain"}).to_string(),
                usage,
                pointer: "/file".into(),
            },
        )
        .await
        .unwrap();
        assert!(
            matches!(receipt.payload, ContentPayload::Text { text } if text == "Read the mission.\n")
        );
        assert_eq!(
            receipt.provenance.final_uri.as_deref(),
            Some(absolute.as_str())
        );
        assert!(receipt.provenance.file_parent_identity.is_some());
    }
    let remote = resolve_content(
        &context,
        ContentUse {
            source_json: json!({"uri":"file://example.invalid/share/mission.txt"}).to_string(),
            usage: ContentUsage::Resource,
            pointer: "/file".into(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(remote.code, "CONTENT_URI");

    let skills = discover_skills(&context).await.unwrap();
    assert_eq!(skills.len(), 1);
    let loaded = load_skill(&context, &skills[0].handle).await.unwrap();
    assert_eq!(loaded.instructions.trim(), "Review the draft.");

    let directory = Dir::open_ambient_dir(&plans, ambient_authority()).unwrap();
    fs::File::create(plans.join("oversized.txt"))
        .unwrap()
        .set_len(MAX_BYTES + 1)
        .unwrap();
    assert!(
        file_helper::read_confined(&directory, Path::new("oversized.txt"), &cancel, None)
            .await
            .is_err()
    );
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&instructions, plans.join("link")).unwrap();
        assert!(
            file_helper::read_confined(&directory, Path::new("link"), &cancel, None)
                .await
                .is_err()
        );
    }
    #[cfg(windows)]
    {
        let error = file_helper::read_confined(
            &directory,
            Path::new("mission text.txt:stream"),
            &cancel,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    }
    cancel.cancel();
    assert!(
        file_helper::read_confined(&directory, Path::new("mission text.txt"), &cancel, None)
            .await
            .is_err()
    );
}
