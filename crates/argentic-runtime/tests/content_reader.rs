use argentic_runtime::content::{
    discover_skills, load_skill, resolve_content, ContentContext, ContentPayload, ContentUsage,
    ContentUse,
};
use argentic_runtime::content_io::file_helper::{self, MAX_BYTES};
use argentic_runtime::permissions::{
    normalize_requested, EffectivePolicy, ExecutionPurpose, PathContext,
};
use cap_std::{ambient_authority, fs::Dir};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{fs, path::Path, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use url::Url;

#[cfg(target_os = "linux")]
#[path = "support/argentic_content.rs"]
mod argentic_content;

fn main() {
    // The Linux and macOS backends launch this test executable as the reader.
    if let Some(status) = file_helper::initialize().unwrap() {
        std::process::exit(status);
    }
    if !file_helper::qualified_platform() {
        assert!(
            std::env::var_os("ARGENTIC_REQUIRE_NATIVE_READER").is_none(),
            "The native content reader is required on this runner"
        );
        println!("Native content reader unavailable on this platform");
        return;
    }
    #[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
    file_helper::enable_qa_chain().unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(15), async {
                exercise_reader().await;
                exercise_project_sources().await;
                #[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
                rejects_substituted_content_helper_image();
                #[cfg(target_os = "linux")]
                argentic_content::run().await;
            })
            .await
            .expect("content-reader integration test timed out");
        });
    println!("Native content reads passed on {}", std::env::consts::OS);
}

#[cfg(all(target_os = "macos", feature = "agsdl-content-qa", debug_assertions))]
fn rejects_substituted_content_helper_image() {
    use argentic_runtime::content_io::macos_launcher::{arm_image_swap, Launch};
    use std::io::Read;
    use std::os::fd::AsRawFd;
    use std::process::Command;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let helper = root.join("helper");
    let source = root.join("probe.c");
    let adhoc = root.join("adhoc");
    fs::write(
        &source,
        "#include <stdio.h>\nint main(void) { puts(\"qa probe\"); }\n",
    )
    .unwrap();
    assert!(Command::new("/usr/bin/cc")
        .args([source.as_os_str(), "-o".as_ref(), adhoc.as_os_str()])
        .status()
        .unwrap()
        .success());
    let directory = Dir::open_ambient_dir(&root, ambient_authority()).unwrap();

    let mut failures = Vec::new();
    for image in [Path::new("/bin/echo"), adhoc.as_path()] {
        let replacement = root.join("replacement");
        fs::copy(std::env::current_exe().unwrap(), &helper).unwrap();
        fs::copy(image, &replacement).unwrap();
        let probe = Command::new(&replacement).arg("qa probe").output().unwrap();
        assert!(probe.status.success());
        assert_eq!(probe.stdout, b"qa probe\n");

        arm_image_swap(&helper, &replacement).unwrap();
        let result = Launch::start(&helper, Path::new("fixture.txt"), directory.as_raw_fd());
        match result {
            Err(error) if error.to_string().ends_with("(4)") => {}
            Err(error) => {
                failures.push(format!("{}: unexpected refusal: {error}", image.display()))
            }
            Ok(mut launch) => {
                launch.cancel();
                let exit = launch.wait(Duration::from_secs(5)).unwrap().unwrap();
                let mut bytes = Vec::new();
                fs::File::from(launch.output)
                    .read_to_end(&mut bytes)
                    .unwrap();
                failures.push(format!(
                    "{}: substituted helper was launched (status {}, signal {}, output {:?})",
                    image.display(),
                    exit.status,
                    exit.signal,
                    String::from_utf8_lossy(&bytes)
                ));
            }
        }
        assert_eq!(fs::read(&helper).unwrap(), fs::read(image).unwrap());
        assert!(!replacement.exists(), "QA replacement was not consumed");
        fs::remove_file(&helper).unwrap();
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

async fn exercise_reader() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let plans = root.join("plans");
    let source = plans.join("document with spaces.json");
    let instructions = plans.join("mission text.txt");
    let nested = plans.join("nested");
    let skill = plans.join("review");
    let document = r#"{"agents":{"worker":{"skills":["review"]}},"skills":{"review":{"uri":"review/SKILL.md"}}}"#;
    fs::create_dir_all(&skill).unwrap();
    fs::create_dir(&nested).unwrap();
    fs::write(&source, document).unwrap();
    fs::write(&instructions, "Read the mission.\n").unwrap();
    fs::write(nested.join("draft-privé.txt"), "Nested draft.\n").unwrap();
    let (denied_file, denied_uri) = if cfg!(windows) {
        ("draft-PRIVÉ.txt", "draft-PRIV%C3%89.txt")
    } else {
        ("draft-privé.txt", "draft-priv%C3%A9.txt")
    };
    fs::write(plans.join(denied_file), "Denied draft.\n").unwrap();
    fs::write(
        skill.join("SKILL.md"),
        "---\nname: review\ndescription: Review a draft\n---\nReview the draft.\n",
    )
    .unwrap();

    let policy = normalize_requested(
        serde_json::from_value(json!({"preset":"read-only","rules":[
            {"action":"read","effect":"deny","match":"plans/*-privé.txt"}
        ]}))
        .unwrap(),
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
    let nested_uri = Url::from_file_path(nested.join("draft-privé.txt")).unwrap();
    for (uri, usage, expected_uri, expected_text) in [
        (
            "mission%20text.txt",
            ContentUsage::Instructions,
            &absolute,
            "Read the mission.\n",
        ),
        (
            absolute.as_str(),
            ContentUsage::Resource,
            &absolute,
            "Read the mission.\n",
        ),
        (
            "nested/draft-priv%C3%A9.txt",
            ContentUsage::Resource,
            &nested_uri,
            "Nested draft.\n",
        ),
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
        assert!(matches!(receipt.payload, ContentPayload::Text { text } if text == expected_text));
        assert_eq!(
            receipt.provenance.final_uri.as_deref(),
            Some(expected_uri.as_str())
        );
        assert!(receipt.provenance.file_parent_identity.is_some());
    }
    for (uri, error_code) in [
        (denied_uri, "PERMISSION"),
        ("file://example.invalid/share/mission.txt", "CONTENT_URI"),
    ] {
        let error = resolve_content(
            &context,
            ContentUse {
                source_json: json!({"uri":uri,"mediaType":"text/plain"}).to_string(),
                usage: ContentUsage::Resource,
                pointer: "/file".into(),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, error_code);
    }

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

async fn exercise_project_sources() {
    use argentic_runtime::{
        persistence::{dto::EngineSourceRevision, start030::StartCapture030},
        source::OpenedSource,
    };
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/agsdl-project");
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().canonicalize().unwrap();
    let workspace = root.join("workspace");
    let data = root.join("data");
    fs::create_dir_all(workspace.join("agents/reviewer")).unwrap();
    fs::create_dir(&data).unwrap();
    let review = root.join("original");
    for name in [
        "agsdl.json",
        "agents/reviewer/agent.json",
        "agents/reviewer/instructions review.md",
        "flows/review.json",
        "mcp/server.json",
        "mcp/asset.txt",
        "secrets/requirements.json",
    ] {
        let target = review.join(name);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(fixture.join("review").join(name), &target).unwrap();
    }
    let bytes = fs::read(review.join("agents/reviewer/instructions review.md")).unwrap();
    // A message may attach a workspace file to a run started from any input.
    let notes = workspace.join("notes.md");
    fs::write(&notes, "Workspace notes.\n").unwrap();
    let notes =
        json!({"uri":Url::from_file_path(&notes).unwrap().as_str(),"mediaType":"text/plain"});
    fs::write(
        workspace.join("agents/reviewer/instructions review.md"),
        &bytes,
    )
    .unwrap();
    let folder = OpenedSource::open_path(&review).unwrap();
    let manifest = OpenedSource::open_path(&review.join("agsdl.json")).unwrap();
    let upload = OpenedSource::open_archive_bytes(include_bytes!(
        "fixtures/agsdl-project/review-history.agsdl"
    ))
    .unwrap();
    let archive = OpenedSource::open_path(&fixture.join("review.agsdl")).unwrap();
    let plain = workspace.join("document.json");
    fs::write(&plain, folder.authored_json()).unwrap();
    let plain = OpenedSource::open_path(&plain).unwrap();
    let policy = normalize_requested(
        serde_json::from_value(json!("read-only")).unwrap(),
        PathContext {
            workspace: workspace.clone(),
            home: workspace.clone(),
        },
    )
    .unwrap();
    let mut compiled_flow = None;
    let mut original = Some(review);
    for input in [plain, folder, manifest, upload, archive] {
        let authored = input.authored_json().to_owned();
        let doc: serde_json::Value = serde_json::from_str(&authored).unwrap();
        let resources = &doc["agents"]["reviewer"]["resources"];
        let source: EngineSourceRevision = serde_json::from_value(json!({
            "owner":{"projectId":"project","planId":"review","branch":"test"},
            "revision":"1", "sourceSha256":input.source_sha256()
        }))
        .unwrap();
        if let Some(mcp) = input.mcp_files() {
            assert_eq!(mcp["local"], ["mcp/asset.txt"]);
            assert_eq!(input.secrets().unwrap()["local"], ["FIXTURE_TOKEN"]);
        }
        let mut scope = json!({"root":workspace});
        let retained = input.retain(&data).unwrap();
        let resource = retained
            .resource_root()
            .join("agents/reviewer/instructions review.md");
        let unlisted = retained.resource_root().join("unlisted.txt");
        let retained_root = retained.resource_root().to_owned();
        retained.bind_scope(&mut scope).unwrap();
        drop((retained, input));
        if scope["projectSnapshot"].is_string() {
            if let Some(original) = original.take() {
                fs::remove_dir_all(original).unwrap();
            }
        }

        #[cfg(feature = "dev-support")]
        let (plan, registry) = {
            let (harness, agents) = argentic_runtime::capture::codex_registry(
                &doc,
                &source.source_sha256,
                &scope,
                &argentic_runtime::api::synthetic_codex_observation(),
            )
            .unwrap();
            let registry = json!({"format":"macro-agsdl-registry/030","version":"test",
                "modules":[{"harness":"codex","moduleJson":argentic_runtime::api::codex_module()}],
                "harnesses":[harness],"agents":agents,"hostCapabilities":[]});
            let result = argentic_runtime::Engine::new()
                .compile_document(source.clone(), authored.clone(), registry.to_string())
                .await
                .unwrap();
            let report: serde_json::Value = serde_json::from_str(&result.result_json).unwrap();
            assert_eq!(report["static"]["valid"], true, "{report}");
            assert_eq!(report["runtime"]["supported"], true, "{report}");
            (
                serde_json::from_str::<serde_json::Value>(
                    report["compiledPlanJson"].as_str().unwrap(),
                )
                .unwrap(),
                registry.to_string(),
            )
        };
        #[cfg(not(feature = "dev-support"))]
        let (plan, registry) = (serde_json::Value::Null, String::new());

        #[cfg(feature = "dev-support")]
        let flow = json!({"entry":plan["entry"],"steps":plan["steps"]});
        #[cfg(not(feature = "dev-support"))]
        let flow = doc["flow"].clone();
        assert!(!flow["steps"].is_null(), "{plan}");
        assert_eq!(compiled_flow.get_or_insert(flow.clone()), &flow);
        let versions = plan.get("versions").cloned().unwrap_or_else(|| {
            json!({
                "snapshot":"2","contract":"agsdl-0.4.0","compiler":"compiler",
                "runtime":"runtime","registry":"registry","profile":"macro-agsdl-030-v1"
            })
        });
        let capture = StartCapture030 {
            identity: serde_json::from_value(json!({
                "systemStartId":"system","flowInvocationId":"flow",
                "snapshotId":"snapshot","epoch":"1"
            }))
            .unwrap(),
            source,
            versions: serde_json::from_value(versions).unwrap(),
            authored_source: authored,
            compiled_plan_json: plan.to_string(),
            registry_json: registry,
            private_capture_json: json!({"callScopeJson":scope.to_string()}).to_string(),
        };
        // Recreate a context from serialized capture data after the source
        // handles have gone away, as an embedding host can do after reopening.
        let capture: StartCapture030 =
            serde_json::from_str(&serde_json::to_string(&capture).unwrap()).unwrap();
        let read_source = async |policy: argentic_runtime::permissions::NormalizedPolicy,
                                 source: &serde_json::Value| {
            // The engine's agent reader also confines resolved targets to the
            // approved workspace. Mirror that recipient scope here.
            let approved = policy.context().workspace.clone();
            let context = ContentContext::capture(
                capture.identity.snapshot_id.clone(),
                capture.source.source_sha256.clone(),
                capture.authored_source.clone(),
                scope["documentLocation"].as_str(),
                "reviewer".into(),
                EffectivePolicy::new(policy.clone(), policy.clone(), policy),
                ExecutionPurpose::Agent,
                CancellationToken::new(),
                Arc::new(|_| Box::pin(async { panic!("unexpected content approval") })),
            )
            .unwrap()
            .with_captured_source(&data, &capture)
            .unwrap()
            .with_read_scope(Arc::new(move |target| {
                if target.starts_with(&approved) {
                    Ok(())
                } else {
                    Err(argentic_runtime::content::ContentError {
                        code: "PERMISSION".into(),
                        pointer: String::new(),
                        message: "Outside the approved workspace".into(),
                    })
                }
            }));
            resolve_content(
                &context,
                ContentUse {
                    source_json: source.to_string(),
                    usage: ContentUsage::Resource,
                    pointer: "/resources/0".into(),
                },
            )
            .await
        };
        let read = async |policy| read_source(policy, &resources["review"]).await;
        let receipt = read(policy.clone()).await.unwrap();
        assert!(matches!(receipt.payload, ContentPayload::Text {text} if text.as_bytes() == bytes));
        let receipt = read_source(policy.clone(), &notes).await.unwrap();
        assert!(
            matches!(receipt.payload, ContentPayload::Text {ref text} if text == "Workspace notes.\n")
        );
        if scope.get("projectSnapshot").is_some() {
            let deny = normalize_requested(
                serde_json::from_value(json!({"preset":"read-only","rules":[
                    {"action":"read","effect":"deny","match":"agents/reviewer/**"}
                ]}))
                .unwrap(),
                PathContext {
                    workspace: workspace.clone(),
                    home: workspace.clone(),
                },
            )
            .unwrap();
            assert_eq!(read(deny).await.unwrap_err().code, "PERMISSION");
            // A workspace whose `agents` links elsewhere does not redirect or
            // refuse a retained member: its workspace path only names it.
            #[cfg(unix)]
            {
                let elsewhere = root.join("elsewhere/agents/reviewer");
                let linked = root.join("linked-workspace");
                if !linked.exists() {
                    fs::create_dir_all(&elsewhere).unwrap();
                    fs::write(elsewhere.join("instructions review.md"), "Other.\n").unwrap();
                    fs::create_dir(&linked).unwrap();
                    std::os::unix::fs::symlink(
                        root.join("elsewhere/agents"),
                        linked.join("agents"),
                    )
                    .unwrap();
                }
                let linked_policy = normalize_requested(
                    serde_json::from_value(json!("read-only")).unwrap(),
                    PathContext {
                        workspace: linked.clone(),
                        home: linked.clone(),
                    },
                )
                .unwrap();
                let receipt = read(linked_policy).await.unwrap();
                assert!(
                    matches!(receipt.payload, ContentPayload::Text {text} if text.as_bytes() == bytes)
                );
            }
            fs::write(&resource, b"changed").unwrap();
            assert_eq!(
                read(policy.clone()).await.unwrap_err().code,
                "CONTENT_UNAVAILABLE"
            );
            fs::remove_file(&resource).unwrap();
            assert_eq!(
                read(policy.clone()).await.unwrap_err().code,
                "CONTENT_UNAVAILABLE"
            );
            fs::write(&resource, &bytes).unwrap();
            // A member redirected to another member has been altered too.
            #[cfg(unix)]
            {
                fs::remove_file(&resource).unwrap();
                std::os::unix::fs::symlink(retained_root.join("mcp/asset.txt"), &resource).unwrap();
                assert_eq!(
                    read(policy.clone()).await.unwrap_err().code,
                    "CONTENT_UNAVAILABLE"
                );
                fs::remove_file(&resource).unwrap();
                fs::write(&resource, &bytes).unwrap();
            }
            // A file added to the retained copy is not a project member.
            fs::write(&unlisted, "Unlisted.\n").unwrap();
            let unlisted = json!({"uri":Url::from_file_path(&unlisted).unwrap().as_str(),"mediaType":"text/plain"});
            assert_eq!(
                read_source(policy.clone(), &unlisted)
                    .await
                    .unwrap_err()
                    .code,
                "PERMISSION"
            );
            fs::remove_file(retained_root.join("unlisted.txt")).unwrap();
        }
    }
}
