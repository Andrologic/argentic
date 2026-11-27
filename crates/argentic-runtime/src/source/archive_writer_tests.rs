use super::{OpenedSource, SelectedFile, MANIFEST};
use std::{fs, path::Path, process::Command};

const SCRIPT: &str = "mcp/run.sh";
const NESTED: &str = "skills/review/nested/測.bin";

fn project(root: &Path) {
    for (path, bytes) in [
        (
            MANIFEST,
            br#"{
                "projectFormat":"agsdl-project-1","contract":"agsdl-0.4.0","id":"export",
                "agents":{"review":{"file":"agents/review.json"}},
                "flow":{"entry":"s","steps":{"s":{"type":"agent","agent":"review"}}},
                "skills":{"review":{"uri":"skills/review/SKILL.md"}},
                "settingsFiles":{"review":"settings.json"},
                "secrets":{"file":"secrets.json"},
                "mcpServers":{"local":{"command":["sh","mcp/run.sh"]}},
                "mcpFiles":{"local":["mcp/run.sh"]}
            }
"#
            .as_slice(),
        ),
        (
            "agents/review.json",
            br#"{"harness":"codex","permissions":"read-only","skills":["review"],
                "resources":{"notes":{"uri":"../notes.md"}}}"#,
        ),
        ("settings.json", br#"{"webSearch":"disabled"}"#),
        ("secrets.json", br#"{"local":["EXAMPLE_TOKEN"]}"#),
        ("notes.md", b"Notes.\r\n"),
        ("skills/review/SKILL.md", b"Review.\n"),
        (NESTED, b"\0\xff\n"),
        (SCRIPT, b"#!/bin/sh\nexit 0\n"),
        ("UNRELATED.txt", b"Unrelated.\n"),
    ] {
        let file = root.join(path);
        fs::create_dir_all(file.parent().unwrap()).unwrap();
        fs::write(file, bytes).unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root.join(SCRIPT), fs::Permissions::from_mode(0o755)).unwrap();
    }
}

fn files(source: &OpenedSource) -> Vec<SelectedFile<'_>> {
    source.selected_files().unwrap().collect()
}

fn archive(source: &OpenedSource) -> Vec<u8> {
    let mut output = Vec::new();
    source.write_archive(&mut output).unwrap();
    output
}

#[test]
fn selected_files_and_archive_keep_the_opened_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    project(&root);
    let folder = OpenedSource::open_path(&root).unwrap();
    let second_open = OpenedSource::open_path(&root.join(MANIFEST)).unwrap();
    let expected = [
        "agents/review.json",
        MANIFEST,
        SCRIPT,
        "notes.md",
        "secrets.json",
        "settings.json",
        "skills/review/SKILL.md",
        NESTED,
    ];
    let selected = files(&folder);
    assert_eq!(
        selected.iter().map(|file| file.path).collect::<Vec<_>>(),
        expected
    );
    for file in &selected {
        assert_eq!(file.bytes, fs::read(root.join(file.path)).unwrap());
        assert_eq!(file.executable, cfg!(unix) && file.path == SCRIPT);
    }

    // Export uses the opened bytes even after the author changes the draft.
    let output = archive(&second_open);
    fs::remove_dir_all(&root).unwrap();
    assert_eq!(output, archive(&folder));
    let reopened = OpenedSource::open_archive_bytes(&output).unwrap();
    assert_eq!(reopened.authored_json(), folder.authored_json());
    assert_eq!(reopened.secrets(), folder.secrets());
    assert_eq!(reopened.mcp_files(), folder.mcp_files());
    assert_eq!(files(&reopened), selected);
    assert_eq!(output, archive(&reopened));
    for source in [folder, reopened] {
        for (pointer, origin) in [
            ("", (MANIFEST, "")),
            ("/flow/entry", (MANIFEST, "/flow/entry")),
            ("/agents/review", ("agents/review.json", "")),
            (
                "/agents/review/resources/notes",
                ("agents/review.json", "/resources/notes"),
            ),
            (
                "/agents/review/harnessSettings/webSearch",
                ("settings.json", "/webSearch"),
            ),
        ] {
            assert_eq!(source.component_origin(pointer), Some(origin));
        }
        assert_eq!(source.component_origin("projectSnapshot"), None);
        assert_eq!(source.component_origin("/agents/review/~2"), None);
        assert_eq!(source.component_origin("/agents/review~"), None);
        assert_eq!(
            source.component_origin("/agents/review/a~1b~0c"),
            Some(("agents/review.json", "/a~1b~0c"))
        );
    }
}

#[test]
fn archive_write_and_flush_failures_are_storage_errors() {
    struct Rejects;
    impl std::io::Write for Rejects {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("rejected"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("rejected"))
        }
    }
    let temp = tempfile::tempdir().unwrap();
    project(temp.path());
    let source = OpenedSource::open_path(temp.path()).unwrap();
    let write = source.write_archive(&mut Rejects).unwrap_err();
    let mut output = std::io::BufWriter::with_capacity(1 << 20, Rejects);
    let flush = source.write_archive(&mut output).unwrap_err();
    for error in [write, flush] {
        assert_eq!(error.code, "STORAGE");
        assert_eq!(error.pointer, "archive");
    }
}

#[cfg(any(unix, windows))]
#[test]
fn a_multiply_linked_project_member_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    project(temp.path());
    let original = temp.path().join("skills/review/notes.md");
    let alias = temp.path().join("skills/review/duplicate.md");
    fs::write(&original, "Shared file").unwrap();
    fs::hard_link(&original, &alias).unwrap();
    let error = OpenedSource::open_path(temp.path()).err().unwrap();
    assert_eq!(error.code, "PROJECT");
    fs::remove_file(alias).unwrap();
    assert!(OpenedSource::open_path(temp.path()).is_ok());
}

#[cfg(unix)]
#[test]
fn a_selected_skill_symlink_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    project(temp.path());
    std::os::unix::fs::symlink("../../notes.md", temp.path().join("skills/review/link.md"))
        .unwrap();
    let error = OpenedSource::open_path(temp.path()).err().unwrap();
    assert_eq!(error.code, "PROJECT");
    assert!(
        error.message.contains("Skill tree contains a link"),
        "{error:?}"
    );
}

#[test]
fn a_json_document_has_no_project_files_or_archive() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("document.json");
    fs::write(&file, "{}").unwrap();
    let source = OpenedSource::open_path(&file).unwrap();
    assert!(source.selected_files().is_none());
    assert_eq!(source.component_origin("/agents"), None);
    let mut output = Vec::new();
    let error = source.write_archive(&mut output).unwrap_err();
    assert_eq!(error.code, "PROJECT");
    assert!(error.message.contains("Source is not an AgSDL project"));
    assert!(output.is_empty());
}

#[test]
fn archive_interchanges_with_reference_tool() {
    let explicit = std::env::var_os("AGSDL_REFERENCE_CHECKOUT").map(std::path::PathBuf::from);
    if let Some(root) = &explicit {
        assert!(
            root.join("tooling/agsdl/archive.py").is_file(),
            "AGSDL_REFERENCE_CHECKOUT does not point to an AgSDL checkout: {}",
            root.display()
        );
    }
    let reference = explicit.or_else(|| {
        let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })?;
        Some(Path::new(&home).join("github/AgSDL"))
    });
    let Some(tooling) = reference
        .map(|root| root.join("tooling"))
        .filter(|path| path.join("agsdl/archive.py").is_file())
    else {
        eprintln!("Skipping archive interoperability: AgSDL reference checkout unavailable");
        return;
    };
    if !Command::new("python3")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
    {
        eprintln!("Skipping archive interoperability: python3 unavailable");
        return;
    }
    let run = |action: &str, paths: &[&Path]| {
        let command = Command::new("python3")
            .current_dir(&tooling)
            .args(["-B", "-m", "agsdl.archive", action])
            .args(paths)
            .output()
            .unwrap();
        assert!(
            command.status.success(),
            "{action} failed: {}",
            String::from_utf8_lossy(&command.stderr)
        );
        command.stdout
    };
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("project");
    project(&root);
    // A selected sidecar name carries project data, even with the metadata header.
    let component = root.join("agents/review.json");
    let mut doc: serde_json::Value =
        serde_json::from_slice(&fs::read(&component).unwrap()).unwrap();
    doc["resources"]["modes"] = serde_json::json!({"uri":"../.agsdl-executables"});
    fs::write(component, doc.to_string()).unwrap();
    fs::write(
        root.join(".agsdl-executables"),
        "AgSDL executable paths v1\nnotes.md",
    )
    .unwrap();
    let folder = OpenedSource::open_path(&root).unwrap();

    let packed = temp.path().join("python.agsdl");
    run("pack", &[&root, &packed]);
    let from_python = OpenedSource::open_archive_bytes(&fs::read(&packed).unwrap()).unwrap();
    assert_eq!(from_python.authored_json(), folder.authored_json());
    assert_eq!(files(&from_python), files(&folder));

    let exported = temp.path().join("rust.agsdl");
    fs::write(&exported, archive(&folder)).unwrap();
    let inspected: serde_json::Value =
        serde_json::from_slice(&run("inspect", &[&exported])).unwrap();
    assert_eq!(inspected["inventory"]["archiveFormat"], "agsdl-archive-1");
    assert!(inspected["inventory"].get("history").is_none());
    assert_eq!(
        inspected["inventory"]["files"][SCRIPT]["executable"],
        cfg!(unix)
    );

    let unpacked = temp.path().join("unpacked");
    run("unpack", &[&exported, &unpacked]);
    let restored = OpenedSource::open_path(&unpacked).unwrap();
    assert_eq!(restored.authored_json(), folder.authored_json());
    assert_eq!(files(&restored), files(&folder));
    assert!(!unpacked.join("UNRELATED.txt").exists());
}
