//! Folder and archive inputs apply the same selection rules.
use super::archive::tests::{row, stored_zip};
use super::*;
use serde_json::json;

fn write(root: &Path, name: &str, bytes: impl AsRef<[u8]>) {
    let path = root.join(name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

/// A minimal project whose agent reads `resource` and uses `skill`.
fn project(root: &Path, resource: &str, skill: Option<&str>) {
    let mut agent = json!({"harness":"codex","permissions":"read-only",
        "resources":{"r":{"uri":resource}}});
    let mut manifest = json!({"projectFormat":"agsdl-project-1","contract":"agsdl-0.4.0",
        "id":"selection","agents":{"a":{"file":"agents/a.json"}},
        "flow":{"entry":"s","steps":{"s":{"type":"agent","agent":"a"}}}});
    if let Some(skill) = skill {
        agent["skills"] = json!(["review"]);
        manifest["skills"] = json!({"review":{"uri":skill}});
    }
    write(root, "agsdl.json", manifest.to_string());
    write(root, "agents/a.json", agent.to_string());
    write(root, "notes.md", "Notes.\n");
}

/// Pack exactly the files a folder selected, as the official tool does.
fn pack(files: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
    let inventory = files
        .iter()
        .map(|(name, bytes)| (name.clone(), row(bytes)))
        .collect::<serde_json::Map<_, _>>();
    let mut entries: BTreeMap<String, Vec<u8>> = files
        .iter()
        .map(|(name, bytes)| (format!("project/{name}"), bytes.clone()))
        .collect();
    entries.insert(
        "archive.json".into(),
        serde_json::to_vec(&json!({"archiveFormat":"agsdl-archive-1","files":inventory})).unwrap(),
    );
    stored_zip(&entries)
}

fn files(opened: &OpenedSource) -> BTreeMap<String, Vec<u8>> {
    opened
        .project
        .as_ref()
        .unwrap()
        .files
        .iter()
        .map(|(name, file)| (name.clone(), file.bytes.clone()))
        .collect()
}

fn refused(result: Result<OpenedSource>) -> String {
    match result {
        Ok(_) => panic!("expected a refusal"),
        Err(error) => format!("{}: {}", error.code, error.message),
    }
}

#[test]
fn dot_dot_after_a_file_or_an_absent_directory_matches_the_official_reader() {
    for reference in ["../agsdl.json/../notes.md", "../ghost/../notes.md"] {
        let temp = tempfile::tempdir().unwrap();
        project(temp.path(), reference, None);
        let folder = OpenedSource::open_path(temp.path()).unwrap();
        let doc: Value = serde_json::from_str(folder.authored_json()).unwrap();
        assert_eq!(doc["agents"]["a"]["resources"]["r"]["uri"], "notes.md");
        let archive = OpenedSource::open_archive_bytes(&pack(&files(&folder))).unwrap();
        assert_eq!(
            archive.authored_json(),
            folder.authored_json(),
            "{reference}"
        );
    }
    // A normal component after a regular file names nothing in either form.
    let temp = tempfile::tempdir().unwrap();
    project(temp.path(), "../agsdl.json/x/../../notes.md", None);
    assert!(refused(OpenedSource::open_path(temp.path())).contains("traverses a regular file"));
    let mut selected = BTreeMap::new();
    project(temp.path(), "../notes.md", None);
    selected.extend(files(&OpenedSource::open_path(temp.path()).unwrap()));
    let agent = json!({"harness":"codex","permissions":"read-only",
        "resources":{"r":{"uri":"../agsdl.json/x/../../notes.md"}}});
    selected.insert("agents/a.json".into(), agent.to_string().into_bytes());
    assert!(refused(OpenedSource::open_archive_bytes(&pack(&selected)))
        .contains("traverses a regular file"));
}

#[cfg(unix)]
#[test]
fn symlinks_in_a_selected_path_are_refused() {
    let temp = tempfile::tempdir().unwrap();
    project(temp.path(), "../linked/notes.md", None);
    std::fs::create_dir(temp.path().join("real")).unwrap();
    write(temp.path(), "real/notes.md", "Notes.\n");
    std::os::unix::fs::symlink(temp.path().join("real"), temp.path().join("linked")).unwrap();
    assert!(refused(OpenedSource::open_path(temp.path())).contains("symbolic link"));
}

#[test]
fn folders_refuse_the_names_archives_refuse() {
    for hidden in [
        ".git/config",
        ".env",
        ".ENV.local",
        "nul.txt",
        "nul .md",
        "COM1 .txt",
    ] {
        let temp = tempfile::tempdir().unwrap();
        project(temp.path(), "../notes.md", Some("skills/review/SKILL.md"));
        write(temp.path(), "skills/review/SKILL.md", "Review.\n");
        let folder = OpenedSource::open_path(temp.path()).unwrap();
        write(
            temp.path(),
            &format!("skills/review/{hidden}"),
            "Private.\n",
        );
        let message = refused(OpenedSource::open_path(temp.path()));
        assert!(
            message.contains("Invalid portable path"),
            "{hidden}: {message}"
        );
        let mut selected = files(&folder);
        selected.insert(format!("skills/review/{hidden}"), b"Private.\n".to_vec());
        refused(OpenedSource::open_archive_bytes(&pack(&selected)));
    }
}

#[test]
fn folder_collisions_fold_case_and_normalize_unicode() {
    let temp = tempfile::tempdir().unwrap();
    let nfc = "caf\u{e9}.md";
    let nfd = "cafe\u{301}.md";
    project(temp.path(), "../caf%C3%A9.md", None);
    let mut agent: Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("agents/a.json")).unwrap()).unwrap();
    agent["resources"]["s"] = json!({"uri":"../cafe%CC%81.md"});
    write(temp.path(), "agents/a.json", agent.to_string());
    write(temp.path(), nfc, "Composed.\n");
    write(temp.path(), nfd, "Decomposed.\n");
    let message = refused(OpenedSource::open_path(temp.path()));
    assert!(message.contains("Case collision"), "{message}");
}

/// Every reference counts toward the 8 MiB JSON budget, as in the official
/// reader, so any folder accepted here can be packed by the official tool.
#[test]
fn json_budget_counts_every_reference_like_the_official_reader() {
    let temp = tempfile::tempdir().unwrap();
    project(temp.path(), "../notes.md", None);
    let mut manifest: Value =
        serde_json::from_slice(&std::fs::read(temp.path().join("agsdl.json")).unwrap()).unwrap();
    let padding = "x".repeat(MAX_JSON as usize - 256);
    let agent = json!({"harness":"codex","permissions":"read-only",
        "resources":{"r":{"uri":"../notes.md"},"pad":{"value":padding}}})
    .to_string();
    write(temp.path(), "agents/a.json", &agent);
    for i in 0..9 {
        manifest["agents"][format!("a{i}")] = json!({"file":"agents/a.json"});
    }
    write(temp.path(), "agsdl.json", manifest.to_string());
    assert!(10 * agent.len() as u64 > MAX_JSON_TOTAL);
    let message = refused(OpenedSource::open_path(temp.path()));
    assert!(message.contains("aggregate limit"), "{message}");
}

#[test]
fn json_documents_keep_the_host_source_codes() {
    let temp = tempfile::tempdir().unwrap();
    let code = |path: &Path| match OpenedSource::open_path(path) {
        Ok(_) => panic!("expected a refusal"),
        Err(error) => error.code,
    };
    assert_eq!(
        code(&temp.path().join("missing.json")),
        "SOURCE_UNAVAILABLE"
    );
    assert_eq!(
        code(&temp.path().join("missing/flow.json")),
        "SOURCE_UNAVAILABLE"
    );
    assert_eq!(
        code(&temp.path().join("missing.agsdl")),
        "SOURCE_UNAVAILABLE"
    );
    let large = temp.path().join("large.json");
    std::fs::File::create(&large)
        .unwrap()
        .set_len(MAX_JSON_TOTAL + 1)
        .unwrap();
    assert_eq!(code(&large), "SOURCE_UNAVAILABLE");
    #[cfg(unix)]
    {
        write(temp.path(), "real.json", "{}");
        let link = temp.path().join("link.json");
        std::os::unix::fs::symlink(temp.path().join("real.json"), &link).unwrap();
        assert_eq!(code(&link), "SOURCE_UNAVAILABLE");
        let folder = temp.path().join("linked-manifest");
        std::fs::create_dir(&folder).unwrap();
        std::os::unix::fs::symlink(temp.path().join("real.json"), folder.join(MANIFEST)).unwrap();
        assert_eq!(code(&folder.join(MANIFEST)), "SOURCE_UNAVAILABLE");
    }
}

#[test]
fn a_folder_is_limited_to_an_inventory_its_archive_could_hold() {
    let temp = tempfile::tempdir().unwrap();
    project(temp.path(), "../notes.md", Some("skills/review/SKILL.md"));
    write(temp.path(), "skills/review/SKILL.md", "Review.\n");
    let deep = format!("skills/review/{}", "d".repeat(200));
    std::fs::create_dir_all(temp.path().join(&deep)).unwrap();
    for i in 0..4000 {
        let name = format!("{deep}/{i:05}{}", "f".repeat(240));
        std::fs::write(temp.path().join(name), "x").unwrap();
    }
    let message = refused(OpenedSource::open_path(temp.path()));
    assert!(message.contains("2 MiB archive limit"), "{message}");
}

#[test]
fn retain_never_publishes_an_index_it_cannot_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let files: Files = (0..3000)
        .map(|i| {
            let name = format!("{}/{i:05}", "p".repeat(3000));
            let file = ProjectFile {
                bytes: b"x".to_vec(),
                executable: false,
            };
            (name, file)
        })
        .collect();
    let error = match snapshot::retain(temp.path(), &sha256(b"{}"), &files) {
        Ok(_) => panic!("expected a refusal"),
        Err(error) => error,
    };
    assert!(error.message.contains("read limit"), "{}", error.message);
    let published = std::fs::read_dir(temp.path().join("agsdl-snapshots"))
        .unwrap()
        .count();
    assert_eq!(published, 0);
}

#[test]
fn archive_extension_is_case_insensitive() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/conformance-050/review-project.agsdl");
    let upper = temp.path().join("REVIEW.AGSDL");
    std::fs::copy(&fixture, &upper).unwrap();
    assert_eq!(
        OpenedSource::open_path(&upper).unwrap().authored_json(),
        OpenedSource::open_path(&fixture).unwrap().authored_json()
    );
}

#[test]
fn only_json_pointers_are_located_in_a_component() {
    let temp = tempfile::tempdir().unwrap();
    project(temp.path(), "../notes.md", None);
    let opened = OpenedSource::open_path(temp.path()).unwrap();
    let located = opened.locate_error(failure("X", "/agents/a/resources/r", "Refused"));
    assert_eq!(located.message, "agents/a.json#/resources/r: Refused");
    for category in ["projectSnapshot", "skills", "source"] {
        let error = opened.locate_error(failure("X", category, "Refused"));
        assert_eq!(error.message, "Refused", "{category}");
    }
}

/// HOST.md: a damaged copy is refused, and removing it lets the next open
/// retain the same identity again.
#[test]
fn a_damaged_copy_is_refused_until_it_is_removed() {
    let temp = tempfile::tempdir().unwrap();
    let project_dir = temp.path().join("project");
    let data = temp.path().join("data");
    std::fs::create_dir(&data).unwrap();
    project(&project_dir, "../notes.md", None);
    let opened = OpenedSource::open_path(&project_dir).unwrap();
    let scope = |retained: &RetainedSource| {
        let mut scope = json!({});
        retained.bind_scope(&mut scope).unwrap();
        scope
    };
    let first = scope(&opened.retain(&data).unwrap());
    let id = first["projectSnapshot"].as_str().unwrap().to_owned();
    let copy = data.join("agsdl-snapshots").join(&id);
    std::fs::write(copy.join("project/notes.md"), "Changed.\n").unwrap();
    assert_eq!(
        opened.retain(&data).err().unwrap().code,
        "CONTENT_UNAVAILABLE"
    );
    std::fs::remove_dir_all(&copy).unwrap();
    assert_eq!(scope(&opened.retain(&data).unwrap()), first);
    assert_eq!(
        std::fs::read(copy.join("project/notes.md")).unwrap(),
        b"Notes.\n"
    );
}

/// A remote upload must not learn host paths from a storage failure.
#[test]
fn retain_failures_name_neither_the_project_nor_host_paths() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("agsdl-snapshots"), "not a directory").unwrap();
    let opened = OpenedSource::open_archive_bytes(include_bytes!(
        "../../tests/fixtures/conformance-050/review-project.agsdl"
    ))
    .unwrap();
    let error = opened.retain(temp.path()).err().unwrap();
    assert_eq!(error.code, "STORAGE");
    let root = temp.path().to_str().unwrap();
    assert!(!error.message.contains(root), "{}", error.message);
}

/// As in the official reader, a component used only on the way to `..`
/// still needs a portable name and no case collision, even when absent.
#[test]
fn components_before_dot_dot_follow_the_name_rules() {
    for (reference, expected) in [
        ("../CON/../notes.md", "Invalid portable path"),
        ("../Agents/../notes.md", "Case collision"),
    ] {
        let temp = tempfile::tempdir().unwrap();
        project(temp.path(), reference, None);
        let message = refused(OpenedSource::open_path(temp.path()));
        assert!(message.contains(expected), "{reference}: {message}");
        let mut selected = BTreeMap::new();
        project(temp.path(), "../notes.md", None);
        selected.extend(files(&OpenedSource::open_path(temp.path()).unwrap()));
        let agent = json!({"harness":"codex","permissions":"read-only",
            "resources":{"r":{"uri":reference}}});
        selected.insert("agents/a.json".into(), agent.to_string().into_bytes());
        let message = refused(OpenedSource::open_archive_bytes(&pack(&selected)));
        assert!(message.contains(expected), "{reference}: {message}");
    }
}

#[cfg(unix)]
#[test]
fn a_linked_project_root_or_snapshot_folder_is_refused() {
    let temp = tempfile::tempdir().unwrap();
    let real = temp.path().join("real");
    project(&real, "../notes.md", None);
    let link = temp.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    for input in [link.clone(), link.join(MANIFEST)] {
        let message = refused(OpenedSource::open_path(&input));
        assert!(
            message.contains("root cannot be a symbolic link"),
            "{message}"
        );
    }
    let data = temp.path().join("data");
    std::fs::create_dir_all(temp.path().join("elsewhere")).unwrap();
    std::fs::create_dir(&data).unwrap();
    std::os::unix::fs::symlink(temp.path().join("elsewhere"), data.join("agsdl-snapshots"))
        .unwrap();
    let error = OpenedSource::open_path(&real)
        .unwrap()
        .retain(&data)
        .err()
        .unwrap();
    assert_eq!(error.code, "STORAGE");
}

#[test]
fn skill_tree_directories_follow_the_name_rules_even_when_empty() {
    for directory in [".git", "CON", "Sub"] {
        let temp = tempfile::tempdir().unwrap();
        project(temp.path(), "../notes.md", Some("skills/review/SKILL.md"));
        write(temp.path(), "skills/review/SKILL.md", "Review.\n");
        write(temp.path(), "skills/review/sub", "File.\n");
        std::fs::create_dir(temp.path().join("skills/review").join(directory)).unwrap();
        let message = refused(OpenedSource::open_path(temp.path()));
        assert!(
            message.contains("Invalid portable path") || message.contains("Case collision"),
            "{directory}: {message}"
        );
    }
}

/// A trailing separator must not let a symlinked root through.
#[cfg(unix)]
#[test]
fn a_linked_root_is_refused_with_a_trailing_separator() {
    let temp = tempfile::tempdir().unwrap();
    let real = temp.path().join("real");
    project(&real, "../notes.md", None);
    std::os::unix::fs::symlink(&real, temp.path().join("link")).unwrap();
    let mut spelled = temp.path().join("link").into_os_string();
    spelled.push("/");
    let message = refused(OpenedSource::open_path(Path::new(&spelled)));
    assert!(
        message.contains("root cannot be a symbolic link"),
        "{message}"
    );
}

/// A plain JSON source is read without listing its directory, as before.
#[cfg(unix)]
#[test]
fn a_plain_document_needs_only_search_permission_on_its_parent() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let drop = temp.path().join("drop");
    write(&drop, "doc.json", r#"{"contract":"agsdl-0.4.0"}"#);
    std::fs::set_permissions(&drop, std::fs::Permissions::from_mode(0o311)).unwrap();
    let opened = OpenedSource::open_path(&drop.join("doc.json"));
    std::fs::set_permissions(&drop, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        opened.unwrap().authored_json(),
        r#"{"contract":"agsdl-0.4.0"}"#
    );
}

/// Filesystems with fixed modes cannot keep the executable bit. The index
/// records it, so a retained copy whose mode differs still reopens.
#[cfg(unix)]
#[test]
fn a_retained_copy_does_not_depend_on_file_modes() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let files: Files = [("run.sh", true), ("notes.md", false)]
        .into_iter()
        .map(|(name, executable)| {
            let file = ProjectFile {
                bytes: b"x".to_vec(),
                executable,
            };
            (name.to_owned(), file)
        })
        .collect();
    let first = snapshot::retain(temp.path(), &sha256(b"{}"), &files).unwrap();
    let script = first.resource_root().join("run.sh");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o600)).unwrap();
    snapshot::retain(temp.path(), &sha256(b"{}"), &files).unwrap();
}
