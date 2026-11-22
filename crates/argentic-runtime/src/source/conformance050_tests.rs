//! AgSDL v0.5.0 conformance: project assembly and archive interchange.
//! The fixtures are the official review example and archives packed from it
//! with the v0.5.0 tool. See `tests/fixtures/conformance-050/README.md`.
use super::*;
use serde_json::json;

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/conformance-050"
);
const REFERENCE: &str = include_str!("../../tests/fixtures/conformance-050/projects/review.json");
const ARCHIVES: [&str; 3] = [
    "review-project.agsdl",
    "review-project-history.agsdl",
    "review-notes.agsdl",
];

fn fixture(name: &str) -> PathBuf {
    Path::new(FIXTURES).join(name)
}

fn files(opened: &OpenedSource) -> BTreeMap<String, Vec<u8>> {
    let project = opened.project.as_ref().expect("a project source");
    project
        .files
        .iter()
        .map(|(name, file)| (name.clone(), file.bytes.clone()))
        .collect()
}

/// The example's files as published, read from the fixture copy of the tag.
fn example_files() -> BTreeMap<String, Vec<u8>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let name = path.strip_prefix(root).unwrap().to_str().unwrap();
                out.insert(name.replace('\\', "/"), std::fs::read(&path).unwrap());
            }
        }
    }
    let root = fixture("review-project");
    let mut out = BTreeMap::new();
    walk(&root, &root, &mut out);
    out
}

fn assert_reference(opened: &OpenedSource, input: &str) {
    let assembled: Value = serde_json::from_str(opened.authored_json()).unwrap();
    let reference: Value = serde_json::from_str(REFERENCE).unwrap();
    assert_eq!(assembled, reference, "{input} assembles to the reference");
    assert_eq!(
        opened.secrets(),
        Some(&BTreeMap::from([(
            "issues".into(),
            vec!["ISSUES_TOKEN".into()]
        )])),
        "{input} keeps the separate secret requirements"
    );
    assert_eq!(opened.mcp_files(), Some(&BTreeMap::new()), "{input}");
}

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let target = to.join(path.file_name().unwrap());
        if path.is_dir() {
            copy_tree(&path, &target);
        } else {
            std::fs::copy(&path, &target).unwrap();
        }
    }
}

#[test]
fn official_review_project_assembles_to_the_reference_document() {
    let folder = fixture("review-project");
    assert_reference(&OpenedSource::open_path(&folder).unwrap(), "folder");
    assert_reference(
        &OpenedSource::open_path(&folder.join(MANIFEST)).unwrap(),
        "manifest",
    );
}

#[test]
fn official_archives_round_trip_to_the_reference_with_identical_bytes() {
    let published = example_files();
    let folder = OpenedSource::open_path(&fixture("review-project")).unwrap();
    assert_eq!(files(&folder), published);
    for name in ARCHIVES {
        let path = fixture(name);
        for (input, opened) in [
            ("path", OpenedSource::open_path(&path).unwrap()),
            (
                "upload",
                OpenedSource::open_archive_bytes(&std::fs::read(&path).unwrap()).unwrap(),
            ),
        ] {
            assert_reference(&opened, &format!("{name} ({input})"));
            assert_eq!(opened.authored_json(), folder.authored_json(), "{name}");
            assert_eq!(files(&opened), published, "{name} ({input}) source bytes");
        }
    }
}

#[test]
fn notes_beside_the_manifest_leave_the_inventory_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let notes = temp.path().join("review-notes");
    copy_tree(&fixture("review-project"), &notes);
    std::fs::write(notes.join("NOTES.md"), "Unrelated notes.\n").unwrap();
    let plain = OpenedSource::open_path(&fixture("review-project")).unwrap();
    let noted = OpenedSource::open_path(&notes).unwrap();
    assert_eq!(files(&noted), files(&plain));
    assert_eq!(noted.source_sha256(), plain.source_sha256());

    // The official packer ran on the same copy with notes. The reader checks
    // the inventory against every entry, so equal files mean equal inventories.
    let inventory = |name: &str| {
        let files = archive::read(std::fs::File::open(fixture(name)).unwrap()).unwrap();
        files
            .into_iter()
            .map(|(name, file)| (name, (sha256(&file.bytes), file.executable)))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        inventory("review-notes.agsdl"),
        inventory("review-project.agsdl")
    );
    assert!(!inventory("review-notes.agsdl").contains_key("NOTES.md"));

    // Every input retains the same snapshot, so a resumed run finds the same bytes.
    let data = temp.path().join("data");
    std::fs::create_dir(&data).unwrap();
    let mut scopes = Vec::new();
    for source in [
        plain,
        noted,
        OpenedSource::open_path(&fixture("review-project.agsdl")).unwrap(),
        OpenedSource::open_path(&fixture("review-project-history.agsdl")).unwrap(),
        OpenedSource::open_path(&fixture("review-notes.agsdl")).unwrap(),
    ] {
        let retained = source.retain(&data).unwrap();
        let mut scope = json!({});
        retained.bind_scope(&mut scope).unwrap();
        assert_eq!(
            std::fs::read(
                retained
                    .resource_root()
                    .join("agents/reviewer/instructions.md")
            )
            .unwrap(),
            example_files()["agents/reviewer/instructions.md"]
        );
        scopes.push(scope);
    }
    assert!(
        scopes.windows(2).all(|pair| pair[0] == pair[1]),
        "{scopes:?}"
    );
}

/// The official example equips the agent with a remote MCP server. The
/// Argentic profile refuses both before a run starts, naming the component.
#[test]
fn argentic_profile_refuses_the_official_example_with_located_diagnostics() {
    for input in [
        fixture("review-project"),
        fixture("review-project-history.agsdl"),
    ] {
        let opened = OpenedSource::open_path(&input).unwrap();
        let doc: Value = serde_json::from_str(opened.authored_json()).unwrap();
        let slice = crate::api::product_slice(opened.authored_json(), None).unwrap();
        assert_eq!(
            (slice.code.as_str(), slice.pointer.as_str()),
            ("UNSUPPORTED_030_FEATURE", "/mcpServers/issues/url")
        );
        let located = opened.locate_error(failure(&slice.code, &slice.pointer, &slice.message));
        assert!(
            located.message.starts_with("mcp/issues.json"),
            "{}",
            located.message
        );
        let codex = crate::api::validate_codex_document(&doc).unwrap_err();
        assert_eq!(
            (codex.code.as_str(), codex.pointer.as_str()),
            ("HARNESS_CAPABILITY", "/agents/reviewer/mcpServers")
        );
        let located = opened.locate_error(codex);
        assert!(
            located.message.starts_with("agents/reviewer/agent.json"),
            "{}",
            located.message
        );
    }
}
