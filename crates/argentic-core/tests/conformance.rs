use argentic_core::validate_document_json;
use serde_json::{json, Value};
use std::{collections::BTreeSet, fs, path::Path};

const EXAMPLE: &[u8] = include_bytes!("fixtures/conformance/modules/example/module.json");
const LIMITED: &[u8] = include_bytes!("fixtures/conformance/modules/limited/module.json");

#[test]
fn published_agsdl_030_cases_have_the_expected_diagnostic_codes() {
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/conformance/expected.json")).unwrap();
    let expected = expected.as_object().unwrap();
    let cases = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/conformance/cases");
    let actual_cases: BTreeSet<_> = cases
        .read_dir()
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(expected.len(), 144);
    assert_eq!(
        actual_cases,
        expected.keys().map(|name| format!("{name}.json")).collect()
    );

    for (name, expected) in expected {
        let bytes = fs::read(cases.join(format!("{name}.json"))).unwrap();
        let report = validate_document_json(&bytes, Some(&[EXAMPLE, LIMITED])).unwrap();
        let report: Value = serde_json::from_str(&report).unwrap();
        assert_eq!(report["contract"], "agsdl-0.3.0", "{name}");
        check_diagnostics(name, &report, expected);
    }
}

fn check_diagnostics(name: &str, report: &Value, expected: &Value) {
    assert_eq!(
        report["valid"],
        expected["errors"].as_array().unwrap().is_empty(),
        "{name}"
    );
    for (severity, field) in [("error", "errors"), ("warning", "warnings")] {
        let actual: BTreeSet<_> = report["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|diagnostic| diagnostic["severity"] == severity)
            .map(|diagnostic| diagnostic["code"].as_str().unwrap())
            .collect();
        let wanted = expected[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|code| code.as_str().unwrap())
            .collect();
        assert_eq!(actual, wanted, "{name} {field}");
    }
}

#[test]
fn agsdl_040_comments_and_join_selections() {
    const MODULE: &[u8] = include_bytes!("fixtures/conformance-040/modules/example/module.json");
    for (name, document, errors) in [
        (
            "approval-comments-path",
            &include_bytes!("fixtures/conformance-040/cases/approval-comments-path.json")[..],
            vec!["MESSAGE_PATH"],
        ),
        (
            "fanout-join-text-not-list",
            &include_bytes!("fixtures/conformance-040/cases/fanout-join-text-not-list.json")[..],
            vec!["MESSAGE_PATH"],
        ),
        (
            "fanout-join-approval-comments",
            &include_bytes!("fixtures/conformance-040/cases/fanout-join-approval-comments.json")[..],
            vec![],
        ),
    ] {
        let report: Value =
            serde_json::from_str(&validate_document_json(document, Some(&[MODULE])).unwrap())
                .unwrap();
        assert_eq!(report["contract"], argentic_core::AGSDL_040);
        check_diagnostics(name, &report, &json!({"errors":errors,"warnings":[]}));
    }
}

#[test]
#[ignore = "requires the reference corpus exported from the published tag"]
fn agsdl_040_full_static_corpus() {
    let dir = std::path::PathBuf::from(std::env::var_os("AGSDL_040_CONFORMANCE").unwrap());
    let expected: Value =
        serde_json::from_slice(&fs::read(dir.join("expected.json")).unwrap()).unwrap();
    let expected = expected.as_object().unwrap();
    assert_eq!(expected.len(), 147);
    let example = fs::read(dir.join("modules/example/module.json")).unwrap();
    let limited = fs::read(dir.join("modules/limited/module.json")).unwrap();
    for (name, expected) in expected {
        let bytes = fs::read(dir.join("cases").join(format!("{name}.json"))).unwrap();
        let report: Value = serde_json::from_str(
            &validate_document_json(&bytes, Some(&[&example, &limited])).unwrap(),
        )
        .unwrap();
        if name == "shape-wrong-contract" {
            assert_eq!(expected["errors"], json!(["SHAPE"]));
            assert_eq!(report["contract"], "agsdl-0.3.0");
            check_diagnostics(name, &report, &json!({"errors":[],"warnings":[]}));
        } else {
            check_diagnostics(name, &report, expected);
        }
    }
}

#[test]
fn report_includes_inferred_joins_and_diagnostic_paths() {
    let source = include_bytes!("fixtures/conformance/cases/parallel-join.json");
    let report = validate_document_json(source, Some(&[EXAMPLE])).unwrap();
    let report: Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        report["joins"]["merge"],
        json!({"origin":"analyze", "branches":["security","performance"]})
    );

    let report = validate_document_json(source, Some(&[])).unwrap();
    let report: Value = serde_json::from_str(&report).unwrap();
    let diagnostics = report["diagnostics"].as_array().unwrap();
    assert!(diagnostics.iter().any(|diagnostic| {
        diagnostic["code"] == "HARNESS_UNVERIFIED"
            && diagnostic["path"] == "/agents/reviewer/harness"
    }));

    let report = validate_document_json(b"\xff", None).unwrap();
    let report: Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report["diagnostics"][0]["code"], "PARSE");
    assert_eq!(report["diagnostics"][0]["path"], "");
}
