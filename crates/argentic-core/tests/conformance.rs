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
