use crate::{
    compile::host,
    native::{s, JsonDecoder, Text},
    native_json::Decoder,
    reader,
    validate::{self, ModuleMap, Report},
    SourceBudget, AGSDL_040, GRAPH_LIMIT_CODE, MAX_SOURCE_BYTES, MAX_SOURCE_WORK,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::time::Instant;

fn approval(next: Value) -> Value {
    json!({"type":"approval","title":"Continue?","next":next})
}

fn validate(json: &str) -> (Report, (usize, usize)) {
    let budget = SourceBudget::default();
    budget.json_input(json.as_bytes(), "").unwrap();
    let parsed = crate::exact::scan::parse_v030(json.as_bytes()).unwrap();
    let report =
        validate::validate_document_with_budget(&parsed.tree, &ModuleMap::new(), &budget).unwrap();
    (report, budget.usage())
}

#[test]
fn converging_approval_choices_only_propagate_each_dependency_once() {
    let mut steps = Map::new();
    steps.insert(
        "entry".into(),
        approval(json!({"left":"l0","right":"r0","direct":{"step":"end","message":{"prompt":"Review."}}})),
    );
    for i in 0..26 {
        let next = if i == 25 {
            json!({"yes":"end","no":"end"})
        } else {
            json!({"yes":format!("l{}", i + 1),"no":format!("r{}", i + 1)})
        };
        steps.insert(format!("l{i}"), approval(next.clone()));
        steps.insert(format!("r{i}"), approval(next));
    }
    steps.insert("end".into(), json!({"type":"agent","agent":"a"}));
    let doc = json!({"contract":AGSDL_040,"id":"approval-choices","agents":{"a":{"harness":"codex","permissions":"read-only"}},"flow":{"entry":"entry","steps":steps}});
    let (report, (work, bytes)) = validate(&doc.to_string());
    assert_eq!(
        report
            .diagnostics
            .iter()
            .filter(|d| d.severity == "error")
            .map(|d| d.code)
            .collect::<Vec<_>>(),
        ["PROMPT"],
        "{}",
        report.to_json()
    );
    assert!(work < 125_000 && bytes < 4 * 1024 * 1024, "{work}, {bytes}");
}

#[test]
fn expanded_compositions_and_deep_json_refuse_with_a_pointer() {
    let compose =
        |name, route: Value| json!({"type":"compose","composition":name,"next":{"done":route}});
    let bodies = (0..14).map(|index| {
        let output = json!({"output":"done"});
        let body = if index == 13 {
            json!({"entry":"one","outputs":["done"],"steps":{"one":approval(json!({"yes":output,"no":[]}))}})
        } else {
            let next = format!("body{}", index + 1);
            json!({"entry":"one","outputs":["done"],"steps":{
                "one":compose(next.clone(), json!("two")),"two":compose(next, output)
            }})
        };
        (format!("body{index}"), body)
    }).collect::<Map<_,_>>();
    let doc = json!({"contract":AGSDL_040,"id":"compositions","agents":{},"compositions":bodies,
        "flow":{"entry":"start","steps":{"start":compose("body0".into(), json!([]))}}});
    let budget = SourceBudget::default();
    let refusal = reader::validate_source(doc.to_string().as_bytes(), &budget).unwrap_err();
    assert_eq!(refusal.code, GRAPH_LIMIT_CODE, "{refusal:?}");
    assert!(refusal.pointer.starts_with("/compositions/"), "{refusal:?}");
    assert!(budget.usage().1 < 32 * 1024 * 1024);

    let input = format!("{}null{}", "[".repeat(4096), "]".repeat(4096));
    let budget = SourceBudget::default();
    let error = reader::validate_source(input.as_bytes(), &budget).unwrap_err();
    assert_eq!(error.code, GRAPH_LIMIT_CODE);
    assert_eq!(error.pointer, "");
    assert!(error.message.contains("nesting depth"));
    assert!(budget.usage().1 < 64 * 1024);
}

#[test]
fn long_max_lexeme_stays_short_in_every_fanout_diagnostic() {
    let mut steps = Map::new();
    steps.insert("origin".into(), json!({"type":"agent","agent":"origin","next":[{"step":"s0","each":["instructions"],"max":"NUMBER"}]}));
    for i in 0..250 {
        let next = if i == 249 {
            json!([])
        } else {
            json!(format!("s{}", i + 1))
        };
        steps.insert(
            format!("s{i}"),
            json!({"type":"agent","agent":"worker","maxVisits":1,"next":next}),
        );
    }
    let doc = json!({"contract":AGSDL_040,"id":"long-literal","agents":{"origin":{"harness":"codex","permissions":"read-only","instructs":true},"worker":{"harness":"codex","permissions":"read-only"}},"flow":{"entry":"origin","steps":steps}});
    let authored = doc
        .to_string()
        .replace("\"NUMBER\"", &"9".repeat(128 * 1024));
    let (report, (work, bytes)) = validate(&authored);
    let warnings: Vec<_> = report
        .diagnostics
        .iter()
        .filter(|d| d.code == "FANOUT_VISITS")
        .collect();
    assert_eq!(warnings.len(), 250, "{}", report.to_json());
    assert!(warnings
        .iter()
        .all(|d| d.message.len() < 400 && d.message.contains('…')));
    assert!(report.valid(), "{}", report.to_json());
    assert!(
        work < 500_000 && bytes < 12 * 1024 * 1024,
        "{work}, {bytes}"
    );
}

#[test]
fn reports_stop_at_the_diagnostic_limit() {
    let agents = (0..1100)
        .map(|i| {
            (
                format!("a{i}"),
                json!({"harness":"example","permissions":"read-only"}),
            )
        })
        .collect::<Map<_, _>>();
    let source = json!({"contract":AGSDL_040,"id":"warnings","agents":agents});
    let (report, (work, bytes)) = validate(&source.to_string());
    assert!(!report.valid());
    assert_eq!(report.diagnostics[0].code, "HARNESS_UNVERIFIED");
    assert!(report.diagnostics.len() <= 1025);
    assert_eq!(report.diagnostics.last().unwrap().code, GRAPH_LIMIT_CODE);
    assert!(report
        .diagnostics
        .last()
        .unwrap()
        .path
        .starts_with("/agents/"));
    assert!(report.to_json().len() < 512 * 1024);
    assert!(
        work < 150_000 && bytes < 12 * 1024 * 1024,
        "{work}, {bytes}"
    );
}

#[test]
fn compilation_of_200_agents_20_joins_and_approvals_stays_below_the_budget() {
    let mut agents = Map::new();
    let mut proofs = Vec::new();
    let mut steps = Map::new();
    let receipt = r#"{"fixture":true}"#;
    for section in 0..20 {
        let join = format!("join{section}");
        let ask = format!("approve{section}");
        let first = section * 10;
        let branches: Vec<_> = (1..10)
            .map(|branch| format!("a{}", first + branch))
            .collect();
        for agent in first..first + 10 {
            let name = format!("a{agent}");
            let definition = json!({"harness":"codex","permissions":"read-only"});
            proofs.push(json!({"agentName":name,"agentDeclarationJson":definition.to_string(),"settingsJson":"{}",
                "permissionsJson":receipt,"scopeJson":receipt,"evidenceJson":receipt,"provenanceJson":receipt}));
            agents.insert(name.clone(), definition);
            let next = if agent == first {
                json!(branches)
            } else {
                json!(join)
            };
            steps.insert(
                name.clone(),
                json!({"type":"agent","agent":name,"next":next}),
            );
        }
        steps.insert(join, json!({"type":"join","next":ask}));
        let next = if section == 19 {
            json!([])
        } else {
            json!(format!("a{}", first + 10))
        };
        steps.insert(ask, approval(json!({"yes":next,"no":[]})));
    }
    let doc = json!({"contract":AGSDL_040,"id":"review-pipeline","agents":agents,"flow":{"entry":"a0","steps":steps}}).to_string();
    let source = json!({"owner":{"projectId":"p","planId":"review-pipeline","branch":"main"},"revision":"1","sourceSha256":format!("{:x}",Sha256::digest(doc.as_bytes()))}).to_string();
    let native = |name| json!({"name":name,"support":"native","evidenceJson":receipt});
    let caps = [
        "result-submission",
        "continuity",
        "permission-enforcement",
        "contained-stop",
        "response-attribution",
        "delivery.queue",
        "resources",
    ]
    .map(native);
    let registry = json!({"format":"macro-agsdl-registry/030","version":"test","modules":[],"agents":proofs,
        "harnesses":[{"harness":"codex","version":"1.0.0","installation":"available","adapterId":"codex-app-server-v1","qualified":true,
            "capabilities":caps,"models":[{"name":"test-model","efforts":[],"defaultEffort":null}],"defaultModel":"test-model"}],
        "hostCapabilities":[native("approval")],"startEntry":{"messageJson":"{}"}}).to_string();
    let budget = SourceBudget::default();
    let t = Instant::now();
    budget.json_input(registry.as_bytes(), "/registry").unwrap();
    budget.json_input(source.as_bytes(), "/source").unwrap();
    budget.bytes(doc.len() * 2, "").unwrap();
    let decoder = Decoder(|number| Ok(number.to_owned()));
    let output = host::compile(
        &s(doc),
        &decoder.parse(&Text::from(source)).unwrap(),
        &decoder.parse(&Text::from(registry)).unwrap(),
        &budget,
        &crate::calculation::default_available_harnesses(),
    )
    .unwrap();
    let elapsed = t.elapsed();
    let (work, bytes) = budget.usage();
    eprintln!("200 agents, 20 joins, 20 approvals: {elapsed:?}, {work} work units, {bytes} charged bytes, {} output bytes", output.len());
    let output: Value = serde_json::from_str(&output).unwrap();
    assert_eq!(
        output["runtime"]["supported"], true,
        "{}",
        output["runtime"]
    );
    assert_eq!(output["plan"]["steps"].as_array().unwrap().len(), 240);
    assert_eq!(output["plan"]["joins"].as_array().unwrap().len(), 20);
    assert_eq!(output["entryValidation"]["valid"], true);
    let encoded: Value =
        serde_json::from_str(output["compiledPlanJson"].as_str().unwrap()).unwrap();
    assert_eq!(encoded, output["plan"]);
    assert!(
        work < MAX_SOURCE_WORK / 4 && bytes < MAX_SOURCE_BYTES / 4,
        "{work}, {bytes}"
    );
}

#[test]
fn joins_with_many_overlapping_branches_bound_diagnostic_work() {
    let targets: Vec<_> = (0..128).map(|i| format!("branch{i}")).collect();
    let mut steps: Map<_, _> = targets
        .iter()
        .map(|name| {
            (
                name.clone(),
                json!({"type":"agent","agent":"a","next":"shared"}),
            )
        })
        .collect();
    let mut targets = targets;
    targets.push("join".into());
    steps.insert(
        "start".into(),
        json!({"type":"agent","agent":"a","next":targets}),
    );
    steps.insert(
        "shared".into(),
        json!({"type":"agent","agent":"a","next":"join"}),
    );
    steps.insert("join".into(), json!({"type":"join"}));
    let doc = json!({"contract":AGSDL_040,"id":"overlapping","agents":{"a":{"harness":"codex","permissions":"read-only"}},"flow":{"entry":"start","steps":steps}});
    let (report, (work, bytes)) = validate(&doc.to_string());
    assert_eq!(report.diagnostics.last().unwrap().code, GRAPH_LIMIT_CODE);
    assert_eq!(
        report.diagnostics.last().unwrap().path,
        "/flow/steps/shared"
    );
    assert!(
        work < 500_000 && bytes < 16 * 1024 * 1024,
        "{work}, {bytes}"
    );
}

#[test]
fn large_permission_match_spends_work_before_building_match_rows() {
    let doc = json!({"contract":AGSDL_040,"id":"command-check","agents":{"a":{
        "harness":"codex","completionCheck":{"command":"x".repeat(4000)},
        "permissions":{"rules":[{"action":"shell","effect":"allow","match":"x*".repeat(2000)}]}
    }}});
    let (report, (work, bytes)) = validate(&doc.to_string());
    let error = report.diagnostics.last().unwrap();
    assert_eq!(error.code, GRAPH_LIMIT_CODE, "{}", report.to_json());
    assert_eq!(error.path, "/agents/a/completionCheck/command");
    assert!(work < 100_000 && bytes < 4 * 1024 * 1024, "{work}, {bytes}");
}
