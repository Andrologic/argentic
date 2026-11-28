use argentic_core::calculation;
use serde_json::{json, Value};

fn number(raw: &str) -> Result<String, String> {
    Ok(raw.to_owned())
}

#[test]
fn human_clock_rejects_non_object_closed_asks() {
    let owner = json!({"systemStartId": "system", "ownerEpoch": "epoch"});
    for closed_asks in [Value::Null, json!(-1), json!("x"), json!([])] {
        let event = json!({
            "owner": owner, "eventId": "hold", "nowMs": "0",
            "event": {
                "kind": "operatorHold", "held": [], "released": [],
                "cancelAsks": [], "closedAsks": closed_asks,
            }
        });
        let input = json!({"operation": "humanClock", "input": {
            "owner": owner, "eventId": "hold", "expectedRevision": "0",
            "stateJson": "", "eventJson": event.to_string(),
            "optionsJson": r#"{"maxSliceMs":1000}"#,
        }});
        let report: Value =
            serde_json::from_str(&calculation::route(&input.to_string(), number).unwrap()).unwrap();
        assert_eq!(report["kind"], "failed", "{closed_asks}");
    }
}

/// One agent route fanning out over `results.items` of the producer output.
fn fanout_input(output: &Value) -> String {
    let output_json = output.to_string();
    let step = |name: &str| {
        json!({
            "address": [name], "type": "agent", "agent": name, "scope": [],
            "declarationJson": json!({"type": "agent", "agent": name}).to_string(),
        })
    };
    let plan = json!({
        "format": "macro-agsdl-compiled-plan/2",
        "documentJson": json!({"agents": {
            "lead": {"results": {"items": {}}}, "worker": {},
        }}).to_string(),
        "steps": [step("lead"), step("worker")],
        "scopes": [{"address": [], "outputs": [], "entry": ["lead"]}],
        "routes": [{
            "source": ["lead"], "kind": "normal", "key": null,
            "declarationJson": r#"{"step":"worker","each":["results","items"]}"#,
            "targets": [{
                "kind": "step", "address": ["worker"], "messageJson": null,
                "eachPathJson": r#"["results","items"]"#,
                "concurrencyJson": null, "maxJson": null,
            }],
        }],
    });
    json!({
        "planJson": plan.to_string(),
        "reference": {"route": {"source": ["lead"], "kind": "normal", "key": null}},
        "producer": {
            "activationId": "a1", "step": ["lead"], "kind": "agent",
            "outputJson": output_json,
        },
        "source": {
            "terminalId": "a1", "producerStep": ["lead"], "producerAgent": "lead",
            "kind": "agent", "outputJson": output_json,
            "provenance": {"instructions": [], "results": null, "prompt": null},
        },
        "nativeReceiptId": "receipt",
        "contentReceipts": {"format": "macro-agsdl-route-content/1", "occurrences": []},
        "deferPromptUris": true,
    })
    .to_string()
}

fn items_output(items: usize, padding: usize) -> Value {
    let item = "x".repeat(padding);
    json!({"text": "", "results": {"items": vec![item; items]}})
}

/// The calculation envelope and, when ready, the decoded prepared result.
fn prepare(output: &Value) -> (Value, Option<String>) {
    let report: Value =
        serde_json::from_str(&calculation::route(&fanout_input(output), number).unwrap()).unwrap();
    let result = report["resultJson"].as_str().map(str::to_owned);
    (report, result)
}

fn prepared(items: usize, padding: usize) -> String {
    let (report, result) = prepare(&items_output(items, padding));
    assert_eq!(report["kind"], "ready", "{report}");
    result.unwrap()
}

#[test]
fn fanout_preparation_delivers_each_item_once() {
    let result: Value = serde_json::from_str(&prepared(3, 4)).unwrap();
    assert_eq!(result["kind"], "ready", "{result}");
    let items = result["batches"][0]["items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(result["provenance"].as_array().unwrap().len(), 3);
    for (index, item) in items.iter().enumerate() {
        assert_eq!(item["itemJson"], r#""xxxx""#);
        assert_eq!(item["occurrence"]["itemIndex"], index.to_string());
        let message: Value = serde_json::from_str(item["messageJson"].as_str().unwrap()).unwrap();
        assert_eq!(message["item"], "xxxx");
    }
}

#[test]
fn fanout_preparation_stops_at_the_route_result_limit() {
    // Each occurrence repeats the source and producer, and the default agent
    // message repeats the result list, so the result grows by about three
    // outputs per item. With ~60 KB outputs, 8 MB holds about 40 items.
    let padding = 300;
    let kind = |items: usize| {
        let result = prepared(items, padding);
        let kind = serde_json::from_str::<Value>(&result).unwrap()["kind"].clone();
        (kind, result.len())
    };
    // Find the largest fan-out that is still prepared.
    let (mut ready, mut refused) = (1, 400);
    while refused - ready > 1 {
        let middle = (ready + refused) / 2;
        if kind(middle).0 == "ready" {
            ready = middle;
        } else {
            refused = middle;
        }
    }
    // It fits the transport, and only just: one more item would not.
    let (_, largest) = kind(ready);
    let (_, smaller) = kind(ready - 1);
    assert!(largest <= argentic_core::MAX_ROUTE_RESULT_BYTES);
    assert!(largest + (largest - smaller) > argentic_core::MAX_ROUTE_RESULT_BYTES);
    // That item fails as the target's fan-out limit, not as an oversized result.
    let (report, result) = prepare(&items_output(refused, padding));
    assert_eq!(report["kind"], "ready", "{report}");
    let failed: Value = serde_json::from_str(result.as_ref().unwrap()).unwrap();
    assert_eq!(failed["kind"], "failed");
    assert_eq!(failed["targetOrdinal"], 0);
    assert_eq!(failed["error"]["code"], "FANOUT_LIMIT");
    // A fan-out of thousands of items fails the same way, without building
    // the multi-gigabyte result it would previously have produced.
    let (report, result) = prepare(&items_output(8000, 1));
    assert_eq!(report["kind"], "ready", "{report}");
    let failed: Value = serde_json::from_str(result.as_ref().unwrap()).unwrap();
    assert_eq!(failed["error"]["code"], "FANOUT_LIMIT");
}

fn nested(depth: usize) -> Value {
    let mut value = json!(0);
    for _ in 0..depth {
        value = json!([value]);
    }
    value
}

fn depth_failure(output: &Value) -> Value {
    let (report, _) = prepare(output);
    report
}

#[test]
fn fanout_preparation_applies_the_source_depth_limit() {
    // JSON parsers downstream accept at most 127 nested levels. At 123 the
    // output nests 126 levels and the default message, which puts the
    // selected list one level deeper than the output does, 127.
    let ready = json!({"text": "", "results": {"items": [nested(123)]}});
    assert_eq!(depth_failure(&ready)["kind"], "ready");
    // At 124 the output still fits but its message would nest 128 levels;
    // beyond, neither fits.
    for item_depth in [124, 125, 126, 1000] {
        let output = json!({"text": "", "results": {"items": [nested(item_depth)]}});
        let report = depth_failure(&output);
        assert_eq!(report["kind"], "failed", "{item_depth}: {report}");
        assert_eq!(report["code"], "INPUT_LIMIT", "{item_depth}: {report}");
    }
}

#[test]
fn fanout_preparation_refuses_a_deep_item_without_recursing() {
    // serde_json cannot build this value, so encode the text directly.
    let depth = 200_000;
    let item = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
    let output = format!(r#"{{"text":"","results":{{"items":[{item}]}}}}"#);
    let input = fanout_input(&json!({"placeholder": true}))
        .replace(r#"{\"placeholder\":true}"#, &output.replace('"', r#"\""#));
    let report: Value = serde_json::from_str(&calculation::route(&input, number).unwrap()).unwrap();
    assert_eq!(report["kind"], "failed", "{report}");
    assert_eq!(report["code"], "INPUT_LIMIT", "{report}");
}

const CLOCK_OPTIONS: &str = r#"{"maxSliceMs":1000}"#;

fn clock_owner() -> Value {
    json!({"systemStartId": "system", "ownerEpoch": "epoch"})
}

fn clock_input(state: &str, revision: usize, event_json: &str) -> String {
    let event: Value = serde_json::from_str(event_json).unwrap();
    json!({"operation": "humanClock", "input": {
        "owner": clock_owner(), "eventId": event["eventId"],
        "expectedRevision": revision.to_string(), "stateJson": state,
        "eventJson": event_json, "optionsJson": CLOCK_OPTIONS,
    }})
    .to_string()
}

/// The calculation envelope and, when ready, its `stateJson` and `receiptJson`.
fn clock_step(state: &str, revision: usize, event_json: &str) -> (Value, Option<(String, String)>) {
    let report: Value = serde_json::from_str(
        &calculation::route(&clock_input(state, revision, event_json), number).unwrap(),
    )
    .unwrap();
    let output = report["resultJson"].as_str().map(|raw| {
        let result: Value = serde_json::from_str(raw).unwrap();
        (
            result["stateJson"].as_str().unwrap().to_owned(),
            result["receiptJson"].as_str().unwrap().to_owned(),
        )
    });
    (report, output)
}

#[test]
fn human_clock_reproduces_a_journal_written_by_the_previous_engine() {
    // Written by main at 629f525: admissions, questions, actions, plans,
    // approvals, expiries, operator holds, withdrawal, acks and retirements.
    let stored = include_str!("fixtures/human-clock-journal-1.json").trim_end();
    let journal: Value = serde_json::from_str(stored).unwrap();
    let entries = journal["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 30);
    let mut state = String::new();
    for (revision, entry) in entries.iter().enumerate() {
        let event = entry["eventJson"].as_str().unwrap();
        let (report, output) = clock_step(&state, revision, event);
        let (next, receipt) = output.unwrap_or_else(|| panic!("{revision}: {report}"));
        assert_eq!(
            receipt,
            entry["receiptJson"].as_str().unwrap(),
            "{revision}"
        );
        state = next;
    }
    assert_eq!(state, stored);
    // The stored journal still replays: a repeated event returns its receipt,
    // and a new event extends it.
    let last = entries.last().unwrap();
    let (_, repeated) = clock_step(stored, entries.len(), last["eventJson"].as_str().unwrap());
    assert_eq!(
        repeated.unwrap(),
        (
            stored.to_owned(),
            last["receiptJson"].as_str().unwrap().to_owned()
        )
    );
    let tick = json!({"owner": clock_owner(), "eventId": "next", "nowMs": "100001",
        "event": {"kind": "tick"}});
    let (report, extended) = clock_step(stored, entries.len(), &tick.to_string());
    assert_eq!(report["kind"], "ready", "{report}");
    let extended: Value = serde_json::from_str(&extended.unwrap().0).unwrap();
    assert_eq!(
        extended["entries"].as_array().unwrap().len(),
        entries.len() + 1
    );
}

#[test]
fn human_clock_refuses_a_journal_the_host_could_not_submit() {
    // A long work identity makes every receipt large, so a few transitions
    // reach the ceiling that hundreds of ordinary works would.
    let work = "w".repeat(1_400_000);
    let event = |id: usize, body: Value| {
        json!({"owner": clock_owner(), "eventId": format!("e{id}"),
            "nowMs": (id * 10).to_string(), "event": body})
        .to_string()
    };
    let admit = event(
        0,
        json!({"kind": "admit", "workId": work,
        "timeoutMs": null, "answerTimeoutMs": null}),
    );
    let (_, output) = clock_step("", 0, &admit);
    let mut state = output.unwrap().0;
    let mut revision = 1;
    loop {
        let tick = event(revision, json!({"kind": "tick"}));
        let (report, output) = clock_step(&state, revision, &tick);
        match output {
            Some((next, _)) => {
                state = next;
                revision += 1;
                assert!(revision < 10, "the journal never reached its limit");
            }
            None => {
                assert_eq!(report["kind"], "failed", "{report}");
                assert_eq!(report["code"], "INPUT_LIMIT", "{report}");
                assert_eq!(report["pointer"], "/stateJson", "{report}");
                break;
            }
        }
    }
    // The last accepted journal still fits the host's clock input with the
    // refused event, so the refusal comes from the engine, not the transport.
    let refused = event(revision, json!({"kind": "tick"}));
    let input = json!({"operation": "humanClock", "input": {
        "identity": {"systemStartId": "system", "flowInvocationId": "flow",
            "snapshotId": "snapshot", "epoch": "epoch"},
        "owner": clock_owner(), "eventId": format!("e{revision}"),
        "expectedRevision": revision.to_string(), "stateJson": state,
        "eventJson": refused, "optionsJson": CLOCK_OPTIONS,
    }})
    .to_string();
    assert!(
        serde_json::to_string(&state).unwrap().len()
            <= argentic_core::MAX_HUMAN_CLOCK_JOURNAL_BYTES
    );
    assert!(input.len() < 8_000_000, "{}", input.len());
}
