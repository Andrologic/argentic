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
