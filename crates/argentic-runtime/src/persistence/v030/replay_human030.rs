//! Readback of 040 uses the same native deadline and pause checks as COMMIT.
use super::*;
use crate::agsdl_host_flow030::{NativeSystemStopReceipt030, SupersededClockTicket030};
use std::collections::BTreeMap;
type NativeError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, NativeError>;
fn failure(_: &str, _: &str, message: &str) -> NativeError {
    message.to_owned().into()
}
fn require(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(failure("CORRELATION", "human", message))
    }
}
fn human_comment_is_empty030(comment: &str) -> bool {
    comment.chars().all(|c| matches!(c,'\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'))
}
#[path = "human_answer030.rs"]
mod answer;
use answer::HumanAnswer030;
#[path = "human_clock_verify030.rs"]
mod core;
use core::*;
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HumanProjection030 {
    pub answers: Vec<Value>,
    pub timeout_ids: Vec<String>,
}
fn parsed(row: &sqlx::sqlite::SqliteRow, key: &str) -> Result<Value> {
    Ok(serde_json::from_str(row.try_get(key)?)?)
}
fn rows_json(value: &Value, key: &str) -> Result<Vec<Value>> {
    Ok(value[key].as_array().ok_or("Missing clock array")?.clone())
}
pub(super) async fn verify(
    db: &mut SqliteConnection,
    state: &FlowState030,
    plan: &FlowPlan030,
) -> Result<HumanProjection030> {
    let id = &state.identity;
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='engine_flow030_clock')",
    )
    .fetch_one(&mut *db)
    .await?;
    let admitted = sqlx::query_scalar::<_, String>(
        "SELECT effect_id FROM engine_flow030_handoffs
         WHERE system_id=? AND invocation_id=? AND receipt_json ->> '$.kind'='nativeOwnedHandoff030'",
    )
    .bind(&id.system_start_id)
    .bind(&id.flow_invocation_id)
    .fetch_all(&mut *db)
    .await?
    .into_iter()
    .collect::<std::collections::BTreeSet<_>>();
    let requires_clock = state
        .activations
        .iter()
        .filter(|a| {
            a.start_effect_id
                .as_ref()
                .is_some_and(|effect| admitted.contains(effect))
        })
        .any(|a| {
            let step = plan
                .steps
                .iter()
                .find(|s| Some(&s.address) == a.step.as_ref());
            step.is_some_and(|s| s.r#type == "approval" || s.timeout_ms_json.is_some())
                || a.agent.as_ref().is_some_and(|name| {
                    plan.agents.iter().any(|agent| {
                        agent["agentName"] == *name
                            && agent["agentDeclarationJson"]
                                .as_str()
                                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                                .is_some_and(|a| {
                                    a.get("answerTimeoutMs").is_some() || a["planFirst"] == "ask"
                                })
                    })
                })
        });
    if exists == 0 {
        require(!requires_clock, "Required human clock schema absent")?;
        return Ok(HumanProjection030::default());
    }
    let clocks =
        sqlx::query("SELECT * FROM engine_flow030_clock WHERE system_id=? AND invocation_id=?")
            .bind(&id.system_start_id)
            .bind(&id.flow_invocation_id)
            .fetch_all(&mut *db)
            .await?;
    let events=sqlx::query("SELECT * FROM engine_flow030_clock_events WHERE system_id=? AND invocation_id=? ORDER BY rowid").bind(&id.system_start_id).bind(&id.flow_invocation_id).fetch_all(&mut *db).await?;
    let work_rows=sqlx::query("SELECT * FROM engine_flow030_clock_works WHERE system_id=? AND invocation_id=? ORDER BY rowid").bind(&id.system_start_id).bind(&id.flow_invocation_id).fetch_all(&mut *db).await?;
    let request_rows=sqlx::query("SELECT * FROM engine_flow030_human_requests WHERE system_id=? AND invocation_id=? ORDER BY rowid").bind(&id.system_start_id).bind(&id.flow_invocation_id).fetch_all(&mut *db).await?;
    if clocks.is_empty() {
        require(
            !requires_clock && events.is_empty() && work_rows.is_empty() && request_rows.is_empty(),
            "Clock parent absent",
        )?;
        return Ok(HumanProjection030::default());
    }
    require(clocks.len() == 1, "Duplicate clock")?;
    let clock = &clocks[0];
    let retained_observed = ms(clock.try_get("observed_ms")?)?;
    let native_owner = sqlx::query(
        "SELECT o.owner_epoch,o.generation,o.status,EXISTS (
            SELECT 1 FROM engine_flow030_interruptions i WHERE i.system_id=o.system_id
        ) AS interrupted FROM engine_flow030_owners o WHERE o.system_id=? AND o.invocation_id=?",
    )
    .bind(&id.system_start_id)
    .bind(&id.flow_invocation_id)
    .fetch_one(&mut *db)
    .await?;
    let stops = sqlx::query(
        "SELECT request_id,request_json,receipt_json,event_id
         FROM engine_flow030_system_stop_requests
         WHERE system_id=? AND invocation_id=? AND owner_epoch=? AND generation=? AND event_id IS NOT NULL",
    )
    .bind(&id.system_start_id)
    .bind(&id.flow_invocation_id)
    .bind(native_owner.get::<&str, _>("owner_epoch"))
    .bind(native_owner.get::<&str, _>("generation"))
    .fetch_all(&mut *db)
    .await?;
    require(stops.len() <= 1, "Duplicate native Stop")?;
    let stop = if let Some(row) = stops.first() {
        let receipt: NativeSystemStopReceipt030 =
            serde_json::from_str(row.try_get("receipt_json")?)?;
        let request: Value = parsed(row, "request_json")?;
        let event_id: &str = row.try_get("event_id")?;
        let record = state
            .events
            .get(event_id)
            .ok_or("Missing native Stop event")?;
        let event: Value = serde_json::from_str(&record.event_json)?;
        require(
            state.stopping
                && receipt.identity == *id
                && receipt.owner_epoch == native_owner.get::<&str, _>("owner_epoch")
                && receipt.generation == native_owner.get::<&str, _>("generation")
                && receipt.request_id == row.get::<&str, _>("request_id")
                && request["requestId"] == receipt.request_id
                && request["systemStartId"] == id.system_start_id
                && request["activationId"].is_null()
                && receipt.flow_event_id.as_deref() == Some(event_id)
                && event_id == format!("system-stop:{}", receipt.request_id)
                && event["kind"] == "Stop"
                && event["expectedRevision"] == receipt.before_revision
                && record.receipt.revision == receipt.revision
                && receipt.stopping,
            "Native Stop differs",
        )?;
        Some(receipt)
    } else {
        None
    };
    let superseded = stop
        .as_ref()
        .and_then(|stop| stop.superseded_clock_ticket.as_ref());
    let fenced_stop = plan.versions.contract == argentic_core::AGSDL_040
        && native_owner.get::<&str, _>("status") == "revoked"
        && stop.is_some()
        && superseded.is_none();
    let mut observed_supersession = false;
    let mut superseded_retirement = None;
    require(
        clock.get::<&str, _>("owner_epoch") == native_owner.get::<&str, _>("owner_epoch")
            && clock.get::<&str, _>("generation") == native_owner.get::<&str, _>("generation"),
        "Clock owner differs",
    )?;
    let owner =
        json!({"systemStartId":id.system_start_id,"ownerEpoch":clock.get::<&str,_>("owner_epoch")});
    let mut works = Vec::<Work>::new();
    let mut requests = Vec::<Request>::new();
    let mut closed_asks = std::collections::BTreeSet::new();
    let mut operator_closures = std::collections::BTreeSet::new();
    let mut submissions = Vec::<Submission>::new();
    let mut previous = String::new();
    let mut observed = 0u64;
    let mut last = json!({});
    let mut journal = Vec::<Value>::new();
    for (index, row) in events.iter().enumerate() {
        let ticket: crate::agsdl_host_clock030::HostClockInput030 =
            serde_json::from_str(row.try_get("ticket_json")?)?;
        require(
            ticket.identity == *id
                && serde_json::to_value(&ticket.owner)? == owner
                && ticket.event_id == row.get::<&str, _>("event_id")
                && ticket.expected_revision == index.to_string()
                && ticket.state_json == previous,
            "Clock ticket correlation",
        )?;
        let options: Value = serde_json::from_str(&ticket.options_json)?;
        require(
            options == json!({"maxSliceMs":2147483647}),
            "Clock options differ",
        )?;
        let envelope: Value = serde_json::from_str(&ticket.event_json)?;
        let event = &envelope["event"];
        let kind = event["kind"].as_str().ok_or("Clock event kind absent")?;
        let now = ms(envelope["nowMs"].as_str().ok_or("Clock time absent")?)?;
        require(
            now >= observed && envelope["owner"] == owner && envelope["eventId"] == ticket.event_id,
            "Clock event identity/time",
        )?;
        match kind {
            "admit" | "approval" => {
                let work_id = event["workId"].as_str().ok_or("Work id absent")?;
                require(
                    !works.iter().any(|w| w.id == work_id),
                    "Duplicate work clock",
                )?;
                let r = work_rows
                    .iter()
                    .find(|r| r.get::<&str, _>("activation_id") == work_id)
                    .ok_or("Clock work row absent")?;
                let work = parsed(r, "work_json")?;
                let activation = state
                    .activations
                    .iter()
                    .find(|a| a.id == work_id)
                    .ok_or("Clock activation absent")?;
                let step = match &activation.step {
                    Some(address) => Some(
                        plan.steps
                            .iter()
                            .find(|s| &s.address == address)
                            .ok_or("Clock step absent")?,
                    ),
                    None if activation.agent.is_some() => None,
                    None => return Err("Clock work has no captured declaration".into()),
                };
                let work_kind = step.map(|s| s.r#type.as_str()).unwrap_or("agent");
                let timeout = step.and_then(|s| s.timeout_ms_json.clone());
                require(
                    r.get::<&str, _>("owner_epoch") == clock.get::<&str, _>("owner_epoch")
                        && r.get::<&str, _>("generation") == clock.get::<&str, _>("generation")
                        && work["identity"] == serde_json::to_value(id)?
                        && work["owner"] == owner
                        && work["generation"] == clock.get::<&str, _>("generation")
                        && work["kind"] == work_kind,
                    "Clock work metadata",
                )?;
                let start = r.get::<&str, _>("start_effect_id");
                require(
                    activation.start_effect_id.as_deref() == Some(start)
                        && work["reference"]
                            == json!({"activationId":activation.id,"startEffectId":start,"agentInstanceId":activation.instance_id})
                        && r.get::<Option<String>, _>("timeout_json") == timeout,
                    "Clock work reference/duration",
                )?;
                let origin=sqlx::query("SELECT started_ms FROM engine_flow030_handoff_clock WHERE system_id=? AND invocation_id=? AND start_effect_id=? AND owner_epoch=? AND generation=?")
                    .bind(&id.system_start_id).bind(&id.flow_invocation_id).bind(start).bind(clock.get::<&str,_>("owner_epoch")).bind(clock.get::<&str,_>("generation")).fetch_one(&mut *db).await?;
                let started = r.get::<&str, _>("admitted_ms");
                require(
                    origin.get::<&str, _>("started_ms") == started
                        && event["startedAtMs"] == started
                        && event["timeoutMs"] == json!(timeout),
                    "Clock origin/duration",
                )?;
                let declaration: Value = step
                    .map(|s| serde_json::from_str(&s.declaration_json))
                    .transpose()?
                    .unwrap_or(Value::Null);
                require(
                    (kind == "approval") == (work_kind == "approval"),
                    "Clock admission kind differs",
                )?;
                if kind == "approval" {
                    let choices = declaration["next"]
                        .as_object()
                        .ok_or("Approval choices absent")?;
                    require(
                        !choices.is_empty()
                            && event["choices"] == json!(choices.keys().collect::<Vec<_>>()),
                        "Approval ticket choices differ",
                    )?;
                }
                let agent_declaration = activation
                    .agent
                    .as_ref()
                    .and_then(|name| plan.agents.iter().find(|a| a["agentName"] == *name))
                    .and_then(|a| a["agentDeclarationJson"].as_str())
                    .map(serde_json::from_str::<Value>)
                    .transpose()?;
                let answer_limit = agent_declaration
                    .as_ref()
                    .unwrap_or(&declaration)
                    .get("answerTimeoutMs")
                    .map(Value::to_string);
                require(
                    r.get::<Option<String>, _>("answer_timeout_json") == answer_limit
                        && event
                            .get("answerTimeoutMs")
                            .map_or(kind == "approval", |limit| *limit == json!(answer_limit)),
                    "Authored answer duration differs",
                )?;
                works.push(Work {
                    id: work_id.into(),
                    kind: work_kind.into(),
                    start: ms(started)?,
                    timeout: r.get("timeout_json"),
                    answer_timeout: r.get("answer_timeout_json"),
                    snapshot: json!({}),
                    retired: false,
                });
            }
            "open" | "associate" | "answer" | "approvalAnswer" | "ack" | "retire" | "tick"
            | "operatorHold" => {}
            _ => return Err("Unknown clock event".into()),
        }
        if kind == "open" || kind == "approval" {
            let request_id = event["requestId"].as_str().ok_or("Request id absent")?;
            require(
                !requests.iter().any(|r| r.id == request_id),
                "Duplicate request",
            )?;
            let r = request_rows
                .iter()
                .find(|r| r.get::<&str, _>("request_id") == request_id)
                .ok_or("Request row absent")?;
            if kind == "approval" {
                require(
                    parsed(r, "payload_json")?["choices"] == event["choices"],
                    "Approval presentation choices differ",
                )?;
            }
            let ids = if kind == "approval" {
                vec![event["workId"]
                    .as_str()
                    .ok_or("Approval work absent")?
                    .to_owned()]
            } else {
                serde_json::from_value(event["workIds"].clone())?
            };
            require(
                ids.len() == 1
                    && works.iter().any(|w| w.id == ids[0])
                    && (kind == "approval" || event["reviewer"] == "person"),
                "Request ownership",
            )?;
            let mut request = Request {
                id: request_id.into(),
                kind: r.get("kind"),
                works: ids,
                opened: ms(r.get("opened_ms"))?,
                duration: None,
                resolution: None,
                acknowledged: false,
                proven: false,
            };
            require(
                (kind == "approval" && request.kind == "approval"
                    || event["requestKind"] == request.kind)
                    && request.opened
                        == if kind == "approval" {
                            ms(event["startedAtMs"]
                                .as_str()
                                .ok_or("Approval time absent")?)?
                        } else {
                            now
                        },
                "Request opening",
            )?;
            request.duration = duration(&request, &works)?;
            requests.push(request);
        }
        if kind == "associate" {
            require(
                requests.iter().any(|r| {
                    Some(r.id.as_str()) == event["requestId"].as_str() && r.kind == "question"
                }),
                "Only questions can reassociate",
            )?;
        }
        if kind == "answer" || kind == "approvalAnswer" {
            let receipt_id = event["nativeReceiptId"]
                .as_str()
                .ok_or("Answer receipt absent")?;
            let row=sqlx::query("SELECT * FROM engine_flow030_human_submissions WHERE system_id=? AND invocation_id=? AND receipt_id=?")
                .bind(&id.system_start_id).bind(&id.flow_invocation_id).bind(receipt_id).fetch_one(&mut *db).await?;
            require(
                !submissions.iter().any(|s| s.id == receipt_id)
                    && event["requestId"] == row.get::<&str, _>("request_id")
                    && now == ms(row.get("received_ms"))?,
                "Native answer receipt",
            )?;
            let answer: HumanAnswer030 = serde_json::from_str(row.get("answer_json"))?;
            require(
                ["person", "synthetic-person"].contains(&row.get::<&str, _>("origin")),
                "Answer origin differs",
            )?;
            let matches = match &answer {
                HumanAnswer030::Approval { choice, comment } => {
                    kind == "approvalAnswer"
                        && event["choice"] == *choice
                        && event["comment"] == *comment
                }
                HumanAnswer030::Decision { decision, comment } => {
                    kind == "answer"
                        && event["answer"] == json!({"decision":decision,"comment":comment})
                }
                HumanAnswer030::Question { answer_json } => {
                    let _: Value = serde_json::from_str(answer_json)?;
                    kind == "answer" && event["answer"].is_null()
                }
            };
            require(matches, "Answer event differs from native submission")?;
            submissions.push(Submission {
                id: receipt_id.into(),
                request: row.get("request_id"),
                received: now,
                answer,
                accepted: None,
            });
        }
        if kind == "ack" {
            let r = requests
                .iter_mut()
                .find(|r| Some(r.id.as_str()) == event["requestId"].as_str())
                .ok_or("Ack request absent")?;
            require(
                r.resolution.is_some() && !closed_asks.contains(&r.id),
                "Invalid request acknowledgment",
            )?;
            r.acknowledged = true;
        }
        if kind == "operatorHold" && event.get("closedAsks").is_some() {
            let closed = &event["closedAsks"];
            let operator_id = closed["operatorEventId"]
                .as_str()
                .ok_or("Missing operator event")?;
            let event_record = state
                .events
                .get(operator_id)
                .ok_or("Missing operator event record")?;
            let operator: Value = serde_json::from_str(&event_record.event_json)?;
            let hold = &operator["kind"]["Operator"];
            require(
                operator["eventId"] == operator_id
                    && operator["identity"] == json!(id)
                    && operator_closures.insert(operator_id.to_owned()),
                "Foreign or duplicate operator closure",
            )?;
            let affected = rows_json(closed, "requestIds")?;
            require(!affected.is_empty(), "Missing closed action requests")?;
            for request_id in affected {
                let r = requests
                    .iter()
                    .find(|r| request_id == r.id)
                    .ok_or("Unknown closed action request")?;
                let cancelled_here = rows_json(event, "cancelAsks")?.contains(&request_id);
                require(
                    r.kind == "ask"
                        && (if cancelled_here {
                            r.resolution.is_none()
                        } else {
                            r.resolution
                                .as_ref()
                                .is_some_and(|value| value["operatorCancelled"] != true)
                        })
                        && !r.acknowledged
                        && r.works.len() == 1
                        && rows_json(event, "held")?.contains(&json!(r.works[0]))
                        && (hold.pointer("/Hold/activation_id") == Some(&json!(r.works[0]))
                            || hold
                                .pointer("/Pause/activation_ids")
                                .and_then(Value::as_array)
                                .is_some_and(|ids| ids.contains(&json!(r.works[0]))))
                        && closed_asks.insert(r.id.clone()),
                    "Invalid action closure",
                )?;
            }
        }
        let Some(output) = row.try_get::<Option<&str>, _>("output_json")? else {
            // Verify facts written at reservation above. A fenced Stop can
            // retain an undecided tail, but it contributes no clock output.
            observed_supersession = superseded
                == Some(&SupersededClockTicket030::new(
                    &ticket.event_id,
                    row.try_get("ticket_json")?,
                ));
            if observed_supersession && kind == "retire" {
                superseded_retirement = event["workId"].as_str().map(str::to_owned);
            }
            let fenced_operator = kind == "operatorHold"
                && native_owner.get::<&str, _>("status") == "revoked"
                && native_owner.get::<bool, _>("interrupted");
            require(
                (observed_supersession || fenced_stop || fenced_operator)
                    && index + 1 == events.len(),
                "Uncommitted clock ticket",
            )?;
            require(
                index != 0 || now >= retained_observed,
                "Initial clock observation is later than its reservation",
            )?;
            break;
        };
        let output: Value = serde_json::from_str(output)?;
        let receipt: Value = serde_json::from_str(
            output["receiptJson"]
                .as_str()
                .ok_or("Clock receipt absent")?,
        )?;
        validate_projection(
            works.clone(),
            requests.clone(),
            submissions.clone(),
            observed,
            &envelope,
            &receipt,
        )?;
        journal.push(json!({"eventJson":ticket.event_json,"receiptJson":output["receiptJson"]}));
        let state_json = output["stateJson"].as_str().ok_or("Clock state absent")?;
        let clock_state: Value = serde_json::from_str(state_json)?;
        require(
            clock_state
                == json!({"format":"macro-agsdl-human-clock-journal/1","owner":owner,"options":options,"entries":journal}),
            "Clock journal differs",
        )?;
        for w in &mut works {
            if kind == "retire" && event["workId"] == w.id {
                w.retired = true;
            }
            for item in rows_json(&receipt, if w.retired { "retiredClocks" } else { "clocks" })? {
                if item["workId"] == w.id {
                    w.snapshot = item["snapshot"].clone();
                }
            }
        }
        for r in &mut requests {
            for snapshot in rows_json(&receipt, "requests")? {
                if snapshot["requestId"] == r.id {
                    r.works = serde_json::from_value(snapshot["workIds"].clone())?;
                    r.duration = serde_json::from_value(snapshot["answerTimeoutMs"].clone())?;
                }
            }
            for resolution in rows_json(&receipt, "pendingResolutions")? {
                if resolution["requestId"] == r.id {
                    r.resolution = Some(resolution);
                    r.proven = true;
                }
            }
        }
        for s in &mut submissions {
            for a in rows_json(&receipt, "nativeAnswers")? {
                if a["nativeReceiptId"] == s.id {
                    s.accepted = a["accepted"].as_bool();
                }
            }
        }
        previous = state_json.into();
        observed = now;
        last = receipt;
    }
    require(
        superseded.is_none() || observed_supersession,
        "Superseded clock ticket differs",
    )?;
    require(
        clock.get::<&str, _>("revision") == journal.len().to_string()
            && clock.get::<&str, _>("state_json") == previous
            && parsed(clock, "receipt_json")? == last
            && (journal.is_empty() || retained_observed == observed),
        "Final clock differs",
    )?;
    require(
        works.len() == work_rows.len() && requests.len() == request_rows.len(),
        "Orphan clock facts",
    )?;
    let submission_rows=sqlx::query("SELECT receipt_id,accepted FROM engine_flow030_human_submissions WHERE system_id=? AND invocation_id=?").bind(&id.system_start_id).bind(&id.flow_invocation_id).fetch_all(&mut *db).await?;
    require(
        submission_rows.len() == submissions.len(),
        "Orphan native submission",
    )?;
    for row in submission_rows {
        require(
            submissions.iter().any(|s| {
                s.id == row.get::<&str, _>("receipt_id")
                    && s.accepted == row.get::<Option<bool>, _>("accepted")
            }),
            "Submission arbitration differs",
        )?;
    }
    let mut projection = HumanProjection030::default();
    for w in &works {
        let r = work_rows
            .iter()
            .find(|r| r.get::<&str, _>("activation_id") == w.id)
            .unwrap();
        require(
            r.get::<i64, _>("retired") == i64::from(w.retired)
                && parsed(r, "snapshot_json")? == w.snapshot,
            "Stored work snapshot differs",
        )?;
        let a = state.activations.iter().find(|a| a.id == w.id).unwrap();
        require(
            w.kind == "approval"
                || a.result.is_none()
                || w.retired
                || superseded_retirement.as_ref() == Some(&w.id),
            "Terminal clock remains active",
        )?;
    }
    for r in &requests {
        verify_existing(r, &submissions)?;
        let row = request_rows
            .iter()
            .find(|row| row.get::<&str, _>("request_id") == r.id)
            .unwrap();
        require(
            parsed(row, "work_ids_json")? == json!(r.works)
                && parsed(row, "deadline_json")?
                    == json!({"openedAtMs":r.opened.to_string(),"timeoutMs":r.duration}),
            "Request snapshot differs",
        )?;
        if let Some(resolution) = &r.resolution {
            let mut expected = resolution.clone();
            expected["reason"] = json!(if resolution["expired"] != true {
                "answer"
            } else if r.kind == "approval" {
                "approvalTimeout"
            } else {
                "answerTimeout"
            });
            let delivery: &str = row.get("delivery");
            let stopped_before_delivery =
                stop.is_some() && !r.acknowledged && matches!(delivery, "pending" | "reserved");
            require(
                parsed(row, "resolution_json")? == expected
                    && row.get::<&str, _>("revision") == "1"
                    && (resolution["operatorCancelled"] == true
                        || (delivery == "acknowledged" && r.acknowledged)
                        || (delivery == "reserved" && closed_asks.contains(&r.id))
                        || stopped_before_delivery),
                "Resolution/readback differs",
            )?;
            let proof=sqlx::query("SELECT p.resolution_json,e.output_json FROM engine_flow030_human_resolution_proofs p JOIN engine_flow030_clock_events e USING(system_id,invocation_id,event_id) WHERE p.system_id=? AND p.invocation_id=? AND p.request_id=?")
                .bind(&id.system_start_id).bind(&id.flow_invocation_id).bind(&r.id).fetch_one(&mut *db).await?;
            require(
                parsed(&proof, "resolution_json")? == expected,
                "Native resolution proof differs",
            )?;
            let proof_output = parsed(&proof, "output_json")?;
            let proof_receipt: Value = serde_json::from_str(
                proof_output["receiptJson"]
                    .as_str()
                    .ok_or("Proof receipt absent")?,
            )?;
            require(
                rows_json(&proof_receipt, "pendingResolutions")?.contains(resolution),
                "Proof event differs",
            )?;
            if r.kind == "approval" {
                let a = state
                    .activations
                    .iter()
                    .find(|a| r.works == [a.id.clone()])
                    .ok_or("Approval activation absent")?;
                let payload = parsed(row, "payload_json")?;
                let step = plan
                    .steps
                    .iter()
                    .find(|s| Some(&s.address) == a.step.as_ref())
                    .ok_or("Approval step absent")?;
                let declaration: Value = serde_json::from_str(&step.declaration_json)?;
                let choices = declaration["next"]
                    .as_object()
                    .ok_or("Approval choices absent")?
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                let message = a
                    .message_json
                    .as_ref()
                    .map(|s| serde_json::from_str::<Value>(s))
                    .transpose()?
                    .unwrap_or(json!({}));
                require(
                    payload["choices"] == json!(choices)
                        && payload["message"] == message
                        && payload["step"] == json!(a.step)
                        && payload["work"]
                            == json!({"activationId":a.id,"startEffectId":a.start_effect_id,"agentInstanceId":a.instance_id}),
                    "Approval presentation differs",
                )?;

                let expected = if resolution["expired"] == true {
                    json!(WorkResultRecord030::Failed {
                        error: Failure030::new("APPROVAL_TIMEOUT")
                    })
                } else {
                    json!(WorkResultRecord030::Completed {output_json:json!({"choice":resolution["answer"]["choice"],"comment":resolution["answer"]["comment"],"message":payload["message"]}).to_string(), route_key:resolution["answer"]["choice"].as_str().map(str::to_owned)})
                };
                require(
                    serde_json::to_value(&a.result)? == expected
                        || (stopped_before_delivery && a.result.is_none()),
                    "Approval terminal differs",
                )?;
            } else if resolution["expired"] == true {
                for id in &r.works {
                    let a = state
                        .activations
                        .iter()
                        .find(|a| &a.id == id)
                        .ok_or("Answered work absent")?;
                    let mut answer = json!({"work":a.step.as_ref().map(|s|s.join("/")).unwrap_or_default(),"answer":"no"});
                    if let Some(occurrence) = &a.occurrence {
                        answer["item"] = serde_json::from_str(&occurrence.item_index)?;
                    }
                    projection.answers.push(answer);
                }
            }
        } else {
            require(
                row.get::<Option<String>, _>("resolution_json").is_none(),
                "Invented resolution",
            )?;
        }
    }
    let controls = sqlx::query(
        "SELECT * FROM engine_flow030_clock_controls WHERE system_id=? AND invocation_id=?",
    )
    .bind(&id.system_start_id)
    .bind(&id.flow_invocation_id)
    .fetch_all(&mut *db)
    .await?;
    for a in &state.activations {
        if matches!(&a.result,Some(WorkResultRecord030::Failed {error}) if error.code=="TIMEOUT") {
            require(
                controls
                    .iter()
                    .any(|r| r.get::<&str, _>("activation_id") == a.id),
                "Missing timeout control",
            )?;
        }
    }
    for row in controls {
        let id = row.get::<&str, _>("activation_id");
        let w = works
            .iter()
            .find(|w| w.id == id)
            .ok_or("Timeout work absent")?;
        require(
            w.snapshot["expired"] == true
                && row.get::<i64, _>("joined") == 1
                && row.get::<&str, _>("reason") == "TIMEOUT",
            "Timeout control differs",
        )?;
        let a = state
            .activations
            .iter()
            .find(|a| a.id == id)
            .ok_or("Timed out activation absent")?;
        require(
            matches!(&a.result,Some(WorkResultRecord030::Failed {error}) if error.code=="TIMEOUT"),
            "Timeout terminal differs",
        )?;
        let observed = row.get::<&str, _>("observed_ms");
        let mut proven = false;
        for event in events.iter().take(journal.len()) {
            let ticket = parsed(event, "ticket_json")?;
            let envelope: Value =
                serde_json::from_str(ticket["eventJson"].as_str().ok_or("Clock event absent")?)?;
            let output = parsed(event, "output_json")?;
            let receipt: Value = serde_json::from_str(
                output["receiptJson"]
                    .as_str()
                    .ok_or("Clock output absent")?,
            )?;
            if envelope["nowMs"] == observed {
                proven = receipt["expiredWorkIds"]
                    .as_array()
                    .is_some_and(|ids| ids.contains(&json!(id)));
                if proven {
                    break;
                }
            }
        }
        require(proven, "Timeout control has no native clock decision")?;
        projection.timeout_ids.push(id.into());
    }
    Ok(projection)
}
