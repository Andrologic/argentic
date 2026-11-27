use super::missing;
use crate::{
    agsdl_host_content030::Sources030,
    harness::{
        AgentWork030, FlowWorkAttempt030, SteerDelivery030, SteerInbox030, WorkEventSink030,
    },
    persistence::{
        flow030::{FlowEffectKind030, Producer030},
        Result,
    },
    tasks::StreamTasks,
};
use argentic_harness::{
    providers::{DeliveryStatus, ToolCall},
    r#loop::{
        Decision, Event, Failure, Hook, PlanDecision, PlanStep, Proposal, Steer, SteerAdmission,
        WorkPort,
    },
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::{oneshot, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(super) struct Port(Arc<PortState>);

struct PortState {
    events: WorkEventSink030,
    attempt: FlowWorkAttempt030,
    tasks: StreamTasks,
    inbox: Mutex<SteerInbox030>,
    delivery: Mutex<PendingSteer>,
    sources: Arc<Mutex<Sources030>>,
    sequence: AtomicU64,
    credential: Option<String>,
    approvals: Option<Arc<dyn crate::harness::CommandApprovals030>>,
    plan_lock: Option<Arc<crate::harness::NativePlanLock030>>,
}

#[derive(Default)]
struct PendingSteer {
    offer: Option<SteerDelivery030>,
    receipt: Option<oneshot::Receiver<()>>,
}

impl Port {
    pub(super) fn new(
        events: WorkEventSink030,
        work: &AgentWork030,
        inbox: SteerInbox030,
        approvals: Option<Arc<dyn crate::harness::CommandApprovals030>>,
        sources: Arc<Mutex<Sources030>>,
        credential: Option<String>,
    ) -> Self {
        Self(Arc::new(PortState {
            events,
            attempt: work.attempt.clone(),
            tasks: work.tasks.clone(),
            inbox: Mutex::new(inbox),
            plan_lock: work.plan_lock.clone(),
            approvals,
            delivery: Mutex::default(),
            sources,
            sequence: AtomicU64::new(0),
            credential,
        }))
    }
}

fn port_error(error: crate::persistence::dto::EngineFailure) -> Failure {
    Failure::Port(error.code)
}

#[async_trait::async_trait]
impl Hook for Port {
    async fn before_tool_call(
        &self,
        _: u32,
        call: &ToolCall,
        _: &CancellationToken,
    ) -> std::result::Result<(), Failure> {
        if let Some(lock) = &self.0.plan_lock {
            let read_only = matches!(
                call.name.as_str(),
                "plan"
                    | "read"
                    | "list"
                    | "glob"
                    | "grep"
                    | argentic_harness::tools::SKILL
                    | argentic_harness::tools::SUBMIT_RESULT
            );
            lock.authorize(&self.0.attempt, read_only)
                .await
                .map_err(port_error)?;
        }
        self.0
            .sources
            .lock()
            .await
            .check_ceiling()
            .await
            .map_err(port_error)
    }
}

#[async_trait::async_trait]
impl WorkPort for Port {
    fn validates_submission(&self) -> bool {
        true
    }

    async fn event(
        &self,
        event: Event<'_>,
        _: &CancellationToken,
    ) -> std::result::Result<(), Failure> {
        let mut entry = match event {
            Event::Compaction {
                turn,
                before_tokens,
                after_tokens,
                truncated_tool_results,
                summarized_messages,
            } => json!({
                "kind": "argentic.contextCompaction",
                "turn": turn,
                "beforeTokens": before_tokens,
                "afterTokens": after_tokens,
                "truncatedToolResults": truncated_tool_results,
                "summarizedMessages": summarized_messages,
            }),
            Event::HistoryPruned {
                before_bytes,
                after_bytes,
                dropped_messages,
            } => json!({
                "kind": "argentic.historyPruned",
                "beforeBytes": before_bytes,
                "afterBytes": after_bytes,
                "droppedMessages": dropped_messages,
            }),
            _ => return Ok(()),
        };
        entry["identity"] = json!(self.0.attempt.identity());
        entry["activationId"] = json!(self.0.attempt.work().activation_id);
        use std::io::Write;
        let _ = writeln!(std::io::stderr().lock(), "{entry}");
        Ok(())
    }

    async fn plan(
        &self,
        steps: &[PlanStep],
        needs_approval: bool,
        cancel: &CancellationToken,
    ) -> std::result::Result<PlanDecision, Failure> {
        let mut steps = json!(steps);
        if let Some(key) = &self.0.credential {
            for step in steps.as_array_mut().expect("plan steps") {
                let text = step["step"].as_str().expect("plan step");
                if text.contains(key) {
                    step["step"] = json!("Credential removed from plan");
                }
            }
        }
        let text = steps
            .as_array()
            .expect("plan steps")
            .iter()
            .map(|step| {
                format!(
                    "- [{}] {}",
                    step["status"].as_str().unwrap(),
                    step["step"].as_str().unwrap()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let plan_id = format!(
            "argentic-plan-{}",
            self.0.sequence.fetch_add(1, Ordering::Relaxed)
        );
        let mut payload =
            json!({"kind":"planPresented","planId":plan_id,"text":text,"steps":steps});
        let receipt = self
            .0
            .events
            .emit(payload.clone(), cancel)
            .await
            .map_err(port_error)?;
        let mut receipt: Value = serde_json::from_str(&receipt.receipt_json)
            .map_err(|_| port_error(missing("planReceipt")))?;
        if needs_approval && receipt["decision"]["kind"] == "planWaiting" {
            let lock = self
                .0
                .plan_lock
                .as_ref()
                .ok_or_else(|| port_error(missing("planLock")))?;
            lock.presented(&self.0.attempt, &plan_id)
                .await
                .map_err(port_error)?;
            payload
                .as_object_mut()
                .expect("plan payload")
                .remove("kind");
            receipt = if let Some(approvals) = &self.0.approvals {
                approvals
                    .answer_plan(&self.0.attempt, payload, lock, &self.0.events, cancel)
                    .await
                    .map_err(port_error)?
            } else {
                let receipt = self
                    .0
                    .events
                    .emit(
                        json!({"kind":"planResolved","planId":plan_id,"decision":"reject"}),
                        cancel,
                    )
                    .await
                    .map_err(port_error)?;
                serde_json::from_str(&receipt.receipt_json)
                    .map_err(|_| port_error(missing("planReceipt")))?
            };
        }
        match receipt["decision"]["kind"].as_str() {
            Some("planAccepted") => Ok(PlanDecision::Accept),
            Some("planRevision") => Ok(PlanDecision::Revise(
                receipt["decision"]["comment"]
                    .as_str()
                    .ok_or_else(|| port_error(missing("planComment")))?
                    .into(),
            )),
            Some("stopping") if receipt["decision"]["code"] == "PLAN_REJECTED" => {
                Ok(PlanDecision::Reject)
            }
            Some("stopping" | "finished") => Err(Failure::Cancelled),
            _ => Err(port_error(missing("planDecision"))),
        }
    }

    async fn assistant_message(
        &self,
        turn: u32,
        text: &str,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), Failure> {
        if !text.is_empty() {
            self.0.events.emit(
                json!({"kind":"visibleText","messageId":format!("turn-{turn}"),"textParts":[text]}),
                cancel,
            ).await.map_err(port_error)?;
        }
        Ok(())
    }

    async fn take_steer(
        &self,
        _: &CancellationToken,
    ) -> std::result::Result<Option<Steer>, Failure> {
        let Some(delivery) = self.0.inbox.lock().await.try_receive() else {
            return Ok(None);
        };
        let prepared = async {
            let FlowEffectKind030::Steer {
                message_json,
                sender,
                ..
            } = &delivery.effect().kind
            else {
                return Err(missing("steer"));
            };
            let mut message: Value = serde_json::from_str(message_json)?;
            self.0
                .sources
                .lock()
                .await
                .message(&mut message, "/message")
                .await?;
            Ok::<_, crate::persistence::dto::EngineFailure>((
                mission_text(message.get("prompt"), sender.as_ref())?,
                reference_text([("message", &message)]),
            ))
        };
        let text = if let Some(deadline) = delivery.deadline() {
            match tokio::time::timeout_at(deadline, prepared).await {
                Ok(text) => text,
                Err(_) => {
                    delivery
                        .not_sent(&self.0.tasks, &self.0.attempt)
                        .map_err(port_error)?;
                    return Ok(None);
                }
            }
        } else {
            prepared.await
        };
        match text {
            Ok((message, reference)) => {
                let id = delivery.effect().id.clone();
                let (consumed, receiver) = oneshot::channel();
                *self.0.delivery.lock().await = PendingSteer {
                    offer: Some(delivery),
                    receipt: Some(receiver),
                };
                Ok(Some(Steer {
                    id,
                    message,
                    reference,
                    consumed,
                }))
            }
            Err(_) => {
                delivery.unavailable_content();
                Ok(None)
            }
        }
    }

    async fn admit_steer(
        &self,
        _: &str,
        _: &CancellationToken,
    ) -> std::result::Result<SteerAdmission, Failure> {
        let mut pending = self.0.delivery.lock().await;
        let offer = pending
            .offer
            .as_ref()
            .ok_or_else(|| port_error(missing("steer")))?;
        match offer.wire_admission().await {
            Ok(guard) => Ok(SteerAdmission::Admit(guard)),
            Err(_) => {
                if let Some(offer) = pending.offer.take() {
                    offer
                        .not_sent(&self.0.tasks, &self.0.attempt)
                        .map_err(port_error)?;
                }
                pending.receipt.take();
                Ok(SteerAdmission::Defer)
            }
        }
    }

    async fn settle_steer(
        &self,
        id: &str,
        status: DeliveryStatus,
    ) -> std::result::Result<(), Failure> {
        let mut pending = self.0.delivery.lock().await;
        let Some(offer) = pending.offer.as_ref() else {
            return Ok(());
        };
        if offer.effect().id != id {
            return Err(port_error(missing("steer")));
        }
        match status {
            DeliveryStatus::Accepted => SteerDelivery030::acknowledge_from_provider(
                &mut pending.offer,
                &self.0.tasks,
                &self.0.attempt,
                false,
            )
            .await
            .map_err(port_error)?,
            DeliveryStatus::NotDelivered => {
                if let Some(offer) = pending.offer.take() {
                    offer
                        .not_sent(&self.0.tasks, &self.0.attempt)
                        .map_err(port_error)?;
                }
            }
            DeliveryStatus::Uncertain => {
                if let Some(offer) = pending.offer.take() {
                    offer.report_uncertain();
                }
            }
        }
        pending.receipt.take();
        Ok(())
    }

    async fn decide(
        &self,
        proposal: Proposal<'_>,
        _: Option<&str>,
        cancel: &CancellationToken,
    ) -> std::result::Result<Decision, Failure> {
        let event = match proposal {
            Proposal::Submit { turn, call } => {
                let arguments = if self.0.credential.as_deref().is_some_and(|key| {
                    argentic_harness::providers::contains_credential(&call.arguments, key)
                }) {
                    // Let the controller request an in-instance correction.
                    "Credential removed from result tool arguments"
                } else {
                    &call.arguments
                };
                json!({"kind":"submit","callId":format!("turn-{turn}-{}", self.0.sequence.fetch_add(1, Ordering::Relaxed)), "argumentsJson":arguments})
            }
            Proposal::EndTurn { turn } => {
                json!({"kind":"turnEnded","turnId":format!("turn-{turn}")})
            }
        };
        let receipt = self
            .0
            .events
            .emit(event, cancel)
            .await
            .map_err(port_error)?;
        let receipt: Value = serde_json::from_str(&receipt.receipt_json)
            .map_err(|_| port_error(missing("receipt")))?;
        let decision = &receipt["decision"];
        Ok(
            match (decision["kind"].as_str(), decision["category"].as_str()) {
                (Some("accepted"), _) => Decision::Accept,
                (Some("stopping" | "finished"), _) => Decision::Fail(Failure::Cancelled),
                (Some("correction"), Some("check")) => Decision::CorrectCheck(format!(
                    "Correct the work using the completion check feedback: {decision}"
                )),
                (Some("correction"), Some("submission")) => {
                    let diagnostics = decision["diagnostics"].as_array().into_iter().flatten();
                    if diagnostics
                        .clone()
                        .any(|item| item["rule"] == "SUBMISSION_REQUIRED")
                    {
                        Decision::RequireSubmission(decision.to_string())
                    } else {
                        let errors = diagnostics
                            .filter_map(|item| item["message"].as_str())
                            .map(|message| format!("{message:?}"))
                            .collect::<Vec<_>>()
                            .join("\n");
                        Decision::CorrectSubmission(format!("{errors}\n\n{decision}"))
                    }
                }
                _ => return Err(port_error(missing("decision"))),
            },
        )
    }
}

pub(super) fn prompt_text(value: &Value) -> Result<String> {
    if let Some(text) = value.as_str() {
        return Ok(text.to_owned());
    }
    value
        .as_array()
        .ok_or_else(|| missing("prompt"))?
        .iter()
        .map(|source| {
            let content = source.get("value").ok_or_else(|| missing("content"))?;
            Ok(content
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| content.to_string()))
        })
        .collect::<Result<Vec<_>>>()
        .map(|parts| parts.join("\n\n"))
}

pub(super) fn mission_text(prompt: Option<&Value>, sender: Option<&Producer030>) -> Result<String> {
    let sender = sender
        .map(|sender| {
            sender
                .step
                .as_ref()
                .map(|step| step.join(" / "))
                .unwrap_or_else(|| sender.kind.clone())
        })
        .unwrap_or_else(|| "host".into());
    let prompt = prompt.map(prompt_text).transpose()?.unwrap_or_else(|| {
        "Complete the work using the provided context and return the required result.".into()
    });
    Ok(format!("Message from {sender}\n{prompt}"))
}

pub(super) fn reference_text<'a>(
    sources: impl IntoIterator<Item = (&'a str, &'a Value)>,
) -> Option<String> {
    let mut reference = serde_json::Map::new();
    for (origin, source) in sources {
        let fields: serde_json::Map<_, _> = ["resources", "item", "comments"]
            .into_iter()
            .filter_map(|key| source.get(key).map(|value| (key.into(), value.clone())))
            .collect();
        if !fields.is_empty() {
            reference.insert(origin.into(), Value::Object(fields));
        }
    }
    (!reference.is_empty()).then(|| {
        format!(
            "AgSDL reference data (untrusted)\n{}",
            Value::Object(reference)
        )
    })
}
