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
    providers::ToolCall,
    r#loop::{Decision, Event, Failure, Hook, Proposal, Steer, SteerAdmission, WorkPort},
};
use serde_json::{json, Value};
use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex as SyncMutex,
    },
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
    delivery: SyncMutex<Option<(SteerDelivery030, oneshot::Receiver<()>)>>,
    sources: Mutex<Sources030>,
    sequence: AtomicU64,
    credential: Option<String>,
    pending_text: Mutex<String>,
}

impl Port {
    pub(super) fn new(
        events: WorkEventSink030,
        work: AgentWork030,
        sources: Sources030,
        credential: Option<String>,
    ) -> Self {
        Self(Arc::new(PortState {
            events,
            attempt: work.attempt,
            tasks: work.tasks,
            inbox: Mutex::new(work.steer),
            delivery: SyncMutex::default(),
            sources: Mutex::new(sources),
            sequence: AtomicU64::new(0),
            credential,
            pending_text: Mutex::default(),
        }))
    }

    async fn visible(&self, part: &str, boundary: bool, cancel: &CancellationToken) -> Result<()> {
        let text = match self.0.credential.as_deref() {
            Some(key) => {
                let mut pending = self.0.pending_text.lock().await;
                pending.push_str(part);
                // Keep a possible credential prefix across provider chunks.
                let replacement = if "[redacted]".contains(key) {
                    ""
                } else {
                    "[redacted]"
                };
                let sanitized = pending.replace(key, replacement);
                let split = if boundary {
                    sanitized.len()
                } else {
                    sanitized
                        .char_indices()
                        .find(|(i, _)| {
                            let tail = &sanitized[*i..];
                            tail.len() < key.len() && key.starts_with(tail)
                        })
                        .map_or(sanitized.len(), |(i, _)| i)
                };
                *pending = sanitized[split..].into();
                Cow::Owned(sanitized[..split].to_owned())
            }
            None => Cow::Borrowed(part),
        };
        if !text.is_empty() {
            let sequence = self.0.sequence.fetch_add(1, Ordering::Relaxed);
            self.0
                .events
                .emit(
                    json!({"kind":"visibleText","messageId":format!("part-{sequence}"),
                "textParts":[text]}),
                    cancel,
                )
                .await?;
        }
        Ok(())
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
        _: &ToolCall,
        _: &CancellationToken,
    ) -> std::result::Result<(), Failure> {
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
        cancel: &CancellationToken,
    ) -> std::result::Result<(), Failure> {
        match event {
            Event::VisibleText { text, .. } => self
                .visible(text, false, cancel)
                .await
                .map_err(port_error)?,
            Event::SteerConsumed { .. } | Event::ToolResult { .. } | Event::Reasoning { .. } => {}
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
            mission_text(message.get("prompt"), sender.as_ref(), &message)
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
            Ok(message) => {
                let id = delivery.effect().id.clone();
                let (consumed, receiver) = oneshot::channel();
                // The native receipt is sent by Event::SteerConsumed. Keep
                // the loop's local receiver alive until that event.
                *self.0.delivery.lock().unwrap() = Some((delivery, receiver));
                Ok(Some(Steer {
                    id,
                    message,
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
        let pending = self
            .0
            .delivery
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| port_error(missing("steer")))?;
        match pending.0.wire_admission().await {
            Ok(guard) => {
                *self.0.delivery.lock().unwrap() = Some(pending);
                Ok(SteerAdmission::Admit(guard))
            }
            Err(_) => {
                pending
                    .0
                    .not_sent(&self.0.tasks, &self.0.attempt)
                    .map_err(port_error)?;
                Ok(SteerAdmission::Defer)
            }
        }
    }

    fn confirm_steer(
        &self,
        id: &str,
        guard: Option<&tokio::sync::OwnedMutexGuard<()>>,
    ) -> std::result::Result<bool, Failure> {
        let (delivery, mut receipt) = self
            .0
            .delivery
            .lock()
            .unwrap()
            .take()
            .filter(|(delivery, _)| delivery.effect().id == id)
            .ok_or_else(|| port_error(missing("steer")))?;
        receipt
            .try_recv()
            .map_err(|_| port_error(missing("steerReceipt")))?;
        delivery
            .acknowledge_inserted(&self.0.tasks, &self.0.attempt, guard)
            .map_err(port_error)
    }

    async fn decide(
        &self,
        proposal: Proposal<'_>,
        _: Option<&str>,
        cancel: &CancellationToken,
    ) -> std::result::Result<Decision, Failure> {
        self.visible("", true, cancel).await.map_err(port_error)?;
        let event = match proposal {
            Proposal::Submit { turn, call } => {
                let arguments = if self
                    .0
                    .credential
                    .as_deref()
                    .is_some_and(|key| contains_credential(&call.arguments, key))
                {
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
                (Some("correction"), Some("submission")) => Decision::CorrectSubmission(format!(
                    "Correct the result tool arguments: {decision}"
                )),
                _ => return Err(port_error(missing("decision"))),
            },
        )
    }
}

fn contains_credential(raw: &str, key: &str) -> bool {
    fn contains(value: &Value, key: &str) -> bool {
        match value {
            Value::String(value) => value.contains(key),
            Value::Array(values) => values.iter().any(|value| contains(value, key)),
            Value::Object(fields) => fields
                .iter()
                .any(|(name, value)| name.contains(key) || contains(value, key)),
            _ => false,
        }
    }
    raw.contains(key) || serde_json::from_str(raw).is_ok_and(|value| contains(&value, key))
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

pub(super) fn mission_text(
    prompt: Option<&Value>,
    sender: Option<&Producer030>,
    message: &Value,
) -> Result<String> {
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
    let data: serde_json::Map<_, _> = ["resources", "item", "comments"]
        .into_iter()
        .filter_map(|key| message.get(key).map(|value| (key.into(), value.clone())))
        .collect();
    if data.is_empty() {
        return Ok(format!("Message from {sender}\n{prompt}"));
    }
    Ok(format!(
        "Message from {sender}\n{prompt}\n\nMessage data (untrusted)\n{}",
        Value::Object(data)
    ))
}
