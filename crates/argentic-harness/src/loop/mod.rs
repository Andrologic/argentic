//! A fresh AgSDL instance for one work, with one conversation across turns.

use std::{borrow::Cow, collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{stream, StreamExt};
use serde_json::{json, Value};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, OwnedMutexGuard};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    config::{Layer, Settings},
    providers::{
        Chunk, Finish, Message, ModelName, Provider, ProviderError, Request, ToolCall,
        ToolDefinition,
    },
    tools::{Registry, SUBMIT_RESULT},
};

pub struct Work {
    pub model: ModelName,
    pub reasoning_effort: Option<String>,
    pub messages: Vec<Message>,
    pub submission_required: bool,
    pub max_corrections: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Outcome {
    /// Concatenated visible text for this work, in emission order.
    pub text: String,
    pub submission: Option<Value>,
}

#[derive(Debug, Error)]
pub enum Failure {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("Work cancelled")]
    Cancelled,
    #[error("Turn limit reached")]
    TurnLimit,
    #[error("Token limit reached")]
    TokenLimit,
    #[error("Provider omitted usage while a total token limit was active")]
    MissingUsage,
    #[error("Provider filtered the response")]
    Filtered,
    #[error("OUTPUT_INVALID: {0}")]
    OutputInvalid(String),
    #[error("Work port: {0}")]
    Port(String),
}

#[derive(Debug)]
pub struct Artifact {
    pub turn: u32,
    pub call_id: String,
    pub file: NamedTempFile,
}

#[derive(Debug)]
pub struct RunReport {
    pub outcome: Result<Outcome, Failure>,
    pub physical_end: PhysicalEnd,
    pub artifacts: Vec<Artifact>,
    pub messages: Vec<Message>,
}

/// Local proof that the provider stream and all tracked tool workers have
/// stopped. Only the host can turn this into a durable AgSDL join receipt.
#[derive(Debug)]
pub struct PhysicalEnd {
    forced: bool,
}

impl PhysicalEnd {
    pub fn forced(&self) -> bool {
        self.forced
    }
}

/// Queue entries are acknowledged only when appended to the conversation.
/// The sender identifies the origin in the mission itself.
pub struct Steer {
    pub id: String,
    pub message: String,
    pub consumed: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum Event<'a> {
    VisibleText {
        turn: u32,
        text: &'a str,
    },
    Reasoning {
        turn: u32,
        text: &'a str,
    },
    ToolResult {
        turn: u32,
        call: &'a ToolCall,
        content: &'a str,
    },
    SteerConsumed {
        id: &'a str,
    },
}

pub enum Proposal<'a> {
    Submit { turn: u32, call: &'a ToolCall },
    EndTurn { turn: u32 },
}

pub enum Decision {
    Accept,
    CorrectSubmission(String),
    CorrectCheck(String),
    Fail(Failure),
}

/// A host can serialize conversation insertion with its own Stop and
/// completion decisions. The guard is released before events are published.
pub enum SteerAdmission {
    Admit(Option<OwnedMutexGuard<()>>),
    Defer,
}

#[async_trait]
pub trait WorkPort: Send + Sync {
    async fn event(&self, _event: Event<'_>, _cancel: &CancellationToken) -> Result<(), Failure> {
        Ok(())
    }

    /// Pull at most one host delivery at the same safe point as queued steer.
    async fn take_steer(&self, _cancel: &CancellationToken) -> Result<Option<Steer>, Failure> {
        Ok(None)
    }

    /// An embedding controller may validate the exact raw submission itself.
    fn validates_submission(&self) -> bool {
        false
    }

    async fn admit_steer(
        &self,
        _id: &str,
        _cancel: &CancellationToken,
    ) -> Result<SteerAdmission, Failure> {
        Ok(SteerAdmission::Admit(None))
    }

    /// Confirm an in-process insertion while admission still holds its guard.
    /// Returning false removes the message before another turn can use it.
    fn confirm_steer(
        &self,
        _id: &str,
        _guard: Option<&OwnedMutexGuard<()>>,
    ) -> Result<bool, Failure> {
        Ok(true)
    }

    /// The host may add validation or run a completion check. The local
    /// validator must also accept before execution can report success.
    async fn decide(
        &self,
        _proposal: Proposal<'_>,
        validation_error: Option<&str>,
        _cancel: &CancellationToken,
    ) -> Result<Decision, Failure> {
        Ok(validation_error.map_or(Decision::Accept, |message| {
            Decision::CorrectSubmission(message.into())
        }))
    }
}

impl WorkPort for () {}

#[async_trait]
pub trait Hook: Send + Sync {
    async fn prepare_request(
        &self,
        _turn: u32,
        _messages: &mut Cow<'_, [Message]>,
        _cancel: &CancellationToken,
    ) -> Result<(), Failure> {
        Ok(())
    }

    async fn before_tool_call(
        &self,
        _turn: u32,
        _call: &ToolCall,
        _cancel: &CancellationToken,
    ) -> Result<(), Failure> {
        Ok(())
    }

    async fn after_tool_call(
        &self,
        _turn: u32,
        _call: &ToolCall,
        _content: &str,
        _cancel: &CancellationToken,
    ) -> Result<(), Failure> {
        Ok(())
    }

    async fn finish_turn(&self, _turn: u32, _cancel: &CancellationToken) -> Result<(), Failure> {
        Ok(())
    }
}

/// Extensions borrow the work's lifetime. Future drops must release their IO
/// and never leave detached work. The registry separately tracks blocking IO.
pub struct Harness {
    provider: Arc<dyn Provider>,
    registry: Arc<Registry>,
    settings: Settings,
    hooks: Vec<Arc<dyn Hook>>,
}

impl Harness {
    pub fn new(provider: Arc<dyn Provider>, registry: Arc<Registry>, settings: Settings) -> Self {
        Self {
            provider,
            registry,
            settings,
            hooks: Vec::new(),
        }
    }

    pub fn with_hook(mut self, hook: Arc<dyn Hook>) -> Self {
        self.hooks.push(hook);
        self
    }

    pub async fn run(
        &self,
        mut work: Work,
        port: &dyn WorkPort,
        steer: mpsc::Receiver<Steer>,
        parent: CancellationToken,
    ) -> RunReport {
        let cancel = parent.child_token();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let tracker = TaskTracker::new();
        let mut instance = Instance {
            harness: self,
            port,
            steer,
            cancel: &cancel,
            tracker: &tracker,
            text: String::new(),
            artifacts: Vec::new(),
            corrections: 0,
            history_end: work.messages.len(),
        };
        let (outcome, forced) = {
            let future = instance.execute(&mut work);
            tokio::pin!(future);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let grace = Duration::from_millis(u64::from(self.settings.limit("cancelGraceMs")));
                    let forced = tokio::time::timeout(grace, &mut future).await.is_err();
                    (Err(Failure::Cancelled), forced)
                }
                outcome = &mut future => (outcome, false),
            }
        };
        let outcome = if cancel.is_cancelled() {
            Err(Failure::Cancelled)
        } else {
            outcome
        };
        cancel.cancel();
        // Closing drops any undelivered steer receipt before joining workers.
        instance.steer.close();
        while instance.steer.try_recv().is_ok() {}
        tracker.close();
        tracker.wait().await;
        work.messages.truncate(instance.history_end);
        RunReport {
            outcome,
            physical_end: PhysicalEnd { forced },
            artifacts: instance.artifacts,
            messages: work.messages,
        }
    }
}

struct Instance<'a> {
    harness: &'a Harness,
    port: &'a dyn WorkPort,
    steer: mpsc::Receiver<Steer>,
    cancel: &'a CancellationToken,
    tracker: &'a TaskTracker,
    text: String,
    artifacts: Vec<Artifact>,
    corrections: u32,
    history_end: usize,
}

impl Instance<'_> {
    async fn execute(&mut self, work: &mut Work) -> Result<Outcome, Failure> {
        let settings = &self.harness.settings;
        let turn_limit = settings.limit("maxTurns");
        let total = settings
            .get("maxTotalTokens")
            .filter(|(_, layer)| *layer != Layer::Default)
            .map(|(limit, _)| u64::from(limit));
        let mut spent = 0u64;
        let definitions: Vec<_> = self
            .harness
            .registry
            .definitions()
            .map(|(_, def)| def.clone())
            .collect();
        for turn in 1..=turn_limit {
            self.active()?;
            let remaining = total.map_or(u64::MAX, |limit| limit.saturating_sub(spent));
            if remaining == 0 {
                return Err(Failure::TokenLimit);
            }
            let max_output = u64::from(settings.limit("maxOutputTokens")).min(remaining) as u32;
            let response = self.request(turn, work, &definitions, max_output).await?;
            if let Some(usage) = response.usage {
                spent = spent
                    .saturating_add(usage.prompt_tokens)
                    .saturating_add(usage.completion_tokens);
                if total.is_some_and(|limit| spent > limit)
                    || usage.completion_tokens > u64::from(max_output)
                {
                    return Err(Failure::TokenLimit);
                }
            } else if total.is_some() {
                return Err(Failure::MissingUsage);
            }
            let finish = response.finish;
            let calls = response.calls;
            work.messages.push(Message::Assistant {
                text: response.text,
                reasoning: response.reasoning,
                calls: calls.clone(),
            });

            let response_index = work.messages.len() - 1;
            let mut pending = calls.as_slice();
            let mut accepted = None;
            while !pending.is_empty() {
                let read_count = pending
                    .iter()
                    .take_while(|call| call.name != SUBMIT_RESULT)
                    .count();
                let (reads, tail) = pending.split_at(read_count);
                self.read_batch(turn, reads, &mut work.messages).await?;
                let Some((call, tail)) = tail.split_first() else {
                    break;
                };
                self.before(turn, call).await?;
                let validation = if self.port.validates_submission() {
                    serde_json::from_str(&call.arguments).map_err(|e| e.to_string())
                } else {
                    self.harness.registry.validate_submission(&call.arguments)
                };
                let decision = self
                    .decide(
                        Proposal::Submit { turn, call },
                        validation.as_ref().err().map(String::as_str),
                    )
                    .await?;
                match self.apply(decision, work.max_corrections)? {
                    None => {
                        accepted = Some(validation.map_err(Failure::OutputInvalid)?);
                        self.publish_tool(turn, call, "Result accepted.", &mut work.messages)
                            .await?;
                        let Message::Assistant {
                            calls: reported, ..
                        } = &mut work.messages[response_index]
                        else {
                            unreachable!("response message is an assistant message");
                        };
                        reported.truncate(calls.len() - tail.len());
                        break;
                    }
                    Some(error) => {
                        self.publish_tool(turn, call, &error, &mut work.messages)
                            .await?;
                    }
                }
                pending = tail;
            }
            self.history_end = work.messages.len();
            // With no outstanding tool messages, the next provider request
            // can include steering without breaking tool-call pairing.
            for hook in &self.harness.hooks {
                hook.finish_turn(turn, self.cancel).await?;
            }
            self.active()?;
            if let Some(submission) = accepted {
                return Ok(Outcome {
                    text: std::mem::take(&mut self.text),
                    submission: Some(submission),
                });
            }
            let can_continue = turn < turn_limit && total.is_none_or(|limit| spent < limit);
            if can_continue && self.consume_steer(&mut work.messages).await? {
                continue;
            }
            if calls.is_empty() && finish == Finish::Stop {
                // An ending work leaves any later arrivals unacknowledged.
                let validation = work
                    .submission_required
                    .then_some("Submit the required result before completing this work.");
                let decision = self.decide(Proposal::EndTurn { turn }, validation).await?;
                match self.apply(decision, work.max_corrections)? {
                    None if validation.is_none() => {
                        return Ok(Outcome {
                            text: std::mem::take(&mut self.text),
                            submission: None,
                        });
                    }
                    None => {
                        return Err(Failure::OutputInvalid(
                            "Required submission is missing".into(),
                        ))
                    }
                    Some(error) => work.messages.push(Message::User(error)),
                }
            }
        }
        Err(Failure::TurnLimit)
    }

    async fn request(
        &mut self,
        turn: u32,
        work: &Work,
        definitions: &[ToolDefinition],
        max_output_tokens: u32,
    ) -> Result<Response, Failure> {
        let mut messages = Cow::Borrowed(work.messages.as_slice());
        for hook in &self.harness.hooks {
            hook.prepare_request(turn, &mut messages, self.cancel)
                .await?;
        }
        self.active()?;
        let mut stream = self
            .harness
            .provider
            .stream(
                Request {
                    model: &work.model,
                    reasoning_effort: work.reasoning_effort.as_deref(),
                    messages: &messages,
                    tools: definitions,
                    max_output_tokens,
                    limits: (&self.harness.settings).into(),
                },
                self.cancel.child_token(),
            )
            .await?;
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Vec::new();
        let mut ids = HashSet::new();
        let mut bytes = 0usize;
        while let Some(chunk) = stream.next().await {
            self.active()?;
            match chunk? {
                Chunk::Text(part) => {
                    account_bytes(&mut bytes, part.len())?;
                    self.port
                        .event(Event::VisibleText { turn, text: &part }, self.cancel)
                        .await?;
                    text.push_str(&part);
                    self.text.push_str(&part);
                }
                Chunk::Reasoning(part) => {
                    account_bytes(&mut bytes, part.len())?;
                    self.port
                        .event(Event::Reasoning { turn, text: &part }, self.cancel)
                        .await?;
                    reasoning.push_str(&part);
                }
                Chunk::ToolCall(call) => {
                    account_bytes(
                        &mut bytes,
                        call.arguments
                            .len()
                            .saturating_add(call.id.len())
                            .saturating_add(call.name.len()),
                    )?;
                    if calls.len() >= 128
                        || call.id.is_empty()
                        || call.name.is_empty()
                        || !ids.insert(call.id.clone())
                    {
                        return Err(ProviderError::Invalid("Invalid tool call batch".into()).into());
                    }
                    calls.push(call);
                }
                Chunk::End { finish, usage } => match finish {
                    Finish::Length => return Err(Failure::TokenLimit),
                    Finish::Filtered => return Err(Failure::Filtered),
                    Finish::ToolCalls if calls.is_empty() => {
                        return Err(ProviderError::Invalid(
                            "Tool call finish without a tool call".into(),
                        )
                        .into())
                    }
                    _ => {
                        return Ok(Response {
                            text,
                            reasoning,
                            calls,
                            finish,
                            usage,
                        })
                    }
                },
            }
        }
        Err(ProviderError::Invalid("Provider ended without a terminal chunk".into()).into())
    }

    async fn read_batch(
        &mut self,
        turn: u32,
        calls: &[ToolCall],
        messages: &mut Vec<Message>,
    ) -> Result<(), Failure> {
        let parallelism = self.harness.settings.limit("maxParallelReads") as usize;
        for batch in calls.chunks(parallelism) {
            for call in batch {
                self.before(turn, call).await?;
            }
            let outputs = stream::iter(batch.iter().cloned())
                .map(|call| {
                    self.harness
                        .registry
                        .execute(call, self.cancel.child_token(), self.tracker)
                })
                .buffered(batch.len())
                .collect::<Vec<_>>()
                .await;
            self.active()?;
            for (call, output) in batch.iter().zip(outputs) {
                let content = match output {
                    Ok(mut page) => {
                        let encoded = serde_json::to_string(&page)
                            .map_err(|e| Failure::Port(e.to_string()))?;
                        if let Some(file) = page.overflow.take() {
                            self.artifacts.push(Artifact {
                                turn,
                                call_id: call.id.clone(),
                                file,
                            });
                        }
                        encoded
                    }
                    Err(error) => json!({"error":error}).to_string(),
                };
                self.publish_tool(turn, call, &content, messages).await?;
            }
        }
        Ok(())
    }

    async fn before(&self, turn: u32, call: &ToolCall) -> Result<(), Failure> {
        self.active()?;
        for hook in &self.harness.hooks {
            hook.before_tool_call(turn, call, self.cancel).await?;
        }
        self.active()
    }

    async fn publish_tool(
        &self,
        turn: u32,
        call: &ToolCall,
        content: &str,
        messages: &mut Vec<Message>,
    ) -> Result<(), Failure> {
        for hook in &self.harness.hooks {
            hook.after_tool_call(turn, call, content, self.cancel)
                .await?;
        }
        self.active()?;
        self.port
            .event(
                Event::ToolResult {
                    turn,
                    call,
                    content,
                },
                self.cancel,
            )
            .await?;
        messages.push(Message::Tool {
            call_id: call.id.clone(),
            content: content.into(),
        });
        Ok(())
    }

    async fn decide(
        &self,
        proposal: Proposal<'_>,
        error: Option<&str>,
    ) -> Result<Decision, Failure> {
        self.active()?;
        let decision = self.port.decide(proposal, error, self.cancel).await?;
        self.active()?;
        Ok(decision)
    }

    fn apply(&mut self, decision: Decision, limit: u32) -> Result<Option<String>, Failure> {
        match decision {
            Decision::Accept => Ok(None),
            Decision::CorrectCheck(message) => Ok(Some(message)),
            Decision::CorrectSubmission(message) => {
                if self.corrections >= limit {
                    return Err(Failure::OutputInvalid(message));
                }
                self.corrections += 1;
                Ok(Some(message))
            }
            Decision::Fail(failure) => Err(failure),
        }
    }

    async fn consume_steer(&mut self, messages: &mut Vec<Message>) -> Result<bool, Failure> {
        let mut consumed = false;
        // Take a finite snapshot. Continuous arrivals cannot hold a turn open.
        for _ in 0..self.steer.len() {
            self.active()?;
            let Ok(steer) = self.steer.try_recv() else {
                break;
            };
            consumed |= self.insert_steer(messages, steer).await?;
        }
        if let Some(steer) = self.port.take_steer(self.cancel).await? {
            consumed |= self.insert_steer(messages, steer).await?;
        }
        Ok(consumed)
    }

    async fn insert_steer(
        &mut self,
        messages: &mut Vec<Message>,
        steer: Steer,
    ) -> Result<bool, Failure> {
        if steer.consumed.is_closed() {
            return Ok(false);
        }
        let SteerAdmission::Admit(guard) = self.port.admit_steer(&steer.id, self.cancel).await?
        else {
            return Ok(false);
        };
        self.active()?;
        messages.push(Message::User(steer.message));
        if steer.consumed.send(()).is_err() {
            messages.pop();
            return Ok(false);
        }
        if !self.port.confirm_steer(&steer.id, guard.as_ref())? {
            messages.pop();
            return Ok(false);
        }
        self.history_end = messages.len();
        drop(guard);
        self.port
            .event(Event::SteerConsumed { id: &steer.id }, self.cancel)
            .await?;
        Ok(true)
    }

    fn active(&self) -> Result<(), Failure> {
        if self.cancel.is_cancelled() {
            Err(Failure::Cancelled)
        } else {
            Ok(())
        }
    }
}

struct Response {
    text: String,
    reasoning: String,
    calls: Vec<ToolCall>,
    finish: Finish,
    usage: Option<crate::providers::Usage>,
}

fn account_bytes(bytes: &mut usize, additional: usize) -> Result<(), Failure> {
    *bytes = bytes.saturating_add(additional);
    if *bytes > 8 * 1024 * 1024 {
        return Err(ProviderError::Invalid("Turn response exceeds 8 MiB".into()).into());
    }
    Ok(())
}
