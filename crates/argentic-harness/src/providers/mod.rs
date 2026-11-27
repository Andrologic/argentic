//! Provider-independent conversation and streaming response types.

mod anthropic_messages;
mod chat_completions;
mod http;
mod openai_responses;
mod reasoning;
pub use anthropic_messages::AnthropicMessages;
pub use chat_completions::ChatCompletions;
pub use http::HttpClient;
pub use openai_responses::OpenAiResponses;
pub use reasoning::{reasoning_capability, ReasoningCapability};

use std::{collections::VecDeque, str::FromStr, sync::Mutex};

use async_trait::async_trait;
use futures::{stream, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct ModelName {
    provider: String,
    model: String,
    variant: Option<String>,
}

impl ModelName {
    pub fn provider(&self) -> &str {
        &self.provider
    }
    pub fn model(&self) -> &str {
        &self.model
    }
    pub fn variant(&self) -> Option<&str> {
        self.variant.as_deref()
    }
}

impl FromStr for ModelName {
    type Err = ProviderError;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        let invalid =
            || ProviderError::Invalid("Expected provider/model or provider/model#variant".into());
        let (provider, tail) = name.split_once('/').ok_or_else(invalid)?;
        let (model, variant) = tail
            .split_once('#')
            .map_or((tail, None), |(model, variant)| (model, Some(variant)));
        if provider.is_empty()
            || provider.contains('#')
            || model.split('/').any(str::is_empty)
            || variant.is_some_and(|part| part.is_empty() || part.contains(['#', '/']))
            || name.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(invalid());
        }
        Ok(Self {
            provider: provider.into(),
            model: model.into(),
            variant: variant.map(str::to_owned),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    /// Raw provider arguments. The tool can return a correction for bad JSON.
    pub arguments: String,
}

#[derive(Clone, Debug)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum Message {
    System(String),
    User(String),
    Assistant {
        text: String,
        reasoning: String,
        calls: Vec<ToolCall>,
        replay: Option<Replay>,
    },
    Tool {
        call_id: String,
        content: String,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
enum WireFormat {
    Anthropic,
    Responses,
}

/// Provider-native assistant items reserved for replay with the same model.
/// Kept out of visible events and redacted in diagnostic output.
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Replay {
    format: WireFormat,
    model: ModelName,
    items: Vec<Value>,
}

impl std::fmt::Debug for Replay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Replay([redacted])")
    }
}

impl Replay {
    /// Approximate serialized size, for history budgets and compaction.
    pub(crate) fn byte_len(&self) -> usize {
        self.items.iter().map(|item| item.to_string().len()).sum()
    }

    fn items(
        &self,
        format: WireFormat,
        model: &ModelName,
        calls: &[ToolCall],
    ) -> Option<Vec<Value>> {
        if self.format != format || &self.model != model {
            return None;
        }
        Some(
            self.items
                .iter()
                .filter(|item| {
                    let call_id = match item["type"].as_str() {
                        Some("tool_use") => item["id"].as_str(),
                        Some("function_call") => item["call_id"].as_str(),
                        _ => return true,
                    };
                    // The loop may record a successful submission and skip later calls.
                    calls.iter().any(|call| Some(call.id.as_str()) == call_id)
                })
                .cloned()
                .collect(),
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ToolChoice<'a> {
    #[default]
    Auto,
    Required,
    Named(&'a str),
}

pub struct Request<'a> {
    pub model: &'a ModelName,
    pub reasoning_effort: Option<&'a str>,
    pub messages: &'a [Message],
    pub tools: &'a [ToolDefinition],
    pub tool_choice: ToolChoice<'a>,
    /// Tool choice fallback shared by requests in one work attempt:
    /// 0 none, 1 named choice rejected, 2 every forced choice rejected.
    pub tool_choice_rejected: Option<&'a std::sync::atomic::AtomicU8>,
    pub max_output_tokens: u32,
    pub limits: TurnLimits,
    pub delivery: DeliveryProgress,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeliveryStatus {
    #[default]
    NotDelivered,
    Uncertain,
    Accepted,
}

/// Reports whether the provider received this turn, independently of whether
/// the response body succeeds. Once accepted, later errors cannot undo it.
#[derive(Clone, Default)]
pub struct DeliveryProgress(tokio::sync::watch::Sender<DeliveryStatus>);

impl DeliveryProgress {
    pub fn report(&self, status: DeliveryStatus) {
        self.0.send_if_modified(|current| {
            if *current == DeliveryStatus::Accepted || *current == status {
                return false;
            }
            *current = status;
            true
        });
    }

    pub fn status(&self) -> DeliveryStatus {
        *self.0.borrow()
    }

    pub(crate) async fn accepted(&self) {
        let _ = self
            .0
            .subscribe()
            .wait_for(|status| *status == DeliveryStatus::Accepted)
            .await;
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TurnLimits {
    pub timeout: std::time::Duration,
    pub response_bytes: usize,
}

impl Default for TurnLimits {
    fn default() -> Self {
        Self::from(&crate::config::Settings::default())
    }
}

impl From<&crate::config::Settings> for TurnLimits {
    fn from(settings: &crate::config::Settings) -> Self {
        Self {
            timeout: std::time::Duration::from_millis(u64::from(settings.limit("maxTurnMs"))),
            response_bytes: settings.limit("maxTurnBytes") as usize,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    /// A subset of prompt_tokens.
    #[serde(default)]
    pub cache_read_tokens: u64,
    /// A subset of prompt_tokens.
    #[serde(default)]
    pub cache_creation_tokens: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Finish {
    Stop,
    ToolCalls,
    Length,
    Filtered,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Chunk {
    Text(String),
    Reasoning(String),
    ToolCall(ToolCall),
    Replay(Replay),
    End {
        finish: Finish,
        usage: Option<Usage>,
    },
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ProviderError {
    #[error("Invalid provider request or response: {0}")]
    Invalid(String),
    #[error("Provider transport: {0}")]
    Transport(String),
    #[error("Provider HTTP {status}: {message}")]
    Http { status: u16, message: String },
    #[error("Provider stream HTTP {status}: {message}")]
    Stream { status: u16, message: &'static str },
    #[error("Provider turn deadline exceeded")]
    TurnTimeout,
    #[error("Provider turn response byte budget exceeded")]
    ResponseTooLarge,
    #[error("Provider request cancelled")]
    Cancelled,
}

pub type ProviderStream = BoxStream<'static, Result<Chunk, ProviderError>>;

/// The stream and request future own their IO. Dropping either must shut down
/// local work. Implementations must not spawn detached work.
#[async_trait]
pub trait Provider: Send + Sync {
    fn min_output_tokens(&self) -> u32 {
        1
    }

    async fn stream(
        &self,
        request: Request<'_>,
        cancel: CancellationToken,
    ) -> Result<ProviderStream, ProviderError>;
}

/// One script per request, consumed in order. There is no timing or network IO.
#[derive(Default)]
pub struct ScriptedProvider(Mutex<VecDeque<Vec<Result<Chunk, ProviderError>>>>);

impl ScriptedProvider {
    pub fn new(turns: impl IntoIterator<Item = Vec<Result<Chunk, ProviderError>>>) -> Self {
        Self(Mutex::new(turns.into_iter().collect()))
    }
}

#[async_trait]
impl Provider for ScriptedProvider {
    async fn stream(
        &self,
        request: Request<'_>,
        cancel: CancellationToken,
    ) -> Result<ProviderStream, ProviderError> {
        if cancel.is_cancelled() {
            return Err(ProviderError::Cancelled);
        }
        let turn = self
            .0
            .lock()
            .map_err(|_| ProviderError::Invalid("Script lock poisoned".into()))?
            .pop_front()
            .ok_or_else(|| ProviderError::Invalid("Script exhausted".into()))?;
        request.delivery.report(DeliveryStatus::Accepted);
        Ok(Box::pin(stream::iter(turn)))
    }
}

fn invalid(message: &'static str) -> ProviderError {
    ProviderError::Invalid(message.into())
}

fn wire_str<'a>(value: &'a Value, field: &str) -> Result<&'a str, ProviderError> {
    value[field]
        .as_str()
        .ok_or_else(|| invalid("Invalid provider event field"))
}

fn wire_index(value: &Value, field: &str) -> Result<u64, ProviderError> {
    value[field]
        .as_u64()
        .ok_or_else(|| invalid("Invalid provider event index"))
}

fn emit_items(
    output: &mut VecDeque<Chunk>,
    finish: Finish,
    usage: Option<Usage>,
    calls: Vec<ToolCall>,
    replay: Replay,
) -> Result<(), ProviderError> {
    if !matches!(finish, Finish::Length | Finish::Filtered) {
        let mut ids = std::collections::HashSet::new();
        if calls.len() > 128
            || calls
                .iter()
                .any(|call| call.id.is_empty() || call.name.is_empty() || !ids.insert(&call.id))
        {
            return Err(invalid("Invalid tool call batch"));
        }
        let call_bytes = calls.iter().fold(0usize, |total, call| {
            total
                .saturating_add(call.id.len())
                .saturating_add(call.name.len())
                .saturating_add(call.arguments.len())
        });
        if call_bytes > http::MAX_EVENT_BYTES {
            return Err(invalid("Tool call batch exceeds 1 MiB"));
        }
        output.extend(calls.into_iter().map(Chunk::ToolCall));
        output.push_back(Chunk::Replay(replay));
    }
    output.push_back(Chunk::End { finish, usage });
    Ok(())
}
