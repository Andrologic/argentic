//! Provider-independent conversation and streaming response types.

mod chat_completions;
mod reasoning;
pub use chat_completions::ChatCompletions;
pub use reasoning::{reasoning_capability, ReasoningCapability};

use std::{collections::VecDeque, str::FromStr, sync::Mutex};

use async_trait::async_trait;
use futures::{stream, stream::BoxStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    System(String),
    User(String),
    Assistant {
        text: String,
        reasoning: String,
        calls: Vec<ToolCall>,
    },
    Tool {
        call_id: String,
        content: String,
    },
}

pub struct Request<'a> {
    pub model: &'a ModelName,
    pub reasoning_effort: Option<&'a str>,
    pub messages: &'a [Message],
    pub tools: &'a [ToolDefinition],
    pub max_output_tokens: u32,
    pub limits: TurnLimits,
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
        _: Request<'_>,
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
        Ok(Box::pin(stream::iter(turn)))
    }
}
