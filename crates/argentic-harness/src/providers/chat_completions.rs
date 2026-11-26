use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use reqwest::{header::AUTHORIZATION, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::{
    http::{bearer, rejects_effort, DecodeError, Events, HttpStream, MAX_EVENT_BYTES},
    Chunk, Finish, Message, ModelName, Provider, ProviderError, ProviderStream, Request, ToolCall,
    Usage,
};

pub struct ChatCompletions {
    http: HttpStream,
    provider: String,
}

impl ChatCompletions {
    /// The endpoint is the full Chat Completions URL, configured by the host.
    pub fn new(
        provider: impl Into<String>,
        endpoint: Url,
        api_key: Option<&str>,
    ) -> Result<Self, ProviderError> {
        Ok(Self {
            http: HttpStream::new(endpoint, bearer(api_key)?, AUTHORIZATION)?,
            provider: provider.into(),
        })
    }

    /// The host creates a fresh provider for every work attempt.
    pub fn with_attempt_effort_fallback(mut self) -> Self {
        self.http.enable_attempt_effort_fallback();
        self
    }
}

#[async_trait]
impl Provider for ChatCompletions {
    async fn stream(
        &self,
        request: Request<'_>,
        cancel: CancellationToken,
    ) -> Result<ProviderStream, ProviderError> {
        if request.model.provider() != self.provider {
            return Err(ProviderError::Invalid(
                "Model belongs to a different provider".into(),
            ));
        }
        let mut body = json!({
            "model": request.model.model(),
            "messages": request.messages.iter().map(message_json).collect::<Vec<_>>(),
            "max_completion_tokens": request.max_output_tokens,
            "stream": true,
            "stream_options": { "include_usage": true }
        });
        if let Some(effort) = request.reasoning_effort {
            body["reasoning_effort"] = json!(effort);
        }
        if !request.tools.is_empty() {
            body["tools"] = json!(request.tools.iter().map(|tool| json!({
                "type": "function",
                "function": { "name": tool.name, "description": tool.description, "parameters": tool.parameters }
            })).collect::<Vec<_>>());
        }
        self.http
            .open::<Accumulator>(body, cancel, request, "reasoning_effort")
            .await
    }
}

fn message_json(message: &Message) -> Value {
    match message {
        Message::System(text) => json!({"role":"system","content":text}),
        Message::User(text) => json!({"role":"user","content":text}),
        Message::Tool { call_id, content } => {
            json!({"role":"tool","tool_call_id":call_id,"content":content})
        }
        Message::Assistant { text, calls, .. } => {
            let mut message = json!({"role":"assistant","content":text});
            if !calls.is_empty() {
                message["tool_calls"] = json!(calls.iter().map(|call| json!({
                    "id":call.id,"type":"function","function":{"name":call.name,"arguments":call.arguments}
                })).collect::<Vec<_>>());
            }
            message
        }
    }
}

#[derive(Deserialize)]
struct Frame {
    #[serde(default)]
    choices: Vec<Choice>,
    usage: Option<Usage>,
    error: Option<Value>,
    #[serde(default)]
    done: bool,
}

#[derive(Deserialize)]
struct Choice {
    #[serde(default)]
    index: u32,
    delta: Option<Delta>,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<String>,
    #[serde(alias = "reasoning")]
    reasoning_content: Option<String>,
    tool_calls: Option<Vec<CallDelta>>,
}

#[derive(Deserialize)]
struct CallDelta {
    index: u32,
    id: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
    function: Option<FunctionDelta>,
}

#[derive(Deserialize)]
struct FunctionDelta {
    name: Option<String>,
    arguments: Option<String>,
}

#[derive(Default)]
struct Accumulator {
    calls: BTreeMap<u32, (String, String, String)>,
    usage: Option<Usage>,
    finish: Option<Finish>,
    call_bytes: usize,
}

impl Events for Accumulator {
    fn frame(&mut self, data: &str, output: &mut VecDeque<Chunk>) -> Result<bool, DecodeError> {
        if data == "[DONE]" {
            return Ok(true);
        }
        let frame: Frame = serde_json::from_str(data)
            .map_err(|_| ProviderError::Invalid("Invalid SSE JSON".into()))?;
        if let Some(error) = frame.error {
            return Err(DecodeError::Remote {
                rejects_effort: rejects_effort(&error.to_string()),
                retryable: false,
            });
        }
        if let Some(usage) = frame.usage {
            self.usage = Some(usage);
        }
        if let Some(choice) = frame.choices.into_iter().find(|choice| choice.index == 0) {
            if let Some(delta) = choice.delta {
                if let Some(text) = delta.content.filter(|text| !text.is_empty()) {
                    output.push_back(Chunk::Text(text));
                }
                if let Some(reasoning) = delta.reasoning_content.filter(|text| !text.is_empty()) {
                    output.push_back(Chunk::Reasoning(reasoning));
                }
                for call in delta.tool_calls.into_iter().flatten() {
                    if call.index >= 128
                        || call.kind.as_deref().is_some_and(|kind| kind != "function")
                    {
                        return Err(ProviderError::Invalid(
                            "Unsupported tool call index or type".into(),
                        )
                        .into());
                    }
                    let (id, name, arguments) = self.calls.entry(call.index).or_default();
                    if let Some(part) = call.id {
                        id.push_str(&part);
                        self.call_bytes += part.len();
                    }
                    if let Some(function) = call.function {
                        for (part, target) in
                            [(function.name, name), (function.arguments, arguments)]
                        {
                            if let Some(part) = part {
                                self.call_bytes += part.len();
                                target.push_str(&part);
                            }
                        }
                    }
                    if self.call_bytes > MAX_EVENT_BYTES {
                        return Err(
                            ProviderError::Invalid("Tool call batch exceeds 1 MiB".into()).into(),
                        );
                    }
                }
            }
            if let Some(reason) = choice.finish_reason {
                self.finish = Some(match reason.as_str() {
                    "stop" => Finish::Stop,
                    "tool_calls" => Finish::ToolCalls,
                    "length" | "max_output_tokens" => Finish::Length,
                    "content_filter" => Finish::Filtered,
                    _ => return Err(ProviderError::Invalid("Unknown finish reason".into()).into()),
                });
            }
        }
        Ok(frame.done)
    }

    fn end(
        &mut self,
        output: &mut VecDeque<Chunk>,
        _: bool,
        _: &ModelName,
    ) -> Result<(), ProviderError> {
        let finish = self
            .finish
            .ok_or_else(|| ProviderError::Invalid("Stream ended without a finish reason".into()))?;
        let mut ids = std::collections::HashSet::new();
        for (id, name, arguments) in std::mem::take(&mut self.calls).into_values() {
            if matches!(finish, Finish::Length | Finish::Filtered) {
                break;
            }
            if id.is_empty() || name.is_empty() || !ids.insert(id.clone()) {
                return Err(ProviderError::Invalid(
                    "Incomplete or duplicate tool call".into(),
                ));
            }
            output.push_back(Chunk::ToolCall(ToolCall {
                id,
                name,
                arguments,
            }));
        }
        output.push_back(Chunk::End {
            finish,
            usage: self.usage,
        });
        Ok(())
    }
}
