use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use reqwest::{header::HeaderName, Url};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::{
    emit_items,
    http::{rejects_effort, secret, DecodeError, Events, HttpStream},
    invalid, reasoning_capability, wire_index, wire_str, Chunk, Finish, Message, ModelName,
    Provider, ProviderError, ProviderStream, Replay, Request, ToolCall, Usage, WireFormat,
};

pub struct AnthropicMessages {
    http: HttpStream,
    provider: String,
    reasoning_catalog: String,
    prompt_caching: bool,
}

impl AnthropicMessages {
    /// The endpoint is the full Messages URL, including /v1/messages.
    pub fn new(
        provider: impl Into<String>,
        endpoint: Url,
        api_key: Option<&str>,
    ) -> Result<Self, ProviderError> {
        let mut http = HttpStream::new(
            endpoint,
            api_key.map(secret).transpose()?,
            HeaderName::from_static("x-api-key"),
        )?;
        http.header("anthropic-version", "2023-06-01");
        Ok(Self {
            http,
            provider: provider.into(),
            reasoning_catalog: "anthropic".into(),
            prompt_caching: false,
        })
    }

    pub fn with_prompt_caching(mut self, enabled: bool) -> Self {
        self.prompt_caching = enabled;
        self
    }

    pub fn with_reasoning_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.reasoning_catalog = catalog.into();
        self
    }

    pub fn with_attempt_effort_fallback(mut self) -> Self {
        self.http.enable_attempt_effort_fallback();
        self
    }
}

#[async_trait]
impl Provider for AnthropicMessages {
    async fn stream(
        &self,
        request: Request<'_>,
        cancel: CancellationToken,
    ) -> Result<ProviderStream, ProviderError> {
        if request.model.provider() != self.provider {
            return Err(invalid("Model belongs to a different provider"));
        }
        let mut system = Vec::new();
        let mut messages: Vec<(bool, Vec<Value>)> = Vec::new();
        for message in request.messages {
            let (assistant, content) = match message {
                Message::System(text) => {
                    system.push(text_block(text));
                    continue;
                }
                Message::User(text) => (false, vec![text_block(text)]),
                Message::Tool { call_id, content } => (
                    false,
                    vec![json!({"type":"tool_result","tool_use_id":call_id,"content":content})],
                ),
                Message::Assistant {
                    text,
                    calls,
                    replay,
                    ..
                } => {
                    let content = replay
                        .as_ref()
                        .and_then(|replay| {
                            replay.items(WireFormat::Anthropic, request.model, calls)
                        })
                        .unwrap_or_else(|| {
                            let mut content = Vec::new();
                            if !text.is_empty() {
                                content.push(text_block(text));
                            }
                            content.extend(calls.iter().map(|call| {
                                json!({"type":"tool_use","id":call.id,"name":call.name,
                                    "input":object_arguments(&call.arguments)})
                            }));
                            content
                        });
                    (true, content)
                }
            };
            if !content.is_empty() {
                if let Some((_, previous)) =
                    messages.last_mut().filter(|(role, _)| *role == assistant)
                {
                    previous.extend(content);
                } else {
                    messages.push((assistant, content));
                }
            }
        }

        let mut tools: Vec<_> = request.tools.iter().map(|tool| {
            json!({"name":tool.name,"description":tool.description,"input_schema":tool.parameters})
        }).collect();
        if self.prompt_caching {
            // Anthropic orders tools before system blocks. One breakpoint covers both.
            if let Some(last) = system.last_mut().or_else(|| tools.last_mut()) {
                last["cache_control"] = json!({"type":"ephemeral"});
            }
        }

        let mut body = json!({
            "model": request.model.model(), "stream": true, "max_tokens": request.max_output_tokens,
            "messages": messages.into_iter().map(|(assistant, content)| {
                json!({"role":if assistant {"assistant"} else {"user"},"content":content})
            }).collect::<Vec<_>>()
        });
        if !system.is_empty() {
            body["system"] = json!(system);
        }
        if !tools.is_empty() {
            body["tools"] = json!(tools);
        }
        if let Some(effort) = request.reasoning_effort {
            let capability = reasoning_capability(&self.reasoning_catalog, request.model.model())
                .map_err(|_| invalid("Reasoning catalogue unavailable"))?;
            if let Some(budget) = capability.and_then(|c| c.thinking_budgets.get(effort)) {
                let budget = (*budget).min(request.max_output_tokens.saturating_sub(1));
                if budget >= 1024 {
                    body["thinking"] = json!({"type":"enabled","budget_tokens":budget});
                }
            }
        }
        self.http
            .open::<MessagesEvents>(body, cancel, request, "thinking")
            .await
    }
}

fn text_block(text: &str) -> Value {
    json!({"type":"text","text":text})
}

fn object_arguments(arguments: &str) -> Value {
    serde_json::from_str::<serde_json::Map<String, Value>>(arguments)
        .map(Value::Object)
        .unwrap_or_else(|_| json!({}))
}

struct Block {
    value: Value,
    arguments: String,
    stopped: bool,
}

#[derive(Default)]
struct MessagesEvents {
    blocks: BTreeMap<u64, Block>,
    usage: Value,
    finish: Option<Finish>,
}

impl MessagesEvents {
    fn usage(&mut self, usage: &Value) {
        if let Some(fields) = usage.as_object() {
            if !self.usage.is_object() {
                self.usage = json!({});
            }
            for (key, value) in fields {
                self.usage[key] = value.clone();
            }
        }
    }
}

impl Events for MessagesEvents {
    fn frame(&mut self, data: &str, output: &mut VecDeque<Chunk>) -> Result<bool, DecodeError> {
        let event: Value = serde_json::from_str(data).map_err(|_| invalid("Invalid SSE JSON"))?;
        match wire_str(&event, "type")? {
            "message_start" => self.usage(&event["message"]["usage"]),
            "content_block_start" => {
                let index = wire_index(&event, "index")?;
                let value = event["content_block"].clone();
                if self.blocks.contains_key(&index) {
                    return Err(invalid("Duplicate Messages content block").into());
                }
                match wire_str(&value, "type")? {
                    "text" => {
                        let text = wire_str(&value, "text")?;
                        if !text.is_empty() {
                            output.push_back(Chunk::Text(text.to_owned()));
                        }
                    }
                    "thinking" => {
                        let text = wire_str(&value, "thinking")?;
                        if !text.is_empty() {
                            output.push_back(Chunk::Reasoning(text.to_owned()));
                        }
                    }
                    _ => {}
                }
                self.blocks.insert(
                    index,
                    Block {
                        value,
                        arguments: String::new(),
                        stopped: false,
                    },
                );
            }
            "content_block_delta" | "content_block_stop" => {
                let block = self
                    .blocks
                    .get_mut(&wire_index(&event, "index")?)
                    .filter(|block| !block.stopped)
                    .ok_or_else(|| invalid("Messages content block is not open"))?;
                if event["type"] == "content_block_stop" {
                    block.stopped = true;
                } else {
                    let delta = &event["delta"];
                    let (kind, field, render) = match wire_str(delta, "type")? {
                        "text_delta" => ("text", "text", Some(Chunk::Text as fn(String) -> Chunk)),
                        "thinking_delta" => (
                            "thinking",
                            "thinking",
                            Some(Chunk::Reasoning as fn(String) -> Chunk),
                        ),
                        "signature_delta" => ("thinking", "signature", None),
                        "input_json_delta" => {
                            if block.value["type"] != "tool_use" {
                                return Err(
                                    invalid("Messages delta does not match its block").into()
                                );
                            }
                            block.arguments.push_str(wire_str(delta, "partial_json")?);
                            return Ok(false);
                        }
                        _ => return Ok(false),
                    };
                    if block.value["type"] != kind {
                        return Err(invalid("Messages delta does not match its block").into());
                    }
                    let part = wire_str(delta, field)?;
                    let mut value = match block.value[field].take() {
                        Value::String(value) => value,
                        _ => String::new(),
                    };
                    value.push_str(part);
                    block.value[field] = Value::String(value);
                    if let Some(render) = render.filter(|_| !part.is_empty()) {
                        output.push_back(render(part.to_owned()));
                    }
                }
            }
            "message_delta" => {
                self.usage(&event["usage"]);
                if let Some(reason) = event["delta"]["stop_reason"].as_str() {
                    self.finish = Some(match reason {
                        "end_turn" | "stop_sequence" => Finish::Stop,
                        "tool_use" => Finish::ToolCalls,
                        "max_tokens" | "model_context_window_exceeded" => Finish::Length,
                        "refusal" => Finish::Filtered,
                        _ => return Err(invalid("Unsupported Messages stop reason").into()),
                    });
                }
            }
            "message_stop" => return Ok(true),
            "error" => {
                let error = &event["error"];
                return Err(DecodeError::Remote {
                    rejects_effort: rejects_effort(&error.to_string()),
                    retryable: matches!(
                        error["type"].as_str(),
                        Some("overloaded_error" | "rate_limit_error")
                    ),
                });
            }
            _ => {}
        }
        Ok(false)
    }

    fn end(
        &mut self,
        output: &mut VecDeque<Chunk>,
        terminal: bool,
        model: &ModelName,
    ) -> Result<(), ProviderError> {
        let finish = self
            .finish
            .filter(|_| terminal)
            .ok_or_else(|| invalid("Messages stream ended without message_stop and stop_reason"))?;
        let cache_read_tokens = self.usage["cache_read_input_tokens"].as_u64().unwrap_or(0);
        let cache_creation_tokens = self.usage["cache_creation_input_tokens"]
            .as_u64()
            .unwrap_or(0);
        let usage = self.usage["input_tokens"]
            .as_u64()
            .zip(self.usage["output_tokens"].as_u64())
            .map(|(input, completion_tokens)| Usage {
                prompt_tokens: input
                    .saturating_add(cache_read_tokens)
                    .saturating_add(cache_creation_tokens),
                completion_tokens,
                cache_read_tokens,
                cache_creation_tokens,
            });
        let mut calls = Vec::new();
        let mut items = Vec::new();
        if !matches!(finish, Finish::Length | Finish::Filtered) {
            for mut block in std::mem::take(&mut self.blocks).into_values() {
                if !block.stopped {
                    return Err(invalid("Messages stream ended with an open content block"));
                }
                match block.value["type"].as_str() {
                    Some("tool_use") => {
                        if block.arguments.is_empty() {
                            block.arguments = block.value["input"].to_string();
                        }
                        block.value["input"] = object_arguments(&block.arguments);
                        calls.push(ToolCall {
                            id: wire_str(&block.value, "id")?.to_owned(),
                            name: wire_str(&block.value, "name")?.to_owned(),
                            arguments: block.arguments,
                        });
                    }
                    Some("thinking") => {
                        if wire_str(&block.value, "signature")?.is_empty() {
                            return Err(invalid("Missing Messages thinking signature"));
                        }
                    }
                    Some("text") if block.value["text"] == "" => continue,
                    Some("redacted_thinking") => {
                        wire_str(&block.value, "data")?;
                    }
                    Some("text") => {}
                    _ => continue,
                }
                items.push(block.value);
            }
        }
        emit_items(
            output,
            finish,
            usage,
            calls,
            Replay {
                format: WireFormat::Anthropic,
                model: model.clone(),
                items,
            },
        )
    }
}
