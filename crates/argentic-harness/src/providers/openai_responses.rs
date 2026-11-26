use std::collections::{BTreeMap, VecDeque};

use async_trait::async_trait;
use reqwest::{header::AUTHORIZATION, Url};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::{
    emit_items,
    http::{bearer, rejects_effort, DecodeError, Events, HttpStream},
    invalid, wire_index, wire_str, Chunk, Finish, Message, ModelName, Provider, ProviderError,
    ProviderStream, Replay, Request, ToolCall, Usage, WireFormat,
};

pub struct OpenAiResponses {
    http: HttpStream,
    provider: String,
}

impl OpenAiResponses {
    pub const MIN_OUTPUT_TOKENS: u32 = 16;

    /// The endpoint is the full Responses URL, including /responses.
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

    pub fn with_attempt_effort_fallback(mut self) -> Self {
        self.http.enable_attempt_effort_fallback();
        self
    }
}

#[async_trait]
impl Provider for OpenAiResponses {
    fn min_output_tokens(&self) -> u32 {
        Self::MIN_OUTPUT_TOKENS
    }

    async fn stream(
        &self,
        request: Request<'_>,
        cancel: CancellationToken,
    ) -> Result<ProviderStream, ProviderError> {
        if request.model.provider() != self.provider {
            return Err(invalid("Model belongs to a different provider"));
        }
        if request.max_output_tokens < Self::MIN_OUTPUT_TOKENS {
            return Err(invalid(
                "Responses requires max_output_tokens of at least 16",
            ));
        }
        let mut input = Vec::new();
        for message in request.messages {
            match message {
                Message::System(text) => input.push(json!({"role":"system","content":text})),
                Message::User(text) => input.push(json!({"role":"user","content":text})),
                Message::Tool { call_id, content } => input.push(json!({
                    "type":"function_call_output","call_id":call_id,"output":content
                })),
                Message::Assistant {
                    text,
                    calls,
                    replay,
                    ..
                } => {
                    if let Some(items) = replay.as_ref().and_then(|replay| {
                        replay.items(WireFormat::Responses, request.model, calls)
                    }) {
                        input.extend(items);
                    } else {
                        if !text.is_empty() {
                            input.push(json!({"role":"assistant","content":text}));
                        }
                        input.extend(calls.iter().map(|call| {
                            json!({
                                "type":"function_call","call_id":call.id,"name":call.name,
                                "arguments":call.arguments
                            })
                        }));
                    }
                }
            }
        }
        let mut body = json!({
            "model":request.model.model(),"input":input,"stream":true,
            "store":false,"include":["reasoning.encrypted_content"],
            "max_output_tokens":request.max_output_tokens
        });
        if !request.tools.is_empty() {
            body["tools"] = json!(request
                .tools
                .iter()
                .map(|tool| json!({
                    "type":"function","name":tool.name,"description":tool.description,
                    "parameters":tool.parameters,"strict":false
                }))
                .collect::<Vec<_>>());
        }
        if let Some(effort) = request.reasoning_effort {
            body["reasoning"] = json!({"effort":effort});
        }
        self.http
            .open::<ResponsesEvents>(body, cancel, request, "reasoning")
            .await
    }
}

#[derive(Default)]
struct Item {
    value: Value,
    arguments: String,
    done: bool,
}

#[derive(Default)]
struct ResponsesEvents {
    items: BTreeMap<u64, Item>,
    usage: Option<Usage>,
    finish: Option<Finish>,
}

impl Events for ResponsesEvents {
    fn frame(&mut self, data: &str, output: &mut VecDeque<Chunk>) -> Result<bool, DecodeError> {
        let mut event: Value =
            serde_json::from_str(data).map_err(|_| invalid("Invalid SSE JSON"))?;
        match wire_str(&event, "type")? {
            "response.output_item.added" => {
                let index = wire_index(&event, "output_index")?;
                if self.items.contains_key(&index) {
                    return Err(invalid("Duplicate Responses output item").into());
                }
                let value = event["item"].take();
                let arguments = value["arguments"].as_str().unwrap_or_default().to_owned();
                self.items.insert(
                    index,
                    Item {
                        value,
                        arguments,
                        done: false,
                    },
                );
            }
            "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
                let item = self
                    .items
                    .get_mut(&wire_index(&event, "output_index")?)
                    .filter(|item| !item.done && item.value["type"] == "function_call")
                    .ok_or_else(|| invalid("Responses function call item is not open"))?;
                if event["type"] == "response.function_call_arguments.delta" {
                    item.arguments.push_str(wire_str(&event, "delta")?);
                } else {
                    item.arguments = wire_str(&event, "arguments")?.to_owned();
                }
            }
            "response.output_item.done" => {
                let item = self
                    .items
                    .entry(wire_index(&event, "output_index")?)
                    .or_default();
                if item.done {
                    return Err(invalid("Duplicate Responses output item completion").into());
                }
                let mut value = event["item"].take();
                if value["type"] == "function_call" && value["arguments"].is_null() {
                    value["arguments"] = Value::String(std::mem::take(&mut item.arguments));
                }
                item.value = value;
                item.done = true;
            }
            "response.output_text.delta" | "response.refusal.delta" => {
                output.push_back(Chunk::Text(wire_str(&event, "delta")?.to_owned()));
            }
            "response.reasoning_summary_text.delta" => {
                output.push_back(Chunk::Reasoning(wire_str(&event, "delta")?.to_owned()));
            }
            "response.completed" | "response.incomplete" => {
                let response = &event["response"];
                if !response.is_object() {
                    return Err(invalid("Missing terminal Responses object").into());
                }
                self.usage = parse_usage(&response["usage"]);
                self.finish = Some(if event["type"] == "response.completed" {
                    Finish::Stop
                } else {
                    match response["incomplete_details"]["reason"].as_str() {
                        Some("max_output_tokens") => Finish::Length,
                        Some("content_filter") => Finish::Filtered,
                        _ => return Err(invalid("Unknown Responses incomplete reason").into()),
                    }
                });
                return Ok(true);
            }
            "response.failed" | "error" => {
                let error = if event["type"] == "response.failed" {
                    &event["response"]["error"]
                } else if event["error"].is_object() {
                    &event["error"]
                } else {
                    &event
                };
                return Err(DecodeError::Remote {
                    rejects_effort: rejects_effort(&error.to_string()),
                    retryable: matches!(
                        error["code"].as_str(),
                        Some("rate_limit_exceeded" | "server_error")
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
        let mut finish = self
            .finish
            .filter(|_| terminal)
            .ok_or_else(|| invalid("Responses stream ended without a terminal event"))?;
        let mut calls = Vec::new();
        let mut items = Vec::new();
        if finish == Finish::Stop {
            if self.items.values().any(|item| !item.done) {
                return Err(invalid("Responses stream ended with an open output item"));
            }
            for item in std::mem::take(&mut self.items).into_values() {
                match wire_str(&item.value, "type")? {
                    "function_call" => calls.push(ToolCall {
                        id: wire_str(&item.value, "call_id")?.to_owned(),
                        name: wire_str(&item.value, "name")?.to_owned(),
                        arguments: wire_str(&item.value, "arguments")?.to_owned(),
                    }),
                    "message" => {}
                    "reasoning"
                        if item.value["encrypted_content"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty()) => {}
                    _ => continue,
                }
                items.push(item.value);
            }
            if !calls.is_empty() {
                finish = Finish::ToolCalls;
            }
        }
        emit_items(
            output,
            finish,
            self.usage,
            calls,
            Replay {
                format: WireFormat::Responses,
                model: model.clone(),
                items,
            },
        )
    }
}

fn parse_usage(usage: &Value) -> Option<Usage> {
    usage["input_tokens"]
        .as_u64()
        .zip(usage["output_tokens"].as_u64())
        .map(|(prompt_tokens, completion_tokens)| Usage {
            prompt_tokens,
            completion_tokens,
            cache_read_tokens: usage["input_tokens_details"]["cached_tokens"]
                .as_u64()
                .unwrap_or(0),
            cache_creation_tokens: 0,
        })
}
