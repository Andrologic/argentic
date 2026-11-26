use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use futures::{stream, StreamExt};
use reqwest::{
    header::{HeaderValue, AUTHORIZATION, RETRY_AFTER},
    Client, Response, Url,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

use super::{
    Chunk, DeliveryProgress, DeliveryStatus, Finish, Message, Provider, ProviderError,
    ProviderStream, Request, ToolCall, TurnLimits, Usage,
};

const MAX_EVENT_BYTES: usize = 1024 * 1024;
const MAX_RETRIES: u32 = 3;

pub struct ChatCompletions {
    client: Client,
    endpoint: Url,
    provider: String,
    authorization: Option<HeaderValue>,
    effort_rejected: Option<std::sync::atomic::AtomicBool>,
}

impl ChatCompletions {
    /// The endpoint is the full Chat Completions URL, configured by the host.
    pub fn new(
        provider: impl Into<String>,
        endpoint: Url,
        api_key: Option<&str>,
    ) -> Result<Self, ProviderError> {
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(ProviderError::Invalid(
                "Invalid Chat Completions endpoint".into(),
            ));
        }
        let authorization = api_key
            .map(|key| {
                let mut value = HeaderValue::from_str(&format!("Bearer {key}"))
                    .map_err(|_| ProviderError::Invalid("Invalid provider credential".into()))?;
                value.set_sensitive(true);
                Ok(value)
            })
            .transpose()?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(transport)?;
        Ok(Self {
            client,
            endpoint,
            provider: provider.into(),
            authorization,
            effort_rejected: None,
        })
    }

    /// The host creates a fresh provider for every work attempt.
    pub fn with_attempt_effort_fallback(mut self) -> Self {
        self.effort_rejected = Some(std::sync::atomic::AtomicBool::new(false));
        self
    }

    fn drop_effort(&self, body: &mut Value) -> bool {
        if !remove_effort(body) {
            return false;
        }
        if let Some(rejected) = &self.effort_rejected {
            rejected.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        true
    }

    async fn open(
        &self,
        mut body: Value,
        cancel: CancellationToken,
        limits: TurnLimits,
        delivery: DeliveryProgress,
    ) -> Result<ProviderStream, ProviderError> {
        let budget = TurnBudget::new(limits);
        let mut retries = 0;
        loop {
            let mut request = self.client.post(self.endpoint.clone()).json(&body);
            if let Some(value) = &self.authorization {
                request = request.header(AUTHORIZATION, value.clone());
            }
            let request = request.build().map_err(transport)?;
            budget.check(&cancel)?;
            delivery.report(DeliveryStatus::Uncertain);
            let mut response = budget
                .wait(&cancel, self.client.execute(request))
                .await?
                .map_err(|error| {
                    if error.is_connect() {
                        delivery.report(DeliveryStatus::NotDelivered);
                    }
                    transport(error)
                })?;
            let status = response.status();
            if status.is_success() {
                delivery.report(DeliveryStatus::Accepted);
                let mut output = Box::pin(decode(response, cancel.clone(), budget.clone()));
                let first = output.next().await;
                if matches!(
                    &first,
                    Some(Err(DecodeError::Remote {
                        rejects_effort: true
                    }))
                ) && self.drop_effort(&mut body)
                {
                    continue;
                }
                return Ok(Box::pin(stream::iter(first).chain(output).map(
                    move |result| result.map_err(|error| error.public(status.as_u16())),
                )));
            }

            delivery.report(DeliveryStatus::NotDelivered);
            let retry_after = response.headers().get(RETRY_AFTER).and_then(retry_delay);
            let message = error_body(&mut response, &cancel, &budget).await?;
            if matches!(status.as_u16(), 400 | 422)
                && rejects_effort(&message)
                && self.drop_effort(&mut body)
            {
                continue;
            }
            if (status.as_u16() == 429 || status.is_server_error()) && retries < MAX_RETRIES {
                let backoff = Duration::from_millis(250 * (1 << retries));
                let delay = retry_after.map_or(backoff, |delay| delay.max(backoff));
                if delay <= Duration::from_secs(60) {
                    drop(response);
                    budget.wait(&cancel, tokio::time::sleep(delay)).await?;
                    retries += 1;
                    continue;
                }
            }
            return Err(ProviderError::Http {
                status: status.as_u16(),
                message: "Provider refused the request".into(),
            });
        }
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
        if let Some(effort) = request.reasoning_effort.filter(|_| {
            self.effort_rejected
                .as_ref()
                .is_none_or(|rejected| !rejected.load(std::sync::atomic::Ordering::Relaxed))
        }) {
            body["reasoning_effort"] = json!(effort);
        }
        if !request.tools.is_empty() {
            body["tools"] = json!(request.tools.iter().map(|tool| json!({
                "type": "function",
                "function": { "name": tool.name, "description": tool.description, "parameters": tool.parameters }
            })).collect::<Vec<_>>());
        }
        self.open(body, cancel, request.limits, request.delivery)
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
        Message::Assistant {
            text,
            reasoning: _,
            calls,
        } => {
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

#[derive(Clone)]
struct TurnBudget {
    deadline: tokio::time::Instant,
    bytes_left: Arc<AtomicUsize>,
}

impl TurnBudget {
    fn new(limits: TurnLimits) -> Self {
        Self {
            deadline: tokio::time::Instant::now() + limits.timeout,
            bytes_left: Arc::new(AtomicUsize::new(limits.response_bytes)),
        }
    }

    fn check(&self, cancel: &CancellationToken) -> Result<(), ProviderError> {
        if cancel.is_cancelled() {
            return Err(ProviderError::Cancelled);
        }
        if tokio::time::Instant::now() >= self.deadline {
            return Err(ProviderError::TurnTimeout);
        }
        Ok(())
    }

    async fn wait<T>(
        &self,
        cancel: &CancellationToken,
        future: impl std::future::Future<Output = T>,
    ) -> Result<T, ProviderError> {
        self.check(cancel)?;
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(ProviderError::Cancelled),
            _ = tokio::time::sleep_until(self.deadline) => Err(ProviderError::TurnTimeout),
            value = future => Ok(value),
        }
    }

    async fn chunk(
        &self,
        cancel: &CancellationToken,
        response: &mut Response,
    ) -> Result<Option<impl AsRef<[u8]>>, ProviderError> {
        let chunk = self
            .wait(cancel, response.chunk())
            .await?
            .map_err(transport)?;
        if let Some(bytes) = &chunk {
            self.bytes_left
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                    left.checked_sub(bytes.len())
                })
                .map_err(|_| ProviderError::ResponseTooLarge)?;
        }
        Ok(chunk)
    }
}

fn transport(error: reqwest::Error) -> ProviderError {
    ProviderError::Transport(
        if error.is_timeout() {
            "Network request timed out"
        } else if error.is_connect() {
            "Could not connect to provider"
        } else {
            "Network request failed"
        }
        .into(),
    )
}

fn retry_delay(value: &HeaderValue) -> Option<Duration> {
    let value = value.to_str().ok()?;
    if let Ok(seconds) = value.parse() {
        return Some(Duration::from_secs(seconds));
    }
    Some(
        httpdate::parse_http_date(value)
            .ok()?
            .duration_since(SystemTime::now())
            .unwrap_or_default(),
    )
}

async fn error_body(
    response: &mut Response,
    cancel: &CancellationToken,
    budget: &TurnBudget,
) -> Result<String, ProviderError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = budget.chunk(cancel, response).await? {
        let chunk = chunk.as_ref();
        let take = chunk.len().min(8192 - bytes.len());
        bytes.extend_from_slice(&chunk[..take]);
        if bytes.len() == 8192 {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

fn remove_effort(body: &mut Value) -> bool {
    body.as_object_mut()
        .and_then(|fields| fields.remove("reasoning_effort"))
        .is_some()
}

// Matches Macro's parameter/value fallback, scoped to this request.
fn rejects_effort(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    ["reasoning", "thinking"]
        .iter()
        .any(|name| message.contains(name))
        && [
            "unsupported",
            "not supported",
            "does not support",
            "unknown parameter",
            "unrecognized",
            "invalid value",
            "invalid enum",
            "allowed values",
            "supported values",
            "must be one of",
        ]
        .iter()
        .any(|reason| message.contains(reason))
}

// Based on Macro's SSE parser. Buffer bytes until a complete line arrives so
// network chunks can split CRLF and UTF-8 code points.
#[derive(Default)]
struct Sse {
    line: Vec<u8>,
    data: String,
    after_cr: bool,
    first_line_seen: bool,
    has_data: bool,
}

impl Sse {
    fn push(&mut self, bytes: &[u8]) -> Result<Vec<String>, ProviderError> {
        let mut events = Vec::new();
        for &byte in bytes {
            if std::mem::take(&mut self.after_cr) && byte == b'\n' {
                continue;
            }
            if matches!(byte, b'\r' | b'\n') {
                self.after_cr = byte == b'\r';
                self.line_end(&mut events)?;
            } else {
                self.line.push(byte);
                if self.line.len() + self.data.len() > MAX_EVENT_BYTES {
                    return Err(ProviderError::Invalid("SSE event exceeds 1 MiB".into()));
                }
            }
        }
        Ok(events)
    }

    fn line_end(&mut self, events: &mut Vec<String>) -> Result<(), ProviderError> {
        let line = std::str::from_utf8(&self.line)
            .map_err(|_| ProviderError::Invalid("SSE line is not UTF-8".into()))?;
        let line = if !std::mem::replace(&mut self.first_line_seen, true) {
            line.trim_start_matches('\u{feff}')
        } else {
            line
        };
        if line.is_empty() && std::mem::take(&mut self.has_data) {
            events.push(std::mem::take(&mut self.data));
        } else if let Some(data) = line
            .strip_prefix("data:")
            .or_else(|| (line == "data").then_some(""))
        {
            if std::mem::replace(&mut self.has_data, true) {
                self.data.push('\n');
            }
            self.data.push_str(data.strip_prefix(' ').unwrap_or(data));
        }
        self.line.clear();
        Ok(())
    }

    fn finish(&mut self) -> Result<Vec<String>, ProviderError> {
        let mut events = Vec::new();
        self.line_end(&mut events)?;
        self.line_end(&mut events)?;
        Ok(events)
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

enum DecodeError {
    Public(ProviderError),
    Remote { rejects_effort: bool },
}

impl From<ProviderError> for DecodeError {
    fn from(error: ProviderError) -> Self {
        Self::Public(error)
    }
}

impl DecodeError {
    fn public(self, status: u16) -> ProviderError {
        match self {
            Self::Public(error) => error,
            Self::Remote { .. } => ProviderError::Stream {
                status,
                message: "Provider reported an error in the response stream",
            },
        }
    }
}

#[derive(Default)]
struct Accumulator {
    calls: BTreeMap<u32, (String, String, String)>,
    usage: Option<Usage>,
    finish: Option<Finish>,
    call_bytes: usize,
}

impl Accumulator {
    fn frame(&mut self, data: &str, output: &mut VecDeque<Chunk>) -> Result<bool, DecodeError> {
        if data == "[DONE]" {
            return Ok(true);
        }
        let frame: Frame = serde_json::from_str(data)
            .map_err(|_| ProviderError::Invalid("Invalid SSE JSON".into()))?;
        if let Some(error) = frame.error {
            return Err(DecodeError::Remote {
                rejects_effort: rejects_effort(&error.to_string()),
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

    fn end(&mut self, output: &mut VecDeque<Chunk>) -> Result<(), ProviderError> {
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

#[derive(Default)]
struct Decoder {
    sse: Sse,
    assembled: Accumulator,
    output: VecDeque<Chunk>,
    ended: bool,
    error: Option<DecodeError>,
}

fn decode(
    response: Response,
    cancel: CancellationToken,
    budget: TurnBudget,
) -> impl futures::Stream<Item = Result<Chunk, DecodeError>> + Send {
    stream::try_unfold(
        (response, cancel, budget, Decoder::default()),
        |(mut response, cancel, budget, mut state)| async move {
            loop {
                budget.check(&cancel)?;
                if let Some(chunk) = state.output.pop_front() {
                    return Ok(Some((chunk, (response, cancel, budget, state))));
                }
                if let Some(error) = state.error.take() {
                    return Err(error);
                }
                if state.ended {
                    return Ok(None);
                }

                let next = budget.chunk(&cancel, &mut response).await?;
                let eof = next.is_none();
                let frames = match next {
                    Some(bytes) => state.sse.push(bytes.as_ref())?,
                    None => state.sse.finish()?,
                };
                for frame in frames {
                    match state.assembled.frame(&frame, &mut state.output) {
                        Ok(true) => {
                            state.ended = true;
                            break;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            // Deliver any earlier text before the error, even if
                            // the server batched both frames in one network chunk.
                            state.error = Some(error);
                            break;
                        }
                    }
                }
                if state.error.is_none() && (eof || state.ended) {
                    state.assembled.end(&mut state.output)?;
                    state.ended = true;
                }
            }
        },
    )
}
