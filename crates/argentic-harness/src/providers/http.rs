use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use futures::{stream, StreamExt};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, RETRY_AFTER},
    Client, Response, Url,
};
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::{Chunk, DeliveryStatus, ModelName, ProviderError, ProviderStream, Request, TurnLimits};

pub(super) const MAX_EVENT_BYTES: usize = 1024 * 1024;
const MAX_RETRIES: u32 = 3;

pub(super) fn secret(value: &str) -> Result<HeaderValue, ProviderError> {
    let mut value = HeaderValue::from_str(value)
        .map_err(|_| ProviderError::Invalid("Invalid provider credential".into()))?;
    value.set_sensitive(true);
    Ok(value)
}

pub(super) fn bearer(api_key: Option<&str>) -> Result<Option<HeaderValue>, ProviderError> {
    api_key
        .map(|key| secret(&format!("Bearer {key}")))
        .transpose()
}

pub(super) struct HttpStream {
    client: Client,
    endpoint: Url,
    headers: HeaderMap,
    effort_rejected: Option<AtomicBool>,
}

impl HttpStream {
    pub(super) fn new(
        endpoint: Url,
        credential: Option<HeaderValue>,
        header: HeaderName,
    ) -> Result<Self, ProviderError> {
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(ProviderError::Invalid("Invalid provider endpoint".into()));
        }
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(120))
            .build()
            .map_err(transport)?;
        let mut headers = HeaderMap::new();
        if let Some(value) = credential {
            headers.insert(header, value);
        }
        Ok(Self {
            client,
            endpoint,
            headers,
            effort_rejected: None,
        })
    }

    pub(super) fn header(&mut self, name: &'static str, value: &'static str) {
        self.headers.insert(name, HeaderValue::from_static(value));
    }

    pub(super) fn enable_attempt_effort_fallback(&mut self) {
        self.effort_rejected = Some(AtomicBool::new(false));
    }

    pub(super) fn attempt_effort_rejected(&self) -> bool {
        self.effort_rejected
            .as_ref()
            .is_some_and(|rejected| rejected.load(Ordering::Relaxed))
    }

    fn drop_effort(&self, body: &mut Value, field: &str) -> bool {
        if body
            .as_object_mut()
            .and_then(|fields| fields.remove(field))
            .is_none()
        {
            return false;
        }
        if let Some(rejected) = &self.effort_rejected {
            rejected.store(true, Ordering::Relaxed);
        }
        true
    }

    pub(super) async fn open<E: Events>(
        &self,
        mut body: Value,
        cancel: CancellationToken,
        request: Request<'_>,
        effort_field: &str,
    ) -> Result<ProviderStream, ProviderError> {
        let budget = TurnBudget::new(request.limits);
        if let Some(fields) = body.as_object_mut() {
            if request
                .tool_choice_rejected
                .is_some_and(|level| level.load(Ordering::Relaxed) >= 2)
            {
                fields.remove("tool_choice");
            }
            if self.attempt_effort_rejected() {
                fields.remove(effort_field);
            }
        }
        let mut retries = 0;
        let mut full_tools = None;
        // An earlier request of this attempt saw the named choice refused.
        if request
            .tool_choice_rejected
            .is_some_and(|level| level.load(Ordering::Relaxed) == 1)
        {
            downgrade_tool_choice(&mut body, &mut full_tools);
        }
        let delivery = request.delivery;
        loop {
            let wire = self
                .client
                .post(self.endpoint.clone())
                .headers(self.headers.clone())
                .json(&body)
                .build()
                .map_err(transport)?;
            budget.check(&cancel)?;
            delivery.report(DeliveryStatus::Uncertain);
            let mut response = budget
                .wait(&cancel, self.client.execute(wire))
                .await?
                .map_err(|error| {
                    if error.is_connect() {
                        delivery.report(DeliveryStatus::NotDelivered);
                    }
                    transport(error)
                })?;
            let status = response.status();
            let retry_after = response.headers().get(RETRY_AFTER).and_then(retry_delay);
            let delay = backoff(retries, retry_after);
            if status.is_success() {
                delivery.report(DeliveryStatus::Accepted);
                let mut output = Box::pin(decode::<E>(
                    response,
                    cancel.clone(),
                    budget.clone(),
                    request.model.clone(),
                ));
                let first = output.next().await;
                if let Some(Err(DecodeError::Remote {
                    rejects_effort,
                    retryable,
                })) = &first
                {
                    if *rejects_effort && self.drop_effort(&mut body, effort_field) {
                        continue;
                    }
                    if let Some(delay) = delay.filter(|_| *retryable) {
                        drop(output);
                        budget.wait(&cancel, tokio::time::sleep(delay)).await?;
                        retries += 1;
                        continue;
                    }
                }
                return Ok(Box::pin(stream::iter(first).chain(output).map(
                    move |result| result.map_err(|error| error.public(status.as_u16())),
                )));
            }

            delivery.report(DeliveryStatus::NotDelivered);
            let message = error_body(&mut response, &cancel, &budget).await?;
            if matches!(status.as_u16(), 400 | 422) {
                if rejects_tool_choice(&message) {
                    if let Some(level) = downgrade_tool_choice(&mut body, &mut full_tools) {
                        if let Some(shared) = request.tool_choice_rejected {
                            shared.fetch_max(level, Ordering::Relaxed);
                        }
                        continue;
                    }
                }
                if rejects_effort(&message) && self.drop_effort(&mut body, effort_field) {
                    continue;
                }
            }
            if let Some(delay) =
                delay.filter(|_| status.as_u16() == 429 || status.is_server_error())
            {
                drop(response);
                budget.wait(&cancel, tokio::time::sleep(delay)).await?;
                retries += 1;
                continue;
            }
            return Err(ProviderError::Http {
                status: status.as_u16(),
                message: "Provider refused the request".into(),
            });
        }
    }
}

fn backoff(retries: u32, retry_after: Option<Duration>) -> Option<Duration> {
    if retries >= MAX_RETRIES {
        return None;
    }
    let backoff = Duration::from_millis(250 * (1 << retries));
    let delay = retry_after.map_or(backoff, |delay| delay.max(backoff));
    (delay <= Duration::from_secs(60)).then_some(delay)
}

pub(super) trait Events: Default + Send + 'static {
    fn frame(&mut self, data: &str, output: &mut VecDeque<Chunk>) -> Result<bool, DecodeError>;
    fn end(
        &mut self,
        output: &mut VecDeque<Chunk>,
        terminal: bool,
        model: &ModelName,
    ) -> Result<(), ProviderError>;
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

// Matches Macro's parameter/value fallback, scoped to this request.
pub(super) fn rejects_effort(message: &str) -> bool {
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

/// Replaces a refused forced tool choice and returns the new fallback level.
/// A named choice becomes `required` (`any` for Messages) over that tool
/// alone; any other forced choice is removed and the full tool list restored.
fn downgrade_tool_choice(body: &mut Value, full_tools: &mut Option<Value>) -> Option<u8> {
    let fields = body.as_object_mut()?;
    let choice = fields.get("tool_choice")?;
    let named = choice["name"]
        .as_str()
        .or_else(|| choice["function"]["name"].as_str())
        .map(str::to_owned);
    if let (Some(name), Some(tools)) = (named, fields.get("tools").and_then(Value::as_array)) {
        let messages_api = choice["type"] == "tool";
        let mut only: Vec<Value> = tools
            .iter()
            .filter(|tool| {
                tool["name"]
                    .as_str()
                    .or_else(|| tool["function"]["name"].as_str())
                    == Some(name.as_str())
            })
            .cloned()
            .collect();
        if let Some(last) = only.last_mut() {
            if let (true, Some(marker)) = (
                messages_api,
                tools.last().and_then(|tool| tool.get("cache_control")),
            ) {
                // The Messages encoder places its single breakpoint on the
                // last tool when no system block carries it.
                last["cache_control"] = marker.clone();
            }
            *full_tools = fields.insert("tools".into(), Value::Array(only));
            fields.insert(
                "tool_choice".into(),
                if messages_api {
                    serde_json::json!({"type": "any", "disable_parallel_tool_use": true})
                } else {
                    serde_json::json!("required")
                },
            );
            return Some(1);
        }
    }
    fields.remove("tool_choice")?;
    if let Some(tools) = full_tools.take() {
        fields.insert("tools".into(), tools);
    }
    Some(2)
}

fn rejects_tool_choice(body: &str) -> bool {
    let parsed = serde_json::from_str::<Value>(body).ok();
    let error = parsed
        .as_ref()
        .map(|value| value.get("error").unwrap_or(value));
    if let Some(param) = error.and_then(|error| error["param"].as_str()) {
        return param == "tool_choice";
    }

    let message = error
        .and_then(|error| error.as_str().or_else(|| error["message"].as_str()))
        .unwrap_or(body)
        .to_ascii_lowercase()
        .replace(['\'', '"', '`'], "")
        .replace("tool choice", "tool_choice");
    let Some((before, after)) = message.trim().split_once("tool_choice") else {
        return false;
    };
    if after.starts_with(|c: char| c.is_ascii_alphanumeric() || c == '_') {
        return false;
    }

    // For errors without a parameter, require wording that names the field
    // as the cause. Another field's rejected value can contain "tool_choice".
    matches!(
        before,
        "unsupported "
            | "unsupported parameter "
            | "unknown parameter "
            | "unrecognized parameter "
            | "invalid "
            | "invalid value for "
            | "invalid enum value for "
    ) || before.ends_with(" does not support ")
        || (before.is_empty()
            && ([
                " is not supported",
                " is unsupported",
                " is invalid",
                " must be ",
                " required is not supported",
                " any is not supported",
            ]
            .iter()
            .any(|phrase| after.starts_with(phrase))
                || (after.starts_with(": type ") && after.contains(" not supported"))))
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

pub(super) enum DecodeError {
    Public(ProviderError),
    Remote {
        rejects_effort: bool,
        retryable: bool,
    },
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
struct Decoder<E> {
    sse: Sse,
    assembled: E,
    output: VecDeque<Chunk>,
    ended: bool,
    error: Option<DecodeError>,
}

fn decode<E: Events>(
    response: Response,
    cancel: CancellationToken,
    budget: TurnBudget,
    model: ModelName,
) -> impl futures::Stream<Item = Result<Chunk, DecodeError>> + Send {
    stream::try_unfold(
        (response, cancel, budget, model, Decoder::<E>::default()),
        |(mut response, cancel, budget, model, mut state)| async move {
            loop {
                budget.check(&cancel)?;
                if let Some(chunk) = state.output.pop_front() {
                    return Ok(Some((chunk, (response, cancel, budget, model, state))));
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
                    if let Err(error) = state.assembled.end(&mut state.output, state.ended, &model)
                    {
                        state.error = Some(error.into());
                    }
                    state.ended = true;
                }
            }
        },
    )
}
