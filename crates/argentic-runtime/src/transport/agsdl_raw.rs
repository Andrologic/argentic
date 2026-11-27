//! Dedicated captured-server transport for AgSDL030. Existing UI clients keep
//! their formatting, connection selection and timeout behavior unchanged.
//! Raw JSON tokens never pass through a floating-point representation.
pub(crate) const MAX_MCP_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(10);
const TOOL_HANDOFF_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const TOOL_BUDGET_EXPIRED: &str = "MCP_TOOL_BUDGET_EXPIRED";
const HTTP_SESSION_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);

use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    time::Duration,
};

use serde::Serialize;
use serde_json::value::RawValue;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::oneshot,
    time::Instant,
};

use crate::agsdl_host_calls::{
    CallError, CallIdentity, CommandGrant, Control, HostPort, HumanReply, PhysicalEvidence, Result,
};
use crate::process::ContainedBackgroundProcess;

#[derive(Clone, Copy)]
pub(crate) enum Protocol {
    Legacy,
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "Native capture currently selects legacy MCP; transport tests exercise modern MCP"
        )
    )]
    Modern,
}

/// Resolved native capture. Callers must authorize every frozen IP, including
/// private/loopback endpoints, before constructing this value. No proxy, redirect,
/// DNS rebinding, reconnect, automatic retry or global UI lookup occurs here.
pub(crate) struct HttpServer {
    pub url: url::Url,
    pub addresses: Vec<SocketAddr>,
    pub headers: reqwest::header::HeaderMap,
    pub secret_values: Vec<String>,
    /// Captured tools/list schema, bound by the native host to this exact tool.
    pub tool_schema: Option<(String, String)>,
}
pub(crate) struct StdioServer {
    pub program: PathBuf,
    pub arguments: Vec<String>,
    pub grant: CommandGrant,
    pub secret_values: Vec<String>,
}
pub(crate) enum ServerTransport {
    Stdio(StdioServer),
    Http(HttpServer),
}
pub(crate) struct CapturedServer {
    pub document_name: String,
    pub capture_digest: String,
    pub protocol: Protocol,
    pub transport: ServerTransport,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct McpReceipt {
    pub is_error: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content_json: Option<String>,
    pub text: String,
    pub raw_result_json: String,
}

/// Reject ambiguous envelopes; preserve each value's original JSON bytes.
fn object(json: &str) -> Result<BTreeMap<String, Box<RawValue>>> {
    struct ObjectVisitor;
    impl<'de> serde::de::Visitor<'de> for ObjectVisitor {
        type Value = BTreeMap<String, Box<RawValue>>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an object without duplicate keys")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                if result.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate JSON key"));
                }
            }
            Ok(result)
        }
    }
    let mut parser = serde_json::Deserializer::from_str(json);
    let value = serde::de::Deserializer::deserialize_map(&mut parser, ObjectVisitor)
        .map_err(|e| CallError::transport(format!("Invalid MCP object: {e}")))?;
    parser
        .end()
        .map_err(|e| CallError::transport(e.to_string()))?;
    Ok(value)
}
fn string(value: &RawValue) -> Result<String> {
    serde_json::from_str(value.get())
        .map_err(|e| CallError::transport(format!("Invalid MCP string: {e}")))
}
fn encode<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|e| CallError::transport(e.to_string()))
}

pub(crate) fn receipt(raw: String) -> Result<McpReceipt> {
    let fields = object(&raw)?;
    let is_error = fields
        .get("isError")
        .map(|v| serde_json::from_str::<bool>(v.get()))
        .transpose()
        .map_err(|_| CallError::transport("MCP isError is not a boolean"))?
        .unwrap_or(false);
    let structured_content_json = fields.get("structuredContent").map(|v| v.get().to_owned());
    let mut texts = Vec::new();
    if let Some(content) = fields.get("content") {
        let blocks: Vec<Box<RawValue>> = serde_json::from_str(content.get())
            .map_err(|e| CallError::transport(format!("Invalid MCP content: {e}")))?;
        for block in blocks {
            let item = object(block.get())?;
            if item.get("type").map(|v| string(v)).transpose()?.as_deref() == Some("text") {
                texts.push(string(
                    item.get("text")
                        .ok_or_else(|| CallError::transport("Missing MCP text"))?,
                )?);
            }
        }
    }
    if !fields.contains_key("content") && structured_content_json.is_none() {
        return Err(CallError::transport(
            "MCP result has neither content nor structuredContent",
        ));
    }
    Ok(McpReceipt {
        is_error,
        structured_content_json,
        text: texts.join("\n"),
        raw_result_json: raw,
    })
}

fn header_value(value: &str) -> String {
    let bytes = value.as_bytes();
    let wrap = bytes.first().is_some_and(|b| matches!(b, b' ' | b'\t'))
        || bytes.last().is_some_and(|b| matches!(b, b' ' | b'\t'))
        || bytes.iter().any(|b| *b < 0x20 || *b > 0x7e)
        || (value.starts_with("=?base64?") && value.ends_with("?="));
    if wrap {
        use base64::Engine;
        format!(
            "=?base64?{}?=",
            base64::engine::general_purpose::STANDARD.encode(value)
        )
    } else {
        value.to_owned()
    }
}

fn parameter_headers(schema: &str, arguments: &str) -> Result<reqwest::header::HeaderMap> {
    use serde_json::Value;
    fn visit(
        value: &Value,
        allowed: bool,
        path: &mut Vec<String>,
        found: &mut Vec<(Vec<String>, String, String)>,
        seen: &mut std::collections::HashSet<String>,
    ) -> Result<()> {
        match value {
            Value::Object(map) => {
                if let Some(annotation) = map.get("x-mcp-header") {
                    let name = annotation
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .ok_or_else(|| CallError::transport("Invalid MCP header annotation"))?;
                    if !allowed || path.is_empty() || !seen.insert(name.to_ascii_lowercase()) {
                        return Err(CallError::transport(
                            "Invalid or duplicate MCP header annotation",
                        ));
                    }
                    let header = format!("mcp-param-{name}");
                    reqwest::header::HeaderName::from_bytes(header.as_bytes())
                        .map_err(|_| CallError::transport("Invalid MCP header name"))?;
                    let kind = map
                        .get("type")
                        .and_then(Value::as_str)
                        .filter(|s| matches!(*s, "string" | "integer" | "boolean"))
                        .ok_or_else(|| {
                            CallError::transport(
                                "MCP header annotation must target a primitive type",
                            )
                        })?;
                    found.push((path.clone(), header, kind.into()));
                }
                for (key, child) in map {
                    if key == "properties" && allowed {
                        let properties = child.as_object().ok_or_else(|| {
                            CallError::transport("MCP schema properties must be an object")
                        })?;
                        for (name, schema) in properties {
                            path.push(name.clone());
                            visit(schema, true, path, found, seen)?;
                            path.pop();
                        }
                    } else if key != "x-mcp-header" {
                        visit(child, false, path, found, seen)?;
                    }
                }
            }
            Value::Array(items) => {
                for child in items {
                    visit(child, false, path, found, seen)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let schema: Value =
        serde_json::from_str(schema).map_err(|e| CallError::transport(e.to_string()))?;
    let args: Value =
        serde_json::from_str(arguments).map_err(|e| CallError::transport(e.to_string()))?;
    let mut found = Vec::new();
    visit(
        &schema,
        true,
        &mut Vec::new(),
        &mut found,
        &mut std::collections::HashSet::new(),
    )?;
    let mut headers = reqwest::header::HeaderMap::new();
    for (path, name, kind) in found {
        let mut value = &args;
        for key in path {
            match value.get(&key) {
                Some(next) => value = next,
                None => {
                    value = &Value::Null;
                    break;
                }
            }
        }
        if value.is_null() {
            continue;
        }
        let text = match kind.as_str() {
            "string" => value.as_str().map(str::to_owned),
            "boolean" => value.as_bool().map(|v| v.to_string()),
            "integer" => value
                .as_i64()
                .filter(|n| n.unsigned_abs() <= 9_007_199_254_740_991)
                .map(|n| n.to_string()),
            _ => None,
        }
        .ok_or_else(|| {
            CallError::transport("MCP annotated argument is not a supported safe primitive")
        })?;
        let name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| CallError::transport("Invalid MCP header name"))?;
        let value = reqwest::header::HeaderValue::from_str(&header_value(&text))
            .map_err(|_| CallError::transport("Invalid MCP header value"))?;
        headers.insert(name, value);
    }
    Ok(headers)
}

pub(crate) type ToolAdmission = Box<
    dyn Fn() -> futures::future::BoxFuture<'static, Result<tokio::sync::OwnedMutexGuard<()>>>
        + Send
        + Sync,
>;

pub(crate) struct Session {
    server: String,
    protocol: Protocol,
    next_id: u64,
    tool_in_flight: bool,
    tool_admission: Option<ToolAdmission>,
    secret_values: Vec<String>,
    wire: Wire,
}
enum Wire {
    Stdio {
        process: ContainedBackgroundProcess,
        stdin: tokio::process::ChildStdin,
        stdout: BufReader<tokio::process::ChildStdout>,
        stderr: tokio::task::JoinHandle<std::io::Result<()>>,
        policy_digest: String,
        profile_digest: String,
    },
    Http(HttpWire),
}
struct HttpWire {
    client: reqwest::Client,
    url: url::Url,
    headers: reqwest::header::HeaderMap,
    session: Option<String>,
    version: Option<&'static str>,
    responses: VecDeque<Body>,
    tool_schema: Option<(String, String)>,
    parameter_headers: reqwest::header::HeaderMap,
    unsolicited: Option<tokio::sync::mpsc::UnboundedReceiver<Result<String>>>,
    listener: Option<tokio::task::JoinHandle<()>>,
    listener_stop: Option<tokio_util::sync::CancellationToken>,
    listener_failed: tokio_util::sync::CancellationToken,
}
type PostResponse = futures::future::BoxFuture<'static, reqwest::Result<reqwest::Response>>;

pub(crate) struct PendingToolCall {
    base: String,
    id: String,
    response: Option<PostResponse>,
}

struct Body {
    response: reqwest::Response,
    buffer: Vec<u8>,
    sse: bool,
    eof: bool,
    last_event_id: Option<String>,
    retry_ms: u64,
}

pub(crate) fn require_stdio() -> Result<()> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        Ok(())
    } else {
        Err(CallError::new(
            "MCP_STDIO_UNAVAILABLE",
            "Confined local MCP requires macOS ARM64",
        ))
    }
}

pub(crate) fn start(server: CapturedServer) -> Result<Session> {
    if matches!(&server.transport, ServerTransport::Stdio(_)) {
        require_stdio()?;
    }
    start_with_stdio(server, |config| {
        config.grant.sandbox.command(
            &config.program,
            &config.arguments,
            &config.grant.workspace,
            &config.grant.environment,
        )
    })
}

/// Linux exercises the real stdio protocol and process ownership with a local
/// test peer. The production spawn still requires the qualified sandbox.
#[cfg(all(test, unix))]
pub(crate) fn start_test_stdio(server: CapturedServer) -> Result<Session> {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        return start(server);
    }
    start_with_stdio(server, |config| {
        let mut command = crate::process::background_tokio_command("/bin/sh");
        command
            .args([
                "-c",
                "printf 'MACRO_SEATBELT_STARTED_V1\n'; exec \"$@\"",
                "mcp-test",
            ])
            .arg(&config.program)
            .args(&config.arguments)
            .current_dir(&config.grant.workspace)
            .env_clear()
            .envs(&config.grant.environment)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    })
}

fn start_with_stdio(
    server: CapturedServer,
    stdio: impl FnOnce(&StdioServer) -> std::result::Result<tokio::process::Command, String>,
) -> Result<Session> {
    if server.document_name.is_empty() || server.capture_digest.is_empty() {
        return Err(CallError::new(
            "CORRELATION",
            "MCP server is not a captured document definition",
        ));
    }
    let secret_values;
    let wire = match server.transport {
        ServerTransport::Stdio(config) => {
            let mut command = stdio(&config).map_err(CallError::transport)?;
            secret_values = config.secret_values;
            let policy = &config.grant.sandbox;
            command.stdin(Stdio::piped());
            let mut process = ContainedBackgroundProcess::spawn(command)
                .map_err(|e| CallError::transport(e.to_string()))?;
            let stdin = process.take_stdin().expect("piped MCP stdin");
            let stdout = BufReader::new(process.take_stdout().expect("piped MCP stdout"));
            let mut stderr = process.take_stderr().expect("piped MCP stderr");
            let stderr = tokio::spawn(async move {
                // Server diagnostics are not normative tool results; drain without
                // retaining secrets or unbounded unrelated log output.
                let mut buffer = [0; 8192];
                while stderr.read(&mut buffer).await? != 0 {}
                Ok(())
            });
            Wire::Stdio {
                process,
                stdin,
                stdout,
                stderr,
                policy_digest: policy.policy_digest().into(),
                profile_digest: policy.profile_digest().into(),
            }
        }
        ServerTransport::Http(config) => {
            if !matches!(config.url.scheme(), "http" | "https")
                || !config.url.username().is_empty()
                || config.url.password().is_some()
                || config.url.fragment().is_some()
                || config.addresses.is_empty()
            {
                return Err(CallError::transport(
                    "MCP endpoint must have an authorized HTTP URL and frozen addresses",
                ));
            }
            let host = config
                .url
                .host_str()
                .ok_or_else(|| CallError::transport("MCP endpoint has no host"))?;
            let port = config
                .url
                .port_or_known_default()
                .ok_or_else(|| CallError::transport("MCP endpoint has no port"))?;
            if config
                .addresses
                .iter()
                .any(|address| address.port() != port)
            {
                return Err(CallError::new(
                    "CORRELATION",
                    "Captured MCP address port differs from endpoint",
                ));
            }
            if let Ok(literal) = host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
            {
                if config
                    .addresses
                    .iter()
                    .any(|address| address.ip() != literal)
                {
                    return Err(CallError::new(
                        "CORRELATION",
                        "MCP IP literal differs from captured addresses",
                    ));
                }
            }
            let mut pinned = config.addresses;
            for address in &mut pinned {
                address.set_port(0);
            }
            let client = reqwest::Client::builder()
                .connect_timeout(HTTP_CONNECT_TIMEOUT)
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .resolve_to_addrs(host, &pinned)
                .build()
                .map_err(|e| CallError::transport(e.to_string()))?;
            secret_values = config.secret_values;
            let mut headers = config.headers;
            let reserved: Vec<_> = headers
                .keys()
                .filter(|name| !can_set_header(name))
                .cloned()
                .collect();
            for name in reserved {
                headers.remove(name);
            }
            Wire::Http(HttpWire {
                client,
                url: config.url,
                headers,
                session: None,
                version: match server.protocol {
                    Protocol::Legacy => None,
                    Protocol::Modern => Some("2026-07-28"),
                },
                responses: VecDeque::new(),
                tool_schema: config.tool_schema,
                parameter_headers: Default::default(),
                unsolicited: None,
                listener: None,
                listener_stop: None,
                listener_failed: tokio_util::sync::CancellationToken::new(),
            })
        }
    };
    Ok(Session {
        server: server.document_name,
        protocol: server.protocol,
        next_id: 1,
        tool_in_flight: false,
        tool_admission: None,
        secret_values,
        wire,
    })
}

pub(crate) fn can_set_header(name: &reqwest::header::HeaderName) -> bool {
    !name.as_str().starts_with("mcp-")
        && !matches!(
            name.as_str(),
            "host"
                | "content-length"
                | "accept"
                | "content-type"
                | "last-event-id"
                | "connection"
                | "transfer-encoding"
                | "upgrade"
                | "proxy-authorization"
                | "proxy-connection"
                | "te"
                | "trailer"
                | "expect"
                | "keep-alive"
        )
}

async fn discovery<T>(
    remote: bool,
    operation: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if remote {
        tokio::time::timeout(HTTP_DISCOVERY_TIMEOUT, operation)
            .await
            .map_err(|_| CallError::new("TIMEOUT", "MCP discovery timed out"))?
    } else {
        operation.await
    }
}

impl Session {
    pub(crate) async fn initialize(
        &mut self,
        host: std::sync::Arc<dyn HostPort>,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<()> {
        let remote = self.is_remote();
        discovery(remote, self.initialize_session(host, identity, control)).await
    }

    async fn initialize_session(
        &mut self,
        host: std::sync::Arc<dyn HostPort>,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<()> {
        if let Wire::Stdio { stdout, .. } = &mut self.wire {
            tokio::select! {
                biased;
                error = control.cancelled() => return Err(error),
                result = crate::agsdl_host_confinement::attest(stdout) => result.map_err(|error| CallError::transport(format!("Sandbox startup not attested: {error}")))?,
            }
        }
        let startup = match self.protocol {
            Protocol::Legacy => {
                let capabilities = if host.supports_human_requests() {
                    r#"{"elicitation":{"form":{}}}"#
                } else {
                    "{}"
                };
                let parameters = format!(
                    r#"{{"protocolVersion":"2025-11-25","capabilities":{capabilities},"clientInfo":{{"name":"Macro AgSDL","version":"0.3.0"}}}}"#
                );
                self.rpc("initialize", &parameters, host.as_ref(), identity, control)
                    .await?
            }
            Protocol::Modern => {
                self.rpc("server/discover", "{}", host.as_ref(), identity, control)
                    .await?
            }
        };
        let startup = object(&startup)?;
        let version = match self.protocol {
            Protocol::Legacy => startup
                .get("protocolVersion")
                .map(|v| string(v))
                .transpose()?
                .ok_or_else(|| CallError::transport("MCP handshake omitted protocolVersion"))?,
            Protocol::Modern => {
                let versions: Vec<String> = serde_json::from_str(
                    startup
                        .get("supportedVersions")
                        .ok_or_else(|| {
                            CallError::transport("MCP discovery omitted supportedVersions")
                        })?
                        .get(),
                )
                .map_err(|e| CallError::transport(e.to_string()))?;
                if !versions.iter().any(|v| v == "2026-07-28") {
                    return Err(CallError::transport(
                        "Captured server does not support modern MCP",
                    ));
                }
                "2026-07-28".into()
            }
        };
        match self.protocol {
            Protocol::Legacy
                if ![
                    "2024-10-07",
                    "2024-11-05",
                    "2025-03-26",
                    "2025-06-18",
                    "2025-11-25",
                ]
                .contains(&version.as_str()) =>
            {
                return Err(CallError::transport(
                    "Unsupported legacy MCP protocol version",
                ))
            }
            Protocol::Modern if version != "2026-07-28" => {
                return Err(CallError::transport(
                    "Captured modern MCP server negotiated another era",
                ))
            }
            _ => {}
        }
        if let Wire::Http(http) = &mut self.wire {
            http.version = Some(match version.as_str() {
                "2024-10-07" => "2024-10-07",
                "2024-11-05" => "2024-11-05",
                "2025-03-26" => "2025-03-26",
                "2025-06-18" => "2025-06-18",
                "2025-11-25" => "2025-11-25",
                _ => "2026-07-28",
            });
        }
        if matches!(self.protocol, Protocol::Legacy) {
            self.send(
                r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
                control,
            )
            .await?;
        }
        if matches!(self.protocol, Protocol::Legacy) && host.supports_human_requests() {
            if let Wire::Http(http) = &mut self.wire {
                http.start_listener(
                    host.clone(),
                    identity.clone(),
                    control.clone(),
                    self.server.clone(),
                    self.secret_values.clone(),
                );
            }
        }
        Ok(())
    }

    pub(crate) fn guard_secrets(&self, raw: &str) -> Result<()> {
        if self.secret_values.is_empty() {
            return Ok(());
        }
        reject_secret_echo(raw, &self.secret_values)
    }

    pub(crate) fn is_remote(&self) -> bool {
        matches!(self.wire, Wire::Http(_))
    }

    pub(crate) async fn list_tools(
        &mut self,
        host: &dyn HostPort,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<Vec<serde_json::Value>> {
        let remote = self.is_remote();
        discovery(remote, self.list_tool_pages(host, identity, control)).await
    }

    async fn list_tool_pages(
        &mut self,
        host: &dyn HostPort,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<Vec<serde_json::Value>> {
        use serde_json::{json, Value};
        let mut tools = Vec::new();
        let mut params = "{}".to_owned();
        let mut bytes = 0;
        for _ in 0..64 {
            let raw = self
                .rpc("tools/list", &params, host, identity, control)
                .await?;
            self.guard_secrets(&raw)?;
            bytes += raw.len();
            let invalid = || {
                CallError::new(
                    "MCP_TOOL_LIST",
                    "MCP tool discovery returned an invalid or oversized list",
                )
            };
            if bytes > MAX_MCP_MESSAGE_BYTES {
                return Err(invalid());
            }
            let mut page: Value = serde_json::from_str(&raw).map_err(|_| invalid())?;
            let batch = page["tools"].as_array_mut().ok_or_else(invalid)?;
            tools.append(batch);
            if tools.len() > 1024 {
                return Err(invalid());
            }
            if page["nextCursor"].is_null() {
                return Ok(tools);
            }
            let cursor = page["nextCursor"].as_str().ok_or_else(invalid)?;
            params = json!({"cursor":cursor}).to_string();
        }
        Err(CallError::new(
            "MCP_TOOL_LIST",
            "MCP tool discovery exceeded the page limit",
        ))
    }

    pub(crate) async fn call(
        &mut self,
        tool: &str,
        args: &str,
        host: std::sync::Arc<dyn HostPort>,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<McpReceipt> {
        object(args)?;
        self.initialize(host.clone(), identity, control).await?;
        let pending = self.dispatch_tool(tool, args, control, None).await?;
        self.finish_tool_call(pending, host.as_ref(), identity, control)
            .await
    }

    pub(crate) fn set_tool_admission(&mut self, admission: ToolAdmission) {
        self.tool_admission = Some(admission);
    }

    /// Delivers the stdio frame or hands the HTTP body to the connection.
    /// The caller can release its admission guards before collecting the reply.
    pub(crate) async fn dispatch_tool(
        &mut self,
        tool: &str,
        args: &str,
        control: &Control,
        budget_deadline: Option<Instant>,
    ) -> Result<PendingToolCall> {
        object(args)?;
        if matches!(self.protocol, Protocol::Modern) {
            if let Wire::Http(http) = &mut self.wire {
                let (captured_tool, schema) = http.tool_schema.as_ref().ok_or_else(|| {
                    CallError::new(
                        "CORRELATION",
                        "Modern MCP HTTP requires the captured tool schema",
                    )
                })?;
                if captured_tool != tool {
                    return Err(CallError::new(
                        "CORRELATION",
                        "MCP schema belongs to a different tool",
                    ));
                }
                http.parameter_headers = parameter_headers(schema, args)?;
            }
        }
        let base = format!("\"name\":{},\"arguments\":{args}", encode(&tool)?);
        let (id, response) = self
            .handoff_tool(&format!("{{{base}}}"), control, budget_deadline)
            .await?;
        Ok(PendingToolCall { base, id, response })
    }

    async fn handoff_tool(
        &mut self,
        params: &str,
        control: &Control,
        budget_deadline: Option<Instant>,
    ) -> Result<(String, Option<PostResponse>)> {
        let (id, message) = self.rpc_request("tools/call", params)?;
        let _admission = match &self.tool_admission {
            Some(admit) => Some(admit().await?),
            None => None,
        };
        control.check()?;
        let now = Instant::now();
        if budget_deadline.is_some_and(|deadline| deadline <= now) {
            return Err(tool_budget_expired());
        }
        let handoff_limit = now + TOOL_HANDOFF_TIMEOUT;
        let deadline = budget_deadline.map_or(handoff_limit, |budget| budget.min(handoff_limit));
        let remote = self.is_remote();
        let listener_failed = match &self.wire {
            Wire::Http(http) => http.listener_failed.clone(),
            Wire::Stdio { .. } => Default::default(),
        };
        let (handoff, mut accepted) = oneshot::channel();
        // A partial stdio write may already have reached the process.
        self.tool_in_flight |= !remote;
        let response = tokio::select! {
            biased;
            error = control.cancelled() => Err(error),
            _ = listener_failed.cancelled() => Err(CallError::new("UNCERTAIN", "MCP listener failed during delivery")),
            _ = tokio::time::sleep_until(deadline) => Err(CallError::new("TIMEOUT", "MCP tool delivery timed out")),
            sent = self.wire.dispatch(message, deadline, handoff, &mut accepted) => sent,
        };
        // Close before checking so a concurrent HTTP body poll either handed
        // off the payload already or can no longer do so.
        accepted.close();
        self.tool_in_flight |= response.is_ok() || accepted.try_recv().is_ok();
        if response.is_err() && budget_deadline.is_some_and(|budget| budget <= Instant::now()) {
            return Err(tool_budget_expired());
        }
        Ok((id, response?))
    }

    pub(crate) async fn finish_tool_call(
        &mut self,
        pending: PendingToolCall,
        host: &dyn HostPort,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<McpReceipt> {
        let mut raw = self
            .receive_tool(pending.id, pending.response, host, identity, control)
            .await?;
        let base = pending.base;
        let mut continuations = 0;
        loop {
            let fields = object(&raw)?;
            if fields.contains_key("task") {
                return Err(CallError::new(
                    "UNCERTAIN",
                    "MCP task termination is unavailable",
                ));
            }
            match fields.get("resultType").map(|v| string(v)).transpose()?.as_deref() {
                Some("input_required") => {
                    if !matches!(self.protocol, Protocol::Modern) || continuations >= 4 {
                        return Err(CallError::new("UNCERTAIN", "Unsupported MCP continuation or interaction limit exceeded"));
                    }
                    continuations += 1;
                    let state = fields.get("requestState").map(|v| string(v)).transpose()?;
                    if state.as_ref().is_some_and(|s| s.len() > 128 * 1024) {
                        return Err(CallError::transport("MCP requestState exceeds the 128 KiB limit"));
                    }
                    let requests = fields.get("inputRequests").map(|v| object(v.get())).transpose()?;
                    if state.is_none() && requests.as_ref().is_none_or(|r| r.is_empty()) {
                        return Err(CallError::transport("MCP input_required has no state or prompts"));
                    }
                    let mut extra = String::new();
                    if let Some(value) = fields.get("requestState") {
                        extra.push_str(&format!(",\"requestState\":{}", value.get()));
                    }
                    let requests = requests.filter(|r| !r.is_empty());
                    let human = if let Some(requests) = &requests {
                        for request in requests.values() {
                            let fields = object(request.get())?;
                            if fields.get("method").map(|v| string(v)).transpose()?.as_deref() != Some("elicitation/create") {
                                return Err(CallError::transport("Only MCP form elicitation is supported"));
                            }
                            let params = object(fields.get("params").ok_or_else(|| CallError::transport("MCP elicitation has no parameters"))?.get())?;
                            if !params.contains_key("requestedSchema") || params.get("mode").map(|v| string(v)).transpose()?.is_some_and(|m| m != "form") {
                                return Err(CallError::transport("Only MCP form elicitation is supported"));
                            }
                        }
                        if !self.secret_values.is_empty() {
                            reject_secret_echo(&raw, &self.secret_values)?;
                        }
                        Some(wait_human(host, identity, &self.server, &raw, control).await?)
                    } else {
                        None
                    };
                    let params = (|| {
                        if let (Some((_, answer)), Some(requests)) = (&human, requests) {
                            let answers_map = object(&answer.body)?;
                            if answers_map.len() != requests.len() || requests.keys().any(|id| !answers_map.contains_key(id)) {
                                return Err(CallError::new("CORRELATION", "MCP form answers do not match requests"));
                            }
                            extra.push_str(&format!(",\"inputResponses\":{}", answer.body));
                        }
                        Ok(format!("{{{base}{extra}}}"))
                    })();
                    let delivery = async {
                        let (id, response) = self.handoff_tool(&params?, control, None).await?;
                        self.receive_tool(id, response, host, identity, control).await
                    }.await;
                    raw = complete_human(host, human, delivery).await?;
                }
                Some("task") => return Err(CallError::new("UNCERTAIN", "MCP task requires a captured task completion adapter; remote effect may remain active")),
                None | Some("complete") => {
                    let result = receipt(raw).map_err(|_| CallError::transport("Malformed MCP tool result"))?;
                    self.tool_in_flight = false;
                    if !self.secret_values.is_empty() {
                        reject_secret_echo(&result.raw_result_json, &self.secret_values)?;
                    }
                    if self.secret_values.iter().any(|secret| result.text.contains(secret)) {
                        return Err(secret_echo());
                    }
                    return Ok(result);
                },
                Some(_) => return Err(CallError::transport("Unknown MCP resultType")),
            }
        }
    }

    async fn receive_tool(
        &mut self,
        id: String,
        response: Option<PostResponse>,
        host: &dyn HostPort,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<String> {
        if let Some(response) = response {
            let response = tokio::select! {
                biased;
                error = control.cancelled() => return Err(error),
                response = response => response.map_err(|e| CallError::transport(e.to_string()))?,
            };
            let Wire::Http(http) = &mut self.wire else {
                unreachable!("HTTP response requires an HTTP session");
            };
            http.accept_response(response)?;
        }
        self.receive_rpc(&id, "tools/call", host, identity, control)
            .await
    }

    async fn send(&mut self, message: &str, control: &Control) -> Result<()> {
        control.check()?;
        let listener_failed = match &self.wire {
            Wire::Http(http) => Some(http.listener_failed.clone()),
            _ => None,
        };
        tokio::select! {
            biased;
            error = control.cancelled() => Err(error),
            _ = async { match listener_failed { Some(token) => token.cancelled().await, None => std::future::pending().await } } => Err(CallError::new("UNCERTAIN", "MCP listener failed during delivery")),
            sent = self.wire.send(message) => sent,
        }
    }
    async fn rpc(
        &mut self,
        method: &str,
        params: &str,
        host: &dyn HostPort,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<String> {
        let (id, message) = self.rpc_request(method, params)?;
        self.send(&message, control).await?;
        self.receive_rpc(&id, method, host, identity, control).await
    }

    fn rpc_request(&mut self, method: &str, params: &str) -> Result<(String, String)> {
        let id = self.next_id.to_string();
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| CallError::transport("MCP request identity overflow"))?;
        let params = if matches!(self.protocol, Protocol::Modern) {
            let mut fields = object(params)?;
            fields.insert("_meta".into(), RawValue::from_string(r#"{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{"elicitation":{"form":{}}},"io.modelcontextprotocol/clientInfo":{"name":"Macro AgSDL","version":"0.3.0"}}"#.into()).expect("static JSON"));
            encode(&fields)?
        } else {
            params.to_owned()
        };
        let message = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":{},\"params\":{params}}}",
            encode(&id)?,
            encode(&method)?
        );
        Ok((id, message))
    }

    async fn receive_rpc(
        &mut self,
        id: &str,
        method: &str,
        host: &dyn HostPort,
        identity: &CallIdentity,
        control: &Control,
    ) -> Result<String> {
        loop {
            let raw = tokio::select! {
                biased;
                error = control.cancelled() => return Err(error),
                raw = self.wire.receive() => raw?,
            };
            let fields = object(&raw)?;
            if fields
                .get("jsonrpc")
                .map(|v| string(v))
                .transpose()?
                .as_deref()
                != Some("2.0")
            {
                return Err(CallError::transport("Invalid MCP JSON-RPC version"));
            }
            if let Some(method) = fields.get("method") {
                let method = string(method)?;
                if let Some(request_id) = fields.get("id") {
                    if matches!(self.protocol, Protocol::Modern) {
                        return Err(CallError::transport(
                            "Modern MCP forbids server-initiated requests",
                        ));
                    }
                    let human = if method == "elicitation/create" && host.supports_human_requests()
                    {
                        if !self.secret_values.is_empty() {
                            reject_secret_echo(&raw, &self.secret_values)?;
                        }
                        Some(wait_human(host, identity, &self.server, &raw, control).await?)
                    } else {
                        None
                    };
                    let delivery = match legacy_reply(
                        &method,
                        &human,
                        "Method not supported by this captured call",
                    ) {
                        Ok(response) => {
                            self.send(
                                &format!(
                                    "{{\"jsonrpc\":\"2.0\",\"id\":{},{response}}}",
                                    request_id.get()
                                ),
                                control,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    };
                    complete_human(host, human, delivery).await?;
                }
                continue;
            }
            if fields.get("id").map(|v| string(v)).transpose()?.as_deref() != Some(id) {
                return Err(CallError::new(
                    "CORRELATION",
                    "MCP response belongs to another request",
                ));
            }
            if let Wire::Http(http) = &mut self.wire {
                // Each request owns its POST response. A server may leave the
                // SSE connection open after delivering the matching result.
                http.responses.pop_front();
            }
            if let Some(error) = fields.get("error") {
                let error = object(error.get())?;
                if fields.contains_key("result")
                    || error
                        .get("code")
                        .is_none_or(|v| serde_json::from_str::<i64>(v.get()).is_err())
                    || error.get("message").is_none_or(|v| string(v).is_err())
                {
                    return Err(CallError::transport("Malformed MCP JSON-RPC error"));
                }
                if method == "tools/call" {
                    self.tool_in_flight = false;
                }
                return Err(CallError::transport("MCP returned a JSON-RPC error"));
            }
            return fields
                .get("result")
                .map(|v| v.get().to_owned())
                .ok_or_else(|| CallError::transport("MCP response has no result"));
        }
    }

    pub(crate) fn tool_in_flight(&self) -> bool {
        self.tool_in_flight
    }

    pub(crate) async fn shutdown(self, received: bool) -> (PhysicalEvidence, Option<CallError>) {
        match self.wire {
            Wire::Http(mut http) => {
                if let Some(stop) = &http.listener_stop {
                    stop.cancel();
                }
                let mut failure = None;
                if let Some(mut listener) = http.listener.take() {
                    if !matches!(
                        tokio::time::timeout(Duration::from_secs(5), &mut listener).await,
                        Ok(Ok(()))
                    ) {
                        listener.abort();
                        let _ = listener.await;
                        failure = Some(CallError::new("UNCERTAIN", "MCP listener did not close"));
                    }
                }
                // Session cleanup is independent of whether a tool stopped.
                http.responses.clear();
                if http.session.is_some() {
                    let _ = tokio::time::timeout(
                        HTTP_SESSION_CLOSE_TIMEOUT,
                        http.request(reqwest::Method::DELETE).send(),
                    )
                    .await;
                }
                (
                    PhysicalEvidence::Remote {
                        response_received: received,
                        termination_proven: false,
                    },
                    failure,
                )
            }
            Wire::Stdio {
                mut process,
                stdin,
                stdout,
                mut stderr,
                policy_digest,
                profile_digest,
            } => {
                drop(stdin);
                drop(stdout);
                let ended = process.terminate_bounded().await;
                let drained = tokio::time::timeout(Duration::from_secs(5), &mut stderr).await;
                let streams_joined = matches!(drained, Ok(Ok(Ok(()))));
                if !streams_joined {
                    stderr.abort();
                    let _ = stderr.await;
                }
                let evidence = PhysicalEvidence::Local {
                    policy_digest,
                    profile_digest,
                    leader_reaped: ended.is_ok(),
                    tree_termination_succeeded: ended.is_ok(),
                    streams_joined,
                };
                let failure = if ended.is_err() || !streams_joined {
                    Some(CallError::new(
                        "UNCERTAIN",
                        "MCP local process cleanup was not attested",
                    ))
                } else {
                    None
                };
                (evidence, failure)
            }
        }
    }
}

fn reject_secret_echo(raw: &str, secrets: &[String]) -> Result<()> {
    use serde_json::Value;
    fn contains(value: &Value, secrets: &[String]) -> bool {
        let matches = |value: &str| secrets.iter().any(|secret| value.contains(secret));
        match value {
            Value::String(value) => matches(value),
            Value::Array(items) => items.iter().any(|value| contains(value, secrets)),
            Value::Object(fields) => fields
                .iter()
                .any(|(key, value)| matches(key) || contains(value, secrets)),
            _ => false,
        }
    }
    // Inspect both raw and decoded text (e.g. an escaped Unicode string).
    // Fail before processing server requests or building a durable receipt.
    let value: Value = serde_json::from_str(raw)
        .map_err(|_| CallError::transport("Invalid secret-bearing MCP message"))?;
    if secrets.iter().any(|secret| raw.contains(secret)) || contains(&value, secrets) {
        return Err(secret_echo());
    }
    Ok(())
}

fn secret_echo() -> CallError {
    CallError::transport("MCP message echoed a server secret")
}

type PendingHuman = (String, HumanReply);

fn legacy_reply(method: &str, human: &Option<PendingHuman>, unsupported: &str) -> Result<String> {
    if let Some((_, reply)) = human {
        object(&reply.body)?;
        Ok(format!("\"result\":{}", reply.body))
    } else if method == "ping" {
        Ok("\"result\":{}".into())
    } else {
        Ok(format!(
            "\"error\":{{\"code\":-32601,\"message\":{}}}",
            encode(&unsupported)?
        ))
    }
}

async fn wait_human(
    host: &dyn HostPort,
    identity: &CallIdentity,
    server: &str,
    raw: &str,
    control: &Control,
) -> Result<PendingHuman> {
    control.check()?;
    let ticket = host.human_open(identity, server, raw).await?;
    let answer = tokio::select! {
        biased;
        error = control.cancelled() => Err(error),
        answer = host.human_answer(&ticket) => answer,
    };
    match answer {
        Ok(reply) => Ok((ticket, reply)),
        Err(error) => {
            host.human_close(&ticket, &Err(error.clone())).await?;
            Err(error)
        }
    }
}

async fn complete_human<T>(
    host: &dyn HostPort,
    human: Option<PendingHuman>,
    delivery: Result<T>,
) -> Result<T> {
    if let Some((ticket, reply)) = human {
        let outcome = match &delivery {
            Ok(_) => {
                reply.acknowledge.await?;
                Ok(reply.body)
            }
            Err(error) => Err(error.clone()),
        };
        host.human_close(&ticket, &outcome).await?;
    }
    delivery
}

impl Drop for HttpWire {
    fn drop(&mut self) {
        if let Some(listener) = self.listener.take() {
            listener.abort();
        }
    }
}
impl HttpWire {
    fn request(&self, method: reqwest::Method) -> reqwest::RequestBuilder {
        let mut request = self
            .client
            .request(method, self.url.clone())
            .headers(self.headers.clone());
        if let Some(version) = self.version {
            request = request.header("mcp-protocol-version", version);
        }
        if let Some(session) = &self.session {
            request = request.header("mcp-session-id", session);
        }
        request
    }

    fn post(&self, message: &str) -> Result<reqwest::RequestBuilder> {
        let mut request = self
            .request(reqwest::Method::POST)
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json");
        if self.version == Some("2026-07-28") {
            let message_fields = object(message)?;
            if let Some(method) = message_fields.get("method") {
                let method = string(method)?;
                request = request.header("mcp-method", &method);
                if method == "tools/call" {
                    request = request.headers(self.parameter_headers.clone());
                    let params = object(
                        message_fields
                            .get("params")
                            .ok_or_else(|| CallError::transport("MCP call has no params"))?
                            .get(),
                    )?;
                    let name = string(
                        params
                            .get("name")
                            .ok_or_else(|| CallError::transport("MCP call has no name"))?,
                    )?;
                    let bytes = name.as_bytes();
                    let wrap = bytes.first().is_some_and(|b| matches!(b, b' ' | b'\t'))
                        || bytes.last().is_some_and(|b| matches!(b, b' ' | b'\t'))
                        || bytes.iter().any(|b| *b < 0x20 || *b > 0x7e)
                        || (name.starts_with("=?base64?") && name.ends_with("?="));
                    let name = if wrap {
                        use base64::Engine;
                        format!(
                            "=?base64?{}?=",
                            base64::engine::general_purpose::STANDARD.encode(name)
                        )
                    } else {
                        name
                    };
                    request = request.header("mcp-name", name);
                }
            }
        }
        Ok(request)
    }

    fn accept_response(&mut self, response: reqwest::Response) -> Result<()> {
        if !response.status().is_success() {
            return Err(CallError::transport(format!(
                "Captured MCP HTTP returned {} without replay",
                response.status()
            )));
        }
        if let Some(session) = response
            .headers()
            .get("mcp-session-id")
            .filter(|_| self.version != Some("2026-07-28"))
        {
            let session = session
                .to_str()
                .map_err(|_| CallError::transport("Invalid MCP session header"))?;
            if self
                .session
                .as_deref()
                .is_some_and(|previous| previous != session)
            {
                return Err(CallError::new(
                    "CORRELATION",
                    "MCP server changed its captured session",
                ));
            }
            self.session = Some(session.into());
        }
        if response.status() == reqwest::StatusCode::ACCEPTED
            || response.status() == reqwest::StatusCode::NO_CONTENT
        {
            return Ok(());
        }
        let kind = response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        let sse = match kind {
            "application/json" => false,
            "text/event-stream" => true,
            _ => return Err(CallError::transport("Unsupported MCP HTTP content type")),
        };
        self.responses.push_back(Body {
            response,
            buffer: Vec::new(),
            sse,
            eof: false,
            last_event_id: None,
            retry_ms: 1000,
        });
        Ok(())
    }

    fn start_listener(
        &mut self,
        host: std::sync::Arc<dyn HostPort>,
        identity: CallIdentity,
        control: Control,
        server: String,
        secret_values: Vec<String>,
    ) {
        if self.listener.is_some() {
            return;
        }
        let client = self.client.clone();
        let url = self.url.clone();
        let headers = self.headers.clone();
        let session = self.session.clone();
        let version = self
            .version
            .expect("initialized legacy session negotiated a version");
        let stop = control.stop.child_token();
        self.listener_stop = Some(stop.clone());
        let control = Control {
            stop,
            machine_timeout: control.machine_timeout,
        };
        let failed = self.listener_failed.clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.unsolicited = Some(rx);
        self.listener = Some(tokio::spawn(async move {
            let mut cursor: Option<String> = None;
            let mut delay = 1000u64;
            loop {
                let mut request = client
                    .get(url.clone())
                    .headers(headers.clone())
                    .header("accept", "text/event-stream")
                    .header("mcp-protocol-version", version);
                if let Some(session) = &session {
                    request = request.header("mcp-session-id", session);
                }
                if let Some(id) = &cursor {
                    request = request.header("last-event-id", id);
                }
                let response = tokio::select! {
                    biased;
                    _ = control.cancelled() => break,
                    response = request.send() => match response { Ok(response) => response, Err(_) => break },
                };
                if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
                    break;
                }
                let content_type = response
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .split(';')
                    .next()
                    .unwrap_or("")
                    .trim();
                if !response.status().is_success() || content_type != "text/event-stream" {
                    break;
                }
                let mut body = Body {
                    response,
                    buffer: Vec::new(),
                    sse: true,
                    eof: false,
                    last_event_id: cursor.clone(),
                    retry_ms: delay,
                };
                loop {
                    let message = tokio::select! {
                        biased;
                        _ = control.cancelled() => return,
                        message = body.next() => message,
                    };
                    match message {
                        Ok(Some(message)) => {
                            let result = process_listener_message(
                                &client,
                                &url,
                                &headers,
                                session.as_deref(),
                                version,
                                host.as_ref(),
                                &identity,
                                &server,
                                &message,
                                &secret_values,
                                &control,
                            )
                            .await;
                            if let Err(error) = result {
                                let _ = tx.send(Err(error));
                                failed.cancel();
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            let _ = tx.send(Err(error));
                            failed.cancel();
                            return;
                        }
                    }
                }
                cursor = body.last_event_id;
                delay = body.retry_ms;
                if cursor.is_none() {
                    break;
                }
                tokio::select! {
                    biased;
                    _ = control.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_millis(delay)) => {},
                }
            }
        }));
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_listener_message(
    client: &reqwest::Client,
    url: &url::Url,
    headers: &reqwest::header::HeaderMap,
    session: Option<&str>,
    version: &str,
    host: &dyn HostPort,
    identity: &CallIdentity,
    server: &str,
    raw: &str,
    secret_values: &[String],
    control: &Control,
) -> Result<()> {
    if !secret_values.is_empty() {
        reject_secret_echo(raw, secret_values)?;
    }
    let fields = object(raw)?;
    if fields
        .get("jsonrpc")
        .map(|v| string(v))
        .transpose()?
        .as_deref()
        != Some("2.0")
    {
        return Err(CallError::transport(
            "Invalid unsolicited MCP JSON-RPC version",
        ));
    }
    let method = fields
        .get("method")
        .map(|v| string(v))
        .transpose()?
        .ok_or_else(|| CallError::new("CORRELATION", "Unsolicited GET carried a response"))?;
    let Some(id) = fields.get("id") else {
        return Ok(());
    };
    let human = if method == "elicitation/create" {
        Some(wait_human(host, identity, server, raw, control).await?)
    } else {
        None
    };
    let response = legacy_reply(&method, &human, "Method not supported");
    let delivery = async {
        let response = response?;
        let body = format!("{{\"jsonrpc\":\"2.0\",\"id\":{},{response}}}", id.get());
        let mut request = client
            .post(url.clone())
            .headers(headers.clone())
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .header("mcp-protocol-version", version)
            .body(body);
        if let Some(session) = session {
            request = request.header("mcp-session-id", session);
        }
        let response = tokio::select! { biased; error = control.cancelled() => return Err(error), response = request.send() => response.map_err(|e| CallError::transport(e.to_string()))? };
        if response.status() != reqwest::StatusCode::ACCEPTED {
            return Err(CallError::transport(
                "MCP GET request response was not accepted",
            ));
        }
        Ok(())
    }.await;
    complete_human(host, human, delivery).await
}

fn tool_budget_expired() -> CallError {
    CallError::new(TOOL_BUDGET_EXPIRED, "Remote MCP tool budget expired")
}

impl Wire {
    async fn send(&mut self, message: &str) -> Result<()> {
        match self {
            Wire::Stdio { stdin, .. } => {
                stdin
                    .write_all(message.as_bytes())
                    .await
                    .map_err(|e| CallError::transport(e.to_string()))?;
                stdin
                    .write_all(b"\n")
                    .await
                    .map_err(|e| CallError::transport(e.to_string()))?;
                stdin
                    .flush()
                    .await
                    .map_err(|e| CallError::transport(e.to_string()))
            }
            Wire::Http(http) => {
                let response = http
                    .post(message)?
                    .body(message.to_owned())
                    .send()
                    .await
                    .map_err(|e| CallError::transport(e.to_string()))?;
                http.accept_response(response)
            }
        }
    }

    async fn dispatch(
        &mut self,
        message: String,
        deadline: Instant,
        handoff: oneshot::Sender<()>,
        accepted: &mut oneshot::Receiver<()>,
    ) -> Result<Option<PostResponse>> {
        let Self::Http(http) = self else {
            self.send(&message).await?;
            return Ok(None);
        };
        let request = http.post(&message)?;
        let length = message.len();
        let stream = futures::stream::once(async move {
            // The connection can poll the body on another task before the
            // caller observes its timer. Check the deadline at the handoff.
            if Instant::now() < deadline && handoff.send(()).is_ok() {
                return Ok(message);
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "MCP tool delivery cancelled",
            ))
        });
        let mut response: PostResponse = Box::pin(
            request
                .header(reqwest::header::CONTENT_LENGTH, length)
                .body(reqwest::Body::wrap_stream(stream))
                .send(),
        );
        tokio::select! {
            biased;
            handed_off = &mut *accepted => {
                handed_off.map_err(|_| CallError::transport("MCP request body was discarded"))?;
                Ok(Some(response))
            }
            reply = &mut response => {
                if accepted.try_recv().is_ok() {
                    return Ok(Some(Box::pin(std::future::ready(reply))));
                }
                let reply = reply.map_err(|e| CallError::transport(e.to_string()))?;
                Err(CallError::transport(format!("MCP HTTP returned {} before tool delivery", reply.status())))
            }
        }
    }

    async fn receive(&mut self) -> Result<String> {
        match self {
            Wire::Stdio { stdout, .. } => {
                let mut bytes = Vec::new();
                let count = stdout
                    .take((MAX_MCP_MESSAGE_BYTES + 1) as u64)
                    .read_until(b'\n', &mut bytes)
                    .await
                    .map_err(|e| CallError::transport(e.to_string()))?;
                if count > MAX_MCP_MESSAGE_BYTES {
                    return Err(CallError::new(
                        "OUTPUT_INVALID",
                        "MCP stdio frame exceeds 16 MiB",
                    ));
                }
                if count == 0 || bytes.last() != Some(&b'\n') {
                    return Err(CallError::transport(
                        "MCP stdio ended without a complete frame",
                    ));
                }
                String::from_utf8(bytes)
                    .map_err(|_| CallError::transport("MCP stdio frame is not UTF-8"))
            }
            Wire::Http(http) => loop {
                let body = http.responses.front_mut().ok_or_else(|| {
                    CallError::transport(
                        "MCP HTTP has no response stream; effect will not be retried",
                    )
                })?;
                let unsolicited = &mut http.unsolicited;
                let received = tokio::select! {
                    biased;
                    Some(message) = async {
                        match unsolicited { Some(rx) => rx.recv().await, None => std::future::pending().await }
                    } => return message,
                    message = body.next() => message?,
                };
                if let Some(value) = received {
                    return Ok(value);
                }
                let body = http.responses.pop_front().expect("response exists");
                if body.sse && http.version != Some("2026-07-28") {
                    if let Some(id) = body.last_event_id {
                        // Resume only the response stream. The original POST is never replayed.
                        tokio::time::sleep(Duration::from_millis(body.retry_ms)).await;
                        let response = http
                            .request(reqwest::Method::GET)
                            .header("accept", "text/event-stream")
                            .header("last-event-id", id.clone())
                            .send()
                            .await
                            .map_err(|e| CallError::transport(e.to_string()))?;
                        let kind = response
                            .headers()
                            .get("content-type")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .split(';')
                            .next()
                            .unwrap_or("")
                            .trim();
                        if !response.status().is_success() || kind != "text/event-stream" {
                            return Err(CallError::new(
                                "UNCERTAIN",
                                "MCP SSE resumption failed; effect was not replayed",
                            ));
                        }
                        http.responses.push_front(Body {
                            response,
                            buffer: Vec::new(),
                            sse: true,
                            eof: false,
                            last_event_id: Some(id),
                            retry_ms: body.retry_ms,
                        });
                    }
                }
            },
        }
    }
}
impl Body {
    async fn next(&mut self) -> Result<Option<String>> {
        loop {
            if self.sse {
                // SSE LF and CRLF framing; data fields join with LF. Heartbeats
                // and non-message events do not fabricate a JSON-RPC response.
                let end = self
                    .buffer
                    .windows(2)
                    .position(|w| w == b"\n\n")
                    .map(|p| (p, 2))
                    .into_iter()
                    .chain(
                        self.buffer
                            .windows(4)
                            .position(|w| w == b"\r\n\r\n")
                            .map(|p| (p, 4)),
                    )
                    .min_by_key(|v| v.0);
                if let Some((end, separator)) = end {
                    let event = String::from_utf8(self.buffer.drain(..end + separator).collect())
                        .map_err(|_| CallError::transport("MCP SSE is not UTF-8"))?;
                    for line in event.lines() {
                        if let Some(value) = line.strip_prefix("id:") {
                            let value = value.strip_prefix(' ').unwrap_or(value);
                            if !value.contains('\0') {
                                self.last_event_id = if value.is_empty() {
                                    None
                                } else {
                                    Some(value.to_owned())
                                };
                            }
                        } else if let Some(value) = line.strip_prefix("retry:") {
                            let value = value.strip_prefix(' ').unwrap_or(value);
                            if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) {
                                if let Ok(ms) = value.parse::<u64>() {
                                    self.retry_ms = ms;
                                }
                            }
                        }
                    }
                    let data: Vec<_> = event
                        .lines()
                        .filter_map(|line| {
                            line.strip_prefix("data:")
                                .map(|v| v.strip_prefix(' ').unwrap_or(v))
                        })
                        .collect();
                    if !data.is_empty() {
                        let data = data.join("\n");
                        if !data.is_empty() {
                            return Ok(Some(data));
                        }
                    }
                    continue;
                }
            }
            if self.eof {
                if self.buffer.is_empty() {
                    return Ok(None);
                }
                if self.sse {
                    return Err(CallError::transport(
                        "MCP SSE ended with an incomplete event",
                    ));
                }
                return String::from_utf8(std::mem::take(&mut self.buffer))
                    .map(Some)
                    .map_err(|_| CallError::transport("MCP HTTP JSON is not UTF-8"));
            }
            match self.response.chunk().await {
                Err(_) if self.sse && self.last_event_id.is_some() => {
                    self.buffer.clear();
                    self.eof = true;
                }
                Err(error) => return Err(CallError::transport(error.to_string())),
                Ok(None) => self.eof = true,
                Ok(Some(chunk)) => {
                    if self.buffer.len().saturating_add(chunk.len()) > MAX_MCP_MESSAGE_BYTES {
                        return Err(CallError::new(
                            "OUTPUT_INVALID",
                            "MCP HTTP message or SSE event exceeds 16 MiB",
                        ));
                    }
                    self.buffer.try_reserve(chunk.len()).map_err(|_| {
                        CallError::transport("Could not retain complete MCP result")
                    })?;
                    self.buffer.extend_from_slice(&chunk);
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "agsdl_raw_tests.rs"]
mod tests;
