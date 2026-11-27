//! One HTTP/1 egress proxy per shell invocation. The sandbox never receives
//! a socket connected to an upstream server or access to the host resolver.
mod clock;
mod tls;

use clock::Clock;

use super::super::{Approval, Registry};
use super::ShellNetwork;
use crate::{
    policy::{normalize_network_host, Action, Effect, Zone},
    providers::ToolCall,
};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use http::uri::Authority;
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::{
    body::{Body, Bytes, Incoming},
    header::{self, HeaderMap, HeaderValue},
    service::service_fn,
    Method, Request, Response, StatusCode, Uri,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    collections::HashMap,
    io::{self, Read},
    net::{IpAddr, SocketAddr},
    os::fd::AsFd,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tls::Intercept;
use tokio::{
    io::{unix::AsyncFd, AsyncRead, AsyncWrite, Interest, ReadBuf, Ready},
    net::{lookup_host, TcpStream},
    sync::{mpsc, Mutex},
    task::{JoinHandle, JoinSet},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const MAX_CONNECTIONS: usize = 128;
const MAX_CONCURRENT: usize = 16;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_CONNECTION_TIME: Duration = Duration::from_secs(120);
const MAX_DIAL_TIME: Duration = Duration::from_secs(10);
const MAX_HEADER_BYTES: usize = 32 * 1024;
const PROXY_ENV: &[&str] = &[
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
];

#[cfg(target_os = "linux")]
type Listener = tokio::net::UnixListener;
#[cfg(target_os = "macos")]
type Listener = tokio::net::TcpListener;

pub(super) struct Proxy {
    cancel: CancellationToken,
    task: Option<JoinHandle<()>>,
    #[cfg(target_os = "macos")]
    pub(super) port: u16,
    url: String,
    #[cfg(target_os = "linux")]
    pub(super) socket: std::path::PathBuf,
    pub(super) ca_bundle: Option<PathBuf>,
    pub(super) clock: Arc<Clock>,
    _directory: tempfile::TempDir,
}

impl Proxy {
    pub(super) fn start(
        registry: Arc<Registry>,
        call: ToolCall,
        cancel: &CancellationToken,
    ) -> io::Result<Self> {
        // The random credential binds connections (including local TCP on
        // macOS) to this invocation. It is never forwarded upstream.
        let mut nonce = [0; 24];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut nonce)?;
        let password = URL_SAFE_NO_PAD.encode(nonce);
        let auth = format!("Basic {}", STANDARD.encode(format!("argentic:{password}")));
        let directory = tempfile::tempdir_in("/tmp")?;
        let ca_bundle = (registry.shell_network == ShellNetwork::Intercept)
            .then(|| {
                directory
                    .path()
                    .canonicalize()
                    .map(|path| path.join("ca.pem"))
            })
            .transpose()?;
        let tls = ca_bundle.as_deref().map(Intercept::new).transpose()?;
        #[cfg(target_os = "linux")]
        let (listener, socket, port) = {
            let socket = directory.path().join("proxy.sock");
            let listener = std::os::unix::net::UnixListener::bind(&socket)?;
            listener.set_nonblocking(true)?;
            (
                Listener::from_std(listener)?,
                socket,
                super::linux::RELAY_PORT,
            )
        };
        #[cfg(target_os = "macos")]
        let (listener, port) = {
            let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
            listener.set_nonblocking(true)?;
            let port = listener.local_addr()?.port();
            (Listener::from_std(listener)?, port)
        };
        let url = format!("http://argentic:{password}@127.0.0.1:{port}");
        let cancel = cancel.child_token();
        let clock = Arc::new(Clock::new());
        let context = Arc::new(Egress {
            registry,
            call,
            cancel: cancel.clone(),
            auth,
            host_tcp_port: cfg!(target_os = "macos").then_some(port),
            bytes: Arc::new(AtomicUsize::new(MAX_BYTES)),
            approvals: Arc::new(Mutex::new(HashMap::new())),
            clock: clock.clone(),
            tls,
            tasks: TaskTracker::new(),
        });
        let task = tokio::spawn(accept(listener, context));
        Ok(Self {
            cancel,
            task: Some(task),
            #[cfg(target_os = "macos")]
            port,
            url,
            #[cfg(target_os = "linux")]
            socket,
            ca_bundle,
            clock,
            _directory: directory,
        })
    }

    pub(super) fn env(&self) -> impl Iterator<Item = (&str, &str)> {
        PROXY_ENV
            .iter()
            .map(|&key| (key, self.url.as_str()))
            .chain([("no_proxy", ""), ("NO_PROXY", "")])
            .chain(self.ca_bundle.iter().flat_map(|path| {
                #[cfg(target_os = "macos")]
                let bundle = path.to_str().unwrap_or_default();
                #[cfg(target_os = "linux")]
                let bundle = {
                    let _ = path;
                    "/run/argentic-ca.pem"
                };
                [
                    "SSL_CERT_FILE",
                    "REQUESTS_CA_BUNDLE",
                    "NODE_EXTRA_CA_CERTS",
                    "CURL_CA_BUNDLE",
                    "GIT_SSL_CAINFO",
                ]
                .map(|key| (key, bundle))
            }))
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            // Proxy is owned by the blocking shell worker.
            let _ = tokio::runtime::Handle::current().block_on(task);
        }
    }
}

struct Egress {
    registry: Arc<Registry>,
    call: ToolCall,
    cancel: CancellationToken,
    auth: String,
    host_tcp_port: Option<u16>,
    bytes: Arc<AtomicUsize>,
    approvals: Arc<Mutex<HashMap<String, bool>>>,
    clock: Arc<Clock>,
    tls: Option<Intercept>,
    tasks: TaskTracker,
}

async fn accept(listener: Listener, context: Arc<Egress>) {
    let mut jobs = JoinSet::new();
    let mut remaining = MAX_CONNECTIONS;
    while remaining > 0 || !jobs.is_empty() {
        tokio::select! {
            biased;
            _ = context.cancel.cancelled() => break,
            _ = jobs.join_next(), if !jobs.is_empty() => {},
            accepted = listener.accept(), if remaining > 0 && jobs.len() < MAX_CONCURRENT => {
                let Ok((client, _)) = accepted else { break };
                remaining -= 1;
                let context = context.clone();
                jobs.spawn(async move {
                    let Ok(fd) = client.as_fd().try_clone_to_owned() else {
                        return;
                    };
                    // SAFETY: The duplicate stays open and unchanged while AsyncFd owns it.
                    let Ok(fd) = (unsafe { AsyncFd::register(fd) }) else {
                        return;
                    };
                    let base = context.clock.base_after(MAX_CONNECTION_TIME);
                    tokio::select! {
                        _ = disconnected(fd) => {},
                        _ = context.clock.expire(base) => {},
                        _ = serve(client, context.clone()) => {},
                    }
                });
            }
        }
    }
    jobs.shutdown().await;
    context.tasks.close();
    context.tasks.wait().await;
}

// Watch the transport independently of Hyper's body backpressure. An EOF
// must withdraw an ask even if the client sent body bytes before disconnecting.
async fn disconnected(fd: AsyncFd<std::os::fd::OwnedFd>) {
    while let Ok(mut ready) = fd.ready(Interest::READABLE).await {
        if ready.ready().is_read_closed() {
            break;
        }
        ready.clear_ready_matching(Ready::READABLE);
    }
}

type ProxyBody = UnsyncBoxBody<Bytes, hyper::Error>;
enum Tunnel {
    Opaque(TcpStream),
    Intercept(Target),
}

async fn serve(client: impl AsyncRead + AsyncWrite + Unpin + Send + 'static, context: Arc<Egress>) {
    let lifetime = context.cancel.child_token();
    let _on_exit = lifetime.clone().drop_guard();
    let (tunnel, mut tunnel_rx) = mpsc::channel(1);
    let client = Metered {
        io: client,
        bytes: context.bytes.clone(),
    };
    let service = service_fn(|request| {
        let (context, tunnel, lifetime) = (context.clone(), tunnel.clone(), lifetime.clone());
        async move {
            let outcome = context.forward(request, tunnel, lifetime).await;
            Ok::<_, std::convert::Infallible>(
                outcome.unwrap_or_else(|(status, error)| message(status, error)),
            )
        }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .keep_alive(false)
        .max_buf_size(MAX_HEADER_BYTES)
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(5))
        .serve_connection(TokioIo::new(client), service)
        .with_upgrades()
        .await;
    if let Ok((upgrade, tunnel)) = tunnel_rx.try_recv() {
        let Ok(client) = upgrade.await else { return };
        let mut client = TokioIo::new(client);
        match tunnel {
            Tunnel::Opaque(mut upstream) => {
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            }
            Tunnel::Intercept(target) => {
                let Some(tls) = &context.tls else { return };
                let Ok(Ok(client)) =
                    tokio::time::timeout(MAX_DIAL_TIME, tls.accept(client, &target.host)).await
                else {
                    return;
                };
                let service = service_fn(|request| {
                    let (context, lifetime) = (context.clone(), lifetime.clone());
                    let target = &target;
                    async move {
                        let outcome = context.forward_tls(request, target, &lifetime).await;
                        Ok::<_, std::convert::Infallible>(
                            outcome.unwrap_or_else(|(status, error)| message(status, error)),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .max_buf_size(MAX_HEADER_BYTES)
                    .timer(TokioTimer::new())
                    .header_read_timeout(Duration::from_secs(5))
                    .serve_connection(TokioIo::new(client), service)
                    .await;
            }
        }
    }
}

fn message(status: StatusCode, error: &'static str) -> Response<ProxyBody> {
    let mut response = Response::new(
        Full::new(Bytes::from_static(error.as_bytes()))
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *response.status_mut() = status;
    response
}

type Failure = (StatusCode, &'static str);
const BAD_TARGET: Failure = (
    StatusCode::BAD_REQUEST,
    "Expected an absolute HTTP URL or CONNECT host:port\n",
);
const DENIED: Failure = (
    StatusCode::FORBIDDEN,
    "Argentic network policy denied this destination\n",
);
const UPSTREAM: Failure = (
    StatusCode::BAD_GATEWAY,
    "Argentic could not reach this destination\n",
);

#[derive(PartialEq, Eq)]
struct Target {
    host: String,
    port: u16,
}

impl Target {
    fn new(authority: &Authority, default_port: Option<u16>) -> Result<Self, Failure> {
        if authority.as_str().contains('@') {
            return Err(BAD_TARGET);
        }
        let port = if authority.as_str() == authority.host() {
            default_port
        } else {
            authority.port_u16()
        }
        .filter(|port| *port != 0)
        .ok_or(BAD_TARGET)?;
        let host = destination(authority.host()).ok_or(BAD_TARGET)?;
        Ok(Self { host, port })
    }

    fn host_header(&self) -> Result<HeaderValue, Failure> {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        HeaderValue::from_str(&format!("{host}:{}", self.port)).map_err(|_| BAD_TARGET)
    }
}

impl Egress {
    async fn forward(
        &self,
        mut request: Request<Incoming>,
        tunnel: mpsc::Sender<(hyper::upgrade::OnUpgrade, Tunnel)>,
        lifetime: CancellationToken,
    ) -> Result<Response<ProxyBody>, Failure> {
        if !request
            .headers()
            .get(header::PROXY_AUTHORIZATION)
            .is_some_and(|value| value.as_bytes() == self.auth.as_bytes())
        {
            let mut response = message(
                StatusCode::PROXY_AUTHENTICATION_REQUIRED,
                "Argentic proxy credentials required\n",
            );
            response.headers_mut().insert(
                header::PROXY_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"argentic\""),
            );
            return Ok(response);
        }
        let uri = request.uri();
        let authority = uri.authority().ok_or(BAD_TARGET)?;
        if request.method() == Method::CONNECT {
            if uri.scheme().is_some()
                || uri.path_and_query().is_some()
                || request.body().size_hint().exact() != Some(0)
            {
                return Err(BAD_TARGET);
            }
            let target = Target::new(authority, None)?;
            let destination = if self.tls.is_some() {
                if self.effect(&target.host) == Effect::Deny {
                    return Err(DENIED);
                }
                Tunnel::Intercept(target)
            } else {
                Tunnel::Opaque(self.dial(&target.host, target.port, &lifetime).await?)
            };
            tunnel
                .try_send((hyper::upgrade::on(&mut request), destination))
                .map_err(|_| UPSTREAM)?;
            return Ok(message(StatusCode::OK, ""));
        }

        if uri.scheme_str() != Some("http") {
            return Err(BAD_TARGET);
        }
        let target = Target::new(authority, Some(80))?;
        let upstream = self.dial(&target.host, target.port, &lifetime).await?;
        self.send(request, &target, upstream, &lifetime).await
    }

    async fn forward_tls(
        &self,
        request: Request<Incoming>,
        target: &Target,
        lifetime: &CancellationToken,
    ) -> Result<Response<ProxyBody>, Failure> {
        let uri = request.uri();
        if request.method() == Method::CONNECT
            || uri.scheme().is_some()
            || uri.authority().is_some()
            || !uri.path().starts_with('/')
        {
            return Err(BAD_TARGET);
        }
        let mut hosts = request.headers().get_all(header::HOST).iter();
        let authority = hosts
            .next()
            .and_then(|value| Authority::try_from(value.as_bytes()).ok())
            .ok_or(BAD_TARGET)?;
        if hosts.next().is_some() || Target::new(&authority, Some(443))? != *target {
            return Err(DENIED);
        }
        let tcp = self.dial(&target.host, target.port, lifetime).await?;
        let tls = self.tls.as_ref().ok_or(UPSTREAM)?;
        let upstream = tokio::time::timeout(MAX_DIAL_TIME, tls.connect(tcp, &target.host))
            .await
            .map_err(|_| UPSTREAM)?
            .map_err(|_| UPSTREAM)?;
        self.send(request, target, upstream, lifetime).await
    }

    async fn send(
        &self,
        request: Request<Incoming>,
        target: &Target,
        upstream: impl AsyncRead + AsyncWrite + Unpin + Send + 'static,
        lifetime: &CancellationToken,
    ) -> Result<Response<ProxyBody>, Failure> {
        let path: Uri = request
            .uri()
            .path_and_query()
            .map_or("/", |value| value.as_str())
            .parse()
            .map_err(|_| BAD_TARGET)?;
        // The destination and Host header always come from the same authority.
        // Hyper supplies fresh framing and never follows redirects on our behalf.
        let (mut parts, body) = request.into_parts();
        parts.uri = path;
        parts.version = hyper::Version::HTTP_11;
        strip_hop_headers(&mut parts.headers);
        parts.headers.insert(header::HOST, target.host_header()?);
        parts
            .headers
            .insert(header::CONNECTION, HeaderValue::from_static("close"));
        let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
            .max_buf_size(MAX_HEADER_BYTES)
            .handshake(TokioIo::new(upstream))
            .await
            .map_err(|_| UPSTREAM)?;
        let lifetime = lifetime.clone();
        self.tasks.spawn(async move {
            tokio::select! {
                _ = lifetime.cancelled() => {},
                _ = connection => {},
            }
        });
        let mut response = sender
            .send_request(Request::from_parts(parts, body))
            .await
            .map_err(|_| UPSTREAM)?;
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            return Err(UPSTREAM);
        }
        strip_hop_headers(response.headers_mut());
        Ok(response.map(BodyExt::boxed_unsync))
    }

    fn effect(&self, host: &str) -> Effect {
        self.registry
            .policy
            .iter()
            .map(|policy| policy.decide(Action::Network, host, Zone::Workspace))
            .max()
            .unwrap_or(Effect::Deny)
    }

    async fn dial(
        &self,
        host: &str,
        port: u16,
        cancel: &CancellationToken,
    ) -> Result<TcpStream, Failure> {
        self.authorize(self.effect(host), host, cancel).await?;

        let addresses = match host.parse::<IpAddr>() {
            Ok(ip) => vec![SocketAddr::new(ip, port)],
            Err(_) => {
                // A fully qualified DNS query avoids resolver search-domain aliases.
                tokio::time::timeout(MAX_DIAL_TIME, lookup_host((format!("{host}."), port)))
                    .await
                    .map_err(|_| UPSTREAM)?
                    .map_err(|_| UPSTREAM)?
                    .take(32)
                    .collect()
            }
        };
        let mut permitted = Vec::new();
        for address in addresses {
            let ip = address.ip().to_canonical();
            if excluded_address(ip, port, self.host_tcp_port) {
                continue;
            }
            let (restriction, exact_grant) = self
                .registry
                .policy
                .iter()
                .map(|policy| policy.network_address_rules(ip))
                .fold((None, false), |(effect, grant), (next, exact)| {
                    (effect.max(next), grant || exact)
                });
            if (restricted_ip(ip) && !exact_grant) || restriction == Some(Effect::Deny) {
                continue;
            }
            if restriction == Some(Effect::Ask) && host.parse::<IpAddr>().is_err() {
                self.authorize(Effect::Ask, &ip.to_string(), cancel).await?;
            }
            permitted.push(address);
        }
        if permitted.is_empty() {
            return Err(DENIED);
        }
        // Connect directly to the checked addresses so DNS cannot change
        // between policy evaluation and connection establishment.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(DENIED),
            result = tokio::time::timeout(MAX_DIAL_TIME, TcpStream::connect(permitted.as_slice())) => {
                result.map_err(|_| UPSTREAM)?.map_err(|_| UPSTREAM)
            }
        }
    }

    async fn authorize(
        &self,
        effect: Effect,
        host: &str,
        cancel: &CancellationToken,
    ) -> Result<(), Failure> {
        if effect != Effect::Ask || !self.registry.has_authorizer() {
            return self
                .registry
                .authorize(effect, &self.call, Approval::Network(host), cancel)
                .await
                .map_err(|_| DENIED);
        }
        // One prompt per canonical host and a hard cap per invocation. Mark
        // it refused before opening the ticket, including on disconnect.
        let mut decisions = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(DENIED),
            guard = self.approvals.clone().lock_owned() => guard,
        };
        if let Some(&allowed) = decisions.get(host) {
            return allowed.then_some(()).ok_or(DENIED);
        }
        if decisions.len() >= MAX_CONNECTIONS {
            return Err(DENIED);
        }
        decisions.insert(host.to_owned(), false);
        let (registry, call, host, cancel) = (
            self.registry.clone(),
            self.call.clone(),
            host.to_owned(),
            cancel.clone(),
        );
        let pause = self.clock.pause();
        // Dropping a connection cancels its token. Keep the broker's future
        // alive until it has withdrawn the ticket and released the clocks.
        self.tasks
            .spawn(async move {
                let result = registry
                    .authorize(Effect::Ask, &call, Approval::Network(&host), &cancel)
                    .await;
                decisions.insert(host, result.is_ok());
                drop(pause);
                result
            })
            .await
            .map_err(|_| DENIED)?
            .map_err(|_| DENIED)
    }
}

fn destination(host: &str) -> Option<String> {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    normalize_network_host(host)
}

fn excluded_address(ip: IpAddr, port: u16, host_tcp_port: Option<u16>) -> bool {
    let ip = ip.to_canonical();
    ip.is_unspecified() || (ip.is_loopback() && host_tcp_port == Some(port))
}

fn restricted_ip(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || matches!(a, 0 | 240..=255)
                || (a == 100 && (64..=127).contains(&b))
        }
        IpAddr::V6(ip) => {
            let [first, second, ..] = ip.segments();
            // Default to global unicast. Require an explicit grant for
            // transition prefixes that can embed another IP destination.
            first & 0xe000 != 0x2000 || first == 0x2002 || (first == 0x2001 && second == 0)
        }
    }
}

fn strip_hop_headers(headers: &mut HeaderMap) {
    let named: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| header::HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect();
    // Content-Length is end-to-end framing and Hyper has already validated
    // it. Never allow Connection to remove framing from a streaming body.
    for name in named
        .into_iter()
        .filter(|name| *name != header::CONTENT_LENGTH)
    {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

/// One shared wire-byte budget across all accepted connections and tunnels.
struct Metered<T> {
    io: T,
    bytes: Arc<AtomicUsize>,
}

impl<T> Metered<T> {
    fn reserve(&self, count: usize) -> io::Result<usize> {
        let previous = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n != 0).then(|| n.saturating_sub(count))
            })
            .map_err(|_| io::Error::other("Argentic proxy byte budget exceeded"))?;
        Ok(previous.min(count))
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Metered<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let reserved = self.reserve(buf.remaining())?;
        let mut portion = ReadBuf::new(buf.initialize_unfilled_to(reserved));
        let result = Pin::new(&mut self.io).poll_read(cx, &mut portion);
        let read = portion.filled().len();
        self.bytes.fetch_add(reserved - read, Ordering::Relaxed);
        buf.advance(read);
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Metered<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let reserved = self.reserve(buf.len())?;
        let result = Pin::new(&mut self.io).poll_write(cx, &buf[..reserved]);
        let written = match result {
            Poll::Ready(Ok(n)) => n,
            _ => 0,
        };
        self.bytes.fetch_add(reserved - written, Ordering::Relaxed);
        result
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn unspecified_addresses_cannot_be_granted() {
        for address in ["0.0.0.0", "::", "::0.0.0.0", "::ffff:0.0.0.0"] {
            assert!(excluded_address(address.parse().unwrap(), 443, None));
        }
    }

    #[test]
    fn only_a_host_tcp_listener_excludes_its_port() {
        let loopback = std::net::Ipv4Addr::LOCALHOST.into();
        assert!(!excluded_address(loopback, 18999, None));
        assert!(excluded_address(loopback, 18999, Some(18999)));
        assert!(!excluded_address(loopback, 18998, Some(18999)));
    }
}
