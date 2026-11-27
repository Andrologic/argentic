use base64::{engine::general_purpose::STANDARD, Engine};
use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair};
use rustls::{
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
    ClientConfig, RootCertStore, ServerConfig,
};
use std::{io, net::IpAddr, path::Path, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{server::TlsStream, LazyConfigAcceptor, TlsConnector};

pub(super) struct Intercept {
    issuer: CertifiedIssuer<'static, KeyPair>,
    connector: TlsConnector,
}

impl Intercept {
    pub(super) fn new(bundle: &Path) -> io::Result<Self> {
        let roots = rustls_native_certs::load_native_certs();
        let mut pem = String::new();
        for cert in &roots.certs {
            pem.push_str("-----BEGIN CERTIFICATE-----\n");
            let encoded = STANDARD.encode(cert);
            for line in encoded.as_bytes().chunks(64) {
                pem.push_str(&String::from_utf8_lossy(line));
                pem.push('\n');
            }
            pem.push_str("-----END CERTIFICATE-----\n");
        }
        let intercept = Self::with_roots(roots.certs)?;
        pem.push_str(&intercept.issuer.pem());
        std::fs::write(bundle, pem)?;
        Ok(intercept)
    }

    fn with_roots(certs: Vec<CertificateDer<'static>>) -> io::Result<Self> {
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(certs);
        if roots.is_empty() {
            return Err(io::Error::other(
                "Argentic TLS interception needs system trust roots",
            ));
        }
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params
            .distinguished_name
            .push(DnType::CommonName, "Argentic shell invocation");
        let issuer =
            CertifiedIssuer::self_signed(params, KeyPair::generate().map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(io::Error::other)?
                .with_root_certificates(roots)
                .with_no_client_auth();
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Self {
            issuer,
            connector: TlsConnector::from(Arc::new(config)),
        })
    }

    pub(super) async fn accept<T: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: T,
        host: &str,
    ) -> io::Result<TlsStream<T>> {
        let start = LazyConfigAcceptor::new(rustls::server::Acceptor::default(), stream).await?;
        let hello = start.client_hello();
        let sni = hello.server_name();
        // TLS clients omit SNI for an IP literal. The leaf then binds the
        // literal IP, and the HTTP authority still has to match CONNECT.
        let matches = match sni {
            Some(name) => {
                super::destination(name).as_deref() == Some(host) && host.parse::<IpAddr>().is_err()
            }
            None => host.parse::<IpAddr>().is_ok(),
        };
        if !matches {
            return Err(io::Error::other(
                "TLS server name differs from CONNECT host",
            ));
        }
        start.into_stream(self.server_config(host)?).await
    }

    fn server_config(&self, host: &str) -> io::Result<Arc<ServerConfig>> {
        let key = KeyPair::generate().map_err(io::Error::other)?;
        let leaf = CertificateParams::new(vec![host.to_owned()])
            .and_then(|params| params.signed_by(&key, &self.issuer))
            .map_err(io::Error::other)?;
        let mut config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(io::Error::other)?
                .with_no_client_auth()
                .with_single_cert(
                    vec![leaf.der().clone()],
                    PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
                )
                .map_err(io::Error::other)?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Arc::new(config))
    }

    pub(super) async fn connect<T: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: T,
        host: &str,
    ) -> io::Result<tokio_rustls::client::TlsStream<T>> {
        let name = ServerName::try_from(host.to_owned()).map_err(io::Error::other)?;
        self.connector.connect(name, stream).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        policy::{Action, Effect, Policy, Preset, Rule},
        providers::ToolCall,
        tools::Registry,
    };
    use std::sync::atomic::AtomicUsize;
    use tokio::{
        io::{duplex, AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::Mutex,
    };
    use tokio_rustls::TlsAcceptor;
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    fn authority() -> CertificateDer<'static> {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .self_signed(&KeyPair::generate().unwrap())
            .unwrap()
            .der()
            .clone()
    }

    #[tokio::test]
    async fn connect_refuses_a_different_sni() {
        let tls = Intercept::with_roots(vec![authority()]).unwrap();
        let client_tls = Intercept::with_roots(vec![tls.issuer.der().clone()]).unwrap();
        let (client, server) = duplex(8192);
        let (client, server) = tokio::join!(
            client_tls.connect(client, "different.example"),
            tls.accept(server, "allowed.example"),
        );
        assert!(client.is_err());
        assert_eq!(
            server.unwrap_err().to_string(),
            "TLS server name differs from CONNECT host"
        );
    }

    #[tokio::test]
    async fn reoriginated_tls_checks_the_upstream_certificate() {
        let upstream = Intercept::with_roots(vec![authority()]).unwrap();
        let (client, server) = duplex(8192);
        let (client, _) = tokio::join!(
            upstream.connect(client, "allowed.example"),
            upstream.accept(server, "allowed.example"),
        );
        assert!(
            client.is_err(),
            "Untrusted upstream certificate was accepted"
        );
    }

    #[tokio::test]
    async fn intercept_checks_each_http_authority_and_forwards_verified_https() {
        let upstream = Intercept::with_roots(vec![authority()]).unwrap();
        let tls = Intercept::with_roots(vec![upstream.issuer.der().clone()]).unwrap();
        let client_tls = Intercept::with_roots(vec![tls.issuer.der().clone()]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let work = tempfile::tempdir().unwrap();
        let policy = Policy::compile_rules(
            work.path(),
            None,
            Some(Preset::Workspace),
            None,
            &[Rule {
                action: Action::Network,
                pattern: Some("127.0.0.1".into()),
                effect: Effect::Allow,
            }],
        )
        .unwrap();
        let registry =
            Registry::new(&[work.path().to_path_buf()], policy, serde_json::json!({})).unwrap();
        let context = Arc::new(super::super::Egress {
            registry: Arc::new(registry),
            call: ToolCall {
                id: "test".into(),
                name: "shell".into(),
                arguments: "{}".into(),
            },
            cancel: CancellationToken::new(),
            auth: "Basic test".into(),
            host_tcp_port: None,
            bytes: Arc::new(AtomicUsize::new(super::super::MAX_BYTES)),
            approvals: Arc::new(Mutex::default()),
            clock: Arc::new(super::super::Clock::new()),
            tls: Some(tls),
            tasks: TaskTracker::new(),
        });
        let upstream_task = tokio::spawn(async move {
            let (server, _) = listener.accept().await.unwrap();
            let mut stream = TlsAcceptor::from(upstream.server_config("127.0.0.1").unwrap())
                .accept(server)
                .await
                .unwrap();
            let header = read_head(&mut stream).await;
            assert!(header.starts_with("GET /accepted HTTP/1.1"));
            stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: https://blocked.example/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            stream.shutdown().await.unwrap();
        });
        let (mut client, server) = duplex(8192);
        let proxy = tokio::spawn(super::super::serve(server, context));
        client
            .write_all(
                format!("CONNECT {address} HTTP/1.1\r\nProxy-Authorization: Basic test\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        assert!(read_head(&mut client).await.starts_with("HTTP/1.1 200"));
        let mut client = client_tls.connect(client, "127.0.0.1").await.unwrap();
        assert_eq!(
            client.get_ref().1.alpn_protocol(),
            Some(b"http/1.1".as_slice())
        );
        for host in [
            format!("blocked.example:{}", address.port()),
            format!("{address}\r\nHost: blocked.example"),
        ] {
            client
                .write_all(format!("GET /rejected HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes())
                .await
                .unwrap();
            let response = read_head(&mut client).await;
            assert!(response.starts_with("HTTP/1.1 403"), "{response}");
            let mut body = vec![0; "Argentic network policy denied this destination\n".len()];
            client.read_exact(&mut body).await.unwrap();
        }
        client
            .write_all(
                format!("GET /accepted HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let response = read_head(&mut client).await;
        assert!(
            response.starts_with("HTTP/1.1 302") && response.contains("https://blocked.example/")
        );
        upstream_task.await.unwrap();
        proxy.await.unwrap();
    }

    async fn read_head(stream: &mut (impl AsyncRead + Unpin)) -> String {
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            header.push(stream.read_u8().await.unwrap());
        }
        String::from_utf8(header).unwrap()
    }
}
