//! Builds an authorized, DNS-pinned Streamable HTTP connection.
use crate::{
    agsdl_host_calls::{raw_mcp, CallError, Control, Result},
    host::{HostContext, McpHttpSecret},
    persistence::dto::EngineSourceRevision,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION};
use std::{collections::BTreeSet, net::SocketAddr};
use url::{Host, Url};

pub(crate) fn endpoint(text: &str) -> Result<Url> {
    let invalid = || {
        CallError::new(
            "MCP_ENDPOINT_UNSUPPORTED",
            "MCP endpoints require HTTPS or loopback HTTP, without userinfo or a fragment",
        )
    };
    let authority = text
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(""));
    if authority.is_none_or(|authority| authority.is_empty() || authority.contains('@'))
        || text
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ' || byte == b'\\')
    {
        return Err(invalid());
    }
    let url = Url::parse(text).map_err(|_| invalid())?;
    let (scheme, host) = (url.scheme(), url.host());
    let supported = match (scheme, host) {
        ("https", Some(_)) | ("http", Some(Host::Domain("localhost"))) => true,
        ("http", Some(Host::Ipv4(ip))) => ip.is_loopback(),
        ("http", Some(Host::Ipv6(ip))) => ip.is_loopback(),
        _ => false,
    };
    if !supported
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default() == Some(0)
    {
        return Err(invalid());
    }
    Ok(url)
}

pub(crate) struct RemoteMcp {
    pub config: HostContext,
    pub source: EngineSourceRevision,
    pub server: String,
    pub url: Url,
    pub names: Vec<String>,
}

impl RemoteMcp {
    pub async fn start(self, control: &Control) -> Result<raw_mcp::Session> {
        let unauthorized = || {
            CallError::new(
                "MCP_ENDPOINT_REFUSED",
                "Host consent for the MCP endpoint is unavailable",
            )
        };
        let authorize = self.config.mcp_http().ok_or_else(unauthorized)?;
        let port = self
            .url
            .port_or_known_default()
            .expect("qualified endpoint");
        let addresses = match self.url.host().expect("qualified endpoint") {
            Host::Ipv4(ip) => vec![SocketAddr::new(ip.into(), port)],
            Host::Ipv6(ip) => vec![SocketAddr::new(ip.into(), port)],
            Host::Domain(domain) => {
                let resolved = tokio::select! {
                    biased;
                    error = control.cancelled() => return Err(error),
                    result = tokio::time::timeout(raw_mcp::HTTP_CONNECT_TIMEOUT, tokio::net::lookup_host((domain, port))) => {
                        result.map_err(|_| CallError::new("MCP_ENDPOINT_UNAVAILABLE", "MCP endpoint resolution timed out"))?
                    },
                }
                .map_err(|_| {
                    CallError::new(
                        "MCP_ENDPOINT_UNAVAILABLE",
                        "Cannot resolve the MCP endpoint",
                    )
                })?;
                resolved.collect::<BTreeSet<_>>().into_iter().collect()
            }
        };
        if addresses.is_empty()
            || (self.url.scheme() == "http" && addresses.iter().any(|a| !a.ip().is_loopback()))
        {
            return Err(unauthorized());
        }
        control.check()?;
        let destinations = authorize(
            &self.source,
            &self.server,
            &self.url,
            &addresses,
            &self.names,
        )
        .ok_or_else(unauthorized)?;
        let invalid = || {
            CallError::new(
                "MCP_SECRET_HEADER",
                "MCP secret header configuration is invalid",
            )
        };
        if destinations.len() != self.names.len()
            || self
                .names
                .iter()
                .any(|name| !destinations.contains_key(name))
        {
            return Err(invalid());
        }
        let mut mapping = Vec::with_capacity(destinations.len());
        let mut used_headers = BTreeSet::new();
        for (secret, destination) in destinations {
            let header = match &destination {
                McpHttpSecret::Bearer => AUTHORIZATION,
                McpHttpSecret::Header(name) => {
                    HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid())?
                }
            };
            if !raw_mcp::can_set_header(&header) || !used_headers.insert(header.as_str().to_owned())
            {
                return Err(invalid());
            }
            mapping.push((secret, destination, header));
        }
        let secrets = super::agsdl_host_call_executor030::server_secrets(
            &self.config,
            &self.source,
            &self.server,
            &self.names,
        )?;
        let mut headers = HeaderMap::new();
        for (secret, destination, header) in mapping {
            let value = &secrets[&secret];
            let mut value = match destination {
                McpHttpSecret::Bearer if !value.is_empty() => {
                    HeaderValue::from_str(&format!("Bearer {value}"))
                }
                McpHttpSecret::Bearer => return Err(invalid()),
                McpHttpSecret::Header(_) => HeaderValue::from_str(value),
            }
            .map_err(|_| invalid())?;
            value.set_sensitive(true);
            headers.insert(header, value);
        }
        control.check()?;
        raw_mcp::start(raw_mcp::CapturedServer {
            document_name: self.server,
            capture_digest: self.source.source_sha256,
            protocol: raw_mcp::Protocol::Legacy,
            transport: raw_mcp::ServerTransport::Http(raw_mcp::HttpServer {
                url: self.url,
                addresses,
                headers,
                secret_values: secrets
                    .into_values()
                    .filter(|value| !value.is_empty())
                    .collect(),
                tool_schema: None,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{collections::BTreeMap, sync::Arc};

    #[test]
    fn endpoint_requires_tls_or_loopback_and_excludes_embedded_credentials() {
        for url in [
            "https://example.invalid/mcp",
            "http://127.0.0.1:9999/mcp",
            "http://[::1]/mcp",
            "http://localhost/mcp",
        ] {
            assert!(endpoint(url).is_ok(), "{url}");
        }
        for url in [
            "http://example.invalid/mcp",
            "https://name:password@example.invalid",
            "https://example.invalid/#fragment",
            "https:///example.invalid",
            "ssh://example.invalid",
            "https://localhost:0",
        ] {
            assert_eq!(endpoint(url).unwrap_err().code, "MCP_ENDPOINT_UNSUPPORTED");
        }
    }

    #[tokio::test]
    async fn http_refusals_happen_before_opening_a_socket() {
        let temp = tempfile::tempdir().unwrap();
        let context = crate::test_context::context(temp.path().to_owned());
        let source = serde_json::from_value::<EngineSourceRevision>(json!({
            "owner":{"projectId":"project","planId":"source","branch":"main"},"revision":"1","sourceSha256":"test"
        })).unwrap();
        let control = Control {
            stop: Default::default(),
            machine_timeout: Default::default(),
        };
        let start = |config| {
            RemoteMcp {
                config,
                source: source.clone(),
                server: "issues".into(),
                url: endpoint("http://127.0.0.1:1/mcp").unwrap(),
                names: vec!["TOKEN".into()],
            }
            .start(&control)
        };
        assert_eq!(
            start(context.clone()).await.err().unwrap().code,
            "MCP_ENDPOINT_REFUSED"
        );
        let invalid = context
            .clone()
            .with_mcp_http(Arc::new(|_, _, _, _, _| {
                Some(BTreeMap::from([(
                    "TOKEN".into(),
                    McpHttpSecret::Header("mcp-session-id".into()),
                )]))
            }))
            .with_mcp_secrets(Arc::new(|_, _, _| {
                panic!("Forbidden header reached the vault")
            }));
        assert_eq!(
            start(invalid).await.err().unwrap().code,
            "MCP_SECRET_HEADER"
        );
        let missing = context.with_mcp_http(Arc::new(|_, _, _, _, _| {
            Some(BTreeMap::from([("TOKEN".into(), McpHttpSecret::Bearer)]))
        }));
        assert_eq!(
            start(missing).await.err().unwrap().code,
            "MCP_SECRETS_UNAVAILABLE"
        );
    }
}
