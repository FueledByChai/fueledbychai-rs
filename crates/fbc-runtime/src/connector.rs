//! The one connector: every connection the runtime opens, WebSocket or HTTP, plain or TLS,
//! goes through [`Connector::connect`], so the proxy applies to all of them (0002).

use std::fmt;

use tokio::net::TcpStream;

use crate::error::{NetError, Step, io};
use crate::socks5;
use crate::target::Target;
use crate::tcp::Tcp;
use crate::tls::{self, Trust};
use crate::transport::Transport;

/// How the runtime reaches the network, chosen by the consumer per process. There is no
/// default: the deployment box connects directly and the owner's laptop through a SOCKS5 host
/// and port, with the same binary (0002).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyConfig {
    /// Open TCP straight to each target, resolving its name locally.
    Direct,
    /// Open TCP to this SOCKS5 proxy and CONNECT through it; the proxy resolves target names.
    /// No authentication: the proxy is a host and port only.
    Socks5 { host: String, port: u16 },
}

/// Opens TCP connections under one [`ProxyConfig`], TLS over them for `wss://` and
/// `https://`, and the WebSocket and HTTP/1.1 exchanges on them ([`Connector::websocket`],
/// [`Connector::http`]).
#[derive(Clone)]
pub struct Connector {
    proxy: ProxyConfig,
    trust: Trust,
}

impl Connector {
    /// A connector under `proxy` that trusts the webpki-roots anchors (Mozilla's roots) for TLS.
    pub fn new(proxy: ProxyConfig) -> Self {
        Connector {
            proxy,
            trust: Trust::webpki(),
        }
    }

    pub fn proxy(&self) -> &ProxyConfig {
        &self.proxy
    }

    /// Adds a DER-encoded certificate as a TLS trust anchor, beside webpki-roots, for every
    /// later `wss://` and `https://` connection; a certificate that cannot be one fails at
    /// [`Step::TlsTrust`]. Hostname verification stays on whatever the anchors are.
    pub fn add_trust_anchor(&mut self, der: &[u8]) -> Result<(), NetError> {
        self.trust.add(der)
    }

    /// Opens `to` through [`Connector::connect`], with the TLS handshake on top when `to` asks
    /// for TLS. The server name is checked before any connection opens. The stream keeps the
    /// kernel receive time of what it reads, on Linux ([`Tcp`]).
    pub(crate) async fn open(&self, to: &Target) -> Result<Transport, NetError> {
        if !to.tls {
            let stream = self.connect(&to.host, to.port).await?;
            return Ok(Transport::Plain(Tcp::new(stream)));
        }
        let name = tls::server_name(&to.host)?;
        let stream = Tcp::new(self.connect(&to.host, to.port).await?);
        let stream = self.trust.handshake(name, stream).await?;
        Ok(Transport::Tls(Box::new(stream)))
    }

    /// A TCP stream to `host:port`, directly or through the proxy, with Nagle's algorithm off.
    /// Through the proxy, `host` is never resolved here: the CONNECT carries the name.
    pub async fn connect(&self, host: &str, port: u16) -> Result<TcpStream, NetError> {
        match &self.proxy {
            ProxyConfig::Direct => tcp(host, port, Step::TargetTcp).await,
            ProxyConfig::Socks5 {
                host: proxy_host,
                port: proxy_port,
            } => {
                let request = socks5::connect_request(host, port)?;
                let mut stream = tcp(proxy_host, *proxy_port, Step::ProxyTcp).await?;
                socks5::handshake(&mut stream, &request).await?;
                Ok(stream)
            }
        }
    }
}

/// The proxy and how many trust anchors the consumer added; never the anchors themselves.
impl fmt::Debug for Connector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connector")
            .field("proxy", &self.proxy)
            .field("added_trust_anchors", &self.trust.added())
            .finish()
    }
}

/// TCP to `host:port` with Nagle's algorithm off; a failure is reported at `step`.
async fn tcp(host: &str, port: u16, step: Step) -> Result<TcpStream, NetError> {
    let stream = TcpStream::connect((host, port)).await.map_err(io(step))?;
    stream.set_nodelay(true).map_err(io(step))?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_connector_keeps_the_consumers_proxy() {
        let socks = ProxyConfig::Socks5 {
            host: "proxy.test".into(),
            port: 1080,
        };
        assert_eq!(Connector::new(socks.clone()).proxy(), &socks);
        assert_eq!(
            Connector::new(ProxyConfig::Direct).proxy(),
            &ProxyConfig::Direct
        );
    }

    #[test]
    fn debug_shows_the_proxy_and_the_count_of_added_anchors() {
        assert_eq!(
            format!("{:?}", Connector::new(ProxyConfig::Direct)),
            "Connector { proxy: Direct, added_trust_anchors: 0 }"
        );
    }
}
