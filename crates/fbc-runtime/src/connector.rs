//! The one connector: every connection the runtime opens, WebSocket or HTTP, goes through
//! [`Connector::connect`], so the proxy applies to all of them (0002).

use tokio::net::TcpStream;

use crate::error::{NetError, Step, io};
use crate::socks5;

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

/// Opens TCP connections under one [`ProxyConfig`], and the WebSocket and HTTP/1.1 exchanges
/// on them ([`Connector::websocket`], [`Connector::http`]).
#[derive(Debug, Clone)]
pub struct Connector {
    proxy: ProxyConfig,
}

impl Connector {
    pub fn new(proxy: ProxyConfig) -> Self {
        Connector { proxy }
    }

    pub fn proxy(&self) -> &ProxyConfig {
        &self.proxy
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
}
