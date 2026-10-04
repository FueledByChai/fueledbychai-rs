//! TLS for `wss://` and `https://`: rustls with the ring crypto provider (0020) over the stream
//! the connector opened, directly or through the SOCKS5 proxy, so the proxy applies to
//! encrypted traffic exactly as to plain (0002).
//!
//! A server's certificate must chain to a trust anchor: webpki-roots (Mozilla's roots) plus any
//! the consumer adds ([`crate::Connector::add_trust_anchor`]). Hostname verification is always
//! on, and the server name sent (SNI) is the target's host name, never the proxy's.

use std::io;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::error::{Cause, NetError, Step};

/// The anchors a server's certificate must chain to, and the client configuration built from
/// them; rebuilt whenever the consumer adds an anchor, never per connection.
#[derive(Clone)]
pub(crate) struct Trust {
    roots: RootCertStore,
    added: usize,
    config: Arc<ClientConfig>,
}

impl Trust {
    /// webpki-roots and nothing else.
    pub(crate) fn webpki() -> Self {
        let roots = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };
        Trust {
            config: client_config(roots.clone()),
            roots,
            added: 0,
        }
    }

    /// Adds one DER-encoded certificate as a trust anchor.
    pub(crate) fn add(&mut self, der: &[u8]) -> Result<(), NetError> {
        self.roots
            .add(CertificateDer::from(der.to_vec()))
            .map_err(|e| NetError::new(Step::TlsTrust, Cause::Detail(e.to_string())))?;
        self.added += 1;
        self.config = client_config(self.roots.clone());
        Ok(())
    }

    /// How many anchors the consumer added.
    pub(crate) fn added(&self) -> usize {
        self.added
    }

    /// The TLS client handshake as `name` over `stream`, verifying the certificate's chain and
    /// that it names `name`.
    pub(crate) async fn handshake(
        &self,
        name: ServerName<'static>,
        stream: TcpStream,
    ) -> Result<TlsStream<TcpStream>, NetError> {
        TlsConnector::from(self.config.clone())
            .connect(name, stream)
            .await
            .map_err(handshake_error)
    }
}

fn client_config(roots: RootCertStore) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports rustls' default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

/// The name the handshake sends and verifies: a DNS name, or an IP literal (sent without SNI,
/// as RFC 6066 requires, and checked against the certificate's IP addresses).
pub(crate) fn server_name(host: &str) -> Result<ServerName<'static>, NetError> {
    ServerName::try_from(host.to_owned()).map_err(|_| {
        NetError::protocol(
            Step::TlsHandshake,
            "the host is not a valid TLS server name",
        )
    })
}

/// A handshake failure: rustls' own description (a refused certificate, an alert, bytes that
/// are not TLS) when there is one, otherwise the I/O error's kind.
fn handshake_error(e: io::Error) -> NetError {
    let cause = match e.get_ref().and_then(|e| e.downcast_ref::<rustls::Error>()) {
        Some(tls) => Cause::Detail(tls.to_string()),
        None => Cause::Io(e.kind()),
    };
    NetError::new(Step::TlsHandshake, cause)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dns_name_or_an_ip_literal_is_a_server_name() {
        assert!(matches!(
            server_name("api.venue.test"),
            Ok(ServerName::DnsName(_))
        ));
        assert!(matches!(server_name("::1"), Ok(ServerName::IpAddress(_))));
        let err = server_name("a..b").unwrap_err();
        assert_eq!(
            err.to_string(),
            "TLS handshake failed: the host is not a valid TLS server name"
        );
    }

    #[test]
    fn the_trust_counts_only_the_anchors_the_consumer_added() {
        let trust = Trust::webpki();
        assert_eq!(trust.added(), 0);
        assert_eq!(trust.roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
    }
}
