//! The one generic runtime (decision 0002): the only code in this workspace that opens a
//! network connection, so the SOCKS5 proxy the consumer configures applies to all of them.
//!
//! So far it is the connector and the two protocols on it, plain (FBC-2ds) or TLS (FBC-27a):
//!
//! - [`ProxyConfig`], supplied by the consumer: [`ProxyConfig::Direct`] or
//!   [`ProxyConfig::Socks5`] with a host and port. There is no default and no proxy address in
//!   code.
//! - [`Connector`], which opens TCP directly or through a SOCKS5 CONNECT (RFC 1928, the
//!   no-authentication method only). The CONNECT names a target host by name (address type
//!   DOMAINNAME), so the proxy resolves it, as `socks5h` does, and the process never resolves a
//!   venue host itself; an IP literal goes as an IP address.
//! - TLS for `wss://` and `https://` over the stream that connector opened, so the proxy applies
//!   to encrypted traffic too: rustls with the ring provider, trusting webpki-roots plus any
//!   anchor the consumer adds ([`Connector::add_trust_anchor`]), with hostname verification
//!   always on and the target's host name as the server name (SNI); see [`Transport`].
//! - A WebSocket client for `ws://` and `wss://` URLs ([`Connector::websocket`], [`ws`]) and an
//!   HTTP/1.1 call for `http://` and `https://` URLs ([`Connector::http`], [`http`]), both over
//!   that connector; an HTTP call reads at most the response-body bytes its caller allows.
//!
//! Every failure is a [`NetError`] that names the [`Step`] that failed and never quotes a
//! URL, so a URL's user information or query cannot reach a log through it (0014 item 8).
//!
//! Not here yet: connection epochs, sessions and reconnects, client certificates and
//! certificate pinning. No call here has a deadline of its own; the caller bounds one with its
//! own timer.

mod connector;
mod error;
pub mod http;
mod socks5;
mod target;
mod tls;
mod transport;
pub mod ws;

pub use connector::{Connector, ProxyConfig};
pub use error::{Cause, NetError, Step};
pub use transport::Transport;
