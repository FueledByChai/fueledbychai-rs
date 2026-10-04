//! The one generic runtime (decision 0002): the only code in this workspace that opens a
//! network connection, so the SOCKS5 proxy the consumer configures applies to all of them.
//!
//! This first slice (FBC-2ds) is the connector and the two protocols on it:
//!
//! - [`ProxyConfig`], supplied by the consumer: [`ProxyConfig::Direct`] or
//!   [`ProxyConfig::Socks5`] with a host and port. There is no default and no proxy address in
//!   code.
//! - [`Connector`], which opens TCP directly or through a SOCKS5 CONNECT (RFC 1928, the
//!   no-authentication method only). The CONNECT names a target host by name (address type
//!   DOMAINNAME), so the proxy resolves it, as `socks5h` does, and the process never resolves a
//!   venue host itself; an IP literal goes as an IP address.
//! - A WebSocket client for `ws://` URLs ([`Connector::websocket`], [`ws`]) and an HTTP/1.1
//!   call for `http://` URLs ([`Connector::http`], [`http`]), both over that connector; an
//!   HTTP call reads at most the response-body bytes its caller allows.
//!
//! Every failure is a [`NetError`] that names the [`Step`] that failed and never quotes a
//! URL, so a URL's user information or query cannot reach a log through it (0014 item 8).
//!
//! Not here yet: TLS (`wss://`, `https://`), connection epochs, sessions and reconnects. No
//! call here has a deadline of its own; the caller bounds one with its own timer.

mod connector;
mod error;
pub mod http;
mod socks5;
mod target;
pub mod ws;

pub use connector::{Connector, ProxyConfig};
pub use error::{Cause, NetError, Step};
