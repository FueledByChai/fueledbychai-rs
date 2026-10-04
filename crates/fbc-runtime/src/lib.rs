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
//! The logic of reconnects (FBC-5pt) has no socket, so every case of it is a
//! unit test:
//!
//! - [`Epochs`] numbers the lives of one stream's connection (a close or a reconnect opens
//!   the next epoch) and drops, counting per [`Input`], a frame, timer firing, HTTP result or
//!   event of an older epoch, so a dead connection's late arrivals never reach the current
//!   codec.
//! - [`Reconciler`] yields, for one stream, only the difference between the subscriptions
//!   wanted and those the current epoch has sent ([`SubscribeCall`]): an active subscription
//!   is never sent twice, a new epoch subscribes the desired set once, and one that could not
//!   be sent stays pending until it is.
//!
//! [`MdSession`] drives one market-data endpoint of a venue's plan over these (FBC-ku8): a fresh
//! codec per epoch, events stamped into envelopes and handed to the consumer's [`MdHandler`] in
//! ingest order, the codec's effects executed, and reconnects paced by the consumer's
//! [`ReconnectPacing`] (decision 0023). The HTTP requests a codec asks for run with their
//! timeouts and come back only to the epoch that asked, and a poll endpoint opens no connection
//! (FBC-klr, decision 0027). [`MdVenue`] runs a venue's endpoints as
//! [`VenueFactory::plan_md`](fbc_core::VenueFactory::plan_md) spreads the desired subscriptions
//! over them, opening, keeping and closing endpoints as the plan changes. With a [`Journal`]
//! set on either, every session records what crosses the shard boundary into the consumer's
//! [`fbc_journal::JournalSink`], each record under the traffic class of what it records, and
//! never waits on it (FBC-f3w, decision 0006).
//!
//! [`MdBooks`] keeps one book per (instrument, book channel) from the events a session hands on,
//! routing each book event by its `BookId`, and exposes each instrument's configured trading
//! book ([`TradingBooks`]); [`BookKeeper`] is the [`MdHandler`] that feeds it and then the
//! consumer's [`BookHandler`] (FBC-nij).
//!
//! Not here yet: order-entry sessions, keepalives, client certificates and certificate pinning.
//! Apart from a codec's HTTP request, no call here has a deadline of its own; the caller bounds
//! one with its own timer.

mod books;
mod connector;
mod epoch;
mod error;
pub mod http;
mod journal;
mod md_venue;
mod pacing;
mod reconcile;
mod session;
mod socks5;
mod target;
mod tls;
mod transport;
pub mod ws;

pub use books::{BookHandler, BookKeeper, MdBooks, TradingBookConflict, TradingBooks};
pub use connector::{Connector, ProxyConfig};
pub use epoch::{Admit, EpochError, Epochs, Input};
pub use error::{Cause, NetError, Step};
pub use journal::Journal;
pub use md_venue::{MdVenue, MdVenueConfig, MdVenueControl, PlanError};
pub use pacing::{PacingError, ReconnectPacing};
pub use reconcile::{ReconcileError, Reconciler, SubscribeCall};
pub use session::{
    IngestClock, MdControl, MdCounters, MdHandler, MdSession, MdSessionConfig, SessionError,
};
pub use transport::Transport;
