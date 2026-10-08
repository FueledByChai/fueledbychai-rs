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
//! never waits on it (FBC-f3w, decision 0006). [`MdReplay`] feeds one session's journal back
//! to the venue's current decoders through the same calls, with the recorded stamps, so a
//! recorded day rebuilds the same envelopes (decoder replay, FBC-3kz, design §10.1); the caller
//! supplies the configuration and spec table the session ran with.
//!
//! [`MdBooks`] keeps one book per (instrument, book channel) from the events a session hands on,
//! routing each book event by its `BookId`, and exposes each instrument's configured trading
//! book ([`TradingBooks`]); [`BookKeeper`] is the [`MdHandler`] that feeds it and then the
//! consumer's [`BookHandler`] (FBC-nij).
//!
//! A [`RateLimiter`] keeps a bucket per declared rate limit and scope key, charged by each
//! frame's, HTTP request's and connection attempt's rate charge, with a [`SafetyReserve`] that
//! normal traffic stops at, and counts refusals and a venue's 429 and 418 by scope (FBC-bel,
//! decision 0030); every session charges it before it writes, asks or connects.
//!
//! Every connection reads through a [`Tcp`] stream that, on Linux, turns on `SO_TIMESTAMPNS` and
//! keeps the kernel receive time of what it reads, beneath TLS and WebSocket, so each frame's
//! stamp carries its `kernel_rx`; a session reports each Safety-class write attributed to a
//! frame with one as a [`TickToWire`], to its handler (FBC-2y3, decision 0031). Elsewhere
//! `kernel_rx` is `None` and nothing is reported.
//!
//! A socket endpoint stays alive by itself (FBC-djl, decision 0033): its codec's keepalive goes
//! out at the declared interval, its connection rotates to the next epoch the consumer's
//! [`Liveness`] margin before the venue's `max_conn_lifetime`, and a stream that hears nothing
//! within the consumer's silence window is reported stale and reconnected under a new epoch.
//!
//! A session's socket write waits on a peer that stopped reading for at most the consumer's
//! [`WriteStall`] window, then ends the epoch as a drop and reconnects through the pacing; the
//! session's timers fire meanwhile (FBC-ha3, decision 0036).
//!
//! [`ExecSession`] drives one account's order-entry connection as
//! [`VenueFactory::plan_exec`](fbc_core::VenueFactory::plan_exec) plans it (FBC-oaz, decision
//! 0053): one [`ExecCodec`](fbc_core::ExecCodec) for the session's life, its `on_open` called on
//! each new epoch with exactly the nonces it asks for, frames decoded inside the venue's decode
//! scope, every event stamped and handed to the consumer's [`ExecHandler`] inline, and reconnects
//! paced by the consumer's [`ReconnectPacing`]. It takes only a venue whose cancel-on-disconnect
//! is per connection, and on every epoch arms it and resyncs before it places or amends
//! (FBC-w19, decision 0058). Its journal comes with a later ticket.
//!
//! Not here yet: client certificates and certificate pinning.
//! Apart from a codec's HTTP request (its own timeout) and a session's connection attempts and
//! socket writes (the consumer's attempt deadline and write-stall window), no call here has a
//! deadline of its own; the caller bounds one with its own timer.

mod books;
mod connector;
mod epoch;
mod error;
mod exec_gate;
mod exec_orders;
mod exec_session;
pub mod http;
mod journal;
mod liveness;
mod md_venue;
mod pacing;
mod ratelimit;
mod reconcile;
mod replay;
mod session;
mod session_core;
mod socks5;
mod stall;
mod target;
mod tcp;
mod tls;
mod transport;
pub mod ws;

/// The conformance toy, whose decode scope builds the venue ids the unit tests need. Its path
/// goes through `tests/`, as the integration tests' include of it does, so the coverage report
/// leaves it out as it leaves theirs (the toy is measured in fbc-conformance).
#[cfg(test)]
#[path = "../tests/../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod toy;

// Decision 0079: a build that links this crate never has the log facade's TRACE level, since
// tungstenite writes each frame, the auth frame's session token included, at TRACE. This crate
// sets `max_level_debug`, but in a build without debug assertions `log` takes a consumer's
// `release_max_level_*` first, so `release_max_level_trace` anywhere in the build would bring
// TRACE back in release; that build fails here instead.
const _: () = assert!(
    (log::STATIC_MAX_LEVEL as usize) <= (log::LevelFilter::Debug as usize),
    "decision 0079: TRACE must stay compiled out of the log facade in every build that links \
     fbc-runtime (tungstenite logs each frame, the session token included, at TRACE); remove \
     log's release_max_level_trace feature from this build"
);

pub use books::{BookHandler, BookKeeper, MdBooks, TradingBookConflict, TradingBooks};
pub use connector::{Connector, ProxyConfig};
pub use epoch::{Admit, EpochError, Epochs, Input};
pub use error::{Cause, NetError, Step};
pub use exec_orders::{ExecOrders, RpcIds, SubmitRefusal};
pub use exec_session::{
    ExecControl, ExecCounters, ExecHandler, ExecSession, ExecSessionConfig, ExecSessionError,
};
pub use journal::Journal;
pub use liveness::{Liveness, LivenessError};
pub use md_venue::{MdVenue, MdVenueConfig, MdVenueControl, PlanError};
pub use pacing::{PacingError, ReconnectPacing};
pub use ratelimit::{
    BucketKey, Charged, RateCounts, RateError, RateLimiter, Refused, Request, SafetyReserve,
    ScopeCounts,
};
pub use reconcile::{ReconcileError, Reconciler, SubscribeCall};
pub use replay::{MdReplay, MdReplayConfig, MdReplayCounters, ReplayError};
pub use session::{
    MdControl, MdCounters, MdHandler, MdSession, MdSessionConfig, Outbox, SessionError,
};
pub use session_core::{IngestClock, TickToWire};
pub use stall::{WriteStall, WriteStallError};
pub use tcp::Tcp;
pub use transport::Transport;
