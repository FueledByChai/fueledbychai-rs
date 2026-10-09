//! The adapter conformance kit (design §6): the failures that hurt the Java stack, played
//! against the runtime and the venue crates in their tests, on the loopback interface only.
//!
//! So far (FBC-jcy):
//!
//! - [`StubServer`], a stub venue on two 127.0.0.1 ephemeral ports: a WebSocket endpoint that
//!   plays a [`WsScript`] (accept, read, push, close, go silent; each step names its connection)
//!   and records every connection and data frame, and an HTTP/1.1 endpoint that answers fixed
//!   responses by path, for a venue's REST anchor. [`StubServer::played`] is the script's
//!   position, which the order-entry checks wait on instead of a count of scheduler turns
//!   (FBC-pn85).
//! - Replies computed from the request (FBC-xg7, decision 0047): a [`Step::Respond`] answers the
//!   frame it reads with what a [`Responder`] computes from it (a reply echoing its request id,
//!   a batch's outcome per item), and an [`HttpRouter`] answers by method and [`PathPattern`]
//!   (a prefix, or a template with variable segments), fixed or computed from the
//!   [`HttpRequest`]. Venue crates take this crate as a
//!   dev-dependency instead of writing servers of their own (design §6 step 12).
//! - Scripts are typed Rust values; a text format is deferred (decision 0025).
//! - [`reconnect_storm`], the storm of [`STORM_RECONNECTS`] forced reconnects, and
//!   [`check_pacing`], which holds the attempts a stub saw to the client's reconnect pacing.
//! - [`duplicate_acks`], every subscription acknowledged twice and each data frame pushed once,
//!   and [`silence_after_ack`], a connection that goes silent once its subscriptions are
//!   acknowledged, followed by the reconnect (FBC-53c).
//! - [`toy`], the conformance toy venue (decision 0044): so far its order entry, every
//!   declared order capability exercised through `ExecCodec` (FBC-7lx), its order updates,
//!   fills, request rejects and venue modes decoded through `DecodeScope` (FBC-7ce), its answers
//!   to queries, batches, resyncs and authentication (FBC-sal), [`toy::ToyFactory`], the
//!   factory the named suite builds it from (FBC-8ew), and [`toy::ToyMd`], its market-data
//!   codec: two book channels on one connection, whole-frame snapshots, refused book ids, a REST
//!   anchor, gap health and a keepalive (FBC-u1d); and its URLs from the configuration with the
//!   credential spans it marks, a resync over REST retried until decoded whole, and a ping on
//!   its order-entry connection (FBC-ja3).
//! - [`suite`], the named conformance suite (design §6, FBC-8ew): [`suite!`], which an adapter
//!   crate's `tests/conformance.rs` invokes with its factory, its fixture directory (whose
//!   layout [`suite`] documents) and the setup its fixtures assume, each named check becoming a
//!   test; and each check as a public function. So far `caps_truthful`,
//!   `commands_selfcontained`, `signing_golden` and `legacy_symbols` (FBC-onw), `fee_sign`,
//!   `liquidity_reported`, `position_signed` and `decoder_deterministic`, which read the
//!   venue's fixture frames (FBC-whw), `encode_deterministic`, `ids_roundtrip`, `restart_cid`
//!   and `price_grid` (FBC-2re), and the market-data checks `continuity`,
//!   `subscriptions_idempotent`, `no_exch_ts_synthesized` and `book_channels` (FBC-vmw); the
//!   conformance toy passes all sixteen, a deliberately broken toy fails `caps_truthful`
//!   (`tests/suite_broken.rs`), a toy whose signer changes one signed byte fails
//!   `signing_golden` (`tests/suite_golden.rs`), toys that flip a fee's sign or decode
//!   nondeterministically fail `fee_sign` and `decoder_deterministic` (`tests/suite_frames.rs`),
//!   a toy whose encode reads the real time fails `encode_deterministic`
//!   (`tests/suite_ids_grid.rs`), and a toy variant that ignores a sequence break fails
//!   `continuity` (`tests/suite_md.rs`). And the order-entry checks `amend_ack`, `mixed_batch`
//!   and `unknown_on_timeout` (FBC-3il, decision 0083), which run fbc-runtime's order-entry
//!   session against the stub, answering as the setup's [`suite::OrderEntryStub`] states, on a
//!   paused clock the check moves, every order command authorized by fbc-oms; a toy variant
//!   that reports every item of a timed-out batch `Accepted` fails `mixed_batch`
//!   (`tests/suite_orders.rs`). `mixed_batch` judges a refused and an unanswered batch item as
//!   the stub's [`suite::BatchFailures`] declares: a toy variant answering as Paradex does (a
//!   refused item and one its reply leaves out both `Unknown` from the reply) passes declared
//!   so, and fails declared as the toy (`tests/suite_batch.rs`, FBC-3pv6). A [`suite::Bootstrap`] in the setup takes every order-entry
//!   codec the suite builds to its authenticated state before a check uses it (FBC-648o): a toy
//!   variant refusing `NotSent(Disconnected)` until authenticated passes `caps_truthful`,
//!   `commands_selfcontained` and `signing_golden` with one and fails each without
//!   (`tests/suite_bootstrap.rs`).
//!
//! Not here yet: the suite's other checks, and the rest of the conformance toy venue.

mod pacing;
mod routes;
mod script;
mod server;
pub mod suite;
pub mod toy;

pub use pacing::{PacingBreach, check_pacing};
pub use routes::{HttpReply, HttpRequest, HttpRouter, HttpRoutes, PathPattern, PatternError};
pub use script::{
    Frame, Responded, Responder, STORM_RECONNECTS, Step, WsScript, duplicate_acks, reconnect_storm,
    silence_after_ack,
};
pub use server::{ConnRecord, ScriptError, StubServer};
