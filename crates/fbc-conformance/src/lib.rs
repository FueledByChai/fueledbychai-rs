//! The adapter conformance kit (design §6): the failures that hurt the Java stack, played
//! against the runtime and the venue crates in their tests, on the loopback interface only.
//!
//! So far (FBC-jcy):
//!
//! - [`StubServer`], a stub venue on two 127.0.0.1 ephemeral ports: a WebSocket endpoint that
//!   plays a [`WsScript`] (accept, read, push, close, go silent; each step names its connection)
//!   and records every connection and data frame, and an HTTP/1.1 endpoint that answers fixed
//!   responses by path, for a venue's REST anchor.
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
//!   declared order capability exercised through `ExecCodec` (FBC-7lx), and its order updates,
//!   fills, request rejects and venue modes decoded through `DecodeScope` (FBC-7ce).
//!
//! Not here yet: the named conformance suite and its macro, and the rest of the conformance toy
//! venue.

mod pacing;
mod routes;
mod script;
mod server;
pub mod toy;

pub use pacing::{PacingBreach, check_pacing};
pub use routes::{HttpReply, HttpRequest, HttpRouter, HttpRoutes, PathPattern, PatternError};
pub use script::{
    Frame, Responded, Responder, STORM_RECONNECTS, Step, WsScript, duplicate_acks, reconnect_storm,
    silence_after_ack,
};
pub use server::{ConnRecord, ScriptError, StubServer};
