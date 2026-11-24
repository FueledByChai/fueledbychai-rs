//! The order-safety glue a live consumer runs beside its order-entry sessions (decision 0155).
//!
//! It holds what the owner-run samples in `crates/fbc-examples` proved on a venue, so a 24/7
//! consumer depends on it by tag rather than rebuilding each piece. So far:
//!
//! - [`link`]: the glue between one account's order-entry session and its fbc-oms registry. Its
//!   [`Link`](link::Link) routes every event the session hands it into the registry, through the
//!   fill ledger, and is the one path the consumer's commands take to the session; its
//!   [`LinkHandler`](link::LinkHandler) is a session's handler that hands each call to the link
//!   and then calls the consumer's wake, a closure.
//!
//! Its normal dependencies are fbc-core, fbc-oms and fbc-runtime only: it depends on no venue
//! crate and branches on no venue's name (decision 0003). It holds nothing that decides what to
//! quote (decision 0001), and every bound comes from the consumer, with no default (decision
//! 0009), except the fixed limits of [`shown`](link::shown) and [`described`](link::described),
//! which keep an echoed credential out of what is printed (decision 0155 item 8). It reads no
//! clock but its session's shard clock (decision 0155 item 3): the link times each submission
//! it sends on the `IngestClock` of the session it was last attached to or submitted through
//! ([`Link::now`](link::Link::now)), and the crate's `clippy.toml` refuses a std clock read.

pub mod link;
