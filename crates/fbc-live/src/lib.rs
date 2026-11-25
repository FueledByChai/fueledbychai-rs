//! The order-safety glue a live consumer runs beside its order-entry sessions (decision 0155).
//!
//! It holds what the owner-run samples in `crates/fbc-examples` proved on a venue, so a 24/7
//! consumer depends on it by tag rather than rebuilding each piece. So far:
//!
//! - [`link`]: the glue between one account's order-entry session and its fbc-oms registry. Its
//!   [`Link`](link::Link) routes every event the session hands it into the registry, through the
//!   fill ledger, and is the one path the consumer's commands take to the session; its
//!   [`LinkHandler`](link::LinkHandler) is a session's handler that hands each call to the link
//!   and then calls the consumer's wake, a closure. Once the link notes a cancel not ours,
//!   [`Link::cancels_not_ours_seen`](link::Link::cancels_not_ours_seen) holds for good.
//! - [`not_ours`]: FBC-aoh5's classifier of cancels not ours, one per market. Its
//!   [`CancelsNotOurs`](not_ours::CancelsNotOurs), under the consumer's span and window
//!   ([`NotOursConfig`](not_ours::NotOursConfig)), tells a cancel of the same event as the one
//!   before from the first of a new window and from a repeat; the warning and the Stop these
//!   lead to stay the consumer's.
//! - [`sizing`]: mark-aware sizing. The operator's caps in lots ([`UsdCaps`](sizing::UsdCaps),
//!   positive with no ceiling), the inventory cap at no less than the consumer's mark, and why
//!   one side's quote is left out ([`LeftOut`](sizing::LeftOut)); it reads no venue.
//! - [`sweep`]: the sweep of a market's orders of ours at Start, after each reconnect and at
//!   Stop. Its [`Sweep`](sweep::Sweep) sends fbc-oms's cancel everything, or explicit cancels of
//!   the orders of ours the link holds live there ([`Link::live_on`](link::Link::live_on)), never
//!   an account cancel-all, sends each cancel due on an acknowledgement and, once the consumer's
//!   [`RateHold`](sweep::RateHold) passes, the cancel of everything again after a refusal for its
//!   rate of any cancel of ours, as the link says which requests are cancels
//!   ([`Link::is_cancel`](link::Link::is_cancel)), and says when it ended
//!   ([`SweepEnd`](sweep::SweepEnd)); it keeps no timeout.
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
pub mod not_ours;
pub mod sizing;
pub mod sweep;
