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
//!   [`Link::cancels_not_ours_seen`](link::Link::cancels_not_ours_seen) holds for good. The
//!   consumer runs fbc-oms's Unknown ladder through it on its own timer
//!   ([`Link::ladder`](link::Link::ladder)), which queries each order on the ladder and sends
//!   each tombstone cancel due, and it queries an unprotected order the registry holds ended or
//!   not at all, so the session releases its market once the answer shows it ended.
//! - [`not_ours`]: FBC-aoh5's classifier of cancels not ours, one per market. Its
//!   [`CancelsNotOurs`](not_ours::CancelsNotOurs), under the consumer's span and window
//!   ([`NotOursConfig`](not_ours::NotOursConfig)), tells a cancel of the same event as the one
//!   before from the first of a new window and from a repeat; the warning and the Stop these
//!   lead to stay the consumer's.
//! - [`positions`]: the positions comparison and its re-read rule (FBC-hpg6). A
//!   [`Reading`](positions::Reading) of the market's position the consumer read is compared
//!   exactly with the registry's, and a [`PositionWatch`](positions::PositionWatch) reads a
//!   mismatch or an unread answer once more after the consumer's wait, then says whether it
//!   cleared or persists ([`Verdict`](positions::Verdict)); the read and what follows a verdict
//!   are the consumer's.
//! - [`reconcile`]: the stream check (the owner's condition in decision 0094). It says whether a
//!   position the venue's stream reported differs from the inventory, or an order of ours has
//!   fills the venue reported on an order event that the fill stream has not brought, and if so
//!   what differs ([`Unreconciled`](reconcile::Unreconciled)); it keeps no clock and no timeout,
//!   and holding the consumer's planning while they differ, and when to Stop, are the consumer's.
//! - [`sizing`]: mark-aware sizing. The operator's caps in lots ([`UsdCaps`](sizing::UsdCaps),
//!   positive with no ceiling), the inventory cap at no less than the consumer's mark, and why
//!   one side's quote is left out ([`LeftOut`](sizing::LeftOut)); it reads no venue.
//! - [`sweep`]: the sweep of a market's orders of ours at Start, after each reconnect and at
//!   Stop. Its [`Sweep`](sweep::Sweep) sends fbc-oms's cancel everything, or explicit cancels of
//!   the orders of ours the link holds live there ([`Link::live_on`](link::Link::live_on)), never
//!   an account cancel-all, sends each cancel of the market's orders due on an acknowledgement
//!   and, once the consumer's [`RateHold`](sweep::RateHold) passes, the cancel of everything
//!   again after a refusal for its rate of any cancel of ours on the market, as the link says
//!   which requests are cancels on it ([`Link::is_cancel_on`](link::Link::is_cancel_on)), and
//!   says when it ended ([`SweepEnd`](sweep::SweepEnd)); it keeps no timeout. Another market's
//!   orders and cancels make nothing due and keep no sweep from ending, but the consumer's hold,
//!   which records each refusal for its rate the consumer reads, another market's included, still
//!   delays the resend.
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
pub mod positions;
pub mod reconcile;
pub mod sizing;
pub mod sweep;
