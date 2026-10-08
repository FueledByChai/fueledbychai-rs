//! `fee_sign`, `liquidity_reported` and `position_signed` (design §6, decision 0004): what a
//! venue's fixture frames decode to holds what the frames state. Each reads the case files
//! ([`frames`](super::frames)) named below from its own subdirectory, each case decoded by an
//! order-entry codec built fresh from the factory, and every case is required:
//!
//! - `fee_sign/rebate.frames` and `fee_sign/paid.frames`: every fill in the first is one the
//!   venue paid us a rebate on (a maker rebate) and decodes to a [`Fee`](fbc_core::Fee) below
//!   zero; every fill in the second is one we paid a fee on (a taker fill) and decodes to one
//!   above zero. A fill decoded the wrong way round breaks `FillCaps.fee_sign`.
//! - `liquidity_reported/maker.frames` and `liquidity_reported/taker.frames`: every fill in the
//!   first reports [`Liquidity3::Maker`], every one in the second [`Liquidity3::Taker`].
//! - `position_signed/long.frames` and `position_signed/short.frames`: every position
//!   ([`ExecEvent::Position`] or [`ExecEvent::ResyncPosition`]) in the first is above zero,
//!   every one in the second below: positions are signed, positive long.
//!
//! A case with no event of its kind fails (a pass that judged nothing would prove nothing), and
//! so does a frame the codec refuses, a case file the check does not read, and a missing
//! subdirectory or case. Each is skipped only for a venue whose caps declare no order entry (it
//! has no fills and no positions); `fee_sign` and `liquidity_reported` also for a venue whose
//! fills are derived from its order updates (`FillCaps.source` is
//! [`FillSource::DerivedFromOrderStatus`]: its codec decodes no fill event), and
//! `liquidity_reported` for one whose caps declare its fills carry no liquidity flag
//! (`FillCaps.liquidity_flag` false: each reports [`Liquidity3::Unknown`]).

use fbc_core::{ExecEvent, FillEvent, FillSource, Liquidity3, SignedLots, VenueCaps};

use super::frames::{Cases, Expect};
use super::harness::Harness;
use super::{Failure, Subject, Verdict};

/// Why a check reading fills or positions is skipped for a venue without order entry.
const NO_EXEC: &str =
    "the caps declare no order entry (VenueCaps.exec is None): no fill or position is decoded";

/// Why a check judging fill events is skipped for a venue whose fills are derived from its
/// order updates (Codex r4216777891).
const DERIVED: &str = "the caps declare fills derived from order updates \
                       (FillCaps.source is DerivedFromOrderStatus): the codec decodes no fill event";

/// Why a check judging fill events has nothing to judge on a venue declaring `caps`, if it has
/// not: no order entry, or fills derived from order updates rather than decoded.
fn no_fill_events(caps: &VenueCaps) -> Option<&'static str> {
    match &caps.exec {
        None => Some(NO_EXEC),
        Some(exec) if exec.fills.source == FillSource::DerivedFromOrderStatus => Some(DERIVED),
        Some(_) => None,
    }
}

/// Runs `fee_sign` against `subject`.
pub fn fee_sign(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    const CHECK: &str = "fee_sign";
    let h = Harness::new(CHECK, subject)?;
    if let Some(why) = no_fill_events(&h.caps) {
        return Ok(Verdict::Skipped { check: CHECK, why });
    }
    let cases = Cases {
        check: CHECK,
        capability: "FillCaps.fee_sign",
        noun: "fill",
        expect: &[
            Expect {
                stem: "rebate",
                judge: |ev| fill(ev).map(|f| fee(f, |n| n < 0, "below zero (a rebate)")),
            },
            Expect {
                stem: "paid",
                judge: |ev| fill(ev).map(|f| fee(f, |n| n > 0, "above zero (paid)")),
            },
        ],
    };
    cases.run(&h, subject)
}

/// Runs `liquidity_reported` against `subject`.
pub fn liquidity_reported(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    const CHECK: &str = "liquidity_reported";
    let h = Harness::new(CHECK, subject)?;
    if let Some(why) = no_fill_events(&h.caps) {
        return Ok(Verdict::Skipped { check: CHECK, why });
    }
    if h.caps
        .exec
        .as_ref()
        .is_some_and(|e| !e.fills.liquidity_flag)
    {
        return Ok(Verdict::Skipped {
            check: CHECK,
            why: "the caps declare fills carry no liquidity (FillCaps.liquidity_flag is false)",
        });
    }
    let cases = Cases {
        check: CHECK,
        capability: "FillCaps.liquidity_flag",
        noun: "fill",
        expect: &[
            Expect {
                stem: "maker",
                judge: |ev| fill(ev).map(|f| liquidity(f, Liquidity3::Maker)),
            },
            Expect {
                stem: "taker",
                judge: |ev| fill(ev).map(|f| liquidity(f, Liquidity3::Taker)),
            },
        ],
    };
    cases.run(&h, subject)
}

/// Runs `position_signed` against `subject`.
pub fn position_signed(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    const CHECK: &str = "position_signed";
    let h = Harness::new(CHECK, subject)?;
    if h.caps.exec.is_none() {
        return Ok(Verdict::Skipped {
            check: CHECK,
            why: NO_EXEC,
        });
    }
    let cases = Cases {
        check: CHECK,
        capability: "SignedLots: positive is long",
        noun: "position",
        expect: &[
            Expect {
                stem: "long",
                judge: |ev| position(ev).map(|q| signed(q, |n| n > 0, "above zero (long)")),
            },
            Expect {
                stem: "short",
                judge: |ev| position(ev).map(|q| signed(q, |n| n < 0, "below zero (short)")),
            },
        ],
    };
    cases.run(&h, subject)
}

/// The fill `ev` is, if it is one.
fn fill(ev: &ExecEvent) -> Option<&FillEvent> {
    match ev {
        ExecEvent::Fill(fill) => Some(fill),
        _ => None,
    }
}

/// The quantity of the position `ev` reports, pushed or in a resync, if it reports one.
fn position(ev: &ExecEvent) -> Option<SignedLots> {
    match ev {
        ExecEvent::Position { qty, .. } | ExecEvent::ResyncPosition { qty, .. } => Some(*qty),
        _ => None,
    }
}

/// Whether `fill`'s fee, as a cost to us, is `want`.
fn fee(fill: &FillEvent, holds: fn(i128) -> bool, want: &str) -> Result<(), String> {
    let cost = fill.fee.cost();
    if holds(cost.nanos) {
        return Ok(());
    }
    Err(format!(
        "a fill's fee decoded as a cost of {} nanos {}, not {want}",
        cost.nanos,
        cost.asset.as_str()
    ))
}

/// Whether `fill` reports `want`.
fn liquidity(fill: &FillEvent, want: Liquidity3) -> Result<(), String> {
    if fill.liquidity == want {
        return Ok(());
    }
    Err(format!(
        "a fill decoded as {:?}, not {want:?}",
        fill.liquidity
    ))
}

/// Whether the position `qty` is `want`.
fn signed(qty: SignedLots, holds: fn(i64) -> bool, want: &str) -> Result<(), String> {
    if holds(qty.0) {
        return Ok(());
    }
    Err(format!("a position decoded as {} lots, not {want}", qty.0))
}
