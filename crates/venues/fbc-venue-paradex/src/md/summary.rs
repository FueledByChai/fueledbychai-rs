//! `MarketSummaryEvent` (template 4, the `markets_summary.{market}` channel) as mark and funding
//! events, at either of the root blocks Paradex documents for it: 216 bytes at schema version 0
//! and 240 at version 1, which appends `forwardRate`, `riskFreeRate` and `fundingRatePrecise`
//! (docs.paradex.trade, "Binary Encoding (SBE)", Schema versioning; the schema's
//! `sinceVersion="1"` fields). Each field is read only inside the block the frame states, so a
//! version-0 frame decodes from the fields version 0 carries.
//!
//! Root fields used (the schema): `ts`@0, `seq`@8, `markPrice`@16 (`Price8`), `fundingRate`@56
//! (`Rate8`, exponent -8) and `fundingRatePrecise`@232 (`Rate12NULL`, exponent -12); `market` is
//! the message's one var-data field. The rest (index, last, volumes, open interest, best bid and
//! ask, the options fields) is not decoded.

use fbc_core::{DecodeError, ExchTsKind, InstrumentId, PxExact, VenueMeta};

use super::sbe::{Block, Message, NULL_I64};
use super::{EXP, frame_market, micros, seq};
use fbc_core::SpecTable;

/// `MarketSummaryEvent`'s template id.
pub const TEMPLATE_SUMMARY: u16 = 4;

/// `markPrice`'s offset.
const MARK: usize = 16;
/// `fundingRate`'s offset: the rate cut to 8 decimals, carried by every version.
const FUNDING_8DP: usize = 56;
/// `fundingRatePrecise`'s offset: the rate at 12 decimals, from version 1 (240-byte block).
const FUNDING_PRECISE: usize = 232;

/// One summary frame's mark price and funding rate.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(super) struct Summary {
    pub inst: InstrumentId,
    pub meta: VenueMeta,
    pub mark: PxExact,
    /// The funding rate in units of 1e-12.
    pub rate_e12: i64,
}

/// Decodes a `MarketSummaryEvent`; refused when the frame's block ends before a field it needs
/// or the field is null.
pub(super) fn decode_summary(msg: &Message<'_>, specs: &SpecTable) -> Result<Summary, DecodeError> {
    let block = msg.block();
    let spec = frame_market(msg, specs)?;
    let mark = present(&block, MARK, "summary mark price")?;
    let rate_e12 = funding_e12(&block)?;
    // The schema does not describe this seq; the captured ETH frame carries 0, which (as a
    // trade's 0) is no sequence.
    let seq = Some(seq(present(&block, 8, "summary seq")?)?).filter(|&s| s != 0);
    let meta = VenueMeta {
        exch_ts: Some(micros(present(&block, 0, "summary ts")?)?),
        // The schema's TradeEvent calls the same leading `ts` the "Feed publish timestamp".
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: seq,
    };
    Ok(Summary {
        inst: spec.id,
        meta,
        mark: PxExact::new(mark, EXP),
        rate_e12,
    })
}

/// The funding rate in units of 1e-12: `fundingRatePrecise` where the block carries it and it
/// is not null, else `fundingRate` scaled from 1e-8 (the schema: "Version 1 clients should
/// prefer this field and fall back to field 13 only when this one is null").
fn funding_e12(block: &Block<'_>) -> Result<i64, DecodeError> {
    match block.i64_at(FUNDING_PRECISE) {
        Some(precise) if precise != NULL_I64 => Ok(precise),
        _ => present(block, FUNDING_8DP, "summary funding rate")?
            .checked_mul(10_000)
            .ok_or(DecodeError::Malformed("summary funding rate out of range")),
    }
}

/// The non-null `int64` at `offset`, refused when the block ends before it or it is null.
fn present(block: &Block<'_>, offset: usize, what: &'static str) -> Result<i64, DecodeError> {
    block
        .i64_at(offset)
        .filter(|&v| v != NULL_I64)
        .ok_or(DecodeError::Malformed(what))
}
