//! Paradex's private `PositionEvent` (SBE template 22, the `positions` channel) decoded into
//! the account's position, and `AccountEvent` (template 23, the `account` channel) into its
//! balance (BT-402, decisions 0004, 0014, 0022, design §4.6).
//!
//! The layouts are the schema's (`paradex_1_0.xml` at the paradex-py commit 0022 cites), read
//! through [`sbe`](crate::md::sbe)'s block-length-gated reader; paradex-py's generated decoder
//! at that commit reads the same offsets, and FueledByChaiTrading's `ParadexSbeTranscoder`
//! reads `AccountEvent`'s.
//!
//! `PositionEvent`'s root block is 154 bytes at every version: `ts`@0, `seq`@8, `side`@16,
//! `size`@17 (`Qty8`), `avgEntryPrice`@25 (`Price8`), `unrealizedPnl`@33, `realizedPnl`@41,
//! `markPrice`@49, `liquidationPrice`@57, `leverage`@65, `updatedAt`@73, `account`@81 (32
//! bytes), `avgEntryPriceUsd`@113, `cost`@121, `costUsd`@129, `cachedFundingIndex`@137,
//! `unrealizedFundingPnl`@145 and `status`@153; then the var data `market` and `lastFillId`.
//! What a position states:
//! - `side` BUY long and SELL short ("BUY=long, SELL=short"), `size` absolute ("Position size
//!   (absolute)") on the instrument's size step: the position is the size's lots with the
//!   side's sign. A flat position (size 0) is flat on any side, NON_REPRESENTABLE included; a
//!   position with a size on no side is refused.
//! - `avgEntryPrice` exactly as sent, mantissa and exponent −8, never rounded to the tick
//!   (0004); it must be above zero. A flat position has none, whatever the frame states there
//!   (a closed one may still state its last).
//! - `status` OPEN, UNSPECIFIED or NON_REPRESENTABLE leaves the position its size; CLOSED with
//!   a size is refused, the frame contradicting itself; any other value is refused.
//! - `seq` the venue sequence ("Unique increasing number (non-sequential)", the `positions`
//!   page), `ts` the venue's publish time, as on the order and fill events.
//! - The P&L, mark, liquidation, leverage, cost and funding fields, `updatedAt`, `account` and
//!   `lastFillId` are not read: a position models none of them. The running funding
//!   (`unrealizedFundingPnl`) is realized into fills (decision 0059).
//!
//! `AccountEvent`'s root block is 113 bytes: `ts`@0, `seq`@8, `totalCollateral`@16,
//! `freeCollateral`@24, `initialMarginReq`@32, `maintenanceMarginReq`@40, `accountValue`@48,
//! `unrealizedPnl`@56 (always null), `updatedAt`@64, `account`@72 (32 bytes), `marginCushion`@104
//! and `status`@112; version 2 may append `lastSeenNotification`@113 (121 bytes), and the
//! schema says version 2 describes both layouts, so the block length is read from the header.
//! Then the var data `settlementAsset`. What a balance states:
//! - The equity is `accountValue` ("Portfolio value including unrealized PnL"), what is
//!   available `freeCollateral` ("Collateral available for new orders"), both as sent, sign
//!   included, in `settlementAsset` ("Settlement asset e.g. USDC"); a frame naming none is
//!   refused.
//! - `status` (ACTIVE, LIQUIDATION) and the margin fields are not read: a balance models none of
//!   them (FBC-6h44 is LIQUIDATION's).
//!
//! Each frame is read whole before anything is pushed, so a refused frame pushes nothing.

use fbc_core::{
    AssetSym, DecodeError, DecodeScope, ExchTsKind, ExecEvent, ExecSink, Money, PxExact, Side,
    SignedLots, SpecTable, VenueMeta,
};

use crate::md::sbe::{Block, Message, NULL_I64};
use crate::md::{lots, market, micros, required, seq};

/// `PositionEvent`'s template id.
pub const TEMPLATE_POSITION: u16 = 22;

/// `AccountEvent`'s template id.
pub const TEMPLATE_ACCOUNT: u16 = 23;

/// The exponent of a `Price8` mantissa.
const EXP: i8 = -8;

/// Nanos per unit of a `Value8` mantissa (exponent -8).
const NANOS_PER_VALUE8: i128 = 10;

/// Decodes one `PositionEvent` frame into the account's position in its market. Refused, with
/// nothing pushed, when the frame is not a `PositionEvent`, is shorter than its header or its
/// declared block, states a value outside the schema's or a position it contradicts, or names a
/// market missing from `specs`.
///
/// `scope` is the one the order-entry session's dispatch lends: a position carries no id or
/// fee for it to decode, and taking it keeps the decoder callable only inside that dispatch,
/// like the order and fill decoders.
pub fn decode_position_event(
    frame: &[u8],
    _scope: &DecodeScope<'_>,
    specs: &SpecTable,
    sink: &mut dyn ExecSink,
) -> Result<(), DecodeError> {
    let msg = Message::parse(frame)?;
    if msg.header().template_id != TEMPLATE_POSITION {
        return Err(DecodeError::Malformed("not a PositionEvent"));
    }
    let block = msg.block();
    let symbol = msg
        .tail()
        .var_str()?
        .ok_or(DecodeError::Malformed("position market"))?;
    let spec = market(symbol, specs)?;

    let meta = publish_meta(&block, "position")?;
    let side = block.u8_at(16);
    let size = lots(spec, required(&block, 17, "position size")?)?;
    let avg_entry = required(&block, 25, "position average entry")?;
    let status = block.u8_at(153);
    let flat = size.get() == 0;
    match status {
        // OPEN, UNSPECIFIED, NON_REPRESENTABLE: the size says what the position is.
        Some(1 | 3 | 254) => {}
        // CLOSED: nothing held.
        Some(2) if flat => {}
        Some(2) => return Err(DecodeError::Malformed("closed position with a size")),
        _ => return Err(DecodeError::Malformed("position status")),
    }
    let (qty, avg_entry) = if flat {
        // BUY, SELL, or NON_REPRESENTABLE: a flat position is on no side.
        match side {
            Some(1 | 2 | 254) => (SignedLots(0), None),
            _ => return Err(DecodeError::Malformed("position side")),
        }
    } else {
        let side = match side {
            Some(1) => Side::Buy,
            Some(2) => Side::Sell,
            _ => return Err(DecodeError::Malformed("position side")),
        };
        if avg_entry == NULL_I64 || avg_entry <= 0 {
            return Err(DecodeError::Malformed("position average entry"));
        }
        (
            SignedLots::of(side, size),
            Some(PxExact::new(avg_entry, EXP)),
        )
    };
    sink.push(
        meta,
        ExecEvent::Position {
            inst: spec.id,
            qty,
            avg_entry,
        },
    );
    Ok(())
}

/// Decodes one `AccountEvent` frame into the account's balance. Refused, with nothing pushed,
/// when the frame is not an `AccountEvent`, is shorter than its header or its declared block,
/// states a null where the schema allows none, or names no settlement asset.
///
/// `scope` is lent as for [`decode_position_event`].
pub fn decode_account_event(
    frame: &[u8],
    _scope: &DecodeScope<'_>,
    sink: &mut dyn ExecSink,
) -> Result<(), DecodeError> {
    let msg = Message::parse(frame)?;
    if msg.header().template_id != TEMPLATE_ACCOUNT {
        return Err(DecodeError::Malformed("not an AccountEvent"));
    }
    let block = msg.block();
    let asset = msg
        .tail()
        .var_str()?
        .and_then(AssetSym::new)
        .ok_or(DecodeError::Malformed("account settlement asset"))?;

    let meta = publish_meta(&block, "account")?;
    let available = value8(&block, 24, "account free collateral")?;
    let equity = value8(&block, 48, "account value")?;
    sink.push(
        meta,
        ExecEvent::Balance {
            equity: Money::new(equity, asset),
            available: Money::new(available, asset),
        },
    );
    Ok(())
}

/// The venue's publish time (`ts`@0) and sequence (`seq`@8), which both templates carry and
/// neither may leave null.
fn publish_meta(block: &Block<'_>, what: &'static str) -> Result<VenueMeta, DecodeError> {
    let ts = micros(required(block, 0, what)?)?;
    let venue_seq = seq(required(block, 8, what)?)?;
    Ok(VenueMeta {
        exch_ts: Some(ts),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(venue_seq),
    })
}

/// The `Value8` at `offset` in nanos; refused when null or when the block ends before it.
fn value8(block: &Block<'_>, offset: usize, what: &'static str) -> Result<i128, DecodeError> {
    match required(block, offset, what)? {
        NULL_I64 => Err(DecodeError::Malformed(what)),
        mantissa => Ok(i128::from(mantissa) * NANOS_PER_VALUE8),
    }
}
