//! Paradex's private `FillEvent` (SBE template 21, the `fills.{market}` channel) decoded into
//! fills (BT-402, decisions 0004, 0009, 0014, 0022).
//!
//! The layout is the schema's (`paradex_1_0.xml` at the paradex-py commit 0022 cites), read
//! through [`sbe`](crate::md::sbe)'s block-length-gated reader; FueledByChaiTrading's
//! `ParadexSbeTranscoder` reads the same offsets. The root block: `ts`@0, `seq`@8 (optional),
//! `fillType`@16, `side`@17, `liquidity`@18, `price`@19, `size`@27, `fee`@35 (`Value8`),
//! `realizedPnl`@43 (`Value8NULL`), `createdAt`@51, `account`@59 (32 bytes),
//! `underlyingPrice`@91 and `realizedFunding`@99, 107 bytes up to version 1; at version 2
//! `flags`@107 and `orderbookSeqNo`@108 are appended (116 bytes). Then the var data: `fillId`,
//! `orderId`, `clientOrderId`, `tradeId`, `market`, and at version 2 `feeCurrency`.
//!
//! What a fill states:
//! - `fillType` FILL and RPI are the account's own executions, their client id read through
//!   [`DecodeScope::client_order_id`] (an empty one is none). LIQUIDATION, UNWIND_TRANSFER,
//!   SETTLE_MARKET and BLOCK_TRADE are the venue's: pushed with no client-id match whatever
//!   client id the frame carries. How the OMS counts them is FBC-5uk's. Any other type is
//!   refused.
//! - The fill id, keyed as `<fillType>:<fillId>` (for example `FILL:8615262148007718001`) for
//!   every type: Paradex states a fill id "Unique string ID of fill per FillType" (AsyncAPI
//!   `ResponsesFillResult.id`), so the id alone could merge a FILL and a LIQUIDATION into one
//!   ledger key and drop the second. The type names hold no `:`, so the spelling is one-to-one;
//!   any later REST fills decoder spells it the same. An empty id is refused, and so is one too
//!   long for the core once prefixed (Paradex's are 19 digits). The order id (none when empty,
//!   as a position transfer's may be) through the scope; FillEvent states no remaining size,
//!   so no cumulative quantity after the fill.
//! - `side` the account's side, as the Java library reads it; `price` and `size` on the
//!   instrument's grid and size step; `liquidity` MAKER or TAKER, NON_REPRESENTABLE the venue
//!   not saying.
//! - The fee through [`DecodeScope::fee`] under the declared sign ("positive = paid, negative =
//!   rebate"), in `feeCurrency`; a frame that names none (version 1, which predates the field,
//!   or an empty one) has it in the instrument's settlement currency.
//! - `realizedPnl` and `realizedFunding` as P&L in the settlement currency, their sign the
//!   venue's ("Realized PnL from this fill", "Realized funding PnL from this fill"); a null one
//!   is none reported.
//! - `seq` the venue sequence, none when null ("null for position transfers"); `ts` the venue's
//!   publish time, as on the order events.
//! - `flags` (INTERACTIVE, RPI, FASTFILL), `orderbookSeqNo`, `tradeId`, `createdAt`,
//!   `underlyingPrice` and `account` are not read: a fill models none of them.
//!
//! The whole frame is read before anything is pushed, so a refused frame pushes nothing.

use fbc_core::{
    AssetSym, DecodeError, DecodeScope, ExchTsKind, ExecEvent, ExecSink, FillEvent, FillIdent,
    Liquidity3, Money, Side, SpecTable, VenueMeta,
};

use crate::md::sbe::{Block, Message, NULL_I64};
use crate::md::{lots, market, micros, price, required, seq};

/// `FillEvent`'s template id.
pub const TEMPLATE_FILL: u16 = 21;

/// Nanos per unit of a `Value8` mantissa (exponent -8).
const NANOS_PER_VALUE8: i128 = 10;

/// Decodes one `FillEvent` frame into the fill it states. Refused, with nothing pushed, when
/// the frame is not a `FillEvent`, is shorter than its header or its declared block, states a
/// value outside the schema's, names a market missing from `specs`, or a fill or order id or
/// fee the scope refuses.
pub fn decode_fill_event(
    frame: &[u8],
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
    sink: &mut dyn ExecSink,
) -> Result<(), DecodeError> {
    let msg = Message::parse(frame)?;
    if msg.header().template_id != TEMPLATE_FILL {
        return Err(DecodeError::Malformed("not a FillEvent"));
    }
    let block = msg.block();
    let mut tail = msg.tail();
    let mut var = |what: &'static str| tail.var_str()?.ok_or(DecodeError::Malformed(what));
    let fill_id = var("fill id")?;
    let order_id = var("fill order id")?;
    let client_id = var("fill client order id")?;
    let _trade_id = var("fill trade id")?;
    let symbol = var("fill market")?;
    // At version 2, `feeCurrency`, missing when absent; a version-1 frame stops after market.
    let fee_currency = if msg.header().version >= 2 {
        tail.var_str()?
    } else {
        None
    };
    let spec = market(symbol, specs)?;

    let ts = micros(required(&block, 0, "fill ts")?)?;
    let venue_seq = match required(&block, 8, "fill seq")? {
        NULL_I64 => None,
        n => Some(seq(n)?),
    };
    let (fill_type, own) = match block.u8_at(16) {
        // FILL and RPI: the account's own executions.
        Some(1) => ("FILL", true),
        Some(5) => ("RPI", true),
        // LIQUIDATION, UNWIND_TRANSFER, SETTLE_MARKET, BLOCK_TRADE: the venue's.
        Some(2) => ("LIQUIDATION", false),
        Some(3) => ("UNWIND_TRANSFER", false),
        Some(4) => ("SETTLE_MARKET", false),
        Some(6) => ("BLOCK_TRADE", false),
        _ => return Err(DecodeError::Malformed("fill type")),
    };
    let side = match block.u8_at(17) {
        Some(1) => Side::Buy,
        Some(2) => Side::Sell,
        _ => return Err(DecodeError::Malformed("fill side")),
    };
    let liquidity = match block.u8_at(18) {
        Some(1) => Liquidity3::Maker,
        Some(2) => Liquidity3::Taker,
        // NON_REPRESENTABLE: the venue does not say.
        Some(254) => Liquidity3::Unknown,
        _ => return Err(DecodeError::Malformed("fill liquidity")),
    };
    let px = price(spec, required(&block, 19, "fill price")?)?;
    let qty = lots(spec, required(&block, 27, "fill size")?)?;
    let fee_raw = value8(&block, 35, "fill fee")?.ok_or(DecodeError::Malformed("fill fee"))?;
    let realized_pnl = value8(&block, 43, "fill realized pnl")?;
    let realized_funding = value8(&block, 99, "fill realized funding")?;

    let fee_asset = match fee_currency {
        None | Some("") => spec.settle_ccy,
        Some(ccy) => AssetSym::new(ccy).ok_or(DecodeError::Malformed("fill fee currency"))?,
    };
    let fee = scope
        .fee(fee_raw, fee_asset)
        .map_err(DecodeError::FeeRefused)?;
    // The id as the venue sent it is checked first, so an empty one is refused, not keyed as
    // the bare prefix; then the key names the fill type before it.
    scope.fill_id(fill_id)?;
    let fill = scope.fill_id(&format!("{fill_type}:{fill_id}"))?;
    let vid = if order_id.is_empty() {
        None
    } else {
        Some(scope.venue_order_id(order_id)?)
    };
    let cid = (own && !client_id.is_empty()).then(|| scope.client_order_id(client_id));
    let pnl = |nanos: i128| Money::new(nanos, spec.settle_ccy);
    let event = FillEvent {
        ident: FillIdent::Venue {
            fill,
            vid,
            cum_after: None,
        },
        cid,
        inst: spec.id,
        side,
        px,
        qty,
        liquidity,
        fee,
        realized_pnl: realized_pnl.map(pnl),
        realized_funding: realized_funding.map(pnl),
        replay: false,
    };
    let meta = VenueMeta {
        exch_ts: Some(ts),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq,
    };
    sink.push(meta, ExecEvent::Fill(event));
    Ok(())
}

/// The `Value8` at `offset` in nanos, `None` when null; refused when the block ends before it.
fn value8(
    block: &Block<'_>,
    offset: usize,
    what: &'static str,
) -> Result<Option<i128>, DecodeError> {
    Ok(match required(block, offset, what)? {
        NULL_I64 => None,
        mantissa => Some(i128::from(mantissa) * NANOS_PER_VALUE8),
    })
}
