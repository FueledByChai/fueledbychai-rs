//! The toy's market-data feeds other than books (FBC-z2s), one record per frame or response
//! body, `kind|key=value|...`, `ts` the venue's time in nanoseconds and `seq` its sequence, each
//! optional:
//!
//! - `trade|sym|id|side|px|qty|ts?|seq?`: a public trade on the socket, `id` the venue's numeric
//!   trade id, `side` the aggressor's (`B` a buyer, `S` a seller), `px` in ticks and `qty` a
//!   positive count of lots. The caps declare an id and an aggressor on every trade, so both are
//!   required.
//! - `mark|sym|px|ts?|seq?` and `index|sym|px|ts?|seq?`: the mark and the index price on the
//!   socket, `px` an exact decimal, never rounded.
//! - `funding|sym|rate|int|next|ts?|seq?`: the funding rate on the socket, in units of 1e-12 per
//!   interval, the interval `int` in whole seconds, at least one, and the next funding time
//!   `next` in wall-clock nanoseconds. The caps declare both reported, so both are required.
//! - `stats|sym|vol?|oi?|ts?|seq?`: the body of a stats poll's response, for the instrument
//!   asked: the 24-hour volume in nanos of its quote asset and the open interest in lots, each
//!   when reported and at least one of them. A negative one is malformed, never absent (Codex
//!   r4172917318).
//!
//! Each record is read whole before anything is pushed (record 0014 item 2).

use core::time::Duration;

use fbc_core::{
    Aggressor, DecodeError, DecodeScope, Feed, InstrumentId, Lots, MdEvent, Money, PxExact,
    SpecTable, Ticks, VenueMeta, WallNs,
};

use super::decode::Record;
use DecodeError::Malformed;

/// The streamed feed `r` carries for `inst`, what the venue said about it, and its event.
pub(super) fn streamed(
    r: &Record<'_>,
    inst: InstrumentId,
    scope: &DecodeScope<'_>,
) -> Result<(Feed, VenueMeta, MdEvent), DecodeError> {
    let (feed, ev) = match r.kind {
        "trade" => (Feed::Trades, trade(r, inst, scope)?),
        "mark" => (
            Feed::Mark,
            MdEvent::Mark {
                inst,
                px: price(r)?,
            },
        ),
        "index" => (
            Feed::Index,
            MdEvent::Index {
                inst,
                px: price(r)?,
            },
        ),
        "funding" => (Feed::Funding, funding(r, inst)?),
        "stats" => return Err(Malformed("stats come only from the poll endpoint")),
        _ => return Err(Malformed("kind")),
    };
    Ok((feed, r.meta(false)?, ev))
}

fn trade(
    r: &Record<'_>,
    inst: InstrumentId,
    scope: &DecodeScope<'_>,
) -> Result<MdEvent, DecodeError> {
    let aggressor = match r.get("side")? {
        "B" => Aggressor::Buyer,
        "S" => Aggressor::Seller,
        _ => return Err(Malformed("side")),
    };
    // A trade exchanges something: no quantity is as malformed as a negative one.
    let qty = Some(r.lots("qty")?).filter(|&q| q != Lots::ZERO);
    let qty = qty.ok_or(Malformed("qty"))?;
    Ok(MdEvent::Trade {
        inst,
        id: Some(scope.trade_id_numeric(r.num("id")?)),
        aggressor,
        px: Ticks(r.num("px")?),
        qty,
    })
}

/// An exact price: a plain decimal, never rounded.
fn price(r: &Record<'_>) -> Result<PxExact, DecodeError> {
    r.num("px")
}

fn funding(r: &Record<'_>, inst: InstrumentId) -> Result<MdEvent, DecodeError> {
    let secs = Some(r.num::<u64>("int")?).filter(|&s| s > 0);
    let interval = Duration::from_secs(secs.ok_or(Malformed("int"))?);
    Ok(MdEvent::Funding {
        inst,
        rate_e12: r.num("rate")?,
        interval: Some(interval),
        next: Some(WallNs(r.num("next")?)),
    })
}

/// The stats a poll's response `r` reports for `inst`, the instrument asked, and what the venue
/// said about them.
pub(super) fn stats(
    r: &Record<'_>,
    inst: InstrumentId,
    specs: &SpecTable,
) -> Result<(VenueMeta, MdEvent), DecodeError> {
    if r.kind != "stats" {
        return Err(Malformed("kind"));
    }
    let spec = r.opt("sym").and_then(|sym| specs.by_symbol(sym));
    let spec = spec
        .filter(|spec| spec.id == inst)
        .ok_or(Malformed("sym"))?;
    let meta = r.meta(false)?;
    let vol = r.opt_num::<i128>("vol")?;
    if vol.is_some_and(|v| v < 0) {
        return Err(Malformed("vol"));
    }
    let oi = r.opt_lots("oi")?;
    if vol.is_none() && oi.is_none() {
        return Err(Malformed("stats"));
    }
    let ev = MdEvent::Stats {
        inst,
        volume_24h_quote: vol.map(|nanos| Money::new(nanos, spec.quote_ccy)),
        oi,
    };
    Ok((meta, ev))
}
