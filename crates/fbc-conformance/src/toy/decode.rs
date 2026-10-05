//! What the toy venue sends on its order-entry stream, decoded through the [`DecodeScope`] the
//! core lends (decision 0004): order updates, fills, rejects of a request and venue modes. One
//! record per frame, `kind|key=value|...`:
//!
//! - `order|cid|vid?|sym|side|st|cum|px?|qty?|po|ro|ts?|seq`: an order update. `st` is `open`,
//!   `filled`, `expired`, `canceled` (with `why`), `rejected` (with `code`, read through
//!   [`REJECT_CODES`]) or `amended` (with `nvid`, the new venue id the toy always issues).
//! - `fill|fid?|vid?|cum?|cid|sym|side|px|qty|liq|fee|fa|pnl|fund|ts?|seq`, and `refill` for one
//!   the venue sends again after a reconnect: a fill. With [`FillIds::Venue`](super::FillIds)
//!   `fid` is required and the order's `vid` and cumulative `cum` optional; with
//!   [`FillIds::Derived`](super::FillIds) `fid` is refused and `vid` and `cum` key the fill.
//! - `reject|rpc|code|msg?`: the venue refused request `rpc` as a whole.
//! - `mode|m|sym?|ts?|seq?`: a venue mode, for market `sym` or, without one, the account.
//!
//! The answers to requests, resyncs and authentication are read here too ([`snapshot`],
//! [`position`], [`refusal`]) but held by the session ([`super::session`]).
//!
//! Every field the caps promise is required (client ids and flags echoed, liquidity, the fee's
//! asset, realized P&L and funding, a sequence on order and fill events), an optional quantity
//! that is present must be a count (Codex r4172917318: negative is malformed, never absent), and
//! the whole record is read before anything is pushed, so a refused frame pushes nothing
//! (record 0014 item 2).

use core::str::FromStr;

use fbc_core::{
    AssetSym, CancelReason, DecodeError, DecodeScope, ExchNs, ExchTsKind, ExecEvent, FillCaps,
    FillEvent, FillIdent, InstrumentSpec, Liquidity3, Lots, ModeScope, Money, NotAmendable,
    OrderUpdate, PxExact, RawFrame, Reject, RejectKind, RpcId, Side, SignedLots, SpecTable,
    SubmitOutcome, TerminalHint, TerminalReject, Ticks, VenueMeta, VenueMode, VenueOrderSnapshot,
    VenueOrderState,
};

use DecodeError::Malformed;

/// The toy's reject codes and the kind each maps to; a code missing here is
/// [`RejectKind::Other`]. A request's reject keeps its code and kind whatever they are; an
/// order's `rejected` state takes only a refusal that ends an order ([`TerminalReject`]), so a
/// code for a refusal that leaves the order as it was is malformed there (record 0014 item 6).
pub const REJECT_CODES: [(&str, RejectKind); 12] = [
    ("1001", RejectKind::PostOnlyWouldCross),
    ("1002", RejectKind::InvalidPrice),
    ("1003", RejectKind::InvalidQty),
    ("1004", RejectKind::MinNotional),
    ("1005", RejectKind::Margin),
    ("1006", RejectKind::RateLimited { retry_after: None }),
    ("2001", RejectKind::NotFound),
    (
        "2002",
        RejectKind::AlreadyTerminal(TerminalHint::Unspecified),
    ),
    (
        "2003",
        RejectKind::NotAmendable(NotAmendable::PartiallyFilled),
    ),
    ("2004", RejectKind::NoChange),
    ("3001", RejectKind::VenueMode(VenueMode::ReduceOnly)),
    ("3002", RejectKind::Unsupported),
];

/// What the toy's reject `code` means.
pub fn reject_kind(code: &str) -> RejectKind {
    let found = REJECT_CODES.iter().find(|(known, _)| *known == code);
    found.map_or(RejectKind::Other, |(_, kind)| *kind)
}

/// The one record `frame` carries.
pub(super) fn record(frame: RawFrame<'_>) -> Result<Record<'_>, DecodeError> {
    let RawFrame::Text(text) = frame else {
        return Err(Malformed("binary frame"));
    };
    if text.lines().count() > 1 {
        return Err(Malformed("one record per frame"));
    }
    Record::parse(text)
}

/// The one event record `r` carries, with what the venue said about its time and order.
pub(super) fn event(
    r: &Record<'_>,
    fills: &FillCaps,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<(VenueMeta, ExecEvent), DecodeError> {
    Ok(match r.kind {
        "order" => (r.meta(true)?, ExecEvent::Order(order(r, scope, specs)?)),
        "fill" | "refill" => (
            r.meta(true)?,
            ExecEvent::Fill(fill(r, fills, scope, specs)?),
        ),
        "reject" => (r.meta(false)?, reject(r)?),
        "mode" => (r.meta(false)?, mode(r, specs)?),
        _ => return Err(Malformed("kind")),
    })
}

fn order(
    r: &Record<'_>,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<OrderUpdate, DecodeError> {
    let (cum_filled, qty) = (r.lots("cum")?, r.opt_lots("qty")?);
    if qty.is_some_and(|qty| cum_filled > qty) {
        return Err(Malformed("cum"));
    }
    Ok(OrderUpdate {
        cid: Some(scope.client_order_id(r.get("cid")?)),
        vid: r.opt("vid").map(|v| scope.venue_order_id(v)).transpose()?,
        inst: r.spec(specs)?.id,
        side: r.side()?,
        state: state(r, scope)?,
        cum_filled,
        px: r.opt_num("px")?.map(Ticks),
        qty,
        post_only: Some(r.flag("po")?),
        reduce_only: Some(r.flag("ro")?),
    })
}

fn state(r: &Record<'_>, scope: &DecodeScope<'_>) -> Result<VenueOrderState, DecodeError> {
    Ok(match r.get("st")? {
        "open" => VenueOrderState::Open,
        "filled" => VenueOrderState::Filled,
        "expired" => VenueOrderState::Expired,
        "canceled" => VenueOrderState::Canceled(match r.get("why")? {
            "requested" => CancelReason::Requested,
            "disconnect" => CancelReason::Disconnect,
            "selftrade" => CancelReason::SelfTrade,
            "postonly" => CancelReason::PostOnly,
            "reduceonly" => CancelReason::ReduceOnly,
            "unfilled" => CancelReason::Unfilled,
            "liquidation" => CancelReason::Liquidation,
            "venue" => CancelReason::Venue,
            _ => return Err(Malformed("why")),
        }),
        "rejected" => {
            let terminal = TerminalReject::new(reject_kind(r.get("code")?));
            VenueOrderState::Rejected(terminal.ok_or(Malformed("code"))?)
        }
        // The toy issues a new venue id for every amended order (`keeps_venue_id` false).
        "amended" => VenueOrderState::Amended {
            new_vid: Some(scope.venue_order_id(r.get("nvid")?)?),
        },
        _ => return Err(Malformed("st")),
    })
}

fn fill(
    r: &Record<'_>,
    fills: &FillCaps,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<FillEvent, DecodeError> {
    let qty = r.lots("qty")?;
    if qty == Lots::ZERO {
        return Err(Malformed("qty"));
    }
    let vid = |v: &str| scope.venue_order_id(v);
    let ident = if fills.fill_id {
        FillIdent::Venue {
            fill: scope.fill_id(r.get("fid")?)?,
            vid: r.opt("vid").map(vid).transpose()?,
            cum_after: r.opt_lots("cum")?,
        }
    } else if r.opt("fid").is_some() {
        return Err(Malformed("fid"));
    } else {
        FillIdent::Derived {
            vid: vid(r.get("vid")?)?,
            cum_after: r.lots("cum")?,
        }
    };
    let spec = r.spec(specs)?;
    let fee_asset = AssetSym::new(r.get("fa")?).ok_or(Malformed("fa"))?;
    let fill = FillEvent {
        ident,
        cid: Some(scope.client_order_id(r.get("cid")?)),
        inst: spec.id,
        side: r.side()?,
        px: Ticks(r.num("px")?),
        qty,
        liquidity: match r.get("liq")? {
            "M" => Liquidity3::Maker,
            "T" => Liquidity3::Taker,
            _ => return Err(Malformed("liq")),
        },
        fee: scope.fee(r.num("fee")?, fee_asset)?,
        realized_pnl: Some(r.money("pnl", spec)?),
        realized_funding: Some(r.money("fund", spec)?),
        replay: r.kind == "refill",
    };
    // The cumulative quantity after a fill includes the fill.
    match fill.cum_after() {
        Some(cum) if cum < qty => Err(Malformed("cum")),
        _ => Ok(fill),
    }
}

/// A refusal of the whole request: its outcome for every item, which answers it.
fn reject(r: &Record<'_>) -> Result<ExecEvent, DecodeError> {
    Ok(ExecEvent::Outcome {
        rpc: RpcId(r.num("rpc")?),
        item: None,
        outcome: SubmitOutcome::Rejected(refusal(r)?),
    })
}

/// The venue's refusal a record states: its `code`, read through [`REJECT_CODES`], and its
/// message `msg` when it sends one.
pub(super) fn refusal(r: &Record<'_>) -> Result<Reject, DecodeError> {
    let code = r.get("code")?;
    Ok(Reject {
        kind: reject_kind(code),
        venue_code: Some(code.into()),
        raw: r.opt("msg").unwrap_or("").into(),
    })
}

/// One order as a query answer or a resync reports it: every field the caps promise is
/// required, and its total quantity covers its filled part; `px` is absent for an order
/// without a price.
pub(super) fn snapshot(
    r: &Record<'_>,
    scope: &DecodeScope<'_>,
    specs: &SpecTable,
) -> Result<VenueOrderSnapshot, DecodeError> {
    let (qty, cum_filled) = (r.lots("qty")?, r.lots("cum")?);
    if cum_filled > qty {
        return Err(Malformed("cum"));
    }
    Ok(VenueOrderSnapshot {
        cid: Some(scope.client_order_id(r.get("cid")?)),
        vid: scope.venue_order_id(r.get("vid")?)?,
        inst: r.spec(specs)?.id,
        side: r.side()?,
        state: state(r, scope)?,
        px: r.opt_num("px")?.map(Ticks),
        qty,
        cum_filled,
        post_only: Some(r.flag("po")?),
        reduce_only: Some(r.flag("ro")?),
    })
}

/// A position: its market `sym`, signed quantity `qty` and, when sent, average entry `avg`.
pub(super) fn position(r: &Record<'_>, specs: &SpecTable) -> Result<ExecEvent, DecodeError> {
    Ok(ExecEvent::ResyncPosition {
        inst: r.spec(specs)?.id,
        qty: SignedLots(r.num("qty")?),
        avg_entry: r.opt_num::<PxExact>("avg")?,
    })
}

fn mode(r: &Record<'_>, specs: &SpecTable) -> Result<ExecEvent, DecodeError> {
    let scope = match r.opt("sym") {
        Some(_) => ModeScope::Instrument(r.spec(specs)?.id),
        None => ModeScope::Account,
    };
    let mode = match r.get("m")? {
        "normal" => VenueMode::Normal,
        "postonly" => VenueMode::PostOnly,
        "reduceonly" => VenueMode::ReduceOnly,
        "cancelonly" => VenueMode::CancelOnly,
        "halted" => VenueMode::Halted,
        _ => return Err(Malformed("m")),
    };
    Ok(ExecEvent::Mode { scope, mode })
}

/// One record: its kind and its fields in order.
pub(super) struct Record<'a> {
    pub(super) kind: &'a str,
    fields: Vec<(&'a str, &'a str)>,
}

impl<'a> Record<'a> {
    fn parse(line: &'a str) -> Result<Record<'a>, DecodeError> {
        let mut parts = line.split('|');
        let kind = parts.next().filter(|kind| !kind.is_empty());
        let kind = kind.ok_or(Malformed("kind"))?;
        let fields = parts.map(|part| part.split_once('=').ok_or(Malformed("field")));
        let fields = fields.collect::<Result<_, _>>()?;
        Ok(Record { kind, fields })
    }

    pub(super) fn opt(&self, key: &str) -> Option<&'a str> {
        self.fields.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
    }

    pub(super) fn get(&self, key: &'static str) -> Result<&'a str, DecodeError> {
        self.opt(key).ok_or(Malformed(key))
    }

    pub(super) fn num<T: FromStr>(&self, key: &'static str) -> Result<T, DecodeError> {
        self.get(key)?.parse().map_err(|_| Malformed(key))
    }

    pub(super) fn opt_num<T: FromStr>(&self, key: &'static str) -> Result<Option<T>, DecodeError> {
        self.opt(key).map(|_| self.num(key)).transpose()
    }

    /// A required count; a negative one is malformed.
    pub(super) fn lots(&self, key: &'static str) -> Result<Lots, DecodeError> {
        Lots::new(self.num(key)?).ok_or(Malformed(key))
    }

    /// An optional count: `None` when absent, malformed when present and negative.
    fn opt_lots(&self, key: &'static str) -> Result<Option<Lots>, DecodeError> {
        self.opt(key).map(|_| self.lots(key)).transpose()
    }

    /// `0` or `1`.
    pub(super) fn flag(&self, key: &'static str) -> Result<bool, DecodeError> {
        match self.get(key)? {
            "0" => Ok(false),
            "1" => Ok(true),
            _ => Err(Malformed(key)),
        }
    }

    fn side(&self) -> Result<Side, DecodeError> {
        match self.get("side")? {
            "B" => Ok(Side::Buy),
            "S" => Ok(Side::Sell),
            _ => Err(Malformed("side")),
        }
    }

    fn spec<'s>(&self, specs: &'s SpecTable) -> Result<&'s InstrumentSpec, DecodeError> {
        let spec = specs.by_symbol(self.get("sym")?);
        spec.ok_or(DecodeError::UnknownInstrument)
    }

    /// An amount in nanos of the instrument's settlement asset, positive for a gain.
    fn money(&self, key: &'static str, spec: &InstrumentSpec) -> Result<Money, DecodeError> {
        Ok(Money::new(self.num(key)?, spec.settle_ccy))
    }

    /// The venue's time when it sends one, and its sequence, required on order and fill events
    /// (the toy's ordering key is its sequence).
    pub(super) fn meta(&self, sequenced: bool) -> Result<VenueMeta, DecodeError> {
        let exch_ts = self.opt_num("ts")?.map(ExchNs);
        let exch_ts_kind = match exch_ts {
            Some(_) => ExchTsKind::MatchingEngine,
            None => ExchTsKind::Unknown,
        };
        let venue_seq = match sequenced {
            true => Some(self.num("seq")?),
            false => self.opt_num("seq")?,
        };
        Ok(VenueMeta {
            exch_ts,
            exch_ts_kind,
            venue_seq,
        })
    }
}
