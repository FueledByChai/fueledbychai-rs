//! Normalized events: what a venue adapter's codecs turn bytes and HTTP results into
//! (decisions 0002, 0005 and 0014, design §4.6).
//!
//! Every event travels in an [`Envelope`]: the runtime's [`Stamp`] (where and when the frame
//! arrived, recorded before decode) and what the venue said about it (exchange time and its
//! kind, venue sequence; [`VenueMeta`]). A codec never builds a stamp: it reports a body and
//! its [`VenueMeta`] to a sink ([`MdSink`](crate::MdSink), [`ExecSink`](crate::ExecSink)) and
//! the runtime stamps it, so a decoder cannot forge receive time or ingest order. Exchange time
//! is never synthesized: a venue that sends none gives `exch_ts: None`.
//!
//! [`MdEvent`] is market data; [`ExecEvent`] is everything an account's order-entry session
//! reports: per-item command outcomes (`Unknown` included: sent, no answer, never resent;
//! decision 0005), order updates with the venue's cumulative filled quantity, asynchronous
//! rejects, incremental fills, positions, balances, funding payments, venue mode and
//! connection state, the resync sequence, query results, fee rates and errors that name no
//! order. Venue order ids, fill ids and fees inside them exist only because a codec built them
//! through [`DecodeScope`](crate::DecodeScope) (decision 0004).
//!
//! # Signs
//!
//! A [`Fee`] is a cost (positive when we paid; decision 0004). The other money amounts here are
//! P&L to us, positive for a gain: [`FillEvent::realized_pnl`], [`FillEvent::realized_funding`]
//! and the amount of [`ExecEvent::FundingPaid`] are positive when we gained (received funding)
//! and negative when we lost (paid it). They are present only when the venue reports them
//! ([`FillCaps`](crate::FillCaps) says which it does); a codec converts the venue's own sign to
//! this one and never computes a value the venue did not send.

use core::time::Duration;

use crate::caps::OpKind;
use crate::codec::Feed;
use crate::command::{Reject, SubmitOutcome, TerminalReject};
use crate::fee::{Fee, FeeRate};
use crate::ids::{CidMatch, ClientOrderId, FillId, InstrumentId, OrderRef, VenueOrderId};
use crate::time::{ExchNs, ExchTsKind, Stamp, WallNs};
use crate::units::{
    Aggressor, BookSide, Channel, Liquidity, Lots, Money, PxExact, Side, SignedLots, Ticks,
};

/// An event with the runtime's stamp and what the venue said about its time and order.
#[derive(Clone, PartialEq, Debug)]
pub struct Envelope<B> {
    /// Where and when the frame carrying it arrived; set by the runtime, never by a codec.
    pub stamp: Stamp,
    /// The venue's timestamp, `None` when the venue sent none; never synthesized.
    pub exch_ts: Option<ExchNs>,
    /// Which instant `exch_ts` marks.
    pub exch_ts_kind: ExchTsKind,
    /// The venue's sequence number, when it sends one.
    pub venue_seq: Option<u64>,
    /// The event.
    pub body: B,
}

impl<B> Envelope<B> {
    /// The event `body`, reported with `meta`, stamped with `stamp`.
    pub fn new(stamp: Stamp, meta: VenueMeta, body: B) -> Envelope<B> {
        Envelope {
            stamp,
            exch_ts: meta.exch_ts,
            exch_ts_kind: meta.exch_ts_kind,
            venue_seq: meta.venue_seq,
            body,
        }
    }

    /// What the venue said about the event's time and order.
    pub fn meta(&self) -> VenueMeta {
        VenueMeta {
            exch_ts: self.exch_ts,
            exch_ts_kind: self.exch_ts_kind,
            venue_seq: self.venue_seq,
        }
    }
}

/// What a venue said about an event's time and order, as a codec reports it to a sink.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct VenueMeta {
    /// The venue's timestamp, `None` when it sent none.
    pub exch_ts: Option<ExchNs>,
    /// Which instant `exch_ts` marks.
    pub exch_ts_kind: ExchTsKind,
    /// The venue's sequence number, when it sends one.
    pub venue_seq: Option<u64>,
}

impl VenueMeta {
    /// Nothing from the venue: for events a codec raises itself (an RPC timeout's `Unknown`).
    pub const NONE: VenueMeta = VenueMeta {
        exch_ts: None,
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: None,
    };
}

/// One price level: a price and the quantity resting at it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Lvl {
    pub px: Ticks,
    pub qty: Lots,
}

/// Which of a venue's touch channels an event came from: an index into
/// [`MdCaps::touch_sources`](crate::MdCaps::touch_sources).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TouchSourceId(pub u8);

/// Which of a venue's book channels an event came from: an index into
/// [`MdCaps::books`](crate::MdCaps::books). One instrument can be subscribed to several book
/// channels at once (a recorder keeping a public and an interactive book), so every book event
/// names its channel and two channels never merge into one book.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct BookId(pub u8);

/// The state of one instrument's market-data feed, as its codec sees it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FeedHealth {
    /// Continuous: no gap since the last snapshot.
    Live,
    /// A gap was detected; the book is invalid until the next snapshot.
    Gap,
    /// Nothing arrived within the feed's expected cadence.
    Stale,
}

/// A market-data event. Prices on the book are [`Ticks`] on the instrument's finest grid;
/// mark and index are exact ([`PxExact`]) and never rounded to the grid.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum MdEvent {
    /// The best bid and offer from one touch channel; `None` for an empty side.
    Touch {
        inst: InstrumentId,
        bid: Option<Lvl>,
        ask: Option<Lvl>,
        source: TouchSourceId,
    },
    /// A snapshot of book channel `book` begins: the levels that follow replace that book,
    /// under `epoch`.
    BookSnapshotBegin {
        inst: InstrumentId,
        book: BookId,
        epoch: u32,
    },
    /// The snapshot of book channel `book` is complete.
    BookSnapshotEnd { inst: InstrumentId, book: BookId },
    /// One level of book channel `book` set to `qty` (zero removes it), in a snapshot or as a
    /// delta.
    Level {
        inst: InstrumentId,
        book: BookId,
        side: BookSide,
        px: Ticks,
        qty: Lots,
    },
    /// Windowed book channel `book`'s levels now cover `lo..=hi`; anything outside is unknown.
    Window {
        inst: InstrumentId,
        book: BookId,
        lo: Ticks,
        hi: Ticks,
    },
    /// A public trade.
    Trade {
        inst: InstrumentId,
        /// The venue's trade id, when it sends one.
        id: Option<u64>,
        aggressor: Aggressor,
        px: Ticks,
        qty: Lots,
    },
    /// The mark price.
    Mark { inst: InstrumentId, px: PxExact },
    /// The index price.
    Index { inst: InstrumentId, px: PxExact },
    /// The funding rate, in units of 1e-12 per interval.
    Funding {
        inst: InstrumentId,
        rate_e12: i64,
        /// The interval the rate is for, when the venue states it.
        interval: Option<Duration>,
        /// The next funding time, when the venue states it.
        next: Option<WallNs>,
    },
    /// Volume and open interest, each when the venue reports it.
    Stats {
        inst: InstrumentId,
        volume_24h_quote: Option<Money>,
        oi: Option<Lots>,
    },
    /// One feed's health changed: a gap on the book says nothing about the trades.
    Health {
        inst: InstrumentId,
        feed: Feed,
        h: FeedHealth,
    },
}

/// One request id: names a command's request from encode to its outcome or timeout.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct RpcId(pub u64);

/// One stream (connection) of a venue session.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct StreamId(pub u16);

/// The state of one stream.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ConnState {
    /// The connection is being opened.
    Connecting,
    /// Open, not yet authenticated (or needing no authentication).
    Open,
    /// Authenticated and ready for the session's traffic.
    Authenticated,
    /// Closed.
    Closed,
}

/// What a venue mode applies to.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ModeScope {
    /// Every market on the account.
    Account,
    /// One market; the account's other markets are unaffected.
    Instrument(InstrumentId),
}

/// A mode the venue puts the whole account, or one market, into ([`ModeScope`]).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum VenueMode {
    /// Orders of every kind are accepted.
    Normal,
    /// Only post-only orders are accepted.
    PostOnly,
    /// Only reducing orders are accepted.
    ReduceOnly,
    /// Only cancels are accepted.
    CancelOnly,
    /// Nothing is accepted (maintenance, halt).
    Halted,
}

/// Which item of a request an outcome is for.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ItemRef {
    /// The item's position in the request (0 for a single command).
    pub idx: u16,
    /// Its client id, when the reply names it.
    pub cid: Option<ClientOrderId>,
    /// Its venue order id, when the reply names it.
    pub vid: Option<VenueOrderId>,
}

/// Why an order was cancelled.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CancelReason {
    /// We asked.
    Requested,
    /// The venue's cancel-on-disconnect or dead-man timer fired.
    Disconnect,
    /// Self-trade prevention.
    SelfTrade,
    /// A post-only order would have crossed.
    PostOnly,
    /// A reduce-only order would have increased the position.
    ReduceOnly,
    /// The unfilled rest of an immediate-or-cancel or fill-or-kill order.
    Unfilled,
    /// A liquidation.
    Liquidation,
    /// The venue cancelled it for another or an unstated reason.
    Venue,
}

/// An order's state as the venue reports it.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum VenueOrderState {
    /// Resting (partly filled or not; the cumulative filled quantity says which).
    Open,
    Filled,
    Canceled(CancelReason),
    /// Refused for good; never for not knowing the order ([`TerminalReject`]).
    Rejected(TerminalReject),
    Expired,
    /// Amended, under `new_vid` where the venue issues a new order id for the amended order.
    /// The new price and total (filled part included) are the event's own `px` and `qty`,
    /// stated once there; `None` where the venue does not echo them.
    Amended {
        new_vid: Option<VenueOrderId>,
    },
}

/// An order event.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct OrderUpdate {
    /// The client id read off the wire: ours, another namespace's, or not canonical; `None`
    /// when the event carries none ([`OrderCaps::cid_echoed_on_events`](crate::OrderCaps)).
    pub cid: Option<CidMatch>,
    pub vid: Option<VenueOrderId>,
    pub inst: InstrumentId,
    pub side: Side,
    pub state: VenueOrderState,
    /// The venue's CUMULATIVE filled quantity.
    pub cum_filled: Lots,
    /// The order's price, when the event carries it.
    pub px: Option<Ticks>,
    /// The order's total quantity (filled part included), when the event carries it.
    pub qty: Option<Lots>,
    /// Post-only, when the venue echoes it ([`OrderCaps::events_echo_flags`](crate::OrderCaps)).
    pub post_only: Option<bool>,
    /// Reduce-only, when the venue echoes it.
    pub reduce_only: Option<bool>,
}

/// The key a fill is deduplicated by (decision 0005, I3). It is computed from a fill's
/// [`FillIdent`] by [`FillEvent::key`], never stated beside it, so it cannot disagree with the
/// order and quantity the fill is applied to.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum FillKey {
    /// The venue's fill id.
    Venue(FillId),
    /// No fill id: the venue order id and the cumulative quantity after the fill.
    Derived { vid: VenueOrderId, cum_after: Lots },
}

/// What a fill names about itself and its order, each stated once.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum FillIdent {
    /// The venue's fill id, with the order's venue id and cumulative filled quantity after the
    /// fill when the venue sends them.
    Venue {
        fill: FillId,
        vid: Option<VenueOrderId>,
        cum_after: Option<Lots>,
    },
    /// No fill id ([`FillCaps::fill_id`](crate::FillCaps::fill_id) false): the order's venue id
    /// and cumulative filled quantity after the fill, both required, which key the fill.
    Derived { vid: VenueOrderId, cum_after: Lots },
}

/// Whether a fill made or took liquidity, or the venue does not say.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Liquidity3 {
    Maker,
    Taker,
    /// The venue does not say ([`FillCaps::liquidity_flag`](crate::FillCaps::liquidity_flag)).
    Unknown,
}

impl Liquidity3 {
    /// The liquidity, when known.
    pub fn known(self) -> Option<Liquidity> {
        match self {
            Liquidity3::Maker => Some(Liquidity::Maker),
            Liquidity3::Taker => Some(Liquidity::Taker),
            Liquidity3::Unknown => None,
        }
    }
}

/// One fill of one of the account's orders.
#[derive(Clone, PartialEq, Debug)]
pub struct FillEvent {
    /// The fill id or, without one, the order id and cumulative quantity that key the fill;
    /// read through [`key`](FillEvent::key), [`vid`](FillEvent::vid) and
    /// [`cum_after`](FillEvent::cum_after).
    pub ident: FillIdent,
    /// The client id read off the wire; `None` when the fill carries none.
    pub cid: Option<CidMatch>,
    pub inst: InstrumentId,
    pub side: Side,
    pub px: Ticks,
    /// The quantity of THIS fill (incremental).
    pub qty: Lots,
    pub liquidity: Liquidity3,
    /// The fee, as a cost to us (decision 0004).
    pub fee: Fee,
    /// The venue's realized P&L for the fill, positive for a gain; `None` when it reports none.
    pub realized_pnl: Option<Money>,
    /// The venue's realized funding for the fill, positive when received; `None` when it
    /// reports none.
    pub realized_funding: Option<Money>,
    /// A fill the venue sent again (a snapshot or a replay after reconnect): it only
    /// reconciles, never moves inventory twice (decision 0005, I3).
    pub replay: bool,
}

impl FillEvent {
    /// What the fill is deduplicated by: the venue's fill id, or the order id and cumulative
    /// quantity after the fill.
    pub fn key(&self) -> FillKey {
        match &self.ident {
            FillIdent::Venue { fill, .. } => FillKey::Venue(fill.clone()),
            FillIdent::Derived { vid, cum_after } => FillKey::Derived {
                vid: vid.clone(),
                cum_after: *cum_after,
            },
        }
    }

    /// The order's venue id, when the fill names it.
    pub fn vid(&self) -> Option<&VenueOrderId> {
        match &self.ident {
            FillIdent::Venue { vid, .. } => vid.as_ref(),
            FillIdent::Derived { vid, .. } => Some(vid),
        }
    }

    /// The order's cumulative filled quantity after this fill, when the venue reports it.
    pub fn cum_after(&self) -> Option<Lots> {
        match &self.ident {
            FillIdent::Venue { cum_after, .. } => *cum_after,
            FillIdent::Derived { cum_after, .. } => Some(*cum_after),
        }
    }
}

/// One order as a venue snapshot or query reports it.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct VenueOrderSnapshot {
    /// The client id read off the wire; `None` when the snapshot carries none.
    pub cid: Option<CidMatch>,
    pub vid: VenueOrderId,
    pub inst: InstrumentId,
    pub side: Side,
    pub state: VenueOrderState,
    /// The order's price; `None` for an order without one (a market order).
    pub px: Option<Ticks>,
    /// Its total quantity, filled part included.
    pub qty: Lots,
    /// The venue's cumulative filled quantity.
    pub cum_filled: Lots,
    /// Post-only, when the venue echoes it.
    pub post_only: Option<bool>,
    /// Reduce-only, when the venue echoes it.
    pub reduce_only: Option<bool>,
}

/// An event from an account's order-entry session.
#[derive(Clone, PartialEq, Debug)]
pub enum ExecEvent {
    /// The outcome of one item of a request; `item: None` applies to every item of the
    /// request (a timeout).
    Outcome {
        rpc: RpcId,
        item: Option<ItemRef>,
        outcome: SubmitOutcome,
    },
    /// An order event.
    Order(OrderUpdate),
    /// A refusal that arrives after the request was accepted (a two-phase venue's risk check).
    AsyncReject {
        target: OrderRef,
        op: OpKind,
        reject: Reject,
    },
    /// A fill.
    Fill(FillEvent),
    /// The account's position in an instrument.
    Position {
        inst: InstrumentId,
        qty: SignedLots,
        avg_entry: Option<PxExact>,
    },
    /// The account's equity and available margin.
    Balance { equity: Money, available: Money },
    /// A funding payment: P&L, positive when we received funding and negative when we paid.
    FundingPaid { inst: InstrumentId, amount: Money },
    /// The venue's mode changed, for the whole account or for one market.
    Mode { scope: ModeScope, mode: VenueMode },
    /// A stream's state changed.
    Conn { stream: StreamId, state: ConnState },
    /// A resync begins; the snapshot reflects the venue at `watermark` (venue-clock aligned).
    ResyncBegin { watermark: WallNs },
    /// One open order of the resync snapshot.
    ResyncOrder(VenueOrderSnapshot),
    /// One position of the resync snapshot.
    ResyncPosition {
        inst: InstrumentId,
        qty: SignedLots,
        avg_entry: Option<PxExact>,
    },
    /// The resync snapshot is complete.
    ResyncEnd,
    /// The answer to an order query; `found: None` when the venue does not know the order.
    QueryResult {
        target: OrderRef,
        found: Option<VenueOrderSnapshot>,
    },
    /// The account's fee rates.
    FeeRates(Vec<(InstrumentId, Channel, Liquidity, FeeRate)>),
    /// An error that names no order or request.
    UncorrelatedError(Reject),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::{ConnKey, MonoNs};

    #[test]
    fn an_envelope_keeps_the_stamp_and_what_the_venue_said() {
        let stamp = Stamp {
            ingest_seq: 1,
            kernel_rx: None,
            recv_mono: MonoNs(2),
            recv_wall: WallNs(3),
            conn: ConnKey { conn: 0, epoch: 1 },
        };
        let meta = VenueMeta {
            exch_ts: Some(ExchNs(4)),
            exch_ts_kind: ExchTsKind::Publish,
            venue_seq: Some(5),
        };
        let health = MdEvent::Health {
            inst: InstrumentId::new(1),
            feed: Feed::Trades,
            h: FeedHealth::Stale,
        };
        let env = Envelope::new(stamp, meta, health);
        assert_eq!((env.stamp, env.meta(), env.body), (stamp, meta, health));
        let bare = Envelope::new(stamp, VenueMeta::NONE, ExecEvent::ResyncEnd);
        assert_eq!((bare.exch_ts, bare.venue_seq), (None, None));
        assert_eq!(bare.exch_ts_kind, ExchTsKind::Unknown);
    }

    #[test]
    fn liquidity_is_known_only_when_the_venue_says() {
        assert_eq!(Liquidity3::Maker.known(), Some(Liquidity::Maker));
        assert_eq!(Liquidity3::Taker.known(), Some(Liquidity::Taker));
        assert_eq!(Liquidity3::Unknown.known(), None);
    }
}
