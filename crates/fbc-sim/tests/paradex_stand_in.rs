//! FBC-2zxk's done line: a SimVenue built from fbc-venue-paradex's declared `ExecCaps` and
//! `MatchingCaps` (decision 0054's caps, as `fbc_venue_paradex::factory::caps` declares them)
//! acknowledges a post-only place and fills it through the queue model, with realized P&L as
//! Paradex reports it; an amend and a cancel are answered; and no command is refused as
//! `Unsupported`. Driven through the codec and engine only, as `sim_venue.rs` drives them, with
//! no runtime and no socket. The instrument, its book and the account's rates are synthetic.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, AckLevel, AckModel, Aggressor, AmendOrder, AssetSym, BookId, BookSide, Bps,
    CancelOrder, CancelReason, CancelScope, Channel, CidMatch, CidMint, ClientOrderId, ConnKey,
    Effect, Effects, EncodeCtx, Envelope, ExecCodec, ExecEvent, ExecSink, FeeBook, FeeEntry,
    FeeKey, FeeRate, FeeSource, Feed, FeedHealth, FillEvent, FundingSpec, InstrumentId,
    InstrumentKind, InstrumentSpec, ItemRef, Liquidity, Liquidity3, Lots, MdEvent, MonoNs,
    Namespace, NamespaceLease, NewOrder, NonceBlock, NotSentReason, OrderKind, OrderRef,
    OrderUpdate, PathStamps, PriceGrid, QueryOrder, RawFrame, RpcId, Side, SizeStep, SpecTable,
    Stamp, StreamId, SubmitOutcome, Ticks, Tif, TifTag, TradingStatus, UnderlyingId, VenueCaps,
    VenueCommand, VenueId, VenueMeta, VenueOrderId, VenueOrderState, WallNs, dispatch,
};
use fbc_sim::{Bracket, QueueConfig, SimCodec, SimConfig, SimEngine, SimLatency};
use fbc_venue_paradex::exec::exec_caps;
use fbc_venue_paradex::factory::caps;
use rust_decimal::Decimal;

const NS: Namespace = Namespace::new(11);
const INST: InstrumentId = InstrumentId::new(1);
const BOOK: BookId = BookId(0);
const STREAM: StreamId = StreamId(7);
const ACCOUNT: AccountKey = AccountKey::new(5);
const MS: u64 = 1_000_000;
const T0: u64 = 1_000 * MS;
const WALL0: i64 = 1_760_000_000_000_000_000;
const TO_VENUE: Duration = Duration::from_millis(5);
const TO_CLIENT: Duration = Duration::from_millis(3);
/// Synthetic rates, in basis points of the notional, positive a cost.
const MAKER_BPS: f64 = 0.3;
const TAKER_BPS: f64 = 2.0;

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn usdc() -> AssetSym {
    AssetSym::new("USDC").unwrap()
}

/// A fresh client id, minted under a namespace lease held for the whole test binary.
fn cid() -> ClientOrderId {
    static MINT: OnceLock<Mutex<CidMint>> = OnceLock::new();
    let mint = MINT.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-sim-paradex-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), NS).unwrap();
        Mutex::new(CidMint::new(lease, 0, 0, WallNs(0)))
    });
    mint.lock().unwrap().mint().unwrap()
}

/// Paradex's caps, order entry included, as its factory declares them.
fn paradex() -> VenueCaps {
    caps()
}

/// A linear perpetual settled in USDC, as Paradex lists them, on a synthetic 0.5 tick and 0.5
/// size step.
fn spec() -> InstrumentSpec {
    let venue_symbol =
        dispatch(&paradex(), NS, |scope| scope.venue_symbol("BTC-USD-PERP")).unwrap();
    let half = Decimal::new(5, 1);
    InstrumentSpec {
        id: INST,
        venue: VenueId::new(1),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed(half).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new(half).unwrap(),
        min_size: lots(1),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usdc(),
        settle_ccy: usdc(),
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    }
}

fn fees() -> FeeBook {
    let mut book = FeeBook::new();
    for (liquidity, bps) in [(Liquidity::Maker, MAKER_BPS), (Liquidity::Taker, TAKER_BPS)] {
        let key = FeeKey {
            account: ACCOUNT,
            instrument: INST,
            channel: Channel::Public,
            liquidity,
        };
        let entry = FeeEntry {
            rate: FeeRate(Bps(bps)),
            source: FeeSource::ConfigOverride,
            as_of: WallNs(0),
        };
        book.insert(key, entry);
    }
    book
}

/// SimVenue standing in for Paradex: its exec and matching caps exactly as declared.
fn config() -> SimConfig {
    let venue = paradex();
    let mut specs = SpecTable::new();
    specs.insert(spec());
    SimConfig {
        exec: exec_caps(),
        matching: venue.matching,
        latency: SimLatency {
            to_venue: TO_VENUE,
            to_client: TO_CLIENT,
        },
        rpc_timeout: Duration::from_secs(2),
        stream: STREAM,
        queue: QueueConfig {
            bracket: Bracket::Pessimistic,
        },
        account: ACCOUNT,
        fees: fees(),
        specs,
        books: BTreeMap::from([(INST, BOOK)]),
    }
}

/// Paradex's risk-reject window, as its caps declare it.
fn window() -> u64 {
    let AckModel::TwoPhase { risk_reject_window } = exec_caps().order.ack else {
        panic!("Paradex declares a two-phase acknowledgement (decision 0054)")
    };
    u64::try_from(risk_reject_window.as_nanos()).unwrap()
}

#[derive(Default)]
struct Sink(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// The codec and engine, joined by hand as the host joins them.
struct Venue {
    codec: SimCodec,
    engine: SimEngine,
    caps: VenueCaps,
    specs: SpecTable,
    ingest: u64,
}

impl Venue {
    fn new() -> Venue {
        let config = config();
        let mut v = Venue {
            codec: SimCodec::new(&config),
            engine: SimEngine::new(&config),
            caps: paradex(),
            specs: config.specs.clone(),
            ingest: 0,
        };
        v.engine.open(MonoNs(0)).unwrap();
        let mut fx = Effects::new();
        v.codec.on_open(STREAM, &ctx(0), &mut fx);
        assert!(fx.is_empty(), "{fx:?}");
        v
    }

    /// Encodes `cmd` at `mono`: refused, or the frames it writes.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: u64,
        mono: u64,
    ) -> Result<Effects, NotSentReason> {
        let mut fx = Effects::new();
        let t = &mut PathStamps::off();
        self.codec
            .encode(cmd, RpcId(rpc), &self.specs, &ctx(mono), t, &mut fx)?;
        Ok(fx)
    }

    /// Encodes `cmd` at `mono`, which Paradex's caps must not refuse, and hands its frame to
    /// the engine.
    fn send(&mut self, cmd: VenueCommand, rpc: u64, mono: u64) {
        let fx = self.encode(&cmd, rpc, mono).unwrap();
        for effect in fx.as_slice() {
            let Effect::Send { stream, frame, .. } = effect else {
                panic!("the codec only writes frames: {effect:?}");
            };
            assert_eq!(*stream, STREAM);
            self.engine.on_order_frame(frame.bytes()).unwrap();
        }
    }

    fn md(&mut self, mono: u64, body: MdEvent) {
        let stamp = Stamp {
            ingest_seq: self.ingest,
            kernel_rx: None,
            recv_mono: MonoNs(mono),
            recv_wall: WallNs(WALL0 + mono as i64),
            conn: ConnKey { conn: 0, epoch: 1 },
        };
        self.ingest += 1;
        let env = Envelope::new(stamp, VenueMeta::NONE, body);
        self.engine.on_market(&env).unwrap();
    }

    fn snapshot(&mut self, mono: u64, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        let begin = MdEvent::BookSnapshotBegin {
            inst: INST,
            book: BOOK,
            epoch: 1,
        };
        self.md(mono, begin);
        for (side, levels) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
            for &(px, qty) in levels {
                self.level(mono, side, px, qty);
            }
        }
        self.md(
            mono,
            MdEvent::BookSnapshotEnd {
                inst: INST,
                book: BOOK,
            },
        );
    }

    fn level(&mut self, mono: u64, side: BookSide, px: i64, qty: i64) {
        let ev = MdEvent::Level {
            inst: INST,
            book: BOOK,
            side,
            px: Ticks(px),
            qty: lots(qty),
        };
        self.md(mono, ev);
    }

    fn trade(&mut self, mono: u64, aggressor: Aggressor, px: i64, qty: i64) {
        let ev = MdEvent::Trade {
            inst: INST,
            id: None,
            aggressor,
            px: Ticks(px),
            qty: lots(qty),
        };
        self.md(mono, ev);
    }

    /// Moves the engine's time only.
    fn tick(&mut self, mono: u64) {
        let ev = MdEvent::Health {
            inst: INST,
            feed: Feed::Trades,
            h: FeedHealth::Live,
        };
        self.md(mono, ev);
    }

    /// Every answer since the last call, decoded through Paradex's decode scope, with its due
    /// time; each is ordered by Paradex's key, the venue sequence.
    fn events(&mut self) -> Vec<(MonoNs, ExecEvent)> {
        let mut out = Vec::new();
        for answer in self.engine.take_answers() {
            let mut sink = Sink::default();
            let mut fx = Effects::new();
            let (codec, specs) = (&mut self.codec, &self.specs);
            let frame = std::str::from_utf8(&answer.frame).unwrap();
            dispatch(&self.caps, NS, |scope| {
                codec.on_frame(
                    STREAM,
                    RawFrame::Text(frame),
                    scope,
                    specs,
                    &mut sink,
                    &mut fx,
                )
            })
            .unwrap();
            assert!(fx.is_empty());
            for (meta, ev) in sink.0 {
                assert!(meta.venue_seq.is_some(), "{ev:?}");
                out.push((answer.at, ev));
            }
        }
        out
    }
}

fn ctx(mono: u64) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(WALL0 + mono as i64),
        mono: MonoNs(mono),
        nonces: NonceBlock::EMPTY,
    }
}

fn order(cid: ClientOrderId, side: Side, kind: OrderKind, qty: i64, tif: Tif) -> NewOrder {
    NewOrder {
        cid,
        inst: INST,
        side,
        qty: lots(qty),
        kind,
        tif,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
    }
}

/// A post-only good-till-cancelled limit order.
fn post_only(cid: ClientOrderId, side: Side, px: i64, qty: i64) -> NewOrder {
    let limit = OrderKind::Limit { px: Ticks(px) };
    NewOrder {
        post_only: true,
        ..order(cid, side, limit, qty, TifTag::Gtc)
    }
}

fn cancel_of(target: OrderRef, side: Side) -> CancelOrder {
    CancelOrder {
        target,
        inst: INST,
        side,
        placement_nonce: None,
    }
}

fn amend_of(target: OrderRef, side: Side, px: i64, qty: i64) -> AmendOrder {
    AmendOrder {
        target,
        inst: INST,
        side,
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(px),
        qty: lots(qty),
        cum_filled: lots(0),
    }
}

const PROVISIONAL: SubmitOutcome = SubmitOutcome::Accepted {
    ack: AckLevel::Provisional,
};
const FINAL: SubmitOutcome = SubmitOutcome::Accepted {
    ack: AckLevel::Final,
};

/// An outcome's rpc, item and outcome.
fn outcome(ev: &ExecEvent) -> (u64, Option<ItemRef>, SubmitOutcome) {
    let ExecEvent::Outcome { rpc, item, outcome } = ev else {
        panic!("{ev:?}")
    };
    (rpc.0, item.clone(), outcome.clone())
}

fn update(ev: &ExecEvent) -> &OrderUpdate {
    let ExecEvent::Order(o) = ev else {
        panic!("{ev:?}")
    };
    o
}

fn fill(ev: &ExecEvent) -> &FillEvent {
    let ExecEvent::Fill(f) = ev else {
        panic!("{ev:?}")
    };
    f
}

/// A venue order id, as Paradex's decode scope builds one.
fn venue_id(wire: &str) -> VenueOrderId {
    dispatch(&paradex(), NS, |scope| scope.venue_order_id(wire)).unwrap()
}

/// The notional of `qty` lots at `px`, in nanos of USDC, from the spec.
fn notional(px: i64, qty: i64) -> i128 {
    spec().notional(Ticks(px), lots(qty)).unwrap().nanos
}

/// The fee cost of `qty` lots at `px` at `bps`, rounded as the engine rounds it.
fn fee(px: i64, qty: i64, bps: f64) -> i128 {
    (notional(px, qty) as f64 * bps / 10_000.0).round() as i128
}

/// Places `o` at `mono` as `rpc` and checks Paradex's first phase: accepted provisionally
/// after the latency, then resting open. Returns its venue id.
fn placed(v: &mut Venue, o: NewOrder, rpc: u64, mono: u64) -> VenueOrderId {
    let (cid, px, qty) = (o.cid, o.kind.limit_px(), o.qty);
    v.send(VenueCommand::Place(o), rpc, mono);
    v.tick(mono + 5 * MS);
    let got = v.events();
    assert_eq!(got.len(), 2, "{got:?}");
    let due = MonoNs(mono) + TO_VENUE + TO_CLIENT;
    assert!(got.iter().all(|(at, _)| *at == due), "{got:?}");
    let (got_rpc, item, ack) = outcome(&got[0].1);
    assert_eq!((got_rpc, ack), (rpc, PROVISIONAL));
    let item = item.unwrap();
    assert_eq!(item.cid, Some(cid));
    let vid = item.vid.unwrap();
    let open = update(&got[1].1);
    assert_eq!(
        (open.state.clone(), open.vid.as_ref(), open.px, open.qty),
        (VenueOrderState::Open, Some(&vid), px, Some(qty))
    );
    assert_eq!(open.post_only, Some(true));
    vid
}

/// Placement `rpc`'s final acceptance, once Paradex's risk-reject window has passed since it
/// acted at `acted`.
fn finalized(v: &mut Venue, rpc: u64, vid: &VenueOrderId, acted: u64) {
    v.tick(acted + window() - 1);
    assert!(v.events().is_empty());
    v.tick(acted + window());
    let got = v.events();
    assert_eq!(got.len(), 1, "{got:?}");
    let (got_rpc, item, ack) = outcome(&got[0].1);
    assert_eq!((got_rpc, ack), (rpc, FINAL));
    assert_eq!(item.unwrap().vid.as_ref(), Some(vid));
}

#[test]
fn simvenue_from_paradexs_caps_fills_a_post_only_place_with_paradexs_realized_pnl_and_answers_an_amend_and_a_cancel()
 {
    let mut v = Venue::new();
    v.snapshot(T0, &[(199, 5), (198, 10), (197, 4)], &[(201, 5), (202, 5)]);

    // Paradex's protection, as its session arms it on every connection (0054).
    v.send(VenueCommand::ArmCancelOnDisconnect(true), 1, T0);
    v.tick(T0 + 5 * MS);
    let got = v.events();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(outcome(&got[0].1), (1, None, FINAL));

    // A post-only buy at the bid: provisional, resting behind the five lots shown there.
    let buyer = cid();
    let sent = T0 + MS;
    let buy = placed(&mut v, post_only(buyer, Side::Buy, 199, 2), 2, sent);
    assert_eq!(v.engine.queue_position(&buy).unwrap().ahead, lots(5));

    // Filled through the queue model, as the maker, while its risk window runs: the trade of
    // five takes the size ahead, the next fills it.
    v.trade(T0 + 20 * MS, Aggressor::Seller, 199, 5);
    v.level(T0 + 21 * MS, BookSide::Bid, 199, 0);
    assert!(v.events().is_empty());
    v.trade(T0 + 22 * MS, Aggressor::Seller, 199, 2);
    let got = v.events();
    assert_eq!(got.len(), 2, "{got:?}");
    let opening = fill(&got[0].1);
    assert_eq!(
        (opening.side, opening.px, opening.qty, opening.liquidity),
        (Side::Buy, Ticks(199), lots(2), Liquidity3::Maker)
    );
    assert_eq!(opening.vid(), Some(&buy));
    assert_eq!(opening.cid, Some(CidMatch::Ours(buyer)));
    // Paradex's fee sign: positive is paid (0054), so a maker fee is a cost here.
    assert_eq!(
        (opening.fee.cost().nanos, opening.fee.cost().asset),
        (fee(199, 2, MAKER_BPS), usdc())
    );
    // A fill that opens the position realizes nothing (decision 0050), in the settlement
    // currency Paradex reports its realized values in.
    let zero = Some(fbc_core::Money::new(0, usdc()));
    assert_eq!(
        (opening.realized_pnl, opening.realized_funding),
        (zero, zero)
    );
    assert_eq!(
        (
            update(&got[1].1).state.clone(),
            update(&got[1].1).cum_filled
        ),
        (VenueOrderState::Filled, lots(2))
    );
    // Paradex's second phase: final once its window has passed, whatever the order did.
    finalized(&mut v, 2, &buy, sent + 5 * MS);

    // A post-only sell at the ask closes the position: its fill carries the price P&L the
    // two fills realize, positive for a gain, as Paradex's FillEvent realizedPnl states it.
    let seller = cid();
    let sent = T0 + 6 * MS + window() + 10 * MS;
    let sell = placed(&mut v, post_only(seller, Side::Sell, 201, 2), 3, sent);
    v.trade(sent + 20 * MS, Aggressor::Buyer, 201, 5);
    v.level(sent + 21 * MS, BookSide::Ask, 201, 0);
    v.trade(sent + 22 * MS, Aggressor::Buyer, 201, 2);
    let got = v.events();
    assert_eq!(got.len(), 2, "{got:?}");
    let closing = fill(&got[0].1);
    assert_eq!(
        (closing.side, closing.px, closing.qty, closing.vid()),
        (Side::Sell, Ticks(201), lots(2), Some(&sell))
    );
    let gain = notional(201, 2) - notional(199, 2);
    assert!(gain > 0);
    assert_eq!(
        closing.realized_pnl,
        Some(fbc_core::Money::new(gain, usdc()))
    );
    assert_eq!(closing.realized_funding, zero);
    assert_eq!(update(&got[1].1).state, VenueOrderState::Filled);
    finalized(&mut v, 3, &sell, sent + 5 * MS);

    // An amend, by the venue id Paradex's order.modify takes: answered provisionally, as
    // Paradex's modify reply is, then settled by the order event reporting the amended order
    // under the same venue id (AmendAck::ReplacedEvent), queued afresh at its new price.
    let quoter = cid();
    let sent = sent + 6 * MS + window() + 10 * MS;
    let quote = placed(&mut v, post_only(quoter, Side::Buy, 198, 2), 4, sent);
    let change = amend_of(OrderRef::Both(quoter, quote.clone()), Side::Buy, 197, 3);
    v.send(VenueCommand::Amend(change), 5, sent + 10 * MS);
    v.tick(sent + 15 * MS);
    let got = v.events();
    assert_eq!(got.len(), 2, "{got:?}");
    let (rpc, item, ack) = outcome(&got[0].1);
    assert_eq!((rpc, ack), (5, PROVISIONAL));
    assert_eq!(item.unwrap().vid.as_ref(), Some(&quote));
    let amended = update(&got[1].1);
    assert_eq!(
        (amended.state.clone(), amended.vid.as_ref()),
        (VenueOrderState::Amended { new_vid: None }, Some(&quote))
    );
    assert_eq!((amended.px, amended.qty), (Some(Ticks(197)), Some(lots(3))));
    assert_eq!(v.engine.queue_position(&quote).unwrap().ahead, lots(4));

    // A cancel, by our client id (order.cancel takes it with the market): answered, and the
    // order ends cancelled at our request.
    let stop = cancel_of(OrderRef::Client(quoter), Side::Buy);
    v.send(VenueCommand::Cancel(stop), 6, sent + 20 * MS);
    v.tick(sent + 25 * MS);
    let got = v.events();
    assert_eq!(got.len(), 2, "{got:?}");
    let (rpc, item, ack) = outcome(&got[0].1);
    assert_eq!((rpc, ack), (6, FINAL));
    assert_eq!(item.unwrap().vid.as_ref(), Some(&quote));
    assert_eq!(
        update(&got[1].1).state,
        VenueOrderState::Canceled(CancelReason::Requested)
    );
    // The placement's own second phase still comes, as for every provisional acceptance.
    finalized(&mut v, 4, &quote, sent + 5 * MS);
    v.tick(sent + 100 * window());
    assert!(v.events().is_empty());
}

#[test]
fn every_order_command_paradexs_caps_offer_is_written_and_none_is_refused_unsupported() {
    let mut v = Venue::new();
    let (a, b) = (cid(), cid());
    let vid = venue_id("S0");
    let limit = OrderKind::Limit { px: Ticks(199) };
    let commands = [
        VenueCommand::ArmCancelOnDisconnect(true),
        VenueCommand::Place(post_only(a, Side::Buy, 199, 1)),
        VenueCommand::Place(order(a, Side::Buy, limit, 1, TifTag::Gtc)),
        VenueCommand::Place(order(a, Side::Sell, limit, 1, TifTag::Ioc)),
        VenueCommand::Place(order(a, Side::Sell, OrderKind::Market, 1, TifTag::Ioc)),
        VenueCommand::Place(NewOrder {
            reduce_only: true,
            ..post_only(a, Side::Sell, 201, 1)
        }),
        VenueCommand::PlaceBatch(vec![
            post_only(a, Side::Buy, 198, 1),
            post_only(b, Side::Sell, 202, 1),
        ]),
        VenueCommand::Amend(amend_of(OrderRef::Both(a, vid.clone()), Side::Buy, 197, 2)),
        VenueCommand::Cancel(cancel_of(OrderRef::Venue(vid.clone()), Side::Buy)),
        VenueCommand::Cancel(cancel_of(OrderRef::Client(a), Side::Buy)),
        VenueCommand::CancelMany(vec![
            cancel_of(OrderRef::Venue(vid.clone()), Side::Buy),
            cancel_of(OrderRef::Both(b, venue_id("S1")), Side::Sell),
        ]),
        VenueCommand::CancelAll(CancelScope::Instrument(INST)),
        VenueCommand::Query(QueryOrder {
            target: OrderRef::Client(a),
            inst: INST,
            placement_nonce: None,
        }),
        VenueCommand::ArmCancelOnDisconnect(false),
    ];
    for (rpc, cmd) in commands.iter().enumerate() {
        let fx = v.encode(cmd, rpc as u64 + 1, T0);
        let fx = fx.unwrap_or_else(|why| panic!("{cmd:?} refused: {why:?}"));
        assert_eq!(fx.as_slice().len(), 1, "{cmd:?}");
    }
    // What Paradex's own rules refuse is refused for those rules, never as unsupported: a
    // post-only immediate-or-cancel order ("Order Instructions").
    let both = NewOrder {
        post_only: true,
        ..order(a, Side::Buy, limit, 1, TifTag::Ioc)
    };
    assert_eq!(
        v.encode(&VenueCommand::Place(both), 99, T0).err(),
        Some(NotSentReason::FlagConflict)
    );
}
