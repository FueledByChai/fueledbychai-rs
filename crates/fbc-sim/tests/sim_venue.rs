//! FBC-uoo's done line: SimVenue driven through its codec and engine only, with no runtime and
//! no socket (decision 0046). A placed order is answered Accepted after the configured latency
//! and filled through the queue model by later trades; a post-only order that would cross is
//! rejected; a cancel ends the order; every fill carries a fee from the fee book, read back
//! through the decode scope; and two runs over the same envelopes and frames give identical
//! answers. Error paths are here too. The venue, its symbols and its rates are synthetic.
//!
//! FBC-nv2's done line follows them: an amend resets the order's queue position unless its caps
//! declare `keeps_priority: Some(true)`; a batch is answered with one outcome per item; a
//! cancel-many and an instrument cancel-all end exactly the orders they name or cover; a query
//! is answered with its rpc; and an injected order is excluded from a later order's queue ahead
//! and never reported as the consumer's own order.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, AckLevel, AckModel, Aggressor, AmendAck, AmendCaps, AmendOrder, AmendQty, AssetSym,
    Batch, BookId, BookSide, Bps, CancelBatch, CancelOnDisconnect, CancelOrder, CancelReason,
    CancelScope, Channel, CidMatch, CidMint, ClientIdFormat, ClientOrderId, ConnKey, ConnTopology,
    CtxCall, DecodeError, Effect, Effects, EncodeCtx, Encoding, Envelope, ExecCaps, ExecCodec,
    ExecEvent, ExecSink, Feature, FeeBook, FeeEntry, FeeKey, FeeRate, FeeSource, Feed, FeedHealth,
    FeedSource, FillCaps, FillIdent, FillSource, FundingCaps, FundingSpec, HttpFailure, HttpTag,
    Inbound, InboundSpans, InstrumentId, InstrumentKind, InstrumentSpec, ItemRef, Liquidity,
    Liquidity3, Lots, MatchingCaps, MdCaps, MdEvent, MonoNs, Namespace, NamespaceLease, NewOrder,
    NonceBlock, NonceScope, NotAmendable, NotSentReason, OpKind, OrderCaps, OrderKind,
    OrderKindTag, OrderRef, OrderUpdate, OrderingKey, PathStamps, PriceGrid, QueryOrder,
    RateCharge, RawFrame, Readiness, RefKind, RejectKind, RpcCall, RpcId, Side, SizeStep,
    SnapshotSource, SpecTable, SpeedBump, SpeedBumpScope, Stamp, StpScope, StreamId, SubmitOutcome,
    Support, TagSet, TerminalHint, Ticks, Tif, TifTag, TimerTag, TradeCaps, TradingStatus,
    TrafficClass, UnderlyingId, VenueCaps, VenueCommand, VenueFeeSign, VenueId, VenueMeta,
    VenueOrderId, VenueOrderState, WallNs, dispatch,
};
use fbc_sim::{
    Answer, Bracket, InjectedOrder, OrderKey, QueueConfig, QueueError, SimCodec, SimConfig,
    SimEngine, SimError, SimFill, SimLatency,
};
use rust_decimal::Decimal;

const NS: Namespace = Namespace::new(9);
const INST: InstrumentId = InstrumentId::new(1);
/// An instrument the venue lists with no trading book configured.
const BOOKLESS: InstrumentId = InstrumentId::new(2);
const BOOK: BookId = BookId(0);
const STREAM: StreamId = StreamId(4);
const ACCOUNT: AccountKey = AccountKey::new(3);
const MS: u64 = 1_000_000;
/// The monotonic instant every script starts at, and the wall clock's offset from it.
const T0: u64 = 1_000 * MS;
const WALL0: i64 = 1_759_363_200_000_000_000;
const TO_VENUE: Duration = Duration::from_millis(5);
const TO_CLIENT: Duration = Duration::from_millis(3);
const TIMEOUT: Duration = Duration::from_secs(2);
/// Basis points: a maker rebate and a taker fee, as the synthetic account's rates.
const MAKER_BPS: f64 = -0.5;
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
        let dir = std::env::temp_dir().join(format!("fbc-sim-tests-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), NS).unwrap();
        Mutex::new(CidMint::new(lease, 0, 0, WallNs(0)))
    });
    mint.lock().unwrap().mint().unwrap()
}

/// The stood-in venue's capabilities, synthetic: limit and market orders, every time in
/// force, both channels, cancels by venue or client id, UUID client ids, fees in `fee_sign`.
fn caps(fee_sign: VenueFeeSign) -> VenueCaps {
    VenueCaps {
        exec: Some(ExecCaps {
            order: OrderCaps {
                kinds: TagSet::of(&[OrderKindTag::Limit, OrderKindTag::Market]),
                tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc, TifTag::Fok]),
                channels: TagSet::of(&[Channel::Public, Channel::Rpi]),
                post_only: true,
                reduce_only: true,
                flag_conflicts: vec![],
                amend: None,
                cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
                query_refs: TagSet::none(),
                cancel_before_ack: true,
                cancel_is_signed: false,
                batch_place: None,
                batch_cancel: None,
                cancel_all_account: Support::Unsupported,
                cancel_all_instrument: Support::Unsupported,
                cancel_on_disconnect: CancelOnDisconnect::None,
                ack: AckModel::SinglePhase,
                client_id: ClientIdFormat::Uuid,
                cid_echoed_on_events: true,
                nonce_scope: NonceScope::None,
                ordering_key: OrderingKey::VenueSeq,
                snapshot_source: SnapshotSource::None,
                events_echo_flags: true,
                sign_cost_hint_us: 0,
            },
            fills: FillCaps {
                source: FillSource::Native,
                liquidity_flag: true,
                realized_pnl: false,
                realized_funding: false,
                fee_sign,
                fee_asset_reported: true,
                fill_id: true,
                replays_fills_on_reconnect: false,
            },
        }),
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::None,
        },
        md: MdCaps {
            encoding: Encoding::Text,
            touch_sources: vec![],
            books: vec![],
            trades: TradeCaps {
                source: FeedSource::None,
                aggressor: true,
                trade_id: false,
            },
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::None,
            index: FeedSource::None,
            ts_precision: Duration::from_nanos(1),
            topology: ConnTopology::PerInstrument,
            max_conn_lifetime: None,
        },
        limits: vec![],
        readiness_ceiling: Readiness::Paper,
    }
}

/// A linear perpetual on a 0.5 tick and a 0.5 size step, quoted in USDC.
fn spec(id: InstrumentId, symbol: &str) -> InstrumentSpec {
    let venue_symbol = dispatch(&caps(VenueFeeSign::PositiveIsCost), NS, |scope| {
        scope.venue_symbol(symbol)
    })
    .unwrap();
    let half = Decimal::new(5, 1);
    InstrumentSpec {
        id,
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

fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    table.insert(spec(INST, "SIM-PERP"));
    table.insert(spec(BOOKLESS, "LONE-PERP"));
    table
}

fn rate(
    inst: InstrumentId,
    liquidity: Liquidity,
    bps: f64,
    source: FeeSource,
) -> (FeeKey, FeeEntry) {
    let key = FeeKey {
        account: ACCOUNT,
        instrument: inst,
        channel: Channel::Public,
        liquidity,
    };
    let entry = FeeEntry {
        rate: FeeRate(Bps(bps)),
        source,
        as_of: WallNs(0),
    };
    (key, entry)
}

/// The account's maker and taker rates on the trading instrument.
fn fees() -> FeeBook {
    let mut book = FeeBook::new();
    for liquidity in [Liquidity::Maker, Liquidity::Taker] {
        let bps = if liquidity == Liquidity::Maker {
            MAKER_BPS
        } else {
            TAKER_BPS
        };
        let (key, entry) = rate(INST, liquidity, bps, FeeSource::ConfigOverride);
        book.insert(key, entry);
    }
    book
}

fn config(bracket: Bracket, fee_sign: VenueFeeSign, fees: FeeBook) -> SimConfig {
    SimConfig {
        exec: caps(fee_sign).exec.unwrap(),
        matching: caps(fee_sign).matching,
        latency: SimLatency {
            to_venue: TO_VENUE,
            to_client: TO_CLIENT,
        },
        rpc_timeout: TIMEOUT,
        stream: STREAM,
        queue: QueueConfig { bracket },
        account: ACCOUNT,
        fees,
        specs: specs(),
        books: BTreeMap::from([(INST, BOOK)]),
    }
}

/// A sink that keeps what the codec reports, as the runtime's would before stamping it.
#[derive(Default)]
struct Sink(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// SimVenue's two halves and the runtime's part, done by hand: encode a command, hand its
/// frame to the engine, feed envelopes, and decode the engine's answers through the core's
/// dispatch.
struct Venue {
    codec: SimCodec,
    engine: SimEngine,
    caps: VenueCaps,
    specs: SpecTable,
    ingest: u64,
    /// Every answer frame, as the engine gave it.
    frames: Vec<Answer>,
}

impl Venue {
    fn new(bracket: Bracket) -> Venue {
        Venue::with(config(bracket, VenueFeeSign::PositiveIsRebate, fees()))
    }

    fn with(config: SimConfig) -> Venue {
        let fee_sign = config.exec.fills.fee_sign;
        Venue {
            codec: SimCodec::new(&config),
            engine: SimEngine::new(&config),
            caps: caps(fee_sign),
            specs: config.specs.clone(),
            ingest: 0,
            frames: Vec::new(),
        }
    }

    fn ctx(mono: u64) -> EncodeCtx {
        EncodeCtx {
            wall: WallNs(WALL0 + mono as i64),
            mono: MonoNs(mono),
            nonces: NonceBlock::EMPTY,
        }
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: u64,
        mono: u64,
    ) -> Result<Effects, NotSentReason> {
        let mut fx = Effects::new();
        let t = &mut PathStamps::off();
        let ctx = Venue::ctx(mono);
        let receipt = self
            .codec
            .encode(cmd, RpcId(rpc), &self.specs, &ctx, t, &mut fx)?;
        assert!(receipt.nonces().is_empty());
        Ok(fx)
    }

    /// Encodes `cmd` at `mono` and hands its frame to the engine.
    fn send(&mut self, cmd: VenueCommand, rpc: u64, mono: u64) {
        let fx = self.encode(&cmd, rpc, mono).unwrap();
        assert!(fx.carry_request(RpcId(rpc), cmd.traffic_class()));
        for effect in fx.as_slice() {
            let Effect::Send { stream, frame, .. } = effect else {
                panic!("the codec only writes frames: {effect:?}");
            };
            assert_eq!(*stream, STREAM);
            self.engine.on_order_frame(frame.bytes()).unwrap();
        }
    }

    fn md(&mut self, mono: u64, body: MdEvent) -> Result<(), SimError> {
        let stamp = Stamp {
            ingest_seq: self.ingest,
            kernel_rx: None,
            recv_mono: MonoNs(mono),
            recv_wall: WallNs(WALL0 + mono as i64),
            conn: ConnKey { conn: 0, epoch: 1 },
        };
        self.ingest += 1;
        self.engine
            .on_market(&Envelope::new(stamp, VenueMeta::NONE, body))
    }

    fn snapshot(&mut self, mono: u64, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
        self.md(
            mono,
            MdEvent::BookSnapshotBegin {
                inst: INST,
                book: BOOK,
                epoch: 1,
            },
        )
        .unwrap();
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
        )
        .unwrap();
    }

    fn level(&mut self, mono: u64, side: BookSide, px: i64, qty: i64) {
        let ev = MdEvent::Level {
            inst: INST,
            book: BOOK,
            side,
            px: Ticks(px),
            qty: lots(qty),
        };
        self.md(mono, ev).unwrap();
    }

    fn trade(&mut self, mono: u64, aggressor: Aggressor, px: i64, qty: i64) {
        let ev = MdEvent::Trade {
            inst: INST,
            id: None,
            aggressor,
            px: Ticks(px),
            qty: lots(qty),
        };
        self.md(mono, ev).unwrap();
    }

    /// Anything that is no book event and no trade: it only moves the engine's time.
    fn tick(&mut self, mono: u64) {
        let ev = MdEvent::Health {
            inst: INST,
            feed: Feed::Trades,
            h: FeedHealth::Live,
        };
        self.md(mono, ev).unwrap();
    }

    /// Decodes one frame through the codec, as the runtime would at its due time.
    fn decode(&mut self, frame: &[u8]) -> Result<Vec<(VenueMeta, ExecEvent)>, DecodeError> {
        let mut sink = Sink::default();
        let mut fx = Effects::new();
        let (codec, specs) = (&mut self.codec, &self.specs);
        dispatch(&self.caps, NS, |scope| {
            codec.on_frame(
                STREAM,
                RawFrame::Text(std::str::from_utf8(frame).unwrap()),
                scope,
                specs,
                &mut sink,
                &mut fx,
            )
        })?;
        assert!(fx.is_empty());
        Ok(sink.0)
    }

    /// The engine's answers since the last call, each decoded, with its due time.
    fn answers(&mut self) -> Vec<(MonoNs, ExecEvent)> {
        let mut out = Vec::new();
        for answer in self.engine.take_answers() {
            let events = self.decode(&answer.frame).unwrap();
            assert_eq!(events.len(), 1, "one answer per frame");
            for (meta, ev) in events {
                assert!(meta.venue_seq.is_some());
                out.push((answer.at, ev));
            }
            self.frames.push(answer);
        }
        out
    }
}

fn place(
    cid: ClientOrderId,
    side: Side,
    kind: OrderKind,
    qty: i64,
    tif: Tif,
    post_only: bool,
) -> VenueCommand {
    VenueCommand::Place(NewOrder {
        cid,
        inst: INST,
        side,
        qty: lots(qty),
        kind,
        tif,
        channel: Channel::Public,
        post_only,
        reduce_only: false,
        reducing: false,
    })
}

fn limit(cid: ClientOrderId, side: Side, px: i64, qty: i64) -> VenueCommand {
    place(
        cid,
        side,
        OrderKind::Limit { px: Ticks(px) },
        qty,
        TifTag::Gtc,
        true,
    )
}

fn cancel(target: OrderRef) -> VenueCommand {
    VenueCommand::Cancel(CancelOrder {
        target,
        inst: INST,
        side: Side::Buy,
        placement_nonce: None,
    })
}

/// The fill's quantity, liquidity and fee cost in nanos, from a decoded event.
fn fill_of(ev: &ExecEvent) -> Option<(i64, i64, Liquidity3, i128)> {
    match ev {
        ExecEvent::Fill(f) => Some((f.px.0, f.qty.get(), f.liquidity, f.fee.cost().nanos)),
        _ => None,
    }
}

fn state_of(ev: &ExecEvent) -> Option<(VenueOrderState, i64)> {
    match ev {
        ExecEvent::Order(o) => Some((o.state.clone(), o.cum_filled.get())),
        _ => None,
    }
}

fn outcome_of(ev: &ExecEvent) -> Option<(u64, SubmitOutcome)> {
    match ev {
        ExecEvent::Outcome { rpc, outcome, .. } => Some((rpc.0, outcome.clone())),
        _ => None,
    }
}

fn rejected(ev: &ExecEvent) -> Option<RejectKind> {
    match outcome_of(ev)?.1 {
        SubmitOutcome::Rejected(r) => Some(r.kind),
        _ => None,
    }
}

/// The fee a fill of `qty` lots at `px` costs at `bps`, from the spec's own notional.
fn expected_fee(px: i64, qty: i64, bps: f64) -> i128 {
    let notional = spec(INST, "SIM-PERP")
        .notional(Ticks(px), lots(qty))
        .unwrap();
    (notional.nanos as f64 * bps / 10_000.0).round() as i128
}

const ACCEPTED: SubmitOutcome = SubmitOutcome::Accepted {
    ack: AckLevel::Final,
};

#[test]
fn a_placed_order_is_accepted_after_the_latency_and_filled_through_the_queue_by_later_trades() {
    let mut v = Venue::new(Bracket::Pessimistic);
    v.snapshot(T0, &[(199, 5), (198, 10)], &[(201, 5), (202, 5)]);
    let order = cid();
    let sent = T0 + MS;
    v.send(limit(order, Side::Buy, 199, 2), 1, sent);

    // The command acts at its encode time plus the latency to the venue, and not before.
    v.tick(sent + 4 * MS);
    assert!(v.answers().is_empty());
    v.tick(sent + 5 * MS);
    let acted = sent + 5 * MS;
    let answered = MonoNs(acted) + TO_CLIENT;
    let got = v.answers();
    assert_eq!(got.len(), 2);
    assert!(got.iter().all(|(at, _)| *at == answered));
    let ExecEvent::Outcome {
        rpc,
        item: Some(item),
        outcome,
    } = &got[0].1
    else {
        panic!("{:?}", got[0]);
    };
    assert_eq!((*rpc, outcome), (RpcId(1), &ACCEPTED));
    assert_eq!(item.cid, Some(order));
    let vid = item.vid.clone().unwrap();
    let ExecEvent::Order(open) = &got[1].1 else {
        panic!()
    };
    assert_eq!(open.cid, Some(CidMatch::Ours(order)));
    assert_eq!(open.vid.as_ref(), Some(&vid));
    assert_eq!(
        (open.state.clone(), open.cum_filled, open.px),
        (VenueOrderState::Open, lots(0), Some(Ticks(199)))
    );
    assert_eq!(
        (open.qty, open.post_only, open.reduce_only),
        (Some(lots(2)), Some(true), Some(false))
    );

    // Five lots were queued ahead of it: a trade of five consumes them, and the level's shrink
    // that follows is the trade, not a cancel.
    v.trade(T0 + 20 * MS, Aggressor::Seller, 199, 5);
    v.level(T0 + 21 * MS, BookSide::Bid, 199, 0);
    // Public size that joins the level queues behind it.
    v.level(T0 + 21 * MS, BookSide::Bid, 199, 3);
    assert!(v.answers().is_empty());

    // The next trade at its price fills it, as the maker.
    let filled_at = T0 + 22 * MS;
    v.trade(filled_at, Aggressor::Seller, 199, 1);
    let got = v.answers();
    assert_eq!(
        got.iter().map(|(at, _)| *at).collect::<Vec<_>>(),
        [MonoNs(filled_at) + TO_CLIENT; 2]
    );
    let maker = expected_fee(199, 1, MAKER_BPS);
    assert!(maker < 0, "a maker rebate is a negative cost");
    assert_eq!(fill_of(&got[0].1), Some((199, 1, Liquidity3::Maker, maker)));
    let ExecEvent::Fill(fill) = &got[0].1 else {
        panic!()
    };
    assert_eq!(
        (fill.vid(), fill.cum_after(), fill.cid),
        (Some(&vid), Some(lots(1)), Some(CidMatch::Ours(order)))
    );
    assert_eq!(fill.fee.cost().asset, usdc());
    assert_eq!(state_of(&got[1].1), Some((VenueOrderState::Open, 1)));

    // A trade through its price fills the rest.
    v.trade(T0 + 23 * MS, Aggressor::Seller, 198, 4);
    let got = v.answers();
    assert_eq!(fill_of(&got[0].1), Some((199, 1, Liquidity3::Maker, maker)));
    assert_eq!(state_of(&got[1].1), Some((VenueOrderState::Filled, 2)));
    // A later trade finds nothing to fill.
    v.trade(T0 + 24 * MS, Aggressor::Seller, 198, 4);
    assert!(v.answers().is_empty());
}

#[test]
fn level_cancels_advance_the_order_by_its_bracket() {
    // Six lots ahead, then the level shrinks by three with no trade: none, floor(3 × 6 / 8),
    // or all three of them were ahead of the order.
    let mut filled = Vec::new();
    for bracket in [Bracket::Pessimistic, Bracket::Middle, Bracket::Optimistic] {
        let mut v = Venue::new(bracket);
        v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
        v.send(limit(cid(), Side::Buy, 199, 2), 1, T0);
        v.tick(T0 + 5 * MS);
        assert_eq!(v.answers().len(), 2);
        v.level(T0 + 10 * MS, BookSide::Bid, 199, 3);
        let mut after = Vec::new();
        for (n, qty) in [4, 2].into_iter().enumerate() {
            v.trade(T0 + (20 + n as u64) * MS, Aggressor::Seller, 199, qty);
            let got = v.answers();
            after.push(
                got.iter()
                    .filter_map(|(_, ev)| fill_of(ev))
                    .map(|f| f.1)
                    .sum::<i64>(),
            );
        }
        filled.push(after);
    }
    assert_eq!(filled, [vec![0, 0], vec![0, 2], vec![1, 1]]);
}

#[test]
fn a_post_only_order_that_would_cross_is_rejected_and_nothing_rests() {
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 5)], &[(201, 5)]);
    let order = cid();
    v.send(limit(order, Side::Buy, 201, 1), 7, T0);
    v.send(limit(cid(), Side::Sell, 199, 1), 8, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(
        got.iter().map(|(_, ev)| rejected(ev)).collect::<Vec<_>>(),
        [Some(RejectKind::PostOnlyWouldCross); 2]
    );
    let ExecEvent::Outcome {
        rpc,
        outcome: SubmitOutcome::Rejected(r),
        ..
    } = &got[0].1
    else {
        panic!()
    };
    assert_eq!(
        (rpc.0, r.venue_code.as_deref(), &*r.raw),
        (7, Some("post_only"), "post_only")
    );
    // It never rested: nothing fills it, and the venue does not know it.
    v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 50);
    assert!(v.answers().is_empty());
    v.send(cancel(OrderRef::Client(order)), 9, T0 + 7 * MS);
    v.tick(T0 + 12 * MS);
    assert_eq!(rejected(&v.answers()[0].1), Some(RejectKind::NotFound));
}

#[test]
fn a_crossing_order_fills_against_the_displayed_levels_as_the_taker() {
    let asks = [(201, 3), (202, 2), (203, 5)];
    let taker = |px, qty| expected_fee(px, qty, TAKER_BPS);
    let mut v = Venue::new(Bracket::Middle);
    v.snapshot(T0, &[(199, 5), (198, 5)], &asks);
    // Good till cancelled: takes 201 and 202, rests the rest at 202.
    let buy = |tif| {
        place(
            cid(),
            Side::Buy,
            OrderKind::Limit { px: Ticks(202) },
            7,
            tif,
            false,
        )
    };
    v.send(buy(TifTag::Gtc), 1, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((1, ACCEPTED)));
    let fills: Vec<_> = got.iter().filter_map(|(_, ev)| fill_of(ev)).collect();
    assert_eq!(
        fills,
        [
            (201, 3, Liquidity3::Taker, taker(201, 3)),
            (202, 2, Liquidity3::Taker, taker(202, 2))
        ]
    );
    assert!(
        fills.iter().all(|f| f.3 > 0),
        "a taker fee is a positive cost"
    );
    let cums: Vec<_> = got
        .iter()
        .filter_map(|(_, ev)| match ev {
            ExecEvent::Fill(f) => f.cum_after().map(Lots::get),
            _ => None,
        })
        .collect();
    assert_eq!(cums, [3, 5]);
    assert_eq!(state_of(&got[3].1), Some((VenueOrderState::Open, 5)));
    // Its rest queues at 202, behind nothing on the bid side: the next sell there fills it.
    v.trade(T0 + 6 * MS, Aggressor::Seller, 202, 1);
    let got = v.answers();
    assert_eq!(
        fill_of(&got[0].1),
        Some((202, 1, Liquidity3::Maker, expected_fee(202, 1, MAKER_BPS)))
    );

    // Immediate or cancel: the rest is cancelled unfilled.
    v.send(buy(TifTag::Ioc), 2, T0 + 10 * MS);
    v.tick(T0 + 15 * MS);
    let got = v.answers();
    assert_eq!(got.iter().filter_map(|(_, ev)| fill_of(ev)).count(), 2);
    assert_eq!(
        state_of(&got[3].1),
        Some((VenueOrderState::Canceled(CancelReason::Unfilled), 5))
    );
    // Fill or kill, more than the levels show: nothing fills.
    v.send(buy(TifTag::Fok), 3, T0 + 20 * MS);
    v.tick(T0 + 25 * MS);
    let got = v.answers();
    assert_eq!(got.len(), 2);
    assert_eq!(
        state_of(&got[1].1),
        Some((VenueOrderState::Canceled(CancelReason::Unfilled), 0))
    );
    // A market order sweeps every displayed level and fills whole.
    let market = place(cid(), Side::Buy, OrderKind::Market, 10, TifTag::Ioc, false);
    v.send(market, 4, T0 + 30 * MS);
    v.tick(T0 + 35 * MS);
    let got = v.answers();
    let fills: Vec<_> = got
        .iter()
        .filter_map(|(_, ev)| fill_of(ev))
        .map(|f| (f.0, f.1))
        .collect();
    assert_eq!(fills, asks);
    let ExecEvent::Order(done) = &got[4].1 else {
        panic!()
    };
    assert_eq!(
        (done.state.clone(), done.cum_filled, done.px),
        (VenueOrderState::Filled, lots(10), None)
    );
    // A sell that crosses the bids takes them, the best first, and only what it needs.
    let sell = place(
        cid(),
        Side::Sell,
        OrderKind::Limit { px: Ticks(198) },
        2,
        TifTag::Ioc,
        false,
    );
    v.send(sell, 5, T0 + 40 * MS);
    v.tick(T0 + 45 * MS);
    let got = v.answers();
    assert_eq!(
        fill_of(&got[1].1),
        Some((199, 2, Liquidity3::Taker, taker(199, 2)))
    );
    assert_eq!(state_of(&got[2].1), Some((VenueOrderState::Filled, 2)));
}

#[test]
fn a_cancel_ends_the_order_and_a_second_is_refused() {
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 1), (198, 1)], &[(201, 5)]);
    let (by_cid, by_vid) = (cid(), cid());
    v.send(limit(by_cid, Side::Buy, 199, 1), 1, T0);
    v.send(limit(by_vid, Side::Buy, 198, 1), 2, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let ExecEvent::Outcome {
        item: Some(ItemRef { vid: Some(vid), .. }),
        ..
    } = &got[2].1
    else {
        panic!("{got:?}");
    };
    let vid = vid.clone();

    v.send(cancel(OrderRef::Client(by_cid)), 3, T0 + 10 * MS);
    v.send(cancel(OrderRef::Venue(vid.clone())), 4, T0 + 10 * MS);
    v.tick(T0 + 15 * MS);
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((3, ACCEPTED)));
    let canceled = Some((VenueOrderState::Canceled(CancelReason::Requested), 0));
    assert_eq!(state_of(&got[1].1), canceled);
    assert_eq!(outcome_of(&got[2].1), Some((4, ACCEPTED)));
    assert_eq!(state_of(&got[3].1), canceled);
    let ExecEvent::Order(o) = &got[3].1 else {
        panic!()
    };
    assert_eq!(
        (o.cid, o.vid.as_ref(), o.px),
        (Some(CidMatch::Ours(by_vid)), Some(&vid), Some(Ticks(198)))
    );
    // Nothing fills a cancelled order.
    v.trade(T0 + 20 * MS, Aggressor::Seller, 197, 10);
    assert!(v.answers().is_empty());
    // A second cancel finds the order ended; one for an order the venue never had is not found.
    v.send(cancel(OrderRef::Both(by_cid, vid)), 5, T0 + 30 * MS);
    v.send(cancel(OrderRef::Client(cid())), 6, T0 + 30 * MS);
    v.tick(T0 + 35 * MS);
    let got = v.answers();
    assert_eq!(
        rejected(&got[0].1),
        Some(RejectKind::AlreadyTerminal(TerminalHint::Canceled))
    );
    assert_eq!(rejected(&got[1].1), Some(RejectKind::NotFound));

    // A filled order's cancel says it filled.
    let filled = cid();
    v.send(
        place(filled, Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
        7,
        T0 + 40 * MS,
    );
    v.send(cancel(OrderRef::Client(filled)), 8, T0 + 41 * MS);
    v.tick(T0 + 50 * MS);
    let got = v.answers();
    assert_eq!(
        rejected(got.last().map(|(_, ev)| ev).unwrap()),
        Some(RejectKind::AlreadyTerminal(TerminalHint::Filled))
    );
}

#[test]
fn every_fill_carries_the_fee_books_fee_in_either_venue_sign() {
    // The engine writes each fee in the stood-in venue's sign and the decode scope reads it
    // back as a cost, so both venues report the same costs.
    for sign in [VenueFeeSign::PositiveIsCost, VenueFeeSign::PositiveIsRebate] {
        let mut v = Venue::with(config(Bracket::Optimistic, sign, fees()));
        v.snapshot(T0, &[(198, 1)], &[(201, 2)]);
        v.send(limit(cid(), Side::Buy, 199, 4), 1, T0);
        v.send(
            place(cid(), Side::Buy, OrderKind::Market, 2, TifTag::Ioc, false),
            2,
            T0,
        );
        v.tick(T0 + 5 * MS);
        v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 4);
        let fees: Vec<_> = v
            .answers()
            .iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| (f.2, f.3))
            .collect();
        assert_eq!(
            fees,
            [
                (Liquidity3::Taker, expected_fee(201, 2, TAKER_BPS)),
                (Liquidity3::Maker, expected_fee(199, 4, MAKER_BPS))
            ],
            "{sign:?}"
        );
    }
}

#[test]
fn an_order_the_fee_book_cannot_charge_is_refused_or_cancelled_by_the_venue() {
    // No taker rate: a crossing order is refused before anything fills.
    let mut makers_only = FeeBook::new();
    let tier_end = WallNs(WALL0 + (T0 + 10 * MS) as i64);
    let (key, entry) = rate(
        INST,
        Liquidity::Maker,
        MAKER_BPS,
        FeeSource::ConfiguredTier {
            epoch_end: Some(tier_end),
        },
    );
    makers_only.insert(key, entry);
    let mut v = Venue::with(config(
        Bracket::Optimistic,
        VenueFeeSign::PositiveIsCost,
        makers_only,
    ));
    v.snapshot(T0, &[(198, 1)], &[(201, 2)]);
    v.send(
        place(cid(), Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
        1,
        T0,
    );
    v.send(limit(cid(), Side::Buy, 199, 2), 2, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let ExecEvent::Outcome {
        outcome: SubmitOutcome::Rejected(r),
        ..
    } = &got[0].1
    else {
        panic!("{got:?}")
    };
    assert_eq!(
        (r.kind, r.venue_code.as_deref()),
        (RejectKind::Other, Some("no_fee"))
    );
    // While the maker tier runs, a trade fills the resting order; once it ended, the venue
    // cancels the order rather than fill it with no fee.
    v.trade(T0 + 9 * MS, Aggressor::Seller, 199, 1);
    assert_eq!(
        v.answers().iter().filter_map(|(_, ev)| fill_of(ev)).count(),
        1
    );
    v.trade(T0 + 10 * MS, Aggressor::Seller, 199, 1);
    let got = v.answers();
    assert_eq!(got.len(), 1);
    assert_eq!(
        state_of(&got[0].1),
        Some((VenueOrderState::Canceled(CancelReason::Venue), 1))
    );
    v.trade(T0 + 11 * MS, Aggressor::Seller, 199, 1);
    assert!(v.answers().is_empty());
}

/// One script of envelopes and commands, returning every answer frame the engine gave.
fn script(bracket: Bracket) -> (Vec<Answer>, Vec<(MonoNs, ExecEvent)>) {
    let mut v = Venue::new(bracket);
    let mut events = Vec::new();
    v.snapshot(T0, &[(199, 6), (198, 4)], &[(201, 3), (202, 6)]);
    let ids = [cid(), cid(), cid()];
    v.send(limit(ids[0], Side::Buy, 199, 2), 1, T0);
    v.send(limit(ids[1], Side::Buy, 199, 3), 2, T0 + MS);
    v.send(
        place(
            ids[2],
            Side::Buy,
            OrderKind::Limit { px: Ticks(201) },
            4,
            TifTag::Gtc,
            false,
        ),
        3,
        T0 + 2 * MS,
    );
    for (n, step) in (0..12).enumerate() {
        let at = T0 + (4 + n as u64) * MS;
        match step % 4 {
            0 => v.level(at, BookSide::Bid, 199, 6 - (step / 4) as i64),
            1 => v.trade(at, Aggressor::Seller, 199, 2),
            2 => v.trade(at, Aggressor::Unknown, 198, 1),
            _ => v.tick(at),
        }
        events.extend(v.answers());
    }
    v.send(cancel(OrderRef::Client(ids[1])), 4, T0 + 20 * MS);
    v.tick(T0 + 30 * MS);
    events.extend(v.answers());
    (v.frames, events)
}

#[test]
fn two_runs_over_the_same_envelopes_and_frames_give_identical_answers() {
    for bracket in [Bracket::Pessimistic, Bracket::Middle, Bracket::Optimistic] {
        let (frames, events) = script(bracket);
        assert!(
            frames.len() > 10,
            "{bracket:?}: the script exercises the venue"
        );
        // Client ids differ between runs (each is minted afresh), so compare what the venue
        // decided: every answer's due time, kind and numbers.
        let (again, again_events) = script(bracket);
        let shape = |events: &[(MonoNs, ExecEvent)]| -> Vec<String> {
            events
                .iter()
                .map(|(at, ev)| {
                    format!(
                        "{} {:?} {:?} {:?} {:?}",
                        at.0,
                        fill_of(ev),
                        state_of(ev),
                        outcome_of(ev).map(|o| o.0),
                        rejected(ev)
                    )
                })
                .collect()
        };
        assert_eq!(shape(&events), shape(&again_events), "{bracket:?}");
        let at = |frames: &[Answer]| frames.iter().map(|a| a.at).collect::<Vec<_>>();
        assert_eq!(at(&frames), at(&again), "{bracket:?}");
    }
}

#[test]
fn the_same_frames_and_envelopes_give_the_same_answer_bytes() {
    // Byte for byte: one set of frames, handed to two fresh engines with the same envelopes.
    let config = config(Bracket::Middle, VenueFeeSign::PositiveIsRebate, fees());
    let mut codec = SimCodec::new(&config);
    let specs = specs();
    let mut frames = Vec::new();
    for (rpc, cmd) in [
        limit(cid(), Side::Buy, 199, 2),
        place(cid(), Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
    ]
    .into_iter()
    .enumerate()
    {
        let mut fx = Effects::new();
        codec
            .encode(
                &cmd,
                RpcId(rpc as u64),
                &specs,
                &Venue::ctx(T0),
                &mut PathStamps::off(),
                &mut fx,
            )
            .unwrap();
        let Some(Effect::Send { frame, .. }) = fx.take().pop() else {
            panic!()
        };
        frames.push(frame);
    }
    let run = || {
        let mut v = Venue::with(config.clone());
        v.snapshot(T0, &[(199, 6)], &[(201, 3)]);
        for frame in &frames {
            v.engine.on_order_frame(frame.bytes()).unwrap();
        }
        v.tick(T0 + 5 * MS);
        v.level(T0 + 6 * MS, BookSide::Bid, 199, 2);
        v.trade(T0 + 7 * MS, Aggressor::Seller, 199, 4);
        v.engine.take_answers()
    };
    let first = run();
    assert_eq!(first.len(), 7);
    assert_eq!(first, run());
}

#[test]
fn the_engine_refuses_what_its_book_cannot_place() {
    let mut v = Venue::new(Bracket::Pessimistic);
    // No book yet, and an instrument with no trading book: refused.
    v.send(limit(cid(), Side::Buy, 199, 1), 1, T0);
    let mut bookless = limit(cid(), Side::Buy, 199, 1);
    if let VenueCommand::Place(o) = &mut bookless {
        o.inst = BOOKLESS;
    }
    v.send(bookless, 2, T0);
    v.tick(T0 + 5 * MS);
    let refused: Vec<_> = v
        .answers()
        .iter()
        .map(|(_, ev)| outcome_of(ev).map(|(_, o)| o))
        .collect();
    for outcome in &refused {
        let Some(SubmitOutcome::Rejected(r)) = outcome else {
            panic!("{refused:?}")
        };
        assert_eq!(
            (r.kind, r.venue_code.as_deref()),
            (RejectKind::Other, Some("no_book"))
        );
    }
    // A price past the deepest level a capped book shows: its size is not known.
    v.snapshot(T0 + 10 * MS, &[(199, 5)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 150, 1), 3, T0 + 10 * MS);
    // A client id the venue already knows.
    let twice = cid();
    v.send(limit(twice, Side::Buy, 199, 1), 4, T0 + 10 * MS);
    v.send(limit(twice, Side::Buy, 199, 1), 5, T0 + 10 * MS);
    v.tick(T0 + 15 * MS);
    let codes: Vec<_> = v
        .answers()
        .iter()
        .filter_map(|(_, ev)| match ev {
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Rejected(r),
                ..
            } => r.venue_code.as_deref().map(str::to_owned),
            _ => None,
        })
        .collect();
    assert_eq!(codes, ["no_book", "duplicate"]);
    // A trade on the instrument with no trading book, or with no order queued, fills nothing.
    v.md(
        T0 + 20 * MS,
        MdEvent::Trade {
            inst: BOOKLESS,
            id: None,
            aggressor: Aggressor::Buyer,
            px: Ticks(1),
            qty: lots(1),
        },
    )
    .unwrap();
    assert!(v.answers().is_empty());
}

#[test]
fn a_trade_without_an_aggressor_is_classified_against_the_touch() {
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 1)], &[(201, 1)]);
    v.send(limit(cid(), Side::Buy, 199, 1), 1, T0);
    v.send(limit(cid(), Side::Sell, 201, 2), 2, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 4);
    // Inside the spread: not classified, nothing fills.
    v.trade(T0 + 6 * MS, Aggressor::Unknown, 200, 5);
    assert!(v.answers().is_empty());
    // At the offer a buy, at the bid a sell.
    v.trade(T0 + 7 * MS, Aggressor::Unknown, 201, 2);
    let got = v.answers();
    let ExecEvent::Fill(f) = &got[0].1 else {
        panic!()
    };
    assert_eq!((f.side, f.px), (Side::Sell, Ticks(201)));
    v.trade(T0 + 8 * MS, Aggressor::Unknown, 199, 2);
    let got = v.answers();
    let ExecEvent::Fill(f) = &got[0].1 else {
        panic!()
    };
    assert_eq!((f.side, f.px), (Side::Buy, Ticks(199)));
    // A trade the venue names a buyer for fills the offer's rest.
    v.trade(T0 + 8 * MS, Aggressor::Buyer, 201, 1);
    let got = v.answers();
    assert_eq!(state_of(&got[1].1), Some((VenueOrderState::Filled, 2)));
    // With the book gapped there is no touch to classify against.
    v.md(
        T0 + 9 * MS,
        MdEvent::Health {
            inst: INST,
            feed: Feed::Book(BOOK),
            h: FeedHealth::Gap,
        },
    )
    .unwrap();
    v.trade(T0 + 10 * MS, Aggressor::Unknown, 199, 1);
    assert!(v.answers().is_empty());
}

#[test]
fn commands_wait_for_time_to_pass_their_arrival_and_act_in_order() {
    let mut v = Venue::new(Bracket::Pessimistic);
    v.snapshot(T0, &[(199, 1)], &[(201, 1)]);
    let order = cid();
    // A cancel sent after the place acts after it, though both wait for the same envelope.
    v.send(limit(order, Side::Buy, 199, 1), 1, T0);
    v.send(cancel(OrderRef::Client(order)), 2, T0 + MS);
    v.engine.advance(MonoNs(T0 + 4 * MS));
    assert!(v.answers().is_empty());
    v.engine.advance(MonoNs(T0 + 5 * MS));
    assert_eq!(v.answers().len(), 2);
    v.engine.advance(MonoNs(T0 + 6 * MS));
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((2, ACCEPTED)));
    assert_eq!(got[0].0, MonoNs(T0 + 6 * MS) + TO_CLIENT);
}

#[test]
fn the_engine_refuses_frames_and_events_it_cannot_read() {
    let mut v = Venue::new(Bracket::Middle);
    let refused = |frame: &str| {
        SimEngine::new(&config(
            Bracket::Middle,
            VenueFeeSign::PositiveIsCost,
            fees(),
        ))
        .on_order_frame(frame.as_bytes())
    };
    assert_eq!(
        v.engine.on_order_frame(&[0xff, 0xfe]),
        Err(SimError::Malformed("utf-8"))
    );
    for (frame, what) in [
        ("place|rpc", "field"),
        ("place|mono=1|wall=1", "rpc"),
        ("place|rpc=1|mono=x|wall=1", "mono"),
        ("hello|rpc=1|mono=1|wall=1", "kind"),
        ("cancel|rpc=1|mono=1|wall=1", "target"),
        ("place|rpc=1|mono=1|wall=1|cid=c|inst=1|side=X", "side"),
        (
            "place|rpc=1|mono=1|wall=1|cid=c|inst=1|side=B|qty=-1",
            "qty",
        ),
        (
            "place|rpc=1|mono=1|wall=1|cid=c|inst=1|side=B|px=z|qty=1",
            "px",
        ),
        (
            "place|rpc=1|mono=1|wall=1|cid=c|inst=1|side=B|qty=1|tif=day",
            "tif",
        ),
        (
            "place|rpc=1|mono=1|wall=1|cid=c|inst=1|side=B|qty=1|tif=gtc|po=2",
            "po",
        ),
    ] {
        assert_eq!(refused(frame), Err(SimError::Malformed(what)), "{frame}");
    }
    // A book event the book refuses.
    let end = MdEvent::BookSnapshotEnd {
        inst: INST,
        book: BOOK,
    };
    let err = v.md(T0, end).unwrap_err();
    assert!(matches!(err, SimError::Book(_)));
    let level = MdEvent::Level {
        inst: INST,
        book: BOOK,
        side: BookSide::Bid,
        px: Ticks(1),
        qty: lots(1),
    };
    assert_eq!(v.md(T0, level), Ok(()));
    let window = MdEvent::Window {
        inst: INST,
        book: BOOK,
        lo: Ticks(2),
        hi: Ticks(1),
    };
    assert!(matches!(v.md(T0, window), Err(SimError::Book(_))));
    // Errors name what was wrong.
    assert_eq!(
        SimError::Malformed("rpc").to_string(),
        "simulated order-entry frame: bad rpc"
    );
    assert!(err.to_string().starts_with("simulated venue's book: "));
    let _: &dyn std::error::Error = &err;
}

#[test]
fn the_codec_sends_only_what_the_stood_in_venue_offers() {
    let mut v = Venue::new(Bracket::Middle);
    let order = |f: &dyn Fn(&mut NewOrder)| {
        let VenueCommand::Place(mut o) = limit(cid(), Side::Buy, 199, 1) else {
            unreachable!()
        };
        f(&mut o);
        VenueCommand::Place(o)
    };
    let unsupported = [
        order(&|o| o.channel = Channel::Rpi),
        VenueCommand::FeeQuery,
        VenueCommand::Amend(AmendOrder {
            target: OrderRef::Client(cid()),
            inst: INST,
            side: Side::Buy,
            tif: TifTag::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
            px: Ticks(1),
            qty: lots(1),
            cum_filled: lots(0),
        }),
    ];
    for cmd in &unsupported {
        assert_eq!(
            v.encode(cmd, 1, T0).unwrap_err(),
            NotSentReason::Unsupported,
            "{cmd:?}"
        );
    }
    let mut missing = order(&|_| {});
    if let VenueCommand::Place(o) = &mut missing {
        o.inst = InstrumentId::new(99);
    }
    assert_eq!(
        v.encode(&missing, 1, T0).unwrap_err(),
        NotSentReason::Unencodable
    );

    // A venue that offers less: no market orders, no post-only or reduce-only, GTC only,
    // cancels by client id or placement nonce only, and client ids too short for ours.
    let mut narrow = config(Bracket::Middle, VenueFeeSign::PositiveIsCost, fees());
    let caps = &mut narrow.exec.order;
    caps.kinds = TagSet::of(&[OrderKindTag::Limit]);
    caps.tifs = TagSet::of(&[TifTag::Gtc]);
    caps.post_only = false;
    caps.reduce_only = false;
    caps.client_id = ClientIdFormat::Numeric { max_digits: 4 };
    caps.cancel_refs = TagSet::of(&[RefKind::Client, RefKind::PlacementNonce]);
    let mut v = Venue::with(narrow);
    for cmd in [
        place(cid(), Side::Buy, OrderKind::Market, 1, TifTag::Gtc, false),
        place(
            cid(),
            Side::Buy,
            OrderKind::Limit { px: Ticks(1) },
            1,
            TifTag::Ioc,
            false,
        ),
        limit(cid(), Side::Buy, 1, 1),
        order(&|o| {
            o.post_only = false;
            o.reduce_only = true;
        }),
    ] {
        assert_eq!(
            v.encode(&cmd, 1, T0).unwrap_err(),
            NotSentReason::Unsupported,
            "{cmd:?}"
        );
    }
    // A cancel by venue id it cannot name, by placement nonce or not at all: the simulated
    // venue tracks no nonces.
    let vid = dispatch(&v.caps, NS, |scope| scope.venue_order_id("S1")).unwrap();
    let by_venue = |placement_nonce| {
        let vid = vid.clone();
        VenueCommand::Cancel(CancelOrder {
            target: OrderRef::Venue(vid),
            inst: INST,
            side: Side::Buy,
            placement_nonce,
        })
    };
    for nonce in [Some(1), None] {
        let cmd = by_venue(nonce);
        assert_eq!(
            v.encode(&cmd, 1, T0).unwrap_err(),
            NotSentReason::Unsupported
        );
    }
    let plain = order(&|o| o.post_only = false);
    assert_eq!(
        v.encode(&plain, 1, T0).unwrap_err(),
        NotSentReason::Unencodable
    );
    assert_eq!(
        v.encode(&cancel(OrderRef::Client(cid())), 1, T0)
            .unwrap_err(),
        NotSentReason::Unencodable
    );

    // What it sends is a request the runtime can time out, charged as the command it is.
    let mut v = Venue::new(Bracket::Middle);
    let fx = v.encode(&cancel(OrderRef::Client(cid())), 9, T0).unwrap();
    let [
        Effect::Send {
            rpc, class, charge, ..
        },
    ] = fx.as_slice()
    else {
        panic!("{fx:?}")
    };
    assert_eq!(
        *rpc,
        Some(RpcCall {
            id: RpcId(9),
            timeout: TIMEOUT
        })
    );
    assert_eq!(
        (*class, *charge),
        (
            TrafficClass::Safety,
            RateCharge::one(OpKind::Cancel, Some(INST))
        )
    );
}

#[test]
fn the_codec_refuses_answers_it_cannot_read_and_does_nothing_else() {
    let mut v = Venue::new(Bracket::Middle);
    for (frame, what) in [
        ("ack|rpc=1", "seq"),
        ("hello|seq=1", "kind"),
        ("reject|seq=1|rpc=1|code=nope", "code"),
        (
            "order|seq=1|cid=c|vid=S1|inst=1|side=B|state=gone|cum=0|qty=1|po=0|ro=0",
            "state",
        ),
        (
            "fill|seq=1|fid=F1|cid=c|vid=S1|inst=1|side=S|px=1|qty=1|cum=1|liq=X|fee=0|asset=USDC",
            "liq",
        ),
        (
            "fill|seq=1|fid=F1|cid=c|vid=S1|inst=1|side=S|px=1|qty=1|cum=1|liq=M|fee=0|asset=",
            "asset",
        ),
    ] {
        assert_eq!(
            v.decode(frame.as_bytes()),
            Err(DecodeError::Malformed(what)),
            "{frame}"
        );
    }
    let empty_vid = "ack|seq=1|rpc=1|cid=c|vid=";
    assert!(matches!(
        v.decode(empty_vid.as_bytes()),
        Err(DecodeError::IdRefused(_))
    ));
    let no_fee = format!(
        "fill|seq=1|fid=F1|cid=c|vid=S1|inst=1|side=S|px=1|qty=1|cum=1|liq=T|fee={}|asset=USDC",
        i128::MIN
    );
    assert!(matches!(
        v.decode(no_fee.as_bytes()),
        Err(DecodeError::FeeRefused(_))
    ));
    // A client id not ours is not named on the item, and an order event says it is not ours.
    let got = v.decode(b"ack|seq=1|rpc=1|cid=c|vid=S1").unwrap();
    let ExecEvent::Outcome {
        item: Some(item), ..
    } = &got[0].1
    else {
        panic!()
    };
    assert_eq!(item.cid, None);
    let got = v
        .decode(b"order|seq=2|cid=c|vid=S1|inst=1|side=S|state=venue|cum=0|qty=1|po=0|ro=1")
        .unwrap();
    assert_eq!(
        state_of(&got[0].1),
        Some((VenueOrderState::Canceled(CancelReason::Venue), 0))
    );
    let ExecEvent::Order(o) = &got[0].1 else {
        panic!()
    };
    assert_eq!(
        (o.cid, o.px, o.reduce_only, got[0].0.venue_seq),
        (Some(CidMatch::Unparseable), None, Some(true), Some(2))
    );

    // No HTTP, no nonces, no timers, no credentials; an unanswered request is Unknown.
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let (codec, specs) = (&mut v.codec, &v.specs);
    let http = dispatch(&v.caps, NS, |scope| {
        codec.on_http(
            HttpTag(1),
            Err(HttpFailure::TimedOut),
            scope,
            specs,
            &mut sink,
            &mut fx,
        )
    });
    assert!(matches!(http, Err(DecodeError::Malformed(_))));
    let ctx = Venue::ctx(T0);
    assert_eq!(v.codec.nonces_for(CtxCall::Open(STREAM)), 0);
    v.codec.on_open(STREAM, &ctx, &mut fx);
    v.codec.on_timer(TimerTag(1), &ctx, &mut fx);
    v.codec.resync(&ctx, &mut fx);
    assert!(fx.is_empty() && sink.0.is_empty());
    assert_eq!(
        v.codec
            .redact_inbound(Inbound::Frame(RawFrame::Text("ack"))),
        InboundSpans::NONE
    );
    v.codec.on_rpc_timeout(RpcId(5), &mut sink);
    let unknown = ExecEvent::Outcome {
        rpc: RpcId(5),
        item: None,
        outcome: SubmitOutcome::Unknown,
    };
    assert_eq!(sink.0, [(VenueMeta::NONE, unknown)]);
}

#[test]
fn the_codec_refuses_an_order_combining_features_the_venue_refuses_together() {
    // Codex r4182154713: a declared flag conflict is refused before anything is sent.
    let mut conflicted = config(Bracket::Middle, VenueFeeSign::PositiveIsCost, fees());
    conflicted.exec.order.flag_conflicts = vec![
        (Feature::PostOnly, Feature::ReduceOnly),
        (Feature::Ioc, Feature::ReduceOnly),
        (Feature::Fok, Feature::PostOnly),
        (Feature::Rpi, Feature::PostOnly),
    ];
    let mut v = Venue::with(conflicted);
    let with = |tif, post_only, reduce_only| {
        let mut cmd = place(
            cid(),
            Side::Buy,
            OrderKind::Limit { px: Ticks(1) },
            1,
            tif,
            post_only,
        );
        if let VenueCommand::Place(o) = &mut cmd {
            o.reduce_only = reduce_only;
        }
        cmd
    };
    for cmd in [
        with(TifTag::Gtc, true, true),
        with(TifTag::Ioc, false, true),
        with(TifTag::Fok, true, false),
    ] {
        assert_eq!(
            v.encode(&cmd, 1, T0).unwrap_err(),
            NotSentReason::FlagConflict,
            "{cmd:?}"
        );
    }
    // Each feature alone, or a pair the venue does not refuse, is sent.
    for cmd in [
        with(TifTag::Gtc, true, false),
        with(TifTag::Gtc, false, true),
        with(TifTag::Fok, false, true),
    ] {
        assert!(v.encode(&cmd, 1, T0).is_ok(), "{cmd:?}");
    }
}

#[test]
fn a_snapshot_that_replaces_the_book_advances_queues_by_its_shrinks() {
    // Codex r4182154723: a level that a replacement snapshot shows smaller, with no trade at
    // it, is a level cancel, as a delta's shrink is.
    let mut filled = Vec::new();
    for bracket in [Bracket::Pessimistic, Bracket::Optimistic] {
        let mut v = Venue::new(bracket);
        v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
        v.send(limit(cid(), Side::Buy, 199, 2), 1, T0);
        // An offer whose level the snapshot leaves as it was.
        v.send(limit(cid(), Side::Sell, 201, 1), 2, T0);
        v.tick(T0 + 5 * MS);
        assert_eq!(v.answers().len(), 4);
        // A print inside the spread, which no resting order meets; the snapshot ends what it
        // explains.
        v.trade(T0 + 5 * MS, Aggressor::Buyer, 200, 1);
        v.md(
            T0 + 6 * MS,
            MdEvent::BookSnapshotBegin {
                inst: INST,
                book: BOOK,
                epoch: 2,
            },
        )
        .unwrap();
        v.level(T0 + 6 * MS, BookSide::Bid, 199, 3);
        v.level(T0 + 6 * MS, BookSide::Ask, 201, 5);
        v.md(
            T0 + 6 * MS,
            MdEvent::BookSnapshotEnd {
                inst: INST,
                book: BOOK,
            },
        )
        .unwrap();
        v.trade(T0 + 7 * MS, Aggressor::Seller, 199, 4);
        filled.push(
            v.answers()
                .iter()
                .filter_map(|(_, ev)| fill_of(ev))
                .map(|f| f.1)
                .sum::<i64>(),
        );
    }
    assert_eq!(filled, [0, 1]);
}

#[test]
fn trades_explain_only_the_next_change_of_their_level() {
    // Codex r4182154731: a trade larger than the shrink that follows it explains that shrink
    // only; a later shrink is a cancel.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 2), 1, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    // Five traded, consuming five of the six ahead; the level's next change shows two of them
    // gone, and the other three traded lots explain nothing after it.
    v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 5);
    v.level(T0 + 7 * MS, BookSide::Bid, 199, 4);
    // Then the last lot ahead is cancelled: the order is at the front.
    v.level(T0 + 8 * MS, BookSide::Bid, 199, 1);
    v.trade(T0 + 9 * MS, Aggressor::Seller, 199, 1);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        1
    );
}

#[test]
fn a_fill_or_kill_that_cannot_fill_needs_no_fee() {
    // Codex r4182154743: an underfilled fill-or-kill order takes nothing, so a missing taker
    // rate does not refuse it.
    let mut makers_only = FeeBook::new();
    let (key, entry) = rate(INST, Liquidity::Maker, MAKER_BPS, FeeSource::ConfigOverride);
    makers_only.insert(key, entry);
    let mut v = Venue::with(config(
        Bracket::Optimistic,
        VenueFeeSign::PositiveIsCost,
        makers_only,
    ));
    v.snapshot(T0, &[(199, 1)], &[(201, 2)]);
    v.send(
        place(
            cid(),
            Side::Buy,
            OrderKind::Limit { px: Ticks(201) },
            5,
            TifTag::Fok,
            false,
        ),
        1,
        T0,
    );
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((1, ACCEPTED)));
    assert_eq!(
        state_of(&got[1].1),
        Some((VenueOrderState::Canceled(CancelReason::Unfilled), 0))
    );
}

#[test]
fn a_fee_that_is_not_a_finite_number_of_nanos_is_never_invented() {
    // Codex r4182154747: a rate whose fee is not finite, or does not fit an i128, refuses the
    // fill rather than saturate or read NaN as zero.
    for bps in [f64::NAN, f64::INFINITY, 1e40] {
        let mut book = FeeBook::new();
        for liquidity in [Liquidity::Maker, Liquidity::Taker] {
            let (key, entry) = rate(INST, liquidity, bps, FeeSource::ConfigOverride);
            book.insert(key, entry);
        }
        let mut v = Venue::with(config(
            Bracket::Optimistic,
            VenueFeeSign::PositiveIsCost,
            book,
        ));
        v.snapshot(T0, &[(198, 1)], &[(201, 2)]);
        v.send(
            place(cid(), Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
            1,
            T0,
        );
        v.send(limit(cid(), Side::Buy, 199, 1), 2, T0);
        v.tick(T0 + 5 * MS);
        let got = v.answers();
        assert_eq!(rejected(&got[0].1), Some(RejectKind::Other), "{bps}");
        v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 1);
        let got = v.answers();
        assert_eq!(
            state_of(&got[0].1),
            Some((VenueOrderState::Canceled(CancelReason::Venue), 0)),
            "{bps}"
        );
    }
}

#[test]
fn an_asset_symbol_with_the_frames_delimiters_survives_the_wire() {
    // Codex r4182154750: field values are escaped, so a symbol holding `|`, `=` or `%` reads
    // back whole.
    let odd = AssetSym::new("U|S=D%").unwrap();
    let mut cfg = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
    let mut table = SpecTable::new();
    table.insert(InstrumentSpec {
        quote_ccy: odd,
        settle_ccy: odd,
        ..spec(INST, "SIM-PERP")
    });
    cfg.specs = table;
    let mut v = Venue::with(cfg);
    v.snapshot(T0, &[(198, 1)], &[(201, 2)]);
    v.send(
        place(cid(), Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
        1,
        T0,
    );
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let ExecEvent::Fill(f) = &got[1].1 else {
        panic!("{got:?}")
    };
    assert_eq!(f.fee.cost().asset, odd);
    // A broken escape is malformed.
    let bad = "fill|seq=1|fid=F1|cid=c|vid=S1|inst=1|side=S|px=1|qty=1|cum=1|liq=M|fee=0|asset=U%7";
    assert_eq!(
        v.decode(bad.as_bytes()),
        Err(DecodeError::Malformed("escape"))
    );
}

#[test]
fn a_fee_of_exactly_i128_min_nanos_is_no_fee() {
    // Codex r4182342652: the decode scope refuses i128::MIN nanos, so the engine must not
    // write it. A fill of one lot at 4 ticks is 1 USDC (1e9 nanos) on this spec; this rate
    // rounds its fee to exactly -2^127 nanos.
    let rate_bps = -(2f64.powi(127)) / 1e5;
    assert_eq!((1e9 * rate_bps / 10_000.0).round(), i128::MIN as f64);
    let mut book = FeeBook::new();
    let (key, entry) = rate(INST, Liquidity::Taker, rate_bps, FeeSource::ConfigOverride);
    book.insert(key, entry);
    let mut v = Venue::with(config(
        Bracket::Optimistic,
        VenueFeeSign::PositiveIsCost,
        book,
    ));
    v.snapshot(T0, &[(3, 1)], &[(4, 1)]);
    v.send(
        place(cid(), Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
        1,
        T0,
    );
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(rejected(&got[0].1), Some(RejectKind::Other));
}

#[test]
fn fills_say_only_what_the_stood_in_venue_reports() {
    // Codex r4182448147, r4182448157: a venue without fill ids keys its fills by order and
    // cumulative quantity, and one without a liquidity flag does not say maker or taker.
    let mut bare = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
    bare.exec.fills.fill_id = false;
    bare.exec.fills.liquidity_flag = false;
    let mut v = Venue::with(bare);
    v.snapshot(T0, &[(198, 1)], &[(201, 2)]);
    v.send(
        place(cid(), Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
        1,
        T0,
    );
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let ExecEvent::Outcome {
        item: Some(ItemRef { vid: Some(vid), .. }),
        ..
    } = &got[0].1
    else {
        panic!("{got:?}")
    };
    let ExecEvent::Fill(f) = &got[1].1 else {
        panic!("{got:?}")
    };
    let derived = FillIdent::Derived {
        vid: vid.clone(),
        cum_after: lots(1),
    };
    assert_eq!((&f.ident, f.liquidity), (&derived, Liquidity3::Unknown));
}

#[test]
fn a_repeated_level_keeps_what_the_trades_explain() {
    // Codex r4182448165: a level update that repeats the level's size changes nothing, so the
    // trades printed at it still explain its next shrink.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 2), 1, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 3);
    v.level(T0 + 7 * MS, BookSide::Bid, 199, 6);
    v.level(T0 + 8 * MS, BookSide::Bid, 199, 3);
    // Three traded of the six ahead: three remain, so a trade of three fills nothing.
    v.trade(T0 + 9 * MS, Aggressor::Seller, 199, 3);
    let got = v.answers();
    assert_eq!(
        got.iter().filter_map(|(_, ev)| fill_of(ev)).count(),
        0,
        "{got:?}"
    );
}

#[test]
fn trades_past_an_i64_of_lots_still_explain_a_shrink() {
    // Codex r4182448176: the trades at a level saturate rather than drop a print. Printed
    // before the order arrived and not yet shown by the level, they took everything ahead of it
    // (Codex r4184245578), so a trade of two fills it; a dropped print would leave nine ahead.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 10)], &[(201, 5)]);
    v.trade(T0 + MS, Aggressor::Seller, 199, 1);
    v.trade(T0 + MS, Aggressor::Seller, 199, i64::MAX);
    v.send(limit(cid(), Side::Buy, 199, 2), 1, T0 + 2 * MS);
    v.tick(T0 + 7 * MS);
    assert_eq!(v.answers().len(), 2);
    // The level's shrink the trades explain moves nothing.
    v.level(T0 + 8 * MS, BookSide::Bid, 199, 5);
    v.trade(T0 + 9 * MS, Aggressor::Seller, 199, 2);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        2,
        "{got:?}"
    );
}

#[test]
fn a_snapshot_that_hides_a_level_ends_what_its_trades_explain() {
    // Codex r4182678488: a replacement snapshot whose depth no longer reaches a level cannot
    // compare its sizes, but it still ends what the trades printed there explain, so a later
    // shrink of that level is a cancel.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6), (198, 4)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 198, 2), 1, T0);
    // Three printed at 198 before the order arrives: no queue meets them.
    v.trade(T0 + MS, Aggressor::Seller, 198, 3);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    // A snapshot down to 199 only: 198 is past its depth.
    v.snapshot(T0 + 6 * MS, &[(199, 6)], &[(201, 5)]);
    // 198 shown again at four, then three of them cancelled: one left ahead.
    v.level(T0 + 7 * MS, BookSide::Bid, 198, 4);
    v.level(T0 + 8 * MS, BookSide::Bid, 198, 1);
    v.trade(T0 + 9 * MS, Aggressor::Seller, 198, 3);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        2,
        "{got:?}"
    );
}

#[test]
fn a_zero_size_placement_is_refused_and_nothing_is_placed() {
    // Codex r4182678519: zero lots is never an order (InstrumentSpec::floor_qty), so the venue
    // refuses it as an invalid quantity rather than report it Filled.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    let zero = |cid| {
        VenueCommand::Place(NewOrder {
            cid,
            inst: INST,
            side: Side::Buy,
            qty: Lots::ZERO,
            kind: OrderKind::Limit { px: Ticks(199) },
            tif: TifTag::Gtc,
            channel: Channel::Public,
            post_only: false,
            reduce_only: false,
            reducing: false,
        })
    };
    let order = cid();
    v.send(zero(order), 1, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(rejected(&got[0].1), Some(RejectKind::InvalidQty));
    // Nothing was placed: a cancel of it is not found.
    v.send(cancel(OrderRef::Client(order)), 2, T0 + 6 * MS);
    v.tick(T0 + 11 * MS);
    let got = v.answers();
    assert_eq!(rejected(&got[0].1), Some(RejectKind::NotFound));
}

#[test]
fn order_events_say_only_what_the_stood_in_venue_echoes() {
    // Codex r4182678498, r4182678504: a venue that does not echo client ids on its events, or
    // the order's flags, says neither; the outcome of our own command still names its order.
    let mut bare = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
    bare.exec.order.cid_echoed_on_events = false;
    bare.exec.order.events_echo_flags = false;
    let mut v = Venue::with(bare);
    v.snapshot(T0, &[(198, 1)], &[(201, 2)]);
    let order = cid();
    v.send(
        place(order, Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
        1,
        T0,
    );
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(got.len(), 3, "{got:?}");
    let ExecEvent::Outcome {
        item: Some(ItemRef {
            cid: Some(echo), ..
        }),
        ..
    } = &got[0].1
    else {
        panic!("{got:?}")
    };
    assert_eq!(*echo, order);
    let ExecEvent::Fill(f) = &got[1].1 else {
        panic!("{got:?}")
    };
    assert_eq!(f.cid, None);
    let ExecEvent::Order(o) = &got[2].1 else {
        panic!("{got:?}")
    };
    assert_eq!((&o.cid, o.post_only, o.reduce_only), (&None, None, None));

    // And a venue that echoes them says them.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(198, 1)], &[(201, 2)]);
    v.send(
        place(order, Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false),
        1,
        T0,
    );
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let ExecEvent::Fill(f) = &got[1].1 else {
        panic!("{got:?}")
    };
    assert_eq!(f.cid, Some(CidMatch::Ours(order)));
    let ExecEvent::Order(o) = &got[2].1 else {
        panic!("{got:?}")
    };
    assert_eq!(
        (&o.cid, o.post_only, o.reduce_only),
        (&Some(CidMatch::Ours(order)), Some(false), Some(false))
    );
}

#[test]
fn a_two_phase_venue_is_not_stood_in_for_yet() {
    // Codex r4182678509: the engine accepts in one phase, so for a venue that acknowledges in
    // two the codec sends no placement rather than report a provisional acceptance as final
    // (FBC-zr1 models both phases). A cancel, which the ack model does not govern, still goes.
    let mut two = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
    two.exec.order.ack = AckModel::TwoPhase {
        risk_reject_window: Duration::from_millis(50),
    };
    let mut v = Venue::with(two);
    let refused = v.encode(&limit(cid(), Side::Buy, 199, 1), 1, T0);
    assert_eq!(refused.err(), Some(NotSentReason::Unsupported));
    assert!(v.encode(&cancel(OrderRef::Client(cid())), 2, T0).is_ok());
}

#[test]
fn a_venue_whose_events_the_engine_cannot_say_yet_is_not_stood_in_for() {
    // Codex r4182991971, r4182991978: the engine orders its answers by a venue sequence and
    // keeps no position, and sends fills of their own; a venue ordered otherwise, whose fills
    // carry realized P&L or funding, or whose fills are derived from order status, gets no
    // placement rather than events unlike its own (FBC-938). Cancels still go.
    let changes: [&dyn Fn(&mut SimConfig); 6] = [
        &|c| c.exec.order.ordering_key = OrderingKey::VenueTs,
        &|c| c.exec.order.ordering_key = OrderingKey::BlockTime,
        &|c| c.exec.order.ordering_key = OrderingKey::None,
        &|c| c.exec.fills.realized_pnl = true,
        &|c| c.exec.fills.realized_funding = true,
        &|c| c.exec.fills.source = FillSource::DerivedFromOrderStatus,
    ];
    for change in changes {
        let mut config = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
        change(&mut config);
        let mut v = Venue::with(config);
        let refused = v.encode(&limit(cid(), Side::Buy, 199, 1), 1, T0);
        assert_eq!(refused.err(), Some(NotSentReason::Unsupported));
        assert!(v.encode(&cancel(OrderRef::Client(cid())), 2, T0).is_ok());
    }
}

#[test]
fn a_cancel_needs_no_spec_for_its_instrument() {
    // Codex r4182991965: a cancel names only the order, so an instrument the spec table no
    // longer lists does not stop it; a placement still needs the spec.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    let order = cid();
    v.send(limit(order, Side::Buy, 199, 1), 1, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    v.specs = SpecTable::new();
    assert_eq!(
        v.encode(&limit(cid(), Side::Buy, 199, 1), 2, T0 + 6 * MS)
            .err(),
        Some(NotSentReason::Unencodable)
    );
    v.send(cancel(OrderRef::Client(order)), 3, T0 + 6 * MS);
    v.tick(T0 + 11 * MS);
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((3, ACCEPTED)), "{got:?}");
}

#[test]
fn a_cancel_names_a_venue_id_by_its_exact_spelling() {
    // Codex r4182991987: "S00" and "S+0" are not "S0"; a cancel naming them finds nothing and
    // the order rests on.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 1), 1, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    for (rpc, spelling) in [(2, "S00"), (3, "S+0")] {
        let vid = dispatch(&v.caps, NS, |scope| scope.venue_order_id(spelling)).unwrap();
        v.send(cancel(OrderRef::Venue(vid)), rpc, T0 + 6 * MS);
    }
    let real = dispatch(&v.caps, NS, |scope| scope.venue_order_id("S0")).unwrap();
    v.send(cancel(OrderRef::Venue(real)), 4, T0 + 6 * MS);
    v.tick(T0 + 11 * MS);
    let got = v.answers();
    assert_eq!(rejected(&got[0].1), Some(RejectKind::NotFound), "{got:?}");
    assert_eq!(rejected(&got[1].1), Some(RejectKind::NotFound), "{got:?}");
    assert_eq!(outcome_of(&got[2].1), Some((4, ACCEPTED)), "{got:?}");
}

#[test]
fn a_placement_whose_level_would_overflow_its_lots_is_refused() {
    // Codex r4183438501: the size at a price, the public size, every simulated order resting
    // there and the new order together, must fit in an i64 of lots; an order that would push
    // it past is refused rather than leave the venue's own size truncated.
    let mut v = Venue::new(Bracket::Middle);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, i64::MAX - 10), 1, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((1, ACCEPTED)), "{got:?}");
    v.send(limit(cid(), Side::Buy, 199, 5), 2, T0 + 6 * MS);
    v.tick(T0 + 11 * MS);
    let got = v.answers();
    assert_eq!(rejected(&got[0].1), Some(RejectKind::InvalidQty), "{got:?}");
    // Four more fit exactly.
    v.send(limit(cid(), Side::Buy, 199, 4), 3, T0 + 12 * MS);
    v.tick(T0 + 17 * MS);
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((3, ACCEPTED)), "{got:?}");
}

#[test]
fn a_placement_outside_the_specs_size_limits_is_refused() {
    // Codex r4183669448: the venue refuses an order below the spec's minimum size or above its
    // largest order, as InvalidQty; the sizes at the limits are orders.
    let mut config = config(Bracket::Optimistic, VenueFeeSign::PositiveIsRebate, fees());
    let mut limited = spec(INST, "SIM-PERP");
    limited.min_size = lots(2);
    limited.max_order_size = Some(lots(4));
    config.specs.insert(limited);
    let mut v = Venue::with(config);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    for (rpc, qty) in [(1, 1), (2, 5), (3, 2), (4, 4)] {
        v.send(limit(cid(), Side::Buy, 199, qty), rpc, T0);
    }
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let outcomes: Vec<_> = got.iter().filter_map(|(_, ev)| outcome_of(ev)).collect();
    assert_eq!(outcomes.len(), 4, "{got:?}");
    assert_eq!(rejected(&got[0].1), Some(RejectKind::InvalidQty), "{got:?}");
    assert_eq!(rejected(&got[1].1), Some(RejectKind::InvalidQty), "{got:?}");
    assert_eq!(outcomes[2], (3, ACCEPTED));
    assert_eq!(outcomes[3], (4, ACCEPTED));
    // A trade of everything ahead and more fills only the two orders that rested.
    v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 20);
    let got = v.answers();
    let filled: Vec<_> = got
        .iter()
        .filter_map(|(_, ev)| fill_of(ev))
        .map(|f| f.1)
        .collect();
    assert_eq!(filled, [2, 4], "{got:?}");
}

#[test]
fn an_engine_without_the_instruments_spec_places_nothing() {
    // Codex r4183669448: with no spec the venue cannot check an order's size, so it refuses
    // the placement (`no_book`) rather than accept a size it cannot judge.
    let mut v = Venue::new(Bracket::Optimistic);
    let mut bare = config(Bracket::Optimistic, VenueFeeSign::PositiveIsRebate, fees());
    bare.specs = SpecTable::new();
    v.engine = SimEngine::new(&bare);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 2), 1, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    assert_eq!(got.len(), 1, "{got:?}");
    let ExecEvent::Outcome {
        outcome: SubmitOutcome::Rejected(r),
        ..
    } = &got[0].1
    else {
        panic!("{got:?}")
    };
    assert_eq!(
        (r.kind, r.venue_code.as_deref()),
        (RejectKind::Other, Some("no_book"))
    );
}

#[test]
fn a_level_a_delta_first_shows_ends_what_earlier_trades_explain() {
    // Codex r4183669454: trades printed at a price the capped book did not reach explain
    // nothing once a delta first shows that level, since its size already reflects them; a
    // later shrink with no trade is a level cancel.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    // 198 is past the book's depth: the print there meets no known level.
    v.trade(T0 + MS, Aggressor::Seller, 198, 3);
    v.level(T0 + 2 * MS, BookSide::Bid, 198, 4);
    v.send(limit(cid(), Side::Buy, 198, 2), 1, T0 + 2 * MS);
    v.tick(T0 + 7 * MS);
    assert_eq!(v.answers().len(), 2);
    // Three of the four ahead cancelled: one left ahead, so a trade of three fills both lots.
    v.level(T0 + 8 * MS, BookSide::Bid, 198, 1);
    v.trade(T0 + 9 * MS, Aggressor::Seller, 198, 3);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        2,
        "{got:?}"
    );
}

#[test]
fn a_trade_the_venue_cannot_charge_spends_nothing_on_the_order_it_cancels() {
    // Codex r4183669469: an order a trade would fill with no maker rate is cancelled, and the
    // print's size it would have taken reaches the next order at the level instead.
    let mut v = Venue::with(config(
        Bracket::Optimistic,
        VenueFeeSign::PositiveIsRebate,
        FeeBook::new(),
    ));
    v.snapshot(T0, &[(199, 1)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 1), 1, T0);
    v.send(limit(cid(), Side::Buy, 199, 1), 2, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 4);
    // One lot ahead of both and one more: the first order cannot be charged, so the venue
    // cancels it and the lot reaches the second, which it cancels too, filling neither.
    v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 2);
    let got = v.answers();
    let states: Vec<_> = got.iter().filter_map(|(_, ev)| state_of(ev)).collect();
    assert_eq!(
        states,
        vec![(VenueOrderState::Canceled(CancelReason::Venue), 0); 2],
        "{got:?}"
    );
    assert_eq!(got.len(), 2, "{got:?}");
}

#[test]
fn a_snapshot_repeating_a_levels_size_ends_what_earlier_trades_explain() {
    // Codex r4184026666: a replacement snapshot restates the level, so its size already
    // reflects the trades printed before it even when it equals the size it replaced (public
    // size joined as much as traded); a later shrink with no trade is a level cancel.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 2), 1, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    // Three of the six ahead trade; the snapshot shows the level at six again.
    v.trade(T0 + 6 * MS, Aggressor::Seller, 199, 3);
    v.snapshot(T0 + 7 * MS, &[(199, 6)], &[(201, 5)]);
    // Three cancelled: the three left ahead go, so a trade of two fills both lots.
    v.level(T0 + 8 * MS, BookSide::Bid, 199, 3);
    v.trade(T0 + 9 * MS, Aggressor::Seller, 199, 2);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        2,
        "{got:?}"
    );
}

#[test]
fn a_limit_price_off_the_specs_grid_is_refused() {
    // Codex r4184245564: a limit price the instrument's grid does not accept (two significant
    // figures here: 19.9 is not one, 19 is) is refused as an invalid price, and nothing rests.
    let mut config = config(Bracket::Optimistic, VenueFeeSign::PositiveIsRebate, fees());
    let mut sig = spec(INST, "SIM-PERP");
    sig.price_grid = PriceGrid::sig_figs(2, 1, false).unwrap();
    config.specs.insert(sig);
    let mut v = Venue::with(config);
    v.snapshot(T0, &[(190, 6), (199, 1)], &[(201, 5)]);
    let off = cid();
    v.send(limit(off, Side::Buy, 199, 1), 1, T0);
    v.send(limit(cid(), Side::Buy, 190, 1), 2, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let ExecEvent::Outcome {
        outcome: SubmitOutcome::Rejected(r),
        ..
    } = &got[0].1
    else {
        panic!("{got:?}")
    };
    assert_eq!(
        (r.kind, r.venue_code.as_deref()),
        (RejectKind::InvalidPrice, Some("invalid_px"))
    );
    assert_eq!(outcome_of(&got[1].1), Some((2, ACCEPTED)), "{got:?}");
    v.send(cancel(OrderRef::Client(off)), 3, T0 + 6 * MS);
    v.tick(T0 + 11 * MS);
    let got = v.answers();
    assert_eq!(rejected(&got[0].1), Some(RejectKind::NotFound), "{got:?}");
}

#[test]
fn a_venue_with_a_speed_bump_is_not_stood_in_for_yet() {
    // Codex r4184245574: the engine acts on a command `to_venue` after it was sent, with no
    // speed bump; a venue that delays orders gets no placement rather than fills it would not
    // give (FBC-7y8). Cancels still go.
    for applies_to in [
        SpeedBumpScope::TakersOnly,
        SpeedBumpScope::AllButCancels,
        SpeedBumpScope::Everything,
    ] {
        let mut config = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
        config.matching.speed_bump = Some(SpeedBump {
            delay: Duration::from_millis(25),
            applies_to,
        });
        let mut v = Venue::with(config);
        let refused = v.encode(&limit(cid(), Side::Buy, 199, 1), 1, T0);
        assert_eq!(refused.err(), Some(NotSentReason::Unsupported));
        assert!(v.encode(&cancel(OrderRef::Client(cid())), 2, T0).is_ok());
    }
}

#[test]
fn an_order_arriving_after_a_trade_its_level_has_not_shown_is_not_behind_that_trade() {
    // Codex r4184245578: a trade printed before the order arrived, whose shrink the level
    // shows only after, had already taken its size: the order queues behind what is left, and
    // the shrink, which that trade explains, moves nothing.
    let mut v = Venue::new(Bracket::Pessimistic);
    v.snapshot(T0, &[(199, 6)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 2), 1, T0);
    v.trade(T0 + 4 * MS, Aggressor::Seller, 199, 4);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    v.level(T0 + 6 * MS, BookSide::Bid, 199, 2);
    // Two left ahead: a trade of four fills both lots.
    v.trade(T0 + 7 * MS, Aggressor::Seller, 199, 4);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        2,
        "{got:?}"
    );
}

#[test]
fn an_order_arriving_after_a_trade_through_its_level_is_not_behind_that_level() {
    // Codex r4184546701: a buy printed at 201 emptied the offer at 200 before it, though the
    // level shows that only later; an order arriving at 200 in between queues behind none of
    // it, and the level's removal moves nothing.
    let mut v = Venue::new(Bracket::Pessimistic);
    v.snapshot(T0, &[(198, 5)], &[(200, 3), (201, 4)]);
    v.send(limit(cid(), Side::Sell, 200, 2), 1, T0);
    v.trade(T0 + 4 * MS, Aggressor::Buyer, 201, 5);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    v.level(T0 + 6 * MS, BookSide::Ask, 200, 0);
    v.trade(T0 + 7 * MS, Aggressor::Buyer, 200, 2);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        2,
        "{got:?}"
    );
}

#[test]
fn a_venue_that_replays_fills_on_reconnect_is_not_stood_in_for_yet() {
    // Codex r4184546713: the engine never replays a fill, so a venue whose fills replay on
    // reconnect gets no placement rather than a session that never shows a replay (FBC-3q6).
    // Cancels still go.
    let mut config = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
    config.exec.fills.replays_fills_on_reconnect = true;
    let mut v = Venue::with(config);
    let refused = v.encode(&limit(cid(), Side::Buy, 199, 1), 1, T0);
    assert_eq!(refused.err(), Some(NotSentReason::Unsupported));
    assert!(v.encode(&cancel(OrderRef::Client(cid())), 2, T0).is_ok());
}

#[test]
fn a_zero_size_trade_through_a_level_explains_nothing_there() {
    // Codex r4184778419: a trade of no size says nothing of the levels it printed through, so
    // the offer at 200 keeps all three lots: an order arriving there queues behind them, and a
    // trade of two at 200 fills none of it.
    let mut v = Venue::new(Bracket::Pessimistic);
    v.snapshot(T0, &[(198, 5)], &[(200, 3), (201, 4)]);
    v.trade(T0 + MS, Aggressor::Buyer, 201, 0);
    v.send(limit(cid(), Side::Sell, 200, 2), 1, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    v.trade(T0 + 7 * MS, Aggressor::Buyer, 200, 2);
    let got = v.answers();
    assert!(got.iter().all(|(_, ev)| fill_of(ev).is_none()), "{got:?}");
    // One lot still ahead: a trade of three fills both of the order's.
    v.trade(T0 + 8 * MS, Aggressor::Buyer, 200, 3);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        2,
        "{got:?}"
    );
}

#[test]
fn a_venue_that_cancels_on_disconnect_is_not_stood_in_for_yet() {
    // Codex r4184778435: the simulated stream has no disconnect, so nothing would cancel what
    // a venue cancels when its connection drops or its dead-man timer lapses; such a venue
    // gets no placement rather than orders that outlive a disconnect it would end them on
    // (FBC-fji models the lifecycle). Cancels still go.
    for on_disconnect in [
        CancelOnDisconnect::PerConnection {
            rearm_on_reconnect: false,
        },
        CancelOnDisconnect::PerConnection {
            rearm_on_reconnect: true,
        },
        CancelOnDisconnect::DeadMan {
            max_ttl: Duration::from_secs(5),
        },
    ] {
        let mut config = config(Bracket::Optimistic, VenueFeeSign::PositiveIsCost, fees());
        config.exec.order.cancel_on_disconnect = on_disconnect;
        let mut v = Venue::with(config);
        let refused = v.encode(&limit(cid(), Side::Buy, 199, 1), 1, T0);
        assert_eq!(refused.err(), Some(NotSentReason::Unsupported));
        assert!(v.encode(&cancel(OrderRef::Client(cid())), 2, T0).is_ok());
    }
}

#[test]
fn a_crossing_order_takes_nothing_an_earlier_trade_already_took() {
    // Codex r4185186386: a buy of five printed at 200 took the whole offer there before the
    // book shows it gone. A crossing buy arriving in between takes its four lots at 201, and
    // a post-only buy at 200 crosses nothing and rests.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(198, 5)], &[(200, 5), (201, 4)]);
    v.trade(T0 + MS, Aggressor::Buyer, 200, 5);
    let taker = place(
        cid(),
        Side::Buy,
        OrderKind::Limit { px: Ticks(201) },
        4,
        TifTag::Gtc,
        false,
    );
    v.send(taker, 1, T0);
    v.tick(T0 + 5 * MS);
    let got = v.answers();
    let fills: Vec<_> = got.iter().filter_map(|(_, ev)| fill_of(ev)).collect();
    assert_eq!(
        fills.iter().map(|f| (f.0, f.1)).collect::<Vec<_>>(),
        [(201, 4)],
        "{got:?}"
    );
    v.send(limit(cid(), Side::Buy, 200, 1), 2, T0 + 6 * MS);
    v.tick(T0 + 11 * MS);
    let got = v.answers();
    assert_eq!(outcome_of(&got[0].1), Some((2, ACCEPTED)), "{got:?}");
}

#[test]
fn a_level_whose_public_size_and_own_orders_pass_an_i64_still_advances_by_its_share() {
    // Codex r4185186401: the order rests behind 2^61 public lots with 2^62 of its own. The
    // public size grows to 2^62 + 2^61, so the level holds 2^63 + 2^61 lots in all, then
    // shrinks back by 2^62 with no trade: the Middle bracket advances the order by
    // floor(2^62 × 2^61 / (2^63 + 2^61)) = floor(2^62 / 5), not by a share of a level
    // truncated to an i64, so a trade a little smaller than what is left ahead fills nothing.
    let p = 1_i64 << 61;
    let mut v = Venue::new(Bracket::Middle);
    v.snapshot(T0, &[(199, p)], &[(201, 5)]);
    v.send(limit(cid(), Side::Buy, 199, 1 << 62), 1, T0);
    v.tick(T0 + 5 * MS);
    assert_eq!(v.answers().len(), 2);
    v.level(T0 + 6 * MS, BookSide::Bid, 199, (1 << 62) + p);
    v.level(T0 + 7 * MS, BookSide::Bid, 199, p);
    let ahead = p - (1 << 62) / 5;
    v.trade(T0 + 8 * MS, Aggressor::Seller, 199, ahead);
    let got = v.answers();
    assert!(got.is_empty(), "{got:?}");
    v.trade(T0 + 9 * MS, Aggressor::Seller, 199, 1);
    let got = v.answers();
    assert_eq!(
        got.iter()
            .filter_map(|(_, ev)| fill_of(ev))
            .map(|f| f.1)
            .sum::<i64>(),
        1,
        "{got:?}"
    );
}

// FBC-nv2: amends, batches, cancel-many, cancel-all, queries and injected orders.

impl Venue {
    /// Every event the engine's answers since the last call decode to, a batch's item outcomes
    /// included, each with its frame's due time.
    fn events(&mut self) -> Vec<(MonoNs, ExecEvent)> {
        let mut out = Vec::new();
        for answer in self.engine.take_answers() {
            let events = self.decode(&answer.frame).unwrap();
            out.extend(events.into_iter().map(|(_, ev)| (answer.at, ev)));
        }
        out
    }

    /// Places `cmd` at `at` and lets it act: the venue id it was given.
    fn rest(&mut self, cmd: VenueCommand, rpc: u64, at: u64) -> VenueOrderId {
        self.send(cmd, rpc, at);
        self.tick(at + 5 * MS);
        let got = self.events();
        let ExecEvent::Outcome {
            item: Some(ItemRef { vid: Some(vid), .. }),
            outcome: SubmitOutcome::Accepted { .. },
            ..
        } = &got[0].1
        else {
            panic!("{got:?}")
        };
        vid.clone()
    }

    /// Where the resting order `vid` sits: the size ahead of it.
    fn ahead(&self, vid: &VenueOrderId) -> i64 {
        self.engine.queue_position(vid).unwrap().ahead.get()
    }
}

/// Amends by venue or client id that change price, size and flags, of partially filled orders
/// too, keeping the venue id and the original on a reject; `keeps_priority` as given.
fn amend_caps(keeps_priority: Option<bool>) -> AmendCaps {
    AmendCaps {
        refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        price: true,
        qty: true,
        flags: true,
        when_partially_filled: true,
        reject_keeps_original: true,
        keeps_venue_id: true,
        ack: AmendAck::ReplacedEvent,
        qty_semantics: AmendQty::TotalIncludingFilled,
        keeps_priority,
    }
}

/// The configuration of a venue offering amends, batches of four, cancel-all by instrument and
/// queries by venue or client id.
fn rich_config(bracket: Bracket) -> SimConfig {
    let mut c = config(bracket, VenueFeeSign::PositiveIsRebate, fees());
    let o = &mut c.exec.order;
    o.amend = Some(amend_caps(None));
    o.batch_place = Some(Batch { max_items: 4 });
    o.batch_cancel = Some(CancelBatch {
        max_items: 4,
        refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
    });
    o.cancel_all_instrument = Support::Native;
    o.query_refs = TagSet::of(&[RefKind::Venue, RefKind::Client]);
    c
}

/// That venue, its order caps then changed by `f`.
fn rich(bracket: Bracket, f: impl FnOnce(&mut OrderCaps)) -> Venue {
    let mut c = rich_config(bracket);
    f(&mut c.exec.order);
    Venue::with(c)
}

/// A post-only good-till-cancelled amend of `target` to `qty` lots in total at `px`, the OMS
/// having seen `cum` filled.
fn amend(target: OrderRef, side: Side, px: i64, qty: i64, cum: i64) -> AmendOrder {
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
        cum_filled: lots(cum),
    }
}

fn order_of(ev: &ExecEvent) -> &OrderUpdate {
    let ExecEvent::Order(o) = ev else {
        panic!("{ev:?}")
    };
    o
}

/// The venue ids of the fills among `got`, in order.
fn filled(got: &[(MonoNs, ExecEvent)]) -> Vec<VenueOrderId> {
    let fills = got.iter().filter_map(|(_, ev)| match ev {
        ExecEvent::Fill(f) => f.vid().cloned(),
        _ => None,
    });
    fills.collect()
}

fn refusal(ev: &ExecEvent) -> (RejectKind, String) {
    let Some((_, SubmitOutcome::Rejected(r))) = outcome_of(ev) else {
        panic!("{ev:?}")
    };
    (r.kind, r.venue_code.as_deref().unwrap().to_owned())
}

#[test]
fn an_amend_resets_the_orders_queue_position_unless_its_caps_say_amends_keep_priority() {
    let mut aheads = Vec::new();
    for keeps in [None, Some(false), Some(true)] {
        let mut v = rich(Bracket::Pessimistic, |o| o.amend = Some(amend_caps(keeps)));
        v.snapshot(T0, &[(199, 10)], &[(201, 5)]);
        let first = cid();
        let a = v.rest(limit(first, Side::Buy, 199, 2), 1, T0);
        let b = v.rest(limit(cid(), Side::Buy, 199, 1), 2, T0 + 10 * MS);
        // Four lots trade at the price and the level shows it; three public lots join behind
        // both orders.
        v.trade(T0 + 20 * MS, Aggressor::Seller, 199, 4);
        v.level(T0 + 21 * MS, BookSide::Bid, 199, 6);
        v.level(T0 + 22 * MS, BookSide::Bid, 199, 9);
        assert_eq!((v.ahead(&a), v.ahead(&b)), (6, 6), "{keeps:?}");

        // The first grows from two lots to three at its price.
        let target = OrderRef::Both(first, a.clone());
        let cmd = VenueCommand::Amend(amend(target, Side::Buy, 199, 3, 0));
        v.send(cmd, 3, T0 + 30 * MS);
        v.tick(T0 + 35 * MS);
        let got = v.events();
        assert_eq!(got.len(), 2, "{got:?}");
        let ExecEvent::Outcome {
            rpc,
            item: Some(item),
            outcome,
        } = &got[0].1
        else {
            panic!("{got:?}")
        };
        assert_eq!((*rpc, outcome), (RpcId(3), &ACCEPTED));
        assert_eq!((item.cid, item.vid.as_ref()), (Some(first), Some(&a)));
        let o = order_of(&got[1].1);
        assert_eq!(
            (o.state.clone(), o.qty, o.px, o.vid.as_ref()),
            (
                VenueOrderState::Open,
                Some(lots(3)),
                Some(Ticks(199)),
                Some(&a)
            )
        );
        let pos = v.engine.queue_position(&a).unwrap();
        assert_eq!(pos.remaining, lots(3));
        aheads.push(pos.ahead.get());

        // Reset, it queues behind the other order and the nine lots now shown; kept, it is
        // still first, six lots from the front. Seven lots sold at the price fill whichever
        // order is first, by one lot.
        v.trade(T0 + 40 * MS, Aggressor::Seller, 199, 7);
        let first_filled = if keeps == Some(true) { &a } else { &b };
        assert_eq!(
            filled(&v.events()),
            std::slice::from_ref(first_filled),
            "{keeps:?}"
        );
    }
    assert_eq!(aheads, [9, 9, 6]);
}

#[test]
fn an_amend_to_another_price_is_a_new_order_there_and_crosses_as_one() {
    let mut v = rich(Bracket::Middle, |o| o.amend = Some(amend_caps(Some(true))));
    v.snapshot(T0, &[(199, 10), (198, 4)], &[(201, 5), (202, 5)]);
    let first = cid();
    let a = v.rest(limit(first, Side::Buy, 199, 2), 1, T0);
    // A new price is no place to keep: it queues behind the four lots at 198.
    let to = |px, qty, post_only| {
        let mut cmd = amend(OrderRef::Venue(a.clone()), Side::Buy, px, qty, 0);
        cmd.post_only = post_only;
        VenueCommand::Amend(cmd)
    };
    v.send(to(198, 2, true), 2, T0 + 10 * MS);
    v.tick(T0 + 15 * MS);
    let got = v.events();
    assert_eq!(outcome_of(&got[0].1), Some((2, ACCEPTED)));
    assert_eq!(order_of(&got[1].1).px, Some(Ticks(198)));
    assert_eq!(v.ahead(&a), 4);

    // Post-only across the offer: refused, and the order keeps its place and price.
    v.send(to(201, 2, true), 3, T0 + 20 * MS);
    v.tick(T0 + 25 * MS);
    let got = v.events();
    assert_eq!(got.len(), 1, "{got:?}");
    let refused = (RejectKind::PostOnlyWouldCross, "post_only".to_owned());
    assert_eq!(refusal(&got[0].1), refused);
    assert_eq!(v.ahead(&a), 4);

    // Without post-only (the caps amend flags), it takes as a placement would and fills.
    v.send(to(201, 2, false), 4, T0 + 30 * MS);
    v.tick(T0 + 35 * MS);
    let got = v.events();
    assert_eq!(outcome_of(&got[0].1), Some((4, ACCEPTED)));
    let taker = expected_fee(201, 2, TAKER_BPS);
    assert_eq!(fill_of(&got[1].1), Some((201, 2, Liquidity3::Taker, taker)));
    let o = order_of(&got[2].1);
    assert_eq!(
        (o.state.clone(), o.cum_filled, o.post_only),
        (VenueOrderState::Filled, lots(2), Some(false))
    );
    assert_eq!(v.engine.queue_position(&a), None);
    // It is over: a further amend is refused as of an order already filled.
    v.send(to(198, 2, true), 5, T0 + 40 * MS);
    v.tick(T0 + 45 * MS);
    let refused = (
        RejectKind::AlreadyTerminal(TerminalHint::Filled),
        "filled".to_owned(),
    );
    assert_eq!(refusal(&v.events()[0].1), refused);
}

#[test]
fn an_amend_its_caps_do_not_allow_is_refused_and_the_order_stays() {
    let unsupported = (
        RejectKind::NotAmendable(NotAmendable::Unsupported),
        "not_amendable".to_owned(),
    );
    let partial = (
        RejectKind::NotAmendable(NotAmendable::PartiallyFilled),
        "partially_filled".to_owned(),
    );
    let invalid_qty = (RejectKind::InvalidQty, "invalid_qty".to_owned());
    let invalid_px = (RejectKind::InvalidPrice, "invalid_px".to_owned());
    // Each case: the caps, the amend of a two-lot buy at 190 of which one lot filled, and
    // the refusal. The grid takes two significant figures (19.0 and 18.0, not 19.1), and
    // orders of at most three lots.
    type Case = (
        fn(&mut AmendCaps),
        fn(&mut AmendOrder),
        (RejectKind, String),
    );
    let cases: Vec<Case> = vec![
        (
            |c| c.price = false,
            |a| a.px = Ticks(180),
            unsupported.clone(),
        ),
        (|c| c.qty = false, |a| a.qty = lots(3), unsupported.clone()),
        (
            |c| c.flags = false,
            |a| a.reduce_only = true,
            unsupported.clone(),
        ),
        (|_| {}, |a| a.side = Side::Sell, unsupported.clone()),
        (|_| {}, |a| a.tif = TifTag::Ioc, unsupported.clone()),
        (|_| {}, |a| a.inst = BOOKLESS, unsupported.clone()),
        (
            |c| c.when_partially_filled = false,
            |a| a.px = Ticks(180),
            partial,
        ),
        // The OMS has not seen the fill: a total of one lot is what the venue has filled.
        (
            |_| {},
            |a| (a.qty, a.cum_filled) = (lots(1), lots(0)),
            invalid_qty.clone(),
        ),
        (|_| {}, |a| a.qty = lots(4), invalid_qty),
        (|_| {}, |a| a.px = Ticks(191), invalid_px),
    ];
    for (caps, change, refused) in cases {
        let mut config = rich_config(Bracket::Middle);
        let mut c = amend_caps(None);
        caps(&mut c);
        config.exec.order.amend = Some(c);
        let mut sig = spec(INST, "SIM-PERP");
        sig.price_grid = PriceGrid::sig_figs(2, 1, false).unwrap();
        sig.max_order_size = Some(lots(3));
        config.specs.insert(sig);
        let mut v = Venue::with(config);
        v.snapshot(T0, &[(180, 4), (190, 4)], &[(200, 5)]);
        let a = v.rest(limit(cid(), Side::Buy, 190, 2), 1, T0);
        v.trade(T0 + 10 * MS, Aggressor::Seller, 190, 5);
        assert_eq!(filled(&v.events()), std::slice::from_ref(&a));
        let mut cmd = amend(OrderRef::Venue(a.clone()), Side::Buy, 190, 2, 1);
        change(&mut cmd);
        v.send(VenueCommand::Amend(cmd), 2, T0 + 20 * MS);
        v.tick(T0 + 25 * MS);
        let got = v.events();
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(refusal(&got[0].1), refused);
        let pos = v.engine.queue_position(&a).unwrap();
        assert_eq!((pos.px, pos.remaining), (Ticks(190), lots(1)));
    }
}

#[test]
fn an_amend_reads_its_quantity_as_the_venue_does_and_a_refused_one_may_end_the_order() {
    // A venue whose amend quantity is what remains, and which drops the order whose amend it
    // refuses.
    let mut v = rich(Bracket::Middle, |o| {
        let mut c = amend_caps(None);
        c.qty_semantics = AmendQty::Remaining;
        c.reject_keeps_original = false;
        o.amend = Some(c);
    });
    v.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let first = cid();
    let a = v.rest(limit(first, Side::Buy, 199, 3), 1, T0);
    v.trade(T0 + 10 * MS, Aggressor::Seller, 199, 5);
    assert_eq!(filled(&v.events()), std::slice::from_ref(&a));
    // A total of five with one filled rests four more: five in all, one filled.
    let cmd = amend(OrderRef::Client(first), Side::Buy, 199, 5, 1);
    v.send(VenueCommand::Amend(cmd), 2, T0 + 20 * MS);
    v.tick(T0 + 25 * MS);
    let got = v.events();
    let o = order_of(&got[1].1);
    assert_eq!((o.qty, o.cum_filled), (Some(lots(5)), lots(1)));
    assert_eq!(v.engine.queue_position(&a).unwrap().remaining, lots(4));
    // Refused (it would cross post-only): the venue cancels the order with it.
    let cmd = amend(OrderRef::Client(first), Side::Buy, 201, 5, 1);
    v.send(VenueCommand::Amend(cmd), 3, T0 + 30 * MS);
    v.tick(T0 + 35 * MS);
    let got = v.events();
    assert_eq!(refusal(&got[0].1).0, RejectKind::PostOnlyWouldCross);
    assert_eq!(
        state_of(&got[1].1),
        Some((VenueOrderState::Canceled(CancelReason::Venue), 1))
    );
    assert_eq!(v.engine.queue_position(&a), None);
    // A remaining quantity that would take the total past an i64 is no quantity; an order
    // never placed is not found. (Written as the codec would, which sends neither.)
    let frame = |rpc: u64, target: &str, qty: i64| {
        format!(
            "amend|rpc={rpc}|mono={}|wall=0|{target}|inst=1|side=B|px=199|qty={qty}|tif=gtc|po=1|ro=0",
            T0 + 40 * MS
        )
    };
    let b = v.rest(limit(cid(), Side::Buy, 199, 2), 4, T0 + 40 * MS);
    // Nothing is left ahead of it (the five lots traded took the four shown): one lot fills.
    v.trade(T0 + 50 * MS, Aggressor::Seller, 199, 1);
    assert_eq!(filled(&v.events()), std::slice::from_ref(&b));
    let b_target = format!("vid={}", b.as_str());
    for (rpc, target, qty) in [(5, b_target.as_str(), i64::MAX), (6, "vid=S99", 1)] {
        let mut sent = frame(rpc, target, qty);
        sent = sent.replace(
            &format!("mono={}", T0 + 40 * MS),
            &format!("mono={}", T0 + 60 * MS),
        );
        v.engine.on_order_frame(sent.as_bytes()).unwrap();
    }
    v.tick(T0 + 65 * MS);
    let got = v.events();
    assert_eq!(refusal(&got[0].1).0, RejectKind::InvalidQty);
    assert_eq!(
        state_of(&got[1].1).unwrap().0,
        VenueOrderState::Canceled(CancelReason::Venue)
    );
    assert_eq!(refusal(&got[2].1).0, RejectKind::NotFound);
}

#[test]
fn an_amend_keeping_priority_still_fits_its_level_and_an_engine_without_amends_refuses_one() {
    let mut v = rich(Bracket::Middle, |o| o.amend = Some(amend_caps(Some(true))));
    v.snapshot(T0, &[(199, i64::MAX - 2)], &[(201, 5)]);
    let first = cid();
    let a = v.rest(limit(first, Side::Buy, 199, 2), 1, T0);
    // Three lots would take the level past an i64: refused, and it keeps its two.
    let cmd = amend(OrderRef::Venue(a.clone()), Side::Buy, 199, 3, 0);
    v.send(VenueCommand::Amend(cmd), 2, T0 + 10 * MS);
    v.tick(T0 + 15 * MS);
    let got = v.events();
    assert_eq!(refusal(&got[0].1).0, RejectKind::InvalidQty);
    assert_eq!(v.engine.queue_position(&a).unwrap().remaining, lots(2));
    // One lot fits, and keeps its place.
    let ahead = v.ahead(&a);
    let cmd = amend(OrderRef::Venue(a.clone()), Side::Buy, 199, 1, 0);
    v.send(VenueCommand::Amend(cmd), 3, T0 + 20 * MS);
    v.tick(T0 + 25 * MS);
    assert_eq!(outcome_of(&v.events()[0].1), Some((3, ACCEPTED)));
    assert_eq!(v.ahead(&a), ahead);

    // An engine standing in for a venue with no amends refuses one written to it.
    let mut v = Venue::new(Bracket::Middle);
    v.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let a = v.rest(limit(cid(), Side::Buy, 199, 2), 1, T0);
    let frame = format!(
        "amend|rpc=2|mono={}|wall=0|vid={}|inst=1|side=B|px=199|qty=3|tif=gtc|po=1|ro=0",
        T0 + 10 * MS,
        a.as_str()
    );
    v.engine.on_order_frame(frame.as_bytes()).unwrap();
    v.tick(T0 + 15 * MS);
    let refused = (
        RejectKind::NotAmendable(NotAmendable::Unsupported),
        "not_amendable".to_owned(),
    );
    assert_eq!(refusal(&v.events()[0].1), refused);
}

#[test]
fn the_codec_sends_an_amend_only_as_the_stood_in_venue_takes_it() {
    let mut v = rich(Bracket::Middle, |_| {});
    let vid = dispatch(&v.caps, NS, |scope| scope.venue_order_id("S1")).unwrap();
    let by_venue = || amend(OrderRef::Venue(vid.clone()), Side::Buy, 199, 2, 0);
    let refused = |v: &mut Venue, a: AmendOrder| v.encode(&VenueCommand::Amend(a), 1, T0).err();
    // Sent: charged as an amend, of the instrument, normal traffic unless it reduces.
    let fx = v.encode(&VenueCommand::Amend(by_venue()), 1, T0).unwrap();
    let [Effect::Send { class, charge, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    assert_eq!(
        (*class, *charge),
        (
            TrafficClass::Normal,
            RateCharge::one(OpKind::Amend, Some(INST))
        )
    );
    // What the venue's order caps do not offer, or the engine does not model.
    let cases: [fn(&mut AmendOrder); 3] = [
        |a| a.channel = Channel::Rpi,
        |a| a.tif = TifTag::Ioc,
        |a| a.target = OrderRef::Client(cid()),
    ];
    let mut narrow = rich(Bracket::Middle, |o| {
        o.tifs = TagSet::of(&[TifTag::Gtc]);
        o.amend = Some(AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            ..amend_caps(None)
        });
    });
    for change in cases {
        let mut a = by_venue();
        change(&mut a);
        assert_eq!(refused(&mut narrow, a), Some(NotSentReason::Unsupported));
    }
    // A venue that gives an amended order a new venue id, or whose events the engine cannot
    // say (a two-phase venue).
    let mut new_id = rich(Bracket::Middle, |o| {
        o.amend = Some(AmendCaps {
            keeps_venue_id: false,
            ..amend_caps(None)
        });
    });
    assert_eq!(
        refused(&mut new_id, by_venue()),
        Some(NotSentReason::Unsupported)
    );
    let mut two_phase = rich(Bracket::Middle, |o| {
        o.ack = AckModel::TwoPhase {
            risk_reject_window: Duration::from_millis(1),
        };
    });
    assert_eq!(
        refused(&mut two_phase, by_venue()),
        Some(NotSentReason::Unsupported)
    );
    // Features the venue refuses together.
    let mut conflicted = rich(Bracket::Middle, |o| {
        o.flag_conflicts = vec![(Feature::PostOnly, Feature::ReduceOnly)];
    });
    let mut both = by_venue();
    both.reduce_only = true;
    assert_eq!(
        refused(&mut conflicted, both),
        Some(NotSentReason::FlagConflict)
    );
    // Nothing to rest, an instrument the table does not list, or a client id the venue's
    // format cannot hold.
    let mut nothing = by_venue();
    nothing.cum_filled = lots(2);
    assert_eq!(refused(&mut v, nothing), Some(NotSentReason::Unencodable));
    let mut unlisted = by_venue();
    unlisted.inst = InstrumentId::new(99);
    assert_eq!(refused(&mut v, unlisted), Some(NotSentReason::Unencodable));
    let mut short = rich(Bracket::Middle, |o| {
        o.client_id = ClientIdFormat::Numeric { max_digits: 4 };
    });
    let mut by_client = by_venue();
    by_client.target = OrderRef::Client(cid());
    assert_eq!(
        refused(&mut short, by_client),
        Some(NotSentReason::Unencodable)
    );
}

fn new_order(cmd: VenueCommand) -> NewOrder {
    let VenueCommand::Place(o) = cmd else {
        panic!("{cmd:?}")
    };
    o
}

fn cancel_of(target: OrderRef) -> CancelOrder {
    let VenueCommand::Cancel(c) = cancel(target) else {
        unreachable!()
    };
    c
}

/// The outcome of item `idx`: its client and venue ids and what became of it.
fn item_of(
    ev: &ExecEvent,
) -> (
    u16,
    Option<ClientOrderId>,
    Option<VenueOrderId>,
    SubmitOutcome,
) {
    let ExecEvent::Outcome {
        item: Some(item),
        outcome,
        ..
    } = ev
    else {
        panic!("{ev:?}")
    };
    (item.idx, item.cid, item.vid.clone(), outcome.clone())
}

#[test]
fn a_batch_is_answered_with_one_outcome_per_item_in_one_frame() {
    let mut v = rich(Bracket::Middle, |_| {});
    v.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let (rests, crosses, takes) = (cid(), cid(), cid());
    let ioc = place(
        takes,
        Side::Buy,
        OrderKind::Limit { px: Ticks(201) },
        2,
        TifTag::Ioc,
        false,
    );
    let orders = vec![
        new_order(limit(rests, Side::Buy, 199, 1)),
        new_order(limit(crosses, Side::Buy, 201, 1)),
        new_order(ioc),
    ];
    let batch = VenueCommand::PlaceBatch(orders);
    let fx = v.encode(&batch, 1, T0).unwrap();
    let [Effect::Send { class, charge, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    let weight = core::num::NonZeroU32::new(3).unwrap();
    let three = RateCharge {
        op: OpKind::Place,
        inst: Some(INST),
        weight,
    };
    assert_eq!((*class, *charge), (TrafficClass::Normal, three));
    v.send(batch, 1, T0);
    v.tick(T0 + 5 * MS);
    let frames = v.engine.take_answers();
    // The first frame answers every item, pushed in one call (the one-call contract, 0014).
    let outcomes = v.decode(&frames[0].frame).unwrap();
    assert_eq!(outcomes.len(), 3, "{outcomes:?}");
    assert!(outcomes.iter().all(|(meta, _)| meta.venue_seq == Some(0)));
    let items: Vec<_> = outcomes.iter().map(|(_, ev)| item_of(ev)).collect();
    assert_eq!(
        (items[0].0, items[0].1, &items[0].3),
        (0, Some(rests), &ACCEPTED)
    );
    assert!(items[0].2.is_some());
    assert_eq!((items[1].0, items[1].1, &items[1].2), (1, None, &None));
    assert_eq!(refusal(&outcomes[1].1).0, RejectKind::PostOnlyWouldCross);
    assert_eq!(
        (items[2].0, items[2].1, &items[2].3),
        (2, Some(takes), &ACCEPTED)
    );
    // Then each accepted item's events, in the batch's order.
    let rest: Vec<_> = frames[1..]
        .iter()
        .flat_map(|a| v.decode(&a.frame).unwrap())
        .map(|(_, ev)| ev)
        .collect();
    assert_eq!(state_of(&rest[0]), Some((VenueOrderState::Open, 0)));
    let taker = expected_fee(201, 2, TAKER_BPS);
    assert_eq!(fill_of(&rest[1]), Some((201, 2, Liquidity3::Taker, taker)));
    assert_eq!(state_of(&rest[2]), Some((VenueOrderState::Filled, 2)));
    assert_eq!(rest.len(), 3);

    // A batch spanning instruments names none in its charge.
    let mut lone = new_order(limit(cid(), Side::Buy, 199, 1));
    lone.inst = BOOKLESS;
    let mixed = VenueCommand::PlaceBatch(vec![new_order(limit(cid(), Side::Buy, 199, 1)), lone]);
    let fx = v.encode(&mixed, 2, T0).unwrap();
    let [Effect::Send { charge, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    assert_eq!(charge.inst, None);
}

#[test]
fn the_codec_sends_a_batch_only_as_the_stood_in_venue_takes_it() {
    let one = || new_order(limit(cid(), Side::Buy, 199, 1));
    let batch = |n: usize| VenueCommand::PlaceBatch((0..n).map(|_| one()).collect());
    // No batches, or longer than the venue takes: unsupported. Empty: nothing to write.
    let mut plain = Venue::new(Bracket::Middle);
    assert_eq!(
        plain.encode(&batch(1), 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
    let mut v = rich(Bracket::Middle, |_| {});
    assert_eq!(
        v.encode(&batch(5), 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
    assert_eq!(
        v.encode(&batch(0), 1, T0).err(),
        Some(NotSentReason::Unencodable)
    );
    // An item the venue does not take, or cannot be written, refuses the whole batch.
    let mut rpi = one();
    rpi.channel = Channel::Rpi;
    let cmd = VenueCommand::PlaceBatch(vec![one(), rpi]);
    assert_eq!(
        v.encode(&cmd, 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
    let mut unlisted = one();
    unlisted.inst = InstrumentId::new(99);
    let cmd = VenueCommand::PlaceBatch(vec![one(), unlisted]);
    assert_eq!(
        v.encode(&cmd, 1, T0).err(),
        Some(NotSentReason::Unencodable)
    );
}

#[test]
fn a_cancel_many_ends_exactly_the_orders_it_names() {
    let mut v = rich(Bracket::Middle, |_| {});
    v.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let (one, two, three) = (cid(), cid(), cid());
    let a = v.rest(limit(one, Side::Buy, 199, 1), 1, T0);
    let b = v.rest(limit(two, Side::Buy, 199, 1), 2, T0 + 10 * MS);
    let c = v.rest(limit(three, Side::Buy, 199, 1), 3, T0 + 20 * MS);
    let never = dispatch(&v.caps, NS, |scope| scope.venue_order_id("S99")).unwrap();
    let cancels = VenueCommand::CancelMany(vec![
        cancel_of(OrderRef::Venue(a.clone())),
        cancel_of(OrderRef::Client(two)),
        cancel_of(OrderRef::Venue(never)),
    ]);
    let fx = v.encode(&cancels, 4, T0 + 30 * MS).unwrap();
    let [Effect::Send { class, charge, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    let weight = core::num::NonZeroU32::new(3).unwrap();
    let three_cancels = RateCharge {
        op: OpKind::Cancel,
        inst: Some(INST),
        weight,
    };
    assert_eq!((*class, *charge), (TrafficClass::Safety, three_cancels));
    v.send(cancels, 4, T0 + 30 * MS);
    v.tick(T0 + 35 * MS);
    let got = v.events();
    let items: Vec<_> = got[..3].iter().map(|(_, ev)| item_of(ev)).collect();
    assert_eq!(
        items[..2],
        [
            (0, Some(one), Some(a.clone()), ACCEPTED),
            (1, Some(two), Some(b.clone()), ACCEPTED)
        ]
    );
    assert_eq!(refusal(&got[2].1).0, RejectKind::NotFound);
    let ended: Vec<_> = got[3..]
        .iter()
        .map(|(_, ev)| order_of(ev).vid.clone())
        .collect();
    assert_eq!(ended, [Some(a.clone()), Some(b.clone())]);
    assert!(got[3..].iter().all(
        |(_, ev)| state_of(ev).unwrap().0 == VenueOrderState::Canceled(CancelReason::Requested)
    ));
    // The order it did not name still rests; one it ended is refused as cancelled.
    assert!(v.engine.queue_position(&c).is_some());
    assert_eq!(v.engine.queue_position(&a), None);
    let again = VenueCommand::CancelMany(vec![cancel_of(OrderRef::Venue(a))]);
    v.send(again, 5, T0 + 40 * MS);
    v.tick(T0 + 45 * MS);
    let refused = (
        RejectKind::AlreadyTerminal(TerminalHint::Canceled),
        "canceled".to_owned(),
    );
    assert_eq!(refusal(&v.events()[0].1), refused);
}

#[test]
fn the_codec_sends_a_cancel_many_only_as_the_stood_in_venue_takes_it() {
    let by_client = || cancel_of(OrderRef::Client(cid()));
    let many = |n: usize| VenueCommand::CancelMany((0..n).map(|_| by_client()).collect());
    let mut plain = Venue::new(Bracket::Middle);
    assert_eq!(
        plain.encode(&many(1), 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
    let mut v = rich(Bracket::Middle, |_| {});
    assert_eq!(
        v.encode(&many(5), 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
    assert_eq!(
        v.encode(&many(0), 1, T0).err(),
        Some(NotSentReason::Unencodable)
    );
    // A batch cancel's items may name orders more narrowly than a single cancel.
    let mut narrow = rich(Bracket::Middle, |o| {
        o.batch_cancel = Some(CancelBatch {
            max_items: 4,
            refs: TagSet::of(&[RefKind::Venue]),
        });
    });
    assert_eq!(
        narrow.encode(&many(1), 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
    let single = cancel(OrderRef::Client(cid()));
    assert!(narrow.encode(&single, 1, T0).is_ok());
}

#[test]
fn an_instrument_cancel_all_ends_exactly_the_orders_it_covers() {
    const OTHER: InstrumentId = InstrumentId::new(3);
    const OTHER_BOOK: BookId = BookId(1);
    let mut config = rich_config(Bracket::Middle);
    config.specs.insert(spec(OTHER, "TWO-PERP"));
    config.books.insert(OTHER, OTHER_BOOK);
    let mut v = Venue::with(config);
    v.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let begin = MdEvent::BookSnapshotBegin {
        inst: OTHER,
        book: OTHER_BOOK,
        epoch: 1,
    };
    v.md(T0, begin).unwrap();
    let bid = MdEvent::Level {
        inst: OTHER,
        book: OTHER_BOOK,
        side: BookSide::Bid,
        px: Ticks(99),
        qty: lots(3),
    };
    v.md(T0, bid).unwrap();
    let end = MdEvent::BookSnapshotEnd {
        inst: OTHER,
        book: OTHER_BOOK,
    };
    v.md(T0, end).unwrap();
    let a = v.rest(limit(cid(), Side::Buy, 199, 1), 1, T0);
    let b = v.rest(limit(cid(), Side::Sell, 201, 1), 2, T0 + 10 * MS);
    let mut elsewhere = new_order(limit(cid(), Side::Buy, 99, 1));
    elsewhere.inst = OTHER;
    let c = v.rest(VenueCommand::Place(elsewhere), 3, T0 + 20 * MS);
    let injected = InjectedOrder {
        inst: INST,
        side: Side::Buy,
        px: Ticks(199),
        qty: lots(2),
    };
    let key = v.engine.inject(MonoNs(T0 + 30 * MS), injected).unwrap();

    let all = VenueCommand::CancelAll(CancelScope::Instrument(INST));
    let fx = v.encode(&all, 4, T0 + 40 * MS).unwrap();
    let [Effect::Send { class, charge, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    let one = RateCharge::one(OpKind::CancelAll, Some(INST));
    assert_eq!((*class, *charge), (TrafficClass::Safety, one));
    v.send(all, 4, T0 + 40 * MS);
    v.tick(T0 + 45 * MS);
    let got = v.events();
    // Accepted as a whole, then each order it covers ends.
    let done = ExecEvent::Outcome {
        rpc: RpcId(4),
        item: None,
        outcome: ACCEPTED,
    };
    assert_eq!(got[0].1, done);
    let ended: Vec<_> = got[1..]
        .iter()
        .map(|(_, ev)| order_of(ev).vid.clone())
        .collect();
    assert_eq!(ended, [Some(a.clone()), Some(b.clone())]);
    for (_, ev) in &got[1..] {
        let canceled = VenueOrderState::Canceled(CancelReason::Requested);
        assert_eq!(state_of(ev).unwrap().0, canceled);
    }
    // The other instrument's order, and the injected one, another process's, still rest.
    assert!(v.engine.queue_position(&c).is_some());
    assert!(v.engine.injected(key).is_some());
    assert_eq!(v.engine.queue_position(&a), None);
}

#[test]
fn the_codec_never_widens_a_cancel_all_to_the_account() {
    let account = VenueCommand::CancelAll(CancelScope::Account);
    let instrument = VenueCommand::CancelAll(CancelScope::Instrument(INST));
    // Never account-wide, even for a venue that offers it.
    let mut v = rich(Bracket::Middle, |o| o.cancel_all_account = Support::Native);
    assert_eq!(
        v.encode(&account, 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
    assert!(v.encode(&instrument, 1, T0).is_ok());
    // And by instrument only where the venue offers it.
    let mut plain = Venue::new(Bracket::Middle);
    assert_eq!(
        plain.encode(&instrument, 1, T0).err(),
        Some(NotSentReason::Unsupported)
    );
}

#[test]
fn a_query_is_answered_with_its_rpc_and_the_order_as_the_venue_holds_it() {
    let mut v = rich(Bracket::Middle, |_| {});
    v.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let first = cid();
    let a = v.rest(limit(first, Side::Buy, 199, 2), 1, T0);
    let query = |target: OrderRef| {
        VenueCommand::Query(QueryOrder {
            target,
            inst: INST,
            placement_nonce: None,
        })
    };
    let answer = |v: &mut Venue, cmd: VenueCommand, rpc: u64, at: u64| {
        v.send(cmd, rpc, at);
        v.tick(at + 5 * MS);
        let got = v.events();
        assert_eq!(got.len(), 1, "{got:?}");
        let ExecEvent::QueryResult(answer) = &got[0].1 else {
            panic!("{got:?}")
        };
        answer.clone()
    };
    let both = OrderRef::Both(first, a.clone());
    let fx = v.encode(&query(both.clone()), 7, T0).unwrap();
    let [Effect::Send { class, charge, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    let one = RateCharge::one(OpKind::Query, Some(INST));
    assert_eq!((*class, *charge), (TrafficClass::Safety, one));
    // Resting: open, as placed, its query named as asked.
    let got = answer(&mut v, query(both.clone()), 7, T0 + 10 * MS);
    assert_eq!((got.rpc(), got.target()), (RpcId(7), &both));
    let found = got.found().unwrap();
    assert_eq!(
        (
            found.cid,
            &found.vid,
            found.state.clone(),
            found.px,
            found.qty,
            found.cum_filled
        ),
        (
            Some(CidMatch::Ours(first)),
            &a,
            VenueOrderState::Open,
            Some(Ticks(199)),
            lots(2),
            lots(0)
        )
    );
    assert_eq!(
        (found.post_only, found.reduce_only),
        (Some(true), Some(false))
    );
    // Ended: as it ended.
    v.send(cancel(OrderRef::Venue(a.clone())), 8, T0 + 20 * MS);
    v.tick(T0 + 25 * MS);
    v.events();
    let got = answer(&mut v, query(OrderRef::Client(first)), 9, T0 + 30 * MS);
    let canceled = VenueOrderState::Canceled(CancelReason::Requested);
    assert_eq!(got.found().unwrap().state, canceled);
    // A market order that filled reports no price.
    let market = cid();
    let cmd = place(market, Side::Buy, OrderKind::Market, 1, TifTag::Ioc, false);
    v.send(cmd, 10, T0 + 40 * MS);
    v.tick(T0 + 45 * MS);
    v.events();
    let got = answer(&mut v, query(OrderRef::Client(market)), 11, T0 + 50 * MS);
    let found = got.found().unwrap();
    assert_eq!(
        (found.state.clone(), found.px),
        (VenueOrderState::Filled, None)
    );
    // An order the venue never had: none.
    let never = dispatch(&v.caps, NS, |scope| scope.venue_order_id("S99")).unwrap();
    let got = answer(
        &mut v,
        query(OrderRef::Venue(never.clone())),
        12,
        T0 + 60 * MS,
    );
    assert_eq!(
        (got.rpc(), got.target(), got.found()),
        (RpcId(12), &OrderRef::Venue(never), None)
    );

    // A venue that echoes neither client ids nor flags reports neither.
    let mut quiet = rich(Bracket::Middle, |o| {
        o.cid_echoed_on_events = false;
        o.events_echo_flags = false;
    });
    quiet.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let first = cid();
    quiet.rest(limit(first, Side::Buy, 199, 2), 1, T0);
    let got = answer(&mut quiet, query(OrderRef::Client(first)), 2, T0 + 10 * MS);
    let found = got.found().unwrap();
    assert_eq!(
        (found.cid, found.post_only, found.reduce_only),
        (None, None, None)
    );
    // A venue that takes no queries, or none by the reference the query has.
    let mut plain = Venue::new(Bracket::Middle);
    let refused = plain.encode(&query(OrderRef::Client(cid())), 1, T0);
    assert_eq!(refused.err(), Some(NotSentReason::Unsupported));
    let mut short = rich(Bracket::Middle, |o| {
        o.client_id = ClientIdFormat::Numeric { max_digits: 4 };
    });
    let vid = dispatch(&v.caps, NS, |scope| scope.venue_order_id("S1")).unwrap();
    let refused = short.encode(&query(OrderRef::Both(cid(), vid)), 1, T0);
    assert_eq!(refused.err(), Some(NotSentReason::Unencodable));
}

fn injected(px: i64, qty: i64) -> InjectedOrder {
    InjectedOrder {
        inst: INST,
        side: Side::Buy,
        px: Ticks(px),
        qty: lots(qty),
    }
}

#[test]
fn an_injected_order_is_excluded_from_a_later_orders_queue_ahead_and_never_reported_as_ours() {
    let mut v = Venue::new(Bracket::Pessimistic);
    v.snapshot(T0, &[(199, 10)], &[(201, 5)]);
    // Another process's order of four lots arrives behind the ten shown, and nothing is
    // answered for it.
    let key = v.engine.inject(MonoNs(T0 + MS), injected(199, 4)).unwrap();
    assert_eq!(v.engine.injected(key).unwrap().ahead, lots(10));
    assert!(v.engine.take_answers().is_empty());
    // The book shows it (fourteen lots), and an order of ours queues behind the ten public
    // lots only: the injected one is modelled, so it is not size ahead.
    v.level(T0 + 2 * MS, BookSide::Bid, 199, 14);
    let ours = v.rest(limit(cid(), Side::Buy, 199, 2), 1, T0 + 10 * MS);
    assert_eq!(v.ahead(&ours), 10);
    // Twelve lots sold at the price take the ten ahead, then two of the injected order, which
    // arrived first. Its fill is never answered on the order-entry stream.
    v.trade(T0 + 20 * MS, Aggressor::Seller, 199, 12);
    assert!(v.events().is_empty());
    let two = |remaining| SimFill {
        key,
        side: Side::Buy,
        px: Ticks(199),
        qty: lots(2),
        remaining: lots(remaining),
    };
    assert_eq!(v.engine.take_injected_fills(), [two(2)]);
    // Four more: the injected order's last two, then two of ours, the only fill answered.
    v.trade(T0 + 21 * MS, Aggressor::Seller, 199, 4);
    let got = v.events();
    assert_eq!(filled(&got), std::slice::from_ref(&ours));
    assert_eq!(fill_of(&got[0].1).unwrap().1, 2);
    assert_eq!(v.engine.take_injected_fills(), [two(0)]);
    assert_eq!(v.engine.injected(key), None);
    // No command of the codec's can name it: its venue number is no order of ours.
    let number = format!("S{}", key.0);
    let named = dispatch(&v.caps, NS, |scope| scope.venue_order_id(&number)).unwrap();
    assert_eq!(v.engine.queue_position(&named), None);
    v.send(cancel(OrderRef::Venue(named)), 2, T0 + 30 * MS);
    v.tick(T0 + 35 * MS);
    assert_eq!(rejected(&v.events()[0].1), Some(RejectKind::NotFound));
}

#[test]
fn a_withdrawn_injected_order_leaves_its_queue_and_its_shrink_moves_no_other_order() {
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 10)], &[(201, 5)]);
    let key = v.engine.inject(MonoNs(T0 + MS), injected(199, 4)).unwrap();
    v.level(T0 + 2 * MS, BookSide::Bid, 199, 14);
    let ours = v.rest(limit(cid(), Side::Buy, 199, 2), 1, T0 + 10 * MS);
    v.engine.withdraw(MonoNs(T0 + 20 * MS), key).unwrap();
    assert_eq!(v.engine.injected(key), None);
    // The book shows it gone: it was never ahead of ours, so ours does not move, even under
    // the optimistic bracket.
    v.level(T0 + 21 * MS, BookSide::Bid, 199, 10);
    assert_eq!(v.ahead(&ours), 10);
    // A second withdrawal, or one of a key never injected, is refused.
    for k in [key, OrderKey(77)] {
        let refused = v.engine.withdraw(MonoNs(T0 + 22 * MS), k);
        assert_eq!(refused, Err(SimError::NotInjected(k)));
    }
    assert_eq!(
        SimError::NotInjected(OrderKey(77)).to_string(),
        "order 77 is not injected"
    );
}

#[test]
fn a_replacement_snapshot_advances_an_injected_order_alone_at_its_level() {
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 10), (198, 5)], &[(201, 5)]);
    let key = v.engine.inject(MonoNs(T0 + MS), injected(199, 1)).unwrap();
    v.snapshot(T0 + 2 * MS, &[(199, 7), (198, 5)], &[(201, 5)]);
    assert_eq!(v.engine.injected(key).unwrap().ahead, lots(7));
}

#[test]
fn the_engine_refuses_an_injected_order_it_cannot_model() {
    let mut v = Venue::new(Bracket::Middle);
    // No book yet, or an instrument with no trading book.
    let at = MonoNs(T0);
    assert_eq!(
        v.engine.inject(at, injected(199, 1)),
        Err(SimError::NoBook(INST))
    );
    let lone = InjectedOrder {
        inst: BOOKLESS,
        ..injected(199, 1)
    };
    assert_eq!(v.engine.inject(at, lone), Err(SimError::NoBook(BOOKLESS)));
    assert_eq!(
        SimError::NoBook(BOOKLESS).to_string(),
        "no simulated trading book for instrument 2"
    );
    // A book that cannot be read: a snapshot in progress.
    let begin = MdEvent::BookSnapshotBegin {
        inst: INST,
        book: BOOK,
        epoch: 1,
    };
    v.md(T0, begin).unwrap();
    let unread = v.engine.inject(at, injected(199, 1));
    assert!(
        matches!(unread, Err(SimError::Queue(QueueError::Book(_)))),
        "{unread:?}"
    );
    // A price past the deepest level shown, and no size.
    v.snapshot(T0, &[(199, i64::MAX - 1)], &[(201, 5)]);
    let past = v.engine.inject(at, injected(150, 1));
    let unknown = QueueError::UnknownLevel {
        side: BookSide::Bid,
        px: Ticks(150),
    };
    assert_eq!(past, Err(SimError::Queue(unknown)));
    let none = v.engine.inject(at, injected(199, 0));
    assert!(matches!(
        none,
        Err(SimError::Queue(QueueError::ZeroQuantity(_)))
    ));
    assert!(
        none.unwrap_err()
            .to_string()
            .starts_with("injected order: ")
    );
    // Lots past an i64 at the level: the size shown and our order there.
    v.rest(limit(cid(), Side::Buy, 199, 1), 1, T0);
    v.level(T0 + 10 * MS, BookSide::Bid, 199, i64::MAX);
    let over = v.engine.inject(MonoNs(T0 + 10 * MS), injected(199, 1));
    let overflow = QueueError::Overflow {
        side: BookSide::Bid,
        px: Ticks(199),
    };
    assert_eq!(over, Err(SimError::Queue(overflow)));
}

#[test]
fn the_engine_refuses_request_frames_it_cannot_read() {
    let mut v = rich(Bracket::Middle, |_| {});
    for (frame, what) in [
        (
            "place|rpc=1|mono=1|wall=1|cid=c|inst=1|side=B|qty=1|tif=gtc|po=0|ro=0\nitem|x=1",
            "item",
        ),
        ("batch|rpc=1|mono=1|wall=1\nplace|cid=c", "item"),
        ("batch|rpc=1|mono=1|wall=1\nitem|cid=c", "inst"),
        ("cancels|rpc=1|mono=1|wall=1\nitem|x=1", "target"),
        ("cancelall|rpc=1|mono=1|wall=1", "inst"),
        ("query|rpc=1|mono=1|wall=1", "target"),
        ("amend|rpc=1|mono=1|wall=1|vid=S0|inst=1|side=B|qty=1", "px"),
        ("amend|rpc=1|mono=1|wall=1", "target"),
    ] {
        let refused = v.engine.on_order_frame(frame.as_bytes());
        assert_eq!(refused, Err(SimError::Malformed(what)), "{frame}");
    }
    // An escaped newline is a value's, not a line break: a cancel of "a\nb", which no order
    // has.
    let frame = format!("cancel|rpc=1|mono={T0}|wall=0|cid=a%0Ab");
    v.engine.on_order_frame(frame.as_bytes()).unwrap();
    v.tick(T0 + 5 * MS);
    assert_eq!(rejected(&v.events()[0].1), Some(RejectKind::NotFound));
}

#[test]
fn the_codec_refuses_request_answers_it_cannot_read() {
    let mut v = rich(Bracket::Middle, |_| {});
    let order = "order|cid=c|vid=S2|inst=1|side=B|state=open|cum=0|qty=1|po=0|ro=0";
    let other = format!("query|seq=1|rpc=1|qvid=S1\n{order}");
    let twice = format!("query|seq=1|rpc=1|qvid=S2\n{order}\n{order}");
    let many = format!(
        "items|seq=1|rpc=1{}",
        "\nack|cid=c|vid=S1".repeat(usize::from(u16::MAX) + 2)
    );
    for (frame, what) in [
        ("ack|seq=1|rpc=1|cid=c|vid=S1\norder|x=1", "item"),
        ("items|seq=1|rpc=1\nhello", "item"),
        ("items|seq=1|rpc=1\nreject|code=nope", "code"),
        ("items|seq=1|rpc=1\nack|cid=c", "vid"),
        ("items|seq=1\nack|cid=c|vid=S1", "rpc"),
        ("done|seq=1", "rpc"),
        ("query|seq=1|qvid=S1", "rpc"),
        ("query|seq=1|rpc=1|qvid=S1\nhello", "item"),
        ("query|seq=1|rpc=1|qvid=S1\norder|cid=c", "vid"),
        (twice.as_str(), "item"),
        ("query|seq=1|rpc=1", "query target"),
        ("query|seq=1|rpc=1|qcid=c", "query cid"),
        (other.as_str(), "query answer names another order"),
        (many.as_str(), "items"),
    ] {
        assert_eq!(
            v.decode(frame.as_bytes()),
            Err(DecodeError::Malformed(what)),
            "{}",
            &frame[..frame.len().min(80)]
        );
    }
    for frame in [
        "items|seq=1|rpc=1\nack|cid=c|vid=",
        "query|seq=1|rpc=1|qvid=",
        "query|seq=1|rpc=1|qvid=S2\norder|cid=c|vid=|inst=1|side=B|state=open|cum=0|qty=1|po=0|ro=0",
    ] {
        let refused = v.decode(frame.as_bytes());
        assert!(matches!(refused, Err(DecodeError::IdRefused(_))), "{frame}");
    }
    // A batch of no items answers nothing; a query answer naming the order queried is read.
    assert_eq!(v.decode(b"items|seq=1|rpc=1").unwrap(), []);
    let same = format!("query|seq=1|rpc=1|qvid=S2\n{order}");
    let got = v.decode(same.as_bytes()).unwrap();
    assert!(matches!(&got[0].1, ExecEvent::QueryResult(a) if a.found().is_some()));
}

#[test]
fn an_injected_order_the_book_does_not_show_yet_is_not_counted_in_its_level() {
    // Codex r4186502479: two orders injected before the book shows either: the second queues
    // behind the ten public lots, not behind ten less the first.
    let mut v = Venue::new(Bracket::Optimistic);
    v.snapshot(T0, &[(199, 10)], &[(201, 5)]);
    let first = v.engine.inject(MonoNs(T0 + MS), injected(199, 4)).unwrap();
    let second = v.engine.inject(MonoNs(T0 + MS), injected(199, 3)).unwrap();
    assert_eq!(v.engine.injected(second).unwrap().ahead, lots(10));
    // The book shows the first (ten to fourteen); an order of ours then queues behind the ten
    // public lots: the first is shown and modelled, the second modelled and not shown yet.
    v.level(T0 + 2 * MS, BookSide::Bid, 199, 14);
    let ours = v.rest(limit(cid(), Side::Buy, 199, 2), 1, T0 + 10 * MS);
    assert_eq!(v.ahead(&ours), 10);
    // The second is withdrawn before the book showed it: nothing of it will leave the book, so
    // a real shrink of two public lots that follows advances ours (optimistic: all of it).
    v.engine.withdraw(MonoNs(T0 + 20 * MS), second).unwrap();
    v.level(T0 + 21 * MS, BookSide::Bid, 199, 12);
    assert_eq!(v.ahead(&ours), 8);
    // The first, shown, withdrawn: the book's shrink of its four moves ours no further.
    v.engine.withdraw(MonoNs(T0 + 30 * MS), first).unwrap();
    v.level(T0 + 31 * MS, BookSide::Bid, 199, 8);
    assert_eq!(v.ahead(&ours), 8);
}

#[test]
fn an_injected_order_filled_before_the_book_shows_it_is_shown_net_of_its_fill() {
    let mut v = Venue::new(Bracket::Pessimistic);
    v.snapshot(T0, &[(199, 2), (198, 5)], &[(201, 5)]);
    let key = v.engine.inject(MonoNs(T0 + MS), injected(199, 4)).unwrap();
    // Three sold: the two public lots, then one of the injected order, not shown yet.
    v.trade(T0 + 2 * MS, Aggressor::Seller, 199, 3);
    assert_eq!(v.engine.take_injected_fills().len(), 1);
    // The book then shows the public lots gone and the injected order's three left; an order
    // of ours queues behind none of it: the three are modelled.
    v.level(T0 + 3 * MS, BookSide::Bid, 199, 0);
    v.level(T0 + 4 * MS, BookSide::Bid, 199, 3);
    let ours = v.rest(limit(cid(), Side::Buy, 199, 1), 1, T0 + 10 * MS);
    assert_eq!(v.ahead(&ours), 0);
    assert_eq!(v.engine.injected(key).unwrap().remaining, lots(3));
}

#[test]
fn an_injected_order_its_level_could_not_hold_is_refused() {
    // Codex r4186502501: the level's size, the simulated orders there and the injected order
    // must fit in lots together.
    let mut v = Venue::new(Bracket::Middle);
    v.snapshot(T0, &[(199, i64::MAX)], &[(201, 5)]);
    let over = v.engine.inject(MonoNs(T0), injected(199, 1));
    let overflow = QueueError::Overflow {
        side: BookSide::Bid,
        px: Ticks(199),
    };
    assert_eq!(over, Err(SimError::Queue(overflow)));
}

#[test]
fn a_priority_keeping_amend_that_grows_where_its_level_is_unknown_is_refused() {
    // Codex r4186502494: a book that shows no bid knows no bid level's size, so a larger
    // order cannot be shown to fit: refused, and the order keeps its size. A smaller one needs
    // no check.
    let mut v = rich(Bracket::Middle, |o| o.amend = Some(amend_caps(Some(true))));
    v.snapshot(T0, &[(199, 4)], &[(201, 5)]);
    let a = v.rest(limit(cid(), Side::Buy, 199, 2), 1, T0);
    v.level(T0 + 10 * MS, BookSide::Bid, 199, 0);
    for (rpc, qty) in [(2, 3), (3, 1)] {
        let cmd = amend(OrderRef::Venue(a.clone()), Side::Buy, 199, qty, 0);
        v.send(VenueCommand::Amend(cmd), rpc, T0 + 10 * MS);
    }
    v.engine.advance(MonoNs(T0 + 15 * MS));
    let got = v.events();
    let refused = (RejectKind::Other, "no_book".to_owned());
    assert_eq!(refusal(&got[0].1), refused);
    assert_eq!(outcome_of(&got[1].1), Some((3, ACCEPTED)));
    assert_eq!(v.engine.queue_position(&a).unwrap().remaining, lots(1));
}
