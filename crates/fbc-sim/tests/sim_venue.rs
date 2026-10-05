//! FBC-uoo's done line: SimVenue driven through its codec and engine only, with no runtime and
//! no socket (decision 0046). A placed order is answered Accepted after the configured latency
//! and filled through the queue model by later trades; a post-only order that would cross is
//! rejected; a cancel ends the order; every fill carries a fee from the fee book, read back
//! through the decode scope; and two runs over the same envelopes and frames give identical
//! answers. Error paths are here too. The venue, its symbols and its rates are synthetic.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use fbc_core::{
    AccountKey, AckLevel, AckModel, Aggressor, AmendOrder, AssetSym, BookId, BookSide, Bps,
    CancelOnDisconnect, CancelOrder, CancelReason, Channel, CidMatch, CidMint, ClientIdFormat,
    ClientOrderId, ConnKey, ConnTopology, CtxCall, DecodeError, Effect, Effects, EncodeCtx,
    Encoding, Envelope, ExecCaps, ExecCodec, ExecEvent, ExecSink, Feature, FeeBook, FeeEntry,
    FeeKey, FeeRate, FeeSource, Feed, FeedHealth, FeedSource, FillCaps, FillIdent, FillSource,
    FundingCaps, FundingSpec, HttpFailure, HttpTag, Inbound, InboundSpans, InstrumentId,
    InstrumentKind, InstrumentSpec, ItemRef, Liquidity, Liquidity3, Lots, MatchingCaps, MdCaps,
    MdEvent, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock, NonceScope, NotSentReason,
    OpKind, OrderCaps, OrderKind, OrderKindTag, OrderRef, OrderingKey, PathStamps, PriceGrid,
    RateCharge, RawFrame, Readiness, RefKind, RejectKind, RpcCall, RpcId, Side, SizeStep,
    SnapshotSource, SpecTable, SpeedBump, SpeedBumpScope, Stamp, StpScope, StreamId, SubmitOutcome,
    Support, TagSet, TerminalHint, Ticks, Tif, TifTag, TimerTag, TradeCaps, TradingStatus,
    TrafficClass, UnderlyingId, VenueCaps, VenueCommand, VenueFeeSign, VenueId, VenueMeta,
    VenueOrderState, WallNs, dispatch,
};
use fbc_sim::{Answer, Bracket, QueueConfig, SimCodec, SimConfig, SimEngine, SimError, SimLatency};
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
        ("amend|rpc=1|mono=1|wall=1", "kind"),
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
