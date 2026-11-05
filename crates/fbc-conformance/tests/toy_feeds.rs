//! FBC-z2s's done line, sans IO: the conformance toy's socket codec ([`ToyMd`]) decodes trades,
//! funding, mark and index into their `MdEvent`s; each of its codecs refuses a feed its
//! transport does not carry, a `Poll` feed on the socket codec and a `Stream` feed on the poll
//! codec ([`ToyStats`]), with nothing sent (Codex r4172917344, r4172649862), and the factory
//! plans each feed on the endpoint its source names (an `MdTransport::Poll` endpoint for stats,
//! r4172456290, r4172752101, record 0014 item 9); the poll codec polls stats over HTTP under its
//! own timer until the last stats subscription is removed (r4172917328); and it refuses negative
//! open interest or volume with nothing pushed (r4172917318). The poll endpoint run by
//! fbc-runtime's session against the stub server is `tests/toy_stats_poll.rs`.

use std::collections::BTreeSet;

use fbc_conformance::toy::{
    self, ANCHORED_BOOK, BOOK, INST_A, INST_B, MD_STREAM, MD_URL_KEY, STATS_EVERY, STATS_STREAM,
    STATS_TIMEOUT, STATS_URL_KEY, ToyFactory, ToyMd, ToyStats, feed_source,
};
use fbc_core::{
    Aggressor, AssetSym, BookId, ConfigError, DecodeError, Effect, Effects, EndpointPlan, ExchNs,
    ExchTsKind, Feed, FeedSource, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag,
    Inbound, InboundSpans, InstrumentId, Lots, MdCodec, MdEvent, MdSink, MdTransport, Money,
    MonoNs, OpKind, PxExact, RateCharge, RawFrame, StreamId, Subscription, Ticks, TimerTag,
    TouchSourceId, TradeId, TrafficClass, VenueConfig, VenueError, VenueFactory, VenueMeta, WallNs,
    WireSlice, WireUrl, dispatch_market_data,
};

use DecodeError::Malformed;

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const STREAM: StreamId = StreamId(4);
const POLL: StreamId = StreamId(6);
const BASE: &str = "http://127.0.0.1:9/md";

/// A sink that keeps every event.
#[derive(Default)]
struct Keep(Vec<(VenueMeta, MdEvent)>);

impl MdSink for Keep {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push((meta, ev));
    }
}

fn on(inst: InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

fn stats(inst: InstrumentId) -> Subscription {
    on(inst, Feed::Stats)
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn usdc(nanos: i128) -> Money {
    Money::new(nanos, AssetSym::new("USDC").unwrap())
}

/// What the venue said about an event: its time when it sent one, its sequence when it did.
fn meta(ts: Option<i64>, seq: Option<u64>) -> VenueMeta {
    VenueMeta {
        exch_ts: ts.map(ExchNs),
        exch_ts_kind: match ts {
            Some(_) => ExchTsKind::MatchingEngine,
            None => ExchTsKind::Unknown,
        },
        venue_seq: seq,
    }
}

/// The venue's numeric trade id `n`, as the decode scope builds it.
fn trade_id(n: u64) -> TradeId {
    dispatch_market_data(&toy::caps(), |scope| scope.trade_id_numeric(n))
}

/// A codec of the toy's, a sink and an effect buffer, driven with the toy's specs.
struct Rig<C> {
    md: C,
    sink: Keep,
    fx: Effects,
}

impl<C: MdCodec> Rig<C> {
    fn new(md: C) -> Rig<C> {
        Rig {
            md,
            sink: Keep::default(),
            fx: Effects::new(),
        }
    }

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
    ) -> Result<(), VenueError> {
        self.md.subscribe(add, remove, &toy::specs(), &mut self.fx)
    }

    fn frame(&mut self, text: &str) -> Result<(), DecodeError> {
        let (md, sink, fx) = (&mut self.md, &mut self.sink, &mut self.fx);
        dispatch_market_data(&toy::caps(), |scope| {
            md.on_frame(RawFrame::Text(text), scope, &toy::specs(), sink, fx)
        })
    }

    fn http(
        &mut self,
        tag: u64,
        resp: Result<(u16, &[u8]), HttpFailure>,
    ) -> Result<(), DecodeError> {
        let (md, sink, fx) = (&mut self.md, &mut self.sink, &mut self.fx);
        let resp = resp.map(|(status, body)| HttpResponse {
            status,
            headers: &[],
            body,
        });
        dispatch_market_data(&toy::caps(), |scope| {
            md.on_http(HttpTag(tag), resp, scope, &toy::specs(), sink, fx)
        })
    }

    /// The stats `body` answering request `tag` with 200.
    fn answer(&mut self, tag: u64, body: &str) -> Result<(), DecodeError> {
        self.http(tag, Ok((200, body.as_bytes())))
    }

    fn timer(&mut self, tag: u64) {
        let (sink, fx) = (&mut self.sink, &mut self.fx);
        self.md
            .on_timer(TimerTag(tag), MonoNs(1), WallNs(1), sink, fx);
    }

    /// The events pushed since the last call.
    fn pushed(&mut self) -> Vec<(VenueMeta, MdEvent)> {
        std::mem::take(&mut self.sink.0)
    }
}

/// The subscribe frame the socket codec writes for `text`, charged to `inst` when it names one
/// instrument only.
fn sub_frame(text: &str, inst: Option<InstrumentId>) -> Effect {
    Effect::Send {
        stream: STREAM,
        frame: WireSlice::plain(text.as_bytes().to_vec()),
        rpc: None,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Subscribe, inst),
    }
}

/// The poll codec's request for `sym`'s stats under `tag`.
fn get(tag: u64, inst: InstrumentId, sym: &str) -> Effect {
    Effect::Http {
        tag: HttpTag(tag),
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(format!("{BASE}/stats/{sym}")),
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout: STATS_TIMEOUT,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Rest, Some(inst)),
    }
}

fn get_a(tag: u64) -> Effect {
    get(tag, INST_A, "TOYA-PERP")
}

fn get_b(tag: u64) -> Effect {
    get(tag, INST_B, "TOYB-PERP")
}

/// The poll codec's timer under `tag`.
fn every(tag: u64) -> Effect {
    Effect::Timer {
        tag: TimerTag(tag),
        after: STATS_EVERY,
    }
}

fn stats_ev(inst: InstrumentId, vol: Option<i128>, oi: Option<i64>) -> MdEvent {
    MdEvent::Stats {
        inst,
        volume_24h_quote: vol.map(usdc),
        oi: oi.map(lots),
    }
}

const STREAMED: [Feed; 4] = [Feed::Trades, Feed::Mark, Feed::Index, Feed::Funding];

// ---------------------------------------------------------------------------------------------
// The socket codec's feeds.
// ---------------------------------------------------------------------------------------------

#[test]
fn trades_funding_mark_and_index_decode_into_their_md_events() {
    let caps = toy::caps().md;
    assert_eq!(caps.trades.source, FeedSource::Stream);
    assert!(caps.trades.aggressor && caps.trades.trade_id);
    assert_eq!(caps.funding.source, FeedSource::Stream);
    assert!(caps.funding.interval_reported && caps.funding.next_time_reported);
    assert_eq!(
        (caps.mark, caps.index),
        (FeedSource::Stream, FeedSource::Stream)
    );

    let mut rig = Rig::new(ToyMd::new(STREAM));
    let subs = STREAMED.map(|feed| on(INST_A, feed));
    rig.subscribe(&subs, &[]).unwrap();
    let text = "sub|add=TOYA-PERP@trades,TOYA-PERP@mark,TOYA-PERP@index,TOYA-PERP@funding";
    assert_eq!(rig.fx.take(), [sub_frame(text, Some(INST_A))]);

    rig.frame("trade|sym=TOYA-PERP|id=42|side=B|px=201|qty=3|ts=1000|seq=7")
        .unwrap();
    rig.frame("trade|sym=TOYA-PERP|id=43|side=S|px=200|qty=1")
        .unwrap();
    rig.frame("mark|sym=TOYA-PERP|px=100.25|ts=1001").unwrap();
    rig.frame("index|sym=TOYA-PERP|px=100.1|seq=8").unwrap();
    rig.frame("funding|sym=TOYA-PERP|rate=-125000|int=3600|next=1759363200000000000|ts=1002")
        .unwrap();
    let (a, trade) = (INST_A, |id, aggressor, px, qty| MdEvent::Trade {
        inst: INST_A,
        id: Some(trade_id(id)),
        aggressor,
        px: Ticks(px),
        qty: lots(qty),
    });
    assert_eq!(
        rig.pushed(),
        [
            (
                meta(Some(1000), Some(7)),
                trade(42, Aggressor::Buyer, 201, 3)
            ),
            (meta(None, None), trade(43, Aggressor::Seller, 200, 1)),
            (
                meta(Some(1001), None),
                MdEvent::Mark {
                    inst: a,
                    px: PxExact::new(10025, -2),
                },
            ),
            (
                meta(None, Some(8)),
                MdEvent::Index {
                    inst: a,
                    px: PxExact::new(1001, -1),
                },
            ),
            (
                meta(Some(1002), None),
                MdEvent::Funding {
                    inst: a,
                    rate_e12: -125_000,
                    interval: Some(core::time::Duration::from_secs(3600)),
                    next: Some(WallNs(1_759_363_200_000_000_000)),
                },
            ),
        ]
    );
    assert!(rig.fx.is_empty());

    // A frame for a feed not subscribed is dropped: another instrument's, or one removed.
    for frame in [
        "trade|sym=TOYB-PERP|id=1|side=B|px=1|qty=1",
        "mark|sym=TOYB-PERP|px=1",
        "index|sym=TOYB-PERP|px=1",
        "funding|sym=TOYB-PERP|rate=1|int=3600|next=1",
    ] {
        assert_eq!(rig.frame(frame), Ok(()), "{frame}");
    }
    assert!(rig.pushed().is_empty());
    rig.subscribe(&[on(INST_B, Feed::Mark)], &[on(INST_A, Feed::Trades)])
        .unwrap();
    let text = "sub|add=TOYB-PERP@mark|remove=TOYA-PERP@trades";
    assert_eq!(rig.fx.take(), [sub_frame(text, None)]);
    rig.frame("trade|sym=TOYA-PERP|id=44|side=B|px=201|qty=1")
        .unwrap();
    rig.frame("mark|sym=TOYB-PERP|px=7").unwrap();
    let mark = MdEvent::Mark {
        inst: INST_B,
        px: PxExact::new(7, 0),
    };
    assert_eq!(rig.pushed(), [(meta(None, None), mark)]);
}

#[test]
fn a_feed_frame_it_cannot_decode_whole_pushes_nothing() {
    let mut rig = Rig::new(ToyMd::new(STREAM));
    rig.subscribe(&STREAMED.map(|feed| on(INST_A, feed)), &[])
        .unwrap();
    rig.fx.take();
    let refused = [
        // A trade of no quantity, or a negative one, is malformed, never absent.
        (
            "trade|sym=TOYA-PERP|id=42|side=B|px=201|qty=-3",
            Malformed("qty"),
        ),
        (
            "trade|sym=TOYA-PERP|id=42|side=B|px=201|qty=0",
            Malformed("qty"),
        ),
        ("trade|sym=TOYA-PERP|id=42|side=B|px=201", Malformed("qty")),
        (
            "trade|sym=TOYA-PERP|id=42|side=X|px=201|qty=3",
            Malformed("side"),
        ),
        // The caps say every trade names its aggressor and its id.
        ("trade|sym=TOYA-PERP|id=42|px=201|qty=3", Malformed("side")),
        ("trade|sym=TOYA-PERP|side=B|px=201|qty=3", Malformed("id")),
        (
            "trade|sym=TOYA-PERP|id=-1|side=B|px=201|qty=3",
            Malformed("id"),
        ),
        (
            "trade|sym=TOYA-PERP|id=42|side=B|px=20.5|qty=3",
            Malformed("px"),
        ),
        (
            "trade|sym=TOYA-PERP|id=42|side=B|px=201|qty=3|ts=y",
            Malformed("ts"),
        ),
        (
            "trade|sym=TOYA-PERP|id=42|side=B|px=201|qty=3|seq=-1",
            Malformed("seq"),
        ),
        ("mark|sym=TOYA-PERP|px=1e3", Malformed("px")),
        ("mark|sym=TOYA-PERP", Malformed("px")),
        ("index|sym=TOYA-PERP|px=", Malformed("px")),
        // The caps say every funding rate states its interval and the next funding time.
        (
            "funding|sym=TOYA-PERP|rate=1.5|int=3600|next=1",
            Malformed("rate"),
        ),
        ("funding|sym=TOYA-PERP|rate=1|next=1", Malformed("int")),
        (
            "funding|sym=TOYA-PERP|rate=1|int=0|next=1",
            Malformed("int"),
        ),
        (
            "funding|sym=TOYA-PERP|rate=1|int=-60|next=1",
            Malformed("int"),
        ),
        ("funding|sym=TOYA-PERP|rate=1|int=3600", Malformed("next")),
        // Stats come only from the poll endpoint.
        (
            "stats|sym=TOYA-PERP|vol=1|oi=1",
            Malformed("stats come only from the poll endpoint"),
        ),
        ("tick|sym=TOYA-PERP|px=1", Malformed("kind")),
        (
            "trade|sym=NOPE|id=42|side=B|px=201|qty=3",
            DecodeError::UnknownInstrument,
        ),
        ("trade|id=42|side=B|px=201|qty=3", Malformed("sym")),
        (
            "mark|sym=TOYA-PERP|px=1\nmark|sym=TOYA-PERP|px=2",
            Malformed("one record per frame"),
        ),
    ];
    for (frame, why) in refused {
        assert_eq!(rig.frame(frame), Err(why), "{frame}");
    }
    assert!(rig.pushed().is_empty());
    assert!(rig.fx.is_empty());
}

// ---------------------------------------------------------------------------------------------
// Each feed on the transport its source names.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_poll_feed_is_refused_on_the_socket_codec_and_a_stream_feed_on_the_poll_codec() {
    // Where the toy's caps say each feed comes from.
    let caps = toy::caps().md;
    for feed in [Feed::Book(BOOK), Feed::Book(ANCHORED_BOOK)]
        .into_iter()
        .chain(STREAMED)
    {
        assert_eq!(feed_source(feed), FeedSource::Stream, "{feed:?}");
    }
    assert_eq!(
        (feed_source(Feed::Stats), caps.stats),
        (FeedSource::Poll, FeedSource::Poll)
    );
    for feed in [Feed::Book(BookId(2)), Feed::Touch(TouchSourceId(0))] {
        assert_eq!(feed_source(feed), FeedSource::None, "{feed:?}");
    }
    let touch = on(INST_A, Feed::Touch(TouchSourceId(0)));

    // The socket codec refuses stats, added or removed, and takes nothing of the call.
    let mut socket = Rig::new(ToyMd::new(STREAM));
    for (add, remove) in [
        (&[stats(INST_A)][..], &[][..]),
        (&[], &[stats(INST_A)]),
        (&[on(INST_A, Feed::Trades), stats(INST_A)], &[]),
    ] {
        let refused = socket.subscribe(add, remove);
        assert_eq!(refused, Err(VenueError::UnsupportedFeed(stats(INST_A))));
    }
    let refused = socket.subscribe(&[touch], &[]);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(touch)));
    assert!(socket.fx.is_empty());
    socket
        .frame("trade|sym=TOYA-PERP|id=1|side=B|px=1|qty=1")
        .unwrap();
    assert!(socket.pushed().is_empty());

    // The poll codec refuses every streamed feed, books included, and takes nothing of the call.
    let mut poll = Rig::new(ToyStats::new(POLL, WireUrl::plain(BASE)));
    let streamed = [Feed::Book(BOOK), Feed::Book(ANCHORED_BOOK)]
        .into_iter()
        .chain(STREAMED);
    for feed in streamed {
        let sub = on(INST_A, feed);
        assert_eq!(
            poll.subscribe(&[sub], &[]),
            Err(VenueError::UnsupportedFeed(sub))
        );
        assert_eq!(
            poll.subscribe(&[stats(INST_B)], &[sub]),
            Err(VenueError::UnsupportedFeed(sub))
        );
    }
    let refused = poll.subscribe(&[stats(INST_A), touch], &[]);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(touch)));
    // An instrument missing from the spec table is named before any feed.
    let unknown = stats(InstrumentId::new(99));
    let refused = poll.subscribe(&[on(INST_A, Feed::Trades)], &[unknown]);
    assert_eq!(refused, Err(VenueError::UnknownInstrument(unknown.inst)));
    assert!(poll.fx.is_empty());
    // Nothing of a refused call was taken: no stats subscription, so its timer is not armed.
    poll.timer(1);
    assert!(poll.fx.is_empty());

    // The factory builds the codec for the endpoint's transport.
    let cfg = VenueConfig::new();
    let specs = toy::specs();
    let socket_ep = EndpointPlan {
        stream: STREAM,
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1:9/md"),
        },
        subs: Vec::new(),
    };
    let poll_ep = EndpointPlan {
        stream: POLL,
        transport: MdTransport::Poll {
            base_url: WireUrl::plain(BASE),
        },
        subs: Vec::new(),
    };
    let mut fx = Effects::new();
    let mut md = ToyFactory.md_codec(&cfg, &socket_ep);
    let refused = md.subscribe(&[stats(INST_A)], &[], &specs, &mut fx);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(stats(INST_A))));
    md.subscribe(&[on(INST_A, Feed::Trades)], &[], &specs, &mut fx)
        .unwrap();
    assert_eq!(
        fx.take(),
        [sub_frame("sub|add=TOYA-PERP@trades", Some(INST_A))]
    );
    let mut md = ToyFactory.md_codec(&cfg, &poll_ep);
    let trades = on(INST_A, Feed::Trades);
    let refused = md.subscribe(&[trades], &[], &specs, &mut fx);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(trades)));
    assert!(fx.is_empty());
    md.subscribe(&[stats(INST_A)], &[], &specs, &mut fx)
        .unwrap();
    assert_eq!(fx.take(), [get_a(1), every(1)]);
    assert_eq!(md.keepalive(), None);
}

#[test]
fn the_factory_plans_streamed_feeds_on_the_socket_and_stats_on_a_poll_endpoint() {
    let specs = toy::specs();
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL_KEY, "ws://127.0.0.1:9/ws");
    cfg.insert(STATS_URL_KEY, BASE);
    let streamed = [
        on(INST_A, Feed::Book(BOOK)),
        on(INST_A, Feed::Trades),
        on(INST_A, Feed::Funding),
        on(INST_B, Feed::Mark),
        on(INST_B, Feed::Index),
    ];
    let polled = [stats(INST_A), stats(INST_B)];
    let all: BTreeSet<Subscription> = streamed.iter().chain(&polled).copied().collect();
    let plans = ToyFactory.plan_md(&cfg, &specs, &all).unwrap();
    let socket = EndpointPlan {
        stream: MD_STREAM,
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1:9/ws"),
        },
        subs: BTreeSet::from(streamed).into_iter().collect(),
    };
    let poll = EndpointPlan {
        stream: STATS_STREAM,
        transport: MdTransport::Poll {
            base_url: WireUrl::plain(BASE),
        },
        subs: polled.to_vec(),
    };
    assert_eq!(plans, [socket.clone(), poll.clone()]);
    assert_ne!(MD_STREAM, STATS_STREAM);

    // Stats alone need no market-data socket, and streamed feeds alone no stats base.
    let mut stats_only = VenueConfig::new();
    stats_only.insert(STATS_URL_KEY, BASE);
    let plans = ToyFactory.plan_md(&stats_only, &specs, &BTreeSet::from(polled));
    assert_eq!(plans, Ok(vec![poll]));
    let mut socket_only = VenueConfig::new();
    socket_only.insert(MD_URL_KEY, "ws://127.0.0.1:9/ws");
    let plans = ToyFactory.plan_md(&socket_only, &specs, &BTreeSet::from(streamed));
    assert_eq!(plans, Ok(vec![socket]));

    // Stats without their base, or under a socket URL, are refused as configuration.
    let refused = ToyFactory.plan_md(&socket_only, &specs, &all);
    assert_eq!(
        refused,
        Err(VenueError::Config(ConfigError::Missing(STATS_URL_KEY)))
    );
    let mut ws = socket_only.clone();
    ws.insert(STATS_URL_KEY, "ws://127.0.0.1:9/md");
    let Err(VenueError::Config(ConfigError::Invalid { key, .. })) =
        ToyFactory.plan_md(&ws, &specs, &all)
    else {
        panic!("planned stats under a socket URL");
    };
    assert_eq!(key, STATS_URL_KEY);
    // A feed the caps do not declare is refused whatever is configured.
    let touch = on(INST_A, Feed::Touch(TouchSourceId(0)));
    let refused = ToyFactory.plan_md(&cfg, &specs, &BTreeSet::from([stats(INST_A), touch]));
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(touch)));
}

// ---------------------------------------------------------------------------------------------
// The stats poll.
// ---------------------------------------------------------------------------------------------

#[test]
fn stats_are_polled_under_the_codecs_own_timer_until_the_last_subscription_is_removed() {
    let mut rig = Rig::new(ToyStats::new(POLL, WireUrl::plain(BASE)));
    rig.md.on_open(&mut rig.fx);
    assert!(rig.fx.is_empty());

    // The first subscription is asked for at once, and the timer armed.
    rig.subscribe(&[stats(INST_A)], &[]).unwrap();
    assert_eq!(rig.fx.take(), [get_a(1), every(1)]);
    rig.answer(1, "stats|sym=TOYA-PERP|vol=1500000000000|oi=42|ts=9")
        .unwrap();
    let a = stats_ev(INST_A, Some(1_500_000_000_000), Some(42));
    assert_eq!(rig.pushed(), [(meta(Some(9), None), a)]);
    assert!(rig.fx.is_empty());

    // Another joins: asked at once, under the timer already armed. One already subscribed is
    // asked for no more often, and one not subscribed removes nothing.
    rig.subscribe(&[stats(INST_B), stats(INST_A)], &[]).unwrap();
    assert_eq!(rig.fx.take(), [get_b(2)]);
    // The timer: every subscription asked, the timer armed again.
    rig.timer(1);
    assert_eq!(rig.fx.take(), [get_a(3), get_b(4), every(1)]);
    // A timer of no poll of its fires into nothing.
    rig.timer(7);
    assert!(rig.fx.is_empty());
    // Answers come in any order, each pushed as it comes.
    rig.answer(4, "stats|sym=TOYB-PERP|oi=7").unwrap();
    rig.answer(3, "stats|sym=TOYA-PERP|vol=5").unwrap();
    rig.answer(2, "stats|sym=TOYB-PERP|vol=0|oi=0").unwrap();
    // A tag answered already, or never asked, is ignored.
    rig.answer(3, "stats|sym=TOYA-PERP|vol=6").unwrap();
    rig.answer(40, "stats|sym=TOYA-PERP|vol=6").unwrap();
    let none = meta(None, None);
    assert_eq!(
        rig.pushed(),
        [
            (none, stats_ev(INST_B, None, Some(7))),
            (none, stats_ev(INST_A, Some(5), None)),
            (none, stats_ev(INST_B, Some(0), Some(0))),
        ]
    );

    // One removed: nothing written (a poll endpoint has no socket), its answer in flight
    // ignored, the next poll asking for the other alone.
    rig.timer(1);
    assert_eq!(rig.fx.take(), [get_a(5), get_b(6), every(1)]);
    rig.subscribe(&[], &[stats(INST_A)]).unwrap();
    assert!(rig.fx.is_empty());
    rig.answer(5, "stats|sym=TOYA-PERP|oi=1").unwrap();
    rig.answer(6, "stats|sym=TOYB-PERP|oi=2").unwrap();
    assert_eq!(rig.pushed(), [(none, stats_ev(INST_B, None, Some(2)))]);
    rig.timer(1);
    assert_eq!(rig.fx.take(), [get_b(7), every(1)]);

    // The last removed: its answer in flight ignored, and the timer fires into nothing, so
    // polling stops.
    rig.subscribe(&[], &[stats(INST_B)]).unwrap();
    assert!(rig.fx.is_empty());
    rig.answer(7, "stats|sym=TOYB-PERP|oi=3").unwrap();
    for _ in 0..3 {
        rig.timer(1);
    }
    assert!(rig.fx.is_empty());
    assert!(rig.pushed().is_empty());

    // Subscribed again: asked at once under a fresh timer; the stopped one's tag still fires
    // into nothing, so it never polls twice as often.
    rig.subscribe(&[stats(INST_A)], &[]).unwrap();
    assert_eq!(rig.fx.take(), [get_a(8), every(2)]);
    rig.timer(1);
    assert!(rig.fx.is_empty());
    rig.timer(2);
    assert_eq!(rig.fx.take(), [get_a(9), every(2)]);
    // Removed and added in one call: still subscribed, the timer left as it is.
    rig.subscribe(&[stats(INST_A)], &[stats(INST_A)]).unwrap();
    assert_eq!(rig.fx.take(), [get_a(10)]);
    rig.timer(2);
    assert_eq!(rig.fx.take(), [get_a(11), every(2)]);
}

#[test]
fn negative_open_interest_or_volume_is_refused_with_nothing_pushed() {
    let mut rig = Rig::new(ToyStats::new(POLL, WireUrl::plain(BASE)));
    rig.subscribe(&[stats(INST_A)], &[]).unwrap();
    assert_eq!(rig.fx.take(), [get_a(1), every(1)]);
    let refused = [
        ("stats|sym=TOYA-PERP|vol=10|oi=-1", Malformed("oi")),
        ("stats|sym=TOYA-PERP|vol=-1|oi=3", Malformed("vol")),
        ("stats|sym=TOYA-PERP|vol=-1", Malformed("vol")),
        ("stats|sym=TOYA-PERP|oi=-5", Malformed("oi")),
        ("stats|sym=TOYA-PERP|vol=x|oi=3", Malformed("vol")),
        ("stats|sym=TOYA-PERP|vol=3|oi=1.5", Malformed("oi")),
        // A record that reports nothing, another instrument's, or no record of stats.
        ("stats|sym=TOYA-PERP", Malformed("stats")),
        ("stats|sym=TOYA-PERP|ts=4", Malformed("stats")),
        ("stats|sym=TOYB-PERP|oi=1", Malformed("sym")),
        ("stats|sym=NOPE|oi=1", Malformed("sym")),
        ("stats|oi=1", Malformed("sym")),
        ("mark|sym=TOYA-PERP|px=1", Malformed("kind")),
        ("stats|sym=TOYA-PERP|oi=1|ts=y", Malformed("ts")),
        ("stats|sym=TOYA-PERP|oi", Malformed("field")),
        (
            "stats|sym=TOYA-PERP|oi=1\nstats|sym=TOYA-PERP|oi=2",
            Malformed("one record per frame"),
        ),
    ];
    let mut tag = 1;
    for (body, why) in refused {
        assert_eq!(rig.answer(tag, body), Err(why), "{body}");
        // The timer is the retry: nothing else is asked for.
        assert!(rig.fx.is_empty(), "{body}");
        rig.timer(1);
        assert_eq!(rig.fx.take(), [get_a(tag + 1), every(1)], "{body}");
        tag += 1;
    }
    assert_eq!(
        rig.http(tag, Ok((200, b"stats|\xff"))),
        Err(Malformed("body"))
    );
    assert!(rig.pushed().is_empty());

    // A status other than 200, or no response, pushes nothing and asks for nothing: the next
    // timer polls again.
    for failed in [
        Ok((503, &b"stats|sym=TOYA-PERP|oi=1"[..])),
        Err(HttpFailure::NotSent),
        Err(HttpFailure::TimedOut),
        Err(HttpFailure::Lost),
    ] {
        rig.timer(1);
        tag += 1;
        assert_eq!(rig.fx.take(), [get_a(tag), every(1)]);
        assert_eq!(rig.http(tag, failed), Ok(()));
        assert!(rig.fx.is_empty());
    }
    assert!(rig.pushed().is_empty());
    // A refused answer spent its request: the same tag answered again is ignored.
    assert_eq!(rig.answer(1, "stats|sym=TOYA-PERP|oi=9"), Ok(()));
    assert!(rig.pushed().is_empty());

    // The next good answer is pushed whole.
    rig.timer(1);
    tag += 1;
    rig.fx.take();
    rig.answer(tag, "stats|sym=TOYA-PERP|vol=0|oi=0|ts=12")
        .unwrap();
    let zero = stats_ev(INST_A, Some(0), Some(0));
    assert_eq!(rig.pushed(), [(meta(Some(12), None), zero)]);
}

#[test]
fn the_poll_codec_takes_no_frame_and_names_its_bases_credentials_wherever_echoed() {
    let mut rig = Rig::new(ToyStats::new(POLL, WireUrl::plain(BASE)));
    rig.subscribe(&[stats(INST_A)], &[]).unwrap();
    rig.fx.take();
    // A poll endpoint has no socket: a frame handed to it is refused, nothing pushed.
    let refused = rig.frame("stats|sym=TOYA-PERP|oi=1");
    assert_eq!(refused, Err(Malformed("a poll endpoint takes no frame")));
    assert!(rig.pushed().is_empty() && rig.fx.is_empty());
    assert_eq!(rig.md.keepalive(), None);

    // A base whose path holds a credential: every request keeps its span, an echo of it in a
    // response or a frame is named, and its Debug shows none of it.
    const SECRET: &str = "SYNTHETIC-STATS-PATH-KEY";
    let text = format!("https://toy.invalid/{SECRET}");
    let span = 20..(20 + SECRET.len() as u32);
    assert_eq!(&text[20..], SECRET);
    let base = WireUrl::redacted(text.clone(), vec![span.clone()]).unwrap();
    let mut rig = Rig::new(ToyStats::new(POLL, base));
    rig.subscribe(&[stats(INST_B)], &[]).unwrap();
    let Some(Effect::Http { req, .. }) = rig.fx.take().into_iter().next() else {
        panic!("no request");
    };
    let want = WireUrl::redacted(format!("{text}/stats/TOYB-PERP"), vec![span]).unwrap();
    assert_eq!(req.url, want);
    let shown = format!("{:?}", rig.md);
    assert!(!shown.contains(SECRET), "{shown}");
    assert!(
        shown.contains("ToyStats") && shown.contains("secrets: 1"),
        "{shown}"
    );

    let body = format!("err|path={SECRET}");
    let at = body.find(SECRET).unwrap() as u32;
    let resp = HttpResponse {
        status: 404,
        headers: &[("x-echo", SECRET)],
        body: body.as_bytes(),
    };
    let input = Inbound::Http(HttpTag(1), resp);
    let named = rig.md.redact_inbound(input);
    let echo = at..at + SECRET.len() as u32;
    let want = InboundSpans::response(vec![(0, fbc_core::HeaderMark::Value)], vec![echo.clone()]);
    assert_eq!(named, want);
    assert_eq!(named.check(input), Ok(()));
    let frame = Inbound::Frame(RawFrame::Text(&body));
    assert_eq!(
        rig.md.redact_inbound(frame),
        InboundSpans::frame(vec![echo])
    );
    // A plain base names nothing.
    let plain = ToyStats::new(POLL, WireUrl::plain(BASE));
    assert_eq!(plain.redact_inbound(input), InboundSpans::NONE);
}
