//! What the Hibachi market-data tests share: a configuration, the spec table discovery reads
//! from `fixtures/hibachi/rest/exchange_info.json` (ETH/USDT-P, SOL/USDT-P and SEI/USDT-P), the
//! hand-built frames of `fixtures/hibachi/md/`, and a driver that hands a [`HibachiMd`] frames
//! and timers and keeps an `fbc-book` book of what it pushes, as a consumer's `MdBooks` would.

#![allow(dead_code)]

use std::path::PathBuf;

use fbc_book::{BookState, Books, L2Book};
use fbc_core::{
    AliasTable, BookSide, DecodeError, Effect, Effects, Feed, InstrumentId, InstrumentResolver,
    Listing, Lots, Lvl, MdCodec, MdEvent, MdSink, MonoNs, RawFrame, SpecTable, StreamId,
    Subscription, Ticks, TimerTag, UnderlyingId, VenueConfig, VenueFactory, VenueId, VenueMeta,
    WallNs, dispatch_market_data,
};
use fbc_venue_hibachi::discover::EXCHANGE_INFO_TAG;
use fbc_venue_hibachi::{
    BOOK_ORDERBOOK, Hibachi, HibachiMd, HibachiMdCounts, KEY_DATA_REST_URL, KEY_DATA_WS_URL,
    KEY_ORDERBOOK_DEPTH, KEY_ORDERBOOK_RESUBSCRIBE, KEY_REST_TIMEOUT, TOUCH_ASK_BID_PRICE,
};

pub const ETH: InstrumentId = InstrumentId::new(1);
pub const SOL: InstrumentId = InstrumentId::new(2);
pub const SEI: InstrumentId = InstrumentId::new(3);

/// The codec's stream.
pub const STREAM: StreamId = StreamId(0);

/// Every key, with the values the tests run under: an orderbook depth of 3, the fixtures', and a
/// resubscribe interval of two seconds.
pub const CONFIG: [(&str, &str); 5] = [
    (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz"),
    (KEY_REST_TIMEOUT, "5000ms"),
    (KEY_ORDERBOOK_DEPTH, "3"),
    (KEY_DATA_WS_URL, "wss://data-api.hibachi.xyz"),
    (KEY_ORDERBOOK_RESUBSCRIBE, "2000ms"),
];

/// The resubscribe interval [`CONFIG`] states.
pub const INTERVAL: std::time::Duration = std::time::Duration::from_millis(2_000);

pub fn config() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, value) in CONFIG {
        cfg.insert(key, value);
    }
    cfg
}

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../fixtures/hibachi")
}

/// The frame `fixtures/hibachi/md/<name>.json`, without its line end.
pub fn frame(name: &str) -> String {
    let path = fixtures().join("md").join(format!("{name}.json"));
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    text.trim_end().to_owned()
}

/// ETH/USDT-P, SOL/USDT-P and SEI/USDT-P as discovery reads them from the REST fixture.
pub fn specs() -> SpecTable {
    let body = std::fs::read(fixtures().join("rest/exchange_info.json")).unwrap();
    let cfg = config();
    let plan = Hibachi.discover(&cfg).unwrap();
    let caps = Hibachi.caps(&cfg).unwrap();
    let answer = fbc_core::HttpResponse {
        status: 200,
        headers: &[],
        body: &body,
    };
    let drafts = dispatch_market_data(&caps, |scope| {
        plan.parse(&[(EXCHANGE_INFO_TAG, Ok(answer))], scope)
    })
    .unwrap()
    .done()
    .unwrap();
    let venue = VenueId::new(6);
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    let mut table = SpecTable::new();
    for (id, symbol) in [
        (ETH, "ETH/USDT-P"),
        (SOL, "SOL/USDT-P"),
        (SEI, "SEI/USDT-P"),
    ] {
        let draft = drafts
            .iter()
            .find(|d| d.venue_symbol.as_wire() == symbol)
            .unwrap()
            .clone();
        let listing = Listing {
            id,
            underlying: UnderlyingId::new(id.get()),
        };
        resolver.list(venue, draft.asset, listing).unwrap();
        table.insert(resolver.spec(venue, draft, 1, WallNs(0)).unwrap());
    }
    table
}

pub fn sub(inst: InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

pub fn book_sub(inst: InstrumentId) -> Subscription {
    sub(inst, Feed::Book(BOOK_ORDERBOOK))
}

pub fn touch_sub(inst: InstrumentId) -> Subscription {
    sub(inst, Feed::Touch(TOUCH_ASK_BID_PRICE))
}

/// Every feed the codec decodes on `inst`.
pub fn every_feed(inst: InstrumentId) -> Vec<Subscription> {
    vec![
        book_sub(inst),
        touch_sub(inst),
        sub(inst, Feed::Trades),
        sub(inst, Feed::Mark),
        sub(inst, Feed::Index),
        sub(inst, Feed::Funding),
    ]
}

/// A sink that keeps what it is pushed.
#[derive(Default)]
pub struct Kept(pub Vec<(VenueMeta, MdEvent)>);

impl MdSink for Kept {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push((meta, ev));
    }
}

/// An ETH price in ticks (0.000001): `px` in hundredths, as the book's levels are written.
pub fn eth_px(hundredths: i64) -> Ticks {
    Ticks(hundredths * 10_000)
}

/// An ETH size in lots (0.000000001): `qty` in thousandths.
pub fn eth_qty(thousandths: i64) -> Lots {
    Lots::new(thousandths * 1_000_000).unwrap()
}

pub fn eth_lvl(hundredths: i64, thousandths: i64) -> Lvl {
    Lvl {
        px: eth_px(hundredths),
        qty: eth_qty(thousandths),
    }
}

/// A codec under [`config`], subscribed, with `fbc-book` books fed what it pushes.
pub struct Driver {
    pub codec: HibachiMd,
    pub specs: SpecTable,
    pub books: Books,
    /// The effects asked for and not yet taken.
    pub fx: Effects,
}

impl Driver {
    /// A codec subscribed to `subs`, its subscribe effects left in [`Driver::fx`].
    pub fn new(subs: &[Subscription]) -> Driver {
        Driver::with_config(&config(), subs)
    }

    pub fn with_config(cfg: &VenueConfig, subs: &[Subscription]) -> Driver {
        let mut codec = HibachiMd::new(STREAM, cfg);
        let specs = specs();
        let mut fx = Effects::new();
        codec.on_open(&mut fx);
        codec.subscribe(subs, &[], &specs, &mut fx).unwrap();
        Driver {
            codec,
            specs,
            books: Books::new(),
            fx,
        }
    }

    /// Hands the codec `text`: what it pushed, each event applied to the books, or its refusal.
    pub fn try_text(&mut self, text: &str) -> Result<Vec<(VenueMeta, MdEvent)>, DecodeError> {
        let caps = Hibachi.caps(&config()).unwrap();
        let mut sink = Kept::default();
        let codec = &mut self.codec;
        let (specs, fx) = (&self.specs, &mut self.fx);
        let result = dispatch_market_data(&caps, |scope| {
            codec.on_frame(RawFrame::Text(text), scope, specs, &mut sink, fx)
        });
        if result.is_err() {
            assert!(sink.0.is_empty(), "a refused frame pushed events");
        }
        result?;
        self.apply(&sink.0);
        self.check_copies();
        Ok(sink.0)
    }

    /// Every book the codec holds a valid copy of is that copy, as the events it pushed built it.
    fn check_copies(&self) {
        for inst in [ETH, SOL, SEI] {
            if let Some(copy) = self.codec.book_copy(inst) {
                let top = self.book(inst).top(usize::MAX).expect("a valid book");
                assert_eq!((top.bids, top.asks), copy, "{inst:?}");
            }
        }
    }

    /// Hands the codec frame `name`, which it must take: what it pushed.
    pub fn feed(&mut self, name: &str) -> Vec<MdEvent> {
        let text = frame(name);
        let pushed = self
            .try_text(&text)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        pushed.into_iter().map(|(_, ev)| ev).collect()
    }

    /// Fires timer `tag`: what the codec pushed.
    pub fn fire(&mut self, tag: TimerTag) -> Vec<MdEvent> {
        let mut sink = Kept::default();
        self.codec
            .on_timer(tag, MonoNs(1), WallNs(1), &mut sink, &mut self.fx);
        self.apply(&sink.0);
        sink.0.into_iter().map(|(_, ev)| ev).collect()
    }

    fn apply(&mut self, events: &[(VenueMeta, MdEvent)]) {
        for (_, ev) in events {
            self.books.apply(ev).unwrap();
        }
    }

    /// The effects asked for since the last call.
    pub fn take_fx(&mut self) -> Vec<Effect> {
        self.fx.take()
    }

    /// The timers asked for since the last take, leaving the other effects.
    pub fn timers(&mut self) -> Vec<TimerTag> {
        let effects = self.fx.take();
        let mut tags = Vec::new();
        for effect in effects {
            match effect {
                Effect::Timer { tag, after } => {
                    assert_eq!(after, INTERVAL);
                    tags.push(tag);
                }
                other => self.fx.push(other),
            }
        }
        tags
    }

    /// `inst`'s orderbook as the events built it.
    pub fn book(&self, inst: InstrumentId) -> &L2Book {
        self.books.get(inst, BOOK_ORDERBOOK).expect("a book")
    }

    pub fn state(&self, inst: InstrumentId) -> BookState {
        self.books
            .get(inst, BOOK_ORDERBOOK)
            .map_or(BookState::AwaitingSnapshot, L2Book::state)
    }

    /// Whether `inst`'s orderbook is valid: a book never made valid, or gapped, is not.
    pub fn valid(&self, inst: InstrumentId) -> bool {
        matches!(self.state(inst), BookState::Valid { .. })
    }

    pub fn counts(&self) -> HibachiMdCounts {
        self.codec.counts()
    }

    /// Sends `remove` through the codec's subscribe.
    pub fn remove(&mut self, remove: &[Subscription]) {
        self.codec
            .subscribe(&[], remove, &self.specs, &mut self.fx)
            .unwrap();
    }
}

/// The gap event of `inst`'s book.
pub fn book_gap(inst: InstrumentId) -> MdEvent {
    MdEvent::Health {
        inst,
        feed: Feed::Book(BOOK_ORDERBOOK),
        h: fbc_core::FeedHealth::Gap,
    }
}

/// An ETH level event of the orderbook.
pub fn level(side: BookSide, hundredths: i64, thousandths: i64) -> MdEvent {
    MdEvent::Level {
        inst: ETH,
        book: BOOK_ORDERBOOK,
        side,
        px: eth_px(hundredths),
        qty: eth_qty(thousandths),
    }
}

/// An ETH one-side prune of the orderbook, in hundredths.
pub fn prune(side: BookSide, lo: i64, hi: i64) -> MdEvent {
    MdEvent::SidePrune {
        inst: ETH,
        book: BOOK_ORDERBOOK,
        side,
        lo: eth_px(lo),
        hi: eth_px(hi),
    }
}
