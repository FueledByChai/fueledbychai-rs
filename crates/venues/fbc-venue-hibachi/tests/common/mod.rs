//! What the Hibachi market-data tests share: a configuration, the spec table discovery reads
//! from `fixtures/hibachi/rest/exchange_info.json` (ETH/USDT-P, SOL/USDT-P and SEI/USDT-P), the
//! hand-built frames of `fixtures/hibachi/md/`, and a driver that hands a [`HibachiMd`] frames
//! and timers and keeps an `fbc-book` book of what it pushes, as a consumer's `MdBooks` would.
//!
//! What the signer's tests share: a reader of `fixtures/hibachi/signing/hibachi-vectors.tsv`, the
//! file `HibachiSignOracle.java` writes from the Java FueledByChaiTrading packer and signer. Every
//! value in it is synthetic (the directory's `SYNTHETIC` file says where each comes from), and the
//! signing key the tests use is read from its header, so that no key-shaped value sits in this
//! crate. Each row's instrument is built here as an `InstrumentSpec` with the row's contract id,
//! asset decimals, tick and step.

#![allow(dead_code)]

pub mod status;

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use fbc_book::{BookState, Books, L2Book};
use fbc_core::{
    AliasTable, AssetSym, BookSide, DecodeError, Effect, Effects, Feed, FundingSpec, InstrumentId,
    InstrumentKind, InstrumentResolver, InstrumentSpec, Listing, Lots, Lvl, MdCodec, MdEvent,
    MdSink, MonoNs, PriceGrid, RawFrame, Side, SigningParams, SizeStep, SpecTable, StreamId,
    Subscription, Ticks, TimerTag, TradingStatus, UnderlyingId, VenueConfig, VenueFactory, VenueId,
    VenueMeta, VenueNativeId, VenueOrderId, WallNs, dispatch_market_data,
};
use fbc_venue_hibachi::discover::EXCHANGE_INFO_TAG;
use fbc_venue_hibachi::sign::{EcdsaKey, HibachiSigner, JsonOnly, OrderMessage};
use fbc_venue_hibachi::{
    BOOK_ORDERBOOK, Hibachi, HibachiMd, HibachiMdCounts, KEY_AUCTION_HALT, KEY_DATA_REST_URL,
    KEY_DATA_WS_URL, KEY_ORDERBOOK_DEPTH, KEY_ORDERBOOK_RESUBSCRIBE, KEY_REST_TIMEOUT,
    KEY_STATUS_INTERVAL, TOUCH_ASK_BID_PRICE,
};
use rust_decimal::Decimal;

pub const ETH: InstrumentId = InstrumentId::new(1);
pub const SOL: InstrumentId = InstrumentId::new(2);
pub const SEI: InstrumentId = InstrumentId::new(3);

/// The codec's stream.
pub const STREAM: StreamId = StreamId(0);

/// Every key, with the values the tests run under: an orderbook depth of 3, the fixtures', a
/// resubscribe interval of two seconds, a status read every minute and a two-minute window after
/// a reopening.
pub const CONFIG: [(&str, &str); 7] = [
    (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz"),
    (KEY_REST_TIMEOUT, "5000ms"),
    (KEY_ORDERBOOK_DEPTH, "3"),
    (KEY_DATA_WS_URL, "wss://data-api.hibachi.xyz"),
    (KEY_ORDERBOOK_RESUBSCRIBE, "2000ms"),
    (KEY_STATUS_INTERVAL, "60s"),
    (KEY_AUCTION_HALT, "120s"),
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

pub fn vectors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/hibachi/signing/hibachi-vectors.tsv")
}

pub fn dec(text: &str) -> Decimal {
    Decimal::from_str_exact(text).unwrap_or_else(|e| panic!("{text}: {e}"))
}

/// Lowercase hex of `bytes`.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The bytes `text` (hex) holds.
pub fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex length");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

/// One row of the vectors file, by column name.
pub struct Row(HashMap<String, String>);

impl Row {
    pub fn get(&self, column: &str) -> &str {
        self.0
            .get(column)
            .unwrap_or_else(|| panic!("no column {column}"))
    }

    pub fn name(&self) -> &str {
        self.get("name")
    }

    pub fn u64(&self, column: &str) -> u64 {
        self.get(column)
            .parse()
            .unwrap_or_else(|_| panic!("{}: {column}", self.name()))
    }

    pub fn dec(&self, column: &str) -> Decimal {
        dec(self.get(column))
    }

    pub fn price(&self) -> Option<Decimal> {
        match self.get("price") {
            "" => None,
            text => Some(dec(text)),
        }
    }

    pub fn side(&self) -> Side {
        match self.get("side") {
            "BID" => Side::Buy,
            "ASK" => Side::Sell,
            other => panic!("{}: side {other}", self.name()),
        }
    }

    pub fn signed(&self) -> bool {
        match self.get("outcome") {
            "signed" => true,
            "refused" => false,
            other => panic!("{}: outcome {other}", self.name()),
        }
    }

    /// The row's instrument: its contract id, decimals, tick and step.
    pub fn spec(&self) -> InstrumentSpec {
        spec(
            self.u64("contract_id"),
            u8::try_from(self.u64("underlying_decimals")).unwrap(),
            u8::try_from(self.u64("settlement_decimals")).unwrap(),
            self.get("tick_size"),
            self.get("step_size"),
        )
    }

    /// The place or modify the row states, for `spec` (the row's own).
    pub fn order<'a>(&self, spec: &'a InstrumentSpec) -> OrderMessage<'a> {
        OrderMessage {
            spec,
            nonce: self.u64("nonce"),
            side: self.side(),
            quantity: self.dec("quantity"),
            price: self.price(),
            max_fees: self.dec("max_fees"),
            json_only: JsonOnly::NONE,
        }
    }
}

/// A synthetic perpetual with contract id `contract`, the asset decimals given, and a `tick`
/// price grid and `step` size step.
pub fn spec(
    contract: u64,
    underlying: u8,
    settlement: u8,
    tick: &str,
    step: &str,
) -> InstrumentSpec {
    let caps = Hibachi.caps(&config()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol("SYNTH/USDT-P"))
        .expect("a venue symbol");
    let usdt = AssetSym::new("USDT").unwrap();
    InstrumentSpec {
        id: InstrumentId::new(1),
        venue: VenueId::new(6),
        venue_symbol,
        native_id: Some(VenueNativeId::Number(contract)),
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed(dec(tick)).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new(dec(step)).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usdt,
        settle_ccy: usdt,
        funding: FundingSpec::Known {
            interval: Duration::from_secs(3_600),
            cap: None,
        },
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: Some(SigningParams::AssetDecimals {
            underlying,
            settlement,
        }),
        version: 1,
        fetched_at: WallNs(0),
    }
}

/// The venue order id `text`, as a decode scope makes one.
pub fn venue_id(text: &str) -> VenueOrderId {
    let caps = Hibachi.caps(&config()).unwrap();
    dispatch_market_data(&caps, |scope| scope.venue_order_id(text)).expect("a venue order id")
}

pub struct Vectors {
    pub header: HashMap<String, String>,
    pub rows: Vec<Row>,
}

impl Vectors {
    /// The file: `# name=value` header lines, one pair each, then a row of column names and a
    /// tab-separated row per vector.
    pub fn read() -> Vectors {
        let text = fs::read_to_string(vectors_path()).expect("the Java vectors file");
        let mut header = HashMap::new();
        let mut columns: Option<Vec<String>> = None;
        let mut rows = Vec::new();
        for line in text.lines() {
            if let Some(comment) = line.strip_prefix("# ") {
                if let Some((k, v)) = comment.split_once('=')
                    && k.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
                {
                    header.insert(k.to_owned(), v.to_owned());
                }
                continue;
            }
            let cells: Vec<&str> = line.split('\t').collect();
            match &columns {
                None => columns = Some(cells.iter().map(|c| (*c).to_owned()).collect()),
                Some(names) => {
                    assert_eq!(cells.len(), names.len(), "row width: {line}");
                    rows.push(Row(names
                        .iter()
                        .cloned()
                        .zip(cells.iter().map(|c| (*c).to_owned()))
                        .collect()));
                }
            }
        }
        Vectors { header, rows }
    }

    /// The synthetic key's text, `0x` and 64 hex digits.
    pub fn key(&self) -> &str {
        &self.header["key"]
    }

    pub fn signer(&self) -> HibachiSigner {
        HibachiSigner::new(EcdsaKey::from_hex(self.key()).unwrap())
    }

    pub fn row(&self, name: &str) -> &Row {
        let mut found = self.rows.iter().filter(|r| r.name() == name);
        let row = found.next().unwrap_or_else(|| panic!("no vector {name}"));
        assert!(found.next().is_none(), "vector {name} twice");
        row
    }
}
