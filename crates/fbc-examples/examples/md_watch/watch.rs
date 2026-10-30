//! md_watch's wiring: one fbc-runtime `MdVenue` per venue asked for, each with the venue's
//! factory, a configuration and spec table built from the command line (Hibachi's spec read from
//! the venue first, `hibachi.rs`), and explicit reconnect pacing, liveness and write-stall
//! settings; a handler per venue that keeps the books (`MdBooks`, one fbc-book book per channel)
//! and prints a line per update. No order is built or sent, and no credential is read: every
//! venue's market data here is public.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io::Write;
use std::ops::Range;
use std::rc::Rc;
use std::time::Duration;

use fbc_core::{
    Aggressor, AssetSym, BookId, ConnKey, Envelope, Feed, FeedHealth, FundingSpec, InstrumentId,
    InstrumentKind, InstrumentSpec, Lots, Lvl, MdEvent, MdTransport, PriceGrid, SizeStep,
    SpecTable, Subscription, Ticks, TouchSourceId, TradingStatus, UnderlyingId, VenueConfig,
    VenueFactory, VenueId, WallNs, dispatch_market_data,
};
use fbc_runtime::{
    Connector, IngestClock, Liveness, MdBooks, MdHandler, MdVenue, MdVenueConfig, MdVenueControl,
    RateLimiter, ReconnectPacing, SafetyReserve, TradingBooks, WriteStall,
};
use fbc_venue_binance_usdm::{
    BOOK_DIFF, BinanceUsdm, KEY_DEPTH_LEVELS, KEY_DEPTH_SPEED, KEY_REST_BASE_URL,
    KEY_SNAPSHOT_LIMIT, KEY_SNAPSHOT_RETRY, KEY_SNAPSHOT_TIMEOUT, KEY_WS_BASE_URL,
    TOUCH_BOOK_TICKER,
};
use fbc_venue_hibachi::{BOOK_ORDERBOOK, HibachiMdCounts, TOUCH_ASK_BID_PRICE};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::MD_URL;
use fbc_venue_paradex::md::{BBO, BBO_INTERACTIVE, DELTAS};
use rust_decimal::Decimal;
use tokio::sync::Notify;

use crate::args::{HibachiOpts, Market, Options, ParadexTouch};
use crate::hibachi::{self, Listed, Tally};

/// Where the lines go: standard output when run, a buffer under test.
pub type Out = Rc<RefCell<dyn Write>>;

/// The lines' destination and what ends md_watch when it fails: the first write that fails
/// wakes the stop, since nothing md_watch prints after that is seen, and is kept so `run` can
/// report it. Standard output closed (`md_watch ... | head`) is the reader leaving, not a
/// failure; any other error (a full disk under `> watch.log`) is returned.
#[derive(Clone)]
struct Sink {
    out: Out,
    closed: Rc<Notify>,
    failed: Rc<RefCell<Option<std::io::Error>>>,
}

impl Sink {
    fn write_line(&self, line: std::fmt::Arguments<'_>) {
        if let Err(e) = writeln!(self.out.borrow_mut(), "{line}") {
            let mut failed = self.failed.borrow_mut();
            if failed.is_none() {
                *failed = Some(e);
                // A permit is kept if the stop is not waiting yet, so no failure is missed.
                self.closed.notify_one();
            }
        }
    }

    /// The first write error, unless it was standard output closing.
    fn error(&self) -> Result<(), String> {
        match self.failed.borrow().as_ref() {
            Some(e) if e.kind() != std::io::ErrorKind::BrokenPipe => {
                Err(format!("standard output: {e}"))
            }
            _ => Ok(()),
        }
    }
}

/// The Paradex market's, the Binance symbol's and the Hibachi contract's instrument ids; each
/// venue has its own spec table, and the ids differ so a line can never be printed under the
/// wrong one.
pub const PARADEX_INST: InstrumentId = InstrumentId::new(1);
pub const BINANCE_INST: InstrumentId = InstrumentId::new(2);
pub const HIBACHI_INST: InstrumentId = InstrumentId::new(3);

/// Each venue's connection numbers, disjoint on the one ingest clock.
const PARADEX_CONNS: Range<u16> = 0..64;
const BINANCE_CONNS: Range<u16> = 64..128;
const HIBACHI_CONNS: Range<u16> = 128..192;

/// Reconnects back off from 250 ms to 30 s, at most 10 attempts a minute, each abandoned if it
/// has not opened in 10 s.
fn pacing() -> ReconnectPacing {
    ReconnectPacing::new(
        Duration::from_millis(250),
        Duration::from_secs(30),
        10,
        Duration::from_secs(60),
        Duration::from_secs(10),
    )
    .expect("valid pacing")
}

/// A stream that hears nothing, the venue's pings included, for longer than `silence` is
/// reported stale and reconnected. A connection rotates 5 minutes before a venue's declared
/// lifetime (Binance's 24 hours; Paradex declares none).
fn liveness(silence: Duration) -> Liveness {
    Liveness::new(silence, Duration::from_secs(300)).expect("valid liveness")
}

/// Paradex's server pings every 55 s, so 90 s of silence means the stream is dead.
const PARADEX_SILENCE: Duration = Duration::from_secs(90);

/// Binance's server pings only every 3 minutes, and a quiet symbol can send nothing else for
/// that long, so its window is longer than 3 minutes: a shorter one would reconnect a quiet
/// symbol, and re-ask its diff-depth snapshot, over and over.
const BINANCE_SILENCE: Duration = Duration::from_secs(200);

/// A write the venue stops reading ends its connection after 10 s; md_watch writes only
/// subscriptions.
fn write_stall() -> WriteStall {
    WriteStall::new(Duration::from_secs(10)).expect("a non-zero window")
}

/// How often a book whose last frame was not followed by another is printed.
const FLUSH_EVERY: Duration = Duration::from_millis(50);

/// Binance's diff-depth book reads a REST snapshot of up to 1 MiB.
const HTTP_MAX_BODY: usize = 1 << 20;

/// Normal traffic stops at 90% of each declared rate limit.
fn reserve() -> SafetyReserve {
    SafetyReserve::percent(10).expect("a valid reserve")
}

/// The Paradex configuration: its public WebSocket URL.
pub fn paradex_config(url: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, url);
    cfg
}

/// The Binance USD-M configuration: its origins, and the diff-depth book anchored on a
/// 1000-level snapshot that times out after 5 s and is asked again 2 s after a failure. The
/// partial-depth settings are required though md_watch does not subscribe to that channel.
pub fn binance_config(ws: &str, rest: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (KEY_WS_BASE_URL, ws),
        (KEY_DEPTH_LEVELS, "20"),
        (KEY_DEPTH_SPEED, "100ms"),
        (KEY_REST_BASE_URL, rest),
        (KEY_SNAPSHOT_LIMIT, "1000"),
        (KEY_SNAPSHOT_TIMEOUT, "5000ms"),
        (KEY_SNAPSHOT_RETRY, "2000ms"),
    ] {
        cfg.insert(key, value);
    }
    cfg
}

/// A perpetual spelled `market.symbol`, decoded on its tick and step, as the one entry of a
/// spec table.
fn specs(
    venue: &dyn VenueFactory,
    cfg: &VenueConfig,
    id: InstrumentId,
    venue_id: VenueId,
    market: &Market,
    quote: &str,
) -> Result<SpecTable, String> {
    let caps = venue.caps(cfg).map_err(|e| e.to_string())?;
    let symbol = &market.symbol;
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol(symbol))
        .map_err(|e| format!("{symbol}: {e:?}"))?;
    let price_grid = PriceGrid::fixed(market.tick).map_err(|e| format!("tick: {e:?}"))?;
    let size_step = SizeStep::new(market.step).ok_or("step: not a valid size step")?;
    let quote = AssetSym::new(quote).ok_or_else(|| format!("{quote}: not an asset symbol"))?;
    let mut table = SpecTable::new();
    table.insert(InstrumentSpec {
        id,
        venue: venue_id,
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(id.get()),
        kind: InstrumentKind::Perpetual,
        price_grid,
        quote_grid: None,
        size_step,
        min_size: Lots::new(1).expect("one lot"),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: quote,
        settle_ccy: quote,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
        version: 1,
        fetched_at: WallNs(0),
    });
    Ok(table)
}

/// The Paradex touch sources `touch` asks for, each with the channel name md_watch prints it
/// under: `bbo` for `bbo.{market}`, `bbo.interactive` for `bbo.{market}.interactive`.
pub fn paradex_touches(touch: ParadexTouch) -> Vec<(TouchSourceId, &'static str)> {
    let bbo = (BBO, "bbo");
    let interactive = (BBO_INTERACTIVE, "bbo.interactive");
    match touch {
        ParadexTouch::Bbo => vec![bbo],
        ParadexTouch::Interactive => vec![interactive],
        ParadexTouch::Both => vec![bbo, interactive],
    }
}

/// One venue's market as md_watch prints it.
struct Labels {
    venue: &'static str,
    symbol: String,
    /// What md_watch's first line says it watches: its channels.
    about: String,
    inst: InstrumentId,
    /// Each touch source subscribed, and its channel name.
    touches: Vec<(TouchSourceId, &'static str)>,
    book: BookId,
    book_name: &'static str,
    tick: Decimal,
    step: Decimal,
}

/// What md_watch prints of a Hibachi contract beside its events: its state, from the contract's
/// read and its session calendar, and what its codec counted.
struct HibachiShown {
    listed: Listed,
    /// The state last printed, `None` before the first `MARKET` line.
    state: Option<String>,
    tally: Tally,
    /// The counts last printed on this connection.
    counts: HibachiMdCounts,
    /// The connection number of the `/ws/market` socket, whose codec the tally counts: the
    /// status poll's connection (decision 0130) counts nothing, so its end prints no `COUNTS`.
    socket: u16,
}

/// The handler of one venue: keeps its books and prints a line per update.
pub struct Watcher {
    labels: Labels,
    hibachi: Option<HibachiShown>,
    top: usize,
    stamps: bool,
    books: MdBooks,
    /// The ingest sequence and wall time of the frame whose book change is not printed yet. A
    /// frame's book events arrive one by one, all in the one call stack that decodes it, so the
    /// book is printed once the frame's last one has been applied: when an event of another
    /// frame arrives, a connection ends, the flush timer fires between frames, or md_watch
    /// stops.
    pending: Option<(u64, WallNs)>,
    /// The book text printed last; an update that leaves it unchanged prints nothing.
    shown: Option<String>,
    stopping: Rc<Cell<bool>>,
    out: Sink,
}

impl Watcher {
    fn new(
        labels: Labels,
        opts: &Options,
        stamps: bool,
        stopping: Rc<Cell<bool>>,
        out: Sink,
    ) -> Watcher {
        let trading = TradingBooks::new([(labels.inst, labels.book)]).expect("one book");
        Watcher {
            labels,
            hibachi: None,
            top: opts.top,
            stamps,
            books: MdBooks::new(trading),
            pending: None,
            shown: None,
            stopping,
            out,
        }
    }

    fn print(&self, at: WallNs, line: &str) {
        if self.stamps {
            self.out.write_line(format_args!("{} {line}", clock(at)));
        } else {
            self.out.write_line(format_args!("{line}"));
        }
    }

    /// `KIND venue market` and the rest of a line.
    fn line(&self, at: WallNs, kind: &str, rest: &str) {
        let l = &self.labels;
        self.print(at, &format!("{kind} {} {} {rest}", l.venue, l.symbol));
    }

    /// The channel name of touch `source`.
    fn touch_name(&self, source: TouchSourceId) -> &'static str {
        let named = self.labels.touches.iter().find(|(s, _)| *s == source);
        named.map_or("touch", |(_, name)| name)
    }

    fn px(&self, px: Ticks) -> Decimal {
        (Decimal::from(px.0) * self.labels.tick).normalize()
    }

    fn qty(&self, qty: Lots) -> Decimal {
        (Decimal::from(qty.get()) * self.labels.step).normalize()
    }

    fn level(&self, lvl: Option<Lvl>) -> String {
        match lvl {
            Some(l) => format!("{} x {}", self.px(l.px), self.qty(l.qty)),
            None => "-".to_owned(),
        }
    }

    /// `bid <px> x <size> ask <px> x <size> mid <px> spread <bps>bps`, `-` for what is unknown.
    fn touch(&self, bid: Option<Lvl>, ask: Option<Lvl>) -> String {
        let (mid, spread) = match (bid, ask) {
            // A zero mid has no spread in basis points.
            (Some(b), Some(a)) if !(self.px(b.px) + self.px(a.px)).is_zero() => {
                let (b, a) = (self.px(b.px), self.px(a.px));
                let mid = (b + a) / Decimal::TWO;
                let bps = (a - b) / mid * Decimal::from(10_000);
                (
                    mid.normalize().to_string(),
                    format!("{:.2}bps", bps.round_dp(2)),
                )
            }
            _ => ("-".to_owned(), "-".to_owned()),
        };
        format!(
            "bid {} ask {} mid {mid} spread {spread}",
            self.level(bid),
            self.level(ask)
        )
    }

    /// The trading book's line and its best `top` levels, or `None` while it is invalid.
    fn book_text(&self) -> Option<String> {
        let book = self.books.trading_book(self.labels.inst)?;
        let top = book.top(self.top.max(1)).ok()?;
        let l = &self.labels;
        let mut text = format!(
            "BOOK {} {} {} {}",
            l.venue,
            l.symbol,
            l.book_name,
            self.touch(top.bids.first().copied(), top.asks.first().copied())
        );
        for rank in 0..self.top.min(top.bids.len().max(top.asks.len())) {
            text.push_str(&format!(
                "\n  {} bid {} | ask {}",
                rank + 1,
                self.level(top.bids.get(rank).copied()),
                self.level(top.asks.get(rank).copied())
            ));
        }
        Some(text)
    }

    /// Prints the book as the last frame left it, when that changed what is shown; then, for a
    /// Hibachi contract, its state when that changed, and its codec's counts when the codec
    /// counted a kind of thing it had not counted before on this connection.
    pub fn flush(&mut self) {
        self.flush_at(now());
    }

    /// [`flush`](Watcher::flush) at `at`, the instant the contract's state is judged at and its
    /// lines are stamped with.
    fn flush_at(&mut self, at: WallNs) {
        self.flush_book();
        self.show_market(at);
        let Some(h) = &self.hibachi else {
            return;
        };
        let counts = h.tally.get();
        if hibachi::newly_counted(&h.counts, &counts) {
            self.line(at, "COUNTS", &hibachi::counts_text(&counts));
            if let Some(h) = &mut self.hibachi {
                h.counts = counts;
            }
        }
    }

    /// A Hibachi contract's `MARKET` line at `at`, when its state then is not the one last
    /// printed.
    pub fn show_market(&mut self, at: WallNs) {
        let Some(h) = &self.hibachi else {
            return;
        };
        let state = h.listed.state(at);
        if h.state.as_ref() == Some(&state) {
            return;
        }
        self.line(at, "MARKET", &state);
        if let Some(h) = &mut self.hibachi {
            h.state = Some(state);
        }
    }

    /// Prints the book as the last frame left it, when that changed what is shown.
    fn flush_book(&mut self) {
        let Some((_, at)) = self.pending.take() else {
            return;
        };
        match self.book_text() {
            Some(text) if self.shown.as_ref() != Some(&text) => {
                self.print(at, &text);
                self.shown = Some(text);
            }
            Some(_) => {}
            // Invalid until its next snapshot, which is printed whatever it shows.
            None => self.shown = None,
        }
    }

    fn feed(&self, feed: Feed) -> &'static str {
        match feed {
            Feed::Touch(source) => self.touch_name(source),
            Feed::Book(_) => self.labels.book_name,
            Feed::Trades => "trades",
            Feed::Mark => "mark",
            Feed::Index => "index",
            Feed::Funding => "funding",
            Feed::Stats => "stats",
        }
    }

    fn on_event(&mut self, env: Envelope<MdEvent>) {
        let (seq, at) = (env.stamp.ingest_seq, env.stamp.recv_wall);
        if self.pending.is_some_and(|(frame, _)| frame != seq) {
            self.flush();
        }
        // A refused event is counted in the books; the line is printed all the same.
        let _ = self.books.apply(env.stamp.conn, &env.body);
        match env.body {
            MdEvent::Touch {
                bid, ask, source, ..
            } => {
                let rest = format!("{} {}", self.touch_name(source), self.touch(bid, ask));
                self.line(at, "TOUCH", &rest);
            }
            MdEvent::Trade {
                aggressor, px, qty, ..
            } => {
                let side = match aggressor {
                    Aggressor::Buyer => "buy",
                    Aggressor::Seller => "sell",
                    Aggressor::Unknown => "unknown",
                };
                let rest = format!("{side} {} @ {}", self.qty(qty), self.px(px));
                self.line(at, "TRADE", &rest);
            }
            MdEvent::Health { feed, h, .. } => {
                let state = match h {
                    FeedHealth::Live => "live",
                    FeedHealth::Gap => "gap",
                    FeedHealth::Stale => "stale",
                    FeedHealth::Refused => "refused",
                };
                self.line(at, "HEALTH", &format!("{} {state}", self.feed(feed)));
                if matches!(feed, Feed::Book(_)) {
                    self.pending = Some((seq, at));
                }
            }
            MdEvent::BookSnapshotBegin { .. }
            | MdEvent::BookSnapshotEnd { .. }
            | MdEvent::Level { .. }
            | MdEvent::Window { .. }
            | MdEvent::SidePrune { .. }
            | MdEvent::SideClear { .. } => self.pending = Some((seq, at)),
            // A trading status (decision 0130) is a line of its own, never a book's.
            MdEvent::Status { status, .. } => {
                let status = match status {
                    TradingStatus::Trading => "trading",
                    TradingStatus::PostOnly => "post-only",
                    TradingStatus::ReduceOnly => "reduce-only",
                    TradingStatus::CancelOnly => "cancel-only",
                    TradingStatus::Halted => "halted",
                    TradingStatus::Delisted => "delisted",
                };
                self.line(at, "STATUS", status);
            }
            MdEvent::Mark { .. }
            | MdEvent::Index { .. }
            | MdEvent::Funding { .. }
            | MdEvent::Stats { .. } => {}
        }
    }

    fn on_end(&mut self, key: ConnKey) {
        self.flush_book();
        self.books.end_epoch(key);
        self.shown = None;
        // What the socket's codec counted, in full; the next connection's starts from none.
        if let Some(h) = &mut self.hibachi
            && key.conn == h.socket
        {
            let counts = h.tally.take();
            h.counts = HibachiMdCounts::default();
            self.line(now(), "COUNTS", &hibachi::counts_text(&counts));
        }
        let kind = if self.stopping.get() {
            "CLOSED"
        } else {
            "RECONNECT"
        };
        let l = &self.labels;
        let line = format!(
            "{kind} {} conn {} epoch {} ended",
            l.venue, key.conn, key.epoch
        );
        self.print(now(), &line);
    }
}

/// The wall clock now, for a line no frame stamped.
pub fn now() -> WallNs {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH);
    WallNs(since.map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)))
}

/// `HH:MM:SS.mmm`, UTC.
pub fn clock(at: WallNs) -> String {
    let ms = at.0.div_euclid(1_000_000);
    let day_ms = ms.rem_euclid(86_400_000);
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        day_ms / 3_600_000,
        day_ms / 60_000 % 60,
        day_ms / 1_000 % 60,
        day_ms % 1_000
    )
}

/// The handler a venue's sessions share; md_watch keeps a clone to print the last book when it
/// stops.
pub struct Shared(Rc<RefCell<Watcher>>);

impl MdHandler for Shared {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        self.0.borrow_mut().on_event(env);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.0.borrow_mut().on_end(key);
    }
}

/// One venue wired: its sessions and its handler. The control that stops them is returned
/// beside it.
struct Wired {
    venue: MdVenue<Shared>,
    watcher: Rc<RefCell<Watcher>>,
}

struct Plan {
    factory: &'static dyn VenueFactory,
    cfg: VenueConfig,
    specs: SpecTable,
    subs: Vec<Subscription>,
    conns: Range<u16>,
    silence: Duration,
    labels: Labels,
    /// The limiter a read the venue's wiring made first was charged to (Hibachi's contract
    /// read), handed to the venue's sessions in place of a fresh one, so a request of theirs that
    /// a declared limit counts would be counted beside the read; `None` for a fresh one. Hibachi
    /// declares one limit, on REST requests, and its market-data sessions make none, so for now
    /// they add nothing to the read's count.
    limiter: Option<RateLimiter>,
    /// A Hibachi contract's state, its codec's counts and its socket's connection number.
    hibachi: Option<(Listed, Tally, u16)>,
}

struct Shard<'a> {
    opts: &'a Options,
    clock: IngestClock,
    connector: Connector,
    stamps: bool,
    stopping: Rc<Cell<bool>>,
    out: Sink,
}

impl Shard<'_> {
    fn wire(&self, plan: Plan) -> Result<(Wired, MdVenueControl), String> {
        let venue = plan.labels.venue;
        let fail = |e: &dyn std::fmt::Display| format!("{venue}: {e}");
        let limiter = match plan.limiter {
            Some(limiter) => limiter,
            None => {
                let caps = plan.factory.caps(&plan.cfg).map_err(|e| fail(&e))?;
                RateLimiter::new(&caps.limits, reserve()).map_err(|e| fail(&e))?
            }
        };
        let config = MdVenueConfig {
            venue: plan.factory,
            cfg: plan.cfg,
            specs: plan.specs,
            connector: self.connector.clone(),
            pacing: pacing(),
            clock: self.clock.clone(),
            http_max_body: HTTP_MAX_BODY,
            conns: plan.conns,
            limiter,
            liveness: liveness(plan.silence),
            write_stall: write_stall(),
        };
        let mut watcher = Watcher::new(
            plan.labels,
            self.opts,
            self.stamps,
            self.stopping.clone(),
            self.out.clone(),
        );
        watcher.hibachi = plan.hibachi.map(|(listed, tally, socket)| HibachiShown {
            listed,
            state: None,
            tally,
            counts: HibachiMdCounts::default(),
            socket,
        });
        let watcher = Rc::new(RefCell::new(watcher));
        let (venue, control) =
            MdVenue::new(config, Shared(watcher.clone())).map_err(|e| fail(&e))?;
        control.set_desired(plan.subs).map_err(|e| fail(&e))?;
        Ok((Wired { venue, watcher }, control))
    }

    fn paradex(&self, market: &Market) -> Result<(Wired, MdVenueControl), String> {
        let factory = &ParadexFactory;
        let cfg = paradex_config(&self.opts.paradex_url);
        let specs = specs(factory, &cfg, PARADEX_INST, VenueId::new(1), market, "USD")
            .map_err(|e| format!("paradex: {e}"))?;
        let sub = |feed| Subscription {
            inst: PARADEX_INST,
            feed,
        };
        let touches = paradex_touches(self.opts.paradex_touch);
        let mut subs: Vec<_> = touches.iter().map(|(s, _)| sub(Feed::Touch(*s))).collect();
        subs.extend([sub(Feed::Trades), sub(Feed::Book(DELTAS))]);
        let names: Vec<_> = touches.iter().map(|(_, name)| *name).collect();
        self.wire(Plan {
            factory,
            cfg,
            specs,
            subs,
            conns: PARADEX_CONNS,
            silence: PARADEX_SILENCE,
            limiter: None,
            hibachi: None,
            labels: Labels {
                venue: "paradex",
                symbol: market.symbol.clone(),
                about: format!("{}, trades, deltas book", names.join(", ")),
                inst: PARADEX_INST,
                touches,
                book: DELTAS,
                book_name: "deltas",
                tick: market.tick,
                step: market.step,
            },
        })
    }

    fn binance(&self, market: &Market) -> Result<(Wired, MdVenueControl), String> {
        let factory = &BinanceUsdm;
        let cfg = binance_config(&self.opts.binance_ws, &self.opts.binance_rest);
        let specs = specs(factory, &cfg, BINANCE_INST, VenueId::new(2), market, "USDT")
            .map_err(|e| format!("binance: {e}"))?;
        let sub = |feed| Subscription {
            inst: BINANCE_INST,
            feed,
        };
        self.wire(Plan {
            factory,
            cfg,
            specs,
            subs: vec![
                sub(Feed::Touch(TOUCH_BOOK_TICKER)),
                sub(Feed::Book(BOOK_DIFF)),
            ],
            conns: BINANCE_CONNS,
            silence: BINANCE_SILENCE,
            limiter: None,
            hibachi: None,
            labels: Labels {
                venue: "binance",
                symbol: market.symbol.clone(),
                about: "bookTicker, diff-depth book".to_owned(),
                inst: BINANCE_INST,
                touches: vec![(TOUCH_BOOK_TICKER, "bookTicker")],
                book: BOOK_DIFF,
                book_name: "diff-depth",
                tick: market.tick,
                step: market.step,
            },
        })
    }

    /// Hibachi's contract, read from the venue first (`hibachi::read_contract`): its
    /// `ask_bid_price` touch, its trades and its orderbook at its own granularity (or
    /// `--hibachi-granularity`), decoded on its own tick and step.
    async fn hibachi(&self, opts: &HibachiOpts) -> Result<(Wired, MdVenueControl), String> {
        let plan = self.hibachi_plan(opts).await?;
        self.wire(plan)
    }

    /// What [`hibachi`](Shard::hibachi) wires: the contract read through the run's connector and
    /// charged to Hibachi's declared rate limit, on the limiter the plan then hands the sessions.
    async fn hibachi_plan(&self, opts: &HibachiOpts) -> Result<Plan, String> {
        let factory = hibachi::Counting::leaked();
        let cfg = hibachi::config(opts);
        let fail = |e: &dyn std::fmt::Display| format!("hibachi: {e}");
        let caps = factory.caps(&cfg).map_err(|e| fail(&e))?;
        let limiter = RateLimiter::new(&caps.limits, reserve()).map_err(|e| fail(&e))?;
        let read =
            hibachi::read_contract(factory, &cfg, opts, HIBACHI_INST, &self.connector, &limiter);
        let (spec, listed) = read.await.map_err(|e| fail(&e))?;
        let (tick, step) = (spec.price_grid.finest(), spec.size_step.get());
        let granularity = spec.quote_grid.unwrap_or(tick);
        let mut specs = SpecTable::new();
        specs.insert(spec);
        let sub = |feed| Subscription {
            inst: HIBACHI_INST,
            feed,
        };
        let subs = vec![
            sub(Feed::Touch(TOUCH_ASK_BID_PRICE)),
            sub(Feed::Trades),
            sub(Feed::Book(BOOK_ORDERBOOK)),
        ];
        // The venue opens its first plan's endpoints under its range's numbers in plan order,
        // and Hibachi plans its status poll beside the socket (decision 0130): the socket's
        // number is its place in that plan.
        let planned = factory
            .plan_md(&cfg, &specs, &subs.iter().copied().collect())
            .map_err(|e| fail(&e))?;
        let socket = planned
            .iter()
            .position(|ep| matches!(ep.transport, MdTransport::Socket { .. }))
            .and_then(|at| u16::try_from(at).ok())
            .map(|at| HIBACHI_CONNS.start + at)
            .ok_or_else(|| fail(&"no /ws/market socket planned"))?;
        Ok(Plan {
            factory,
            cfg,
            specs,
            subs,
            conns: HIBACHI_CONNS,
            silence: hibachi::SILENCE,
            limiter: Some(limiter),
            hibachi: Some((listed, factory.tally.clone(), socket)),
            labels: Labels {
                venue: "hibachi",
                symbol: opts.symbol.clone(),
                about: format!(
                    "ask_bid_price, trades, orderbook book of {} levels at granularity {}",
                    opts.depth,
                    granularity.normalize()
                ),
                inst: HIBACHI_INST,
                touches: vec![(TOUCH_ASK_BID_PRICE, "ask_bid_price")],
                book: BOOK_ORDERBOOK,
                book_name: "orderbook",
                tick,
                step,
            },
        })
    }
}

/// Runs a venue's sessions, if it was asked for, until its control is dropped.
async fn drive(wired: Option<&mut Wired>) -> Result<(), String> {
    let Some(w) = wired else {
        return Ok(());
    };
    let venue = w.watcher.borrow().labels.venue;
    w.venue.run().await.map_err(|e| format!("{venue}: {e}"))
}

/// Watches the markets `opts` names, printing to `out` (each line led by its UTC time when
/// `stamps`), until `stop` completes, a write to `out` fails or a venue's sessions fail; a
/// failure of the sessions, or a write error other than a closed pipe, is returned as text.
pub async fn run(
    opts: &Options,
    out: Out,
    stamps: bool,
    stop: impl Future<Output = ()>,
) -> Result<(), String> {
    let out = Sink {
        out,
        closed: Rc::new(Notify::new()),
        failed: Rc::new(RefCell::new(None)),
    };
    let shard = Shard {
        opts,
        clock: IngestClock::new(),
        connector: Connector::new(opts.proxy.clone()),
        stamps,
        stopping: Rc::new(Cell::new(false)),
        out: out.clone(),
    };
    let mut controls = Vec::new();
    let mut wire = |wired: Option<(Wired, MdVenueControl)>| {
        wired.map(|(w, control)| {
            controls.push(control);
            w
        })
    };
    let paradex = opts
        .paradex
        .as_ref()
        .map(|m| shard.paradex(m))
        .transpose()?;
    let binance = opts
        .binance
        .as_ref()
        .map(|m| shard.binance(m))
        .transpose()?;
    let hibachi = match &opts.hibachi {
        Some(h) => Some(shard.hibachi(h).await?),
        None => None,
    };
    let (mut paradex, mut binance, mut hibachi) = (wire(paradex), wire(binance), wire(hibachi));
    let watchers: Vec<_> = paradex
        .iter()
        .chain(&binance)
        .chain(&hibachi)
        .map(|w| w.watcher.clone())
        .collect();
    let watching: Vec<String> = watchers
        .iter()
        .map(|w| {
            let l = &w.borrow().labels;
            format!("{} {} ({})", l.venue, l.symbol, l.about)
        })
        .collect();
    out.write_line(format_args!(
        "md_watch: {}; market data only, no order is ever sent",
        watching.join(", ")
    ));
    for watcher in &watchers {
        watcher.borrow_mut().show_market(now());
    }
    let stopping = shard.stopping.clone();
    let closed = out.closed.clone();
    let stopper = async move {
        tokio::select! {
            () = stop => {}
            () = closed.notified() => {}
        }
        stopping.set(true);
        drop(controls);
        Ok::<(), String>(())
    };
    let running = async {
        tokio::try_join!(
            drive(paradex.as_mut()),
            drive(binance.as_mut()),
            drive(hibachi.as_mut()),
            stopper
        )
        .map(|_| ())
    };
    // Every session runs on this one thread and hands a frame's events on within one poll, so
    // the timer fires only between frames: a quiet market's last book is printed within
    // FLUSH_EVERY rather than when its next frame arrives.
    let flush = |watchers: &[Rc<RefCell<Watcher>>]| {
        for watcher in watchers {
            watcher.borrow_mut().flush();
        }
    };
    let flusher = async {
        loop {
            tokio::time::sleep(FLUSH_EVERY).await;
            flush(&watchers);
        }
    };
    let ran = tokio::select! {
        ran = running => ran,
        never = flusher => never,
    };
    flush(&watchers);
    ran?;
    out.error()
}

/// FBC-n35m (decision 0124): md_watch treats a prune or a clear of one side as an event of its
/// book, printing the book a frame holding only such events leaves. Run in tests/md_watch.rs,
/// which includes this file by path.
#[cfg(test)]
mod tests {
    use fbc_core::{BookSide, MonoNs, Stamp, VenueMeta};

    use super::*;

    const BOOK: BookId = BookId(0);

    fn options() -> Options {
        Options {
            paradex: None,
            paradex_touch: ParadexTouch::Bbo,
            binance: None,
            hibachi: None,
            proxy: fbc_runtime::ProxyConfig::Direct,
            seconds: None,
            top: 2,
            paradex_url: String::new(),
            binance_ws: String::new(),
            binance_rest: String::new(),
        }
    }

    fn sink(out: Out) -> Sink {
        Sink {
            out,
            closed: Rc::new(Notify::new()),
            failed: Rc::new(RefCell::new(None)),
        }
    }

    fn watcher(out: Out) -> Watcher {
        stamping(out, false)
    }

    /// A watcher on a toy market, its lines led by their times when `stamps`, as md_watch runs.
    fn stamping(out: Out, stamps: bool) -> Watcher {
        let labels = Labels {
            venue: "toy",
            symbol: "TOY-PERP".to_owned(),
            about: String::new(),
            inst: PARADEX_INST,
            touches: Vec::new(),
            book: BOOK,
            book_name: "book",
            tick: Decimal::ONE,
            step: Decimal::ONE,
        };
        let stopping = Rc::new(Cell::new(false));
        Watcher::new(labels, &options(), stamps, stopping, sink(out))
    }

    fn env(seq: u64, body: MdEvent) -> Envelope<MdEvent> {
        received(seq, WallNs(0), body)
    }

    /// `body`, as the runtime hands it on: the input `seq` of the shard, stamped `recv_wall`
    /// on receipt.
    fn received(seq: u64, recv_wall: WallNs, body: MdEvent) -> Envelope<MdEvent> {
        let stamp = Stamp {
            ingest_seq: seq,
            kernel_rx: None,
            recv_mono: MonoNs(0),
            recv_wall,
            conn: ConnKey { conn: 0, epoch: 0 },
        };
        Envelope::new(stamp, VenueMeta::NONE, body)
    }

    fn level(side: BookSide, px: i64) -> MdEvent {
        MdEvent::Level {
            inst: PARADEX_INST,
            book: BOOK,
            side,
            px: Ticks(px),
            qty: Lots::new(1).unwrap(),
        }
    }

    #[test]
    fn a_frame_of_only_side_prunes_or_clears_prints_the_book_it_leaves() {
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let mut w = watcher(buf.clone());
        let inst = PARADEX_INST;
        let snapshot = [
            MdEvent::BookSnapshotBegin {
                inst,
                book: BOOK,
                epoch: 1,
            },
            level(BookSide::Bid, 99),
            level(BookSide::Bid, 98),
            level(BookSide::Ask, 101),
            level(BookSide::Ask, 102),
            MdEvent::BookSnapshotEnd { inst, book: BOOK },
        ];
        for ev in snapshot {
            w.on_event(env(1, ev));
        }
        let prune = MdEvent::SidePrune {
            inst,
            book: BOOK,
            side: BookSide::Ask,
            lo: Ticks(101),
            hi: Ticks(101),
        };
        w.on_event(env(2, prune));
        let clear = MdEvent::SideClear {
            inst,
            book: BOOK,
            side: BookSide::Bid,
        };
        w.on_event(env(3, clear));
        w.flush();
        let text = String::from_utf8(buf.borrow().clone()).unwrap();
        let books: Vec<&str> = text.split("BOOK ").skip(1).collect();
        assert_eq!(
            books,
            [
                "toy TOY-PERP book bid 99 x 1 ask 101 x 1 mid 100 spread 200.00bps\n  \
                 1 bid 99 x 1 | ask 101 x 1\n  2 bid 98 x 1 | ask 102 x 1\n",
                "toy TOY-PERP book bid 99 x 1 ask 101 x 1 mid 100 spread 200.00bps\n  \
                 1 bid 99 x 1 | ask 101 x 1\n  2 bid 98 x 1 | ask -\n",
                "toy TOY-PERP book bid - ask 101 x 1 mid - spread -\n  1 bid - | ask 101 x 1\n",
            ],
            "{text}"
        );
    }

    /// FBC-9o7e: a Hibachi codec's counts are judged per connection. A kind is printed as it is
    /// first counted, all of them again at the connection's end before its `RECONNECT` line, and
    /// the next connection starts from none: the same kind counted there is printed again as
    /// first counted, though the connection before printed it already.
    #[test]
    fn a_hibachi_codecs_counts_are_printed_afresh_after_each_reconnect() {
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let mut w = watcher(buf.clone());
        let tally = Tally::default();
        w.hibachi = Some(HibachiShown {
            listed: Listed {
                status: TradingStatus::Trading,
                sessions: None,
            },
            state: None,
            tally: tally.clone(),
            counts: HibachiMdCounts::default(),
            socket: 0,
        });
        let one = HibachiMdCounts {
            unsized_touches: 1,
            ..HibachiMdCounts::default()
        };
        let two = HibachiMdCounts {
            unsized_touches: 2,
            ..HibachiMdCounts::default()
        };
        // The first connection: counted once, printed; counted again, the same kind, printed
        // only in full at the connection's end, which leaves its tally at none.
        tally.set(one);
        w.flush_at(WallNs(0));
        tally.set(two);
        w.flush_at(WallNs(1));
        w.on_end(ConnKey { conn: 0, epoch: 0 });
        assert_eq!(tally.get(), HibachiMdCounts::default());
        // The second connection's codec starts from none, and counts the same kind.
        tally.set(HibachiMdCounts::default());
        w.flush_at(WallNs(2));
        tally.set(one);
        w.flush_at(WallNs(3));
        let text = String::from_utf8(buf.borrow().clone()).unwrap();
        assert_eq!(
            text,
            "MARKET toy TOY-PERP open\n\
             COUNTS toy TOY-PERP unsized_touches 1\n\
             COUNTS toy TOY-PERP unsized_touches 2\n\
             RECONNECT toy conn 0 epoch 0 ended\n\
             COUNTS toy TOY-PERP unsized_touches 1\n"
        );
    }

    /// FBC-9o7e: a Hibachi contract's `MARKET` line follows its session calendar through the
    /// run, not only at its start: each flush judges the state at its own instant and prints it
    /// when it is not the one last printed. Here a contract listed trading, closed by its
    /// calendar from 1 000 ns until 2 000 ns, is flushed before, inside and after that window.
    #[test]
    fn a_hibachi_contracts_market_line_follows_its_calendar_at_each_flush() {
        use fbc_core::{CalendarParts, ClosedWindow, SessionCalendar};

        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let mut w = watcher(buf.clone());
        let closed = ClosedWindow::new(WallNs(1_000), WallNs(2_000)).unwrap();
        let sessions = SessionCalendar::new(CalendarParts {
            first_open: None,
            final_close: None,
            windows: vec![closed],
            weekly: Vec::new(),
        });
        w.hibachi = Some(HibachiShown {
            listed: Listed {
                status: TradingStatus::Trading,
                sessions,
            },
            state: None,
            tally: Tally::default(),
            counts: HibachiMdCounts::default(),
            socket: 0,
        });
        for at in [0, 999, 1_000, 1_999, 2_000, 5_000] {
            w.flush_at(WallNs(at));
        }
        let text = String::from_utf8(buf.borrow().clone()).unwrap();
        assert_eq!(
            text,
            "MARKET toy TOY-PERP open\n\
             MARKET toy TOY-PERP closed by its session calendar\n\
             MARKET toy TOY-PERP open\n"
        );
    }

    /// Reviewer A RA213-2: a line an input brought is led by the time the runtime stamped that
    /// input on receipt, `recv_wall`, not by the time md_watch prints it: a `TOUCH`, a `TRADE`
    /// and a `HEALTH` line at once, and a `BOOK` line when a later flush prints the book the
    /// snapshot left. Each input here was received on 1970-01-01, which no line printed now
    /// could be stamped with.
    #[test]
    fn a_line_an_input_brought_is_led_by_its_receive_time() {
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let mut w = stamping(buf.clone(), true);
        // 01:02:03.456, 01:02:04.001, 01:02:05.999 and 01:02:06.000 UTC on 1970-01-01.
        let at = |ms: i64| WallNs(ms * 1_000_000);
        let (touch, trade, health, book) =
            (at(3_723_456), at(3_724_001), at(3_725_999), at(3_726_000));
        let inst = PARADEX_INST;
        let lvl = |px| Lvl {
            px: Ticks(px),
            qty: Lots::new(1).unwrap(),
        };
        let source = TouchSourceId(0);
        let quote = MdEvent::Touch {
            inst,
            bid: Some(lvl(99)),
            ask: Some(lvl(101)),
            source,
        };
        w.on_event(received(1, touch, quote));
        let traded = MdEvent::Trade {
            inst,
            id: None,
            aggressor: Aggressor::Buyer,
            px: Ticks(100),
            qty: Lots::new(1).unwrap(),
        };
        w.on_event(received(2, trade, traded));
        let feed = Feed::Touch(source);
        let gap = MdEvent::Health {
            inst,
            feed,
            h: FeedHealth::Gap,
        };
        w.on_event(received(3, health, gap));
        let snapshot = [
            MdEvent::BookSnapshotBegin {
                inst,
                book: BOOK,
                epoch: 1,
            },
            level(BookSide::Bid, 99),
            level(BookSide::Ask, 101),
            MdEvent::BookSnapshotEnd { inst, book: BOOK },
        ];
        for ev in snapshot {
            w.on_event(received(4, book, ev));
        }
        w.flush();
        let text = String::from_utf8(buf.borrow().clone()).unwrap();
        assert_eq!(
            text,
            "01:02:03.456 TOUCH toy TOY-PERP touch bid 99 x 1 ask 101 x 1 mid 100 spread \
             200.00bps\n\
             01:02:04.001 TRADE toy TOY-PERP buy 1 @ 100\n\
             01:02:05.999 HEALTH toy TOY-PERP touch gap\n\
             01:02:06.000 BOOK toy TOY-PERP book bid 99 x 1 ask 101 x 1 mid 100 spread \
             200.00bps\n  1 bid 99 x 1 | ask 101 x 1\n"
        );
    }

    /// Reviewer A RA213-1: Hibachi's contract read is charged to Hibachi's declared rate limit
    /// on the limiter the plan then hands the venue's sessions, not on one of its own: the
    /// plan's limiter has the read's one request counted. (Hibachi's one declared limit counts
    /// REST requests, which its market-data sessions do not make, so nothing of theirs adds to
    /// that count yet.)
    #[tokio::test]
    async fn a_hibachi_contracts_read_is_charged_to_the_limiter_its_sessions_are_given() {
        use fbc_conformance::{HttpReply, HttpRoutes, StubServer, WsScript};
        use fbc_runtime::{BucketKey, ProxyConfig};

        let answer = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/hibachi/rest/exchange_info.json"
        );
        let body = std::fs::read(answer).unwrap();
        let reply = HttpReply { status: 200, body };
        let routes = HttpRoutes::from([("/market/exchange-info".to_owned(), reply)]);
        let stub = StubServer::start(WsScript::new(Vec::new()), routes)
            .await
            .unwrap();
        let contract = HibachiOpts {
            symbol: "ETH/USDT-P".to_owned(),
            rest: stub.http_url(""),
            ws: stub.ws_url(""),
            timeout_ms: 5_000,
            depth: 3,
            granularity: None,
            retry_ms: 2_000,
            status_s: 60,
            auction_halt_s: 900,
        };
        let opts = Options {
            hibachi: Some(contract.clone()),
            ..options()
        };
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let shard = Shard {
            opts: &opts,
            clock: IngestClock::new(),
            connector: Connector::new(ProxyConfig::Direct),
            stamps: false,
            stopping: Rc::new(Cell::new(false)),
            out: sink(buf.clone()),
        };
        let plan = shard.hibachi_plan(&contract).await.unwrap();
        assert_eq!(stub.http_requests(), ["GET /market/exchange-info"]);
        let limiter = plan
            .limiter
            .as_ref()
            .expect("the read's limiter, for the sessions");
        let now = tokio::time::Instant::now();
        assert_eq!(limiter.used(now, 0, BucketKey::Shared), 1);
    }

    /// FBC-1632 (decision 0130): a trading status prints a line of its own and leaves the book
    /// as it was: no book line follows it.
    #[test]
    fn a_trading_status_prints_a_status_line_and_no_book() {
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let mut w = watcher(buf.clone());
        let inst = PARADEX_INST;
        let snapshot = [
            MdEvent::BookSnapshotBegin {
                inst,
                book: BOOK,
                epoch: 1,
            },
            level(BookSide::Bid, 99),
            level(BookSide::Ask, 101),
            MdEvent::BookSnapshotEnd { inst, book: BOOK },
        ];
        for ev in snapshot {
            w.on_event(env(1, ev));
        }
        w.flush();
        for (seq, status) in [(2, TradingStatus::Halted), (3, TradingStatus::Trading)] {
            w.on_event(env(seq, MdEvent::Status { inst, status }));
        }
        w.flush();
        let text = String::from_utf8(buf.borrow().clone()).unwrap();
        let lines: Vec<&str> = text.lines().filter(|l| !l.starts_with("  ")).collect();
        assert_eq!(lines.len(), 3, "{text}");
        assert!(
            lines[0].contains("BOOK toy TOY-PERP book bid 99 x 1"),
            "{text}"
        );
        assert!(lines[1].ends_with("STATUS toy TOY-PERP halted"), "{text}");
        assert!(lines[2].ends_with("STATUS toy TOY-PERP trading"), "{text}");
    }
}
