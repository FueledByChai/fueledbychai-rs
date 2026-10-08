//! md_watch's wiring: one fbc-runtime `MdVenue` per venue asked for, each with the venue's
//! factory, a configuration and spec table built from the command line, and explicit
//! reconnect pacing, liveness and write-stall settings; a handler per venue that keeps the
//! books (`MdBooks`, one fbc-book book per channel) and prints a line per update. No order is
//! built or sent, and no credential is read: both venues' market data is public.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io::Write;
use std::ops::Range;
use std::rc::Rc;
use std::time::Duration;

use fbc_core::{
    Aggressor, AssetSym, BookId, ConnKey, Envelope, Feed, FeedHealth, FundingSpec, InstrumentId,
    InstrumentKind, InstrumentSpec, Lots, Lvl, MdEvent, PriceGrid, SizeStep, SpecTable,
    Subscription, Ticks, TouchSourceId, TradingStatus, UnderlyingId, VenueConfig, VenueFactory,
    VenueId, WallNs, dispatch_market_data,
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
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::MD_URL;
use fbc_venue_paradex::md::{BBO, BBO_INTERACTIVE, DELTAS};
use rust_decimal::Decimal;
use tokio::sync::Notify;

use crate::args::{Market, Options, ParadexTouch};

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

/// The Paradex market's and the Binance symbol's instrument ids; each venue has its own spec
/// table, and the ids differ so a line can never be printed under the wrong one.
pub const PARADEX_INST: InstrumentId = InstrumentId::new(1);
pub const BINANCE_INST: InstrumentId = InstrumentId::new(2);

/// Each venue's connection numbers, disjoint on the one ingest clock.
const PARADEX_CONNS: Range<u16> = 0..64;
const BINANCE_CONNS: Range<u16> = 64..128;

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
    inst: InstrumentId,
    /// Each touch source subscribed, and its channel name.
    touches: Vec<(TouchSourceId, &'static str)>,
    book: BookId,
    book_name: &'static str,
    tick: Decimal,
    step: Decimal,
}

/// The handler of one venue: keeps its books and prints a line per update.
pub struct Watcher {
    labels: Labels,
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

    /// Prints the book as the last frame left it, when that changed what is shown.
    pub fn flush(&mut self) {
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
            | MdEvent::Window { .. } => self.pending = Some((seq, at)),
            MdEvent::Mark { .. }
            | MdEvent::Index { .. }
            | MdEvent::Funding { .. }
            | MdEvent::Stats { .. } => {}
        }
    }

    fn on_end(&mut self, key: ConnKey) {
        self.flush();
        self.books.end_epoch(key);
        self.shown = None;
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
fn now() -> WallNs {
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
        let caps = plan.factory.caps(&plan.cfg).map_err(|e| fail(&e))?;
        let limiter = RateLimiter::new(&caps.limits, reserve()).map_err(|e| fail(&e))?;
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
        let watcher = Watcher::new(
            plan.labels,
            self.opts,
            self.stamps,
            self.stopping.clone(),
            self.out.clone(),
        );
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
        self.wire(Plan {
            factory,
            cfg,
            specs,
            subs,
            conns: PARADEX_CONNS,
            silence: PARADEX_SILENCE,
            labels: Labels {
                venue: "paradex",
                symbol: market.symbol.clone(),
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
            labels: Labels {
                venue: "binance",
                symbol: market.symbol.clone(),
                inst: BINANCE_INST,
                touches: vec![(TOUCH_BOOK_TICKER, "bookTicker")],
                book: BOOK_DIFF,
                book_name: "diff-depth",
                tick: market.tick,
                step: market.step,
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
    let (mut paradex, mut binance) = (wire(paradex), wire(binance));
    let mut watching = Vec::new();
    if let Some(m) = &opts.paradex {
        let touches = paradex_touches(opts.paradex_touch);
        let names: Vec<_> = touches.iter().map(|(_, name)| *name).collect();
        watching.push(format!(
            "paradex {} ({}, trades, deltas book)",
            m.symbol,
            names.join(", ")
        ));
    }
    if let Some(m) = &opts.binance {
        watching.push(format!(
            "binance {} (bookTicker, diff-depth book)",
            m.symbol
        ));
    }
    out.write_line(format_args!(
        "md_watch: {}; market data only, no order is ever sent",
        watching.join(", ")
    ));
    let watchers: Vec<_> = paradex
        .iter()
        .chain(&binance)
        .map(|w| w.watcher.clone())
        .collect();
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
        tokio::try_join!(drive(paradex.as_mut()), drive(binance.as_mut()), stopper).map(|_| ())
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
