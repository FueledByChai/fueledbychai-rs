//! md_watch's Hibachi pieces (FBC-9o7e): its configuration, the contract read from
//! `GET /market/exchange-info` before anything subscribes, the market's open or closed state,
//! and the codec's counts made visible.
//!
//! Hibachi's orderbook is subscribed at the contract's own granularity,
//! `quote_grid.unwrap_or(tick)` (decision 0129), and its levels are refused off its tick, so
//! md_watch reads the contract the way a consumer does, through `fbc-venue-hibachi`'s discovery
//! and `fbc-core`'s resolver, rather than decoding it on the finest grid it takes for Paradex and
//! Binance by default: a granularity Hibachi does not list would gap the book on every frame, the
//! very sign the owner's first run reads as the venue ignoring the subscription's fields.
//!
//! What the codec counts (a frame whose `granularity` or `depth` is not the subscription's, an
//! `ask_bid_price` frame with no sizes) reaches a consumer only through the concrete
//! [`HibachiMd`] (decision 0129, item 10), while the runtime holds each codec as a
//! `Box<dyn MdCodec>`. So md_watch hands the runtime [`Counting`], `Hibachi` itself in every call
//! but `md_codec`, whose socket codec copies its counts into a [`Tally`] md_watch reads after each
//! call. Hibachi's plan also has a status poll (decision 0130), whose codec md_watch takes as
//! `Hibachi` builds it: its trading statuses reach md_watch as `MdEvent::Status`.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use fbc_core::{
    AccountSummary, AliasTable, AssetKey, ConfigError, ConnKey, DecodeError, DecodeScope, Effect,
    Effects, EndpointPlan, ExecCodec, ExecEndpoint, FieldSpec, HttpAnswer, HttpFailure, HttpMethod,
    HttpPlan, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId, InstrumentResolver,
    InstrumentSpec, InstrumentSpecDraft, Keepalive, Listing, MdCodec, MdSink, MdTransport, MonoNs,
    RawFrame, Secrets, SessionCalendar, SpecTable, Subscription, SymbolError, TimerTag,
    TradingStatus, UnderlyingId, VenueCaps, VenueConfig, VenueError, VenueFactory, VenueId, WallNs,
    dispatch_market_data,
};
use fbc_runtime::http::{Bytes, Method, Request};
use fbc_runtime::{Connector, RateLimiter};
use fbc_venue_hibachi::{
    Hibachi, HibachiMd, HibachiMdCounts, KEY_AUCTION_HALT, KEY_DATA_REST_URL, KEY_DATA_WS_URL,
    KEY_ORDERBOOK_DEPTH, KEY_ORDERBOOK_RESUBSCRIBE, KEY_REST_TIMEOUT, KEY_STATUS_INTERVAL,
};
use rust_decimal::Decimal;

use crate::args::HibachiOpts;
use crate::watch::now;

/// The Hibachi configuration from the command line: its two origins, the REST read's timeout,
/// the orderbook depth, the resubscribe interval, the status poll's interval and the window after
/// a reopening, every key `fbc-venue-hibachi` requires.
pub fn config(opts: &HibachiOpts) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (KEY_DATA_REST_URL, opts.rest.clone()),
        (KEY_REST_TIMEOUT, format!("{}ms", opts.timeout_ms)),
        (KEY_ORDERBOOK_DEPTH, opts.depth.to_string()),
        (KEY_DATA_WS_URL, opts.ws.clone()),
        (KEY_ORDERBOOK_RESUBSCRIBE, format!("{}ms", opts.retry_ms)),
        (KEY_STATUS_INTERVAL, format!("{}s", opts.status_s)),
        (KEY_AUCTION_HALT, format!("{}s", opts.auction_halt_s)),
    ] {
        cfg.insert(key, &value);
    }
    cfg
}

/// The counts of the current connection's codec, as its last call left them; set back to
/// none when md_watch prints them at the connection's end ([`Tally::take`]), and by the next
/// connection's codec.
#[derive(Clone, Default)]
pub struct Tally(Arc<Mutex<HibachiMdCounts>>);

impl Tally {
    fn lock(&self) -> MutexGuard<'_, HibachiMdCounts> {
        // A poisoned lock still holds a usable count: nothing panics while holding it.
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The counts now.
    pub fn get(&self) -> HibachiMdCounts {
        *self.lock()
    }

    /// The counts now, leaving none.
    pub fn take(&self) -> HibachiMdCounts {
        std::mem::take(&mut *self.lock())
    }

    /// Sets the counts: the current codec's, after each of its calls.
    pub fn set(&self, counts: HibachiMdCounts) {
        *self.lock() = counts;
    }
}

/// `Hibachi`, its codecs' counts kept in a [`Tally`].
pub struct Counting {
    pub tally: Tally,
}

impl Counting {
    /// A factory the runtime can hold, which takes a `'static` one: leaked, once per run.
    pub fn leaked() -> &'static Counting {
        Box::leak(Box::new(Counting {
            tally: Tally::default(),
        }))
    }
}

impl VenueFactory for Counting {
    fn id(&self) -> &'static str {
        Hibachi.id()
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        Hibachi.config_schema()
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Hibachi.caps(cfg)
    }

    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        Hibachi.parse_fbc_common_symbol(s)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Hibachi.discover(cfg)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        Hibachi.plan_md(cfg, specs, subs)
    }

    /// `Hibachi`'s codec, as `Hibachi::md_codec` builds it: the status poll's (decision 0130)
    /// as it is, and the socket's with its counts copied out after every call; a new
    /// connection's socket codec starts the tally from none.
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        if let MdTransport::Poll { .. } = ep.transport {
            return Hibachi.md_codec(cfg, ep);
        }
        self.tally.set(HibachiMdCounts::default());
        Box::new(Counted {
            inner: HibachiMd::new(ep.stream, cfg),
            tally: self.tally.clone(),
        })
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Hibachi.plan_exec(cfg)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Hibachi.exec_codec(cfg, creds)
    }

    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        Hibachi.test_connection(cfg, creds)
    }
}

/// A [`HibachiMd`] whose counts are copied into its [`Tally`] after each call.
struct Counted {
    inner: HibachiMd,
    tally: Tally,
}

impl Counted {
    fn copied<R>(&mut self, call: impl FnOnce(&mut HibachiMd) -> R) -> R {
        let r = call(&mut self.inner);
        self.tally.set(self.inner.counts());
        r
    }
}

impl MdCodec for Counted {
    fn on_open(&mut self, fx: &mut Effects) {
        self.copied(|c| c.on_open(fx));
    }

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        self.copied(|c| c.subscribe(add, remove, specs, fx))
    }

    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.copied(|c| c.on_frame(f, scope, specs, sink, fx))
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        self.copied(|c| c.on_http(tag, resp, scope, specs, sink, fx))
    }

    fn on_timer(
        &mut self,
        tag: TimerTag,
        now: MonoNs,
        wall: WallNs,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        self.copied(|c| c.on_timer(tag, now, wall, sink, fx));
    }

    fn keepalive(&self) -> Option<Keepalive> {
        self.inner.keepalive()
    }

    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        self.inner.redact_inbound(input)
    }
}

/// Each counter that is not zero, by its name in [`HibachiMdCounts`], in its order there.
pub fn counted(c: &HibachiMdCounts) -> Vec<(&'static str, u64)> {
    // Every field named, so a counter the codec adds fails to compile here until it is printed.
    let HibachiMdCounts {
        null_sides,
        malformed_books,
        param_mismatches,
        off_granularity,
        empty_snapshots,
        bounds_failures,
        thin_sides,
        locked_books,
        updates_without_book,
        resubscribes,
        reconnects,
        unsized_touches,
        crossed_touches,
        malformed_touches,
        acks,
        unsubscribed_frames,
    } = *c;
    [
        ("null_sides", null_sides),
        ("malformed_books", malformed_books),
        ("param_mismatches", param_mismatches),
        ("off_granularity", off_granularity),
        ("empty_snapshots", empty_snapshots),
        ("bounds_failures", bounds_failures),
        ("thin_sides", thin_sides),
        ("locked_books", locked_books),
        ("updates_without_book", updates_without_book),
        ("resubscribes", resubscribes),
        ("reconnects", reconnects),
        ("unsized_touches", unsized_touches),
        ("crossed_touches", crossed_touches),
        ("malformed_touches", malformed_touches),
        ("acks", acks),
        ("unsubscribed_frames", unsubscribed_frames),
    ]
    .into_iter()
    .filter(|(_, n)| *n > 0)
    .collect()
}

/// `<counter> <n> ...`, or `none`.
pub fn counts_text(c: &HibachiMdCounts) -> String {
    let named = counted(c);
    if named.is_empty() {
        return "none".to_owned();
    }
    let parts: Vec<String> = named
        .iter()
        .map(|(name, n)| format!("{name} {n}"))
        .collect();
    parts.join(" ")
}

/// Whether `now` counts a counter that `shown` had at zero: a kind of thing the codec had not
/// counted before on this connection.
pub fn newly_counted(shown: &HibachiMdCounts, now: &HibachiMdCounts) -> bool {
    let before: Vec<&str> = counted(shown).into_iter().map(|(name, _)| name).collect();
    counted(now).iter().any(|(name, _)| !before.contains(name))
}

/// What the contract's read said of its state: its trading status, and the session calendar
/// that closes it (none for a contract that trades around the clock).
#[derive(Clone, Debug)]
pub struct Listed {
    pub status: TradingStatus,
    pub sessions: Option<SessionCalendar>,
}

impl Listed {
    /// `open`, or `closed` and why, at `at`: a contract Hibachi listed as anything but trading
    /// (`PAUSED` for an FX weekend, an exchange in maintenance) is closed by its status, and one
    /// it listed trading is closed while its session calendar says so (an FX pair from Friday
    /// 17:00 to Sunday 17:00 New York time). The status is the start read's, and holds both ways
    /// here: a contract listed closed then reads closed all run, even once Hibachi reopens it,
    /// and a halt Hibachi lists later does not close it. Each change the status poll reads after
    /// it (FBC-1632, decision 0130) is a `STATUS` line of its own.
    pub fn state(&self, at: WallNs) -> String {
        match self.status {
            TradingStatus::Trading => match &self.sessions {
                Some(calendar) if !calendar.is_open(at) => {
                    "closed by its session calendar".to_owned()
                }
                _ => "open".to_owned(),
            },
            other => format!("closed: listed {other:?}"),
        }
    }
}

/// The venue id md_watch's Hibachi spec table is under.
pub const VENUE: VenueId = VenueId::new(3);

/// The connection key discovery's request is charged under: Hibachi declares no
/// per-connection limit, and an HTTP request is on no connection.
const NO_CONN: ConnKey = ConnKey {
    conn: u16::MAX,
    epoch: 0,
};

/// A contract's read answer can list every contract Hibachi has: 1 MiB is far above it.
const MAX_BODY: usize = 1 << 20;

/// What a request of the contract's read was answered with: its tag, the status and the body.
pub type Answer = (HttpTag, u16, Bytes);

/// What every refusal of the contract's read starts with.
const WHAT: &str = "GET /market/exchange-info";

/// The contract `opts.symbol` names, read as a consumer reads it: `fbc-venue-hibachi`'s
/// discovery plan (one public `GET /market/exchange-info`, no header, no credential) made by
/// [`fetch`], and its answer resolved by [`contract`] into `inst`'s spec. With
/// `opts.granularity`, the orderbook is subscribed at that granularity in place of the
/// contract's own. Refused, saying why, when the request fails, the answer is refused, or the
/// contract is not listed.
pub async fn read_contract(
    factory: &dyn VenueFactory,
    cfg: &VenueConfig,
    opts: &HibachiOpts,
    inst: InstrumentId,
    connector: &Connector,
    limiter: &RateLimiter,
) -> Result<(InstrumentSpec, Listed), String> {
    let plan = factory.discover(cfg).map_err(|e| format!("{WHAT}: {e}"))?;
    let caps = factory.caps(cfg).map_err(|e| format!("{WHAT}: {e}"))?;
    let answers = fetch(plan.requests(), connector, limiter).await?;
    contract(plan, &caps, &answers, opts, inst)
}

/// Makes a discovery plan's `requests`, in order, each charged to `limiter` and then sent
/// through `connector` within its timeout. md_watch makes only a bare public GET: a request with
/// a header, a body or another method is refused, as is one `limiter` has no room for, before
/// it is charged or sent.
pub async fn fetch(
    requests: &[Effect],
    connector: &Connector,
    limiter: &RateLimiter,
) -> Result<Vec<Answer>, String> {
    let mut answers = Vec::new();
    for effect in requests {
        let Effect::Http {
            tag, req, timeout, ..
        } = effect
        else {
            return Err(format!("{WHAT}: the plan asks for something not HTTP"));
        };
        // A public read: md_watch never sends a header, a body or anything but a GET.
        let public = req.method == HttpMethod::Get && req.headers.is_empty();
        if !public || !req.body.bytes().is_empty() {
            return Err(format!(
                "{WHAT}: the plan asks for a request md_watch does not make"
            ));
        }
        let charge = fbc_runtime::Request::of(effect).expect("an HTTP request is charged");
        limiter
            .charge(tokio::time::Instant::now(), NO_CONN, &[charge])
            .map_err(|_| format!("{WHAT}: refused by Hibachi's declared rate limit"))?;
        let request = Request::builder()
            .method(Method::GET)
            .uri(req.url.as_str())
            .body(Bytes::new())
            .map_err(|e| format!("{WHAT}: {e}"))?;
        let response = tokio::time::timeout(*timeout, connector.http(request, MAX_BODY))
            .await
            .map_err(|_| format!("{WHAT}: no answer within {} ms", timeout.as_millis()))?
            .map_err(|e| format!("{WHAT}: {e}"))?;
        answers.push((*tag, response.status().as_u16(), response.into_body()));
    }
    Ok(answers)
}

/// The contract `opts.symbol` names in `plan`'s `answers`: parsed in a decode scope, and the
/// draft resolved into `inst`'s spec, its orderbook granularity `opts.granularity` when given.
/// Refused, saying why, when an answer is refused, the contract is not listed, or the
/// granularity given is not a whole number of the contract's ticks.
pub fn contract(
    plan: HttpPlan<Vec<InstrumentSpecDraft>>,
    caps: &VenueCaps,
    answers: &[Answer],
    opts: &HibachiOpts,
    inst: InstrumentId,
) -> Result<(InstrumentSpec, Listed), String> {
    let answers: Vec<HttpAnswer<'_>> = answers
        .iter()
        .map(|(tag, status, body)| {
            let response = HttpResponse {
                status: *status,
                headers: &[],
                body,
            };
            (*tag, Ok(response))
        })
        .collect();
    let drafts = dispatch_market_data(caps, |scope| plan.parse(&answers, scope))
        .map_err(|e| format!("{WHAT}: {e}"))?
        .done()
        .ok_or_else(|| format!("{WHAT}: the answer asked for another round"))?;
    let symbol = &opts.symbol;
    let mut draft = drafts
        .into_iter()
        .find(|d| d.venue_symbol.as_wire() == symbol)
        .ok_or_else(|| format!("{symbol}: not a contract Hibachi lists"))?;
    if let Some(granularity) = opts.granularity {
        // A level is refused off the tick as well as off the granularity, so a granularity that
        // is not a whole number of ticks would gap every frame for a reason of md_watch's own.
        // The remainder is the codec's own check, and exact: no granularity overflows it.
        let tick = draft.price_grid.finest();
        if granularity.checked_rem(tick) != Some(Decimal::ZERO) {
            return Err(format!(
                "--hibachi-granularity {granularity}: not a whole number of {symbol}'s tick, \
                 {tick}"
            ));
        }
        // Asked for in its shortest spelling (0.10 as 0.1), and as no quote grid at the tick
        // itself, as `fbc-core` defines `quote_grid` (the book on the acceptance grid).
        draft.quote_grid = (granularity != tick).then(|| granularity.normalize());
    }
    let listed = Listed {
        status: draft.status,
        sessions: draft.sessions.clone(),
    };
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    let listing = Listing {
        id: inst,
        underlying: UnderlyingId::new(inst.get()),
    };
    resolver
        .list(VENUE, draft.asset, listing)
        .map_err(|e| format!("{symbol}: {e:?}"))?;
    let spec = resolver
        .spec(VENUE, draft, 1, now())
        .map_err(|e| format!("{symbol}: {e:?}"))?;
    Ok((spec, listed))
}

/// A stream that hears nothing for longer than this is reported stale and reconnected. Hibachi
/// documents no ping for `/ws/market`, while its orderbook is sent on a steady venue tick even
/// when nothing changed (FBC-2kmp's notes, from a live contract), so half a minute of silence on
/// a subscribed book is a dead stream. Whether a closed contract's feed (an FX pair over its
/// weekend, an exchange in maintenance) stays as live is not known: should it fall silent, this
/// reports it stale and reconnects it while its `MARKET` line says closed, which the owner's
/// first run over a closure tells (the crate's README).
pub const SILENCE: Duration = Duration::from_secs(30);
