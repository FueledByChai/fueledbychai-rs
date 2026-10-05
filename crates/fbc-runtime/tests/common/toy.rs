//! A toy market-data venue for the runtime's session tests (FBC-ku8, FBC-klr, FBC-nij):
//! `exec: None`, trades and two book channels, a text protocol of one `kind|key=value|...`
//! record per frame. It describes no real venue.
//!
//! Plan: trades only (a session test names its book subscriptions in its own plan), at most two subscriptions per socket endpoint in instrument order; the
//! `n`th endpoint is `StreamId(n)` at the configuration's `toy.url.<n>`.
//! Out: `hello|codec=<n>|plan=<ids>` on open (`n` counts the codecs its factory built, `ids`
//! are the instruments of the plan it was built for), and
//! `sub|add=A,B|remove=C` per subscribe call (empty parts left out).
//! In: `trade|sym=A|px=<ticks>|qty=<lots>|seq=<n>` is a trade; `arm|sym=A|ms=<n>` sets a timer
//! whose firing reports the instrument's trades stale; `big|kb=<n>` asks for an `n` KiB frame;
//! `bye` asks for a reconnect; `say` asks to send `said`; `get|tag=<n>|ms=<t>|url=<u>` asks for
//! a GET of `u` with a `t` ms timeout (and, with `|kb=<k>`, then a `k` KiB frame, and with
//! `|bye=1`, then a reconnect; with `|auth=1`, it carries an `Authorization` header); `get` with `ms=max` asks for a timeout past the end of the clock;
//! `odd` asks for a frame and a reconnect on another stream, which a session refuses;
//! `cancel|id=<n>` asks to send the cancel-shaped frame `cancel|id=<n>` as Safety traffic (FBC-f3w:
//! the runtime treats a write by its class, not its content); `auth` asks to send
//! `auth|key=toy-secret` with the key marked as a redaction span, as a codec sends a credential;
//! `bauth` asks to send [`BINARY_AUTH`] as a binary frame, its key (the only bytes that are not
//! UTF-8) marked as a redaction span (FBC-q7b).
//! Book channel `b` of instrument `A`: `begin|sym=A|book=<b>|epoch=<e>|seq=<n>` begins a
//! snapshot and anchors the channel's sequence; `lvl|sym=A|book=<b>|side=bid|px=<ticks>|qty=<lots>|seq=<n>`
//! sets a level (in the snapshot or as a delta) and `end|sym=A|book=<b>|seq=<n>` ends the
//! snapshot, each only when its seq is the channel's last plus one: any other seq reports a gap
//! on that channel, and until its next `begin` the channel's records are dropped.
//! Anything else, and every binary frame, is malformed.
//!
//! Rate charges (FBC-bel): `hello` is one `Control` unit, and each `sub` one `Subscribe` unit,
//! counted against its instrument when the call names one. `say` and `get` take optional
//! `op=<place|cancel|rest|control>`, `sym=<A>`, `w=<weight>` and `class=safety` fields for
//! theirs (by default one `Control` unit of normal traffic, no instrument), and `say` with
//! `id=<x>` sends `said|id=<x>`. The venue declares the limits it is built with
//! ([`ToyVenue::with_limits`]), none by default.
//!
//! Every `on_http` is logged as `<codec>/<tag>:<status>:<x-toy header>` or
//! `<codec>/<tag>:<failure>`, and a response body's lines are read as frames. On a poll
//! endpoint the codec sends nothing: its first subscribe asks for
//! `<base_url>/poll?syms=<subscribed>` at once, and each answer sets a 10 ms timer whose
//! firing asks again.
//!
//! FBC-djl: the configuration's `toy.lifetime_ms` declares that many milliseconds as the
//! venue's `max_conn_lifetime`, and `toy.keepalive` (`ping:<ms>` or `frame:<ms>`) gives its codec
//! a WebSocket ping, or the frame `ka`, every `<ms>` milliseconds.
//!
//! FBC-53c: `ack|sym=A` acknowledges instrument `A`'s subscription, as a venue does. The first
//! acknowledgement a codec reads marks `A` live and, when the configuration's `toy.snapshot`
//! names a base URL, asks for a GET of `<base>/A` (tag: the instrument's id, timeout 1 s), whose
//! body is read like any response; a repeated acknowledgement asks for nothing, until a
//! subscribe call removes `A`.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbc_core::{
    Aggressor, AssetKey, AssetSym, BookCaps, BookId, BookSide, Cadence, Channel, ConfigError,
    ConnTopology, Continuity, DecodeError, DecodeScope, Effect, Effects, Encoding, EndpointPlan,
    ExchTsKind, ExecCodec, ExecEndpoint, Feed, FeedHealth, FeedSource, FieldSpec, FundingCaps,
    FundingSpec, Header, HttpFailure, HttpMethod, HttpPlan, HttpRequest, HttpResponse, HttpTag,
    Inbound, InboundSpans, InstrumentId, InstrumentKind, InstrumentSpec, InstrumentSpecDraft,
    Keepalive, KeepaliveKind, Lots, MatchingCaps, MdCaps, MdCodec, MdEvent, MdSink, MdTransport,
    MonoNs, OpKind, PriceGrid, QueueModelQuality, RateCharge, RateLimit, RawFrame, Readiness,
    SizeStep, SpecTable, StpScope, StreamId, Subscription, SymbolError, TagSet, Ticks, TimerTag,
    TradeCaps, TradingStatus, TrafficClass, UnderlyingId, VenueCaps, VenueConfig, VenueError,
    VenueFactory, VenueId, VenueMeta, WallNs, WireSlice, WireUrl, dispatch_market_data,
};
use fbc_runtime::{RateLimiter, SafetyReserve};
use rust_decimal::Decimal;

/// The toy's one stream.
pub const STREAM: StreamId = StreamId(0);
/// The configuration key that makes the toy refuse its configuration.
pub const REFUSE: &str = "toy.refuse";
/// The configuration key that makes the toy's subscribe calls send one frame per instrument,
/// each of the weight it names, or of one.
pub const SPLIT: &str = "toy.split";
/// The configuration key that makes the toy name every endpoint of a plan stream 0.
pub const ONE_STREAM: &str = "toy.one_stream";
/// The configuration key that declares the venue's connection lifetime, in milliseconds.
pub const LIFETIME_MS: &str = "toy.lifetime_ms";
/// The configuration key that gives the codec a keepalive: `ping:<ms>` or `frame:<ms>`.
pub const KEEPALIVE: &str = "toy.keepalive";
/// The configuration key that names the base URL an acknowledged subscription's snapshot is
/// asked for under.
pub const SNAPSHOT: &str = "toy.snapshot";
/// The toy's instruments, by id: 1 is `A`, 2 is `B`, 3 is `C`.
const SYMBOLS: [&str; 3] = ["A", "B", "C"];

/// The toy venue; it counts the codecs it builds, keeps the instruments of the plan each was
/// built for, and logs their `on_http` calls.
#[derive(Default)]
pub struct ToyVenue {
    codecs: AtomicU32,
    plans: Mutex<Vec<Vec<u32>>>,
    http: Arc<Mutex<Vec<String>>>,
    subscribes: Arc<AtomicU32>,
    snapshots: Arc<AtomicU32>,
    limits: Vec<RateLimit>,
}

impl ToyVenue {
    /// A venue of the test's own, with its codec count at zero.
    pub fn leak() -> &'static ToyVenue {
        Box::leak(Box::default())
    }

    /// A venue of the test's own that declares `limits`.
    pub fn with_limits(limits: Vec<RateLimit>) -> &'static ToyVenue {
        Box::leak(Box::new(ToyVenue {
            limits,
            ..ToyVenue::default()
        }))
    }

    /// A limiter for its limits, keeping `percent` of each bucket for safety traffic.
    pub fn limiter(&self, percent: u8) -> RateLimiter {
        let reserve = SafetyReserve::percent(percent).unwrap();
        RateLimiter::new(&self.limits, reserve).unwrap()
    }

    /// How many codecs it has built.
    pub fn codecs(&self) -> u32 {
        self.codecs.load(Ordering::SeqCst)
    }

    /// How many subscribe calls its codecs had.
    pub fn subscribe_calls(&self) -> u32 {
        self.subscribes.load(Ordering::SeqCst)
    }

    /// How many snapshots its codecs asked for on an acknowledgement.
    pub fn snapshots(&self) -> u32 {
        self.snapshots.load(Ordering::SeqCst)
    }

    /// The instruments of the plan each codec was built for, in the order it built them.
    pub fn plans(&self) -> Vec<Vec<u32>> {
        self.plans.lock().unwrap().clone()
    }

    /// Every `on_http` call its codecs had, in order.
    pub fn http_log(&self) -> Vec<String> {
        self.http.lock().unwrap().clone()
    }
}

/// Subscriptions per socket endpoint in the toy's plan.
const PER_SOCKET: usize = 2;
/// The poll codec's timer.
const POLL: TimerTag = TimerTag(0);

/// Trades on instrument `inst`.
pub fn sub(inst: u32) -> Subscription {
    Subscription {
        inst: InstrumentId::new(inst),
        feed: Feed::Trades,
    }
}

/// Book channel `book` of instrument `inst`.
pub fn book(inst: u32, book: BookId) -> Subscription {
    Subscription {
        inst: InstrumentId::new(inst),
        feed: Feed::Book(book),
    }
}

/// One of the toy's two book channels: sequenced plus one, no window, no REST anchor.
fn book_caps(channel: &'static str) -> BookCaps {
    BookCaps {
        channel,
        // The toy's records carry any number of levels.
        max_depth: u16::MAX,
        cadence: Cadence::Realtime,
        continuity: Continuity::PlusOne,
        windowed: false,
        rest_anchor: false,
        includes_channels: TagSet::of(&[Channel::Public]),
        queue_model: QueueModelQuality::BracketOnly,
    }
}

/// The toy's three instruments.
pub fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    for (id, symbol) in (1..).zip(SYMBOLS) {
        let venue_symbol =
            dispatch_market_data(&caps(), |scope| scope.venue_symbol(symbol)).unwrap();
        let usd = AssetSym::new("USD").unwrap();
        table.insert(InstrumentSpec {
            id: InstrumentId::new(id),
            venue: VenueId::new(1),
            venue_symbol,
            native_id: None,
            underlying: UnderlyingId::new(id),
            kind: InstrumentKind::Perpetual,
            price_grid: PriceGrid::fixed(Decimal::ONE).unwrap(),
            quote_grid: None,
            size_step: SizeStep::new(Decimal::ONE).unwrap(),
            min_size: Lots::new(1).unwrap(),
            min_notional: None,
            max_order_size: None,
            position_limit: None,
            price_band: None,
            max_open_orders: None,
            multiplier: Decimal::ONE,
            quote_ccy: usd,
            settle_ccy: usd,
            funding: FundingSpec::Unknown,
            public_fees: None,
            status: TradingStatus::Trading,
            version: 1,
            fetched_at: WallNs(0),
        });
    }
    table
}

/// Trades only, no exec block (0015).
pub fn caps() -> VenueCaps {
    VenueCaps {
        exec: None,
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            encoding: Encoding::Json,
            touch_sources: Vec::new(),
            books: vec![book_caps("book0"), book_caps("book1")],
            trades: TradeCaps {
                source: FeedSource::Stream,
                aggressor: false,
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
            topology: ConnTopology::Shared {
                max_subscriptions: None,
            },
            max_conn_lifetime: None,
        },
        limits: Vec::new(),
        readiness_ceiling: Readiness::Record,
    }
}

impl VenueFactory for ToyVenue {
    fn id(&self) -> &'static str {
        "TOY"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        match cfg.get(REFUSE) {
            Some(_) => Err(ConfigError::Invalid {
                key: REFUSE,
                reason: "refused",
            }),
            None => {
                let mut caps = VenueCaps {
                    limits: self.limits.clone(),
                    ..caps()
                };
                let lifetime = cfg.get(LIFETIME_MS).and_then(|ms| ms.parse().ok());
                caps.md.max_conn_lifetime = lifetime.map(Duration::from_millis);
                Ok(caps)
            }
        }
    }

    /// The toy has no Java-era tickers.
    fn parse_fbc_common_symbol(&self, _s: &str) -> Result<AssetKey, SymbolError> {
        Err(SymbolError::NoRule)
    }

    /// The tests state the toy's instruments.
    fn discover(
        &self,
        _cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Err(VenueError::NoDiscovery)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let mut plans: Vec<EndpointPlan> = Vec::new();
        for (i, sub) in subs.iter().enumerate() {
            if sub.feed != Feed::Trades {
                return Err(VenueError::UnsupportedFeed(*sub));
            }
            specs
                .get(sub.inst)
                .ok_or(VenueError::UnknownInstrument(sub.inst))?;
            let n = i / PER_SOCKET;
            if plans.len() == n {
                let url = cfg.get(&format!("toy.url.{n}"));
                let url = url.ok_or(ConfigError::Missing("toy.url.<n>"))?;
                let one = cfg.get(ONE_STREAM).is_some();
                plans.push(EndpointPlan {
                    stream: StreamId(if one { 0 } else { n as u16 }),
                    transport: MdTransport::Socket {
                        url: WireUrl::plain(url),
                    },
                    subs: Vec::new(),
                });
            }
            plans[n].subs.push(*sub);
        }
        Ok(plans)
    }

    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        let n = self.codecs.fetch_add(1, Ordering::SeqCst);
        let ids = ep.subs.iter().map(|s| s.inst.get()).collect();
        self.plans.lock().unwrap().push(ids);
        let plan = ep.subs.iter().map(|s| s.inst.get().to_string());
        let plan = plan.collect::<Vec<_>>().join(",");
        let poll = match &ep.transport {
            MdTransport::Poll { base_url } => Some(base_url.as_str().to_owned()),
            MdTransport::Socket { .. } => None,
        };
        Box::new(ToyMd {
            n,
            plan,
            stream: ep.stream,
            poll,
            syms: Vec::new(),
            polling: false,
            log: self.http.clone(),
            books: BTreeMap::new(),
            subscribes: self.subscribes.clone(),
            split: cfg.get(SPLIT).map(|w| w.parse().unwrap_or(1)),
            keepalive: cfg.get(KEEPALIVE).map(keepalive),
            snapshot: cfg.get(SNAPSHOT).map(str::to_owned),
            live: BTreeSet::new(),
            snapshots: self.snapshots.clone(),
        })
    }

    fn plan_exec(&self, _: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(Vec::new())
    }

    fn exec_codec(&self, _: &VenueConfig) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        None
    }
}

/// One epoch's codec, numbered in the order the factory built it, and its plan's instruments;
/// on a poll endpoint, its base URL and the symbols it polls.
struct ToyMd {
    n: u32,
    plan: String,
    stream: StreamId,
    poll: Option<String>,
    syms: Vec<String>,
    polling: bool,
    log: Arc<Mutex<Vec<String>>>,
    /// Each anchored book channel's last seq; a channel absent waits for its next `begin`.
    books: BTreeMap<(InstrumentId, BookId), i64>,
    subscribes: Arc<AtomicU32>,
    /// One frame per instrument of a subscribe call, each charged this weight to its
    /// instrument.
    split: Option<u32>,
    keepalive: Option<Keepalive>,
    /// The base URL of the snapshots acknowledgements ask for.
    snapshot: Option<String>,
    /// The instruments whose subscription an acknowledgement marked live.
    live: BTreeSet<InstrumentId>,
    snapshots: Arc<AtomicU32>,
}

/// The keepalive `ping:<ms>` or `frame:<ms>` names.
fn keepalive(spec: &str) -> Keepalive {
    let (kind, ms) = spec.split_once(':').expect("kind:ms");
    let kind = match kind {
        "ping" => KeepaliveKind::WsPing,
        _ => KeepaliveKind::Frame(WireSlice::plain(b"ka".to_vec())),
    };
    Keepalive {
        interval: Duration::from_millis(ms.parse().expect("milliseconds")),
        kind,
        charge: CONTROL,
    }
}

const CONTROL: RateCharge = RateCharge::one(OpKind::Control, None);

/// The binary frame `bauth` asks to send: its key, the bytes after `bauth|key=`, is the only
/// part that is not UTF-8.
pub const BINARY_AUTH: &[u8] = b"bauth|key=\xff\xfe\x80";

fn send(stream: StreamId, text: String) -> Effect {
    send_as(TrafficClass::Normal, stream, text)
}

fn send_as(class: TrafficClass, stream: StreamId, text: String) -> Effect {
    charged(stream, text, (CONTROL, class))
}

fn charged(stream: StreamId, text: String, (charge, class): (RateCharge, TrafficClass)) -> Effect {
    Effect::Send {
        stream,
        frame: WireSlice::plain(text.into_bytes()),
        rpc: None,
        class,
        charge,
    }
}

/// The charge and class a record's `op`, `sym`, `w` and `class` fields name.
fn charge_of(
    fields: &[(&str, &str)],
    specs: &SpecTable,
) -> Result<(RateCharge, TrafficClass), DecodeError> {
    let op = match field(fields, "op").unwrap_or("control") {
        "place" => OpKind::Place,
        "cancel" => OpKind::Cancel,
        "rest" => OpKind::Rest,
        "control" => OpKind::Control,
        _ => return Err(DecodeError::Malformed("op")),
    };
    let inst = match field(fields, "sym") {
        Ok(sym) => Some(
            specs
                .by_symbol(sym)
                .ok_or(DecodeError::UnknownInstrument)?
                .id,
        ),
        Err(_) => None,
    };
    let weight = match field(fields, "w") {
        Ok(_) => NonZeroU32::new(num(fields, "w")? as u32).ok_or(DecodeError::Malformed("w"))?,
        Err(_) => NonZeroU32::MIN,
    };
    let class = match field(fields, "class") {
        Ok("safety") => TrafficClass::Safety,
        _ => TrafficClass::Normal,
    };
    Ok((RateCharge { op, inst, weight }, class))
}

fn field<'a>(fields: &[(&'a str, &'a str)], key: &'static str) -> Result<&'a str, DecodeError> {
    let found = fields.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
    found.ok_or(DecodeError::Malformed(key))
}

fn num(fields: &[(&str, &str)], key: &'static str) -> Result<i64, DecodeError> {
    field(fields, key)?
        .parse()
        .map_err(|_| DecodeError::Malformed(key))
}

fn get(tag: u64, url: String, timeout: Duration) -> Effect {
    get_charged(tag, url, timeout, (CONTROL, TrafficClass::Normal))
}

fn get_charged(
    tag: u64,
    url: String,
    timeout: Duration,
    (charge, class): (RateCharge, TrafficClass),
) -> Effect {
    Effect::Http {
        tag: HttpTag(tag),
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(url),
            headers: Vec::new(),
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout,
        class,
        charge,
    }
}

impl MdCodec for ToyMd {
    fn on_open(&mut self, fx: &mut Effects) {
        if self.poll.is_some() {
            return;
        }
        let hello = format!("hello|codec={}|plan={}", self.n, self.plan);
        fx.push(send(self.stream, hello));
    }

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        self.subscribes.fetch_add(1, Ordering::SeqCst);
        // A removed subscription's acknowledgement is forgotten: one added again is
        // acknowledged afresh (Codex r4181466463).
        for sub in remove {
            self.live.remove(&sub.inst);
        }
        let (add_subs, remove_subs) = (add, remove);
        let spell = |subs: &[Subscription]| -> Result<Vec<&str>, VenueError> {
            let spec = |s: &Subscription| {
                specs
                    .get(s.inst)
                    .ok_or(VenueError::UnknownInstrument(s.inst))
            };
            subs.iter()
                .map(|s| Ok(spec(s)?.venue_symbol.as_wire()))
                .collect()
        };
        // A call of one instrument counts against it where a limit is per pair.
        let mut insts = add.iter().chain(remove).map(|s| s.inst);
        let inst = insts.next().filter(|first| insts.all(|i| i == *first));
        let charge = RateCharge::one(OpKind::Subscribe, inst);
        let (add, remove) = (spell(add)?, spell(remove)?);
        if let Some(base) = &self.poll {
            self.syms.retain(|s| !remove.contains(&s.as_str()));
            self.syms.extend(add.iter().map(|s| s.to_string()));
            if !self.polling {
                self.polling = true;
                fx.push(self.poll_get(base));
            }
            return Ok(());
        }
        if let Some(weight) = self.split.and_then(NonZeroU32::new) {
            for (part, subs) in [("add", add_subs), ("remove", remove_subs)] {
                for sub in subs {
                    let sym = spell(std::slice::from_ref(sub))?.join("");
                    let one = RateCharge::one(OpKind::Subscribe, Some(sub.inst));
                    let charge = RateCharge { weight, ..one };
                    let text = format!("sub|{part}={sym}");
                    fx.push(charged(self.stream, text, (charge, TrafficClass::Normal)));
                }
            }
            return Ok(());
        }
        let mut text = String::from("sub");
        for (part, syms) in [("add", add), ("remove", remove)] {
            if !syms.is_empty() {
                text += &format!("|{part}={}", syms.join(","));
            }
        }
        fx.push(charged(self.stream, text, (charge, TrafficClass::Normal)));
        Ok(())
    }

    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        _: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let RawFrame::Text(line) = f else {
            return Err(DecodeError::Malformed("binary frame"));
        };
        self.line(line, specs, sink, fx)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let entry = match resp {
            Ok(r) => {
                let toy = r
                    .headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("x-toy"));
                format!("{}:{}", r.status, toy.map_or("-", |(_, v)| *v))
            }
            Err(failure) => format!("{failure:?}"),
        };
        self.log
            .lock()
            .unwrap()
            .push(format!("{}/{}:{entry}", self.n, tag.0));
        if self.poll.is_some() {
            fx.push(Effect::Timer {
                tag: POLL,
                after: Duration::from_millis(10),
            });
        }
        let body = resp.map_or(&b""[..], |r| r.body);
        let body = std::str::from_utf8(body).map_err(|_| DecodeError::Malformed("body"))?;
        for line in body.lines() {
            self.line(line, specs, sink, fx)?;
        }
        Ok(())
    }

    fn on_timer(
        &mut self,
        tag: TimerTag,
        _: MonoNs,
        _: WallNs,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        if let (POLL, Some(base)) = (tag, &self.poll) {
            fx.push(self.poll_get(base));
            return;
        }
        let inst = InstrumentId::new(tag.0 as u32);
        let h = FeedHealth::Stale;
        sink.push(
            VenueMeta::NONE,
            MdEvent::Health {
                inst,
                feed: Feed::Trades,
                h,
            },
        );
    }

    fn keepalive(&self) -> Option<Keepalive> {
        self.keepalive.clone()
    }

    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

impl ToyMd {
    /// The poll of the subscribed symbols.
    fn poll_get(&self, base: &str) -> Effect {
        let url = format!("{base}/poll?syms={}", self.syms.join(","));
        get(0, url, Duration::from_secs(1))
    }

    /// One record of the toy's protocol, from a frame or a response body.
    fn line(
        &mut self,
        line: &str,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let mut parts = line.split('|');
        let kind = parts.next().unwrap_or_default();
        let fields: Vec<_> = parts.filter_map(|p| p.split_once('=')).collect();
        let inst = || {
            let spec = specs.by_symbol(field(&fields, "sym")?);
            spec.map(|s| s.id).ok_or(DecodeError::UnknownInstrument)
        };
        match kind {
            "trade" => {
                let qty = Lots::new(num(&fields, "qty")?).ok_or(DecodeError::Malformed("qty"))?;
                let meta = VenueMeta {
                    exch_ts: None,
                    exch_ts_kind: ExchTsKind::Unknown,
                    venue_seq: Some(num(&fields, "seq")? as u64),
                };
                let ev = MdEvent::Trade {
                    inst: inst()?,
                    id: None,
                    aggressor: Aggressor::Unknown,
                    px: Ticks(num(&fields, "px")?),
                    qty,
                };
                sink.push(meta, ev);
            }
            "ack" => {
                let inst = inst()?;
                if let (true, Some(base)) = (self.live.insert(inst), &self.snapshot) {
                    self.snapshots.fetch_add(1, Ordering::SeqCst);
                    let url = format!("{base}/{}", field(&fields, "sym")?);
                    fx.push(get(u64::from(inst.get()), url, Duration::from_secs(1)));
                }
            }
            "arm" => fx.push(Effect::Timer {
                tag: TimerTag(u64::from(inst()?.get())),
                after: Duration::from_millis(num(&fields, "ms")? as u64),
            }),
            "big" => {
                let kb = num(&fields, "kb")? as usize;
                fx.push(send(self.stream, "x".repeat(kb * 1024)));
            }
            "bye" => fx.push(Effect::Reconnect {
                stream: self.stream,
                reason: "bye",
            }),
            "say" => {
                let text = match field(&fields, "id") {
                    Ok(id) => format!("said|id={id}"),
                    Err(_) => "said".into(),
                };
                fx.push(charged(self.stream, text, charge_of(&fields, specs)?));
            }
            "auth" => {
                let text = "auth|key=toy-secret";
                let span = "auth|key=".len() as u32..text.len() as u32;
                let frame = WireSlice::redacted(text.as_bytes().to_vec(), vec![span])
                    .expect("the span lies inside the frame");
                fx.push(Effect::Send {
                    stream: self.stream,
                    frame,
                    rpc: None,
                    class: TrafficClass::Normal,
                    charge: CONTROL,
                });
            }
            "bauth" => {
                let span = "bauth|key=".len() as u32..BINARY_AUTH.len() as u32;
                let frame = WireSlice::redacted(BINARY_AUTH.to_vec(), vec![span])
                    .expect("the span lies inside the frame");
                fx.push(Effect::Send {
                    stream: self.stream,
                    frame,
                    rpc: None,
                    class: TrafficClass::Normal,
                    charge: CONTROL,
                });
            }
            "cancel" => {
                let id = num(&fields, "id")?;
                let text = format!("cancel|id={id}");
                fx.push(send_as(TrafficClass::Safety, self.stream, text));
            }
            "get" => {
                let tag = num(&fields, "tag")?;
                let timeout = match field(&fields, "ms")? {
                    "max" => Duration::MAX,
                    _ => Duration::from_millis(num(&fields, "ms")? as u64),
                };
                let url = field(&fields, "url")?.to_owned();
                let charge = charge_of(&fields, specs)?;
                let mut get = get_charged(tag as u64, url, timeout, charge);
                if let (Ok(_), Effect::Http { req, .. }) = (field(&fields, "auth"), &mut get) {
                    req.headers.push(Header {
                        name: "Authorization",
                        value: "toy-token".into(),
                        redact: false,
                    });
                }
                fx.push(get);
                if let Ok(kb) = num(&fields, "kb") {
                    fx.push(send(self.stream, "x".repeat(kb as usize * 1024)));
                }
                if field(&fields, "bye").is_ok() {
                    fx.push(Effect::Reconnect {
                        stream: self.stream,
                        reason: "bye",
                    });
                }
            }
            "begin" | "lvl" | "end" => {
                let (inst, seq) = (inst()?, num(&fields, "seq")?);
                let book = BookId(num(&fields, "book")? as u8);
                let ev = match kind {
                    "begin" => {
                        let epoch = num(&fields, "epoch")? as u32;
                        MdEvent::BookSnapshotBegin { inst, book, epoch }
                    }
                    "end" => MdEvent::BookSnapshotEnd { inst, book },
                    _ => MdEvent::Level {
                        inst,
                        book,
                        side: match field(&fields, "side")? {
                            "bid" => BookSide::Bid,
                            "ask" => BookSide::Ask,
                            _ => return Err(DecodeError::Malformed("side")),
                        },
                        px: Ticks(num(&fields, "px")?),
                        qty: Lots::new(num(&fields, "qty")?)
                            .ok_or(DecodeError::Malformed("qty"))?,
                    },
                };
                let meta = VenueMeta {
                    exch_ts: None,
                    exch_ts_kind: ExchTsKind::Unknown,
                    venue_seq: Some(seq as u64),
                };
                let last = self.books.get(&(inst, book)).copied();
                if kind != "begin" && last.and_then(|l| l.checked_add(1)) != Some(seq) {
                    if last.is_some() {
                        self.books.remove(&(inst, book));
                        let h = FeedHealth::Gap;
                        let feed = Feed::Book(book);
                        sink.push(meta, MdEvent::Health { inst, feed, h });
                    }
                    return Ok(());
                }
                self.books.insert((inst, book), seq);
                sink.push(meta, ev);
            }
            "odd" => {
                let other = StreamId(9);
                fx.push(send(other, "stray".into()));
                fx.push(Effect::Reconnect {
                    stream: other,
                    reason: "stray",
                });
            }
            _ => return Err(DecodeError::Malformed("kind")),
        }
        Ok(())
    }
}
