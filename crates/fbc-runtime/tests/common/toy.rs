//! A toy market-data venue for the runtime's session tests (FBC-ku8, FBC-klr): `exec: None`,
//! trades only, a text protocol of one `kind|key=value|...` record per frame. It describes no
//! real venue.
//!
//! Plan: trades only, at most two subscriptions per socket endpoint in instrument order; the
//! `n`th endpoint is `StreamId(n)` at the configuration's `toy.url.<n>`.
//! Out: `hello|codec=<n>|plan=<ids>` on open (`n` counts the codecs its factory built, `ids`
//! are the instruments of the plan it was built for), and
//! `sub|add=A,B|remove=C` per subscribe call (empty parts left out).
//! In: `trade|sym=A|px=<ticks>|qty=<lots>|seq=<n>` is a trade; `arm|sym=A|ms=<n>` sets a timer
//! whose firing reports the instrument's trades stale; `big|kb=<n>` asks for an `n` KiB frame;
//! `bye` asks for a reconnect; `say` asks to send `said`; `get|tag=<n>|ms=<t>|url=<u>` asks for
//! a GET of `u` with a `t` ms timeout (and, with `|kb=<k>`, then a `k` KiB frame); `get` with
//! `ms=max` asks for a timeout past the end of the clock; `odd` asks for a frame and a reconnect on another stream,
//! which a session refuses. Anything else, and every binary frame, is malformed.
//!
//! Every `on_http` is logged as `<codec>/<tag>:<status>:<x-toy header>` or
//! `<codec>/<tag>:<failure>`, and a response body's lines are read as frames. On a poll
//! endpoint the codec sends nothing: its first subscribe asks for
//! `<base_url>/poll?syms=<subscribed>` at once, and each answer sets a 10 ms timer whose
//! firing asks again.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbc_core::{
    Aggressor, AssetSym, ConfigError, ConnTopology, DecodeError, DecodeScope, Effect, Effects,
    Encoding, EndpointPlan, ExchTsKind, ExecCodec, ExecEndpoint, Feed, FeedHealth, FeedSource,
    FieldSpec, FundingCaps, FundingSpec, HttpFailure, HttpMethod, HttpRequest, HttpResponse,
    HttpTag, InstrumentId, InstrumentKind, InstrumentSpec, Keepalive, Lots, MatchingCaps, MdCaps,
    MdCodec, MdEvent, MdSink, MdTransport, MonoNs, OpKind, PriceGrid, RateCharge, RawFrame,
    Readiness, SizeStep, SpecTable, StpScope, StreamId, Subscription, Ticks, TimerTag, TradeCaps,
    TradingStatus, TrafficClass, UnderlyingId, VenueCaps, VenueConfig, VenueError, VenueFactory,
    VenueId, VenueMeta, WallNs, WireSlice, WireUrl, dispatch_market_data,
};
use rust_decimal::Decimal;

/// The toy's one stream.
pub const STREAM: StreamId = StreamId(0);
/// The configuration key that makes the toy refuse its configuration.
pub const REFUSE: &str = "toy.refuse";
/// The configuration key that makes the toy name every endpoint of a plan stream 0.
pub const ONE_STREAM: &str = "toy.one_stream";
/// The toy's instruments, by id: 1 is `A`, 2 is `B`, 3 is `C`.
const SYMBOLS: [&str; 3] = ["A", "B", "C"];

/// The toy venue; it counts the codecs it builds and logs their `on_http` calls.
#[derive(Default)]
pub struct ToyVenue {
    codecs: AtomicU32,
    http: Arc<Mutex<Vec<String>>>,
}

impl ToyVenue {
    /// A venue of the test's own, with its codec count at zero.
    pub fn leak() -> &'static ToyVenue {
        Box::leak(Box::default())
    }

    /// How many codecs it has built.
    pub fn codecs(&self) -> u32 {
        self.codecs.load(Ordering::SeqCst)
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
            books: Vec::new(),
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
            None => Ok(caps()),
        }
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

    fn md_codec(&self, _: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        let n = self.codecs.fetch_add(1, Ordering::SeqCst);
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
}

const CONTROL: RateCharge = RateCharge::one(OpKind::Control, None);

fn send(stream: StreamId, text: String) -> Effect {
    Effect::Send {
        stream,
        frame: WireSlice::plain(text.into_bytes()),
        rpc: None,
        class: TrafficClass::Normal,
        charge: CONTROL,
    }
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
        class: TrafficClass::Normal,
        charge: CONTROL,
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
        let mut text = String::from("sub");
        for (part, syms) in [("add", add), ("remove", remove)] {
            if !syms.is_empty() {
                text += &format!("|{part}={}", syms.join(","));
            }
        }
        fx.push(send(self.stream, text));
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
        None
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
            "say" => fx.push(send(self.stream, "said".into())),
            "get" => {
                let tag = num(&fields, "tag")?;
                let timeout = match field(&fields, "ms")? {
                    "max" => Duration::MAX,
                    _ => Duration::from_millis(num(&fields, "ms")? as u64),
                };
                let url = field(&fields, "url")?.to_owned();
                fx.push(get(tag as u64, url, timeout));
                if let Ok(kb) = num(&fields, "kb") {
                    fx.push(send(self.stream, "x".repeat(kb as usize * 1024)));
                }
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
