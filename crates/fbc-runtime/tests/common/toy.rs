//! A toy market-data venue for the runtime's session tests (FBC-ku8): `exec: None`, trades only,
//! a text protocol of one `kind|key=value|...` record per frame. It describes no real venue.
//!
//! Out: `hello|codec=<n>` on open (`n` counts the codecs its factory built), and
//! `sub|add=A,B|remove=C` per subscribe call (empty parts left out).
//! In: `trade|sym=A|px=<ticks>|qty=<lots>|seq=<n>` is a trade; `arm|sym=A|ms=<n>` sets a timer
//! whose firing reports the instrument's trades stale; `bye` asks for a reconnect; `odd` asks for
//! a frame and a reconnect on another stream and an HTTP request, which a session refuses.
//! Anything else, and every binary frame, is malformed.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbc_core::{
    Aggressor, AssetSym, ConfigError, ConnTopology, DecodeError, DecodeScope, Effect, Effects,
    Encoding, EndpointPlan, ExchTsKind, ExecCodec, ExecEndpoint, Feed, FeedHealth, FeedSource,
    FieldSpec, FundingCaps, FundingSpec, HttpFailure, HttpMethod, HttpRequest, HttpResponse,
    HttpTag, InstrumentId, InstrumentKind, InstrumentSpec, Keepalive, Lots, MatchingCaps, MdCaps,
    MdCodec, MdEvent, MdSink, MonoNs, OpKind, PriceGrid, RateCharge, RawFrame, Readiness, SizeStep,
    SpecTable, StpScope, StreamId, Subscription, Ticks, TimerTag, TradeCaps, TradingStatus,
    TrafficClass, UnderlyingId, VenueCaps, VenueConfig, VenueError, VenueFactory, VenueId,
    VenueMeta, WallNs, WireSlice, WireUrl, dispatch_market_data,
};
use rust_decimal::Decimal;

/// The toy's one stream.
pub const STREAM: StreamId = StreamId(0);
/// The configuration key that makes the toy refuse its configuration.
pub const REFUSE: &str = "toy.refuse";
/// The toy's instruments, by id: 1 is `A`, 2 is `B`, 3 is `C`.
const SYMBOLS: [&str; 3] = ["A", "B", "C"];

/// The toy venue; it counts the codecs it builds.
#[derive(Default)]
pub struct ToyVenue {
    codecs: AtomicU32,
}

impl ToyVenue {
    /// A venue of the test's own, with its codec count at zero.
    pub fn leak() -> &'static ToyVenue {
        Box::leak(Box::default())
    }
}

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

    /// Planning is FBC-klr's; the tests hand the session its plan.
    fn plan_md(
        &self,
        _: &VenueConfig,
        _: &SpecTable,
        _: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        Ok(Vec::new())
    }

    fn md_codec(&self, _: &VenueConfig, _: &EndpointPlan) -> Box<dyn MdCodec> {
        let n = self.codecs.fetch_add(1, Ordering::SeqCst);
        Box::new(ToyMd { n })
    }

    fn plan_exec(&self, _: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(Vec::new())
    }

    fn exec_codec(&self, _: &VenueConfig) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        None
    }
}

/// One epoch's codec, numbered in the order the factory built it.
struct ToyMd {
    n: u32,
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

impl MdCodec for ToyMd {
    fn on_open(&mut self, fx: &mut Effects) {
        fx.push(send(STREAM, format!("hello|codec={}", self.n)));
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
        let mut text = String::from("sub");
        for (part, syms) in [("add", add), ("remove", remove)] {
            if !syms.is_empty() {
                text += &format!("|{part}={}", syms.join(","));
            }
        }
        fx.push(send(STREAM, text));
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
            "bye" => fx.push(Effect::Reconnect {
                stream: STREAM,
                reason: "bye",
            }),
            "odd" => {
                let other = StreamId(9);
                fx.push(send(other, "stray".into()));
                fx.push(Effect::Reconnect {
                    stream: other,
                    reason: "stray",
                });
                fx.push(Effect::Http {
                    tag: HttpTag(1),
                    req: HttpRequest {
                        method: HttpMethod::Get,
                        url: WireUrl::plain("http://toy.invalid/"),
                        headers: Vec::new(),
                        body: WireSlice::plain(Vec::new()),
                    },
                    rpc: None,
                    timeout: Duration::from_secs(1),
                    class: TrafficClass::Normal,
                    charge: CONTROL,
                });
            }
            _ => return Err(DecodeError::Malformed("kind")),
        }
        Ok(())
    }

    /// The toy asks for no HTTP a session would answer.
    fn on_http(
        &mut self,
        _: HttpTag,
        _: Result<HttpResponse<'_>, HttpFailure>,
        _: &DecodeScope<'_>,
        _: &SpecTable,
        _: &mut dyn MdSink,
        _: &mut Effects,
    ) -> Result<(), DecodeError> {
        Ok(())
    }

    fn on_timer(
        &mut self,
        tag: TimerTag,
        _: MonoNs,
        _: WallNs,
        sink: &mut dyn MdSink,
        _: &mut Effects,
    ) {
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
