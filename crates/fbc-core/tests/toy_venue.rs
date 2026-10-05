//! Decision 0002, design §4.6–§4.8: a venue adapter is a sans-IO codec. The toy venue below
//! implements `MdCodec`, `ExecCodec` and `VenueFactory` over a hand-written text protocol. It
//! decodes frames into `MdEvent` and `ExecEvent` only inside the `DecodeScope` the core's
//! dispatch lends (decision 0004: venue ids, fill ids and fees come from nowhere else), encodes
//! a `VenueCommand` taking time and nonces only from `EncodeCtx`, and does no IO: everything it
//! wants done comes back as `Effects`. Its protocol, symbols and values describe no real venue.
//!
//! The toy proves FBC-5's, FBC-ji6's, FBC-ahf's and FBC-b3b's done lines; it is not a
//! conformance suite, and its `VenueCaps` declare only what its codecs do. One socket carries
//! the touch; one carries order entry (limit orders out, each signed with a toy hash, its signer
//! call marked as the sign stage through `PathStamps`; acks, account events, fills and a resync
//! answered in one frame in). It has no book, trades or other feed, cancels, amends and queries
//! nothing, and its codecs ask for no HTTP; it keeps one resync in flight, and its one timer
//! asks for that resync again while it is unanswered. Its market-data socket is kept alive with
//! a ping frame.
//!
//! Its factory discovers its instruments with one `GET` of its market list, a plan of effects
//! whose parser builds one `InstrumentSpecDraft` per `market` record inside the decode scope,
//! and reads Java-era tickers by its own FBC rule: `X/USDT` is `X-PERP`, listed in USDC.
//!
//! Its one configuration key is its credential (FBC-b3b, decision 0043): the exec codec takes
//! it from the `Secrets` the factory is handed and sends it in its hello as a redaction span,
//! and `test_connection` proves it with a plan of one `GET` of the account whose header carries
//! it redacted.
//!
//! Every frame it asks for, and its ping, carries the rate charge its declared limits count
//! (decision 0018): orders per instrument, a resync as a weighted query against the account,
//! and subscriptions, its hello and its pings against the connection they are written on.
//!
//! Protocol: one record per line, `kind|key=value|...`; `ts` is the venue's matching-engine time
//! in nanoseconds and `seq` its sequence. Prices are ticks, sizes lots, money nanos of USDC. A
//! market record states its tick and size step as decimals and its minimum size in lots.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbc_core::{
    AccountSummary, AckLevel, AckModel, AliasTable, AssetKey, AssetSym, Cadence,
    CancelOnDisconnect, Channel, Charset, CidMatch, CidMint, ClientIdFormat, ClientOrderId,
    ConfigError, ConnKey, ConnTopology, CtxCall, DecodeError, DecodeScope, Effect, Effects,
    EncodeCtx, EncodeReceipt, Encoding, EndpointPlan, Envelope, ExchNs, ExchTsKind, ExecCaps,
    ExecCodec, ExecEndpoint, ExecEvent, ExecSink, Feed, FeedHealth, FeedSource, FieldSpec,
    FillCaps, FillEvent, FillIdent, FillSource, FundingCaps, FundingSpec, HttpFailure, HttpMethod,
    HttpPlan, HttpRequest, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId,
    InstrumentKind, InstrumentResolver, InstrumentSpec, InstrumentSpecDraft, ItemRef, Keepalive,
    KeepaliveKind, LimitScope, Liquidity3, Listing, Lots, Lvl, MatchingCaps, MdCaps, MdCodec,
    MdEvent, MdSink, MdTransport, Money, MonoNs, Namespace, NamespaceLease, NewOrder, NonceBlock,
    NonceScope, NotSentReason, OpKind, OrderCaps, OrderKind, OrderKindTag, OrderingKey, PathEdge,
    PathMark, PathRecorder, PathStage, PathStamps, PlaceWire, PlanError, PriceGrid, PxExact,
    RateCharge, RateLimit, RawFrame, Readiness, ResolveError, RpcCall, RpcId, SeqDomain, Side, Sig,
    SignedLots, SizeStep, SnapshotSource, SpecTable, Stamp, StpScope, StreamId, SubmitHandle,
    SubmitOutcome, Subscription, Support, SymbolError, TagSet, Ticks, TifTag, TimerTag,
    TouchSourceCaps, TouchSourceId, TradeCaps, TradingStatus, TrafficClass, UnderlyingId,
    VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, VenueFeeSign, VenueId,
    VenueMeta, VenueOrderSnapshot, VenueOrderState, Via, WallNs, WireSlice, WireUrl,
    common_symbol_parts, decode_cid, dispatch, dispatch_market_data, encode_cid,
};
use fbc_core::{ConfigScope, FieldUnit, Header, Secret, Secrets};
use rust_decimal::Decimal;

// ---------------------------------------------------------------------------------------------
// The toy venue.
// ---------------------------------------------------------------------------------------------

const SYMBOL: &str = "TOY-PERP";
const INST: InstrumentId = InstrumentId::new(7);
const OWN_NS: Namespace = Namespace::new(5);
const MD_STREAM: StreamId = StreamId(0);
const EXEC_STREAM: StreamId = StreamId(1);
/// The toy's endpoints: constants that carry no credential.
const MD_URL: &str = "wss://toy.invalid/md";
const EXEC_URL: &str = "wss://toy.invalid/exec";
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
/// The timer that asks again for a resync left unanswered for `RPC_TIMEOUT`.
const RESYNC_TAG: TimerTag = TimerTag(1);
/// The toy reports fees with a positive rebate, so the scope must flip their sign.
const FEE_SIGN: VenueFeeSign = VenueFeeSign::PositiveIsRebate;
const CID_FORMAT: ClientIdFormat = ClientIdFormat::Alnum {
    max_len: 32,
    charset: Charset::Alphanumeric,
};
/// The hello on open and the keepalive ping: control frames, counted on their connection.
const CONTROL: RateCharge = RateCharge::one(OpKind::Control, None);
/// A resync: a query of the whole account, weighted against the account's query budget.
const RESYNC_CHARGE: RateCharge = RateCharge {
    op: OpKind::Query,
    inst: None,
    weight: NonZeroU32::new(5).unwrap(),
};
/// How often the market-data socket is pinged.
const PING_EVERY: Duration = Duration::from_secs(30);
/// Where the toy lists its markets, and the tag of the one request discovery makes there.
const MARKETS_URL: &str = "https://toy.invalid/markets";
const MARKETS_TAG: HttpTag = HttpTag(1);
/// The toy's credential, by its configuration key: an account-scoped secret.
const KEY: &str = "toy.key";
const SCHEMA: &[FieldSpec] = &[FieldSpec {
    key: KEY,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "the account's key, sent in the hello and in the account request",
}];
/// The account request `test_connection` makes, its tag and header.
const ACCOUNT_URL: &str = "https://toy.invalid/account";
const ACCOUNT_TAG: HttpTag = HttpTag(1);
const KEY_HEADER: &str = "X-Toy-Key";

fn usdc() -> AssetSym {
    AssetSym::new("USDC").unwrap()
}

/// One record, split into its kind and its fields.
struct Frame<'a> {
    kind: &'a str,
    fields: Vec<(&'a str, &'a str)>,
}

impl<'a> Frame<'a> {
    fn parse(line: &'a str) -> Result<Frame<'a>, DecodeError> {
        let mut parts = line.trim().split('|');
        let kind = parts.next().filter(|k| !k.is_empty());
        let kind = kind.ok_or(DecodeError::Malformed("kind"))?;
        let fields = parts
            .map(|part| part.split_once('=').ok_or(DecodeError::Malformed("field")))
            .collect::<Result<_, _>>()?;
        Ok(Frame { kind, fields })
    }

    fn opt(&self, key: &str) -> Option<&'a str> {
        self.fields.iter().find(|(k, _)| *k == key).map(|(_, v)| *v)
    }

    fn get(&self, key: &'static str) -> Result<&'a str, DecodeError> {
        self.opt(key).ok_or(DecodeError::Malformed(key))
    }

    fn num<T: FromStr>(&self, key: &'static str) -> Result<T, DecodeError> {
        let bad = DecodeError::Malformed(key);
        self.get(key)?.parse().map_err(|_| bad)
    }

    fn opt_num<T: FromStr>(&self, key: &'static str) -> Result<Option<T>, DecodeError> {
        self.opt(key).map(|_| self.num(key)).transpose()
    }

    fn meta(&self) -> Result<VenueMeta, DecodeError> {
        let exch_ts = self.opt_num::<i64>("ts")?.map(ExchNs);
        let exch_ts_kind = match exch_ts {
            Some(_) => ExchTsKind::MatchingEngine,
            None => ExchTsKind::Unknown,
        };
        let venue_seq = self.opt_num("seq")?;
        Ok(VenueMeta {
            exch_ts,
            exch_ts_kind,
            venue_seq,
        })
    }

    fn inst(&self, specs: &SpecTable) -> Result<InstrumentId, DecodeError> {
        let spec = specs.by_symbol(self.get("sym")?);
        spec.map(|s| s.id).ok_or(DecodeError::UnknownInstrument)
    }

    /// A required quantity; a negative one is malformed.
    fn lots(&self, key: &'static str) -> Result<Lots, DecodeError> {
        Lots::new(self.num(key)?).ok_or(DecodeError::Malformed(key))
    }

    fn usdc(&self, key: &'static str) -> Result<Money, DecodeError> {
        Ok(Money::new(self.num(key)?, usdc()))
    }

    fn side(&self) -> Result<Side, DecodeError> {
        match self.get("side")? {
            "B" => Ok(Side::Buy),
            "S" => Ok(Side::Sell),
            _ => Err(DecodeError::Malformed("side")),
        }
    }

    /// A position: instrument, signed quantity and, when sent, average entry price.
    fn position(
        &self,
        specs: &SpecTable,
    ) -> Result<(InstrumentId, SignedLots, Option<PxExact>), DecodeError> {
        let qty = SignedLots(self.num("qty")?);
        Ok((self.inst(specs)?, qty, self.opt_num("avg")?))
    }

    /// `pxXqty`: one price level, `None` for an empty side.
    fn level(&self, key: &'static str) -> Result<Option<Lvl>, DecodeError> {
        let Some(text) = self.opt(key) else {
            return Ok(None);
        };
        let bad = DecodeError::Malformed(key);
        let (px, qty) = text.split_once('x').ok_or(bad)?;
        let px = Ticks(px.parse().map_err(|_| bad)?);
        let qty = qty.parse().ok().and_then(Lots::new).ok_or(bad)?;
        Ok(Some(Lvl { px, qty }))
    }
}

/// The text of a text frame.
fn text(f: RawFrame<'_>) -> Result<&str, DecodeError> {
    match f {
        RawFrame::Text(text) => Ok(text),
        RawFrame::Binary(_) => Err(DecodeError::Malformed("binary frame")),
    }
}

/// Asks for `text` to be written to `stream`, with a deadline when it is request `rpc`, charged
/// to the venue's rate limits as `charge`.
fn send(
    stream: StreamId,
    text: &str,
    rpc: Option<RpcId>,
    class: TrafficClass,
    charge: RateCharge,
) -> Effect {
    let frame = WireSlice::plain(text.as_bytes().to_vec());
    let rpc = rpc.map(|id| RpcCall {
        id,
        timeout: RPC_TIMEOUT,
    });
    Effect::Send {
        stream,
        frame,
        rpc,
        class,
        charge,
    }
}

/// The toy's signature over an order as it goes on the wire: an FNV-1a hash of its fields in
/// hexadecimal. It reads only the wire view, which takes its time and nonce from the encode
/// context.
fn toy_sign(w: &PlaceWire<'_>) -> Sig {
    let signed = format!(
        "{}|{}|{:?}|{:?}|{}|{:?}|{:?}|{}|{}|{}|{:?}",
        w.spec.venue_symbol.as_wire(),
        w.cid,
        w.side,
        w.kind,
        w.qty.get(),
        w.tif,
        w.channel,
        w.post_only,
        w.reduce_only,
        w.wall.0,
        w.nonce,
    );
    let hash = signed.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    Sig::new(format!("{hash:016x}").as_bytes()).expect("16 bytes fit a signature")
}

/// Refuses a subscription to a feed the toy's caps do not offer, or an instrument missing from
/// the spec table; otherwise the instrument's venue symbol.
fn spell(specs: &SpecTable, sub: Subscription) -> Result<&str, VenueError> {
    let touch_sources = toy_caps().md.touch_sources.len();
    if !matches!(sub.feed, Feed::Touch(id) if usize::from(id.0) < touch_sources) {
        return Err(VenueError::UnsupportedFeed(sub));
    }
    let spec = specs.get(sub.inst);
    let spec = spec.ok_or(VenueError::UnknownInstrument(sub.inst))?;
    Ok(spec.venue_symbol.as_wire())
}

/// The toy's market-data codec: one socket, nothing but frames.
struct ToyMd;

impl MdCodec for ToyMd {
    fn on_open(&mut self, _fx: &mut Effects) {}

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        // Everything is checked and spelled before the first push, so a refusal pushes nothing.
        let mut frames = Vec::new();
        for (verb, subs) in [("sub", add), ("unsub", remove)] {
            for sub in subs {
                let symbol = spell(specs, *sub)?;
                let frame = format!("{verb}|sym={symbol}|feed={:?}", sub.feed);
                frames.push((frame, RateCharge::one(OpKind::Subscribe, Some(sub.inst))));
            }
        }
        for (frame, charge) in frames {
            fx.push(send(MD_STREAM, &frame, None, TrafficClass::Normal, charge));
        }
        Ok(())
    }

    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let f = Frame::parse(text(f)?)?;
        if f.kind == "refused" {
            // The venue refused the subscription to the toy's one feed: reported by instrument
            // and feed, and nothing asked for, so nothing is retried.
            let (inst, feed) = (f.inst(specs)?, Feed::Touch(TouchSourceId(0)));
            let h = FeedHealth::Refused;
            sink.push(VenueMeta::NONE, MdEvent::Health { inst, feed, h });
            return Ok(());
        }
        if f.kind != "touch" {
            return Err(DecodeError::Malformed("kind"));
        }
        // Everything is read before the push, so a frame that fails pushes nothing. The touch
        // channel's own sequence is required (SeqDomain::Own).
        let (meta, inst) = (f.meta()?, f.inst(specs)?);
        meta.venue_seq.ok_or(DecodeError::Malformed("seq"))?;
        let (bid, ask, source) = (f.level("bid")?, f.level("ask")?, TouchSourceId(0));
        let touch = MdEvent::Touch {
            inst,
            bid,
            ask,
            source,
        };
        sink.push(meta, touch);
        Ok(())
    }

    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed("the toy asks for no HTTP"))
    }

    fn on_timer(
        &mut self,
        _tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) {
    }

    /// A ping frame, counted against the connection like any other control frame.
    fn keepalive(&self) -> Option<Keepalive> {
        Some(Keepalive {
            interval: PING_EVERY,
            kind: KeepaliveKind::Frame(WireSlice::plain(b"ping".to_vec())),
            charge: CONTROL,
        })
    }
    /// Public market data: nothing to redact.
    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

/// The toy's order-entry codec. Every field it writes comes from the command, the spec table or
/// the encode context. It keeps one value: the instant of the resync it asked for, which is the
/// resync's watermark (record 0014), since the venue's echo of it cannot be trusted alone.
struct ToyExec {
    resync_at: Option<WallNs>,
    key: Secret,
}

impl ToyExec {
    /// One order as a resync reports it; every field the caps promise is required, and its
    /// quantity includes its filled part.
    fn snapshot(
        f: &Frame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
    ) -> Result<VenueOrderSnapshot, DecodeError> {
        let (qty, cum_filled) = (f.lots("qty")?, f.lots("cum")?);
        if cum_filled > qty {
            return Err(DecodeError::Malformed("cum"));
        }
        Ok(VenueOrderSnapshot {
            cid: Some(scope.client_order_id(f.get("cid")?)),
            vid: scope.venue_order_id(f.get("vid")?)?,
            inst: f.inst(specs)?,
            side: f.side()?,
            state: VenueOrderState::Open,
            px: Some(Ticks(f.num("px")?)),
            qty,
            cum_filled,
            post_only: None,
            reduce_only: None,
        })
    }

    /// Asks for the resync of instant `at`, and again after `RPC_TIMEOUT` unless answered.
    fn ask_resync(at: WallNs, fx: &mut Effects) {
        let request = format!("snapshot|ts={}", at.0);
        let charge = RESYNC_CHARGE;
        fx.push(send(
            EXEC_STREAM,
            &request,
            None,
            TrafficClass::Safety,
            charge,
        ));
        let after = RPC_TIMEOUT;
        fx.push(Effect::Timer {
            tag: RESYNC_TAG,
            after,
        });
    }

    /// A resync answer, one frame: `rbegin`, then `rorder` and `rpos` records, then `rend`.
    /// The whole envelope is checked before anything is returned, and the answer must echo the
    /// instant of the resync asked for, `requested`, which is its watermark.
    fn resync_frame(
        lines: &[&str],
        requested: Option<WallNs>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
    ) -> Result<Vec<ExecEvent>, DecodeError> {
        let [first, body @ .., last] = lines else {
            return Err(DecodeError::Malformed("resync envelope"));
        };
        let begin = Frame::parse(first)?;
        if begin.kind != "rbegin" || Frame::parse(last)?.kind != "rend" {
            return Err(DecodeError::Malformed("resync envelope"));
        }
        let watermark = requested.ok_or(DecodeError::Malformed("no resync asked for"))?;
        if WallNs(begin.num("wm")?) != watermark {
            return Err(DecodeError::Malformed("resync for another request"));
        }
        let mut events = vec![ExecEvent::ResyncBegin { watermark }];
        for line in body {
            let f = Frame::parse(line)?;
            events.push(match f.kind {
                "rorder" => ExecEvent::ResyncOrder(ToyExec::snapshot(&f, scope, specs)?),
                "rpos" => {
                    let (inst, qty, avg_entry) = f.position(specs)?;
                    ExecEvent::ResyncPosition {
                        inst,
                        qty,
                        avg_entry,
                    }
                }
                _ => return Err(DecodeError::Malformed("resync record")),
            });
        }
        events.push(ExecEvent::ResyncEnd);
        Ok(events)
    }

    fn event(
        f: &Frame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
    ) -> Result<ExecEvent, DecodeError> {
        Ok(match f.kind {
            "pos" => {
                let (inst, qty, avg_entry) = f.position(specs)?;
                ExecEvent::Position {
                    inst,
                    qty,
                    avg_entry,
                }
            }
            "bal" => {
                let (equity, available) = (f.usdc("equity")?, f.usdc("avail")?);
                ExecEvent::Balance { equity, available }
            }
            "fundpay" => {
                let (inst, amount) = (f.inst(specs)?, f.usdc("amt")?);
                ExecEvent::FundingPaid { inst, amount }
            }
            // Every field the caps promise is required: fill id, client id, liquidity, realized
            // P&L and realized funding.
            "fill" => ExecEvent::Fill(FillEvent {
                ident: FillIdent::Venue {
                    fill: scope.fill_id(f.get("fid")?)?,
                    vid: Some(scope.venue_order_id(f.get("vid")?)?),
                    cum_after: Some(f.lots("cum")?),
                },
                cid: Some(scope.client_order_id(f.get("cid")?)),
                inst: f.inst(specs)?,
                side: f.side()?,
                px: Ticks(f.num("px")?),
                qty: f.lots("qty")?,
                liquidity: match f.get("liq")? {
                    "M" => Liquidity3::Maker,
                    "T" => Liquidity3::Taker,
                    _ => return Err(DecodeError::Malformed("liq")),
                },
                fee: scope.fee(f.num("fee")?, usdc())?,
                realized_pnl: Some(f.usdc("pnl")?),
                realized_funding: Some(f.usdc("fund")?),
                replay: false,
            }),
            // A placement accepted: the venue's id for it, stated once on the item.
            "ack" => {
                let vid = Some(scope.venue_order_id(f.get("vid")?)?);
                let item = Some(ItemRef {
                    idx: 0,
                    cid: None,
                    vid,
                });
                let outcome = SubmitOutcome::Accepted {
                    ack: AckLevel::Final,
                };
                let rpc = RpcId(f.num("rpc")?);
                ExecEvent::Outcome { rpc, item, outcome }
            }
            _ => return Err(DecodeError::Malformed("kind")),
        })
    }
}

impl ExecCodec for ToyExec {
    fn nonces_for(&self, _call: CtxCall) -> u16 {
        0
    }

    /// Says hello with the key, which the frame marks as a credential span.
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        let head = format!("hello|ts={}|key=", ctx.wall.0);
        let mut bytes = head.clone().into_bytes();
        bytes.extend_from_slice(self.key.expose().as_bytes());
        let span = head.len() as u32..bytes.len() as u32;
        let frame = WireSlice::redacted(bytes, vec![span]).unwrap();
        fx.push(Effect::Send {
            stream,
            frame,
            rpc: None,
            class: TrafficClass::Safety,
            charge: CONTROL,
        });
    }

    /// Encodes and signs a limit order, its time and nonce from `ctx` alone, marking the signer
    /// call through `t`; the toy offers nothing else.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let VenueCommand::Place(o) = cmd else {
            return Err(NotSentReason::Unsupported);
        };
        let spec = specs.get(o.inst).ok_or(NotSentReason::Unencodable)?;
        let caps = toy_caps().exec.expect("the toy takes orders").order;
        if !(caps.tifs.contains(o.tif) && caps.channels.contains(o.channel)) {
            return Err(NotSentReason::Unsupported);
        }
        let px = o.kind.limit_px().ok_or(NotSentReason::Unsupported)?;
        let cid = encode_cid(&CID_FORMAT, o.cid).map_err(|_| NotSentReason::Unencodable)?;
        let mut receipt = EncodeReceipt::new();
        let nonce = receipt.use_nonce(ctx, 0);
        let nonce = nonce.ok_or(NotSentReason::Unencodable)?;
        let wire = PlaceWire {
            spec,
            cid: cid.as_str(),
            side: o.side,
            kind: o.kind,
            qty: o.qty,
            tif: o.tif,
            channel: o.channel,
            post_only: o.post_only,
            reduce_only: o.reduce_only,
            wall: ctx.wall,
            nonce: Some(nonce),
        };
        // The signer call is the sign stage; the marks give back no time (0034).
        let sig = t.span(PathStage::Sign, || toy_sign(&wire));
        let frame = format!(
            "rpc={}\nplace|cid={cid}|sym={}|side={}|px={}|qty={}|po={}|ro={}|ts={}|nonce={nonce}\
             |sig={}",
            rpc.0,
            spec.venue_symbol.as_wire(),
            if o.side == Side::Buy { "B" } else { "S" },
            px.0,
            o.qty.get(),
            u8::from(o.post_only),
            u8::from(o.reduce_only),
            ctx.wall.0,
            String::from_utf8_lossy(sig.as_bytes()),
        );
        // An order counts against its instrument's limit.
        let charge = RateCharge::one(OpKind::Place, Some(o.inst));
        let class = cmd.traffic_class();
        fx.push(send(EXEC_STREAM, &frame, Some(rpc), class, charge));
        Ok(receipt)
    }

    fn on_frame(
        &mut self,
        _stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let lines: Vec<&str> = text(f)?.lines().collect();
        let first = Frame::parse(lines.first().copied().unwrap_or(""))?;
        let (meta, events) = match lines.len() {
            _ if first.kind == "rbegin" => {
                let events = ToyExec::resync_frame(&lines, self.resync_at, scope, specs)?;
                self.resync_at = None;
                (VenueMeta::NONE, events)
            }
            1 => (first.meta()?, vec![ToyExec::event(&first, scope, specs)?]),
            _ => return Err(DecodeError::Malformed("one record per frame")),
        };
        events.into_iter().for_each(|ev| sink.push(meta, ev));
        Ok(())
    }

    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn ExecSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed("the toy asks for no HTTP"))
    }

    /// The resync timer: a resync still unanswered is asked for again, with the same instant.
    fn on_timer(&mut self, tag: TimerTag, _ctx: &EncodeCtx, fx: &mut Effects) {
        if let (RESYNC_TAG, Some(at)) = (tag, self.resync_at) {
            ToyExec::ask_resync(at, fx);
        }
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let (item, outcome) = (None, SubmitOutcome::Unknown);
        sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
    }

    /// One resync in flight: while one is unanswered, its timer chain asks again.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        if self.resync_at.is_none() {
            self.resync_at = Some(ctx.wall);
            ToyExec::ask_resync(ctx.wall, fx);
        }
    }
    /// The toy authenticates with no credential, so its frames carry none.
    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}

/// The one request discovery makes: its market list, a REST request no limit of the toy's
/// counts.
fn markets_request() -> Effect {
    Effect::Http {
        tag: MARKETS_TAG,
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(MARKETS_URL),
            headers: vec![],
            body: WireSlice::plain(vec![]),
        },
        rpc: None,
        timeout: RPC_TIMEOUT,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Rest, None),
    }
}

/// A field the market record must state: refused by name when it does not.
fn required<'a>(frame: &Frame<'a>, key: &'static str) -> Result<&'a str, PlanError> {
    frame.opt(key).ok_or(PlanError::Missing(key))
}

/// A field that must parse as a `T`; a value that does not is malformed.
fn read<T: FromStr>(frame: &Frame<'_>, key: &'static str) -> Result<T, PlanError> {
    let bad = DecodeError::Malformed(key);
    required(frame, key)?.parse().map_err(|_| bad.into())
}

fn asset_sym(frame: &Frame<'_>, key: &'static str) -> Result<AssetSym, PlanError> {
    let bad = DecodeError::Malformed(key);
    AssetSym::new(required(frame, key)?).ok_or(bad.into())
}

/// One market record as a draft, its symbol built by the decode scope. Every field the toy's
/// protocol carries is required; what it never states (funding, fees, limits) is said
/// explicitly. A missing field refuses the record by name.
fn draft(line: &str, scope: &DecodeScope<'_>) -> Result<InstrumentSpecDraft, PlanError> {
    let frame = Frame::parse(line)?;
    if frame.kind != "market" {
        return Err(DecodeError::Malformed("kind").into());
    }
    let venue_symbol = scope.venue_symbol(required(&frame, "sym")?)?;
    let asset = AssetKey {
        base: asset_sym(&frame, "base")?,
        quote: asset_sym(&frame, "quote")?,
        kind: InstrumentKind::Perpetual,
    };
    let settle_ccy = asset_sym(&frame, "settle")?;
    let price_grid = PriceGrid::fixed(read(&frame, "tick")?);
    let price_grid = price_grid.map_err(|_| DecodeError::Malformed("tick"))?;
    let size_step = SizeStep::new(read(&frame, "step")?);
    let size_step = size_step.ok_or(DecodeError::Malformed("step"))?;
    let min_size = Lots::new(read(&frame, "min")?).ok_or(DecodeError::Malformed("min"))?;
    let status = match required(&frame, "status")? {
        "trading" => TradingStatus::Trading,
        "halted" => TradingStatus::Halted,
        _ => return Err(DecodeError::Malformed("status").into()),
    };
    Ok(InstrumentSpecDraft {
        asset,
        venue_symbol,
        native_id: None,
        price_grid,
        quote_grid: None,
        size_step,
        min_size,
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        settle_ccy,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status,
    })
}

/// The one request the toy's `test_connection` makes: a `GET` of the account with the key in a
/// redacted header, which the plan's `Debug` and the journal never show.
fn account_request(key: &Secret) -> Effect {
    let key = Header {
        name: KEY_HEADER,
        value: key.expose().to_owned(),
        redact: true,
    };
    Effect::Http {
        tag: ACCOUNT_TAG,
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(ACCOUNT_URL),
            headers: vec![key],
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout: RPC_TIMEOUT,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Query, None),
    }
}

/// The account answer, `account|id=...|equity=...` with equity in nanos of USDC, as the summary
/// test_connection ends in. A field it leaves out refuses it by name.
fn account(text: &str) -> Result<AccountSummary, PlanError> {
    let frame = Frame::parse(text)?;
    if frame.kind != "account" {
        return Err(DecodeError::Malformed("kind").into());
    }
    let account = required(&frame, "id")?.to_owned();
    let equity = Money::new(read(&frame, "equity")?, usdc());
    Ok(AccountSummary {
        account,
        equity: Some(equity),
    })
}

/// The toy's key, moved out of `creds`, or the configuration refusal that names it missing.
fn take_key(mut creds: Secrets) -> Result<Secret, VenueError> {
    creds
        .take(KEY)
        .ok_or(VenueError::Config(ConfigError::Missing(KEY)))
}

/// The toy venue's factory. The toy reads one key, its credential.
struct ToyFactory;

impl VenueFactory for ToyFactory {
    fn id(&self) -> &'static str {
        "TOY"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        SCHEMA
    }

    fn caps(&self, _cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(toy_caps())
    }

    /// The toy's FBC rule: `X/USDT` is `X-PERP`, which the toy lists in USDC; the ticker's
    /// quote is kept as written, and the alias table reads both as USD.
    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        let (base, quote) = common_symbol_parts(s)?;
        if quote.as_str() != "USDT" {
            return Err(SymbolError::Unmapped);
        }
        let kind = InstrumentKind::Perpetual;
        Ok(AssetKey { base, quote, kind })
    }

    /// One `GET` of the market list; each line of the answer is one market.
    fn discover(
        &self,
        _cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        let mut fx = Effects::new();
        fx.push(markets_request());
        let plan = HttpPlan::new(fx, |responses, scope| {
            // The plan holds one request, so its parser is handed one response.
            let list = responses.first().ok_or(PlanError::Answers)?;
            let text = core::str::from_utf8(list.body);
            let text = text.map_err(|_| DecodeError::Malformed("utf-8"))?;
            text.lines().map(|line| draft(line, scope)).collect()
        });
        Ok(plan.expect("one HTTP request makes a valid plan"))
    }

    fn plan_md(
        &self,
        _cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        subs.iter()
            .try_for_each(|sub| spell(specs, *sub).map(drop))?;
        if subs.is_empty() {
            return Ok(Vec::new());
        }
        let url = WireUrl::plain(MD_URL);
        Ok(vec![EndpointPlan {
            stream: MD_STREAM,
            transport: MdTransport::Socket { url },
            subs: subs.iter().copied().collect(),
        }])
    }

    fn md_codec(&self, _cfg: &VenueConfig, _ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(ToyMd)
    }

    fn plan_exec(&self, _cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let url = WireUrl::plain(EXEC_URL);
        Ok(vec![ExecEndpoint {
            stream: EXEC_STREAM,
            url,
        }])
    }

    fn exec_codec(
        &self,
        _cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(take_key(creds).map(|key| {
            let codec: Box<dyn ExecCodec> = Box::new(ToyExec {
                resync_at: None,
                key,
            });
            codec
        }))
    }

    fn test_connection(
        &self,
        _cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        Some(take_key(creds).map(|key| {
            let mut fx = Effects::new();
            fx.push(account_request(&key));
            // The key is in the request; the parser needs none of it, and the `Secret` that
            // held it is zeroed when it drops here.
            let plan = HttpPlan::new(fx, |responses, _scope| {
                // The plan holds one request, so its parser is handed one response.
                let answer = responses.first().ok_or(PlanError::Answers)?;
                let text = core::str::from_utf8(answer.body);
                account(text.map_err(|_| DecodeError::Malformed("utf-8"))?)
            });
            plan.expect("one HTTP request makes a valid plan")
        }))
    }
}

/// What the toy does, and nothing more.
fn toy_caps() -> VenueCaps {
    VenueCaps {
        exec: Some(ExecCaps {
            order: OrderCaps {
                kinds: TagSet::of(&[OrderKindTag::Limit]),
                tifs: TagSet::of(&[TifTag::Gtc]),
                channels: TagSet::of(&[Channel::Public]),
                post_only: true,
                reduce_only: true,
                flag_conflicts: vec![],
                amend: None,
                cancel_refs: TagSet::none(),
                query_refs: TagSet::none(),
                cancel_before_ack: false,
                cancel_is_signed: false,
                batch_place: None,
                batch_cancel: None,
                cancel_all_account: Support::Unsupported,
                cancel_all_instrument: Support::Unsupported,
                cancel_on_disconnect: CancelOnDisconnect::None,
                ack: AckModel::SinglePhase,
                client_id: CID_FORMAT,
                cid_echoed_on_events: true,
                nonce_scope: NonceScope::PerAccountMonotonic,
                ordering_key: OrderingKey::VenueSeq,
                snapshot_source: SnapshotSource::Trustworthy,
                events_echo_flags: false,
                sign_cost_hint_us: 0,
            },
            fills: FillCaps {
                source: FillSource::Native,
                liquidity_flag: true,
                realized_pnl: true,
                realized_funding: true,
                fee_sign: FEE_SIGN,
                fee_asset_reported: false,
                fill_id: true,
                replays_fills_on_reconnect: false,
            },
        }),
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            // Pipe-delimited `key=value` text (the protocol at the top of this file).
            encoding: Encoding::Text,
            touch_sources: vec![TouchSourceCaps {
                channel: "touch",
                cadence: Cadence::Realtime,
                seq_domain: SeqDomain::Own,
                ts_kind: ExchTsKind::MatchingEngine,
                includes_channels: TagSet::of(&[Channel::Public]),
            }],
            books: vec![],
            trades: TradeCaps {
                source: FeedSource::None,
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
        // Each limit counts traffic the toy sends (decision 0018).
        limits: vec![
            RateLimit {
                scope: LimitScope::Pair,
                ops: TagSet::of(&[OpKind::Place]),
                per: Duration::from_secs(1),
                units: 10,
            },
            RateLimit {
                scope: LimitScope::Account,
                ops: TagSet::of(&[OpKind::Query]),
                per: Duration::from_secs(60),
                units: 100,
            },
            RateLimit {
                scope: LimitScope::Connection,
                ops: TagSet::of(&[OpKind::Subscribe, OpKind::Control]),
                per: Duration::from_secs(1),
                units: 5,
            },
        ],
        readiness_ceiling: Readiness::Record,
    }
}

// ---------------------------------------------------------------------------------------------
// Test harness: the runtime's part, done by hand.
// ---------------------------------------------------------------------------------------------

/// What the runtime would record for the frame being decoded.
const STAMP: Stamp = Stamp {
    ingest_seq: 42,
    kernel_rx: None,
    recv_mono: MonoNs(1_000),
    recv_wall: WallNs(1_759_363_200_123_000_000),
    conn: ConnKey { conn: 1, epoch: 3 },
};

/// A sink that stamps what a codec reports, as the runtime does.
struct Collect<B>(Vec<Envelope<B>>);

impl<B> Collect<B> {
    fn bodies(&self) -> Vec<&B> {
        self.0.iter().map(|e| &e.body).collect()
    }
}

impl MdSink for Collect<MdEvent> {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push(Envelope::new(STAMP, meta, ev));
    }
}

impl ExecSink for Collect<ExecEvent> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push(Envelope::new(STAMP, meta, ev));
    }
}

/// Runs `f` with the decode scope the core's dispatch lends for the toy venue's caps.
fn with_scope<R>(f: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R) -> R {
    dispatch(&toy_caps(), OWN_NS, f)
}

fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    table.insert(InstrumentSpec {
        id: INST,
        venue: VenueId::new(9),
        venue_symbol: with_scope(|scope| scope.venue_symbol(SYMBOL)).unwrap(),
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed(Decimal::new(5, 1)).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new(Decimal::new(1, 3)).unwrap(),
        min_size: Lots::new(1).unwrap(),
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
    });
    table
}

/// A synthetic key: no venue or account uses it (0009).
const SYNTHETIC_KEY: &str = "SYNTHETIC-toy-key-7f3a";

/// The toy's credentials, holding the synthetic key.
fn creds() -> Secrets {
    let mut creds = Secrets::new();
    creds.insert(KEY, Secret::new(SYNTHETIC_KEY.to_owned()));
    creds
}

/// The hello the toy's exec codec says on open at `wall`: the synthetic key in a redaction span.
fn hello(wall: i64) -> Effect {
    let head = format!("hello|ts={wall}|key=");
    let text = format!("{head}{SYNTHETIC_KEY}");
    let span = head.len() as u32..text.len() as u32;
    Effect::Send {
        stream: EXEC_STREAM,
        frame: WireSlice::redacted(text.into_bytes(), vec![span]).unwrap(),
        rpc: None,
        class: TrafficClass::Safety,
        charge: CONTROL,
    }
}

fn exec_codec() -> Box<dyn ExecCodec> {
    ToyFactory
        .exec_codec(&VenueConfig::new(), creds())
        .unwrap()
        .unwrap()
}

/// Decodes `frames` through `codec` inside the decode scope: each result and what was pushed.
fn decode_exec(
    codec: &mut dyn ExecCodec,
    frames: &[&str],
) -> (Vec<Result<(), DecodeError>>, Collect<ExecEvent>) {
    let (specs, mut sink, mut fx) = (specs(), Collect(Vec::new()), Effects::new());
    let results = with_scope(|scope| {
        let mut decode = |f| codec.on_frame(EXEC_STREAM, f, scope, &specs, &mut sink, &mut fx);
        frames.iter().map(|f| decode(RawFrame::Text(f))).collect()
    });
    assert!(fx.is_empty(), "decoding asked for effects: {fx:?}");
    (results, sink)
}

/// Decodes `frames` through the market-data codec: each result and what was pushed.
fn decode_md(frames: &[RawFrame<'_>]) -> (Vec<Result<(), DecodeError>>, Collect<MdEvent>) {
    let (specs, mut sink, mut fx) = (specs(), Collect(Vec::new()), Effects::new());
    let results = with_scope(|scope| {
        let mut decode = |f| ToyMd.on_frame(f, scope, &specs, &mut sink, &mut fx);
        frames.iter().map(|f| decode(*f)).collect()
    });
    assert!(fx.is_empty(), "decoding asked for effects: {fx:?}");
    (results, sink)
}

/// A client id minted in the toy's namespace, under a lease in a fresh directory.
fn mint() -> ClientOrderId {
    static N: AtomicU32 = AtomicU32::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fbc-toy-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let lease = NamespaceLease::acquire(&dir, fbc_core::AccountKey::new(1), OWN_NS).unwrap();
    let cid = CidMint::new(lease, 0, 0, WallNs(1_759_363_200_000_000_000)).mint();
    let _ = std::fs::remove_dir_all(&dir);
    cid.unwrap()
}

fn order(cid: ClientOrderId) -> NewOrder {
    NewOrder {
        cid,
        inst: INST,
        side: Side::Buy,
        qty: Lots::new(25).unwrap(),
        kind: OrderKind::Limit { px: Ticks(130_865) },
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

fn ctx(wall: i64, nonces: &[u64]) -> EncodeCtx {
    let (wall, mono) = (WallNs(wall), MonoNs(77));
    let nonces = NonceBlock::new(nonces.to_vec());
    EncodeCtx { wall, mono, nonces }
}

/// Encodes `cmd` as request 11 with a fresh codec: the receipt and the effects asked for, or
/// why it was not sent (and then nothing was asked for).
fn encode(
    cmd: &VenueCommand,
    ctx: &EncodeCtx,
) -> Result<(EncodeReceipt, Vec<Effect>), NotSentReason> {
    let mut fx = Effects::new();
    let off = &mut PathStamps::off();
    let receipt = exec_codec().encode(cmd, RpcId(11), &specs(), ctx, off, &mut fx);
    assert!(
        receipt.is_ok() || fx.is_empty(),
        "a command not sent pushed {fx:?}"
    );
    Ok((receipt?, fx.take()))
}

/// The bytes of the one frame `cmd` encodes to under `ctx`.
fn bytes(cmd: &VenueCommand, ctx: &EncodeCtx) -> Vec<u8> {
    match encode(cmd, ctx).unwrap().1.as_slice() {
        [Effect::Send { frame, .. }] => frame.bytes().to_vec(),
        other => panic!("expected one frame, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn the_toy_decodes_account_events_a_full_resync_and_a_fill_into_their_exec_events() {
    let cid = mint();
    let wire_cid = encode_cid(&CID_FORMAT, cid).unwrap();
    let resync = format!(
        "rbegin|wm=1759363200050000000\n\
         rorder|sym=TOY-PERP|vid=V-1|cid={wire_cid}|side=B|px=130865|qty=25|cum=5\n\
         rpos|sym=TOY-PERP|qty=-25|avg=65432.125\n\
         rend"
    );
    let fill = format!(
        "fill|ts=1759363200200000000|seq=4|sym=TOY-PERP|fid=F-9|vid=V-1|cid={wire_cid}\
         |side=B|px=130865|qty=3|cum=8|liq=M|fee=150|pnl=2500000|fund=-30000"
    );
    let frames = [
        "pos|ts=1759363200100000000|seq=1|sym=TOY-PERP|qty=-25|avg=65432.125",
        "bal|ts=1759363200100000001|seq=2|equity=1250000000000|avail=900000000000",
        "fundpay|ts=1759363200100000002|seq=3|sym=TOY-PERP|amt=-1250000",
        &resync,
        &fill,
    ];
    // The resync was asked for at the watermark instant; the reply echoes it.
    let mut codec = exec_codec();
    codec.resync(&ctx(1_759_363_200_050_000_000, &[]), &mut Effects::new());
    let (results, sink) = decode_exec(codec.as_mut(), &frames);
    assert!(results.iter().all(Result::is_ok), "{results:?}");

    let (vid, fid, fee) = with_scope(|scope| {
        let vid = scope.venue_order_id("V-1").unwrap();
        (
            vid,
            scope.fill_id("F-9").unwrap(),
            scope.fee(150, usdc()).unwrap(),
        )
    });
    // The toy reports a 150-nano rebate as +150; the scope applied its declared sign.
    assert_eq!(fee.cost(), Money::new(-150, usdc()));
    let (lots, money) = (|n| Lots::new(n).unwrap(), |n| Money::new(n, usdc()));
    let (qty, avg_entry) = (
        SignedLots(-25),
        Some("65432.125".parse::<PxExact>().unwrap()),
    );
    let expected = [
        ExecEvent::Position {
            inst: INST,
            qty,
            avg_entry,
        },
        ExecEvent::Balance {
            equity: money(1_250_000_000_000),
            available: money(900_000_000_000),
        },
        ExecEvent::FundingPaid {
            inst: INST,
            amount: money(-1_250_000),
        },
        ExecEvent::ResyncBegin {
            watermark: WallNs(1_759_363_200_050_000_000),
        },
        ExecEvent::ResyncOrder(VenueOrderSnapshot {
            cid: Some(CidMatch::Ours(cid)),
            vid: vid.clone(),
            inst: INST,
            side: Side::Buy,
            state: VenueOrderState::Open,
            px: Some(Ticks(130_865)),
            qty: lots(25),
            cum_filled: lots(5),
            post_only: None,
            reduce_only: None,
        }),
        ExecEvent::ResyncPosition {
            inst: INST,
            qty,
            avg_entry,
        },
        ExecEvent::ResyncEnd,
        ExecEvent::Fill(FillEvent {
            ident: FillIdent::Venue {
                fill: fid,
                vid: Some(vid),
                cum_after: Some(lots(8)),
            },
            cid: Some(CidMatch::Ours(cid)),
            inst: INST,
            side: Side::Buy,
            px: Ticks(130_865),
            qty: lots(3),
            liquidity: Liquidity3::Maker,
            fee,
            realized_pnl: Some(money(2_500_000)),
            realized_funding: Some(money(-30_000)),
            replay: false,
        }),
    ];
    assert_eq!(sink.bodies(), expected.iter().collect::<Vec<_>>());

    // Every envelope carries the runtime's stamp; the venue's time and sequence are kept as
    // sent, and a frame without them gets none (exchange time is never synthesized).
    assert!(sink.0.iter().all(|e| e.stamp == STAMP));
    let fill = sink.0.last().unwrap();
    let meta = (fill.exch_ts, fill.exch_ts_kind, fill.venue_seq);
    let ts = Some(ExchNs(1_759_363_200_200_000_000));
    assert_eq!(meta, (ts, ExchTsKind::MatchingEngine, Some(4)));
    assert_eq!(sink.0[3].meta(), VenueMeta::NONE);
}

#[test]
fn encoding_the_same_command_with_the_same_encode_ctx_twice_gives_identical_bytes() {
    let cmd = VenueCommand::Place(order(mint()));
    let at = ctx(1_759_363_200_300_000_000, &[9_000]);
    let mut codec = exec_codec();
    let twice = [0, 1].map(|_| {
        let mut fx = Effects::new();
        let off = &mut PathStamps::off();
        codec
            .encode(&cmd, RpcId(11), &specs(), &at, off, &mut fx)
            .unwrap();
        fx.take()
    });
    // The same codec twice, and a fresh one: codec state does not leak into the bytes.
    assert_eq!(twice[0], twice[1]);
    assert_eq!(twice[0], encode(&cmd, &at).unwrap().1);
}

/// A path recorder with a clock of its own, as the runtime's reads its: each mark is kept at the
/// next tick, `step` nanoseconds after the one before, from `from`.
struct Tape {
    at: u64,
    step: u64,
    marks: Vec<(PathMark, MonoNs)>,
}

impl Tape {
    fn new(from: u64, step: u64) -> Tape {
        let marks = Vec::new();
        Tape {
            at: from,
            step,
            marks,
        }
    }

    /// The marks without their times.
    fn stages(&self) -> Vec<PathMark> {
        self.marks.iter().map(|(mark, _)| *mark).collect()
    }
}

impl PathRecorder for Tape {
    fn mark(&mut self, mark: PathMark) {
        self.marks.push((mark, MonoNs(self.at)));
        self.at += self.step;
    }
}

const fn mark(stage: PathStage, edge: PathEdge) -> PathMark {
    PathMark { stage, edge }
}

const SIGN_START: PathMark = mark(PathStage::Sign, PathEdge::Start);
const SIGN_END: PathMark = mark(PathStage::Sign, PathEdge::End);

#[test]
fn encode_marks_its_signer_call_and_its_bytes_are_the_same_whatever_the_stamps_recorded() {
    // FBC-ji6, decision 0034: the codec marks the start and end of its signer call through
    // PathStamps, and learns no time from it, so the same command under the same context encodes
    // to the same bytes whether the marks are recorded at one time, another, or not at all.
    let cmd = VenueCommand::Place(order(mint()));
    let at = ctx(1_759_363_200_300_000_000, &[9_000]);
    let run = |t: &mut PathStamps<'_>| {
        let mut fx = Effects::new();
        let receipt = exec_codec().encode(&cmd, RpcId(11), &specs(), &at, t, &mut fx);
        (receipt.unwrap(), fx.take())
    };
    let (mut early, mut late) = (Tape::new(0, 1), Tape::new(1 << 62, 450_000));
    let off = run(&mut PathStamps::off());
    assert_eq!(run(&mut PathStamps::new(&mut early)), off);
    assert_eq!(run(&mut PathStamps::new(&mut late)), off);
    assert_eq!(off, encode(&cmd, &at).unwrap());

    // Each recorder kept the sign stage at its own times.
    assert_eq!(
        early.marks,
        [(SIGN_START, MonoNs(0)), (SIGN_END, MonoNs(1))]
    );
    assert_eq!(late.stages(), [SIGN_START, SIGN_END]);
    assert_eq!(
        late.marks[1].1 - late.marks[0].1,
        Duration::from_nanos(450_000)
    );

    // A command refused before it is signed marks nothing.
    let ioc = VenueCommand::Place(NewOrder {
        tif: TifTag::Ioc,
        ..order(mint())
    });
    let mut refused = Tape::new(0, 1);
    let mut fx = Effects::new();
    let t = &mut PathStamps::new(&mut refused);
    let result = exec_codec().encode(&ioc, RpcId(12), &specs(), &at, t, &mut fx);
    assert_eq!(result.err(), Some(NotSentReason::Unsupported));
    assert!(refused.marks.is_empty() && fx.is_empty());
}

/// A gateway over the toy's codec, as a live one is without its sockets: it marks the encode
/// stage around the codec's call, hands the codec the same stamps to mark its signer call, and
/// keeps what the codec asked to send. A real gateway implements `fbc-oms`'s `OrderGateway`,
/// whose submit takes the command inside an authorization only `fbc-oms` issues (decision
/// 0045); this crate cannot name that type, so the toy takes the command it would hold.
struct ToyGateway {
    codec: Box<dyn ExecCodec>,
    next_rpc: u64,
    sent: Vec<Effect>,
}

impl ToyGateway {
    fn submit(
        &mut self,
        cmd: VenueCommand,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> SubmitHandle {
        let rpc = RpcId(self.next_rpc);
        self.next_rpc += 1;
        let mut fx = Effects::new();
        t.start(PathStage::Encode);
        let receipt = self.codec.encode(&cmd, rpc, &specs(), ctx, t, &mut fx);
        t.end(PathStage::Encode);
        self.sent.extend(fx.take());
        SubmitHandle { rpc, receipt }
    }
}

#[test]
fn submit_threads_path_stamps_through_encode_to_the_signer_call() {
    let mut gateway = ToyGateway {
        codec: exec_codec(),
        next_rpc: 11,
        sent: Vec::new(),
    };
    let cmd = VenueCommand::Place(order(mint()));
    let at = ctx(1_759_363_200_300_000_000, &[9_000]);
    let mut tape = Tape::new(1_000, 10);
    let handle = gateway.submit(cmd.clone(), &at, &mut PathStamps::new(&mut tape));

    // The sign stage nests inside the encode stage, every mark at the recorder's own time.
    let (encode_start, encode_end) = (
        mark(PathStage::Encode, PathEdge::Start),
        mark(PathStage::Encode, PathEdge::End),
    );
    let times = [1_000, 1_010, 1_020, 1_030].map(MonoNs);
    let marks = [encode_start, SIGN_START, SIGN_END, encode_end];
    assert_eq!(tape.marks, marks.into_iter().zip(times).collect::<Vec<_>>());

    // The request went out as the codec encodes it without stamps.
    assert_eq!(handle.rpc, RpcId(11));
    assert_eq!(handle.receipt.unwrap().nonces(), [(0, 9_000)]);
    assert_eq!(gateway.sent, encode(&cmd, &at).unwrap().1);
}

#[test]
fn encode_takes_time_and_nonces_only_from_the_encode_ctx() {
    let cid = mint();
    let cmd = VenueCommand::Place(order(cid));
    let at = ctx(1_759_363_200_300_000_000, &[9_000]);
    let (receipt, effects) = encode(&cmd, &at).unwrap();
    let wire_cid = encode_cid(&CID_FORMAT, cid).unwrap();
    let frame = format!(
        "rpc=11\nplace|cid={wire_cid}|sym=TOY-PERP|side=B|px=130865|qty=25|po=1|ro=0\
         |ts=1759363200300000000|nonce=9000|sig=ed3eb2c5eed9bf85"
    );
    // One frame with its deadline and its command's traffic class; the receipt names the
    // nonce the context reserved, for the OMS to keep.
    let (rpc, charge) = (Some(RpcId(11)), RateCharge::one(OpKind::Place, Some(INST)));
    assert_eq!(
        effects,
        [send(EXEC_STREAM, &frame, rpc, TrafficClass::Normal, charge)]
    );
    assert_eq!(receipt.nonces(), [(0, 9_000)]);
    let mut fx = Effects::new();
    effects.iter().cloned().for_each(|e| fx.push(e));
    assert!(fx.carry_request(RpcId(11), cmd.traffic_class()));

    // Another wall time or nonce changes the bytes; another monotonic time does not, since
    // nothing in the payload is a monotonic instant.
    let same = bytes(&cmd, &at);
    assert_ne!(bytes(&cmd, &ctx(at.wall.0 + 1, &[9_000])), same);
    assert_ne!(bytes(&cmd, &ctx(at.wall.0, &[9_001])), same);
    let other_mono = EncodeCtx {
        mono: MonoNs(1),
        ..at.clone()
    };
    assert_eq!(bytes(&cmd, &other_mono), same);

    // Without the nonce in the context the command is not sent, nor is a command or a time in
    // force the toy does not offer; nothing is pushed for any of them.
    let ioc = VenueCommand::Place(NewOrder {
        tif: TifTag::Ioc,
        ..order(cid)
    });
    let unsupported = Some(NotSentReason::Unsupported);
    let unencodable = Some(NotSentReason::Unencodable);
    assert_eq!(encode(&cmd, &ctx(1, &[])).err(), unencodable);
    assert_eq!(encode(&ioc, &at).err(), unsupported);
    assert_eq!(encode(&VenueCommand::FeeQuery, &at).err(), unsupported);
}

#[test]
fn the_toy_charges_its_encode_resync_keepalive_and_subscribe_traffic() {
    // FBC-hof, decision 0018: every frame the toy asks for, and its ping, carries the charge its
    // venue counts it as, including traffic it sends on its own (the hello, a resync and its
    // retry), so the runtime can charge each to the right bucket.
    let mut fx = Effects::new();
    let place = VenueCommand::Place(order(mint()));
    let off = &mut PathStamps::off();
    exec_codec()
        .encode(&place, RpcId(11), &specs(), &ctx(1, &[1]), off, &mut fx)
        .unwrap();
    let touch = Subscription {
        inst: INST,
        feed: Feed::Touch(TouchSourceId(0)),
    };
    ToyMd
        .subscribe(&[touch], &[touch], &specs(), &mut fx)
        .unwrap();
    let mut exec = exec_codec();
    exec.on_open(EXEC_STREAM, &ctx(1_000, &[]), &mut fx);
    exec.resync(&ctx(2_000, &[]), &mut fx);
    exec.on_timer(RESYNC_TAG, &ctx(9_000, &[]), &mut fx);
    // Every request is a frame (the toy asks for no HTTP), and so is the ping.
    let mut sent: Vec<(RateCharge, Via)> =
        fx.as_slice().iter().filter_map(Effect::charge).collect();
    sent.push((ToyMd.keepalive().unwrap().charge, Via::Frame));
    assert!(sent.iter().all(|(_, via)| *via == Via::Frame));
    let charges: Vec<RateCharge> = sent.iter().map(|(charge, _)| *charge).collect();

    // The order names its instrument, the resync costs its weight, and the subscriptions, hello
    // and ping are counted on their connection.
    let subscribe = RateCharge::one(OpKind::Subscribe, Some(INST));
    let expected = [
        RateCharge::one(OpKind::Place, Some(INST)),
        subscribe,
        subscribe,
        CONTROL,
        RESYNC_CHARGE,
        RESYNC_CHARGE,
        CONTROL,
    ];
    assert_eq!(charges, expected);
    assert_eq!(RESYNC_CHARGE.weight.get(), 5);

    // Each charge is counted by a declared limit, and by every limit that lists its operation:
    // a per-pair limit never misses one for want of its instrument. Each limit counts some of
    // the toy's traffic, so the declaration is no wider than what the codecs send.
    let limits = toy_caps().limits;
    for charge in &charges {
        assert!(
            limits.iter().any(|l| l.counts(charge, Via::Frame)),
            "{charge:?}"
        );
        for limit in limits.iter().filter(|l| l.ops.contains(charge.op)) {
            assert!(
                limit.counts(charge, Via::Frame),
                "{limit:?} misses {charge:?}"
            );
        }
    }
    for limit in &limits {
        assert!(
            charges.iter().any(|c| limit.counts(c, Via::Frame)),
            "{limit:?}"
        );
    }
    let unkeyed = RateCharge::one(OpKind::Place, None);
    assert!(!limits[0].counts(&unkeyed, Via::Frame));
}

// ---------------------------------------------------------------------------------------------
// The rest of the boundary the toy uses.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_subscription_the_venue_refused_is_reported_by_instrument_and_feed_and_nothing_is_retried() {
    // The subscription goes out as one frame.
    let (specs, mut fx) = (specs(), Effects::new());
    let touch = Subscription {
        inst: INST,
        feed: Feed::Touch(TouchSourceId(0)),
    };
    ToyMd.subscribe(&[touch], &[], &specs, &mut fx).unwrap();
    assert_eq!(fx.len(), 1);
    // The venue's refusal is an event naming the instrument and the feed, not a decode error
    // the runtime would only count; decoding it asks for no effect (decode_md checks), so no
    // subscribe is resent and no reconnect is asked for. A refusal naming an instrument the
    // spec table lacks is refused itself, pushing nothing.
    let (results, sink) = decode_md(&[
        RawFrame::Text("refused|sym=TOY-PERP"),
        RawFrame::Text("refused|sym=NOPE-PERP"),
    ]);
    assert_eq!(results, [Ok(()), Err(DecodeError::UnknownInstrument)]);
    let refused = MdEvent::Health {
        inst: touch.inst,
        feed: touch.feed,
        h: FeedHealth::Refused,
    };
    assert_eq!(sink.bodies(), [&refused]);
    assert_eq!(sink.0[0].venue_seq, None);
}

#[test]
fn the_toy_decodes_market_data_frames_into_md_events() {
    let (results, sink) = decode_md(&[
        RawFrame::Text("touch|ts=10|seq=5|sym=TOY-PERP|bid=130865x12|ask=130866x7"),
        RawFrame::Text("touch|seq=6|sym=TOY-PERP|ask=130866x7"),
        // Refused, pushing nothing: a bad level, an unknown instrument or kind, binary.
        RawFrame::Text("touch|seq=8|sym=TOY-PERP|bid=130865x12|ask=130866x-7"),
        RawFrame::Text("touch|seq=7|sym=NOPE-PERP"),
        // The touch channel's own sequence (SeqDomain::Own) is required.
        RawFrame::Text("touch|sym=TOY-PERP|ask=130866x7"),
        RawFrame::Text("trade|sym=TOY-PERP"),
        RawFrame::Binary(b"\x00"),
    ]);
    let ok = results.iter().map(Result::is_ok);
    assert_eq!(
        Vec::from_iter(ok),
        [true, true, false, false, false, false, false]
    );
    let lvl = |px, qty| Lvl {
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    };
    let (bid, ask) = (Some(lvl(130_865, 12)), Some(lvl(130_866, 7)));
    let (inst, source) = (INST, TouchSourceId(0));
    let expected = [
        MdEvent::Touch {
            inst,
            bid,
            ask,
            source,
        },
        MdEvent::Touch {
            inst,
            bid: None,
            ask,
            source,
        },
    ];
    assert_eq!(sink.bodies(), expected.iter().collect::<Vec<_>>());
    let meta = [&sink.0[0], &sink.0[1]].map(|e| (e.exch_ts, e.venue_seq));
    assert_eq!(meta, [(Some(ExchNs(10)), Some(5)), (None, Some(6))]);
}

#[test]
fn a_frame_that_fails_to_decode_pushes_nothing() {
    // A resync without its end, with a bad record, out of order, for an instant other than
    // the one requested, or with an order filled past its quantity; two records in one frame; a fill missing a field its caps promise
    // (fill id, realized funding) or with a negative quantity. A resync that fails pushes no
    // begin without an end.
    let wire_cid = encode_cid(&CID_FORMAT, mint()).unwrap();
    let full = format!(
        "fill|sym=TOY-PERP|fid=F-1|vid=V-1|cid={wire_cid}|side=B|px=1|qty=3|cum=3|liq=M|fee=1\
         |pnl=0|fund=0"
    );
    let without = |key: &str| {
        let kept: Vec<&str> = full.split('|').filter(|kv| !kv.starts_with(key)).collect();
        kept.join("|")
    };
    // Codex r4173320335: a snapshot's quantity includes its filled part.
    let order = |cum: u32| {
        format!(
            "rbegin|wm=900\nrorder|sym=TOY-PERP|vid=V-1|cid={wire_cid}|side=B|px=1|qty=1\
             |cum={cum}\nrend"
        )
    };
    let bad = [
        "rbegin|wm=900\nrpos|sym=TOY-PERP|qty=0".to_owned(),
        "rbegin|wm=900\nrpos|sym=NOPE-PERP|qty=0\nrend".to_owned(),
        "rbegin|wm=900\nrend\nrpos|sym=TOY-PERP|qty=0".to_owned(),
        "rbegin|wm=901\nrend".to_owned(),
        "pos|sym=TOY-PERP|qty=0\nbal|equity=1|avail=1".to_owned(),
        without("fid="),
        without("fund="),
        full.replace("qty=3", "qty=-3"),
        order(2),
    ];
    let bad: Vec<&str> = bad.iter().map(String::as_str).collect();
    let mut codec = exec_codec();
    codec.resync(&ctx(900, &[]), &mut Effects::new());
    let (results, sink) = decode_exec(codec.as_mut(), &bad);
    assert!(results.iter().all(Result::is_err), "{results:?}");
    assert!(sink.0.is_empty());
    // The full fill and the requested resync decode, so each refusal above is the one thing it
    // changed; the same resync again, with none requested, is refused.
    let good = [&order(1), &full, "rbegin|wm=900\nrend"];
    let (results, _) = decode_exec(codec.as_mut(), &good);
    assert_eq!(
        results.iter().map(Result::is_ok).collect::<Vec<_>>(),
        [true, true, false]
    );
}

#[test]
fn the_factory_plans_and_builds_codecs_whose_only_output_is_effects() {
    let (factory, cfg): (&dyn VenueFactory, _) = (&ToyFactory, VenueConfig::new());
    assert_eq!(factory.id(), "TOY");
    assert_eq!(
        Vec::from_iter(factory.config_schema().iter().map(|f| f.key)),
        [KEY]
    );
    assert_eq!(factory.caps(&cfg), Ok(toy_caps()));

    // Planning and subscribing see the spec table, so the venue's own symbol goes on the wire.
    let touch = Subscription {
        inst: INST,
        feed: Feed::Touch(TouchSourceId(0)),
    };
    let plan = factory
        .plan_md(&cfg, &specs(), &BTreeSet::from([touch]))
        .unwrap();
    let url = WireUrl::plain(MD_URL);
    assert_eq!(
        (plan.len(), plan[0].stream, &plan[0].subs),
        (1, MD_STREAM, &vec![touch])
    );
    assert_eq!(plan[0].transport, MdTransport::Socket { url });
    let none = factory.plan_md(&cfg, &specs(), &BTreeSet::new());
    assert_eq!(none, Ok(vec![]));
    let mut md = factory.md_codec(&cfg, &plan[0]);
    let mut fx = Effects::new();
    md.on_open(&mut fx);
    md.subscribe(&[touch], &[touch], &specs(), &mut fx).unwrap();
    let sent = ["sub", "unsub"].map(|verb| {
        let frame = format!("{verb}|sym=TOY-PERP|feed=Touch(TouchSourceId(0))");
        let charge = RateCharge::one(OpKind::Subscribe, Some(INST));
        send(MD_STREAM, &frame, None, TrafficClass::Normal, charge)
    });
    assert_eq!(fx.take(), sent);
    let ping = md
        .keepalive()
        .expect("the toy pings its market-data socket");
    assert_eq!((ping.interval, ping.charge), (PING_EVERY, CONTROL));
    // A market-data timer gets a sink, so a codec can report a silent feed Stale when its
    // cadence is missed (Codex r4173389311); the toy sets no such timer, so one firing reports
    // and asks for nothing.
    let mut sink = Collect(Vec::new());
    md.on_timer(TimerTag(9), MonoNs(1), WallNs(1), &mut sink, &mut fx);
    assert!(sink.0.is_empty() && fx.is_empty());

    // A feed the caps do not offer, or an instrument missing from the spec table, is refused
    // by planning and by subscribing, and nothing goes on the wire.
    let book = Subscription {
        inst: INST,
        feed: Feed::Book(fbc_core::BookId(0)),
    };
    let stranger = Subscription {
        inst: InstrumentId::new(99),
        ..touch
    };
    let unknown = VenueError::UnknownInstrument(stranger.inst);
    for (sub, err) in [
        (book, VenueError::UnsupportedFeed(book)),
        (stranger, unknown),
    ] {
        let both = BTreeSet::from([touch, sub]);
        assert_eq!(factory.plan_md(&cfg, &specs(), &both), Err(err));
        let mut fx = Effects::new();
        let refused = md.subscribe(&[touch, sub], &[], &specs(), &mut fx);
        assert_eq!((refused, fx.is_empty()), (Err(err), true));
    }

    // The order-entry connection, and what the exec codec asks for on it: a hello on open and
    // a snapshot request on resync, both from the context's wall time and needing no nonce.
    let url = WireUrl::plain(EXEC_URL);
    let stream = EXEC_STREAM;
    assert_eq!(
        factory.plan_exec(&cfg),
        Ok(vec![ExecEndpoint { stream, url }])
    );
    let mut exec = exec_codec();
    assert_eq!(exec.nonces_for(CtxCall::Open(EXEC_STREAM)), 0);
    assert_eq!(exec.nonces_for(CtxCall::Resync), 0);
    let mut fx = Effects::new();
    exec.on_open(EXEC_STREAM, &ctx(1_000, &[]), &mut fx);
    exec.resync(&ctx(2_000, &[]), &mut fx);
    // One resync is in flight at a time (Codex r4173243261): asking again while one is
    // unanswered starts no second request or timer. Its timer asks for the same resync again,
    // with the same instant, so a slow reply to either copy still matches; once a resync is
    // answered, the timer asks for nothing.
    exec.resync(&ctx(5_000, &[]), &mut fx);
    assert_eq!(exec.nonces_for(CtxCall::Timer(RESYNC_TAG)), 0);
    exec.on_timer(RESYNC_TAG, &ctx(9_000, &[]), &mut fx);
    let safety = |text, charge| send(EXEC_STREAM, text, None, TrafficClass::Safety, charge);
    let retry = Effect::Timer {
        tag: RESYNC_TAG,
        after: RPC_TIMEOUT,
    };
    let asked = "snapshot|ts=2000";
    let expected = [
        hello(1_000),
        safety(asked, RESYNC_CHARGE),
        retry.clone(),
        safety(asked, RESYNC_CHARGE),
        retry,
    ];
    assert_eq!(fx.take(), expected);
    let (results, _) = decode_exec(exec.as_mut(), &["rbegin|wm=2000\nrend"]);
    assert_eq!(results, [Ok(())]);
    exec.on_timer(RESYNC_TAG, &ctx(16_000, &[]), &mut fx);
    assert!(fx.is_empty());
    // An ack answers its request, so the runtime clears that deadline (ExecEvent::answers);
    // a request that times out unanswered is reported Unknown, which answers nothing.
    let (results, mut sink) = decode_exec(exec.as_mut(), &["ack|rpc=11|vid=V-2"]);
    assert_eq!(results, [Ok(())]);
    exec.on_rpc_timeout(RpcId(12), &mut sink);
    let vid = Some(with_scope(|scope| scope.venue_order_id("V-2").unwrap()));
    let ack = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    let item = Some(ItemRef {
        idx: 0,
        cid: None,
        vid,
    });
    let expected = [
        (RpcId(11), item, ack),
        (RpcId(12), None, SubmitOutcome::Unknown),
    ]
    .map(|(rpc, item, outcome)| ExecEvent::Outcome { rpc, item, outcome });
    assert_eq!(sink.bodies(), expected.iter().collect::<Vec<_>>());
    let answers = expected.iter().map(ExecEvent::answers);
    assert_eq!(Vec::from_iter(answers), [Some(RpcId(11)), None]);

    // The client-id format has one source, the capabilities: what the exec codec puts on the
    // wire decodes under it as ours.
    let format = factory.caps(&cfg).unwrap().exec.unwrap().order.client_id;
    let cid = mint();
    let place = VenueCommand::Place(order(cid));
    let text = String::from_utf8(bytes(&place, &ctx(1, &[1]))).unwrap();
    let wire = text.split('|').find_map(|kv| kv.strip_prefix("cid="));
    assert_eq!(
        decode_cid(&format, OWN_NS, wire.unwrap()),
        CidMatch::Ours(cid)
    );
}

// ---------------------------------------------------------------------------------------------
// Discovery and resolution (FBC-ahf).
// ---------------------------------------------------------------------------------------------

/// The venue the consumer numbers the toy as, as in `specs()`.
const TOY_VENUE: VenueId = VenueId::new(9);

/// The toy's market list: the market the other tests trade, and a halted one nobody lists.
const MARKETS: &str =
    "market|sym=TOY-PERP|base=TOY|quote=USDC|settle=USDC|tick=0.5|step=0.001|min=1|status=trading
market|sym=TOYB-PERP|base=TOYB|quote=USDC|settle=USDC|tick=0.01|step=0.1|min=10|status=halted";

/// Discovery's plan answered with `answer`, parsed in the decode scope market data is
/// dispatched with (discovery holds no engine namespace).
fn discover_with(
    answer: Result<HttpResponse<'_>, HttpFailure>,
) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let plan = ToyFactory.discover(&VenueConfig::new()).unwrap();
    dispatch_market_data(&toy_caps(), |scope| {
        plan.parse(&[(MARKETS_TAG, answer)], scope)
    })
}

fn discover(body: &str) -> Result<Vec<InstrumentSpecDraft>, PlanError> {
    let (status, headers) = (200, &[][..]);
    discover_with(Ok(HttpResponse {
        status,
        headers,
        body: body.as_bytes(),
    }))
}

fn perp(base: &str, quote: &str) -> AssetKey {
    AssetKey {
        base: AssetSym::new(base).unwrap(),
        quote: AssetSym::new(quote).unwrap(),
        kind: InstrumentKind::Perpetual,
    }
}

#[test]
fn discovery_is_a_plan_of_http_effects_whose_parser_builds_drafts_in_the_decode_scope() {
    let plan = ToyFactory.discover(&VenueConfig::new()).unwrap();
    // Only effects: one GET of the market list, no frame, timer or reconnect, and no IO.
    assert_eq!(plan.requests(), [markets_request()]);
    assert!(
        plan.requests()
            .iter()
            .all(|fx| matches!(fx, Effect::Http { rpc: None, .. }))
    );

    let drafts = discover(MARKETS).unwrap();
    let symbols = drafts.iter().map(|d| d.venue_symbol.as_wire());
    assert_eq!(Vec::from_iter(symbols), ["TOY-PERP", "TOYB-PERP"]);
    let expected = InstrumentSpecDraft {
        asset: perp("TOY", "USDC"),
        venue_symbol: with_scope(|scope| scope.venue_symbol(SYMBOL)).unwrap(),
        native_id: None,
        price_grid: PriceGrid::fixed(Decimal::new(5, 1)).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new(Decimal::new(1, 3)).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        settle_ccy: usdc(),
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
    };
    assert_eq!(drafts[0], expected);
    assert_eq!(drafts[1].status, TradingStatus::Halted);
}

#[test]
fn a_draft_and_a_java_era_ticker_resolve_to_one_instrument_through_the_alias_table() {
    let drafts = discover(MARKETS).unwrap();
    let legacy = ToyFactory.parse_fbc_common_symbol("TOY/USDT").unwrap();
    // The venue states USDC and the Java-era ticker USDT; the consumer lists the instrument
    // once, as TOY/USD.
    assert_eq!(drafts[0].asset, perp("TOY", "USDC"));
    assert_eq!(legacy, perp("TOY", "USDT"));
    let mut resolver = InstrumentResolver::new(AliasTable::seeded());
    let listing = Listing {
        id: INST,
        underlying: UnderlyingId::new(1),
    };
    resolver
        .list(TOY_VENUE, perp("TOY", "USD"), listing)
        .unwrap();
    for key in [drafts[0].asset, legacy] {
        assert_eq!(resolver.resolve(TOY_VENUE, key), Ok(INST));
    }
    // The resolved draft is the spec the other tests trade against.
    let spec = resolver.spec(TOY_VENUE, drafts[0].clone(), 1, WallNs(0));
    assert_eq!(spec.as_ref(), Ok(specs().get(INST).unwrap()));

    // Nothing is guessed: a market the consumer did not list, or the same keys read without
    // the aliases, resolve to nothing.
    let unlisted = resolver.spec(TOY_VENUE, drafts[1].clone(), 1, WallNs(0));
    let key = perp("TOYB", "USD");
    let venue = TOY_VENUE;
    assert_eq!(unlisted, Err(ResolveError::NotListed { venue, key }));
    let mut bare = InstrumentResolver::new(AliasTable::empty());
    bare.list(TOY_VENUE, perp("TOY", "USD"), listing).unwrap();
    for key in [drafts[0].asset, legacy] {
        let refused = Err(ResolveError::NotListed { venue, key });
        assert_eq!(bare.resolve(TOY_VENUE, key), refused);
    }

    // The toy's FBC rule reads only X/USDT, and only in the common form.
    let rule = |s| ToyFactory.parse_fbc_common_symbol(s);
    assert_eq!(rule("TOY/BTC"), Err(SymbolError::Unmapped));
    assert_eq!(rule("TOYUSDT"), Err(SymbolError::NotCommonForm));
    assert_eq!(rule("TOY-PERP"), Err(SymbolError::NotCommonForm));
}

#[test]
fn a_market_missing_a_required_field_is_refused_by_name() {
    let first = MARKETS.lines().next().unwrap();
    for field in [
        "sym", "base", "quote", "settle", "tick", "step", "min", "status",
    ] {
        let kept = first
            .split('|')
            .filter(|part| !part.starts_with(&format!("{field}=")));
        let line = Vec::from_iter(kept).join("|");
        // The whole answer is refused, the complete second market with it.
        let answer = format!("{line}\n{}", MARKETS.lines().nth(1).unwrap());
        assert_eq!(discover(&answer), Err(PlanError::Missing(field)), "{field}");
    }
    // A value the venue states but that is no valid value is malformed, not missing.
    for (from, to, part) in [
        ("tick=0.5", "tick=0", "tick"),
        ("tick=0.5", "tick=x", "tick"),
        ("step=0.001", "step=0", "step"),
        ("min=1", "min=-1", "min"),
        ("base=TOY", "base=TOOLONGBASE", "base"),
        ("status=trading", "status=open", "status"),
        ("market|", "other|", "kind"),
    ] {
        let line = first.replace(from, to);
        let malformed = Err(PlanError::Decode(DecodeError::Malformed(part)));
        assert_eq!(discover(&line), malformed, "{to}");
    }
    let symbol = Err(PlanError::Decode(DecodeError::IdRefused(
        fbc_core::IdError::Empty,
    )));
    assert_eq!(discover(&first.replace("TOY-PERP", "")), symbol);
    let not_text = discover_with(Ok(HttpResponse {
        status: 200,
        headers: &[],
        body: &[0xff],
    }));
    let utf8 = Err(PlanError::Decode(DecodeError::Malformed("utf-8")));
    assert_eq!(not_text, utf8);
    // An answer that is not a 2xx response never reaches the parser.
    let failed = discover_with(Err(HttpFailure::TimedOut));
    let (tag, failure) = (MARKETS_TAG, HttpFailure::TimedOut);
    assert_eq!(failed, Err(PlanError::Http { tag, failure }));
    let body = MARKETS.as_bytes();
    let unavailable = discover_with(Ok(HttpResponse {
        status: 503,
        headers: &[],
        body,
    }));
    assert_eq!(unavailable, Err(PlanError::Status { tag, status: 503 }));
}

#[test]
fn the_exec_codec_takes_its_key_from_secrets_and_no_debug_shows_it() {
    // FBC-b3b, decision 0043: credentials reach the codec only as `Secrets`, built here from a
    // synthetic key. Formatting them, or anything the codec asks for with the key in it, shows
    // none of it.
    let secrets = creds();
    for shown in [
        format!("{secrets:?}"),
        format!("{secrets:#?}"),
        secrets.to_string(),
    ] {
        assert!(!shown.contains(SYNTHETIC_KEY), "{shown}");
    }
    assert!(format!("{secrets:?}").contains(KEY));

    // The codec sends the key in its hello, marked as the frame's one credential span, so the
    // journal keeps it as a keyed hash; the effect's Debug shows the span by length only.
    let mut exec = exec_codec();
    let mut fx = Effects::new();
    exec.on_open(EXEC_STREAM, &ctx(1_000, &[]), &mut fx);
    assert_eq!(fx.as_slice(), [hello(1_000)]);
    let Effect::Send { frame, .. } = &fx.as_slice()[0] else {
        unreachable!()
    };
    let [span] = frame.redactions() else {
        panic!("one span")
    };
    let span = span.start as usize..span.end as usize;
    assert_eq!(&frame.bytes()[span], SYNTHETIC_KEY.as_bytes());
    let shown = format!("{fx:?}");
    assert!(!shown.contains(SYNTHETIC_KEY), "{shown}");

    // Without its key the factory builds no codec, and its refusal names the key, not a value.
    let refused = ToyFactory.exec_codec(&VenueConfig::new(), Secrets::new());
    let Some(Err(err)) = refused else {
        panic!("a codec without its key")
    };
    assert_eq!(err, VenueError::Config(ConfigError::Missing(KEY)));
}

// ---------------------------------------------------------------------------------------------
// Credentials and test_connection (FBC-b3b).
// ---------------------------------------------------------------------------------------------

/// The toy's test_connection plan for the synthetic key.
fn connection_plan() -> HttpPlan<AccountSummary> {
    ToyFactory
        .test_connection(&VenueConfig::new(), creds())
        .unwrap()
        .unwrap()
}

/// `plan` answered with `answer` for its account request, parsed in the decode scope.
fn connect_with(
    plan: HttpPlan<AccountSummary>,
    answer: Result<HttpResponse<'_>, HttpFailure>,
) -> Result<AccountSummary, PlanError> {
    dispatch_market_data(&toy_caps(), |scope| {
        plan.parse(&[(ACCOUNT_TAG, answer)], scope)
    })
}

/// The toy's test_connection answered `status` with `body`.
fn connect(status: u16, body: &[u8]) -> Result<AccountSummary, PlanError> {
    let headers = &[];
    connect_with(
        connection_plan(),
        Ok(HttpResponse {
            status,
            headers,
            body,
        }),
    )
}

/// The account request the toy asks for with the synthetic key.
fn asked_for_account() -> Effect {
    let key = Header {
        name: KEY_HEADER,
        value: SYNTHETIC_KEY.to_owned(),
        redact: true,
    };
    Effect::Http {
        tag: ACCOUNT_TAG,
        req: HttpRequest {
            method: HttpMethod::Get,
            url: WireUrl::plain(ACCOUNT_URL),
            headers: vec![key],
            body: WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout: RPC_TIMEOUT,
        class: TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Query, None),
    }
}

#[test]
fn test_connection_proves_the_key_and_logs_no_account_or_balance() {
    // FBC-b3b, design §4.7: test_connection is an HttpPlan (0035, 0043), its request built from
    // the key and its parser reading the AccountSummary inside the decode scope. The plan's
    // Debug shows the request but not the key its redacted header carries.
    let plan = connection_plan();
    assert_eq!(plan.requests(), [asked_for_account()]);
    let shown = format!("{plan:?}");
    assert!(shown.contains(ACCOUNT_URL), "{shown}");
    assert!(!shown.contains(SYNTHETIC_KEY), "{shown}");

    let (account, equity) = ("SYNTHETIC-ACCT-42", 1_234_500_000_000_i128);
    let body = format!("account|id={account}|equity={equity}");
    let summary = connect(200, body.as_bytes()).unwrap();
    let expected = AccountSummary {
        account: account.to_owned(),
        equity: Some(Money::new(equity, usdc())),
    };
    assert_eq!(summary, expected);
    // Its Debug shows neither the account nor the balance (0009).
    let shown = format!("{summary:?}");
    assert!(!shown.contains(account), "{shown}");
    assert!(!shown.contains(&equity.to_string()), "{shown}");
    assert_eq!(
        shown,
        format!(
            "AccountSummary {{ account: <{} bytes>, equity: Some(<redacted>) }}",
            account.len()
        )
    );
    let none = AccountSummary {
        equity: None,
        ..summary
    };
    assert!(format!("{none:?}").ends_with("equity: None }"));

    // A refusal, a missing response and an answer it cannot read each end the plan with why,
    // and none of them carries what the venue sent.
    let tag = ACCOUNT_TAG;
    let lost = connect_with(connection_plan(), Err(HttpFailure::TimedOut));
    let failure = HttpFailure::TimedOut;
    assert_eq!(lost, Err(PlanError::Http { tag, failure }));
    let malformed = |part| PlanError::Decode(DecodeError::Malformed(part));
    let cases: [(u16, &[u8], PlanError); 7] = [
        (
            401,
            SYNTHETIC_KEY.as_bytes(),
            PlanError::Status { tag, status: 401 },
        ),
        (200, b"\xff", malformed("utf-8")),
        (200, b"account|x", malformed("field")),
        (200, b"fill|id=a", malformed("kind")),
        (200, b"account|equity=1", PlanError::Missing("id")),
        (200, b"account|id=a", PlanError::Missing("equity")),
        (200, b"account|id=a|equity=x", malformed("equity")),
    ];
    for (status, body, err) in cases {
        assert_eq!(connect(status, body), Err(err), "{err}");
        assert!(!err.to_string().contains(SYNTHETIC_KEY), "{err}");
    }

    // Without its key there is no plan, and the refusal names the key.
    let refused = ToyFactory.test_connection(&VenueConfig::new(), Secrets::new());
    let Some(Err(err)) = refused else {
        panic!("a plan without its key")
    };
    assert_eq!(err, VenueError::Config(ConfigError::Missing(KEY)));
}

#[test]
fn a_refused_test_connection_draws_no_nonce_and_never_reaches_its_parser() {
    // Codex r4180237818: a plan that reserved nonces for a follow-up before it saw the answer
    // would reserve them on a 401 too. An HttpPlan has no follow-up to reserve for. Its requests
    // are the whole round, built (and signed, for a venue that signs) when the factory makes the
    // plan, before any answer; its parser is handed only the 2xx answers and the decode scope,
    // no EncodeCtx and no Effects, so it can neither sign nor ask for more; and `parse`
    // consumes the plan. Every request is fixed before the first answer, so a refused answer
    // changes nothing it asked for.
    let plan = connection_plan();
    let before = plan.requests().to_vec();
    assert_eq!(before, [asked_for_account()]);
    let refused = connect_with(
        plan,
        Ok(HttpResponse {
            status: 401,
            headers: &[],
            body: b"",
        }),
    );
    let tag = ACCOUNT_TAG;
    assert_eq!(refused, Err(PlanError::Status { tag, status: 401 }));

    // The refusal is decided before the parser runs: a plan of the same request whose parser
    // panics if called ends the same way.
    let mut fx = Effects::new();
    fx.push(asked_for_account());
    let unreachable = HttpPlan::new(fx, |_, _| -> Result<AccountSummary, PlanError> {
        panic!("a refused answer reached the parser")
    });
    let refused = connect_with(
        unreachable.unwrap(),
        Ok(HttpResponse {
            status: 401,
            headers: &[],
            body: b"",
        }),
    );
    assert_eq!(refused, Err(PlanError::Status { tag, status: 401 }));
}
