//! Decision 0002, design §4.6–§4.8: a venue adapter is a sans-IO codec. The toy venue below
//! implements `MdCodec`, `ExecCodec` and `VenueFactory` over a hand-written text protocol. It
//! decodes frames into `MdEvent` and `ExecEvent` only inside the `DecodeScope` the core's
//! dispatch lends (decision 0004: venue ids, fill ids and fees come from nowhere else), encodes
//! a `VenueCommand` taking time and nonces only from `EncodeCtx`, and does no IO: everything it
//! wants done comes back as `Effects`. Its protocol, symbols and values describe no real venue.
//!
//! The toy proves FBC-5's done line; it is not a conformance suite, and its `VenueCaps` declare
//! only what its codecs do. One socket carries the touch; one carries order entry (unsigned
//! limit orders out; acks, account events, fills and a resync answered in one frame in). It has
//! no book, trades or other feed, cancels, amends and queries nothing, reads no configuration,
//! and asks for no HTTP; it keeps one resync in flight, and its one timer asks for that resync
//! again while it is unanswered. Its market-data socket is kept alive with a ping frame.
//!
//! Every frame it asks for, and its ping, carries the rate charge its declared limits count
//! (decision 0018): orders per instrument, a resync as a weighted query against the account,
//! and subscriptions, its hello and its pings against the connection they are written on.
//!
//! Protocol: one record per line, `kind|key=value|...`; `ts` is the venue's matching-engine time
//! in nanoseconds and `seq` its sequence. Prices are ticks, sizes lots, money nanos of USDC.

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbc_core::{
    AckLevel, AckModel, AssetSym, Cadence, CancelOnDisconnect, Channel, Charset, CidMatch, CidMint,
    ClientIdFormat, ClientOrderId, ConfigError, ConnKey, ConnTopology, CtxCall, DecodeError,
    DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt, Encoding, EndpointPlan, Envelope,
    ExchNs, ExchTsKind, ExecCaps, ExecCodec, ExecEndpoint, ExecEvent, ExecSink, Feed, FeedSource,
    FieldSpec, FillCaps, FillEvent, FillIdent, FillSource, FundingCaps, FundingSpec, HttpFailure,
    HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId, InstrumentKind, InstrumentSpec,
    ItemRef, Keepalive, KeepaliveKind, LimitScope, Liquidity3, Lots, Lvl, MatchingCaps, MdCaps,
    MdCodec, MdEvent, MdSink, MdTransport, Money, MonoNs, Namespace, NamespaceLease, NewOrder,
    NonceBlock, NonceScope, NotSentReason, OpKind, OrderCaps, OrderKind, OrderKindTag, OrderingKey,
    PriceGrid, PxExact, RateCharge, RateLimit, RawFrame, Readiness, RpcCall, RpcId, SeqDomain,
    Side, SignedLots, SizeStep, SnapshotSource, SpecTable, Stamp, StpScope, StreamId,
    SubmitOutcome, Subscription, Support, TagSet, Ticks, TifTag, TimerTag, TouchSourceCaps,
    TouchSourceId, TradeCaps, TradingStatus, TrafficClass, UnderlyingId, VenueCaps, VenueCommand,
    VenueConfig, VenueError, VenueFactory, VenueFeeSign, VenueId, VenueMeta, VenueOrderSnapshot,
    VenueOrderState, Via, WallNs, WireSlice, WireUrl, decode_cid, dispatch, encode_cid,
};
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

    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        let hello = format!("hello|ts={}", ctx.wall.0);
        fx.push(send(stream, &hello, None, TrafficClass::Safety, CONTROL));
    }

    /// Encodes a limit order, its time and nonce from `ctx` alone; the toy offers nothing else.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
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
        let frame = format!(
            "rpc={}\nplace|cid={cid}|sym={}|side={}|px={}|qty={}|po={}|ro={}|ts={}|nonce={nonce}",
            rpc.0,
            spec.venue_symbol.as_wire(),
            if o.side == Side::Buy { "B" } else { "S" },
            px.0,
            o.qty.get(),
            u8::from(o.post_only),
            u8::from(o.reduce_only),
            ctx.wall.0,
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

/// The toy venue's factory. The toy reads no configuration.
struct ToyFactory;

impl VenueFactory for ToyFactory {
    fn id(&self) -> &'static str {
        "TOY"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
    }

    fn caps(&self, _cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(toy_caps())
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

    fn exec_codec(&self, _cfg: &VenueConfig) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(Ok(Box::new(ToyExec { resync_at: None })))
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

fn exec_codec() -> Box<dyn ExecCodec> {
    ToyFactory.exec_codec(&VenueConfig::new()).unwrap().unwrap()
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
    let receipt = exec_codec().encode(cmd, RpcId(11), &specs(), ctx, &mut fx);
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
        codec
            .encode(&cmd, RpcId(11), &specs(), &at, &mut fx)
            .unwrap();
        fx.take()
    });
    // The same codec twice, and a fresh one: codec state does not leak into the bytes.
    assert_eq!(twice[0], twice[1]);
    assert_eq!(twice[0], encode(&cmd, &at).unwrap().1);
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
         |ts=1759363200300000000|nonce=9000"
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
    exec_codec()
        .encode(&place, RpcId(11), &specs(), &ctx(1, &[1]), &mut fx)
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
    assert!(factory.config_schema().is_empty());
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
    let (hello, asked) = ("hello|ts=1000", "snapshot|ts=2000");
    let expected = [
        safety(hello, CONTROL),
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
