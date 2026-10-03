//! Decision 0002, design §4.6–§4.8: a venue adapter is a sans-IO codec. The toy venue below
//! implements `MdCodec`, `ExecCodec` and `VenueFactory` over a hand-written text protocol. It
//! decodes frames into `MdEvent` and `ExecEvent` only inside the `DecodeScope` the core's
//! dispatch lends (decision 0004: venue ids, fill ids and fees come from nowhere else), encodes
//! `VenueCommand`s taking time and nonces only from `EncodeCtx`, and does no IO: everything it
//! wants done comes back as `Effects`. Its protocol, symbols and values describe no real venue.
//!
//! Protocol: one frame per line, `kind|key=value|...`; `ts` is the venue's matching-engine time
//! in nanoseconds and `seq` its sequence. Prices are ticks, sizes lots, money nanos of USDC.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fbc_core::{
    AckLevel, AckModel, Aggressor, AmendCaps, AmendOrder, AmendQty, AmendWire, AssetSym, BookCaps,
    BookSide, Cadence, CancelOnDisconnect, CancelOrder, CancelWire, Channel, Charset, CidMatch,
    CidMint, ClientIdFormat, ClientOrderId, ConfigError, ConfigScope, ConnKey, ConnTopology,
    Continuity, DecodeError, DecodeScope, EncodeCtx, EncodeReceipt, Encoding, EndpointPlan,
    Envelope, ExchNs, ExchTsKind, ExecCodec, ExecEndpoint, ExecEvent, ExecSink, Feed, FeedHealth,
    FeedSource, FieldSpec, FieldUnit, FillCaps, FillEvent, FillKey, FillSource, FundingCaps,
    FundingSpec, Header, HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag, InstrumentId,
    InstrumentKind, InstrumentSpec, ItemRef, Keepalive, KeepaliveKind, Liquidity3, Lots, Lvl,
    MatchingCaps, MdCaps, MdCodec, MdEvent, MdSink, ModeScope, Money, MonoNs, Namespace,
    NamespaceLease, NewOrder, NonceBlock, NonceScope, NotSentReason, OrderCaps, OrderKind,
    OrderKindTag, OrderRef, OrderSigner, OrderUpdate, OrderingKey, PlaceWire, PriceGrid, PxExact,
    QueryOrder, QueueModelQuality, RawFrame, Readiness, RefKind, Reject, RejectKind, RpcCall,
    RpcId, SeqDomain, Side, Sig, SignError, SignedLots, SizeStep, SnapshotSource, SpecTable, Stamp,
    StpScope, StreamId, SubmitOutcome, Subscription, Support, TagSet, Ticks, TifTag, TimerTag,
    TouchSourceCaps, TouchSourceId, TradeCaps, TradingStatus, TrafficClass, UnderlyingId,
    VenueCaps, VenueCommand, VenueConfig, VenueError, VenueFactory, VenueFeeSign, VenueId,
    VenueMeta, VenueMode, VenueOrderSnapshot, VenueOrderState, WallNs, WireSlice, decode_cid,
    dispatch, encode_cid,
};
use fbc_core::{AmendAck, Batch, Effect, Effects};
use rust_decimal::Decimal;

// ---------------------------------------------------------------------------------------------
// The toy venue.
// ---------------------------------------------------------------------------------------------

const SYMBOL: &str = "TOY-PERP";
const INST: InstrumentId = InstrumentId::new(7);
const OWN_NS: Namespace = Namespace::new(5);
const EXEC_STREAM: StreamId = StreamId(1);
const RESYNC_TAG: HttpTag = HttpTag(1);
const ANCHOR_TAG: HttpTag = HttpTag(2);
const PING_TAG: TimerTag = TimerTag(1);
const RESYNC_RETRY_TAG: TimerTag = TimerTag(2);
const RPC_TIMEOUT: Duration = Duration::from_secs(5);
const URL_KEY: &str = "toy.url";
/// The toy reports fees with a positive rebate, so the scope must flip their sign.
const FEE_SIGN: VenueFeeSign = VenueFeeSign::PositiveIsRebate;
const CID_FORMAT: ClientIdFormat = ClientIdFormat::Alnum {
    max_len: 32,
    charset: Charset::Alphanumeric,
};

fn usdc() -> AssetSym {
    AssetSym::new("USDC").unwrap()
}

/// One frame, split into its kind and its fields.
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
        self.get(key)?
            .parse()
            .map_err(|_| DecodeError::Malformed(key))
    }

    fn opt_num<T: FromStr>(&self, key: &'static str) -> Result<Option<T>, DecodeError> {
        self.opt(key)
            .map(|v| v.parse().map_err(|_| DecodeError::Malformed(key)))
            .transpose()
    }

    fn meta(&self) -> Result<VenueMeta, DecodeError> {
        let exch_ts = self.opt_num::<i64>("ts")?.map(ExchNs);
        Ok(VenueMeta {
            exch_ts,
            exch_ts_kind: if exch_ts.is_some() {
                ExchTsKind::MatchingEngine
            } else {
                ExchTsKind::Unknown
            },
            venue_seq: self.opt_num("seq")?,
        })
    }

    fn inst(&self, specs: &SpecTable) -> Result<InstrumentId, DecodeError> {
        let spec = specs.by_symbol(self.get("sym")?);
        spec.map(|s| s.id).ok_or(DecodeError::UnknownInstrument)
    }

    fn lots(&self, key: &'static str) -> Result<Lots, DecodeError> {
        Lots::new(self.num(key)?).ok_or(DecodeError::Malformed(key))
    }

    fn usdc(&self, key: &'static str) -> Result<Money, DecodeError> {
        Ok(Money::new(self.num(key)?, usdc()))
    }

    fn opt_usdc(&self, key: &'static str) -> Result<Option<Money>, DecodeError> {
        Ok(self.opt_num(key)?.map(|n| Money::new(n, usdc())))
    }

    fn side(&self) -> Result<Side, DecodeError> {
        match self.get("side")? {
            "B" => Ok(Side::Buy),
            "S" => Ok(Side::Sell),
            _ => Err(DecodeError::Malformed("side")),
        }
    }

    fn px_exact(&self, key: &'static str) -> Result<Option<PxExact>, DecodeError> {
        self.opt_num(key)
    }

    fn flag(&self, key: &'static str) -> Option<bool> {
        self.opt(key).map(|v| v == "1")
    }

    /// `pxXqty`: one price level.
    fn level(&self, key: &'static str) -> Result<Option<Lvl>, DecodeError> {
        let Some(text) = self.opt(key) else {
            return Ok(None);
        };
        let bad = DecodeError::Malformed(key);
        let (px, qty) = text.split_once('x').ok_or(bad)?;
        let px = px.parse().map_err(|_| bad)?;
        let qty = qty.parse().ok().and_then(Lots::new).ok_or(bad)?;
        Ok(Some(Lvl { px: Ticks(px), qty }))
    }
}

/// The toy's market-data codec.
struct ToyMd {
    pings: u32,
}

impl ToyMd {
    fn decode_md(line: &str, specs: &SpecTable, sink: &mut dyn MdSink) -> Result<(), DecodeError> {
        let f = Frame::parse(line)?;
        let meta = f.meta()?;
        let inst = f.inst(specs)?;
        match f.kind {
            "touch" => sink.push(
                meta,
                MdEvent::Touch {
                    inst,
                    bid: f.level("bid")?,
                    ask: f.level("ask")?,
                    source: TouchSourceId(0),
                },
            ),
            "snap" => {
                let epoch = f.num("epoch")?;
                sink.push(meta, MdEvent::BookSnapshotBegin { inst, epoch });
                for (key, side) in [("b", BookSide::Bid), ("a", BookSide::Ask)] {
                    if let Some(lvl) = f.level(key)? {
                        let (px, qty) = (lvl.px, lvl.qty);
                        sink.push(
                            meta,
                            MdEvent::Level {
                                inst,
                                side,
                                px,
                                qty,
                            },
                        );
                    }
                }
                sink.push(meta, MdEvent::BookSnapshotEnd { inst });
            }
            "trade" => {
                let aggressor = match f.get("aggr")? {
                    "B" => Aggressor::Buyer,
                    "S" => Aggressor::Seller,
                    _ => Aggressor::Unknown,
                };
                let (px, qty) = (Ticks(f.num("px")?), f.lots("qty")?);
                let id = f.opt_num("id")?;
                sink.push(
                    meta,
                    MdEvent::Trade {
                        inst,
                        id,
                        aggressor,
                        px,
                        qty,
                    },
                );
            }
            "funding" => sink.push(
                meta,
                MdEvent::Funding {
                    inst,
                    rate_e12: f.num("rate")?,
                    interval: f.opt_num("every")?.map(Duration::from_secs),
                    next: f.opt_num("next")?.map(WallNs),
                },
            ),
            "mark" => {
                let px = f.px_exact("px")?.ok_or(DecodeError::Malformed("px"))?;
                sink.push(meta, MdEvent::Mark { inst, px });
            }
            "gap" => {
                let feed = match f.get("feed")? {
                    "book" => Feed::Book(0),
                    _ => Feed::Trades,
                };
                let h = FeedHealth::Gap;
                sink.push(meta, MdEvent::Health { inst, feed, h });
            }
            _ => return Err(DecodeError::Malformed("kind")),
        }
        Ok(())
    }
}

/// The venue's spelling of `inst`, from the spec table.
fn symbol_of(specs: &SpecTable, inst: InstrumentId) -> Result<&str, VenueError> {
    specs
        .get(inst)
        .map(|spec| spec.venue_symbol.as_wire())
        .ok_or(VenueError::UnknownInstrument(inst))
}

impl MdCodec for ToyMd {
    fn on_open(&mut self, fx: &mut Effects) {
        fx.push(Effect::Timer {
            tag: PING_TAG,
            after: Duration::from_secs(15),
        });
    }

    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        // Every instrument is spelled before anything is pushed, so a refusal pushes nothing.
        let mut frames = Vec::new();
        for (verb, subs) in [("sub", add), ("unsub", remove)] {
            for sub in subs {
                let symbol = symbol_of(specs, sub.inst)?;
                frames.push(format!("{verb}|sym={symbol}|feed={:?}", sub.feed));
            }
        }
        for frame in frames {
            fx.push(Effect::Send {
                stream: StreamId(0),
                frame: WireSlice::plain(frame.into_bytes()),
                rpc: None,
                class: TrafficClass::Normal,
            });
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
        let RawFrame::Text(line) = f else {
            return Err(DecodeError::Malformed("binary frame"));
        };
        ToyMd::decode_md(line, specs, sink)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let resp = match resp {
            Ok(resp) if tag == ANCHOR_TAG && resp.status == 200 => resp,
            _ => return Err(DecodeError::Malformed("anchor response")),
        };
        let body = std::str::from_utf8(resp.body).map_err(|_| DecodeError::Malformed("body"))?;
        ToyMd::decode_md(body, specs, sink)
    }

    fn on_timer(&mut self, tag: TimerTag, _now: MonoNs, wall: WallNs, fx: &mut Effects) {
        assert_eq!(tag, PING_TAG);
        self.pings += 1;
        let frame = format!("ping|n={}|at={}", self.pings, wall.0);
        fx.push(Effect::Send {
            stream: StreamId(0),
            frame: WireSlice::plain(frame.into_bytes()),
            rpc: None,
            class: TrafficClass::Safety,
        });
    }

    fn keepalive(&self) -> Option<Keepalive> {
        Some(Keepalive {
            interval: Duration::from_secs(15),
            kind: KeepaliveKind::WsPing,
        })
    }
}

/// The toy's signer: a keyless FNV-1a digest of the signed fields, standing in for a real
/// signature. It reads nothing but the wire view it is given.
struct ToySigner;

fn fnv(text: &str) -> Sig {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    Sig::new(&hash.to_be_bytes()).unwrap()
}

impl OrderSigner for ToySigner {
    fn sign_place(&mut self, w: &PlaceWire<'_>) -> Result<Sig, SignError> {
        let px = w
            .kind
            .limit_px()
            .ok_or(SignError::Unsignable("market order"))?;
        Ok(fnv(&format!(
            "{}|{}|{:?}|{}|{}|{:?}|{:?}|{}|{}|{}|{:?}",
            w.spec.venue_symbol.as_wire(),
            w.cid,
            w.side,
            px.0,
            w.qty.get(),
            w.tif,
            w.channel,
            w.post_only,
            w.reduce_only,
            w.wall.0,
            w.nonce
        )))
    }

    fn sign_amend(&mut self, w: &AmendWire<'_>) -> Result<Sig, SignError> {
        let vid = w.vid.ok_or(SignError::Unsignable("venue order id"))?;
        Ok(fnv(&format!(
            "{}|{}|{:?}|{:?}|{}|{}|{:?}|{:?}|{}|{}|{}|{:?}",
            w.spec.venue_symbol.as_wire(),
            vid.as_str(),
            w.cid,
            w.side,
            w.px.0,
            w.qty.get(),
            w.tif,
            w.channel,
            w.post_only,
            w.reduce_only,
            w.wall.0,
            w.nonce
        )))
    }

    fn sign_cancel(&mut self, _w: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        Ok(None)
    }
}

fn hex(sig: &Sig) -> String {
    sig.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

/// The toy's order-entry codec. It keeps no order registry: every field it writes comes from
/// the command, the spec table or the encode context.
struct ToyExec {
    signer: Box<dyn OrderSigner>,
    url: String,
}

impl ToyExec {
    fn new(url: &str) -> ToyExec {
        ToyExec {
            signer: Box::new(ToySigner),
            url: url.to_owned(),
        }
    }

    fn send(fx: &mut Effects, frame: String, rpc: Option<RpcId>, class: TrafficClass) {
        fx.push(Effect::Send {
            stream: EXEC_STREAM,
            frame: WireSlice::plain(frame.into_bytes()),
            rpc: rpc.map(|id| RpcCall {
                id,
                timeout: RPC_TIMEOUT,
            }),
            class,
        });
    }

    fn class(reducing: bool) -> TrafficClass {
        if reducing {
            TrafficClass::Safety
        } else {
            TrafficClass::Normal
        }
    }

    /// One `place` frame for item `idx`, signed, with its nonce.
    fn place_frame(
        &mut self,
        o: &NewOrder,
        idx: u16,
        specs: &SpecTable,
        ctx: &EncodeCtx,
    ) -> Result<(String, u64), NotSentReason> {
        let spec = specs.get(o.inst).ok_or(NotSentReason::Unencodable)?;
        let nonce = ctx.nonce(idx).ok_or(NotSentReason::Unencodable)?;
        let cid = encode_cid(&CID_FORMAT, o.cid).map_err(|_| NotSentReason::Unencodable)?;
        let px = o.kind.limit_px().ok_or(NotSentReason::Unsupported)?;
        let wire = PlaceWire {
            spec,
            cid: &cid,
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
        let sig = self
            .signer
            .sign_place(&wire)
            .map_err(|_| NotSentReason::SignFailed)?;
        let frame = format!(
            "place|i={idx}|cid={cid}|sym={}|side={}|px={}|qty={}|tif={:?}|po={}|ro={}|ts={}|nonce={nonce}|sig={}",
            spec.venue_symbol.as_wire(),
            side_code(o.side),
            px.0,
            o.qty.get(),
            o.tif,
            u8::from(o.post_only),
            u8::from(o.reduce_only),
            ctx.wall.0,
            hex(&sig)
        );
        Ok((frame, nonce))
    }

    fn decode_exec(
        line: &str,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
    ) -> Result<(), DecodeError> {
        let f = Frame::parse(line)?;
        let meta = f.meta()?;
        let event = match f.kind {
            "pos" | "rpos" => {
                let (inst, qty) = (f.inst(specs)?, SignedLots(f.num("qty")?));
                let avg_entry = f.px_exact("avg")?;
                if f.kind == "pos" {
                    ExecEvent::Position {
                        inst,
                        qty,
                        avg_entry,
                    }
                } else {
                    ExecEvent::ResyncPosition {
                        inst,
                        qty,
                        avg_entry,
                    }
                }
            }
            "bal" => ExecEvent::Balance {
                equity: f.usdc("equity")?,
                available: f.usdc("avail")?,
            },
            "fundpay" => ExecEvent::FundingPaid {
                inst: f.inst(specs)?,
                amount: f.usdc("amt")?,
            },
            "rbegin" => ExecEvent::ResyncBegin {
                watermark: WallNs(f.num("wm")?),
            },
            "rorder" => ExecEvent::ResyncOrder(VenueOrderSnapshot {
                cid: f.opt("cid").map(|c| scope.client_order_id(c)),
                vid: scope.venue_order_id(f.get("vid")?)?,
                inst: f.inst(specs)?,
                side: f.side()?,
                state: VenueOrderState::Open,
                px: f.opt_num("px")?.map(Ticks),
                qty: f.lots("qty")?,
                cum_filled: f.lots("cum")?,
                post_only: f.flag("po"),
                reduce_only: f.flag("ro"),
            }),
            "rend" => ExecEvent::ResyncEnd,
            "ord" => ExecEvent::Order(OrderUpdate {
                cid: f.opt("cid").map(|c| scope.client_order_id(c)),
                vid: f.opt("vid").map(|v| scope.venue_order_id(v)).transpose()?,
                inst: f.inst(specs)?,
                side: f.side()?,
                state: VenueOrderState::Open,
                cum_filled: f.lots("cum")?,
                px: f.opt_num("px")?.map(Ticks),
                qty: f.opt_num("qty")?.and_then(Lots::new),
                post_only: f.flag("po"),
                reduce_only: f.flag("ro"),
            }),
            "mode" => ExecEvent::Mode {
                scope: match f.opt("sym") {
                    Some(_) => ModeScope::Instrument(f.inst(specs)?),
                    None => ModeScope::Account,
                },
                mode: match f.get("m")? {
                    "halted" => VenueMode::Halted,
                    "cancel_only" => VenueMode::CancelOnly,
                    _ => return Err(DecodeError::Malformed("mode")),
                },
            },
            "fill" => ExecEvent::Fill(FillEvent {
                key: FillKey::Venue(scope.fill_id(f.get("fid")?)?),
                cid: f.opt("cid").map(|c| scope.client_order_id(c)),
                vid: f.opt("vid").map(|v| scope.venue_order_id(v)).transpose()?,
                inst: f.inst(specs)?,
                side: f.side()?,
                px: Ticks(f.num("px")?),
                qty: f.lots("qty")?,
                cum_after: f.opt_num("cum")?.and_then(Lots::new),
                liquidity: match f.opt("liq") {
                    Some("M") => Liquidity3::Maker,
                    Some("T") => Liquidity3::Taker,
                    _ => Liquidity3::Unknown,
                },
                fee: scope.fee(f.num("fee")?, usdc())?,
                realized_pnl: f.opt_usdc("pnl")?,
                realized_funding: f.opt_usdc("fund")?,
                replay: f.flag("replay").unwrap_or(false),
            }),
            "ack" => ExecEvent::Outcome {
                rpc: RpcId(f.num("rpc")?),
                item: Some(ItemRef {
                    idx: f.num("i")?,
                    cid: None,
                    vid: None,
                }),
                outcome: SubmitOutcome::Accepted {
                    vid: Some(scope.venue_order_id(f.get("vid")?)?),
                    ack: AckLevel::Final,
                },
            },
            "rej" => ExecEvent::Outcome {
                rpc: RpcId(f.num("rpc")?),
                item: Some(ItemRef {
                    idx: f.num("i")?,
                    cid: None,
                    vid: None,
                }),
                outcome: SubmitOutcome::Rejected(Reject {
                    kind: match f.get("code")? {
                        "PO" => RejectKind::PostOnlyWouldCross,
                        _ => RejectKind::Other,
                    },
                    venue_code: Some(f.get("code")?.into()),
                    raw: Arc::from(line),
                }),
            },
            _ => return Err(DecodeError::Malformed("kind")),
        };
        sink.push(meta, event);
        Ok(())
    }
}

fn side_code(side: Side) -> &'static str {
    match side {
        Side::Buy => "B",
        Side::Sell => "S",
    }
}

impl ExecCodec for ToyExec {
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects) {
        assert_eq!(stream, EXEC_STREAM);
        let frame = format!("hello|ts={}|nonce={:?}", ctx.wall.0, ctx.nonce(0));
        ToyExec::send(fx, frame, None, TrafficClass::Safety);
    }

    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason> {
        let mut receipt = EncodeReceipt::default();
        let (frame, class) = match cmd {
            VenueCommand::Place(o) => {
                let (frame, nonce) = self.place_frame(o, 0, specs, ctx)?;
                receipt.nonces.push((0, nonce));
                (frame, ToyExec::class(o.reduce_only))
            }
            VenueCommand::PlaceBatch(orders) => {
                let mut frames = Vec::new();
                for (idx, o) in (0u16..).zip(orders) {
                    let (frame, nonce) = self.place_frame(o, idx, specs, ctx)?;
                    frames.push(frame);
                    receipt.nonces.push((idx, nonce));
                }
                // Safety only when every item is reducing.
                let reducing = orders.iter().all(|o| o.reduce_only);
                (frames.join("\n"), ToyExec::class(reducing))
            }
            VenueCommand::Amend(a) => {
                let spec = specs.get(a.inst).ok_or(NotSentReason::Unencodable)?;
                // The toy amends by venue id only: an amend naming none has no target.
                let vid = a.target.venue().ok_or(NotSentReason::Unsupported)?;
                let nonce = ctx.nonce(0).ok_or(NotSentReason::Unencodable)?;
                // The toy's amend quantity is the remaining quantity (its caps say so).
                let qty = a
                    .wire_qty(AmendQty::Remaining)
                    .ok_or(NotSentReason::Unencodable)?;
                let wire = AmendWire {
                    spec,
                    vid: Some(vid),
                    cid: None,
                    side: a.side,
                    px: a.px,
                    qty,
                    tif: a.tif,
                    channel: a.channel,
                    post_only: a.post_only,
                    reduce_only: a.reduce_only,
                    wall: ctx.wall,
                    nonce: Some(nonce),
                };
                let sig = self
                    .signer
                    .sign_amend(&wire)
                    .map_err(|_| NotSentReason::SignFailed)?;
                receipt.nonces.push((0, nonce));
                let frame = format!(
                    "amend|vid={}|px={}|qty={}|ts={}|nonce={nonce}|sig={}",
                    vid.as_str(),
                    a.px.0,
                    qty.get(),
                    ctx.wall.0,
                    hex(&sig)
                );
                (frame, ToyExec::class(a.reduce_only))
            }
            VenueCommand::Cancel(c) => {
                let spec = specs.get(c.inst).ok_or(NotSentReason::Unencodable)?;
                let wire = CancelWire {
                    spec,
                    target: &c.target,
                    side: c.side,
                    placement_nonce: c.placement_nonce,
                    wall: ctx.wall,
                    nonce: None,
                };
                let sig = self
                    .signer
                    .sign_cancel(&wire)
                    .map_err(|_| NotSentReason::SignFailed)?;
                assert!(sig.is_none(), "the toy's cancels are unsigned");
                let vid = c.target.venue().ok_or(NotSentReason::Unsupported)?;
                let frame = format!("cancel|vid={}|ts={}", vid.as_str(), ctx.wall.0);
                (frame, TrafficClass::Safety)
            }
            VenueCommand::Query(q) => {
                let spec = specs.get(q.inst).ok_or(NotSentReason::Unencodable)?;
                let by = match (q.target.venue(), q.placement_nonce) {
                    (Some(vid), _) => format!("vid={}", vid.as_str()),
                    (None, Some(nonce)) => format!("nonce={nonce}"),
                    (None, None) => return Err(NotSentReason::Unsupported),
                };
                let frame = format!("query|sym={}|{by}", spec.venue_symbol.as_wire());
                (frame, TrafficClass::Safety)
            }
            _ => return Err(NotSentReason::Unsupported),
        };
        ToyExec::send(fx, format!("rpc={}\n{frame}", rpc.0), Some(rpc), class);
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
        let text = std::str::from_utf8(f.bytes()).map_err(|_| DecodeError::Malformed("utf-8"))?;
        ToyExec::decode_exec(text, scope, specs, sink)
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let resp = match resp {
            Ok(resp) if tag == RESYNC_TAG && resp.status == 200 => resp,
            // No snapshot came: ask again in a second.
            Err(_) if tag == RESYNC_TAG => {
                fx.push(Effect::Timer {
                    tag: RESYNC_RETRY_TAG,
                    after: Duration::from_secs(1),
                });
                return Ok(());
            }
            _ => return Err(DecodeError::Malformed("resync response")),
        };
        let body = std::str::from_utf8(resp.body).map_err(|_| DecodeError::Malformed("body"))?;
        body.lines()
            .try_for_each(|line| ToyExec::decode_exec(line, scope, specs, sink))
    }

    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects) {
        if tag == RESYNC_RETRY_TAG {
            return self.resync(ctx, fx);
        }
        assert_eq!(tag, PING_TAG);
        ToyExec::send(
            fx,
            format!("ping|ts={}", ctx.wall.0),
            None,
            TrafficClass::Safety,
        );
    }

    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink) {
        let outcome = SubmitOutcome::Unknown;
        let item = None;
        sink.push(VenueMeta::NONE, ExecEvent::Outcome { rpc, item, outcome });
    }

    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects) {
        fx.push(Effect::Http {
            tag: RESYNC_TAG,
            req: HttpRequest {
                method: HttpMethod::Get,
                url: format!("{}/snapshot?at={}", self.url, ctx.wall.0),
                headers: vec![Header {
                    name: "Accept",
                    value: "text/plain".to_owned(),
                    redact: false,
                }],
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: RPC_TIMEOUT,
            class: TrafficClass::Safety,
        });
    }
}

/// The toy venue's factory.
struct ToyFactory;

static SCHEMA: [FieldSpec; 1] = [FieldSpec {
    key: URL_KEY,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "Base URL of the toy venue.",
}];

fn url(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    cfg.get(URL_KEY).ok_or(ConfigError::Missing(URL_KEY))
}

impl VenueFactory for ToyFactory {
    fn id(&self) -> &'static str {
        "TOY"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        &SCHEMA
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        url(cfg)?;
        Ok(toy_caps())
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let mut symbols = BTreeSet::new();
        for sub in subs {
            symbols.insert(symbol_of(specs, sub.inst)?);
        }
        let symbols: Vec<&str> = symbols.into_iter().collect();
        let mut url = format!("{}/md", url(cfg)?);
        if !symbols.is_empty() {
            url = format!("{url}?symbols={}", symbols.join(","));
        }
        Ok(vec![EndpointPlan {
            stream: StreamId(0),
            url,
            subs: subs.iter().copied().collect(),
        }])
    }

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(vec![ExecEndpoint {
            stream: EXEC_STREAM,
            url: format!("{}/exec", url(cfg)?),
        }])
    }

    fn md_codec(&self, _cfg: &VenueConfig, _ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(ToyMd { pings: 0 })
    }

    fn exec_codec(&self, cfg: &VenueConfig) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(
            url(cfg)
                .map(|url| Box::new(ToyExec::new(url)) as Box<dyn ExecCodec>)
                .map_err(VenueError::from),
        )
    }
}

fn toy_caps() -> VenueCaps {
    VenueCaps {
        order: Some(OrderCaps {
            kinds: TagSet::of(&[OrderKindTag::Limit]),
            tifs: TagSet::of(&[TifTag::Gtc]),
            channels: TagSet::of(&[Channel::Public]),
            post_only: true,
            reduce_only: true,
            flag_conflicts: vec![],
            amend: Some(AmendCaps {
                price: true,
                qty: true,
                flags: false,
                when_partially_filled: true,
                reject_keeps_original: true,
                keeps_venue_id: true,
                ack: AmendAck::ReplacedEvent,
                qty_semantics: AmendQty::Remaining,
                keeps_priority: None,
            }),
            cancel_refs: TagSet::of(&[RefKind::Venue]),
            query_refs: TagSet::of(&[RefKind::Venue, RefKind::PlacementNonce]),
            cancel_before_ack: false,
            cancel_is_signed: false,
            batch_place: Some(Batch { max_items: 4 }),
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
            events_echo_flags: true,
            sign_cost_hint_us: 1,
        }),
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
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            encoding: Encoding::Json,
            touch_sources: vec![TouchSourceCaps {
                channel: "touch",
                cadence: Cadence::Realtime,
                seq_domain: SeqDomain::SharedWithBook,
                ts_kind: ExchTsKind::MatchingEngine,
                includes_channels: TagSet::of(&[Channel::Public]),
            }],
            books: vec![BookCaps {
                channel: "snap",
                max_depth: 1,
                cadence: Cadence::Realtime,
                continuity: Continuity::PlusOne,
                windowed: false,
                rest_anchor: true,
                includes_channels: TagSet::of(&[Channel::Public]),
                queue_model: QueueModelQuality::BracketOnly,
            }],
            trades: TradeCaps {
                source: FeedSource::Stream,
                aggressor: true,
                trade_id: true,
            },
            funding: FundingCaps {
                source: FeedSource::Stream,
                interval_reported: true,
                next_time_reported: true,
            },
            stats: FeedSource::None,
            ts_precision: Duration::from_nanos(1),
            topology: ConnTopology::Shared {
                max_subscriptions: None,
            },
            max_conn_lifetime: None,
        },
        limits: vec![],
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
struct Collect<B> {
    out: Vec<Envelope<B>>,
}

impl<B> Collect<B> {
    fn new() -> Collect<B> {
        Collect { out: Vec::new() }
    }

    fn bodies(&self) -> Vec<&B> {
        self.out.iter().map(|e| &e.body).collect()
    }
}

impl MdSink for Collect<MdEvent> {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.out.push(Envelope::new(STAMP, meta, ev));
    }
}

impl ExecSink for Collect<ExecEvent> {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.out.push(Envelope::new(STAMP, meta, ev));
    }
}

/// Runs `f` with the decode scope the core's dispatch lends for the toy venue.
fn with_scope<R>(f: impl for<'s> FnOnce(&'s DecodeScope<'s>) -> R) -> R {
    dispatch(&CID_FORMAT, OWN_NS, FEE_SIGN, f)
}

fn toy_spec() -> InstrumentSpec {
    InstrumentSpec {
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
    }
}

fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    table.insert(toy_spec());
    table
}

fn config() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(URL_KEY, "https://toy.invalid");
    cfg
}

fn exec_codec() -> Box<dyn ExecCodec> {
    ToyFactory.exec_codec(&config()).unwrap().unwrap()
}

/// Decodes `lines`, one frame each, through the toy's exec codec inside the decode scope.
fn decode_exec(codec: &mut dyn ExecCodec, lines: &[&str]) -> Collect<ExecEvent> {
    let (specs, mut sink, mut fx) = (specs(), Collect::new(), Effects::new());
    with_scope(|scope| {
        for line in lines {
            let frame = RawFrame::Text(line);
            let decoded = codec.on_frame(EXEC_STREAM, frame, scope, &specs, &mut sink, &mut fx);
            decoded.unwrap();
        }
    });
    assert!(fx.is_empty(), "decoding asked for effects: {fx:?}");
    sink
}

/// A directory for namespace leases, removed when dropped.
struct LockDir(PathBuf);

impl LockDir {
    fn new() -> LockDir {
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fbc-toy-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        LockDir(dir)
    }
}

impl Drop for LockDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `n` client ids minted in the toy's namespace.
fn mint(n: usize) -> Vec<ClientOrderId> {
    let dir = LockDir::new();
    let lease = NamespaceLease::acquire(&dir.0, fbc_core::AccountKey::new(1), OWN_NS).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_759_363_200_000_000_000));
    (0..n).map(|_| mint.mint().unwrap()).collect()
}

fn order(cid: ClientOrderId, px: i64) -> NewOrder {
    NewOrder {
        cid,
        inst: INST,
        side: Side::Buy,
        qty: Lots::new(25).unwrap(),
        kind: OrderKind::Limit { px: Ticks(px) },
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
    }
}

fn ctx(wall: i64, first_nonce: u64, len: u16) -> EncodeCtx {
    EncodeCtx {
        wall: WallNs(wall),
        mono: MonoNs(77),
        nonces: NonceBlock::consecutive(first_nonce, len).unwrap(),
    }
}

/// Encodes `cmd` and returns the bytes of its one frame.
fn encode_once(codec: &mut dyn ExecCodec, cmd: &VenueCommand, ctx: &EncodeCtx) -> Vec<u8> {
    let mut fx = Effects::new();
    codec
        .encode(cmd, RpcId(11), &specs(), ctx, &mut fx)
        .unwrap();
    match fx.take().as_slice() {
        [Effect::Send { frame, .. }] => frame.bytes().to_vec(),
        other => panic!("expected one frame, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn the_toy_decodes_account_events_a_full_resync_and_a_fill_into_their_exec_events() {
    let [cid] = mint(1).try_into().unwrap();
    let wire_cid = encode_cid(&CID_FORMAT, cid).unwrap();
    let mut codec = exec_codec();
    let frames = [
        "pos|ts=1759363200100000000|seq=1|sym=TOY-PERP|qty=-25|avg=65432.125".to_owned(),
        "bal|ts=1759363200100000001|seq=2|equity=1250000000000|avail=900000000000".to_owned(),
        "fundpay|ts=1759363200100000002|seq=3|sym=TOY-PERP|amt=-1250000".to_owned(),
        "rbegin|wm=1759363200050000000".to_owned(),
        format!("rorder|sym=TOY-PERP|vid=V-1|cid={wire_cid}|side=B|px=130865|qty=25|cum=5|po=1"),
        "rpos|sym=TOY-PERP|qty=-25|avg=65432.125".to_owned(),
        "rend".to_owned(),
        format!(
            "fill|ts=1759363200200000000|seq=4|sym=TOY-PERP|fid=F-9|vid=V-1|cid={wire_cid}\
             |side=B|px=130865|qty=3|cum=8|liq=M|fee=150|pnl=2500000|fund=-30000"
        ),
    ];
    let lines: Vec<&str> = frames.iter().map(String::as_str).collect();
    let sink = decode_exec(codec.as_mut(), &lines);

    let (vid, fid, fee) = with_scope(|scope| {
        (
            scope.venue_order_id("V-1").unwrap(),
            scope.fill_id("F-9").unwrap(),
            scope.fee(150, usdc()).unwrap(),
        )
    });
    // The toy reports a 150-nano rebate as +150; the scope applied its declared sign.
    assert_eq!(fee.cost(), Money::new(-150, usdc()));
    let avg = Some("65432.125".parse::<PxExact>().unwrap());
    let expected = [
        ExecEvent::Position {
            inst: INST,
            qty: SignedLots(-25),
            avg_entry: avg,
        },
        ExecEvent::Balance {
            equity: Money::new(1_250_000_000_000, usdc()),
            available: Money::new(900_000_000_000, usdc()),
        },
        ExecEvent::FundingPaid {
            inst: INST,
            amount: Money::new(-1_250_000, usdc()),
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
            qty: Lots::new(25).unwrap(),
            cum_filled: Lots::new(5).unwrap(),
            post_only: Some(true),
            reduce_only: None,
        }),
        ExecEvent::ResyncPosition {
            inst: INST,
            qty: SignedLots(-25),
            avg_entry: avg,
        },
        ExecEvent::ResyncEnd,
        ExecEvent::Fill(FillEvent {
            key: FillKey::Venue(fid),
            cid: Some(CidMatch::Ours(cid)),
            vid: Some(vid),
            inst: INST,
            side: Side::Buy,
            px: Ticks(130_865),
            qty: Lots::new(3).unwrap(),
            cum_after: Some(Lots::new(8).unwrap()),
            liquidity: Liquidity3::Maker,
            fee,
            realized_pnl: Some(Money::new(2_500_000, usdc())),
            realized_funding: Some(Money::new(-30_000, usdc())),
            replay: false,
        }),
    ];
    assert_eq!(sink.bodies(), expected.iter().collect::<Vec<_>>());

    // Every envelope carries the runtime's stamp; the venue's time and sequence are kept as
    // sent, and a frame without them gets none (exchange time is never synthesized).
    assert!(sink.out.iter().all(|e| e.stamp == STAMP));
    let fill = sink.out.last().unwrap();
    assert_eq!(fill.exch_ts, Some(ExchNs(1_759_363_200_200_000_000)));
    assert_eq!(fill.exch_ts_kind, ExchTsKind::MatchingEngine);
    assert_eq!(fill.venue_seq, Some(4));
    assert_eq!(sink.out[3].meta(), VenueMeta::NONE);
}

#[test]
fn encoding_the_same_command_with_the_same_encode_ctx_twice_gives_identical_bytes() {
    let [a, b] = mint(2).try_into().unwrap();
    let place = VenueCommand::Place(order(a, 130_865));
    let batch = VenueCommand::PlaceBatch(vec![order(a, 130_865), order(b, 130_864)]);
    let ctx = ctx(1_759_363_200_300_000_000, 9_000, 2);
    for cmd in [&place, &batch] {
        let mut codec = exec_codec();
        let first = encode_once(codec.as_mut(), cmd, &ctx);
        // The same codec again, and a fresh one: codec state does not leak into the bytes.
        assert_eq!(encode_once(codec.as_mut(), cmd, &ctx), first);
        assert_eq!(encode_once(exec_codec().as_mut(), cmd, &ctx), first);
    }
}

#[test]
fn encode_takes_time_and_nonces_only_from_the_encode_ctx() {
    let [a, b] = mint(2).try_into().unwrap();
    let place = VenueCommand::Place(order(a, 130_865));
    let at = ctx(1_759_363_200_300_000_000, 9_000, 1);
    let text = String::from_utf8(encode_once(exec_codec().as_mut(), &place, &at)).unwrap();
    let wire_cid = encode_cid(&CID_FORMAT, a).unwrap();
    assert!(
        text.starts_with(&format!(
            "rpc=11\nplace|i=0|cid={wire_cid}|sym=TOY-PERP|side=B|px=130865|qty=25|tif=Gtc|po=1\
             |ro=0|ts=1759363200300000000|nonce=9000|sig="
        )),
        "{text}"
    );

    // Another wall time or nonce changes the bytes; another monotonic time does not, since
    // nothing in the payload is a monotonic instant.
    let mut codec = exec_codec();
    let bytes = encode_once(codec.as_mut(), &place, &at);
    let later = EncodeCtx {
        wall: WallNs(at.wall.0 + 1),
        ..at.clone()
    };
    let next_nonce = ctx(at.wall.0, 9_001, 1);
    let other_mono = EncodeCtx {
        mono: MonoNs(1),
        ..at
    };
    assert_ne!(encode_once(codec.as_mut(), &place, &later), bytes);
    assert_ne!(encode_once(codec.as_mut(), &place, &next_nonce), bytes);
    assert_eq!(encode_once(codec.as_mut(), &place, &other_mono), bytes);

    // A batch takes one nonce per item from the block, and the receipt reports each.
    let batch = VenueCommand::PlaceBatch(vec![order(a, 130_865), order(b, 130_864)]);
    assert_eq!(batch.items(), Some(2));
    let mut fx = Effects::new();
    let receipt = codec
        .encode(&batch, RpcId(12), &specs(), &ctx(5, 700, 2), &mut fx)
        .unwrap();
    assert_eq!(receipt.nonces, vec![(0, 700), (1, 701)]);
    let [
        Effect::Send {
            frame,
            rpc,
            class,
            stream,
        },
    ] = fx.as_slice()
    else {
        panic!("expected one frame: {fx:?}");
    };
    // An RPC frame always carries its deadline.
    let call = RpcCall {
        id: RpcId(12),
        timeout: RPC_TIMEOUT,
    };
    assert_eq!(
        (*stream, *rpc, *class),
        (EXEC_STREAM, Some(call), TrafficClass::Normal)
    );
    assert!(frame.redactions().is_empty());

    // A batch of reducing orders only is safety traffic, like a single reducing order; a batch
    // that mixes in a non-reducing order is normal traffic (TrafficClass).
    let reducing = |cid, px| NewOrder {
        reduce_only: true,
        ..order(cid, px)
    };
    let class_of = |cmd: &VenueCommand| {
        let mut fx = Effects::new();
        exec_codec()
            .encode(cmd, RpcId(12), &specs(), &ctx(5, 700, 2), &mut fx)
            .unwrap();
        match fx.as_slice() {
            [Effect::Send { class, .. }] => *class,
            other => panic!("{other:?}"),
        }
    };
    let all_reducing = VenueCommand::PlaceBatch(vec![reducing(a, 130_866), reducing(b, 130_867)]);
    let mixed = VenueCommand::PlaceBatch(vec![reducing(a, 130_866), order(b, 130_864)]);
    assert_eq!(class_of(&all_reducing), TrafficClass::Safety);
    assert_eq!(class_of(&mixed), TrafficClass::Normal);

    // Nonces need not be consecutive (a venue whose NonceScope is Random): each item takes the
    // value reserved for it, whatever the others are.
    let random = EncodeCtx {
        nonces: NonceBlock::new(vec![0x9e37_79b9_7f4a_7c15, 13]),
        ..ctx(5, 0, 0)
    };
    let mut fx = Effects::new();
    let receipt = codec
        .encode(&batch, RpcId(12), &specs(), &random, &mut fx)
        .unwrap();
    assert_eq!(receipt.nonces, vec![(0, 0x9e37_79b9_7f4a_7c15), (1, 13)]);

    // Without enough nonces in the context the command is not sent, and nothing is pushed.
    let mut fx = Effects::new();
    let short = codec.encode(&batch, RpcId(13), &specs(), &ctx(5, 700, 1), &mut fx);
    assert_eq!(short, Err(NotSentReason::Unencodable));
    assert!(fx.is_empty());
}

// ---------------------------------------------------------------------------------------------
// The rest of the boundary.
// ---------------------------------------------------------------------------------------------

#[test]
fn the_toy_decodes_market_data_frames_into_md_events() {
    let mut codec = ToyFactory.md_codec(
        &config(),
        &ToyFactory
            .plan_md(&config(), &specs(), &BTreeSet::new())
            .unwrap()[0],
    );
    let (specs, mut sink, mut fx) = (specs(), Collect::new(), Effects::new());
    let lines = [
        "touch|ts=10|seq=5|sym=TOY-PERP|bid=130865x12|ask=130866x7",
        "touch|ts=11|seq=6|sym=TOY-PERP|ask=130866x7",
        "trade|ts=12|sym=TOY-PERP|id=99|aggr=S|px=130865|qty=4",
        "funding|ts=13|sym=TOY-PERP|rate=125000|every=3600|next=1759366800000000000",
        "mark|sym=TOY-PERP|px=65432.123456789",
        "gap|sym=TOY-PERP|feed=book",
        "gap|sym=TOY-PERP|feed=trades",
    ];
    with_scope(|scope| {
        for line in lines {
            codec
                .on_frame(RawFrame::Text(line), scope, &specs, &mut sink, &mut fx)
                .unwrap();
        }
        // The book anchor arrives over HTTP and decodes into a snapshot.
        let resp = HttpResponse {
            status: 200,
            headers: &[("content-type", "text/plain")],
            body: b"snap|seq=7|sym=TOY-PERP|epoch=3|b=130865x12|a=130866x7",
        };
        codec
            .on_http(ANCHOR_TAG, Ok(resp), scope, &specs, &mut sink, &mut fx)
            .unwrap();
        // A request that got no response comes back too, with why.
        let lost = codec.on_http(
            ANCHOR_TAG,
            Err(HttpFailure::Lost),
            scope,
            &specs,
            &mut sink,
            &mut fx,
        );
        assert_eq!(lost, Err(DecodeError::Malformed("anchor response")));
        // A frame naming an instrument the table lacks, or no kind, is refused.
        let unknown = RawFrame::Text("touch|sym=NOPE-PERP");
        let refused = codec.on_frame(unknown, scope, &specs, &mut sink, &mut fx);
        assert_eq!(refused, Err(DecodeError::UnknownInstrument));
        let binary = RawFrame::Binary(b"\x00\x01");
        let refused = codec.on_frame(binary, scope, &specs, &mut sink, &mut fx);
        assert_eq!(refused, Err(DecodeError::Malformed("binary frame")));
    });
    assert!(fx.is_empty());
    let lvl = |px, qty| Lvl {
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    };
    let expected = vec![
        MdEvent::Touch {
            inst: INST,
            bid: Some(lvl(130_865, 12)),
            ask: Some(lvl(130_866, 7)),
            source: TouchSourceId(0),
        },
        MdEvent::Touch {
            inst: INST,
            bid: None,
            ask: Some(lvl(130_866, 7)),
            source: TouchSourceId(0),
        },
        MdEvent::Trade {
            inst: INST,
            id: Some(99),
            aggressor: Aggressor::Seller,
            px: Ticks(130_865),
            qty: Lots::new(4).unwrap(),
        },
        MdEvent::Funding {
            inst: INST,
            rate_e12: 125_000,
            interval: Some(Duration::from_secs(3_600)),
            next: Some(WallNs(1_759_366_800_000_000_000)),
        },
        MdEvent::Mark {
            inst: INST,
            px: PxExact::new(65_432_123_456_789, -9),
        },
        // A gap names the feed it is on: the book is invalid, the trades are not.
        MdEvent::Health {
            inst: INST,
            feed: Feed::Book(0),
            h: FeedHealth::Gap,
        },
        MdEvent::Health {
            inst: INST,
            feed: Feed::Trades,
            h: FeedHealth::Gap,
        },
        MdEvent::BookSnapshotBegin {
            inst: INST,
            epoch: 3,
        },
        MdEvent::Level {
            inst: INST,
            side: BookSide::Bid,
            px: Ticks(130_865),
            qty: Lots::new(12).unwrap(),
        },
        MdEvent::Level {
            inst: INST,
            side: BookSide::Ask,
            px: Ticks(130_866),
            qty: Lots::new(7).unwrap(),
        },
        MdEvent::BookSnapshotEnd { inst: INST },
    ];
    assert_eq!(sink.bodies(), expected.iter().collect::<Vec<_>>());
    assert_eq!(sink.out[0].venue_seq, Some(5));
    assert_eq!(sink.out[4].exch_ts, None);
}

#[test]
fn the_factory_plans_and_builds_codecs_whose_only_output_is_effects() {
    let factory: &dyn VenueFactory = &ToyFactory;
    assert_eq!(factory.id(), "TOY");
    assert_eq!(factory.config_schema()[0].key, URL_KEY);
    assert_eq!(factory.caps(&config()), Ok(toy_caps()));
    let empty = VenueConfig::new();
    assert_eq!(factory.caps(&empty), Err(ConfigError::Missing(URL_KEY)));
    let refused = factory.exec_codec(&empty).unwrap().err();
    assert_eq!(
        refused,
        Some(VenueError::Config(ConfigError::Missing(URL_KEY)))
    );

    let subs: BTreeSet<Subscription> = [Feed::Book(0), Feed::Touch(TouchSourceId(0))]
        .into_iter()
        .map(|feed| Subscription { inst: INST, feed })
        .collect();
    // Planning and subscribing see the spec table, so the venue's own symbol goes on the wire
    // (in the URL and in the subscription frames), not the library's instrument id.
    let plan = factory.plan_md(&config(), &specs(), &subs).unwrap();
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].url, "https://toy.invalid/md?symbols=TOY-PERP");
    let mut md = factory.md_codec(&config(), &plan[0]);
    let mut fx = Effects::new();
    md.on_open(&mut fx);
    md.subscribe(&plan[0].subs, &[], &specs(), &mut fx).unwrap();
    md.on_timer(PING_TAG, MonoNs(5), WallNs(6), &mut fx);
    let sent: Vec<&[u8]> = fx
        .as_slice()
        .iter()
        .filter_map(|e| match e {
            Effect::Send { frame, .. } => Some(frame.bytes()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sent,
        [
            b"sub|sym=TOY-PERP|feed=Touch(TouchSourceId(0))".as_slice(),
            b"sub|sym=TOY-PERP|feed=Book(0)",
            b"ping|n=1|at=6"
        ]
    );
    assert!(matches!(
        fx.as_slice()[0],
        Effect::Timer { tag: PING_TAG, .. }
    ));
    assert_eq!(md.keepalive().map(|k| k.kind), Some(KeepaliveKind::WsPing));

    // An instrument missing from the spec table cannot be spelled: planning and subscribing
    // refuse it by id, and subscribing pushes nothing, not even for the instruments it knows.
    let stranger = Subscription {
        inst: InstrumentId::new(99),
        feed: Feed::Trades,
    };
    let unknown = Err(VenueError::UnknownInstrument(InstrumentId::new(99)));
    let mixed: BTreeSet<Subscription> = subs.iter().copied().chain([stranger]).collect();
    assert_eq!(factory.plan_md(&config(), &specs(), &mixed), unknown);
    let mut fx = Effects::new();
    let refused = md.subscribe(&plan[0].subs, &[stranger], &specs(), &mut fx);
    assert_eq!(
        refused,
        Err(VenueError::UnknownInstrument(InstrumentId::new(99)))
    );
    assert!(fx.is_empty());

    // The factory plans the order-entry connection the runtime opens before on_open, so the
    // runtime needs no venue knowledge to reach order entry.
    let exec_plan = factory.plan_exec(&config()).unwrap();
    assert_eq!(
        exec_plan,
        [ExecEndpoint {
            stream: EXEC_STREAM,
            url: "https://toy.invalid/exec".to_owned()
        }]
    );
    assert_eq!(
        factory.plan_exec(&empty),
        Err(VenueError::Config(ConfigError::Missing(URL_KEY)))
    );

    // The client-id format has one source, the capabilities: the runtime decodes with it, and
    // what the exec codec puts on the wire decodes under it as ours.
    let format = factory.caps(&config()).unwrap().order.unwrap().client_id;
    let [cid] = mint(1).try_into().unwrap();
    let text = String::from_utf8(encode_once(
        exec_codec().as_mut(),
        &VenueCommand::Place(order(cid, 130_865)),
        &ctx(1, 1, 1),
    ))
    .unwrap();
    let wire = text
        .split('|')
        .find_map(|kv| kv.strip_prefix("cid="))
        .unwrap();
    assert_eq!(decode_cid(&format, OWN_NS, wire), CidMatch::Ours(cid));

    // The exec codec opens, pings and resyncs by asking for effects, and decodes the resync
    // response it asked for.
    let mut exec = exec_codec();
    let mut fx = Effects::new();
    let at = ctx(1_000, 1, 1);
    exec.on_open(EXEC_STREAM, &at, &mut fx);
    exec.on_timer(PING_TAG, &at, &mut fx);
    exec.resync(&at, &mut fx);
    let effects = fx.take();
    assert!(fx.is_empty());
    assert_eq!(effects.len(), 3);
    let Effect::Http {
        tag, req, class, ..
    } = &effects[2]
    else {
        panic!("expected the resync request: {effects:?}");
    };
    assert_eq!((*tag, *class), (RESYNC_TAG, TrafficClass::Safety));
    assert_eq!(req.url, "https://toy.invalid/snapshot?at=1000");
    let body = b"rbegin|wm=900\nrpos|sym=TOY-PERP|qty=0\nrend";
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body,
    };
    let (specs, mut sink) = (specs(), Collect::new());
    with_scope(|scope| {
        exec.on_http(RESYNC_TAG, Ok(resp), scope, &specs, &mut sink, &mut fx)
            .unwrap();
    });
    assert_eq!(
        sink.bodies(),
        [
            &ExecEvent::ResyncBegin {
                watermark: WallNs(900)
            },
            &ExecEvent::ResyncPosition {
                inst: INST,
                qty: SignedLots(0),
                avg_entry: None
            },
            &ExecEvent::ResyncEnd,
        ]
    );

    // A snapshot request that times out (or never went out) comes back to the codec, which
    // asks for a retry; the retry timer requests the snapshot again. Nothing is decoded.
    for failure in [
        HttpFailure::TimedOut,
        HttpFailure::NotSent,
        HttpFailure::Lost,
    ] {
        let (mut sink, mut fx) = (Collect::new(), Effects::new());
        with_scope(|scope| {
            exec.on_http(RESYNC_TAG, Err(failure), scope, &specs, &mut sink, &mut fx)
                .unwrap();
        });
        assert!(sink.out.is_empty());
        assert_eq!(
            fx.as_slice(),
            [Effect::Timer {
                tag: RESYNC_RETRY_TAG,
                after: Duration::from_secs(1)
            }]
        );
        exec.on_timer(RESYNC_RETRY_TAG, &ctx(2_000, 1, 1), &mut fx);
        assert!(matches!(
            &fx.as_slice()[1],
            Effect::Http { tag: RESYNC_TAG, req, .. } if req.url.ends_with("at=2000")
        ));
    }
}

#[test]
fn an_event_without_a_client_id_says_none_rather_than_unparseable() {
    // A venue that does not echo client ids on events (OrderCaps::cid_echoed_on_events false)
    // gives no client id: that is None, not Unparseable (a non-canonical id that was present).
    let sink = decode_exec(
        exec_codec().as_mut(),
        &[
            "fill|sym=TOY-PERP|fid=F-1|vid=V-1|side=S|px=130865|qty=1|fee=0",
            "rorder|sym=TOY-PERP|vid=V-1|side=S|px=130865|qty=2|cum=1",
            "fill|sym=TOY-PERP|fid=F-2|vid=V-1|cid=java-1759363200123|side=S|px=130865|qty=1|fee=0",
        ],
    );
    let cids: Vec<Option<CidMatch>> = sink
        .bodies()
        .into_iter()
        .map(|ev| match ev {
            ExecEvent::Fill(fill) => fill.cid,
            ExecEvent::ResyncOrder(order) => order.cid,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(cids, [None, None, Some(CidMatch::Unparseable)]);
}

#[test]
fn a_live_order_update_keeps_the_flags_the_venue_echoes() {
    let [cid] = mint(1).try_into().unwrap();
    let wire_cid = encode_cid(&CID_FORMAT, cid).unwrap();
    let line =
        format!("ord|sym=TOY-PERP|vid=V-1|cid={wire_cid}|side=B|px=130860|qty=25|cum=5|po=1|ro=0");
    let sink = decode_exec(
        exec_codec().as_mut(),
        &[line.as_str(), "ord|sym=TOY-PERP|vid=V-1|side=B|cum=5"],
    );
    let vid = with_scope(|scope| scope.venue_order_id("V-1").unwrap());
    let update = |cid, px, qty, post_only, reduce_only| {
        ExecEvent::Order(OrderUpdate {
            cid,
            vid: Some(vid.clone()),
            inst: INST,
            side: Side::Buy,
            state: VenueOrderState::Open,
            cum_filled: Lots::new(5).unwrap(),
            px,
            qty,
            post_only,
            reduce_only,
        })
    };
    assert_eq!(
        sink.bodies(),
        [
            &update(
                Some(CidMatch::Ours(cid)),
                Some(Ticks(130_860)),
                Lots::new(25),
                Some(true),
                Some(false)
            ),
            &update(None, None, None, None, None),
        ]
    );
}

#[test]
fn a_venue_mode_names_the_market_it_applies_to_or_the_whole_account() {
    let sink = decode_exec(
        exec_codec().as_mut(),
        &["mode|m=halted", "mode|sym=TOY-PERP|m=cancel_only"],
    );
    assert_eq!(
        sink.bodies(),
        [
            &ExecEvent::Mode {
                scope: ModeScope::Account,
                mode: VenueMode::Halted,
            },
            &ExecEvent::Mode {
                scope: ModeScope::Instrument(INST),
                mode: VenueMode::CancelOnly,
            },
        ]
    );
}

#[test]
fn outcomes_amends_and_cancels_go_through_the_same_boundary() {
    let [cid] = mint(1).try_into().unwrap();
    let mut exec = exec_codec();

    // An unanswered request is Unknown for every item; it is never resent.
    let mut sink = Collect::new();
    exec.on_rpc_timeout(RpcId(11), &mut sink);
    assert_eq!(
        sink.bodies(),
        [&ExecEvent::Outcome {
            rpc: RpcId(11),
            item: None,
            outcome: SubmitOutcome::Unknown
        }]
    );

    // Per-item acks and rejects.
    let sink = decode_exec(
        exec.as_mut(),
        &[
            "ack|rpc=11|i=0|vid=V-1",
            "rej|rpc=11|i=1|code=PO|msg=would cross",
        ],
    );
    let vid = with_scope(|scope| scope.venue_order_id("V-1").unwrap());
    let ExecEvent::Outcome { outcome, item, .. } = &sink.out[0].body else {
        panic!("{:?}", sink.out[0]);
    };
    assert_eq!(item.as_ref().map(|i| i.idx), Some(0));
    assert_eq!(
        outcome,
        &SubmitOutcome::Accepted {
            vid: Some(vid.clone()),
            ack: AckLevel::Final
        }
    );
    let ExecEvent::Outcome {
        outcome: SubmitOutcome::Rejected(reject),
        ..
    } = &sink.out[1].body
    else {
        panic!("{:?}", sink.out[1]);
    };
    assert_eq!(reject.kind, RejectKind::PostOnlyWouldCross);
    assert_eq!(reject.venue_code.as_deref(), Some("PO"));

    // An amend carries the full order and is signed over the venue id. The toy's wire takes
    // the remaining quantity: a total of 25 with 5 filled goes out as 20, the resting quantity
    // the OMS checked. An amend to the filled quantity is not sent. A cancel of a reducing
    // kind is safety traffic.
    let amend_to = |qty, cum_filled| {
        VenueCommand::Amend(AmendOrder {
            target: OrderRef::Both(cid, vid.clone()),
            inst: INST,
            side: Side::Buy,
            tif: TifTag::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            px: Ticks(130_860),
            qty: Lots::new(qty).unwrap(),
            cum_filled: Lots::new(cum_filled).unwrap(),
        })
    };
    let amend = amend_to(25, 5);
    let text = String::from_utf8(encode_once(exec.as_mut(), &amend, &ctx(9, 4, 1))).unwrap();
    assert!(
        text.starts_with("rpc=11\namend|vid=V-1|px=130860|qty=20|ts=9|nonce=4|sig="),
        "{text}"
    );
    let mut fx = Effects::new();
    let filled = exec.encode(&amend_to(5, 5), RpcId(16), &specs(), &ctx(9, 4, 1), &mut fx);
    assert_eq!(filled, Err(NotSentReason::Unencodable));
    assert!(fx.is_empty());
    let cancel = VenueCommand::Cancel(CancelOrder {
        target: OrderRef::Venue(vid.clone()),
        inst: INST,
        side: Side::Buy,
        placement_nonce: Some(4),
    });
    let mut fx = Effects::new();
    let receipt = exec
        .encode(&cancel, RpcId(14), &specs(), &ctx(9, 5, 1), &mut fx)
        .unwrap();
    assert!(receipt.nonces.is_empty());
    let [Effect::Send { frame, class, .. }] = fx.as_slice() else {
        panic!("{fx:?}");
    };
    assert_eq!(*class, TrafficClass::Safety);
    assert_eq!(frame.bytes(), b"rpc=14\ncancel|vid=V-1|ts=9");

    // A query for an order the venue never acknowledged (no venue id yet: the Unknown ladder)
    // names it by the nonce it was placed with, which the command carries.
    let query = VenueCommand::Query(QueryOrder {
        target: OrderRef::Client(cid),
        inst: INST,
        placement_nonce: Some(4),
    });
    let text = String::from_utf8(encode_once(exec.as_mut(), &query, &ctx(9, 6, 1))).unwrap();
    assert_eq!(text, "rpc=11\nquery|sym=TOY-PERP|nonce=4");
    let by_vid = VenueCommand::Query(QueryOrder {
        target: OrderRef::Both(cid, vid.clone()),
        inst: INST,
        placement_nonce: Some(4),
    });
    let text = String::from_utf8(encode_once(exec.as_mut(), &by_vid, &ctx(9, 6, 1))).unwrap();
    assert_eq!(text, "rpc=11\nquery|sym=TOY-PERP|vid=V-1");
    let neither = VenueCommand::Query(QueryOrder {
        target: OrderRef::Client(cid),
        inst: INST,
        placement_nonce: None,
    });
    let mut fx = Effects::new();
    let refused = exec.encode(&neither, RpcId(17), &specs(), &ctx(9, 6, 1), &mut fx);
    assert_eq!(refused, Err(NotSentReason::Unsupported));
    assert!(fx.is_empty());

    // What the toy does not offer is not sent, with no effect pushed.
    let mut fx = Effects::new();
    let unsupported = exec.encode(
        &VenueCommand::FeeQuery,
        RpcId(15),
        &specs(),
        &ctx(9, 6, 1),
        &mut fx,
    );
    assert_eq!(unsupported, Err(NotSentReason::Unsupported));
    assert!(fx.is_empty());
}

#[test]
fn the_toy_declares_the_query_references_its_codec_encodes() {
    // The toy queries by venue id or, before an ack, by placement nonce: the caps say both, so
    // the capability-driven Unknown ladder reaches the nonce query.
    let caps = toy_caps().order.unwrap();
    assert!(caps.query_refs.contains(RefKind::Venue));
    assert!(caps.query_refs.contains(RefKind::PlacementNonce));
    assert!(!caps.query_refs.contains(RefKind::Client));
}

#[test]
fn planning_market_data_without_a_url_is_refused() {
    let subs: BTreeSet<Subscription> = [Subscription {
        inst: INST,
        feed: Feed::Trades,
    }]
    .into_iter()
    .collect();
    let refused = Err(VenueError::Config(ConfigError::Missing(URL_KEY)));
    for subs in [BTreeSet::new(), subs] {
        assert_eq!(
            ToyFactory.plan_md(&VenueConfig::new(), &specs(), &subs),
            refused
        );
    }
}

#[test]
fn an_amend_naming_the_order_only_by_client_id_is_not_sent() {
    // The toy amends by venue id only (its caps); an amend that names no venue id has no
    // encodable target, so it is not sent and nothing is pushed.
    let [cid] = mint(1).try_into().unwrap();
    let amend = VenueCommand::Amend(AmendOrder {
        target: OrderRef::Client(cid),
        inst: INST,
        side: Side::Buy,
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        px: Ticks(130_860),
        qty: Lots::new(25).unwrap(),
        cum_filled: Lots::new(5).unwrap(),
    });
    let mut fx = Effects::new();
    let refused = exec_codec().encode(&amend, RpcId(18), &specs(), &ctx(9, 4, 1), &mut fx);
    assert_eq!(refused, Err(NotSentReason::Unsupported));
    assert!(fx.is_empty());
}
