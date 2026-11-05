//! What the checks share: the venue's caps and first instrument under the assumed setup,
//! codecs built fresh from the factory and taken through the setup's bootstrap, client ids
//! minted under a lease of the suite's own, venue ids made in the venue's decode scope, and
//! the commands built from them, each order at a price and size inside the instrument's limits.

use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::{
    AccountKey, AmendOrder, BookId, CancelOrder, CapTag, Channel, CidMint, ClientOrderId,
    ConnState, ConnTopology, CtxCall, DecodeScope, Effect, Effects, EncodeCtx, EncodeReceipt,
    EndpointPlan, ExecCodec, ExecEvent, ExecSink, Feature, Feed, HttpResponse, HttpTag,
    InstrumentId, InstrumentSpec, Lots, MdCodec, MdTransport, Money, MonoNs, Namespace,
    NamespaceLease, NewOrder, NonceBlock, NotSentReason, OrderCaps, OrderKind, OrderKindTag,
    OrderRef, PathStamps, QueryOrder, RawFrame, RefKind, RpcId, Side, SpecTable, StreamId,
    Subscription, TagSet, Ticks, TifTag, VenueCaps, VenueCommand, VenueMeta, VenueOrderId, WallNs,
    WireUrl, dispatch, dispatch_market_data,
};

use super::{BootReply, Bootstrap, Failure, Subject};

/// The request id every probed command is encoded as.
pub(crate) const RPC: RpcId = RpcId(1);
/// The account and namespace the suite mints its client ids under, in a directory of its own.
const ACCOUNT: AccountKey = AccountKey::new(1);
pub(crate) const NAMESPACE: Namespace = Namespace::new(1);
/// The fixed encode time.
pub(crate) const WALL: WallNs = WallNs(1_759_363_200_000_000_000);
/// The stream a market-data codec the suite builds is on.
pub(crate) const MD_STREAM: StreamId = StreamId(0);
/// The address of the endpoint a market-data codec the suite builds is planned for: never
/// opened, the suite driving the codec itself.
const MD_URL: &str = "wss://conformance.invalid/md";
/// The first placement nonce an order carries; item `i`'s is this plus `i`.
const PLACEMENT_NONCE: u64 = 1_000;
/// The limit price aimed at: the valid price at or above it on the instrument's grid is used,
/// or a higher one where the instrument's minimum notional needs it ([`probe`]).
const PX: Ticks = Ticks(100);

/// What an encode gave.
#[derive(Debug)]
pub(crate) struct Encoded {
    pub result: Result<EncodeReceipt, NotSentReason>,
    pub fx: Effects,
}

/// The venue under the assumed setup.
pub(crate) struct Harness<'s> {
    check: &'static str,
    subject: &'s Subject<'s>,
    pub caps: VenueCaps,
    specs: SpecTable,
    pub inst: InstrumentId,
    /// The second instrument by id, where the setup lists one.
    pub other_inst: Option<InstrumentId>,
    /// The stream fixture frames are handed to the codec on.
    pub exec_stream: StreamId,
    /// The size of the orders the checks build, within the instrument's limits ([`probe`]).
    pub(crate) qty: Lots,
    /// Their limit price, valid on the instrument's grid (Codex r4189256906, r4189398089).
    pub(crate) px: Ticks,
    /// Why no order fits the instrument's limits, by the spec field, where none does: `px` and
    /// `qty` are then the price and size [`probe`] starts from.
    unfit: Option<(&'static str, String)>,
}

impl<'s> Harness<'s> {
    /// The venue's caps and its first instrument under the assumed setup.
    pub fn new(check: &'static str, subject: &'s Subject<'s>) -> Result<Harness<'s>, Failure> {
        let setup = subject.setup();
        let caps = Harness::caps(check, subject)?;
        let Some(spec) = setup.specs.iter().next() else {
            return Err(Failure::one(
                check,
                "setup",
                "the spec table lists no instrument",
            ));
        };
        let inst = spec.id;
        let ((px, qty), unfit) = match probe(spec) {
            Ok(order) => (order, None),
            Err(unfit) => (start(spec), Some(unfit)),
        };
        let other_inst = setup.specs.iter().nth(1).map(|spec| spec.id);
        Ok(Harness {
            check,
            subject,
            caps,
            specs: setup.specs,
            inst,
            other_inst,
            exec_stream: setup.exec_stream,
            qty,
            px,
            unfit,
        })
    }

    /// A failure naming the instrument's spec field where no order fits its limits, so a check
    /// that builds orders refuses the setup rather than send orders a codec may refuse for it.
    pub fn sized(&self) -> Result<(), Failure> {
        match &self.unfit {
            Some((field, what)) => Err(self.fail(field, what.clone())),
            None => Ok(()),
        }
    }

    /// The venue's caps under the assumed setup, read before anything needs an instrument.
    pub fn caps(check: &'static str, subject: &Subject<'_>) -> Result<VenueCaps, Failure> {
        let setup = subject.setup();
        subject.factory().caps(&setup.cfg).map_err(|e| {
            Failure::one(
                check,
                "VenueFactory::caps",
                format!("refused the setup: {e}"),
            )
        })
    }

    /// A failure of this check with the one breach of `capability`.
    pub fn fail(&self, capability: &str, what: impl Into<String>) -> Failure {
        Failure::one(self.check, capability, what)
    }

    /// A codec built fresh under the setup and taken through its bootstrap, where it has one
    /// (FBC-648o): `None` when the factory builds none.
    pub fn codec(&self) -> Result<Option<Box<dyn ExecCodec>>, Failure> {
        let setup = self.subject.setup();
        match self.subject.factory().exec_codec(&setup.cfg, setup.creds) {
            None => Ok(None),
            Some(Ok(mut codec)) => {
                if let Some(boot) = &setup.bootstrap {
                    self.bootstrap(codec.as_mut(), boot)?;
                }
                Ok(Some(codec))
            }
            Some(Err(e)) => Err(self.fail(
                "VenueFactory::exec_codec",
                format!("refused the setup: {e}"),
            )),
        }
    }

    /// `codec` opened on the order-entry stream at the fixed encode time and handed each of
    /// `boot`'s replies in turn, in the decode scope the core lends for the venue's caps: a
    /// frame on that stream, an HTTP response answering the earliest request the codec asked
    /// for that no reply has answered yet, the one its tag names or else the earliest. What it
    /// pushes and asks for otherwise is dropped. A reply it refuses, a response with no such
    /// request to answer, and an open or a reply after which the codec reports a connection
    /// closed or asks for a reconnect break `Setup.bootstrap`, named by position and kind,
    /// never by bytes (a reply may echo a credential).
    fn bootstrap(&self, codec: &mut dyn ExecCodec, boot: &Bootstrap) -> Result<(), Failure> {
        let stream = self.exec_stream;
        let ctx = self.ctx(codec.nonces_for(CtxCall::Open(stream)));
        let mut fx = Effects::new();
        codec.on_open(stream, &ctx, &mut fx);
        let mut asked: VecDeque<HttpTag> = VecDeque::new();
        if read(&mut fx, &mut asked) {
            let what = "the codec, opened, asked for a reconnect";
            return Err(self.fail("Setup.bootstrap", what));
        }
        let specs = &self.specs;
        self.decode_scope(|scope| {
            for (i, reply) in boot.replies.iter().enumerate() {
                let (mut sink, mut fx) = (Watched(false), Effects::new());
                let result = match reply {
                    BootReply::Text(text) => {
                        let frame = RawFrame::Text(text);
                        codec.on_frame(stream, frame, scope, specs, &mut sink, &mut fx)
                    }
                    BootReply::Binary(bytes) => {
                        let frame = RawFrame::Binary(bytes);
                        codec.on_frame(stream, frame, scope, specs, &mut sink, &mut fx)
                    }
                    BootReply::Http {
                        tag,
                        status,
                        headers,
                        body,
                    } => {
                        let tag = match tag {
                            None => asked.pop_front().ok_or_else(|| {
                                format!(
                                    "reply {i} is an HTTP response, but the codec has asked \
                                     for no HTTP request it is still waiting on"
                                )
                            }),
                            Some(tag) => match asked.iter().position(|t| t == tag) {
                                Some(at) => Ok(asked.remove(at).expect("a position held")),
                                None => Err(format!(
                                    "reply {i} answers HTTP request {}, which the codec has \
                                     not asked for or no longer waits on",
                                    tag.0
                                )),
                            },
                        };
                        let tag = tag.map_err(|what| self.fail("Setup.bootstrap", what))?;
                        let headers: Vec<(&str, &str)> = headers
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.as_str()))
                            .collect();
                        let resp = HttpResponse {
                            status: *status,
                            headers: &headers,
                            body,
                        };
                        codec.on_http(tag, Ok(resp), scope, specs, &mut sink, &mut fx)
                    }
                };
                if let Err(e) = result {
                    let kind = reply.kind();
                    let what = format!("reply {i} ({kind}) refused by the codec: {e}");
                    return Err(self.fail("Setup.bootstrap", what));
                }
                if read(&mut fx, &mut asked) || sink.0 {
                    let kind = reply.kind();
                    let what = format!(
                        "reply {i} ({kind}) closed the connection: the codec reported it closed \
                         or asked for a reconnect, so it never authenticated"
                    );
                    return Err(self.fail("Setup.bootstrap", what));
                }
            }
            Ok(())
        })
    }

    /// A codec built fresh, for a venue whose caps declare order entry.
    pub fn exec_codec(&self) -> Result<Box<dyn ExecCodec>, Failure> {
        self.codec()?.ok_or_else(|| {
            self.fail(
                "VenueCaps.exec",
                "declares order entry, yet exec_codec builds no codec",
            )
        })
    }

    /// Book channel `book` on as many instruments of the setup, in the spec table's order, as
    /// one connection carries under the venue's [`ConnTopology`] (Codex r4217682420): the first
    /// instrument alone where each has its own connection, the first `max_subscriptions` where
    /// a shared connection is capped, every one otherwise.
    pub fn book_subs(&self, book: BookId) -> Vec<Subscription> {
        let feed = Feed::Book(book);
        let cap = match self.caps.md.topology {
            ConnTopology::PerInstrument => Some(1),
            ConnTopology::Shared { max_subscriptions }
            | ConnTopology::SharedOneBookPerInstrument { max_subscriptions } => max_subscriptions,
            ConnTopology::PerChannel => None,
        };
        let cap = cap.map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX));
        self.specs
            .iter()
            .take(cap)
            .map(|spec| Subscription {
                inst: spec.id,
                feed,
            })
            .collect()
    }

    /// A market-data codec built fresh under the setup, for a socket endpoint on [`MD_STREAM`]
    /// planned with `subs`.
    pub fn md_codec(&self, subs: Vec<Subscription>) -> Box<dyn MdCodec> {
        let plan = EndpointPlan {
            stream: MD_STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(MD_URL),
            },
            subs,
        };
        self.subject
            .factory()
            .md_codec(&self.subject.setup().cfg, &plan)
    }

    /// Runs `f` in the decode scope the core lends a market-data session for the venue's caps.
    pub fn md_scope<R>(&self, f: impl for<'a> FnOnce(&'a DecodeScope<'a>) -> R) -> R {
        dispatch_market_data(&self.caps, f)
    }

    /// The spec table the setup gives.
    pub fn specs(&self) -> &SpecTable {
        &self.specs
    }

    /// The fixed encode time with the nonces `1..=n`.
    pub fn ctx(&self, n: u16) -> EncodeCtx {
        EncodeCtx {
            wall: WALL,
            mono: MonoNs(1),
            nonces: NonceBlock::new((1..=u64::from(n)).collect()),
        }
    }

    /// Runs `f` in the decode scope the core lends for the venue's caps, in the suite's own
    /// namespace.
    pub fn decode_scope<R>(&self, f: impl for<'a> FnOnce(&'a DecodeScope<'a>) -> R) -> R {
        dispatch(&self.caps, NAMESPACE, f)
    }

    /// `cmd` encoded by `codec` as request `rpc` at the fixed time, with one nonce per item, and
    /// the effects it asked for.
    pub fn encode(&self, codec: &mut dyn ExecCodec, cmd: &VenueCommand, rpc: RpcId) -> Encoded {
        let ctx = self.ctx(cmd.items().unwrap_or(u16::MAX));
        let mut fx = Effects::new();
        let stamps = &mut PathStamps::off();
        let result = codec.encode(cmd, rpc, &self.specs, &ctx, stamps, &mut fx);
        Encoded { result, fx }
    }

    /// Our first `n` client ids: sequence numbers 1 to `n`, as a mint with no high-water mark
    /// started at the epoch issues them.
    pub fn cids(&self, n: usize) -> Result<Vec<ClientOrderId>, Failure> {
        self.cids_from(0, 0, WallNs(0), n)
    }

    /// `n` client ids from a mint seeded with the high-water mark `hwm`, the snapshot's highest
    /// sequence number `snapshot_max` and the start time `start` ([`CidMint::new`]).
    pub fn cids_from(
        &self,
        hwm: u64,
        snapshot_max: u64,
        start: WallNs,
        n: usize,
    ) -> Result<Vec<ClientOrderId>, Failure> {
        mint(hwm, snapshot_max, start, n).map_err(|why| self.fail("setup", why))
    }

    /// The venue's order ids `conformance-0` to `conformance-<n-1>`.
    pub fn vids(&self, n: usize) -> Vec<VenueOrderId> {
        dispatch(&self.caps, NAMESPACE, |scope| {
            (0..n)
                .map(|i| scope.venue_order_id(&format!("conformance-{i}")))
                .collect::<Result<_, _>>()
        })
        .expect("a short non-empty venue id")
    }
}

/// A sink dropping what a bootstrap reply pushes, noting only whether it reported a
/// connection closed (Codex r4226499484).
struct Watched(bool);

impl ExecSink for Watched {
    fn push(&mut self, _meta: VenueMeta, ev: ExecEvent) {
        if let ExecEvent::Conn {
            state: ConnState::Closed,
            ..
        } = ev
        {
            self.0 = true;
        }
    }
}

/// The HTTP requests `fx` asks for, their tags appended to `asked` in order; whether it asks
/// for a reconnect. The rest is dropped.
fn read(fx: &mut Effects, asked: &mut VecDeque<HttpTag>) -> bool {
    let mut reconnect = false;
    for effect in fx.take() {
        match effect {
            Effect::Http { tag, .. } => asked.push_back(tag),
            Effect::Reconnect { .. } => reconnect = true,
            _ => {}
        }
    }
    reconnect
}

/// The parts of an order a venue's caps refuse or allow: its kind, time in force, channel and
/// flags.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct Shape {
    pub kind: OrderKindTag,
    pub tif: TifTag,
    pub channel: Channel,
    pub post_only: bool,
    pub reduce_only: bool,
}

impl Shape {
    /// The plainest order the caps allow: the first of their kinds (a limit order first),
    /// times in force and channels, tried in declaration order, that no declared flag conflict
    /// refuses, with no flag; `None` when they allow none.
    pub fn plain(o: &OrderCaps) -> Option<Shape> {
        Shape::sendable(o, OrderKindTag::ALL)
    }

    /// The plainest order of a kind in `kinds` the caps allow, as [`Shape::plain`].
    pub fn sendable(o: &OrderCaps, kinds: &[OrderKindTag]) -> Option<Shape> {
        let kinds = kinds.iter().copied().filter(|&k| o.kinds.contains(k));
        kinds
            .flat_map(|kind| o.tifs.iter().map(move |tif| (kind, tif)))
            .flat_map(|(kind, tif)| o.channels.iter().map(move |channel| (kind, tif, channel)))
            .map(|(kind, tif, channel)| Shape {
                kind,
                tif,
                channel,
                post_only: false,
                reduce_only: false,
            })
            .find(|shape| shape.refusal(o).is_none())
    }

    /// The first order the caps name, allowed or not (a limit order, good till cancelled, on
    /// the public book where they name none): what a probe sends when they allow no order.
    pub fn first(o: &OrderCaps) -> Shape {
        Shape {
            kind: o.kinds.iter().next().unwrap_or(OrderKindTag::Limit),
            tif: o.tifs.iter().next().unwrap_or(TifTag::Gtc),
            channel: o.channels.iter().next().unwrap_or(Channel::Public),
            post_only: false,
            reduce_only: false,
        }
    }

    /// Whether an order of this shape has `feature`.
    pub fn has(self, feature: Feature) -> bool {
        let features = [
            (Feature::PostOnly, self.post_only),
            (Feature::ReduceOnly, self.reduce_only),
            (Feature::Ioc, self.tif == TifTag::Ioc),
            (Feature::Fok, self.tif == TifTag::Fok),
            (Feature::Rpi, self.channel == Channel::Rpi),
        ];
        features.contains(&(feature, true))
    }

    /// This shape with `feature` too.
    pub fn with(self, feature: Feature) -> Shape {
        match feature {
            Feature::PostOnly => Shape {
                post_only: true,
                ..self
            },
            Feature::ReduceOnly => Shape {
                reduce_only: true,
                ..self
            },
            Feature::Ioc => Shape {
                tif: TifTag::Ioc,
                ..self
            },
            Feature::Fok => Shape {
                tif: TifTag::Fok,
                ..self
            },
            Feature::Rpi => Shape {
                channel: Channel::Rpi,
                ..self
            },
        }
    }

    /// Whether the caps declare a pair of this shape's features in conflict.
    pub fn conflicts(self, o: &OrderCaps) -> bool {
        o.flag_conflicts
            .iter()
            .any(|&(a, b)| self.has(a) && self.has(b))
    }

    /// Why the caps refuse an order of this shape: `Unsupported` for a kind, time in force,
    /// channel or flag they do not declare, then `FlagConflict` for a pair of features they
    /// declare in conflict; `None` when they allow it.
    pub fn refusal(self, o: &OrderCaps) -> Option<NotSentReason> {
        let offered = o.kinds.contains(self.kind)
            && o.tifs.contains(self.tif)
            && o.channels.contains(self.channel)
            && (!self.post_only || o.post_only)
            && (!self.reduce_only || o.reduce_only);
        if !offered {
            return Some(NotSentReason::Unsupported);
        }
        self.conflicts(o).then_some(NotSentReason::FlagConflict)
    }
}

/// The ids the checks build commands from: item `i` of a batch is order `i`.
pub(crate) struct Ids {
    pub cids: Vec<ClientOrderId>,
    pub vids: Vec<VenueOrderId>,
}

impl Ids {
    /// Ids for `n` orders.
    pub fn new(h: &Harness<'_>, n: usize) -> Result<Ids, Failure> {
        Ok(Ids {
            cids: h.cids(n)?,
            vids: h.vids(n),
        })
    }

    /// Order `i` placed with `shape` on the harness's instrument.
    pub fn order(&self, h: &Harness<'_>, i: usize, shape: Shape) -> NewOrder {
        let kind = match shape.kind {
            OrderKindTag::Limit => OrderKind::Limit { px: h.px },
            OrderKindTag::Market => OrderKind::Market,
        };
        NewOrder {
            cid: self.cids[i],
            inst: h.inst,
            side: Side::Buy,
            qty: h.qty,
            kind,
            tif: shape.tif,
            channel: shape.channel,
            post_only: shape.post_only,
            reduce_only: shape.reduce_only,
            reducing: false,
        }
    }

    /// An amend of order `i`, to `shape` as a limit order (an amend targets a resting limit
    /// order), naming it by `target`.
    pub fn amend(&self, h: &Harness<'_>, i: usize, shape: Shape, target: OrderRef) -> AmendOrder {
        let kind = OrderKindTag::Limit;
        let order = self.order(h, i, Shape { kind, ..shape });
        AmendOrder {
            target,
            inst: order.inst,
            side: order.side,
            tif: order.tif,
            channel: order.channel,
            post_only: order.post_only,
            reduce_only: order.reduce_only,
            reducing: false,
            px: h.px,
            qty: order.qty,
            cum_filled: Lots::new(0).expect("zero is a count"),
        }
    }

    /// Order `i`'s references of the kinds in `kinds`, as a target and a placement nonce, or
    /// `None` when `kinds` holds neither our id nor the venue's: every target names one.
    pub fn carrying(&self, i: usize, kinds: &[RefKind]) -> Option<(OrderRef, Option<u64>)> {
        let (cid, vid) = (self.cids[i], self.vids[i].clone());
        let target = match (
            kinds.contains(&RefKind::Client),
            kinds.contains(&RefKind::Venue),
        ) {
            (true, true) => OrderRef::Both(cid, vid),
            (true, false) => OrderRef::Client(cid),
            (false, true) => OrderRef::Venue(vid),
            (false, false) => return None,
        };
        let nonce = kinds
            .contains(&RefKind::PlacementNonce)
            .then(|| placement_nonce(i));
        Some((target, nonce))
    }

    /// A cancel of order `i` carrying `refs`.
    pub fn cancel(&self, h: &Harness<'_>, refs: (OrderRef, Option<u64>)) -> CancelOrder {
        let (target, placement_nonce) = refs;
        CancelOrder {
            target,
            inst: h.inst,
            side: Side::Buy,
            placement_nonce,
        }
    }

    /// A query for an order carrying `refs`.
    pub fn query(&self, h: &Harness<'_>, refs: (OrderRef, Option<u64>)) -> QueryOrder {
        let (target, placement_nonce) = refs;
        QueryOrder {
            target,
            inst: h.inst,
            placement_nonce,
        }
    }
}

/// The nonce order `i` was placed with.
pub(crate) fn placement_nonce(i: usize) -> u64 {
    PLACEMENT_NONCE + i as u64
}

/// The price and size an order the checks build starts from: the valid price at or above
/// [`PX`] on the instrument's grid (the lowest valid where none is), and its least size
/// ([`least_lots`]).
fn start(spec: &InstrumentSpec) -> (Ticks, Lots) {
    let grid = &spec.price_grid;
    let px = grid
        .ceil_valid(PX)
        .or_else(|| grid.lowest_valid())
        .unwrap_or(PX);
    (px, least_lots(spec))
}

/// The least size of an order on `spec`: its `min_size`, one lot at least, so an amend always
/// leaves something to rest.
fn least_lots(spec: &InstrumentSpec) -> Lots {
    Lots::new(spec.min_size.get().max(1)).expect("a positive count")
}

/// Why `spec` takes no order at any price, by the spec field: its `max_order_size` below its
/// least size ([`least_lots`]).
pub(crate) fn inverted(spec: &InstrumentSpec) -> Option<(&'static str, String)> {
    let least = least_lots(spec);
    let most = spec.max_order_size.filter(|&most| most < least)?;
    let what = format!(
        "{} takes orders of at most {} lots, fewer than its least, {} lots (its min_size, and \
         one lot at least): no order fits to probe with",
        spec.venue_symbol.as_wire(),
        most.get(),
        least.get()
    );
    Some(("InstrumentSpec.max_order_size", what))
}

/// `spec`'s `max_order_size` as a clause of what a refusal says, empty where it sets none.
pub(crate) fn within(spec: &InstrumentSpec) -> String {
    match spec.max_order_size {
        Some(max) => format!(" within its max_order_size of {} lots", max.get()),
        None => String::new(),
    }
}

/// Whether `q` lots at `px` on `spec` reach `min` of notional ([`InstrumentSpec::notional`]):
/// rising with the price and the size, a notional too large to hold reaches any minimum.
fn reaches(spec: &InstrumentSpec, min: Money, px: Ticks, q: i64) -> bool {
    let notional = spec.notional(px, Lots::new(q).expect("a count"));
    notional.is_none_or(|n| n.nanos >= min.nanos)
}

/// Whether the notional of `q` lots at `px` on `spec` is one it can hold, and reaches `min`.
fn holds(spec: &InstrumentSpec, min: Money, px: Ticks, q: i64) -> bool {
    let notional = spec.notional(px, Lots::new(q).expect("a count"));
    notional.is_some_and(|n| n.nanos >= min.nanos)
}

/// The least size of an order at `px` inside `spec`'s limits (Codex r4189398089): from its least
/// size ([`least_lots`]), the least whose notional ([`InstrumentSpec::notional`], in its quote
/// currency, as `min_notional` is stated) reaches its `min_notional`, never past its
/// `max_order_size`. A binary search over the spec's own notional. `None` where no size within
/// them reaches it, or the least that does has a notional too large to hold.
pub(crate) fn least_size(spec: &InstrumentSpec, px: Ticks) -> Option<Lots> {
    let least = least_lots(spec).get();
    let most = spec.max_order_size.map_or(i64::MAX, Lots::get);
    let q = match spec.min_notional {
        None => (least <= most).then_some(least)?,
        Some(min) => {
            let q = least_true(least, most, |q| reaches(spec, min, px, q))?;
            holds(spec, min, px, q).then_some(q)?
        }
    };
    Lots::new(q)
}

/// Whether an order of `q` at `px` is inside `spec`'s limits: no smaller than its least size
/// ([`least_lots`]), no larger than its `max_order_size`, and, where it sets a `min_notional`,
/// of a notional it can hold that reaches it.
pub(crate) fn fits(spec: &InstrumentSpec, px: Ticks, q: Lots) -> bool {
    q >= least_lots(spec)
        && spec.max_order_size.is_none_or(|most| q <= most)
        && spec
            .min_notional
            .is_none_or(|min| holds(spec, min, px, q.get()))
}

/// The price and size of the orders the checks build on `spec`, inside its limits (Codex
/// r4189398089): from [`start`], the least size there ([`least_size`]); where no size within
/// its `max_order_size` reaches its `min_notional` at that price, the lowest valid price above at
/// which the largest does, with the least size that reaches it there. Why no order fits, by the
/// spec field, where none does.
fn probe(spec: &InstrumentSpec) -> Result<(Ticks, Lots), (&'static str, String)> {
    let (px, least) = start(spec);
    if let Some(unfit) = inverted(spec) {
        return Err(unfit);
    }
    let Some(min) = spec.min_notional else {
        return Ok((px, least));
    };
    let most = spec.max_order_size.map_or(i64::MAX, Lots::get);
    let sized = |px: Ticks| least_size(spec, px).map(|q| (px, q));
    let raised = || {
        let t = least_true(px.0, i64::MAX, |t| reaches(spec, min, Ticks(t), most))?;
        spec.price_grid.ceil_valid(Ticks(t))
    };
    let unfit = || {
        let what = format!(
            "no order of {}{} reaches its min_notional at a valid price: no order fits to probe \
             with",
            spec.venue_symbol.as_wire(),
            within(spec)
        );
        ("InstrumentSpec.min_notional", what)
    };
    sized(px)
        .or_else(|| raised().and_then(sized))
        .ok_or_else(unfit)
}

/// The least `x` in `lo..=hi` at which `holds`, false and then true across the range, holds;
/// `None` where it does not hold at `hi`.
fn least_true(lo: i64, hi: i64, holds: impl Fn(i64) -> bool) -> Option<i64> {
    if lo > hi || !holds(hi) {
        return None;
    }
    let (mut lo, mut hi) = (lo, hi);
    while lo < hi {
        let half = (i128::from(hi) - i128::from(lo)) / 2;
        let mid = lo + i64::try_from(half).expect("half a range of i64s");
        if holds(mid) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    Some(lo)
}

/// The reference kinds of `all` not in `declared`.
pub(crate) fn undeclared(declared: TagSet<RefKind>, all: &[RefKind]) -> Vec<RefKind> {
    all.iter()
        .copied()
        .filter(|&k| !declared.contains(k))
        .collect()
}

/// `n` client ids minted under the suite's own lease, from a mint seeded as
/// [`Harness::cids_from`] says, in a directory made for them and removed after.
fn mint(
    hwm: u64,
    snapshot_max: u64,
    start: WallNs,
    n: usize,
) -> Result<Vec<ClientOrderId>, String> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let k = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fbc-conformance-{}-{k}", std::process::id()));
    let cids = mint_in(&dir, (hwm, snapshot_max, start), n);
    let _ = fs::remove_dir_all(&dir);
    cids
}

/// `n` client ids minted under a lease taken in `dir`, created if missing, from a mint seeded
/// with `seed` (the high-water mark, the snapshot's highest sequence number, the start time).
fn mint_in(dir: &Path, seed: (u64, u64, WallNs), n: usize) -> Result<Vec<ClientOrderId>, String> {
    let shown = dir.display();
    fs::create_dir_all(dir).map_err(|e| format!("cannot create {shown}: {e}"))?;
    let lease = NamespaceLease::acquire(dir, ACCOUNT, NAMESPACE);
    let lease = lease.map_err(|e| format!("cannot mint client ids in {shown}: {e}"))?;
    let (hwm, snapshot_max, start) = seed;
    let mut mint = CidMint::new(lease, hwm, snapshot_max, start);
    (0..n)
        .map(|_| mint.mint().map_err(|e| format!("cannot mint: {e}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    #[test]
    fn the_plainest_order_skips_a_combination_a_declared_conflict_refuses() {
        // Codex r4188835160: IOC first, RPI only, and IOC with RPI in conflict.
        let mut o = crate::toy::caps().exec.unwrap().order;
        o.tifs = TagSet::of(&[TifTag::Ioc, TifTag::Fok]);
        o.channels = TagSet::of(&[Channel::Rpi]);
        o.flag_conflicts = vec![(Feature::Ioc, Feature::Rpi)];
        let plain = Shape::plain(&o).unwrap();
        assert_eq!((plain.tif, plain.channel), (TifTag::Fok, Channel::Rpi));
        assert_eq!(
            Shape::first(&o).refusal(&o),
            Some(NotSentReason::FlagConflict)
        );
        o.tifs = TagSet::of(&[TifTag::Ioc]);
        assert_eq!(Shape::plain(&o), None);
    }

    /// The toy's first instrument (a 0.5 tick, a 0.001 step, one lot least), its orders at
    /// least `min` nanos of notional and at most `max` lots.
    fn limited(min: Option<i128>, max: Option<i64>) -> InstrumentSpec {
        let mut spec = crate::toy::specs().get(crate::toy::INST_A).unwrap().clone();
        spec.min_notional = min.map(|nanos| fbc_core::Money::new(nanos, spec.quote_ccy));
        spec.max_order_size = max.map(|max| Lots::new(max).unwrap());
        spec
    }

    /// `probe`'s order as a tick and a count of lots.
    fn probed(spec: &InstrumentSpec) -> (i64, i64) {
        let (px, qty) = probe(spec).unwrap();
        (px.0, qty.get())
    }

    #[test]
    fn the_probe_order_is_the_least_size_then_the_lowest_price_inside_the_limits() {
        const USDC: i128 = 1_000_000_000;
        // No limit: one lot at tick 100 (50.0).
        assert_eq!(probed(&limited(None, None)), (100, 1));
        assert_eq!(probed(&limited(None, Some(1))), (100, 1));
        // 10 USDC: 200 lots at 50.0, within 500 lots or none.
        assert_eq!(probed(&limited(Some(10 * USDC), Some(500))), (100, 200));
        assert_eq!(probed(&limited(Some(10 * USDC), None)), (100, 200));
        // Within 100 lots: 100.0 (tick 200), and a tick above it for a cent more.
        assert_eq!(probed(&limited(Some(10 * USDC), Some(100))), (200, 100));
        let cent = USDC / 100;
        assert_eq!(
            probed(&limited(Some(10 * USDC + cent), Some(100))),
            (201, 100)
        );

        // A banded grid, 100.0 in steps of 0.5 and 200.0 (tick 400) in steps of 1.5: the price
        // rises to the first band, then onto the second band's step, and the size to reach the
        // minimum there.
        let mut banded = limited(Some(30 * USDC + 5 * cent), Some(100));
        let bands = [
            (Decimal::new(1000, 1), Decimal::new(5, 1)),
            (Decimal::new(2000, 1), Decimal::new(15, 1)),
        ];
        banded.price_grid = fbc_core::PriceGrid::banded(&bands).unwrap();
        assert_eq!(probed(&banded), (603, 100));
        banded.min_notional = None;
        assert_eq!(probed(&banded), (200, 1));
    }

    #[test]
    fn the_least_size_at_a_price_reaches_the_minimum_within_the_largest_order() {
        const USDC: i128 = 1_000_000_000;
        let lots = |n: i64| Lots::new(n).unwrap();
        // One lot (0.001) at tick 9 (4.5) is 0.0045 USDC: 2223 lots reach 10 USDC, and 2000 at
        // tick 10 (5.0).
        let spec = limited(Some(10 * USDC), Some(2223));
        assert_eq!(least_size(&spec, Ticks(9)), Some(lots(2223)));
        assert_eq!(least_size(&spec, Ticks(10)), Some(lots(2000)));
        assert!(fits(&spec, Ticks(10), lots(2223)));
        assert!(!fits(&spec, Ticks(10), lots(1999)));
        assert!(!fits(&spec, Ticks(10), lots(2224)));
        assert_eq!(
            least_size(&limited(Some(10 * USDC), Some(2222)), Ticks(9)),
            None
        );
        // No minimum: the least size wherever the largest order takes it.
        let mut spec = limited(None, Some(1));
        assert_eq!(least_size(&spec, Ticks(9)), Some(lots(1)));
        assert_eq!(inverted(&spec), None);
        spec.min_size = lots(2);
        assert_eq!(least_size(&spec, Ticks(9)), None);
        assert!(!fits(&spec, Ticks(9), lots(1)));
        assert_eq!(inverted(&spec).unwrap().0, "InstrumentSpec.max_order_size");
        assert_eq!(within(&spec), " within its max_order_size of 1 lots");
        assert_eq!(within(&limited(None, None)), "");
    }

    #[test]
    fn a_search_over_no_range_or_one_never_true_finds_nothing() {
        assert_eq!(least_true(2, 1, |_| true), None);
        assert_eq!(least_true(1, 9, |_| false), None);
        assert_eq!(least_true(i64::MIN, i64::MAX, |x| x >= -3), Some(-3));
        assert_eq!(least_true(5, 5, |_| true), Some(5));
    }

    #[test]
    fn minting_reports_a_directory_it_cannot_create() {
        let file =
            std::env::temp_dir().join(format!("fbc-conformance-file-{}", std::process::id()));
        fs::write(&file, b"not a directory").unwrap();
        let err = mint_in(&file.join("under"), (0, 0, WallNs(0)), 1).unwrap_err();
        let _ = fs::remove_file(&file);
        assert!(err.starts_with("cannot create"), "{err}");
    }

    #[test]
    fn minting_reports_a_lease_held_elsewhere() {
        let dir = std::env::temp_dir().join(format!("fbc-conformance-held-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let held = NamespaceLease::acquire(&dir, ACCOUNT, NAMESPACE).unwrap();
        let err = mint_in(&dir, (0, 0, WallNs(0)), 1).unwrap_err();
        drop(held);
        let _ = fs::remove_dir_all(&dir);
        assert!(err.starts_with("cannot mint client ids"), "{err}");
    }
}
