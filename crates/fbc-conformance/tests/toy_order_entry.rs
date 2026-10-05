//! FBC-7lx's done line: the conformance toy venue's order entry, every declared order capability
//! exercised through `ExecCodec` alone (decision 0044). Each command kind is encoded with a fixed
//! `EncodeCtx`; the signer is shown only the reference the request carries; a remaining-quantity
//! amend sends the total less `cum_filled`; an undeclared reference, time in force or channel is
//! refused `NotSent(Unsupported)` with no effect and no signer call; a batch over `max_items` is
//! refused; and every frame carries its rpc, its command's traffic class and its rate charge.

use std::sync::{Arc, Mutex, OnceLock};

use fbc_conformance::toy::{
    self, EXEC_STREAM, INST_A, INST_B, MAX_BATCH, OWN_NS, RPC_TIMEOUT, ToyExec, ToySigner,
};
use fbc_core::{
    AccountKey, AmendOrder, AmendRef, AmendWire, CancelOrder, CancelRef, CancelScope, CancelWire,
    Channel, CidMint, ClientOrderId, CtxCall, DecodeError, Effect, Effects, EncodeCtx,
    EncodeReceipt, ExecCodec, ExecEvent, ExecSink, HttpFailure, HttpTag, Inbound, InboundSpans,
    InstrumentId, Lots, MonoNs, NamespaceLease, NewOrder, NonceBlock, NotSentReason, OpKind,
    OrderKind, OrderRef, OrderSigner, PathEdge, PathMark, PathRecorder, PathStage, PathStamps,
    PlaceWire, QueryOrder, RateCharge, RawFrame, RpcCall, RpcId, Side, Sig, SignError, StreamId,
    SubmitOutcome, Ticks, TifTag, TimerTag, TrafficClass, VenueCommand, VenueMeta, VenueOrderId,
    Via, WallNs, encode_cid,
};

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const WALL: i64 = 1_759_363_200_000_000_000;
const RPC: RpcId = RpcId(11);

/// The fixed encode context: its time and one nonce per item, for batches of up to four.
fn ctx() -> EncodeCtx {
    ctx_with(WALL, &[100, 101, 102, 103])
}

fn ctx_with(wall: i64, nonces: &[u64]) -> EncodeCtx {
    let (wall, mono) = (WallNs(wall), MonoNs(77));
    let nonces = NonceBlock::new(nonces.to_vec());
    EncodeCtx { wall, mono, nonces }
}

fn vid(wire: &str) -> VenueOrderId {
    toy::with_scope(|scope| scope.venue_order_id(wire)).unwrap()
}

/// Our `n`th client id, minted once per test binary under a lease in a fresh directory.
fn cid(n: u64) -> ClientOrderId {
    static CIDS: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let cids = CIDS.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("fbc-conformance-toy-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), OWN_NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(WALL));
        let cids = (0..16).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        cids
    });
    cids[usize::try_from(n).unwrap()]
}

/// `cid` as the toy's wire spells it.
fn wire_cid(cid: ClientOrderId) -> String {
    let caps = toy::caps().exec.unwrap().order;
    encode_cid(&caps.client_id, cid).unwrap().to_string()
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn order(seq: u64, inst: InstrumentId, side: Side) -> NewOrder {
    NewOrder {
        cid: cid(seq),
        inst,
        side,
        qty: lots(25),
        kind: OrderKind::Limit { px: Ticks(130_865) },
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// An exit: reduce-only and classified reducing by the OMS.
fn exit(seq: u64, inst: InstrumentId) -> NewOrder {
    NewOrder {
        tif: TifTag::Ioc,
        post_only: false,
        reduce_only: true,
        reducing: true,
        ..order(seq, inst, Side::Sell)
    }
}

/// An amend to 10 lots at 130 860 ticks of an order 4 lots of which have filled.
fn amend(target: OrderRef) -> AmendOrder {
    AmendOrder {
        target,
        inst: INST_A,
        side: Side::Buy,
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(130_860),
        qty: lots(10),
        cum_filled: lots(4),
    }
}

fn cancel(target: OrderRef, inst: InstrumentId, side: Side) -> CancelOrder {
    CancelOrder {
        target,
        inst,
        side,
        placement_nonce: None,
    }
}

/// What a signer was shown, owned.
#[derive(Clone, Eq, PartialEq, Debug)]
enum Seen {
    Place { cid: String, nonce: Option<u64> },
    Amend { target: Target, qty: i64 },
    Cancel { target: Target, nonce: Option<u64> },
}

/// The one order reference a signer was shown.
#[derive(Clone, Eq, PartialEq, Debug)]
enum Target {
    Venue(String),
    Client(String),
    Nonce(u64),
}

impl Target {
    /// The field the toy's wire spells this reference as.
    fn field(&self) -> String {
        match self {
            Target::Venue(vid) => format!("vid={vid}"),
            Target::Client(cid) => format!("cid={cid}"),
            Target::Nonce(nonce) => format!("pnonce={nonce}"),
        }
    }
}

/// The toy's signer, recording what each call was shown.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Seen>>>);

impl Recorder {
    fn seen(&self) -> Vec<Seen> {
        self.0.lock().unwrap().clone()
    }

    fn see(&self, seen: Seen) {
        self.0.lock().unwrap().push(seen);
    }
}

impl OrderSigner for Recorder {
    fn sign_place(&mut self, w: &PlaceWire<'_>) -> Result<Sig, SignError> {
        let (cid, nonce) = (w.cid.to_owned(), w.nonce);
        self.see(Seen::Place { cid, nonce });
        ToySigner.sign_place(w)
    }

    fn sign_amend(&mut self, w: &AmendWire<'_>) -> Result<Sig, SignError> {
        let target = match w.target {
            AmendRef::Venue(vid) => Target::Venue(vid.as_str().to_owned()),
            AmendRef::Client(cid) => Target::Client(cid.to_owned()),
        };
        self.see(Seen::Amend {
            target,
            qty: w.qty.get(),
        });
        ToySigner.sign_amend(w)
    }

    fn sign_cancel(&mut self, w: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        let target = match w.target {
            CancelRef::Venue(vid) => Target::Venue(vid.as_str().to_owned()),
            CancelRef::Client(cid) => Target::Client(cid.to_owned()),
            CancelRef::PlacementNonce(nonce) => Target::Nonce(nonce),
        };
        let nonce = w.nonce;
        self.see(Seen::Cancel { target, nonce });
        ToySigner.sign_cancel(w)
    }
}

/// A codec over a recording signer, and the recorder.
fn recorded() -> (ToyExec, Recorder) {
    let recorder = Recorder::default();
    (ToyExec::new(Box::new(recorder.clone())), recorder)
}

/// A recorder of path marks.
#[derive(Default)]
struct Tape(Vec<PathMark>);

impl PathRecorder for Tape {
    fn mark(&mut self, mark: PathMark) {
        self.0.push(mark);
    }
}

/// What one encode did: its result, the effects it asked for, the path marks it made.
struct Encoded {
    result: Result<EncodeReceipt, NotSentReason>,
    fx: Vec<Effect>,
    marks: Vec<PathMark>,
}

impl Encoded {
    /// The one frame's text, for an encode that sent one.
    fn text(&self) -> String {
        match self.fx.as_slice() {
            [Effect::Send { frame, .. }] => String::from_utf8(frame.bytes().to_vec()).unwrap(),
            other => panic!("expected one frame, got {other:?}"),
        }
    }
}

fn encode_on(codec: &mut ToyExec, cmd: &VenueCommand, ctx: &EncodeCtx) -> Encoded {
    let (mut fx, mut tape) = (Effects::new(), Tape::default());
    let mut t = PathStamps::new(&mut tape);
    let result = codec.encode(cmd, RPC, &toy::specs(), ctx, &mut t, &mut fx);
    Encoded {
        result,
        fx: fx.take(),
        marks: tape.0,
    }
}

fn encode(cmd: &VenueCommand) -> Encoded {
    encode_on(&mut ToyExec::new(Box::new(ToySigner)), cmd, &ctx())
}

/// The commands the toy sends, one or more of every kind, with the frame each encodes to under
/// the fixed context.
fn sent() -> Vec<(VenueCommand, String)> {
    let (a, b) = (wire_cid(cid(1)), wire_cid(cid(5)));
    let golden = [
        (
            VenueCommand::Place(order(1, INST_A, Side::Buy)),
            format!(
                "place|rpc=11|cid={a}|sym=TOYA-PERP|side=B|px=130865|qty=25|tif=gtc|ch=public\
                 |po=1|ro=0|ts={WALL}|nonce=100|sig=d31e556178a14859"
            ),
        ),
        (
            VenueCommand::PlaceBatch(vec![exit(2, INST_A), order(3, INST_B, Side::Buy)]),
            format!(
                "batch|rpc=11|n=2\n\
                 place|i=0|cid={}|sym=TOYA-PERP|side=S|px=130865|qty=25|tif=ioc|ch=public|po=0\
                 |ro=1|ts={WALL}|nonce=100|sig=86ffcc459692fac7\n\
                 place|i=1|cid={}|sym=TOYB-PERP|side=B|px=130865|qty=25|tif=gtc|ch=public|po=1\
                 |ro=0|ts={WALL}|nonce=101|sig=469aa06c862eb162",
                wire_cid(cid(2)),
                wire_cid(cid(3)),
            ),
        ),
        (
            VenueCommand::Amend(amend(OrderRef::Both(cid(4), vid("V-4")))),
            format!(
                "amend|rpc=11|vid=V-4|sym=TOYA-PERP|side=B|px=130860|qty=6|tif=gtc|ch=public\
                 |po=1|ro=0|ts={WALL}|nonce=100|sig=d6e5cd2cb83b1bd9"
            ),
        ),
        (
            VenueCommand::Cancel(cancel(OrderRef::Client(cid(5)), INST_A, Side::Sell)),
            format!(
                "cancel|rpc=11|cid={b}|sym=TOYA-PERP|side=S|ts={WALL}|nonce=100\
                 |sig=71ba179b963647e8"
            ),
        ),
        (
            VenueCommand::CancelMany(vec![
                cancel(OrderRef::Venue(vid("V-6")), INST_A, Side::Buy),
                cancel(OrderRef::Both(cid(7), vid("V-7")), INST_A, Side::Sell),
            ]),
            format!(
                "cancels|rpc=11|n=2\n\
                 cancel|i=0|vid=V-6|sym=TOYA-PERP|side=B|ts={WALL}|nonce=100\
                 |sig=164101784f9b50dc\n\
                 cancel|i=1|vid=V-7|sym=TOYA-PERP|side=S|ts={WALL}|nonce=101\
                 |sig=424cea0e57288672"
            ),
        ),
        (
            VenueCommand::CancelAll(CancelScope::Instrument(INST_B)),
            "cancelall|rpc=11|sym=TOYB-PERP".to_owned(),
        ),
        (
            VenueCommand::ArmCancelOnDisconnect(true),
            "deadman|rpc=11|ttl_ms=10000".to_owned(),
        ),
        (
            VenueCommand::ArmCancelOnDisconnect(false),
            "deadman|rpc=11|ttl_ms=0".to_owned(),
        ),
        (
            VenueCommand::RefreshDeadMan,
            "heartbeat|rpc=11|ttl_ms=10000".to_owned(),
        ),
        (
            VenueCommand::Query(QueryOrder {
                target: OrderRef::Both(cid(8), vid("V-8")),
                inst: INST_A,
                placement_nonce: Some(98),
            }),
            "query|rpc=11|vid=V-8|sym=TOYA-PERP".to_owned(),
        ),
        (
            // An order in Unknown, with no venue id yet, is queried by its placement nonce.
            VenueCommand::Query(QueryOrder {
                target: OrderRef::Client(cid(9)),
                inst: INST_B,
                placement_nonce: Some(99),
            }),
            "query|rpc=11|pnonce=99|sym=TOYB-PERP".to_owned(),
        ),
        (VenueCommand::FeeQuery, "fees|rpc=11".to_owned()),
    ];
    golden.into_iter().collect()
}

/// The charge the toy's account limit counts each sent command by.
fn charge_of(cmd: &VenueCommand) -> RateCharge {
    let weighted = |op, inst, n| RateCharge {
        op,
        inst,
        weight: std::num::NonZeroU32::new(n).unwrap(),
    };
    match cmd {
        VenueCommand::Place(o) => RateCharge::one(OpKind::Place, Some(o.inst)),
        // The batch spans both instruments, so it names neither.
        VenueCommand::PlaceBatch(_) => weighted(OpKind::Place, None, 2),
        VenueCommand::Amend(a) => RateCharge::one(OpKind::Amend, Some(a.inst)),
        VenueCommand::Cancel(c) => RateCharge::one(OpKind::Cancel, Some(c.inst)),
        // Both cancels name one instrument.
        VenueCommand::CancelMany(_) => weighted(OpKind::Cancel, Some(INST_A), 2),
        VenueCommand::CancelAll(_) => RateCharge::one(OpKind::CancelAll, Some(INST_B)),
        VenueCommand::ArmCancelOnDisconnect(_) | VenueCommand::RefreshDeadMan => {
            RateCharge::one(OpKind::Control, None)
        }
        VenueCommand::Query(q) => RateCharge::one(OpKind::Query, Some(q.inst)),
        VenueCommand::FeeQuery => RateCharge::one(OpKind::Query, None),
    }
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn every_command_kind_encodes_through_the_toy_with_a_fixed_encode_ctx() {
    let sent = sent();
    let kinds: std::collections::HashSet<_> = sent
        .iter()
        .map(|(cmd, _)| std::mem::discriminant(cmd))
        .collect();
    assert_eq!(kinds.len(), 10, "every VenueCommand kind is encoded");
    for (cmd, golden) in &sent {
        let encoded = encode(cmd);
        assert!(encoded.result.is_ok(), "{cmd:?}: {:?}", encoded.result);
        assert_eq!(encoded.text(), *golden, "{cmd:?}");
        // The same command under the same context encodes to the same bytes.
        assert_eq!(encode(cmd).fx, encoded.fx, "{cmd:?}");
    }
    // The only kind refused is the account-wide cancel-all, which the toy does not declare.
    let account = encode(&VenueCommand::CancelAll(CancelScope::Account));
    assert_eq!(account.result, Err(NotSentReason::Unsupported));
    assert!(account.fx.is_empty());
}

#[test]
fn the_signer_is_shown_only_the_reference_the_request_carries() {
    let cases = [
        // An amend of an acknowledged order names it by the venue's id only (its only declared
        // amend reference), though the command carries our id too.
        (
            VenueCommand::Amend(amend(OrderRef::Both(cid(4), vid("V-4")))),
            vec![Target::Venue("V-4".into())],
        ),
        // A cancel names the first declared reference it carries: the venue's id, else ours.
        (
            VenueCommand::Cancel(cancel(
                OrderRef::Both(cid(5), vid("V-5")),
                INST_A,
                Side::Buy,
            )),
            vec![Target::Venue("V-5".into())],
        ),
        (
            VenueCommand::Cancel(cancel(OrderRef::Client(cid(5)), INST_A, Side::Buy)),
            vec![Target::Client(wire_cid(cid(5)))],
        ),
        (
            VenueCommand::CancelMany(vec![
                cancel(OrderRef::Both(cid(6), vid("V-6")), INST_A, Side::Buy),
                cancel(OrderRef::Venue(vid("V-7")), INST_B, Side::Sell),
            ]),
            vec![Target::Venue("V-6".into()), Target::Venue("V-7".into())],
        ),
        // A batch cancel names an order it has no venue id for by its placement nonce, never by
        // our id.
        (
            VenueCommand::CancelMany(vec![CancelOrder {
                placement_nonce: Some(97),
                ..cancel(OrderRef::Client(cid(8)), INST_A, Side::Sell)
            }]),
            vec![Target::Nonce(97)],
        ),
    ];
    for (cmd, shown) in cases {
        let (mut codec, recorder) = recorded();
        let text = encode_on(&mut codec, &cmd, &ctx()).text();
        let seen: Vec<Target> = recorder
            .seen()
            .into_iter()
            .map(|seen| match seen {
                Seen::Amend { target, .. } | Seen::Cancel { target, .. } => target,
                Seen::Place { .. } => panic!("{cmd:?} signed a placement"),
            })
            .collect();
        assert_eq!(seen, shown, "{cmd:?}");
        // The wire names the same reference, and no other.
        let lines: Vec<&str> = text
            .lines()
            .filter(|l| !l.starts_with("cancels|"))
            .collect();
        for (line, target) in lines.iter().zip(&shown) {
            assert!(line.contains(&format!("|{}|", target.field())), "{line}");
            let named = ["|vid=", "|cid=", "|pnonce="];
            let count = named.iter().filter(|field| line.contains(*field)).count();
            assert_eq!(count, 1, "{line}");
        }
    }
    // A placement's signer is shown its client id and nonce as the wire carries them.
    let (mut codec, recorder) = recorded();
    let placed = VenueCommand::Place(order(1, INST_A, Side::Buy));
    let text = encode_on(&mut codec, &placed, &ctx()).text();
    let shown = Seen::Place {
        cid: wire_cid(cid(1)),
        nonce: Some(100),
    };
    assert_eq!(recorder.seen(), [shown]);
    assert!(text.contains(&format!("|cid={}|", wire_cid(cid(1)))));
}

#[test]
fn a_remaining_quantity_amend_sends_the_total_less_the_filled_quantity() {
    // Ten lots in total, four filled: the toy's amend quantity is what is left, six.
    let (mut codec, recorder) = recorded();
    let cmd = VenueCommand::Amend(amend(OrderRef::Venue(vid("V-4"))));
    let text = encode_on(&mut codec, &cmd, &ctx()).text();
    assert!(text.contains("|qty=6|"), "{text}");
    let target = Target::Venue("V-4".into());
    assert_eq!(recorder.seen(), [Seen::Amend { target, qty: 6 }]);
    // Nothing filled yet: the whole total.
    let fresh = AmendOrder {
        cum_filled: lots(0),
        ..amend(OrderRef::Venue(vid("V-4")))
    };
    assert!(
        encode(&VenueCommand::Amend(fresh))
            .text()
            .contains("|qty=10|")
    );
    // Amending to the filled quantity or below leaves nothing to rest: not an amend, not sent.
    for qty in [4, 3] {
        let (mut codec, recorder) = recorded();
        let spent = AmendOrder {
            qty: lots(qty),
            ..amend(OrderRef::Venue(vid("V-4")))
        };
        let encoded = encode_on(&mut codec, &VenueCommand::Amend(spent), &ctx());
        assert_eq!(encoded.result, Err(NotSentReason::Unencodable));
        assert!(encoded.fx.is_empty() && recorder.seen().is_empty());
    }
}

#[test]
fn an_undeclared_reference_tif_or_channel_is_refused_unsupported_with_no_effect() {
    let fok = NewOrder {
        tif: TifTag::Fok,
        ..order(1, INST_A, Side::Buy)
    };
    let rpi = NewOrder {
        channel: Channel::Rpi,
        ..order(2, INST_A, Side::Buy)
    };
    let market = NewOrder {
        kind: OrderKind::Market,
        ..order(3, INST_A, Side::Buy)
    };
    let acked = OrderRef::Venue(vid("V-4"));
    let refused = [
        // An amend whose only reference is our id: the toy amends by the venue's id only.
        VenueCommand::Amend(amend(OrderRef::Client(cid(4)))),
        // A batch cancel names orders by the venue's id or the placement nonce, so an item
        // carrying only our id refuses the whole batch (a single cancel by our id is sent,
        // below).
        VenueCommand::CancelMany(vec![
            cancel(OrderRef::Venue(vid("V-5")), INST_A, Side::Buy),
            cancel(OrderRef::Client(cid(6)), INST_A, Side::Buy),
        ]),
        // A time in force, channel or kind the toy does not declare, alone or in a batch.
        VenueCommand::Place(fok.clone()),
        VenueCommand::Place(rpi.clone()),
        VenueCommand::Place(market),
        VenueCommand::PlaceBatch(vec![order(7, INST_A, Side::Buy), fok]),
        VenueCommand::PlaceBatch(vec![rpi, order(8, INST_A, Side::Buy)]),
        VenueCommand::Amend(AmendOrder {
            tif: TifTag::Fok,
            ..amend(acked.clone())
        }),
        VenueCommand::Amend(AmendOrder {
            channel: Channel::Rpi,
            ..amend(acked)
        }),
        // A query of an order with neither a venue id nor a placement nonce.
        VenueCommand::Query(QueryOrder {
            target: OrderRef::Client(cid(9)),
            inst: INST_A,
            placement_nonce: None,
        }),
    ];
    for cmd in &refused {
        let (mut codec, recorder) = recorded();
        let encoded = encode_on(&mut codec, cmd, &ctx());
        assert_eq!(encoded.result, Err(NotSentReason::Unsupported), "{cmd:?}");
        assert!(encoded.fx.is_empty(), "{cmd:?} asked for {:?}", encoded.fx);
        assert!(recorder.seen().is_empty(), "{cmd:?} reached the signer");
        assert!(
            encoded.marks.is_empty(),
            "{cmd:?} marked {:?}",
            encoded.marks
        );
    }
    let single = VenueCommand::Cancel(cancel(OrderRef::Client(cid(6)), INST_A, Side::Buy));
    assert!(encode(&single).result.is_ok());
}

#[test]
fn a_batch_over_max_items_is_refused() {
    let max = usize::from(MAX_BATCH);
    let places = |n: usize| {
        let orders = (0..n).map(|i| order(i as u64 + 1, INST_A, Side::Buy));
        VenueCommand::PlaceBatch(orders.collect())
    };
    let cancels = |n: usize| {
        let wire = |i: usize| vid(&format!("V-{i}"));
        let items = (0..n).map(|i| cancel(OrderRef::Venue(wire(i)), INST_A, Side::Buy));
        VenueCommand::CancelMany(items.collect())
    };
    let batches: [fn(usize) -> VenueCommand; 2] = [places, cancels];
    for batch in batches {
        let full = encode(&batch(max));
        assert!(full.result.is_ok(), "{:?}", full.result);
        assert_eq!(full.text().lines().count(), max + 1);
        let (mut codec, recorder) = recorded();
        let over = encode_on(
            &mut codec,
            &batch(max + 1),
            &ctx_with(WALL, &[1, 2, 3, 4, 5]),
        );
        assert_eq!(over.result, Err(NotSentReason::Unsupported));
        assert!(over.fx.is_empty() && recorder.seen().is_empty() && over.marks.is_empty());
        // An empty batch cannot be written.
        let empty = encode(&batch(0));
        assert_eq!(empty.result, Err(NotSentReason::Unencodable));
        assert!(empty.fx.is_empty());
    }
}

#[test]
fn every_frame_carries_its_rpc_its_commands_traffic_class_and_its_rate_charge() {
    let limits = toy::caps().limits;
    let mut sent: Vec<VenueCommand> = sent().into_iter().map(|(cmd, _)| cmd).collect();
    // Exits are safety traffic, alone, in a batch of exits, or amended.
    let exits = vec![exit(1, INST_A), exit(2, INST_A)];
    let reducing = AmendOrder {
        reducing: true,
        ..amend(OrderRef::Venue(vid("V-1")))
    };
    sent.extend([
        VenueCommand::Place(exit(3, INST_B)),
        VenueCommand::PlaceBatch(exits),
        VenueCommand::Amend(reducing),
    ]);
    for (n, cmd) in (40..).zip(&sent) {
        let rpc = RpcId(n);
        let mut fx = Effects::new();
        let mut codec = ToyExec::new(Box::new(ToySigner));
        let off = &mut PathStamps::off();
        let encoded = codec.encode(cmd, rpc, &toy::specs(), &ctx(), off, &mut fx);
        assert!(encoded.is_ok(), "{cmd:?}");
        let class = cmd.traffic_class();
        assert!(fx.carry_request(rpc, class), "{cmd:?}: {fx:?}");
        let [
            Effect::Send {
                stream,
                frame,
                rpc: call,
                charge,
                ..
            },
        ] = fx.as_slice()
        else {
            panic!("{cmd:?} did not encode to one frame: {fx:?}");
        };
        assert_eq!(*stream, EXEC_STREAM);
        let deadline = RpcCall {
            id: rpc,
            timeout: RPC_TIMEOUT,
        };
        assert_eq!(*call, Some(deadline), "{cmd:?}");
        // The frame names its rpc on the wire too.
        let text = String::from_utf8(frame.bytes().to_vec()).unwrap();
        let first = text.lines().next().unwrap();
        assert!(first.contains(&format!("|rpc={n}")), "{first}");
        // Its charge is the one the toy states for the command, which a declared limit counts.
        let expected = match cmd {
            VenueCommand::Place(o) => RateCharge::one(OpKind::Place, Some(o.inst)),
            VenueCommand::PlaceBatch(orders) if orders.iter().all(|o| o.inst == INST_A) => {
                RateCharge {
                    weight: std::num::NonZeroU32::new(2).unwrap(),
                    ..RateCharge::one(OpKind::Place, Some(INST_A))
                }
            }
            other => charge_of(other),
        };
        assert_eq!(*charge, expected, "{cmd:?}");
        let counted = limits.iter().any(|l| l.counts(charge, Via::Frame));
        assert!(counted, "no declared limit counts {cmd:?}");
    }
    // The class is the command's: a mixed batch is normal traffic, a batch of exits safety
    // traffic, and turning cancel-on-disconnect off is normal.
    let class = |cmd: &VenueCommand| match encode(cmd).fx.as_slice() {
        [Effect::Send { class, .. }] => *class,
        other => panic!("{other:?}"),
    };
    let (safety, normal) = (TrafficClass::Safety, TrafficClass::Normal);
    let mixed = VenueCommand::PlaceBatch(vec![exit(1, INST_A), order(2, INST_A, Side::Buy)]);
    let exits = VenueCommand::PlaceBatch(vec![exit(1, INST_A), exit(2, INST_B)]);
    assert_eq!(class(&mixed), normal);
    assert_eq!(class(&exits), safety);
    assert_eq!(class(&VenueCommand::ArmCancelOnDisconnect(false)), normal);
    assert_eq!(class(&VenueCommand::ArmCancelOnDisconnect(true)), safety);
    assert_eq!(class(&VenueCommand::FeeQuery), normal);
}

// ---------------------------------------------------------------------------------------------
// Signing stages, nonces, and what is not sent.
// ---------------------------------------------------------------------------------------------

const SIGN: [PathMark; 2] = [
    PathMark {
        stage: PathStage::Sign,
        edge: PathEdge::Start,
    },
    PathMark {
        stage: PathStage::Sign,
        edge: PathEdge::End,
    },
];

#[test]
fn encode_marks_each_signer_call_as_a_sign_stage_and_its_bytes_do_not_depend_on_the_marks() {
    for (cmd, _) in sent() {
        let encoded = encode(&cmd);
        let signed = match &cmd {
            VenueCommand::Place(_) | VenueCommand::Amend(_) | VenueCommand::Cancel(_) => 1,
            VenueCommand::PlaceBatch(items) => items.len(),
            VenueCommand::CancelMany(items) => items.len(),
            _ => 0,
        };
        assert_eq!(encoded.marks, SIGN.repeat(signed), "{cmd:?}");
        let mut fx = Effects::new();
        let mut codec = ToyExec::new(Box::new(ToySigner));
        let off = &mut PathStamps::off();
        codec
            .encode(&cmd, RPC, &toy::specs(), &ctx(), off, &mut fx)
            .unwrap();
        assert_eq!(fx.take(), encoded.fx, "{cmd:?}");
    }
}

#[test]
fn nonces_and_time_come_only_from_the_encode_ctx() {
    let place = VenueCommand::Place(order(1, INST_A, Side::Buy));
    let batch = VenueCommand::PlaceBatch(vec![order(2, INST_A, Side::Buy), exit(3, INST_B)]);
    // A placement keeps the nonce of each item in the receipt, as the order's placement nonce.
    let receipt = |cmd| encode(cmd).result.unwrap();
    assert_eq!(receipt(&place).nonces(), [(0, 100)]);
    assert_eq!(receipt(&batch).nonces(), [(0, 100), (1, 101)]);
    // An amend's or a cancel's nonce only signs it.
    let amended = VenueCommand::Amend(amend(OrderRef::Venue(vid("V-1"))));
    let cancelled = VenueCommand::Cancel(cancel_of("V-2"));
    assert!(receipt(&amended).nonces().is_empty());
    assert!(receipt(&cancelled).nonces().is_empty());
    let cancels = VenueCommand::CancelMany(vec![cancel_of("V-3"), cancel_of("V-4")]);
    // Another time and other nonces change the bytes, through the signature too.
    for cmd in [&place, &batch, &amended, &cancelled, &cancels] {
        let codec = &mut ToyExec::new(Box::new(ToySigner));
        let base = encode_on(codec, cmd, &ctx()).text();
        let later = encode_on(codec, cmd, &ctx_with(WALL + 1, &[100, 101])).text();
        let renonced = encode_on(codec, cmd, &ctx_with(WALL, &[200, 201])).text();
        assert!(later.contains(&format!("|ts={}|", WALL + 1)), "{later}");
        assert!(renonced.contains("|nonce=200|"), "{renonced}");
        let sig = |text: &str| text.rsplit("|sig=").next().unwrap().to_owned();
        assert_ne!(sig(&base), sig(&later), "{cmd:?}");
        assert_ne!(sig(&base), sig(&renonced), "{cmd:?}");
        // Without a nonce for every item nothing is sent.
        let short = encode_on(codec, cmd, &ctx_with(WALL, &[100]));
        let one_item = !matches!(
            cmd,
            VenueCommand::PlaceBatch(_) | VenueCommand::CancelMany(_)
        );
        if one_item {
            assert!(short.result.is_ok(), "{cmd:?}");
            let none = encode_on(codec, cmd, &ctx_with(WALL, &[]));
            assert_eq!(none.result, Err(NotSentReason::Unencodable), "{cmd:?}");
            assert!(none.fx.is_empty());
        } else {
            assert_eq!(short.result, Err(NotSentReason::Unencodable), "{cmd:?}");
            assert!(short.fx.is_empty());
        }
    }
}

fn cancel_of(wire: &str) -> CancelOrder {
    cancel(OrderRef::Venue(vid(wire)), INST_A, Side::Buy)
}

/// A signer that fails, or signs no cancel.
struct Failing {
    cancel_unsigned: bool,
}

impl OrderSigner for Failing {
    fn sign_place(&mut self, _w: &PlaceWire<'_>) -> Result<Sig, SignError> {
        Err(SignError::Backend("synthetic failure"))
    }

    fn sign_amend(&mut self, _w: &AmendWire<'_>) -> Result<Sig, SignError> {
        Err(SignError::Unsignable("synthetic failure"))
    }

    fn sign_cancel(&mut self, _w: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        if self.cancel_unsigned {
            Ok(None)
        } else {
            Err(SignError::Backend("synthetic failure"))
        }
    }
}

#[test]
fn what_cannot_be_written_or_signed_is_not_sent() {
    // An instrument missing from the spec table.
    let unknown = InstrumentId::new(99);
    let unencodable = [
        VenueCommand::Place(order(1, unknown, Side::Buy)),
        VenueCommand::PlaceBatch(vec![
            order(2, INST_A, Side::Buy),
            order(3, unknown, Side::Buy),
        ]),
        VenueCommand::Amend(AmendOrder {
            inst: unknown,
            ..amend(OrderRef::Venue(vid("V-1")))
        }),
        VenueCommand::Cancel(cancel(OrderRef::Venue(vid("V-2")), unknown, Side::Buy)),
        VenueCommand::CancelMany(vec![cancel(
            OrderRef::Venue(vid("V-3")),
            unknown,
            Side::Buy,
        )]),
        VenueCommand::CancelAll(CancelScope::Instrument(unknown)),
        VenueCommand::Query(QueryOrder {
            target: OrderRef::Venue(vid("V-4")),
            inst: unknown,
            placement_nonce: None,
        }),
    ];
    for cmd in &unencodable {
        let encoded = encode(cmd);
        assert_eq!(encoded.result, Err(NotSentReason::Unencodable), "{cmd:?}");
        assert!(encoded.fx.is_empty(), "{cmd:?}");
    }
    // A signer that fails, or signs no cancel where the toy's cancels are signed.
    let signed = [
        VenueCommand::Place(order(1, INST_A, Side::Buy)),
        VenueCommand::PlaceBatch(vec![order(2, INST_A, Side::Buy)]),
        VenueCommand::Amend(amend(OrderRef::Venue(vid("V-1")))),
        VenueCommand::Cancel(cancel_of("V-2")),
        VenueCommand::CancelMany(vec![cancel_of("V-3")]),
    ];
    for cancel_unsigned in [false, true] {
        for cmd in &signed {
            let codec = &mut ToyExec::new(Box::new(Failing { cancel_unsigned }));
            let encoded = encode_on(codec, cmd, &ctx());
            assert_eq!(encoded.result, Err(NotSentReason::SignFailed), "{cmd:?}");
            assert!(encoded.fx.is_empty(), "{cmd:?}");
            assert_eq!(encoded.marks, SIGN, "the failed call is still marked");
        }
    }
}

/// A sink that keeps what a codec pushed.
#[derive(Default)]
struct Collect(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

#[test]
fn outside_encode_the_toy_signs_nothing_decodes_nothing_yet_and_times_a_request_out_as_unknown() {
    let mut codec = ToyExec::new(Box::new(ToySigner));
    for call in [
        CtxCall::Open(EXEC_STREAM),
        CtxCall::Timer(TimerTag(1)),
        CtxCall::Resync,
    ] {
        assert_eq!(codec.nonces_for(call), 0);
    }
    let (mut fx, mut sink) = (Effects::new(), Collect::default());
    codec.on_open(EXEC_STREAM, &ctx(), &mut fx);
    codec.on_timer(TimerTag(1), &ctx(), &mut fx);
    codec.resync(&ctx(), &mut fx);
    assert!(fx.is_empty());
    let frame = RawFrame::Text("ack|rpc=11");
    let specs = toy::specs();
    toy::with_scope(|scope| {
        let decoded = codec.on_frame(StreamId(1), frame, scope, &specs, &mut sink, &mut fx);
        assert!(matches!(decoded, Err(DecodeError::Malformed(_))));
        let failure = Err(HttpFailure::Lost);
        let answered = codec.on_http(HttpTag(1), failure, scope, &specs, &mut sink, &mut fx);
        assert!(matches!(answered, Err(DecodeError::Malformed(_))));
    });
    assert!(sink.0.is_empty() && fx.is_empty());
    assert_eq!(
        codec.redact_inbound(Inbound::Frame(frame)),
        InboundSpans::NONE
    );
    codec.on_rpc_timeout(RPC, &mut sink);
    let unknown = ExecEvent::Outcome {
        rpc: RPC,
        item: None,
        outcome: SubmitOutcome::Unknown,
    };
    assert_eq!(sink.0, [(VenueMeta::NONE, unknown)]);
}
