//! FBC-sal's done line: the conformance toy answers order queries by venue id and by placement
//! nonce with a `QueryResult` carrying the query's rpc (r4172456271, r4172835741); a batch whose
//! items are answered in two frames is pushed in one call, and `on_rpc_timeout` reports the held
//! outcomes and `Unknown` only for the items still unanswered (record 0014 item 3); a resync
//! answered in frames is decoded into begin, orders, positions and end and pushed whole at its
//! end, with nothing pushed from one cut short (0014 item 2); and `Authenticated` is reported
//! only on the authentication acknowledgement, whose toy token `redact_inbound` names (0028).
//! Every refusal pushes nothing.

use std::sync::OnceLock;

use fbc_conformance::toy::{
    self, EXEC_STREAM, INST_A, INST_B, OWN_NS, TOY_TOKEN, ToyExec, ToySigner,
};
use fbc_core::{
    AccountKey, AckLevel, CancelOrder, Channel, CidMatch, CidMint, ClientOrderId, ConnState,
    DecodeError, Effect, Effects, EncodeCtx, ExchNs, ExchTsKind, ExecCodec, ExecEvent, ExecSink,
    HttpResponse, HttpTag, IdError, Inbound, InboundSpans, ItemRef, Lots, MonoNs, NamespaceLease,
    NewOrder, NonceBlock, OpKind, OrderKind, OrderRef, PathStamps, PxExact, QueryOrder, RateCharge,
    RawFrame, Reject, RejectKind, RpcCall, RpcId, Side, SignedLots, SubmitOutcome, Ticks, TifTag,
    TrafficClass, VenueCommand, VenueMeta, VenueOrderId, VenueOrderSnapshot, VenueOrderState,
    WallNs, encode_cid,
};

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const WALL: i64 = 1_759_363_200_000_000_000;

fn ctx_at(wall: i64) -> EncodeCtx {
    let nonces = NonceBlock::new(vec![100, 101, 102, 103]);
    EncodeCtx {
        wall: WallNs(wall),
        mono: MonoNs(77),
        nonces,
    }
}

fn ctx() -> EncodeCtx {
    ctx_at(WALL)
}

/// Our `n`th client id, minted once per test binary under a lease in a fresh directory.
fn cid(n: usize) -> ClientOrderId {
    static CIDS: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let cids = CIDS.get_or_init(|| {
        let name = format!("fbc-conformance-toy-answers-{}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), OWN_NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(WALL));
        let cids = (0..8).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        cids
    });
    cids[n]
}

/// `cid` as the toy's wire spells it.
fn wire_cid(cid: ClientOrderId) -> String {
    let caps = toy::caps().exec.unwrap().order;
    encode_cid(&caps.client_id, cid).unwrap().to_string()
}

fn vid(wire: &str) -> VenueOrderId {
    toy::with_scope(|scope| scope.venue_order_id(wire)).unwrap()
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn order(n: usize, inst: fbc_core::InstrumentId) -> NewOrder {
    NewOrder {
        cid: cid(n),
        inst,
        side: Side::Buy,
        qty: lots(25),
        kind: OrderKind::Limit { px: Ticks(130_865) },
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// A sink that keeps what it is given, and counts the calls that gave it something.
#[derive(Default)]
struct Collect(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// One codec over the toy's signer, fed frames and commands in turn.
struct Session {
    codec: ToyExec,
}

impl Session {
    fn new() -> Session {
        Session {
            codec: ToyExec::new(Box::new(ToySigner)),
        }
    }

    /// Encodes `cmd` as request `rpc`, which must be sent.
    fn send(&mut self, cmd: VenueCommand, rpc: u64) {
        let mut fx = Effects::new();
        let mut t = PathStamps::off();
        let sent = self
            .codec
            .encode(&cmd, RpcId(rpc), &toy::specs(), &ctx(), &mut t, &mut fx);
        assert!(sent.is_ok(), "{cmd:?} not sent: {sent:?}");
        assert!(fx.carry_request(RpcId(rpc), cmd.traffic_class()));
    }

    /// Decodes `text`; its result and what this one call pushed.
    fn feed(&mut self, text: &str) -> (Result<(), DecodeError>, Vec<(VenueMeta, ExecEvent)>) {
        let (mut fx, mut sink, specs) = (Effects::new(), Collect::default(), toy::specs());
        let frame = RawFrame::Text(text);
        let result = toy::with_scope(|scope| {
            self.codec
                .on_frame(EXEC_STREAM, frame, scope, &specs, &mut sink, &mut fx)
        });
        assert!(fx.is_empty(), "a decode asks for no effect");
        (result, sink.0)
    }

    /// `text` decodes, and this call pushes `want`.
    fn pushes(&mut self, text: &str, want: Vec<(VenueMeta, ExecEvent)>) {
        let (result, pushed) = self.feed(text);
        assert_eq!(result, Ok(()), "{text}");
        assert_eq!(pushed, want, "{text}");
    }

    /// `text` decodes and this call pushes nothing.
    fn holds(&mut self, text: &str) {
        self.pushes(text, Vec::new());
    }

    /// `text` is refused as `Malformed(what)` and nothing is pushed.
    fn refuses(&mut self, text: &str, what: &'static str) {
        let (result, pushed) = self.feed(text);
        assert_eq!(result, Err(DecodeError::Malformed(what)), "{text}");
        assert!(pushed.is_empty(), "{text} pushed {pushed:?}");
    }

    fn timeout(&mut self, rpc: u64) -> Vec<(VenueMeta, ExecEvent)> {
        let mut sink = Collect::default();
        self.codec.on_rpc_timeout(RpcId(rpc), &mut sink);
        sink.0
    }

    fn open(&mut self) -> Vec<Effect> {
        let mut fx = Effects::new();
        self.codec.on_open(EXEC_STREAM, &ctx(), &mut fx);
        fx.take()
    }

    fn resync(&mut self, wall: i64) -> Vec<Effect> {
        let mut fx = Effects::new();
        self.codec.resync(&ctx_at(wall), &mut fx);
        fx.take()
    }
}

fn outcome(rpc: u64, item: Option<ItemRef>, outcome: SubmitOutcome) -> ExecEvent {
    ExecEvent::Outcome {
        rpc: RpcId(rpc),
        item,
        outcome,
    }
}

fn item(idx: u16, cid: Option<ClientOrderId>, vid: Option<VenueOrderId>) -> Option<ItemRef> {
    Some(ItemRef { idx, cid, vid })
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

fn rejected(code: &str, kind: RejectKind, raw: &str) -> SubmitOutcome {
    SubmitOutcome::Rejected(Reject {
        kind,
        venue_code: Some(code.into()),
        raw: raw.into(),
    })
}

fn seq(ts: i64, seq: u64) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(ts)),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: Some(seq),
    }
}

fn none(ev: ExecEvent) -> (VenueMeta, ExecEvent) {
    (VenueMeta::NONE, ev)
}

/// Our open buy `n` on TOYA-PERP as the toy's wire reports it in a query answer or a resync.
fn snapshot_fields(n: usize, wire_vid: &str) -> String {
    format!(
        "cid={}|vid={wire_vid}|sym=TOYA-PERP|side=B|st=open|px=130865|qty=25|cum=4|po=1|ro=0",
        wire_cid(cid(n))
    )
}

fn snapshot(n: usize, wire_vid: &str) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(cid(n))),
        vid: vid(wire_vid),
        inst: INST_A,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(130_865)),
        qty: lots(25),
        cum_filled: lots(4),
        post_only: Some(true),
        reduce_only: Some(false),
    }
}

fn query(target: OrderRef, placement_nonce: Option<u64>) -> VenueCommand {
    VenueCommand::Query(QueryOrder {
        target,
        inst: INST_A,
        placement_nonce,
    })
}

fn the_answer(pushed: &[(VenueMeta, ExecEvent)]) -> &fbc_core::QueryAnswer {
    match pushed {
        [(_, ExecEvent::QueryResult(answer))] => answer,
        other => panic!("expected one query answer, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------------------------
// Queries.
// ---------------------------------------------------------------------------------------------

#[test]
fn queries_by_venue_id_and_by_placement_nonce_are_answered_carrying_the_querys_rpc() {
    let mut s = Session::new();
    // By venue id, found.
    let by_vid = OrderRef::Venue(vid("V-1"));
    s.send(query(by_vid.clone(), None), 21);
    let found = format!(
        "qres|rpc=21|found=1|{}|ts=5|seq=9",
        snapshot_fields(0, "V-1")
    );
    let (result, pushed) = s.feed(&found);
    assert_eq!(result, Ok(()));
    let answer = the_answer(&pushed);
    assert_eq!(pushed[0].0, seq(5, 9));
    assert_eq!(answer.rpc(), RpcId(21));
    assert_eq!(pushed[0].1.answers(), Some(RpcId(21)));
    assert_eq!(answer.target(), &by_vid);
    assert_eq!(answer.found(), Some(&snapshot(0, "V-1")));

    // By placement nonce, for an order in Unknown with no venue id: found, then (another query)
    // absent.
    let by_nonce = OrderRef::Client(cid(1));
    s.send(query(by_nonce.clone(), Some(100)), 22);
    s.send(query(OrderRef::Client(cid(2)), Some(101)), 23);
    let (result, pushed) = s.feed(&format!(
        "qres|rpc=22|found=1|{}",
        snapshot_fields(1, "V-2")
    ));
    assert_eq!(result, Ok(()));
    let answer = the_answer(&pushed);
    assert_eq!((answer.rpc(), answer.target()), (RpcId(22), &by_nonce));
    assert_eq!(answer.found(), Some(&snapshot(1, "V-2")));
    assert_eq!(pushed[0].0, VenueMeta::NONE);
    let (result, pushed) = s.feed("qres|rpc=23|found=0");
    assert_eq!(result, Ok(()));
    let answer = the_answer(&pushed);
    assert_eq!(answer.rpc(), RpcId(23));
    assert_eq!(answer.target(), &OrderRef::Client(cid(2)));
    assert_eq!(answer.found(), None);
    // Each query is answered once: an answer to one already answered names no query.
    s.refuses("qres|rpc=23|found=0", "rpc");
}

#[test]
fn a_query_answer_naming_another_order_or_no_query_is_refused_and_the_query_still_waits() {
    let mut s = Session::new();
    s.send(query(OrderRef::Venue(vid("V-1")), None), 21);
    s.send(query(OrderRef::Client(cid(1)), Some(100)), 22);
    // Another order's snapshot never answers a query (QueryAnswer::new, 0014 item 6).
    let other_vid = format!("qres|rpc=21|found=1|{}", snapshot_fields(0, "V-9"));
    s.refuses(&other_vid, "another order");
    let other_cid = format!("qres|rpc=22|found=1|{}", snapshot_fields(3, "V-2"));
    s.refuses(&other_cid, "another order");
    // A frame that names no query, or a request that is not one.
    s.refuses("qres|rpc=99|found=0", "rpc");
    s.send(VenueCommand::Place(order(4, INST_A)), 30);
    s.refuses("qres|rpc=30|found=0", "rpc");
    s.refuses("qres|found=0", "rpc");
    s.refuses("qres|rpc=21|found=2", "found");
    s.refuses("qres|rpc=21", "found");
    // A found order must carry every field the caps promise, its total covering its filled part.
    for (field, what) in [
        ("cid", "cid"),
        ("vid", "vid"),
        ("sym", "sym"),
        ("side", "side"),
        ("st", "st"),
        ("qty", "qty"),
        ("cum", "cum"),
        ("po", "po"),
        ("ro", "ro"),
    ] {
        let fields = snapshot_fields(0, "V-1");
        let kept = fields
            .split('|')
            .filter(|f| !f.starts_with(&format!("{field}=")));
        let text = format!("qres|rpc=21|found=1|{}", kept.collect::<Vec<_>>().join("|"));
        s.refuses(&text, what);
    }
    let overfilled = snapshot_fields(0, "V-1").replace("cum=4", "cum=26");
    s.refuses(&format!("qres|rpc=21|found=1|{overfilled}"), "cum");
    let negative = snapshot_fields(0, "V-1").replace("qty=25", "qty=-1");
    s.refuses(&format!("qres|rpc=21|found=1|{negative}"), "qty");
    let bad_px = snapshot_fields(0, "V-1").replace("px=130865", "px=x");
    s.refuses(&format!("qres|rpc=21|found=1|{bad_px}"), "px");
    let unknown = snapshot_fields(0, "V-1").replace("TOYA-PERP", "NOPE-PERP");
    let (result, pushed) = s.feed(&format!("qres|rpc=21|found=1|{unknown}"));
    assert_eq!(result, Err(DecodeError::UnknownInstrument));
    assert!(pushed.is_empty());
    // Both queries still wait for their answers, and a market order's snapshot has no price.
    let no_px = snapshot_fields(0, "V-1").replace("|px=130865", "");
    let (result, pushed) = s.feed(&format!("qres|rpc=21|found=1|{no_px}"));
    assert_eq!(result, Ok(()));
    let want = VenueOrderSnapshot {
        px: None,
        ..snapshot(0, "V-1")
    };
    assert_eq!(the_answer(&pushed).found(), Some(&want));
    let (result, pushed) = s.feed("qres|rpc=22|found=0");
    assert_eq!(result, Ok(()));
    assert_eq!(the_answer(&pushed).rpc(), RpcId(22));
}

#[test]
fn a_query_timed_out_is_unknown_as_a_whole_and_its_late_answer_names_no_query() {
    let mut s = Session::new();
    s.send(query(OrderRef::Venue(vid("V-1")), None), 21);
    assert_eq!(
        s.timeout(21),
        [none(outcome(21, None, SubmitOutcome::Unknown))]
    );
    s.refuses("qres|rpc=21|found=0", "rpc");
}

// ---------------------------------------------------------------------------------------------
// Request outcomes answered item by item.
// ---------------------------------------------------------------------------------------------

#[test]
fn a_batch_answered_in_two_frames_is_pushed_in_one_call() {
    let mut s = Session::new();
    s.send(
        VenueCommand::PlaceBatch(vec![order(0, INST_A), order(1, INST_B)]),
        31,
    );
    // The first item's answer is held: pushing it would clear the whole batch's deadline.
    s.holds("item|rpc=31|i=1|res=rej|code=1001|msg=would cross|ts=7|seq=3");
    // The second completes the batch: both outcomes, in item order, in this one call.
    s.pushes(
        "item|rpc=31|i=0|res=ok|vid=V-7",
        vec![
            none(outcome(
                31,
                item(0, Some(cid(0)), Some(vid("V-7"))),
                accepted(),
            )),
            (
                seq(7, 3),
                outcome(
                    31,
                    item(1, Some(cid(1)), None),
                    rejected("1001", RejectKind::PostOnlyWouldCross, "would cross"),
                ),
            ),
        ],
    );
    // Answered, the batch is gone: a late or repeated item names no request.
    s.refuses("item|rpc=31|i=0|res=ok|vid=V-7", "rpc");

    // A batch cancel's items name the orders by our id where the command carries one.
    let cancel = |target: OrderRef| CancelOrder {
        target,
        inst: INST_A,
        side: Side::Buy,
        placement_nonce: Some(100),
    };
    s.send(
        VenueCommand::CancelMany(vec![
            cancel(OrderRef::Both(cid(2), vid("V-2"))),
            cancel(OrderRef::Venue(vid("V-3"))),
        ]),
        32,
    );
    s.holds("item|rpc=32|i=0|res=ok");
    s.pushes(
        "item|rpc=32|i=1|res=rej|code=2001",
        vec![
            none(outcome(32, item(0, Some(cid(2)), None), accepted())),
            none(outcome(
                32,
                item(1, None, None),
                rejected("2001", RejectKind::NotFound, ""),
            )),
        ],
    );
}

#[test]
fn a_single_request_is_answered_by_its_one_item_at_once() {
    let mut s = Session::new();
    s.send(VenueCommand::Place(order(0, INST_A)), 41);
    s.pushes(
        "item|rpc=41|i=0|res=ok|vid=V-1",
        vec![none(outcome(
            41,
            item(0, Some(cid(0)), Some(vid("V-1"))),
            accepted(),
        ))],
    );
    s.send(VenueCommand::ArmCancelOnDisconnect(true), 42);
    s.pushes(
        "item|rpc=42|i=0|res=ok",
        vec![none(outcome(42, item(0, None, None), accepted()))],
    );
}

#[test]
fn on_rpc_timeout_reports_unknown_only_for_the_items_still_unanswered() {
    let mut s = Session::new();
    s.send(
        VenueCommand::PlaceBatch(vec![order(0, INST_A), order(1, INST_A), order(2, INST_B)]),
        51,
    );
    s.holds("item|rpc=51|i=1|res=ok|vid=V-8|ts=4|seq=2");
    // The deadline passes: the acknowledged item keeps its venue id, and only the two others
    // are Unknown, by index.
    assert_eq!(
        s.timeout(51),
        [
            none(outcome(
                51,
                item(0, Some(cid(0)), None),
                SubmitOutcome::Unknown
            )),
            (
                seq(4, 2),
                outcome(51, item(1, Some(cid(1)), Some(vid("V-8"))), accepted()),
            ),
            none(outcome(
                51,
                item(2, Some(cid(2)), None),
                SubmitOutcome::Unknown
            )),
        ]
    );
    // An item answered after the deadline names no request.
    s.refuses("item|rpc=51|i=0|res=ok|vid=V-9", "rpc");

    // A request with no item answered is Unknown as a whole, once.
    s.send(
        VenueCommand::PlaceBatch(vec![order(3, INST_A), order(4, INST_A)]),
        52,
    );
    let whole = [none(outcome(52, None, SubmitOutcome::Unknown))];
    assert_eq!(s.timeout(52), whole);
    assert_eq!(s.timeout(52), whole);
}

#[test]
fn an_item_answer_that_cannot_be_held_is_refused_and_the_batch_still_waits() {
    let mut s = Session::new();
    s.send(
        VenueCommand::PlaceBatch(vec![order(0, INST_A), order(1, INST_A)]),
        61,
    );
    s.refuses("item|rpc=99|i=0|res=ok", "rpc");
    s.refuses("item|i=0|res=ok", "rpc");
    s.refuses("item|rpc=61|i=2|res=ok", "i");
    s.refuses("item|rpc=61|res=ok", "i");
    s.refuses("item|rpc=61|i=0|res=maybe", "res");
    s.refuses("item|rpc=61|i=0", "res");
    s.refuses("item|rpc=61|i=0|res=rej", "code");
    let (result, pushed) = s.feed("item|rpc=61|i=0|res=ok|vid=");
    assert_eq!(result, Err(DecodeError::IdRefused(IdError::Empty)));
    assert!(pushed.is_empty());
    s.refuses("item|rpc=61|i=0|res=ok|ts=x", "ts");
    // A query is answered by its result, not an item.
    s.send(query(OrderRef::Venue(vid("V-1")), None), 62);
    s.refuses("item|rpc=62|i=0|res=ok", "rpc");
    // An item is answered once.
    s.holds("item|rpc=61|i=0|res=ok|vid=V-1");
    s.refuses("item|rpc=61|i=0|res=ok|vid=V-1", "i");
    s.pushes(
        "item|rpc=61|i=1|res=ok|vid=V-2",
        vec![
            none(outcome(
                61,
                item(0, Some(cid(0)), Some(vid("V-1"))),
                accepted(),
            )),
            none(outcome(
                61,
                item(1, Some(cid(1)), Some(vid("V-2"))),
                accepted(),
            )),
        ],
    );
}

#[test]
fn a_refusal_of_the_whole_request_answers_every_item_and_ends_it() {
    let mut s = Session::new();
    s.send(
        VenueCommand::PlaceBatch(vec![order(0, INST_A), order(1, INST_A)]),
        71,
    );
    s.holds("item|rpc=71|i=0|res=ok|vid=V-1");
    s.pushes(
        "reject|rpc=71|code=1006",
        vec![none(outcome(
            71,
            None,
            rejected("1006", RejectKind::RateLimited { retry_after: None }, ""),
        ))],
    );
    s.refuses("item|rpc=71|i=1|res=ok|vid=V-2", "rpc");
}

// ---------------------------------------------------------------------------------------------
// Resync.
// ---------------------------------------------------------------------------------------------

const WM: i64 = WALL + 5_000;

fn resync_frames() -> Vec<String> {
    vec![
        format!("rsbegin|wm={WM}"),
        format!("rsorder|{}", snapshot_fields(0, "V-1")),
        format!("rsorder|{}", snapshot_fields(1, "V-2")),
        "rspos|sym=TOYA-PERP|qty=-12|avg=65432.5".into(),
        "rspos|sym=TOYB-PERP|qty=0".into(),
        "rsend".into(),
    ]
}

#[test]
fn a_resync_asks_once_and_is_decoded_into_begin_orders_positions_and_end_pushed_at_its_end() {
    let mut s = Session::new();
    let asked = s.resync(WM);
    let charge = RateCharge::one(OpKind::Query, None);
    assert_eq!(
        asked,
        [Effect::Send {
            stream: EXEC_STREAM,
            frame: fbc_core::WireSlice::plain(format!("resync|ts={WM}").into_bytes()),
            rpc: None,
            class: TrafficClass::Safety,
            charge,
        }]
    );
    let frames = resync_frames();
    let (last, body) = frames.split_last().unwrap();
    for frame in body {
        s.holds(frame);
    }
    s.pushes(
        last,
        vec![
            none(ExecEvent::ResyncBegin {
                watermark: WallNs(WM),
            }),
            none(ExecEvent::ResyncOrder(snapshot(0, "V-1"))),
            none(ExecEvent::ResyncOrder(snapshot(1, "V-2"))),
            none(ExecEvent::ResyncPosition {
                inst: INST_A,
                qty: SignedLots(-12),
                avg_entry: Some(PxExact::new(654_325, -1)),
            }),
            none(ExecEvent::ResyncPosition {
                inst: INST_B,
                qty: SignedLots(0),
                avg_entry: None,
            }),
            none(ExecEvent::ResyncEnd),
        ],
    );
    // Delivered, the resync is over: its frames again are refused.
    s.refuses(&frames[0], "no resync asked for");
    s.refuses("rsend", "no resync begun");
}

#[test]
fn a_resync_cut_short_pushes_nothing() {
    let mut s = Session::new();
    let frames = resync_frames();
    // Cut short by the connection: a reopened stream drops what was held.
    s.resync(WM);
    s.holds(&frames[0]);
    s.holds(&frames[1]);
    s.open();
    s.refuses("rsend", "no resync begun");
    s.refuses(&frames[0], "no resync asked for");

    // Cut short by a frame that cannot be read: the whole resync is dropped, not the frame.
    s.resync(WM);
    s.holds(&frames[0]);
    s.holds(&frames[1]);
    s.refuses("rsorder|vid=V-3", "qty");
    s.refuses(&frames[2], "no resync begun");
    s.refuses("rsend", "no resync begun");

    // Cut short by a new resync: the older one's frames are held no longer, and the new one
    // waits for a begin that echoes its own instant.
    s.resync(WM);
    s.holds(&frames[0]);
    s.holds(&frames[1]);
    s.resync(WM + 1);
    s.refuses("rsend", "no resync begun");
    s.resync(WM + 2);
    s.refuses(&frames[0], "resync for another request");
    // That refusal ended the resync asked for too.
    s.refuses(&format!("rsbegin|wm={}", WM + 2), "no resync asked for");

    // Every other way a resync frame can be wrong drops it, nothing pushed.
    for (bad, what) in [
        ("rsbegin", "wm"),
        ("rsbegin|wm=x", "wm"),
        ("rspos|sym=TOYA-PERP|qty=x", "qty"),
        ("rspos|sym=TOYA-PERP|qty=1|avg=1e3", "avg"),
        ("rspos|qty=1", "sym"),
        ("rsorder|vid=V-3", "qty"),
    ] {
        s.resync(WM);
        if bad.starts_with("rsbegin") {
            s.refuses(bad, what);
        } else {
            s.holds(&frames[0]);
            s.refuses(bad, what);
        }
        s.refuses("rsend", "no resync begun");
    }
    s.resync(WM);
    s.holds(&frames[0]);
    s.refuses(&frames[0], "resync begun twice");
    s.refuses("rsend", "no resync begun");
    // An order or position outside a begun resync.
    s.resync(WM);
    s.refuses(&frames[1], "no resync begun");
    s.resync(WM);
    s.refuses(&frames[3], "no resync begun");
}

// ---------------------------------------------------------------------------------------------
// Authentication.
// ---------------------------------------------------------------------------------------------

/// The one span of a toy token starting at byte `at`.
fn token_at(at: u32) -> Vec<std::ops::Range<u32>> {
    let span = at..at + TOY_TOKEN.len() as u32;
    vec![span]
}

fn spans_of(codec: &ToyExec, text: &str) -> InboundSpans {
    codec.redact_inbound(Inbound::Frame(RawFrame::Text(text)))
}

#[test]
fn on_open_authenticates_with_the_toy_token_in_a_redaction_span() {
    let mut s = Session::new();
    let sent = s.open();
    let [
        Effect::Send {
            stream,
            frame,
            rpc,
            class,
            charge,
        },
    ] = sent.as_slice()
    else {
        panic!("expected one frame, got {sent:?}");
    };
    let head = format!("auth|ts={WALL}|token=");
    let text = format!("{head}{TOY_TOKEN}");
    assert_eq!(frame.bytes(), text.as_bytes());
    let span = head.len() as u32..text.len() as u32;
    assert_eq!(frame.redactions(), [span]);
    assert_eq!(*stream, EXEC_STREAM);
    assert_eq!(*rpc, None::<RpcCall>);
    assert_eq!(*class, TrafficClass::Safety);
    assert_eq!(*charge, RateCharge::one(OpKind::Control, None));
    // Its Debug shows no token.
    assert!(!format!("{frame:?}").contains(TOY_TOKEN));
}

#[test]
fn authenticated_is_reported_only_on_the_authentication_acknowledgement() {
    let mut s = Session::new();
    let ack = format!("auth|ok=1|token={TOY_TOKEN}");
    // Before the stream asked, an acknowledgement is not believed.
    s.refuses(&ack, "no authentication asked");
    s.open();
    // Nothing else the venue sends reports the stream authenticated.
    s.send(VenueCommand::Place(order(0, INST_A)), 81);
    let (_, pushed) = s.feed("item|rpc=81|i=0|res=ok|vid=V-1");
    assert!(
        !pushed
            .iter()
            .any(|(_, ev)| matches!(ev, ExecEvent::Conn { .. }))
    );
    s.refuses("auth|ok=1", "token");
    s.refuses("auth|ok=1|token=", "token");
    s.refuses(&format!("auth|ok=1|token={TOY_TOKEN}|ts=x"), "ts");
    s.refuses("auth|ok=2", "ok");
    s.refuses("auth", "ok");
    let authenticated = ExecEvent::Conn {
        stream: EXEC_STREAM,
        state: ConnState::Authenticated,
    };
    s.pushes(&ack, vec![none(authenticated.clone())]);
    // Once.
    s.refuses(&ack, "no authentication asked");

    // Replay hands the codec the acknowledgement with its token blanked (0028 item 5): it
    // decodes the same.
    s.open();
    let spans = spans_of(&s.codec, &ack);
    let mut blanked = ack.clone().into_bytes();
    for span in spans.body() {
        blanked[span.start as usize..span.end as usize].fill(b'2');
    }
    let blanked = String::from_utf8(blanked).unwrap();
    assert_ne!(blanked, ack);
    s.pushes(&blanked, vec![none(authenticated)]);
}

#[test]
fn a_refused_authentication_reports_the_refusal_and_never_authenticated() {
    let mut s = Session::new();
    s.open();
    s.pushes(
        "auth|ok=0|code=3002|msg=bad token",
        vec![none(ExecEvent::UncorrelatedError(Reject {
            kind: RejectKind::Unsupported,
            venue_code: Some("3002".into()),
            raw: "bad token".into(),
        }))],
    );
    s.refuses(
        &format!("auth|ok=1|token={TOY_TOKEN}"),
        "no authentication asked",
    );
    s.open();
    s.refuses("auth|ok=0", "code");
    // The refused frame left the stream waiting for its acknowledgement.
    s.pushes(
        &format!("auth|ok=1|token={TOY_TOKEN}"),
        vec![none(ExecEvent::Conn {
            stream: EXEC_STREAM,
            state: ConnState::Authenticated,
        })],
    );
}

#[test]
fn redact_inbound_names_the_toy_tokens_span_and_nothing_else() {
    let codec = ToyExec::new(Box::new(ToySigner));
    let ack = format!("auth|ok=1|token={TOY_TOKEN}|ts=3");
    let at = ack.find(TOY_TOKEN).unwrap() as u32;
    let spans = spans_of(&codec, &ack);
    assert_eq!(spans, InboundSpans::frame(token_at(at)));
    assert_eq!(spans.check(Inbound::Frame(RawFrame::Text(&ack))), Ok(()));
    // A token at the end of a record, and in a later line of a frame, is named too; an empty
    // one names nothing.
    let two = format!("order|x=1\nauth|token={TOY_TOKEN}");
    let at = two.find(TOY_TOKEN).unwrap() as u32;
    assert_eq!(spans_of(&codec, &two), InboundSpans::frame(token_at(at)));
    for none in ["auth|ok=1|token=", "order|cid=x|tokens=1", "", "x=token=1"] {
        assert_eq!(spans_of(&codec, none), InboundSpans::NONE, "{none}");
    }
    // A binary frame and an HTTP response carry no token of the toy's.
    let binary = Inbound::Frame(RawFrame::Binary(b"auth|token=abc"));
    assert_eq!(codec.redact_inbound(binary), InboundSpans::NONE);
    let resp = HttpResponse {
        status: 200,
        headers: &[],
        body: b"auth|token=abc",
    };
    let http = Inbound::Http(HttpTag(1), resp);
    assert_eq!(codec.redact_inbound(http), InboundSpans::NONE);
}
