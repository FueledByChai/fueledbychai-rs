//! FBC-7ce's done line: the conformance toy venue decodes hand-written frames through the
//! `DecodeScope` the core lends, and nothing else, into an order update with its echoed flags,
//! an `Amended` update naming its new venue id, a venue mode event per scope, a reject mapped by
//! the toy's code table, a fill keyed by its venue fill id and one keyed `FillKey::Derived`
//! carrying realized P&L and funding; and a frame with a negative optional quantity is refused
//! with nothing pushed (Codex r4172917318), as is every other malformed frame (record 0014
//! item 2).

use std::collections::HashSet;
use std::sync::OnceLock;

use fbc_conformance::toy::{
    self, EXEC_STREAM, FillIds, INST_A, INST_B, OWN_NS, REJECT_CODES, ToyExec, ToySigner,
};
use fbc_core::{
    AccountKey, AmendAck, AssetSym, CancelReason, CidMatch, CidMint, ClientOrderId, DecodeError,
    Effects, ExchNs, ExchTsKind, ExecCodec, ExecEvent, ExecSink, FeeError, FillEvent, FillIdent,
    FillKey, IdError, Liquidity3, Lots, ModeScope, Money, NamespaceLease, OrderUpdate, RawFrame,
    Reject, RejectKind, RpcId, Side, SubmitOutcome, TerminalReject, Ticks, VenueMeta, VenueMode,
    VenueOrderId, VenueOrderState, WallNs, encode_cid,
};

// ---------------------------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------------------------

const WALL: i64 = 1_759_363_200_000_000_000;

/// Our `n`th client id, minted once per test binary under a lease in a fresh directory.
fn cid(n: usize) -> ClientOrderId {
    static CIDS: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let cids = CIDS.get_or_init(|| {
        let name = format!("fbc-conformance-toy-events-{}", std::process::id());
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), OWN_NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(WALL));
        let cids = (0..4).map(|_| mint.mint().unwrap()).collect();
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

fn usdc(nanos: i128) -> Money {
    Money::new(nanos, AssetSym::new("USDC").unwrap())
}

/// A sink that keeps what it is given.
#[derive(Default)]
struct Collect(Vec<(VenueMeta, ExecEvent)>);

impl ExecSink for Collect {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent) {
        self.0.push((meta, ev));
    }
}

/// Decodes `frame` with the toy declaring `fill_ids`, through the decode scope its caps lend
/// alone; what it pushed, and its result. A decode asks for no effect.
fn decode_as(
    fill_ids: FillIds,
    frame: RawFrame<'_>,
) -> (Result<(), DecodeError>, Vec<(VenueMeta, ExecEvent)>) {
    let mut codec = ToyExec::with_fill_ids(Box::new(ToySigner), fill_ids);
    let (mut fx, mut sink, specs) = (Effects::new(), Collect::default(), toy::specs());
    let result = toy::with_scope_for(fill_ids, |scope| {
        codec.on_frame(EXEC_STREAM, frame, scope, &specs, &mut sink, &mut fx)
    });
    assert!(fx.is_empty(), "a decode asks for no effect");
    (result, sink.0)
}

/// The one event `text` decodes to, with the toy's fills carrying venue fill ids.
fn decoded(text: &str) -> (VenueMeta, ExecEvent) {
    decoded_as(FillIds::Venue, text)
}

fn decoded_as(fill_ids: FillIds, text: &str) -> (VenueMeta, ExecEvent) {
    let (result, pushed) = decode_as(fill_ids, RawFrame::Text(text));
    assert_eq!(result, Ok(()), "{text}");
    match <[_; 1]>::try_from(pushed) {
        Ok([one]) => one,
        Err(pushed) => panic!("expected one event from {text}, got {pushed:?}"),
    }
}

/// `text` is refused with `err` and nothing pushed.
fn refused_as(fill_ids: FillIds, text: &str, err: DecodeError) {
    let (result, pushed) = decode_as(fill_ids, RawFrame::Text(text));
    assert_eq!(result, Err(err), "{text}");
    assert!(pushed.is_empty(), "{text} pushed {pushed:?}");
}

fn refused(text: &str, err: DecodeError) {
    refused_as(FillIds::Venue, text, err);
}

/// Sequenced at `seq`, stamped by the matching engine at `ts`.
fn meta(ts: i64, seq: u64) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(ts)),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: Some(seq),
    }
}

/// An open buy of ours on TOYA-PERP as the toy's wire reports it, before its state.
fn order_head(n: usize) -> String {
    format!(
        "order|cid={}|vid=V-1|sym=TOYA-PERP|side=B",
        wire_cid(cid(n))
    )
}

fn update(state: VenueOrderState) -> OrderUpdate {
    OrderUpdate {
        cid: Some(CidMatch::Ours(cid(0))),
        vid: Some(vid("V-1")),
        inst: INST_A,
        side: Side::Buy,
        state,
        cum_filled: lots(4),
        px: Some(Ticks(130_865)),
        qty: Some(lots(25)),
        post_only: Some(true),
        reduce_only: Some(false),
    }
}

/// A fill's fields after its identity: 3 lots of a sell on TOYB-PERP, as a maker, its fee a
/// rebate, its realized P&L a loss and its realized funding a gain.
fn fill_tail() -> String {
    format!(
        "cid={}|sym=TOYB-PERP|side=S|px=130870|qty=3|liq=M|fee=-150000|fa=USDC|pnl=-2500000000\
         |fund=125000000|ts=7|seq=30",
        wire_cid(cid(1))
    )
}

fn fill(ident: FillIdent, replay: bool) -> FillEvent {
    let fee = toy::with_scope(|scope| scope.fee(-150_000, AssetSym::new("USDC").unwrap()));
    FillEvent {
        ident,
        cid: Some(CidMatch::Ours(cid(1))),
        inst: INST_B,
        side: Side::Sell,
        px: Ticks(130_870),
        qty: lots(3),
        liquidity: Liquidity3::Maker,
        fee: fee.unwrap(),
        realized_pnl: Some(usdc(-2_500_000_000)),
        realized_funding: Some(usdc(125_000_000)),
        replay,
    }
}

fn fill_id(wire: &str) -> fbc_core::FillId {
    toy::with_scope(|scope| scope.fill_id(wire)).unwrap()
}

// ---------------------------------------------------------------------------------------------
// The done line.
// ---------------------------------------------------------------------------------------------

#[test]
fn an_order_update_decodes_with_the_flags_the_venue_echoes() {
    let caps = toy::caps().exec.unwrap().order;
    assert!(caps.events_echo_flags && caps.cid_echoed_on_events);
    let text = format!(
        "{}|st=open|cum=4|px=130865|qty=25|po=1|ro=0|ts=5|seq=9",
        order_head(0)
    );
    assert_eq!(
        decoded(&text),
        (meta(5, 9), ExecEvent::Order(update(VenueOrderState::Open)))
    );
    // The other flag echoed; price and quantity absent are `None`, never zero, and a venue
    // with no exchange time says so.
    let text = format!("{}|st=filled|cum=25|po=0|ro=1|seq=10", order_head(0));
    let (meta, ev) = decoded(&text);
    let unstamped = VenueMeta {
        exch_ts: None,
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: Some(10),
    };
    assert_eq!(meta, unstamped);
    let filled = OrderUpdate {
        cum_filled: lots(25),
        px: None,
        qty: None,
        post_only: Some(false),
        reduce_only: Some(true),
        ..update(VenueOrderState::Filled)
    };
    assert_eq!(ev, ExecEvent::Order(filled));
    // Every state the toy reports, and every cancel reason.
    let tail = "cum=4|px=130865|qty=25|po=1|ro=0|ts=5|seq=9";
    let canceled = VenueOrderState::Canceled;
    let states = [
        ("st=expired", VenueOrderState::Expired),
        (
            "st=canceled|why=requested",
            canceled(CancelReason::Requested),
        ),
        (
            "st=canceled|why=disconnect",
            canceled(CancelReason::Disconnect),
        ),
        (
            "st=canceled|why=selftrade",
            canceled(CancelReason::SelfTrade),
        ),
        ("st=canceled|why=postonly", canceled(CancelReason::PostOnly)),
        (
            "st=canceled|why=reduceonly",
            canceled(CancelReason::ReduceOnly),
        ),
        ("st=canceled|why=unfilled", canceled(CancelReason::Unfilled)),
        (
            "st=canceled|why=liquidation",
            canceled(CancelReason::Liquidation),
        ),
        ("st=canceled|why=venue", canceled(CancelReason::Venue)),
    ];
    for (state, want) in states {
        let text = format!("{}|{state}|{tail}", order_head(0));
        assert_eq!(decoded(&text).1, ExecEvent::Order(update(want)), "{state}");
    }
    // An order that names no venue id yet, and one minted by another system.
    let text = format!("order|cid=JAVA-7|sym=TOYA-PERP|side=B|st=open|{tail}");
    let ExecEvent::Order(seen) = decoded(&text).1 else {
        panic!("an order update");
    };
    assert_eq!((seen.cid, seen.vid), (Some(CidMatch::Unparseable), None));
}

#[test]
fn an_amended_update_names_the_new_venue_id_and_states_the_new_price_and_total_once() {
    // The toy issues a new venue id for an amended order and reports the replaced order with an
    // order event, as its caps declare.
    let amend = toy::caps().exec.unwrap().order.amend.unwrap();
    assert_eq!(
        (amend.keeps_venue_id, amend.ack),
        (false, AmendAck::ReplacedEvent)
    );
    let text = format!(
        "{}|st=amended|nvid=V-2|cum=4|px=130860|qty=10|po=1|ro=0|ts=6|seq=11",
        order_head(0)
    );
    let amended = OrderUpdate {
        px: Some(Ticks(130_860)),
        qty: Some(lots(10)),
        ..update(VenueOrderState::Amended {
            new_vid: Some(vid("V-2")),
        })
    };
    assert_eq!(decoded(&text), (meta(6, 11), ExecEvent::Order(amended)));
    // Since the venue always issues one, an amended update without its new id is refused.
    let text = format!("{}|st=amended|cum=4|po=1|ro=0|seq=11", order_head(0));
    refused(&text, DecodeError::Malformed("nvid"));
}

#[test]
fn a_venue_mode_event_names_the_market_or_the_whole_account() {
    let modes = [
        ("normal", VenueMode::Normal),
        ("postonly", VenueMode::PostOnly),
        ("reduceonly", VenueMode::ReduceOnly),
        ("cancelonly", VenueMode::CancelOnly),
        ("halted", VenueMode::Halted),
    ];
    for (wire, mode) in modes {
        let one = decoded(&format!("mode|m={wire}|sym=TOYB-PERP|seq=3"));
        let scope = ModeScope::Instrument(INST_B);
        let sequenced = VenueMeta {
            exch_ts: None,
            exch_ts_kind: ExchTsKind::Unknown,
            venue_seq: Some(3),
        };
        assert_eq!(one, (sequenced, ExecEvent::Mode { scope, mode }));
        let all = decoded(&format!("mode|m={wire}|ts=8"));
        let scope = ModeScope::Account;
        let stamped = VenueMeta {
            exch_ts: Some(ExchNs(8)),
            exch_ts_kind: ExchTsKind::MatchingEngine,
            venue_seq: None,
        };
        assert_eq!(all, (stamped, ExecEvent::Mode { scope, mode }));
    }
}

#[test]
fn a_reject_is_mapped_by_the_toys_code_table() {
    let codes: HashSet<&str> = REJECT_CODES.iter().map(|(code, _)| *code).collect();
    assert_eq!(
        codes.len(),
        REJECT_CODES.len(),
        "each code maps to one kind"
    );
    let mut cases: Vec<(&str, RejectKind)> = REJECT_CODES.to_vec();
    // A code the table does not hold is `Other`, never a guess.
    cases.push(("9999", RejectKind::Other));
    for (code, kind) in cases {
        // A refused request: its outcome, for the whole request, answers it.
        let (meta, ev) = decoded(&format!("reject|rpc=12|code={code}|msg=no luck"));
        assert_eq!(meta, VenueMeta::NONE);
        let reject = Reject {
            kind,
            venue_code: Some(code.into()),
            raw: "no luck".into(),
        };
        let outcome = SubmitOutcome::Rejected(reject);
        let (rpc, item) = (RpcId(12), None);
        assert_eq!(ev, ExecEvent::Outcome { rpc, item, outcome });
        assert_eq!(ev.answers(), Some(rpc));
        // A refused order: its state, only for a refusal that ends it. A refusal that leaves
        // the order as it was (not found, already terminal, not amendable, no change) is never
        // an order's state, so such a frame is refused (record 0014 item 6).
        let text = format!(
            "{}|st=rejected|code={code}|cum=4|px=130865|qty=25|po=1|ro=0|ts=5|seq=9",
            order_head(0)
        );
        match TerminalReject::new(kind) {
            Some(terminal) => {
                let rejected = update(VenueOrderState::Rejected(terminal));
                assert_eq!(decoded(&text).1, ExecEvent::Order(rejected), "{code}");
            }
            None => refused(&text, DecodeError::Malformed("code")),
        }
    }
    // The table holds both sorts.
    let terminal = |kind: &RejectKind| TerminalReject::new(*kind).is_some();
    assert!(REJECT_CODES.iter().any(|(_, k)| terminal(k)));
    assert!(REJECT_CODES.iter().any(|(_, k)| !terminal(k)));
    // A reject without a message keeps an empty one; one without a code is refused.
    let (_, ev) = decoded("reject|rpc=12|code=1001");
    let ExecEvent::Outcome {
        outcome: SubmitOutcome::Rejected(reject),
        ..
    } = ev
    else {
        panic!("a rejected outcome");
    };
    assert_eq!(&*reject.raw, "");
    refused("reject|rpc=12|msg=x", DecodeError::Malformed("code"));
    refused("reject|code=1001", DecodeError::Malformed("rpc"));
}

#[test]
fn a_fill_is_keyed_by_the_venue_fill_id() {
    let fills = toy::caps().exec.unwrap().fills;
    assert!(fills.fill_id && fills.realized_pnl && fills.realized_funding);
    let text = format!("fill|fid=F-1|vid=V-1|cum=7|{}", fill_tail());
    let ident = FillIdent::Venue {
        fill: fill_id("F-1"),
        vid: Some(vid("V-1")),
        cum_after: Some(lots(7)),
    };
    let (meta, ev) = decoded(&text);
    assert_eq!(
        (meta, &ev),
        (self::meta(7, 30), &ExecEvent::Fill(fill(ident, false)))
    );
    let ExecEvent::Fill(seen) = ev else {
        panic!("a fill");
    };
    assert_eq!(seen.key(), FillKey::Venue(fill_id("F-1")));
    // The order's id and cumulative quantity are optional beside a fill id: absent is `None`.
    let text = format!("fill|fid=F-2|{}", fill_tail());
    let ident = FillIdent::Venue {
        fill: fill_id("F-2"),
        vid: None,
        cum_after: None,
    };
    assert_eq!(decoded(&text).1, ExecEvent::Fill(fill(ident, false)));
    // A taker fill says so.
    let taker = fill_tail().replace("liq=M", "liq=T");
    let ExecEvent::Fill(seen) = decoded(&format!("fill|fid=F-3|{taker}")).1 else {
        panic!("a fill");
    };
    assert_eq!(seen.liquidity, Liquidity3::Taker);
    // A venue that declares fill ids sends one on every fill (Codex r4172835753).
    let text = format!("fill|vid=V-1|cum=7|{}", fill_tail());
    refused(&text, DecodeError::Malformed("fid"));
}

#[test]
fn a_fill_without_a_fill_id_is_keyed_by_order_and_cumulative_quantity_with_realized_pnl_and_funding()
 {
    // The toy declared without fill ids (Codex r4172835753: the declaration and the frames
    // agree).
    let fills = toy::caps_for(FillIds::Derived).exec.unwrap().fills;
    assert!(!fills.fill_id && fills.realized_pnl && fills.realized_funding);
    let text = format!("fill|vid=V-1|cum=7|{}", fill_tail());
    let (meta, ev) = decoded_as(FillIds::Derived, &text);
    let ident = FillIdent::Derived {
        vid: vid("V-1"),
        cum_after: lots(7),
    };
    assert_eq!(
        (meta, &ev),
        (self::meta(7, 30), &ExecEvent::Fill(fill(ident, false)))
    );
    let ExecEvent::Fill(seen) = ev else {
        panic!("a fill");
    };
    let key = FillKey::Derived {
        vid: vid("V-1"),
        cum_after: lots(7),
    };
    assert_eq!(seen.key(), key);
    assert_eq!(
        (seen.realized_pnl, seen.realized_funding),
        (Some(usdc(-2_500_000_000)), Some(usdc(125_000_000)))
    );
    // Both parts of the key are required, and a fill id the venue declares it never sends is
    // refused.
    let derived = |text: &str, key| refused_as(FillIds::Derived, text, DecodeError::Malformed(key));
    derived(&format!("fill|cum=7|{}", fill_tail()), "vid");
    derived(&format!("fill|vid=V-1|{}", fill_tail()), "cum");
    derived(
        &format!("fill|fid=F-1|vid=V-1|cum=7|{}", fill_tail()),
        "fid",
    );
}

#[test]
fn a_replayed_fill_is_marked_replay() {
    let fills = toy::caps().exec.unwrap().fills;
    assert!(fills.replays_fills_on_reconnect);
    let text = format!("refill|fid=F-1|vid=V-1|cum=7|{}", fill_tail());
    let ident = FillIdent::Venue {
        fill: fill_id("F-1"),
        vid: Some(vid("V-1")),
        cum_after: Some(lots(7)),
    };
    assert_eq!(decoded(&text).1, ExecEvent::Fill(fill(ident, true)));
    let text = format!("refill|vid=V-1|cum=7|{}", fill_tail());
    let ident = FillIdent::Derived {
        vid: vid("V-1"),
        cum_after: lots(7),
    };
    assert_eq!(
        decoded_as(FillIds::Derived, &text).1,
        ExecEvent::Fill(fill(ident, true))
    );
}

#[test]
fn a_negative_optional_quantity_refuses_the_frame_with_nothing_pushed() {
    // Codex r4172917318: absent is `None`, but a negative count is malformed, never absent.
    let order = |qty: &str| format!("{}|st=open|cum=0|{qty}po=1|ro=0|seq=9", order_head(0));
    let ExecEvent::Order(absent) = decoded(&order("")).1 else {
        panic!("an order update");
    };
    assert_eq!(absent.qty, None);
    refused(&order("qty=-1|"), DecodeError::Malformed("qty"));
    // A fill's cumulative quantity beside its fill id is optional too.
    let fill = |cum: &str| format!("fill|fid=F-1|{cum}{}", fill_tail());
    let ExecEvent::Fill(absent) = decoded(&fill("")).1 else {
        panic!("a fill");
    };
    assert_eq!(absent.cum_after(), None);
    refused(&fill("cum=-3|"), DecodeError::Malformed("cum"));
}

#[test]
fn a_malformed_frame_is_refused_with_nothing_pushed() {
    let (result, pushed) = decode_as(FillIds::Venue, RawFrame::Binary(b"order"));
    assert_eq!(result, Err(DecodeError::Malformed("binary frame")));
    assert!(pushed.is_empty());
    let open = |rest: &str| format!("{}|st=open|{rest}", order_head(0));
    let malformed = [
        // The record itself.
        (
            "mode|m=halted\nmode|m=normal".to_owned(),
            "one record per frame",
        ),
        (String::new(), "kind"),
        ("|m=halted".to_owned(), "kind"),
        ("mode|halted".to_owned(), "field"),
        ("ack|rpc=11".to_owned(), "kind"),
        ("mode|m=halted|ts=soon".to_owned(), "ts"),
        ("mode|m=halted|seq=-1".to_owned(), "seq"),
        ("mode|m=paused".to_owned(), "m"),
        ("mode".to_owned(), "m"),
        // Order updates: every field the caps promise is required.
        (open("cum=0|po=1|ro=0"), "seq"),
        (open("cum=0|po=1|seq=9"), "ro"),
        (open("cum=0|po=yes|ro=0|seq=9"), "po"),
        (open("po=1|ro=0|seq=9"), "cum"),
        (open("cum=x|po=1|ro=0|seq=9"), "cum"),
        (open("cum=-1|po=1|ro=0|seq=9"), "cum"),
        (open("cum=5|qty=4|po=1|ro=0|seq=9"), "cum"),
        (open("cum=0|px=high|po=1|ro=0|seq=9"), "px"),
        (open("cum=0|qty=many|po=1|ro=0|seq=9"), "qty"),
        (format!("{}|cum=0|po=1|ro=0|seq=9", order_head(0)), "st"),
        (
            format!("{}|st=parked|cum=0|po=1|ro=0|seq=9", order_head(0)),
            "st",
        ),
        (
            format!("{}|st=canceled|cum=0|po=1|ro=0|seq=9", order_head(0)),
            "why",
        ),
        (
            format!(
                "{}|st=canceled|why=bored|cum=0|po=1|ro=0|seq=9",
                order_head(0)
            ),
            "why",
        ),
        (
            format!("{}|st=rejected|cum=0|po=1|ro=0|seq=9", order_head(0)),
            "code",
        ),
        (
            "order|vid=V-1|sym=TOYA-PERP|side=B|st=open|cum=0|po=1|ro=0|seq=9".to_owned(),
            "cid",
        ),
        (
            format!(
                "order|cid={}|vid=V-1|sym=TOYA-PERP|side=X|st=open|cum=0|po=1|ro=0|seq=9",
                wire_cid(cid(0))
            ),
            "side",
        ),
        (
            format!(
                "order|cid={}|vid=V-1|sym=TOYA-PERP|st=open|cum=0|po=1|ro=0|seq=9",
                wire_cid(cid(0))
            ),
            "side",
        ),
        (
            format!(
                "order|cid={}|vid=V-1|side=B|st=open|cum=0|po=1|ro=0|seq=9",
                wire_cid(cid(0))
            ),
            "sym",
        ),
        // Fills.
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("|seq=30", "")),
            "seq",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("qty=3", "qty=0")),
            "qty",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("qty=3", "qty=-3")),
            "qty",
        ),
        (format!("fill|fid=F-1|cum=2|{}", fill_tail()), "cum"),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("liq=M", "liq=X")),
            "liq",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("liq=M|", "")),
            "liq",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("fa=USDC", "fa=")),
            "fa",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("fa=USDC|", "")),
            "fa",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("fee=-150000|", "")),
            "fee",
        ),
        (
            format!(
                "fill|fid=F-1|{}",
                fill_tail().replace("pnl=-2500000000|", "")
            ),
            "pnl",
        ),
        (
            format!(
                "fill|fid=F-1|{}",
                fill_tail().replace("fund=125000000|", "")
            ),
            "fund",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("px=130870|", "")),
            "px",
        ),
        (
            format!("fill|fid=F-1|{}", fill_tail().replace("cid=", "xid=")),
            "cid",
        ),
    ];
    for (text, part) in &malformed {
        refused(text, DecodeError::Malformed(part));
    }
    // An instrument missing from the spec table, and ids or a fee the decode scope refuses.
    let unknown = open("cum=0|po=1|ro=0|seq=9").replace("TOYA-PERP", "TOYZ-PERP");
    refused(&unknown, DecodeError::UnknownInstrument);
    let tail = fill_tail();
    refused(
        &format!("fill|fid=F-1|{}", tail.replace("TOYB-PERP", "TOYZ-PERP")),
        DecodeError::UnknownInstrument,
    );
    refused(
        "mode|m=halted|sym=TOYZ-PERP",
        DecodeError::UnknownInstrument,
    );
    let empty = DecodeError::IdRefused(IdError::Empty);
    refused(
        &open("cum=0|po=1|ro=0|seq=9").replace("vid=V-1", "vid="),
        empty,
    );
    let amended = format!("{}|st=amended|nvid=|cum=0|po=1|ro=0|seq=9", order_head(0));
    refused(&amended, empty);
    refused(&format!("fill|fid=|{tail}"), empty);
    refused(&format!("fill|fid=F-1|vid=|{tail}"), empty);
    refused_as(FillIds::Derived, &format!("fill|vid=|cum=7|{tail}"), empty);
    let min = tail.replace("fee=-150000", &format!("fee={}", i128::MIN));
    refused(
        &format!("fill|fid=F-1|{min}"),
        DecodeError::FeeRefused(FeeError::OutOfRange),
    );
}

#[test]
fn the_toy_declared_without_fill_ids_differs_only_in_its_fill_id() {
    let (venue, derived) = (toy::caps(), toy::caps_for(FillIds::Derived));
    assert_eq!(toy::caps_for(FillIds::Venue), venue);
    let mut without = venue.clone();
    without.exec.as_mut().unwrap().fills.fill_id = false;
    assert_eq!(derived, without);
}
