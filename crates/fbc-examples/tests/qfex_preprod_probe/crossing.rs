//! FBC-su9i: what stage two's crossing session stands on, at function level and against the
//! QFEX model ([`super::model`]) on no socket. Declared by path in `tests/qfex_preprod_probe.rs`,
//! so it is part of that test binary and Cargo builds no target of its own for it. Nothing places
//! on a venue, and every symbol, key, id and value is made up.
//!
//! - the model's `ack_echo` knob writes each place's `ACK` client id exactly, upper-cased,
//!   escaped, absent, null or empty, its `alo_cross` knob answers a post-only order through
//!   the touch `CANCELLED`, with an unmapped status, with an `err` echo, or by filling it as a
//!   taker, resting every other order as before, and its `cancel_misses` knob answers every
//!   cancel `NO_SUCH_ORDER`, the order left open;
//! - the run's tee, fed the model's frames as the journal offers them, gives the exact-echo gate
//!   and the report's items ([`stage_two`]): how each kind of frame gave back our client id,
//!   which statuses were rejects and which ends, the characters the client ids exercised by
//!   class, whether `add_order` was accepted as encoded, and how the crossing post-only came
//!   back;
//! - X and Y count ended after a drop only on all three of 0151 item 5's reads;
//! - the place stages' session machinery ([`stage::Policy`]): a session outside the drop test
//!   cancels by venue id at every stop, Ctrl-C included, and once it ran its course, while a drop
//!   session cancels only at Ctrl-C or once a fill of ours came; held cancels end a session
//!   outside the drop test by the abrupt drop only after its account's drop test passed in the
//!   run; and its span bound is met one `--rpc-timeout-ms` early;
//! - stage two's record: a blocked crossing order's stop, the registry's count of an order
//!   resting (an `Unknown` one included), the accounts that cross, the reads' stop after a
//!   crossing session, and the report's and the evidence's every branch, with no id or value.
//!
//! The acceptance's gate and no-other-order tests are in [`super::place_stages`], on these
//! helpers.

use super::*;

use std::collections::BTreeSet;

use fbc_core::{
    AccountKey, CidMint, ClientOrderId, InstrumentId, ItemRef, Lots, Namespace, NamespaceLease,
    NewOrder, OrderKind, Reject, RejectKind, Side, SubmitOutcome, Ticks, Tif,
};
use fbc_journal::RecordRef;
use fbc_oms::{OrdState, OrderOp, Registry};
use fbc_venue_qfex::exec::CLIENT_ID_FORMAT;
use model::{AloCross, Echo, QfexModel, State, UNMAPPED};
use stage::{Ending, Policy};
use stage_two::{AsEncoded, Blocked, Built, Characters, Crossed, EchoGate, Echoes, Statuses};

/// A journal sink that takes every record and keeps none: what the tee wraps here.
pub(super) struct Null;

impl JournalSink for Null {
    fn record(&mut self, _: TrafficClass, _: WallNs, _: &Record) -> Recorded {
        Recorded::Ok
    }

    fn record_ref(&mut self, _: TrafficClass, _: WallNs, _: RecordRef<'_>) -> Recorded {
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }

    fn open_gap(&self) -> Option<u64> {
        None
    }
}

/// A run's tee over a sink that keeps nothing, and its tap.
pub(super) fn teed() -> (tee::Tee<Null>, tee::Tap) {
    let tee = tee::Tee::new(Null);
    let tap = tee.tap();
    (tee, tap)
}

/// Offers `text` to `tee` as the session journals a frame it sent.
pub(super) fn sent(tee: &mut tee::Tee<Null>, text: &str) {
    let record = Record::Outbound {
        at: MonoNs(1),
        conn: ConnKey { conn: 0, epoch: 1 },
        rpc: None,
        opcode: Opcode::Text,
        frame: WireSlice::redacted(text.as_bytes().to_vec(), Vec::new()).unwrap(),
    };
    let _ = tee.record_ref(TrafficClass::Safety, WallNs(1), RecordRef::Owned(&record));
}

/// Offers `frames`, an answer of the model, to `tee` as the session journals each frame it
/// receives.
pub(super) fn heard(tee: &mut tee::Tee<Null>, frames: &[Frame]) {
    for frame in frames {
        let Frame::Text(text) = frame else {
            panic!("a binary frame")
        };
        let record = RecordRef::Inbound {
            stamp: Stamp {
                ingest_seq: 0,
                kernel_rx: None,
                recv_mono: MonoNs(2),
                recv_wall: WallNs(2),
                conn: ConnKey { conn: 0, epoch: 1 },
            },
            frame: RawFrame::Text(text),
            redact: &[],
        };
        let _ = tee.record_ref(TrafficClass::Safety, WallNs(2), record);
    }
}

/// Offers `tee` an `ack` or `err` echo, `envelope`, of `frame`, an `add_order` naming `cid`, its
/// client id at the span the codec names for it, as the journal keeps the echo.
pub(super) fn heard_echo(tee: &mut tee::Tee<Null>, envelope: &str, frame: &str, cid: &str) {
    heard_err(tee, envelope, "InvalidOrder", frame, cid);
}

/// [`heard_echo`], an `err` echo's `error_code` being `code`.
pub(super) fn heard_err(
    tee: &mut tee::Tee<Null>,
    envelope: &str,
    code: &str,
    frame: &str,
    cid: &str,
) {
    let echo = match envelope {
        "ack" => format!(r#"{{"ack":{{"incoming_message":{frame}}}}}"#),
        _ => format!(r#"{{"type":"Err","error_code":"{code}","incoming_message":{frame}}}"#),
    };
    let at = echo.find(&format!("\"{cid}\"")).unwrap() + 1;
    let at = u32::try_from(at).unwrap();
    let span = at..at + u32::try_from(cid.len()).unwrap();
    let spans: Vec<std::ops::Range<u32>> = std::iter::once(span).collect();
    let record = RecordRef::Inbound {
        stamp: Stamp {
            ingest_seq: 0,
            kernel_rx: None,
            recv_mono: MonoNs(2),
            recv_wall: WallNs(2),
            conn: ConnKey { conn: 0, epoch: 1 },
        },
        frame: RawFrame::Text(&echo),
        redact: &spans,
    };
    let _ = tee.record_ref(TrafficClass::Safety, WallNs(2), record);
}

/// `n` client ids, minted under a namespace lease of their own as the run mints its ids.
pub(super) fn minted(n: usize) -> Vec<ClientOrderId> {
    let (dir, _removed) = temp_dir("stage-two-cids");
    let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), Namespace::new(7)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    (0..n).map(|_| mint.mint().unwrap()).collect()
}

/// `cid` as the codec writes it on QFEX.
pub(super) fn text_of(cid: ClientOrderId) -> String {
    fbc_core::encode_cid(&CLIENT_ID_FORMAT, cid)
        .unwrap()
        .to_string()
}

/// An `add_order` for a post-only GTC of 0.01 on the model's symbol, `side` at `price`, with
/// the client id `cid`, as the codec writes one.
pub(super) fn add_order(cid: &str, side: &str, price: &str) -> String {
    format!(
        concat!(
            r#"{{"type":"add_order","params":{{"symbol":"SYN-USD","side":"{side}","#,
            r#""order_type":"ALO","order_time_in_force":"GTC","quantity":0.01,"price":{price},"#,
            r#""client_order_id":"{cid}"}}}}"#,
        ),
        side = side,
        price = price,
        cid = cid
    )
}

/// Opens trade connection `conn` on `model` as the primary account's session.
pub(super) fn session(model: &mut QfexModel, conn: usize) {
    model
        .trade(conn, &Frame::text(r#"{"type":"auth","params":{}}"#))
        .unwrap();
    let subscribe = r#"{"type":"subscribe","params":{"channels":["orders"]}}"#;
    model.trade(conn, &Frame::text(subscribe)).unwrap();
}

/// Sends `text` to `model` on connection 0, offering it to `tee` as sent and the answer as
/// heard: the answer.
pub(super) fn exchange(model: &mut QfexModel, tee: &mut tee::Tee<Null>, text: &str) -> Vec<Frame> {
    sent(tee, text);
    let answer = model.trade(0, &Frame::text(text)).unwrap();
    heard(tee, &answer);
    answer
}

/// The text of the `n`th frame of `frames`.
fn text_at(frames: &[Frame], n: usize) -> &str {
    match &frames[n] {
        Frame::Text(text) => text,
        Frame::Binary(_) => panic!("a binary frame"),
    }
}

/// The market every registry here trades.
pub(super) const INST: InstrumentId = InstrumentId::new(1);

/// A post-only buy of `qty` lots at 100 for `cid` on [`INST`].
pub(super) fn order(cid: ClientOrderId, qty: i64) -> NewOrder {
    NewOrder {
        cid,
        inst: INST,
        side: Side::Buy,
        qty: Lots::new(qty).unwrap(),
        kind: OrderKind::Limit { px: Ticks(100) },
        tif: Tif::Gtc,
        channel: fbc_core::Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// How an order registered in a test registry stands.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum As {
    /// Registered and sent, its outcome not yet known: PendingNew.
    Sent,
    /// Its place answered `Unknown` at its deadline.
    Unknown,
    /// Its place accepted.
    Open,
    /// Its place refused: terminal.
    Refused,
}

/// Registers `cid`'s order of one lot in `reg`, standing as `stands`.
pub(super) fn holding(reg: &mut Registry, cid: ClientOrderId, stands: As) {
    holding_on(reg, cid, INST, stands);
}

/// [`holding`], the order on `inst`.
pub(super) fn holding_on(reg: &mut Registry, cid: ClientOrderId, inst: InstrumentId, stands: As) {
    reg.insert(NewOrder {
        inst,
        ..order(cid, 1)
    })
    .unwrap();
    reg.placement_sent(cid, MonoNs(0), WallNs(1)).unwrap();
    let outcome = match stands {
        As::Sent => return,
        As::Unknown => SubmitOutcome::Unknown,
        As::Open => SubmitOutcome::Accepted {
            ack: fbc_core::AckLevel::Final,
        },
        As::Refused => SubmitOutcome::Rejected(Reject {
            kind: RejectKind::PostOnlyWouldCross,
            venue_code: None,
            raw: "synthetic".into(),
        }),
    };
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: None,
    };
    reg.on_outcome(cid, OrderOp::Place, &item, &outcome, MonoNs(1))
        .unwrap();
}

/// A clean never-arming read of an account: no open order, no TWAP, flat.
pub(super) fn clean() -> capture::AccountRead {
    capture::AccountRead {
        flat: true,
        ..capture::AccountRead::default()
    }
}

// ----- The model's knobs (FBC-su9i) -----

#[test]
fn the_models_ack_echo_writes_each_places_client_id_as_its_knob_says() {
    let cid = text_of(minted(1)[0]);
    for (echo, written) in [
        (Echo::Exact, Some(format!(r#""client_order_id":"{cid}""#))),
        (
            Echo::Upper,
            Some(format!(r#""client_order_id":"{}""#, cid.to_uppercase())),
        ),
        (
            Echo::Escaped,
            Some(format!(
                r#""client_order_id":"\u{:04x}{}""#,
                u32::from(cid.chars().next().unwrap()),
                &cid[1..]
            )),
        ),
        (Echo::Absent, None),
        (Echo::Null, Some(r#""client_order_id":null"#.to_owned())),
        (Echo::Empty, Some(r#""client_order_id":"""#.to_owned())),
    ] {
        let mut model = QfexModel {
            ack_echo: echo,
            ..QfexModel::default()
        };
        session(&mut model, 0);
        for n in 0..2 {
            let placed = model.trade(0, &Frame::text(add_order(&cid, "BUY", "99.50")));
            let placed = placed.unwrap();
            assert_eq!(placed.len(), 1, "{echo:?}");
            let text = text_at(&placed, 0);
            let parsed = json(text);
            assert_eq!(parsed["order_response"]["status"], "ACK");
            match &written {
                Some(member) => assert!(text.contains(member.as_str()), "{echo:?} {n}: {text}"),
                None => assert!(!text.contains("client_order_id"), "{text}"),
            }
            // Escaped, the member still reads as our client id once decoded.
            if echo == Echo::Escaped {
                assert_eq!(parsed["order_response"]["client_order_id"], cid.as_str());
            }
        }
        // The order rests whatever its ACK wrote.
        assert!(model.orders.iter().all(|o| o.state == State::Open));
    }
}

#[test]
fn the_models_alo_cross_answers_a_post_only_through_the_touch_as_its_knob_says() {
    let cids = minted(4);
    let cid = text_of(cids[0]);
    // At the ask, a buy crosses; at the bid, a sell does.
    for (side, price) in [("BUY", "100.10"), ("SELL", "100.00")] {
        for how in [
            AloCross::Cancelled,
            AloCross::Unmapped,
            AloCross::Err,
            AloCross::AckedCancelled,
            AloCross::Fills,
        ] {
            let mut model = QfexModel {
                alo_cross: Some(how),
                ..QfexModel::default()
            };
            session(&mut model, 0);
            let frame = add_order(&cid, side, price);
            let answered = model.trade(0, &Frame::text(&frame)).unwrap();
            let first = json(text_at(&answered, 0));
            let (frames, state) = match how {
                AloCross::Cancelled => {
                    assert_eq!(first["order_response"]["status"], "CANCELLED");
                    assert_eq!(first["order_response"]["client_order_id"], cid.as_str());
                    (1, Some(State::Cancelled))
                }
                AloCross::Unmapped => {
                    assert_eq!(first["order_response"]["status"], UNMAPPED);
                    assert_eq!(first["order_response"]["client_order_id"], cid.as_str());
                    (1, Some(State::Cancelled))
                }
                AloCross::Err => {
                    assert_eq!(first["error_code"], "InvalidOrder");
                    assert_eq!(first["incoming_message"], json(&frame));
                    (1, None)
                }
                AloCross::AckedCancelled => {
                    assert_eq!(first["order_response"]["status"], "ACK");
                    assert_eq!(first["order_response"]["client_order_id"], cid.as_str());
                    let ended = json(text_at(&answered, 1));
                    assert_eq!(ended["order_response"]["status"], "CANCELLED");
                    assert_eq!(ended["order_response"]["client_order_id"], cid.as_str());
                    let id = &first["order_response"]["order_id"];
                    assert_eq!(&ended["order_response"]["order_id"], id);
                    (2, Some(State::Cancelled))
                }
                AloCross::Fills => {
                    let fill = &first["fill_response"];
                    assert_eq!(fill["execution_type"], "TAKER");
                    assert_eq!(fill["client_order_id"], cid.as_str());
                    let filled = json(text_at(&answered, 1));
                    assert_eq!(filled["order_response"]["status"], "FILLED");
                    assert_eq!(model.fills.len(), 1);
                    (2, Some(State::Filled))
                }
            };
            assert_eq!(answered.len(), frames, "{side} {how:?}");
            assert_eq!(model.orders.first().map(|o| o.state), state, "{how:?}");
            assert!(model.log.iter().any(|e| e.what == format!("cross {how:?}")));
            // Behind the touch it rests, its ACK alone, whatever the knob.
            let behind = match side {
                "BUY" => "99.50",
                _ => "100.60",
            };
            let other = text_of(cids[1]);
            let rests = model.trade(0, &Frame::text(add_order(&other, side, behind)));
            let rests = rests.unwrap();
            assert_eq!(rests.len(), 1);
            assert_eq!(json(text_at(&rests, 0))["order_response"]["status"], "ACK");
        }
    }
    // A limit order through the touch is no post-only: it rests as before, and with no knob a
    // post-only through the touch does too. A maker fill's execution type is unchanged.
    let limit = add_order(&cid, "BUY", "100.10").replace(r#""ALO""#, r#""LIMIT""#);
    for (knob, frame) in [
        (Some(AloCross::Cancelled), limit),
        (None, add_order(&cid, "BUY", "100.10")),
    ] {
        let mut model = QfexModel {
            alo_cross: knob,
            on_ack: Some((0, model::OnAck::MakerFill)),
            ..QfexModel::default()
        };
        session(&mut model, 0);
        let answered = model.trade(0, &Frame::text(frame)).unwrap();
        assert_eq!(
            json(text_at(&answered, 0))["order_response"]["status"],
            "ACK"
        );
        assert_eq!(
            json(text_at(&answered, 1))["fill_response"]["execution_type"],
            "MAKER"
        );
        assert!(model.log.iter().all(|e| !e.what.starts_with("cross ")));
    }
}

#[test]
fn the_models_cancel_misses_answers_every_cancel_no_such_order_and_leaves_the_order_open() {
    let cid = text_of(minted(1)[0]);
    for misses in [false, true] {
        let mut model = QfexModel {
            cancel_misses: misses,
            ..QfexModel::default()
        };
        session(&mut model, 0);
        let acked = model.trade(0, &Frame::text(add_order(&cid, "BUY", "99.50")));
        let id = json(text_at(&acked.unwrap(), 0))["order_response"]["order_id"].clone();
        let cancel = format!(r#"{{"type":"cancel_order","params":{{"order_id":{id}}}}}"#);
        let answer = model.trade(0, &Frame::text(cancel)).unwrap();
        let status = json(text_at(&answer, 0))["order_response"]["status"].clone();
        let (want, state) = match misses {
            true => ("NO_SUCH_ORDER", State::Open),
            false => ("CANCELLED", State::Cancelled),
        };
        assert_eq!(status, want);
        assert_eq!(model.orders[0].state, state);
    }
}

// ----- The run's tee: the gate and the report's items -----

#[test]
fn echoes_count_each_kind_of_frame_and_how_it_gave_back_our_client_id() {
    let cids: Vec<String> = minted(5).into_iter().map(text_of).collect();
    let mut model = QfexModel {
        ack_echo: Echo::Upper,
        alo_cross: Some(AloCross::Cancelled),
        ..QfexModel::default()
    };
    session(&mut model, 0);
    let (mut tee, tap) = teed();
    // An ACK upper-cased, then our cancel of it answered CANCELLED, exactly: an end.
    let acked = exchange(&mut model, &mut tee, &add_order(&cids[0], "BUY", "99.50"));
    let id = json(text_at(&acked, 0))["order_response"]["order_id"].clone();
    let cancel = format!(r#"{{"type":"cancel_order","params":{{"order_id":{id}}}}}"#);
    exchange(&mut model, &mut tee, &cancel);
    // Our cancel again, which finds no order: NO_SUCH_ORDER, naming no client id.
    exchange(&mut model, &mut tee, &cancel);
    // A crossing post-only refused CANCELLED before any ACK: a reject.
    exchange(&mut model, &mut tee, &add_order(&cids[1], "BUY", "100.10"));
    // One the model answers with an err echo, and one it fills as a taker.
    model.alo_cross = Some(AloCross::Err);
    exchange(&mut model, &mut tee, &add_order(&cids[2], "BUY", "100.10"));
    model.alo_cross = Some(AloCross::Fills);
    exchange(&mut model, &mut tee, &add_order(&cids[3], "BUY", "100.10"));
    let kept = tap.kept();
    let echoes = Echoes::of(&kept);
    let one = |exact, differs, absent| stage_two::Exactness {
        exact,
        differs,
        absent,
        ..stage_two::Exactness::default()
    };
    assert_eq!(echoes.acks, one(0, 1, 0));
    // Our cancel's CANCELLED after the ACK, and the taker's FILLED, which no ACK came before but
    // which ends its order by what it traded, so it is no reject.
    assert_eq!(echoes.ends, one(2, 0, 0));
    // The crossing post-only's CANCELLED before any ACK.
    assert_eq!(echoes.rejects, one(1, 0, 0));
    assert_eq!(echoes.unnamed, one(0, 0, 1));
    assert_eq!(echoes.fills, one(1, 0, 0));
    // The err echo names our client id exactly; it is recorded, and opens nothing.
    assert_eq!(echoes.err_echoes, one(1, 0, 0));
    assert_eq!(echoes.ack_echoes, stage_two::Exactness::default());
    let said = echoes.says();
    assert_eq!(said[0].0, "ACK");
    assert_eq!(
        said[1].0,
        "rejects (a status before its order's ACK, not a fill's or an IOC's end)"
    );
    assert_eq!(
        said[2].0,
        "ends (a status after its order's ACK, or a fill's or an IOC's end wherever it came)"
    );
    assert_eq!(
        said[0].1,
        "1 not byte for byte: ours, in another case or with a JSON escape"
    );
    assert_eq!(said[2].1, "exact, byte for byte (2)");
    assert_eq!(said[3].1, "1 absent or null");
    assert_eq!(said[5].1, "none heard");
    // The gate stays shut on an upper-cased ACK, and says so.
    let gate = EchoGate::of(&kept);
    assert!(!gate.open);
    assert!(gate.says().starts_with("shut: no ACK"), "{}", gate.says());
    // An empty and a foreign client id, each said by its count.
    let mixed = stage_two::Exactness {
        empty: 2,
        foreign: 1,
        ..one(3, 0, 0)
    };
    assert_eq!(
        mixed.says(),
        "3 exact, byte for byte; 2 empty; 1 naming a client id not ours"
    );
}

#[test]
fn an_add_orders_err_and_ack_echoes_are_counted_and_never_open_the_gate() {
    let cid = text_of(minted(1)[0]);
    let (mut tee, tap) = teed();
    let frame = add_order(&cid, "BUY", "99.50");
    sent(&mut tee, &frame);
    heard_echo(&mut tee, "ack", &frame, &cid);
    heard_echo(&mut tee, "err", &frame, &cid);
    let kept = tap.kept();
    let gate = EchoGate::of(&kept);
    assert!(!gate.open);
    let exact = stage_two::Exactness {
        exact: 1,
        ..stage_two::Exactness::default()
    };
    assert_eq!(
        (gate.echoes.ack_echoes, gate.echoes.err_echoes),
        (exact, exact)
    );
    assert_eq!(Statuses::of(&kept).err_echoes, 1);
}

#[test]
fn statuses_name_rejects_ends_and_those_the_adapter_does_not_map() {
    let cids: Vec<String> = minted(6).into_iter().map(text_of).collect();
    let mut model = QfexModel {
        alo_cross: Some(AloCross::Unmapped),
        ..QfexModel::default()
    };
    session(&mut model, 0);
    let (mut tee, tap) = teed();
    let acked = exchange(&mut model, &mut tee, &add_order(&cids[0], "BUY", "99.50"));
    let id = json(text_at(&acked, 0))["order_response"]["order_id"].clone();
    let cancel = format!(r#"{{"type":"cancel_order","params":{{"order_id":{id}}}}}"#);
    exchange(&mut model, &mut tee, &cancel);
    exchange(&mut model, &mut tee, &cancel);
    exchange(&mut model, &mut tee, &add_order(&cids[1], "BUY", "100.10"));
    model.alo_cross = Some(AloCross::Cancelled);
    exchange(&mut model, &mut tee, &add_order(&cids[2], "BUY", "100.10"));
    // One that trades at once, FILLED with no ACK before it, and IOC statuses with none: ends.
    model.alo_cross = Some(AloCross::Fills);
    exchange(&mut model, &mut tee, &add_order(&cids[3], "BUY", "100.10"));
    for (status, cid) in [
        ("IOC_PARTIALLY_FILLED", &cids[4]),
        ("IOC_CANCELLED", &cids[5]),
    ] {
        sent(&mut tee, &add_order(cid, "BUY", "100.10"));
        let ended = format!(
            concat!(
                r#"{{"order_response":{{"order_id":"o-{cid}","status":"{status}","#,
                r#""client_order_id":"{cid}"}}}}"#,
            ),
            cid = cid,
            status = status
        );
        heard(&mut tee, &[Frame::text(ended)]);
    }
    let statuses = Statuses::of(&tap.kept());
    assert_eq!(statuses.rejects, [UNMAPPED, "CANCELLED"]);
    assert_eq!(
        statuses.ends,
        [
            "CANCELLED",
            "FILLED",
            "IOC_PARTIALLY_FILLED",
            "IOC_CANCELLED"
        ]
    );
    assert_eq!(
        stage_two::ENDS,
        ["FILLED", "IOC_PARTIALLY_FILLED", "IOC_CANCELLED"]
    );
    assert_eq!(statuses.unnamed, ["NO_SUCH_ORDER"]);
    assert_eq!(
        statuses.says(),
        "rejects (before the order's ACK, not a fill's or an IOC's end): REJECTED (unmapped by \
         the adapter), CANCELLED; ends (after it, or a fill's or an IOC's end wherever it came): \
         CANCELLED, FILLED, IOC_PARTIALLY_FILLED, IOC_CANCELLED; naming no client id of ours: \
         NO_SUCH_ORDER; err echoes of an add_order: 0"
    );
    assert_eq!(
        Statuses::default().says(),
        "rejects (before the order's ACK, not a fill's or an IOC's end): none; ends (after it, \
         or a fill's or an IOC's end wherever it came): none; naming no client id of ours: none; \
         err echoes of an add_order: 0"
    );
}

#[test]
fn a_rate_limited_order_response_names_no_order_and_is_no_status_or_echo() {
    let (mut tee, tap) = teed();
    let limited = r#"{"order_response":{"symbol":"SYN-USD","status":"RATE_LIMITED"}}"#;
    heard(&mut tee, &[Frame::text(limited)]);
    let kept = tap.kept();
    assert_eq!(Echoes::of(&kept), Echoes::default());
    assert_eq!(Statuses::of(&kept), Statuses::default());
}

#[test]
fn the_characters_exercised_are_named_by_class_and_the_rest_not_measured() {
    let cids: Vec<String> = minted(2).into_iter().map(text_of).collect();
    let mut model = QfexModel::default();
    session(&mut model, 0);
    let (mut tee, tap) = teed();
    // The first ACK echoes exactly; the second id goes out but its ACK is upper-cased.
    exchange(&mut model, &mut tee, &add_order(&cids[0], "BUY", "99.50"));
    model.ack_echo = Echo::Upper;
    exchange(&mut model, &mut tee, &add_order(&cids[1], "BUY", "99.50"));
    let chars = Characters::of(&tap.kept(), &CLIENT_ID_FORMAT);
    assert_eq!(chars.outside, 0);
    let names: Vec<&str> = chars.classes.iter().map(|c| c.name).collect();
    assert_eq!(
        names,
        ["decimal digits", "lowercase letters a to f", "the hyphen"]
    );
    let first: BTreeSet<char> = cids[0].chars().collect();
    let all: BTreeSet<char> = cids.iter().flat_map(|c| c.chars()).collect();
    let alphabet = stage_two::alphabet(&CLIENT_ID_FORMAT);
    for (class, range) in chars.classes.iter().zip(&alphabet) {
        let in_class = |set: &BTreeSet<char>| -> BTreeSet<char> {
            set.iter()
                .copied()
                .filter(|c| range.chars.contains(c))
                .collect()
        };
        // Echoed: the first id's; sent only: the second's but for those; the rest unmeasured.
        let sent = in_class(&all);
        assert_eq!(class.echoed, in_class(&first));
        let sent_only: BTreeSet<char> = sent.difference(&class.echoed).copied().collect();
        assert_eq!(class.sent_only, sent_only);
        let whole: BTreeSet<char> = range.chars.clone().collect();
        let unmeasured: BTreeSet<char> = whole.difference(&sent).copied().collect();
        assert_eq!(class.not_measured, unmeasured);
    }
    assert!(chars.classes[2].echoed.contains(&'-'));
    let said = chars.says();
    assert!(
        said.starts_with("decimal digits: echoed byte for byte by an ACK "),
        "{said}"
    );
    let hyphen = "the hyphen: echoed byte for byte by an ACK -; sent, not shown so none; not \
                  measured none";
    assert!(said.contains(hyphen), "{said}");
    // No id reaches what it says.
    assert!(cids.iter().all(|c| !said.contains(c.as_str())));
    // A run that sent nothing measures nothing, and a character outside the alphabet is counted.
    let (mut tee, tap) = teed();
    let none = Characters::of(&tap.kept(), &CLIENT_ID_FORMAT);
    assert!(none.says().contains("not measured 0-9;"), "{}", none.says());
    sent(&mut tee, &add_order("ab_Z", "BUY", "99.50"));
    let odd = Characters::of(&tap.kept(), &CLIENT_ID_FORMAT);
    assert_eq!(odd.outside, 2);
    assert!(
        odd.says()
            .ends_with("; 2 characters sent outside the format's alphabet")
    );
    // Every format's classes.
    let alnum = |charset| fbc_core::ClientIdFormat::Alnum {
        max_len: 32,
        charset,
    };
    let count = |fmt: &fbc_core::ClientIdFormat| stage_two::alphabet(fmt).len();
    assert_eq!(count(&alnum(fbc_core::Charset::Alphanumeric)), 3);
    assert_eq!(count(&alnum(fbc_core::Charset::LowerAlphanumeric)), 2);
    assert_eq!(
        count(&fbc_core::ClientIdFormat::Numeric { max_digits: 20 }),
        1
    );
    assert_eq!(count(&fbc_core::ClientIdFormat::Hex { len: 30 }), 2);
}

#[test]
fn add_order_as_encoded_is_said_from_what_was_sent_and_acknowledged() {
    let cid = text_of(minted(1)[0]);
    let (mut tee, tap) = teed();
    assert_eq!(AsEncoded::of(&tap.kept()), AsEncoded::NotSent);
    sent(&mut tee, &add_order(&cid, "BUY", "99.50"));
    assert_eq!(AsEncoded::of(&tap.kept()), AsEncoded::NotAcked);
    let mut model = QfexModel::default();
    session(&mut model, 0);
    let acked = model.trade(0, &Frame::text(add_order(&cid, "BUY", "99.50")));
    heard(&mut tee, &acked.unwrap());
    assert_eq!(AsEncoded::of(&tap.kept()), AsEncoded::Accepted);
    let carried =
        add_order(&cid, "BUY", "99.50").replace(r#""GTC","#, r#""GTC","reduce_only":true,"#);
    sent(&mut tee, &carried);
    assert_eq!(AsEncoded::of(&tap.kept()), AsEncoded::Carried);
    for (said, want) in [
        (AsEncoded::NotSent, "not measured: no add_order was sent"),
        (AsEncoded::Carried, "not as encoded: an add_order carried"),
        (
            AsEncoded::Accepted,
            "yes: QFEX acknowledged (ACK) an add_order sent as encoded",
        ),
        (
            AsEncoded::NotAcked,
            "not shown: every add_order went out as encoded",
        ),
    ] {
        assert!(said.says().starts_with(want), "{said:?}");
    }
}

#[test]
fn the_crossing_post_only_is_classified_by_each_answer() {
    let cids: Vec<String> = minted(2).into_iter().map(text_of).collect();
    let through = add_order(&cids[1], "BUY", "100.10");
    let cases = [
        (
            Some(AloCross::Cancelled),
            Crossed::Cancelled { exact: true },
        ),
        (
            Some(AloCross::Unmapped),
            Crossed::Unmapped {
                status: UNMAPPED.to_owned(),
                exact: true,
            },
        ),
        (Some(AloCross::Err), Crossed::ErrEcho(tee::EchoCid::Exact)),
        (
            Some(AloCross::AckedCancelled),
            Crossed::AckedThenEnded("CANCELLED".to_owned()),
        ),
        (Some(AloCross::Fills), Crossed::Filled),
        (None, Crossed::Rested),
    ];
    for (knob, want) in cases {
        let mut model = QfexModel::default();
        session(&mut model, 0);
        let (mut tee, tap) = teed();
        // An earlier session's ACK of another order, before this session's first frame.
        exchange(&mut model, &mut tee, &add_order(&cids[0], "BUY", "99.50"));
        let from = tap.kept().heard.len();
        model.alo_cross = knob;
        let frame = &through;
        sent(&mut tee, frame);
        let answered = model.trade(0, &Frame::text(frame)).unwrap();
        // The err echo names our client id at its span, as the codec names it.
        for answer in &answered {
            let Frame::Text(text) = answer else {
                panic!("text")
            };
            let spans: Vec<std::ops::Range<u32>> = match text.find(&format!("\"{}\"", cids[1])) {
                Some(at) if text.contains("\"Err\"") => {
                    let at = u32::try_from(at + 1).unwrap();
                    std::iter::once(at..at + u32::try_from(cids[1].len()).unwrap()).collect()
                }
                _ => Vec::new(),
            };
            let record = RecordRef::Inbound {
                stamp: Stamp {
                    ingest_seq: 0,
                    kernel_rx: None,
                    recv_mono: MonoNs(2),
                    recv_wall: WallNs(2),
                    conn: ConnKey { conn: 0, epoch: 1 },
                },
                frame: RawFrame::Text(text),
                redact: &spans,
            };
            let _ = tee.record_ref(TrafficClass::Safety, WallNs(2), record);
        }
        let crossed = Crossed::of(&tap.kept(), from, &cids[1], Some(OrdState::Open));
        assert_eq!(crossed, want, "{knob:?}");
    }
    // A mapped status other than CANCELLED before any ACK; then nothing heard of it at all.
    let (mut tee, tap) = teed();
    sent(&mut tee, &through);
    let ended = format!(
        concat!(
            r#"{{"order_response":{{"order_id":"o-1","status":"IOC_CANCELLED","#,
            r#""client_order_id":"{}"}}}}"#,
        ),
        cids[1]
    );
    heard(&mut tee, &[Frame::text(ended)]);
    let kept = tap.kept();
    let ended = Crossed::of(&kept, 0, &cids[1], None);
    let ioc = |exact| Crossed::Ended {
        status: "IOC_CANCELLED".to_owned(),
        exact,
    };
    assert_eq!(ended, ioc(true));
    // Its frames are the session's from `from` on: past them, nothing is heard.
    let past = kept.heard.len();
    let unknown = Crossed::of(&kept, past, &cids[1], Some(OrdState::Unknown));
    assert_eq!(unknown, Crossed::Unknown);
    let unanswered = Crossed::of(&kept, past, &cids[1], Some(OrdState::PendingNew));
    assert_eq!(unanswered, Crossed::Unanswered);
    // A client id the run never sent names no frame.
    assert_eq!(
        Crossed::of(&kept, 0, "never-sent", None),
        Crossed::Unanswered
    );
    drop(kept);
    for (crossed, said) in [
        (
            Crossed::Cancelled { exact: true },
            "refused: an order_response CANCELLED before any ACK, echoing our client id byte for \
             byte",
        ),
        (
            Crossed::Cancelled { exact: false },
            "refused: an order_response CANCELLED before any ACK, naming our client id, not byte \
             for byte (another case or a JSON escape)",
        ),
        (
            Crossed::Unmapped {
                status: UNMAPPED.to_owned(),
                exact: true,
            },
            "refused: an order_response REJECTED before any ACK, a status the adapter does not \
             map (FBC-jdj7), echoing our client id byte for byte",
        ),
        (
            Crossed::Unmapped {
                status: UNMAPPED.to_owned(),
                exact: false,
            },
            "refused: an order_response REJECTED before any ACK, a status the adapter does not \
             map (FBC-jdj7), naming our client id, not byte for byte",
        ),
        (
            ioc(true),
            "ended: an order_response IOC_CANCELLED before any ACK, echoing our client id byte \
             for byte",
        ),
        (
            ioc(false),
            "ended: an order_response IOC_CANCELLED before any ACK, naming our client id, not \
             byte",
        ),
        (
            Crossed::ErrEcho(tee::EchoCid::Exact),
            "refused: an err echo of its add_order echoing our client id byte for byte, its place \
             left Unknown",
        ),
        (
            Crossed::ErrEcho(tee::EchoCid::Differs),
            "refused: an err echo of its add_order naming our client id, not byte for byte \
             (another case or a JSON escape), its place left Unknown",
        ),
        (
            Crossed::ErrEcho(tee::EchoCid::Foreign),
            "refused: an err echo of its add_order naming a client id not ours, its place left \
             Unknown",
        ),
        (
            Crossed::ErrEcho(tee::EchoCid::Absent),
            "refused: an err echo of its add_order naming no client id, its place left Unknown",
        ),
        (
            Crossed::ErrEcho(tee::EchoCid::Unechoed),
            "refused: an err echo of its add_order naming no client id, its place left Unknown",
        ),
        (
            Crossed::ErrUnechoed,
            "not known: an err echoing no message came after its add_order, perhaps its \
             refusal; nothing named it, so its place ended Unknown at its deadline",
        ),
        (
            Crossed::RateLimited,
            "not measured: a provider rate refusal (RateLimited) answered its add_order",
        ),
        (
            Crossed::AckedThenEnded("CANCELLED".to_owned()),
            "refused after an ACK: QFEX acknowledged it (ACK), then an order_response CANCELLED \
             ended it, and it did not fill",
        ),
        (
            Crossed::AckedThenEnded(UNMAPPED.to_owned()),
            "refused after an ACK: QFEX acknowledged it (ACK), then an order_response REJECTED \
             ended it, and it did not fill, a status the adapter does not map (FBC-jdj7)",
        ),
        (
            Crossed::Filled,
            "filled: it crossed and traded, a stop (UnexpectedFill",
        ),
        (
            Crossed::Rested,
            "rested: QFEX acknowledged it (ACK), nothing ended it after, and it did not fill",
        ),
        (
            Crossed::Unknown,
            "Unknown at its deadline: nothing answered it, so it is never resent",
        ),
        (Crossed::Unanswered, "no answer heard"),
    ] {
        assert!(
            crossed.says().starts_with(said),
            "{crossed:?}: {}",
            crossed.says()
        );
    }
}

#[test]
fn a_refusal_of_the_crossing_post_only_says_how_exactly_it_named_our_client_id() {
    let cid = text_of(minted(1)[0]);
    let upper = cid.to_uppercase();
    assert_ne!(upper, cid);
    let frame = add_order(&cid, "BUY", "100.10");
    // CANCELLED, an unmapped status and another mapped one, each naming our client id in
    // another case: ours, not byte for byte.
    for (status, want) in [
        ("CANCELLED", Crossed::Cancelled { exact: false }),
        (
            UNMAPPED,
            Crossed::Unmapped {
                status: UNMAPPED.to_owned(),
                exact: false,
            },
        ),
        (
            "IOC_CANCELLED",
            Crossed::Ended {
                status: "IOC_CANCELLED".to_owned(),
                exact: false,
            },
        ),
    ] {
        let (mut tee, tap) = teed();
        sent(&mut tee, &frame);
        let ended = format!(
            concat!(
                r#"{{"order_response":{{"order_id":"o-1","status":"{status}","#,
                r#""client_order_id":"{upper}"}}}}"#,
            ),
            status = status,
            upper = upper
        );
        heard(&mut tee, &[Frame::text(ended)]);
        assert_eq!(tap.stop(), None, "{status}");
        let crossed = Crossed::of(&tap.kept(), 0, &cid, None);
        assert_eq!(crossed, want, "{status}");
        assert!(crossed.says().contains("not byte for byte"), "{status}");
    }
    // An err echo of the add_order naming our client id upper-cased, or a client id not ours:
    // the crossing's refusal all the same, each said by how it named it.
    for (written, want) in [
        (upper.clone(), tee::EchoCid::Differs),
        ("someone-else".to_owned(), tee::EchoCid::Foreign),
    ] {
        let (mut tee, tap) = teed();
        sent(&mut tee, &frame);
        let echoed = frame.replace(&cid, &written);
        heard_echo(&mut tee, "err", &echoed, &written);
        let kept = tap.kept();
        assert_eq!(
            Crossed::of(&kept, 0, &cid, Some(OrdState::Unknown)),
            Crossed::ErrEcho(want)
        );
        assert_eq!(Statuses::of(&kept).err_echoes, 1);
        let errs = Echoes::of(&kept).err_echoes;
        let counted = match want {
            tee::EchoCid::Differs => errs.differs,
            _ => errs.foreign,
        };
        assert_eq!((errs.total(), counted), (1, 1), "{want:?}");
    }
    let foreign = stage_two::Exactness {
        foreign: 1,
        ..stage_two::Exactness::default()
    };
    assert_eq!(foreign.says(), "1 naming a client id not ours");
    // An err echoing no message after the add_order, and nothing else: perhaps its refusal,
    // said so; it is counted as no echo of an add_order, and listed as what it is.
    let (mut tee, tap) = teed();
    sent(&mut tee, &frame);
    let bare = r#"{"type":"Err","error_code":"InvalidOrder","message":"synthetic"}"#;
    heard(&mut tee, &[Frame::text(bare)]);
    let kept = tap.kept();
    for state in [Some(OrdState::Unknown), None] {
        assert_eq!(Crossed::of(&kept, 0, &cid, state), Crossed::ErrUnechoed);
    }
    assert_eq!(Statuses::of(&kept).err_echoes, 0);
    assert_eq!(
        Echoes::of(&kept).err_echoes,
        stage_two::Exactness::default()
    );
    assert_eq!(places::echoes(&kept, 0), ["err echoing no message"]);
    drop(kept);
    // An err echo of the add_order after it is its refusal before that bare err is.
    heard_echo(&mut tee, "err", &frame, &cid);
    let crossed = Crossed::of(&tap.kept(), 0, &cid, None);
    assert_eq!(crossed, Crossed::ErrEcho(tee::EchoCid::Exact));
    // An err of either kind heard before the add_order was sent answered something else.
    let (mut tee, tap) = teed();
    heard(&mut tee, &[Frame::text(bare)]);
    heard_echo(&mut tee, "err", &frame, &cid);
    let record = Record::Outbound {
        at: MonoNs(3),
        conn: ConnKey { conn: 0, epoch: 1 },
        rpc: None,
        opcode: Opcode::Text,
        frame: WireSlice::redacted(frame.as_bytes().to_vec(), Vec::new()).unwrap(),
    };
    let _ = tee.record_ref(TrafficClass::Safety, WallNs(3), RecordRef::Owned(&record));
    let crossed = Crossed::of(&tap.kept(), 0, &cid, Some(OrdState::Unknown));
    assert_eq!(crossed, Crossed::Unknown);
}

#[test]
fn a_rate_limited_answer_to_the_crossing_post_only_is_not_measured_and_no_refusal() {
    let cid = text_of(minted(1)[0]);
    let frame = add_order(&cid, "BUY", "100.10");
    // A RateLimited err echoing its add_order, naming our client id exactly.
    let (mut tee, tap) = teed();
    sent(&mut tee, &frame);
    heard_err(&mut tee, "err", "RateLimited", &frame, &cid);
    assert!(matches!(tap.stop(), Some(places::Stop::RateRefused { .. })));
    let kept = tap.kept();
    let rate_limited = (kept.heard.iter()).any(|h| {
        matches!(
            h,
            tee::Heard::Echo {
                rate_limited: true,
                ..
            }
        )
    });
    assert!(rate_limited);
    // Not the crossing's refusal, even once it ends Unknown at its deadline, and counted as no
    // err echo of an add_order.
    for state in [Some(OrdState::Unknown), Some(OrdState::PendingNew), None] {
        assert_eq!(Crossed::of(&kept, 0, &cid, state), Crossed::RateLimited);
    }
    assert_eq!(
        Echoes::of(&kept).err_echoes,
        stage_two::Exactness::default()
    );
    assert_eq!(Statuses::of(&kept).err_echoes, 0);
    assert_eq!(
        places::echoes(&kept, 0),
        ["err RateLimited of add_order: exact"]
    );
    drop(kept);
    // Any other err echo of it after the rate refusal is the crossing's refusal.
    heard_echo(&mut tee, "err", &frame, &cid);
    let kept = tap.kept();
    let exact = Crossed::ErrEcho(tee::EchoCid::Exact);
    assert_eq!(Crossed::of(&kept, 0, &cid, None), exact);
    assert_eq!(Echoes::of(&kept).err_echoes.exact, 1);
    assert_eq!(Statuses::of(&kept).err_echoes, 1);
    assert_eq!(places::echoes(&kept, 1), ["err of add_order: exact"]);
    drop(kept);
    // A RATE_LIMITED order_response, which names no order, answering it alone.
    let (mut tee, tap) = teed();
    sent(&mut tee, &frame);
    let limited = r#"{"order_response":{"symbol":"SYN-USD","status":"RATE_LIMITED"}}"#;
    heard(&mut tee, &[Frame::text(limited)]);
    let kept = tap.kept();
    let crossed = Crossed::of(&kept, 0, &cid, Some(OrdState::Unknown));
    assert_eq!(crossed, Crossed::RateLimited);
    // An ACK of it before the rate refusal: QFEX rested it.
    drop(kept);
    let (mut tee, tap) = teed();
    let mut model = QfexModel::default();
    session(&mut model, 0);
    exchange(&mut model, &mut tee, &add_order(&cid, "BUY", "99.50"));
    heard(&mut tee, &[Frame::text(limited)]);
    let crossed = Crossed::of(&tap.kept(), 0, &cid, Some(OrdState::Open));
    assert_eq!(crossed, Crossed::Rested);
}

// ----- X and Y counted ended after the drop (0151 item 5) -----

/// A drop session two whose reads after the drop showed X and Y each absent, untraded and
/// terminal, all three read whole.
fn ended_both() -> places::TwoRun {
    let listing = verdict::Listing {
        x: false,
        y: false,
        flat: true,
    };
    places::TwoRun {
        after_drop: Some(verdict::AfterDrop {
            last: Some(listing),
            ..verdict::AfterDrop::default()
        }),
        after: Some(places::Traded {
            naming: vec![0, 0],
            terminal: vec![Some("CANCELLED".to_owned()), Some("CANCELLED".to_owned())],
            trades_whole: true,
            historic_whole: true,
            ..places::Traded::default()
        }),
        ..places::TwoRun::default()
    }
}

#[test]
fn x_and_y_count_ended_only_on_all_three_reads_after_the_drop() {
    assert_eq!(stage_two::ended_after_drop(&ended_both()), [true, true]);
    type Change = fn(&mut places::TwoRun);
    let cases: [(Change, [bool; 2]); 9] = [
        (|t| t.after_drop = None, [false, false]),
        (|t| t.after = None, [false, false]),
        (
            |t| t.after_drop.as_mut().unwrap().last = None,
            [false, false],
        ),
        (
            |t| t.after_drop.as_mut().unwrap().refused = Some(places::RefusedOn::ReadSocket),
            [false, false],
        ),
        (
            |t| t.after_drop.as_mut().unwrap().last.as_mut().unwrap().y = true,
            [true, false],
        ),
        // A row naming X is a trade on the account: Y's absence no longer rules out its fill.
        (|t| t.after.as_mut().unwrap().naming[0] = 1, [false, false]),
        (
            |t| t.after.as_mut().unwrap().trades_whole = false,
            [false, false],
        ),
        (
            |t| t.after.as_mut().unwrap().terminal[1] = None,
            [true, false],
        ),
        (
            |t| t.after.as_mut().unwrap().historic_whole = false,
            [false, false],
        ),
    ];
    for (n, (change, want)) in cases.into_iter().enumerate() {
        let mut two = ended_both();
        change(&mut two);
        assert_eq!(stage_two::ended_after_drop(&two), want, "case {n}");
    }
}

#[test]
fn x_and_y_count_ended_only_with_no_trade_since_xs_send_and_the_position_flat() {
    // Decision 0132 item 4, as `places::close_verdict` reads X after drop session one: an
    // absence counts as an end, not a fill, only with the trades since X's send read whole with
    // no row at all and the last read's position flat. A row naming the other order, another
    // order or none is something trading on the account, so neither absence rules out a fill.
    type Change = fn(&mut places::TwoRun);
    let cases: [(&str, Change); 4] = [
        ("a row naming Y", |t| {
            let after = t.after.as_mut().unwrap();
            (after.trades, after.naming[1]) = (1, 1);
        }),
        ("a row naming another order or none", |t| {
            t.after.as_mut().unwrap().trades = 1;
        }),
        ("a position not flat", |t| {
            t.after_drop.as_mut().unwrap().last.as_mut().unwrap().flat = false;
        }),
        ("no count of the rows naming each", |t| {
            t.after.as_mut().unwrap().naming.clear();
        }),
    ];
    for (what, change) in cases {
        let mut two = ended_both();
        change(&mut two);
        assert_eq!(stage_two::ended_after_drop(&two), [false, false], "{what}");
    }
    // So no_other_order, which lets a crossing order go once both are shown ended, still counts
    // X and Y resting on the drop registry after a row of another order, and keeps it back
    // (0151 item 5).
    let cids = minted(2);
    let mut reg = Registry::new();
    for cid in &cids {
        holding(&mut reg, *cid, As::Open);
    }
    let judge = |two: &places::TwoRun| {
        let shown = stage_two::ended_after_drop(two);
        let ended: Vec<ClientOrderId> = (cids.iter().zip(shown))
            .filter(|(_, e)| *e)
            .map(|(c, _)| *c)
            .collect();
        let built = [Built {
            account: "primary",
            step: ledger::Step::Drop,
            registry: &reg,
            ended: &ended,
        }];
        stage_two::no_other_order(INST, &built, &[])
    };
    assert_eq!(judge(&ended_both()), Ok(()));
    let mut two = ended_both();
    two.after.as_mut().unwrap().trades = 1;
    let blocked = Blocked::Registry {
        account: "primary",
        step: ledger::Step::Drop,
    };
    assert_eq!(judge(&two), Err(blocked));
}

#[test]
fn no_other_order_refuses_a_count_it_cannot_make_and_an_account_left_unread() {
    let cids = minted(2);
    // Two orders of the most lots there are, one a side: their sum cannot be counted.
    let mut reg = Registry::new();
    for (cid, side) in cids.iter().zip([Side::Buy, Side::Sell]) {
        reg.insert(NewOrder {
            side,
            qty: Lots::new(i64::MAX).unwrap(),
            ..order(*cid, 1)
        })
        .unwrap();
    }
    let built = [Built {
        account: "primary",
        step: ledger::Step::Cross,
        registry: &reg,
        ended: &[],
    }];
    let refused = stage_two::no_other_order(INST, &built, &[]);
    let uncounted = Blocked::Uncounted {
        account: "primary",
        step: ledger::Step::Cross,
    };
    assert_eq!(refused, Err(uncounted.clone()));
    assert!(uncounted.says().contains("could not be counted"));
    let read = clean();
    let reads = [("primary", Some(&read)), ("subaccount", None)];
    let refused = stage_two::no_other_order(INST, &[], &reads);
    let unread = Blocked::Unread {
        account: "subaccount",
    };
    assert_eq!(refused, Err(unread.clone()));
    assert_eq!(
        unread.says(),
        "subaccount has no never-arming read to show it holds no order of ours"
    );
}

#[test]
fn no_other_order_subtracts_only_the_ended_orders_on_its_market() {
    // A drop registry whose X, counted ended, rests on another market, while an order of ours
    // rests on this one: X's lots there mask nothing here.
    let cids = minted(2);
    let mut reg = Registry::new();
    holding_on(&mut reg, cids[0], InstrumentId::new(2), As::Open);
    holding(&mut reg, cids[1], As::Open);
    let ended = [cids[0]];
    let built = [Built {
        account: "primary",
        step: ledger::Step::Drop,
        registry: &reg,
        ended: &ended,
    }];
    let refused = stage_two::no_other_order(INST, &built, &[]);
    let want = Blocked::Registry {
        account: "primary",
        step: ledger::Step::Drop,
    };
    assert_eq!(refused, Err(want));
    // Counted ended on this market, it is subtracted, and the other market's order counts for
    // nothing here.
    let mut reg = Registry::new();
    holding(&mut reg, cids[0], As::Open);
    holding_on(&mut reg, cids[1], InstrumentId::new(2), As::Open);
    let built = [Built {
        account: "primary",
        step: ledger::Step::Drop,
        registry: &reg,
        ended: &ended,
    }];
    assert_eq!(stage_two::no_other_order(INST, &built, &[]), Ok(()));
}

// ----- The session machinery's policy -----

#[test]
fn a_stage_session_cancels_at_every_stop_and_once_it_ran_its_course() {
    let stage = Policy::Stage { drop_passed: false };
    for stop in places::Stop::ALL {
        // Outside the drop test every stop cancels, the owner's Ctrl-C and the span bound among
        // them (decision 0132 item 7, 0148 item 8).
        assert!(stage.cancels(Some(stop), false), "{stop:?}");
    }
    assert!(stage.cancels(None, false));
    // A drop session cancels only at Ctrl-C or once a fill of ours came (0151 item 2).
    for abrupt in [false, true] {
        let drop = Policy::Drop { abrupt };
        assert!(drop.cancels(Some(places::Stop::Interrupted), false));
        assert!(drop.cancels(Some(places::Stop::SpanBound), true));
        assert!(!drop.cancels(Some(places::Stop::SpanBound), false));
        assert!(!drop.cancels(None, true));
    }
}

#[test]
fn held_cancels_end_a_stage_session_by_a_drop_only_after_a_passed_drop_test() {
    // Refused locally or met by a provider rate refusal: 0151 item 11's two causes.
    let held = places::Cancels {
        held: true,
        blocked: true,
        ..places::Cancels::default()
    };
    // Answered other than accepted alone (NO_SUCH_ORDER once the order ended first, say): held,
    // so no comparison follows, but neither of 0151 item 11's causes, so a graceful close.
    let answered = places::Cancels {
        held: true,
        ..places::Cancels::default()
    };
    let seen = places::Cancels {
        ended: true,
        ..places::Cancels::default()
    };
    let stop = Some(places::Stop::Interrupted);
    let passed = Policy::Stage { drop_passed: true };
    let failed = Policy::Stage { drop_passed: false };
    assert_eq!(passed.ending(stop, false, Some(&held)), Ending::Drop);
    assert_eq!(passed.ending(None, false, Some(&held)), Ending::Drop);
    assert_eq!(failed.ending(stop, false, Some(&held)), Ending::Close);
    assert_eq!(passed.ending(stop, false, Some(&seen)), Ending::Close);
    assert_eq!(passed.ending(stop, false, Some(&answered)), Ending::Close);
    assert_eq!(passed.ending(None, false, Some(&answered)), Ending::Close);
    assert_eq!(passed.ending(stop, true, None), Ending::Close);
    // Drop session two that ran its course drops; every other drop session closes.
    let two = Policy::Drop { abrupt: true };
    assert_eq!(two.ending(None, false, None), Ending::Drop);
    assert_eq!(two.ending(stop, false, Some(&held)), Ending::Close);
    assert_eq!(two.ending(None, true, None), Ending::Close);
    let one = Policy::Drop { abrupt: false };
    assert_eq!(one.ending(None, false, None), Ending::Close);
}

#[test]
fn a_stop_cancels_blocked_only_when_refused_locally_or_rate_refused() {
    let accepted = SubmitOutcome::Accepted {
        ack: fbc_core::AckLevel::Final,
    };
    let rejected = SubmitOutcome::Rejected(Reject {
        kind: RejectKind::NotFound,
        venue_code: None,
        raw: "synthetic".into(),
    });
    let not_sent = SubmitOutcome::NotSent(fbc_core::NotSentReason::RateBudget);
    // (answers, a rate refusal) -> (held, blocked)
    let cases = [
        (vec![], false, (false, false)),
        (vec![accepted.clone()], false, (false, false)),
        (
            vec![accepted.clone(), rejected.clone()],
            false,
            (true, false),
        ),
        (vec![SubmitOutcome::Unknown], false, (true, false)),
        (vec![accepted.clone(), not_sent], false, (true, true)),
        (vec![accepted.clone()], true, (true, true)),
        (vec![rejected], true, (true, true)),
    ];
    for (n, (answers, rate, want)) in cases.into_iter().enumerate() {
        let mut c = places::Cancels::default();
        c.answered(&answers, rate);
        assert_eq!((c.held, c.blocked), want, "case {n}");
    }
    // A cancel refused locally: held and blocked, and a later accepted answer clears neither.
    let mut c = places::Cancels::default();
    c.refused_locally();
    assert_eq!((c.held, c.blocked), (true, true));
    c.answered(&[accepted], false);
    assert_eq!((c.held, c.blocked), (true, true));
}

#[tokio::test(start_paused = true)]
async fn a_stage_sessions_cancels_begin_one_rpc_timeout_before_its_span_bound() {
    let now = tokio::time::Instant::now();
    let span_end = now + Duration::from_secs(600);
    let rpc = Duration::from_millis(600);
    assert_eq!(stage::cancels_begin(span_end, rpc, now), span_end - rpc);
    // A bound nearer than one timeout is met at once: before now.
    let near = now + Duration::from_millis(100);
    assert!(stage::cancels_begin(near, rpc, now) <= now);
    // A bound nearer the clock's origin than one timeout, so no instant one timeout before it can
    // be named: at once, now, never the bound itself; or the bound when it came first.
    assert_eq!(stage::cancels_begin(span_end, Duration::MAX, now), now);
    let later = now + Duration::from_secs(1);
    assert_eq!(stage::cancels_begin(now, Duration::MAX, later), now);
}

// ----- Stage two's record and its judgments -----

#[test]
fn a_blocked_crossing_order_is_the_stop_its_cause_names() {
    let (account, step) = ("primary", ledger::Step::Cross);
    for (blocked, stop) in [
        (
            Blocked::Registry { account, step },
            places::Stop::LeftResting,
        ),
        (
            Blocked::Uncounted { account, step },
            places::Stop::LeftResting,
        ),
        (Blocked::Unread { account }, places::Stop::ReadDisagrees),
        (Blocked::Listed { account }, places::Stop::ReadDisagrees),
    ] {
        assert_eq!(blocked.stop(), stop, "{blocked:?}");
    }
}

#[test]
fn a_registry_counts_an_order_resting_while_it_is_open_in_flight_or_unknown() {
    let cids = minted(4);
    let empty = Registry::new();
    assert!(!stage_two::counted_resting(&empty, INST));
    for (n, (stands, counted)) in [
        (As::Open, true),
        (As::Sent, true),
        (As::Unknown, true),
        (As::Refused, false),
    ]
    .into_iter()
    .enumerate()
    {
        let mut reg = Registry::new();
        holding(&mut reg, cids[n], stands);
        assert_eq!(
            stage_two::counted_resting(&reg, INST),
            counted,
            "{stands:?}"
        );
        // On another market it is not on this one.
        assert!(!stage_two::counted_resting(&reg, InstrumentId::new(2)));
    }
    // A count that overflows is counted resting.
    let mut reg = Registry::new();
    for (cid, side) in minted(2).into_iter().zip([Side::Buy, Side::Sell]) {
        let most = NewOrder {
            side,
            qty: Lots::new(i64::MAX).unwrap(),
            ..order(cid, 1)
        };
        reg.insert(most).unwrap();
    }
    assert!(stage_two::counted_resting(&reg, INST));
}

/// A drop test on `account` whose verdict passed when `passed`, or that has none.
fn drop_run(account: &'static str, passed: Option<bool>) -> places::DropRun {
    places::DropRun {
        account,
        verdict: passed.map(|passed| verdict::DropVerdict {
            passed,
            failures: Vec::new(),
            reach: verdict::Reach::Cancelled,
            arm: verdict::ArmCancel::Survived,
            terminal: [None, None],
            refused: None,
        }),
        ..places::DropRun::default()
    }
}

#[test]
fn only_an_account_whose_drop_test_passed_in_this_run_crosses() {
    let named = ["primary", "subaccount"];
    let passed = [
        drop_run("primary", Some(true)),
        drop_run("subaccount", Some(true)),
    ];
    assert_eq!(stage_two::crossers(&passed, &named), named);
    // A failed verdict, none at all, or no drop test of the account in this run: none of them.
    let failed = [
        drop_run("primary", Some(false)),
        drop_run("subaccount", Some(true)),
    ];
    assert_eq!(stage_two::crossers(&failed, &named), ["subaccount"]);
    let unjudged = [
        drop_run("primary", None),
        drop_run("subaccount", Some(true)),
    ];
    assert_eq!(stage_two::crossers(&unjudged, &named), ["subaccount"]);
    let one = [drop_run("primary", Some(true))];
    assert_eq!(stage_two::crossers(&one, &named), ["primary"]);
    assert!(stage_two::crossers(&[], &named).is_empty());
}

#[test]
fn a_client_id_is_written_as_the_codec_writes_it() {
    let cid = minted(1)[0];
    assert_eq!(stage_two::cid_text(cid).unwrap(), text_of(cid));
}

#[test]
fn the_reads_after_a_crossing_session_stop_the_run_on_what_they_show() {
    let rate = places::Stop::RateRefused {
        on: places::RefusedOn::Rest,
    };
    let after = "after the crossing session";
    let unread = "after the crossing session, a named account not read after it";
    for ((refused, pressed, not_flat, resting, unread_too), want) in [
        (
            (Some(rate), true, true, true, true),
            Some((rate, "in the reads after the crossing session")),
        ),
        (
            (None, true, true, true, true),
            Some((
                places::Stop::Interrupted,
                "during the reads after the crossing session",
            )),
        ),
        (
            (None, false, true, true, true),
            Some((places::Stop::NotFlat, after)),
        ),
        (
            (None, false, false, true, true),
            Some((places::Stop::LeftResting, after)),
        ),
        (
            (None, false, false, false, true),
            Some((places::Stop::LeftResting, unread)),
        ),
        ((None, false, false, false, false), None),
    ] {
        let got = stage::cross_read_stop(refused, pressed, not_flat, (resting, unread_too));
        assert_eq!(got, want);
    }
}

#[test]
fn the_reads_after_a_crossing_session_order_an_order_not_ours_after_a_position_and_before_ours() {
    // stage::cross_after_stop: a provider rate refusal, then the owner's Ctrl-C, then a position
    // not flat, then an order or a TWAP not ours (ReadDisagrees), then an order of ours resting
    // or an account not read (LeftResting), each outranking every one after it (decision 0148
    // item 8; FBC-su9i's review).
    let rate = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    let others = (places::Stop::ReadDisagrees, stage::CROSS_OTHERS);
    let after = "after the crossing session";
    let unread = "after the crossing session, a named account not read after it";
    for ((refused, pressed, not_flat, unexpected, resting, not_read), want) in [
        (
            (Some(rate), true, true, true, true, true),
            Some((rate, "in the reads after the crossing session")),
        ),
        (
            (None, true, true, true, true, true),
            Some((
                places::Stop::Interrupted,
                "during the reads after the crossing session",
            )),
        ),
        (
            (Some(rate), false, false, true, false, false),
            Some((rate, "in the reads after the crossing session")),
        ),
        (
            (None, true, false, true, false, false),
            Some((
                places::Stop::Interrupted,
                "during the reads after the crossing session",
            )),
        ),
        (
            (None, false, true, true, true, true),
            Some((places::Stop::NotFlat, after)),
        ),
        ((None, false, false, true, true, true), Some(others)),
        ((None, false, false, true, true, false), Some(others)),
        ((None, false, false, true, false, true), Some(others)),
        ((None, false, false, true, false, false), Some(others)),
        (
            (None, false, false, false, true, false),
            Some((places::Stop::LeftResting, after)),
        ),
        (
            (None, false, false, false, false, true),
            Some((places::Stop::LeftResting, unread)),
        ),
        ((None, false, false, false, false, false), None),
    ] {
        let got =
            stage::cross_after_stop(refused, pressed, not_flat, unexpected, (resting, not_read));
        assert_eq!(
            got, want,
            "{refused:?} {pressed} {not_flat} {unexpected} {resting} {not_read}"
        );
    }
}

#[test]
fn the_reads_after_a_crossing_drop_end_on_what_ends_them_and_fold_without_erasing() {
    // stage::AfterReads::read_again: a round listing an order of ours is read again, unless it
    // also shows what ends the reads.
    let ours = stage::AfterReads {
        resting: true,
        ours_resting: true,
        read: true,
        x_read: true,
        ..stage::AfterReads::default()
    };
    assert!(ours.read_again());
    assert!(!stage::AfterReads::default().read_again());
    let rate = places::Stop::RateRefused {
        on: places::RefusedOn::Rest,
    };
    for ended in [
        stage::AfterReads {
            unexpected: true,
            ..ours
        },
        stage::AfterReads {
            not_flat: true,
            ..ours
        },
        stage::AfterReads {
            unread: true,
            ..ours
        },
        stage::AfterReads {
            pressed: true,
            ..ours
        },
        stage::AfterReads {
            refused: Some(rate),
            ..ours
        },
    ] {
        assert!(!ended.read_again(), "{ended:?}");
    }
    // A judged round replaces what an earlier one showed of our orders: the drop's cancel shown.
    let gone = stage::AfterReads {
        read: true,
        x_read: true,
        ..stage::AfterReads::default()
    };
    let mut after = ours;
    after.fold(gone, true);
    assert_eq!(after, gone);
    // A round that is not judged (late, or an account not read) erases nothing: the round
    // before it stands, with what it showed added.
    let unread = stage::AfterReads {
        unread: true,
        not_flat: true,
        ..stage::AfterReads::default()
    };
    let mut after = ours;
    after.fold(unread, false);
    assert_eq!(
        after,
        stage::AfterReads {
            unread: true,
            not_flat: true,
            ..ours
        }
    );
    // Whatever either showed of a refusal, a press, an order not ours or a position is kept,
    // judged or not.
    let shown = stage::AfterReads {
        refused: Some(rate),
        pressed: true,
        unexpected: true,
        not_flat: true,
        ..ours
    };
    for judged in [true, false] {
        let mut after = shown;
        after.fold(gone, judged);
        assert!(
            after.refused == Some(rate) && after.pressed && after.unexpected && after.not_flat,
            "{judged} {after:?}"
        );
        let mut after = ours;
        after.fold(shown, judged);
        assert!(
            after.refused == Some(rate) && after.pressed && after.unexpected && after.not_flat,
            "{judged} {after:?}"
        );
    }
}

#[test]
fn a_position_after_a_crossing_session_is_said_the_owners_whatever_stop_came_first() {
    // stage_two::position_said (0151 item 9; FBC-su9i's review).
    assert_eq!(
        stage_two::position_said(Some(true), Some("NotFlat")),
        stage_two::CROSS_POSITION
    );
    for stop in [
        Some("ReadDisagrees"),
        Some("RateRefused"),
        Some("Interrupted"),
        None,
    ] {
        assert_eq!(
            stage_two::position_said(Some(true), stop),
            stage_two::CROSS_POSITION_BESIDE
        );
    }
    assert_eq!(stage_two::position_said(Some(false), None), "no");
    assert!(stage_two::position_said(None, None).starts_with("not known"));
    for said in [stage_two::CROSS_POSITION, stage_two::CROSS_POSITION_BESIDE] {
        assert!(said.contains("the position is the owner's (0151 item 9)"));
    }
}

/// A crossing session's record as every branch of the report renders it.
fn cross_run(account: &'static str) -> stage_two::CrossRun {
    stage_two::CrossRun {
        account,
        authenticated: true,
        arm: Some("Accepted".to_owned()),
        resynced: true,
        started: true,
        ..stage_two::CrossRun::default()
    }
}

#[test]
fn stage_twos_report_and_evidence_say_each_crossing_with_no_id_or_value() {
    let cancels = |held, blocked, ended| places::Cancels {
        sent: 1,
        held,
        blocked,
        ended,
        presses: 1,
        ..places::Cancels::default()
    };
    let compared = compare::Compared {
        step: "the crossing post-only",
        reads: vec![(Duration::ZERO, compare::Took::Agreed)],
        first_agreed: Some(Duration::from_millis(5)),
        ended: compare::Ended::Agreed,
        stop: None,
    };
    let crosses = vec![
        // Refused, blocked by nothing, its cancels seen to end, read clean: the next follows.
        stage_two::CrossRun {
            checked: true,
            crossed: Some(Crossed::Cancelled { exact: true }),
            cancels: Some(cancels(false, false, true)),
            compared: vec![compared],
            read: true,
            ended_by: Some(stage::GRACEFUL),
            ..cross_run("primary")
        },
        // Filled, its cancels held by a rate refusal, the span bound's lead, left resting.
        stage_two::CrossRun {
            crossed: Some(Crossed::Filled),
            cancels: Some(cancels(true, true, false)),
            cancels_lead: Some(Duration::from_millis(300)),
            resting: true,
            read: true,
            stop: Some("UnexpectedFill"),
            ..cross_run("subaccount")
        },
        // Kept back, counted resting, its cancels answered otherwise, not read after it.
        stage_two::CrossRun {
            blocked: Some("synthetic reason".to_owned()),
            cancels: Some(cancels(true, false, false)),
            counted_resting: true,
            ..cross_run("primary")
        },
        // Not seen to end in time, its connection's own end, then the run's last.
        stage_two::CrossRun {
            cancels: Some(cancels(false, false, false)),
            read: true,
            ..cross_run("subaccount")
        },
        // Not read after it, and the run's last.
        cross_run("primary"),
    ];
    let two = stage_two::StageTwo {
        gate: Some(true),
        crosses,
        ..stage_two::StageTwo::default()
    };
    let mut report = String::new();
    stage_two::markdown(&mut report, &two);
    for said in [
        "## Stage two (FBC-su9i, decision 0132 item 7)",
        "- The exact-echo gate: open: an ACK in this run echoed",
        "- The characters our client ids exercised, by class of QFEX's client-id format \
         (decision 0132's Consequences; the rest not measured): not read",
        "reduce_only (FBC-blcw): not read",
        "  - ACK: none heard",
        "- How QFEX refused it: refused: an order_response CANCELLED before any ACK",
        &format!("- What followed: {}", stage_two::NEXT_CROSS),
        "every order they named was seen to end; 1 press of Ctrl-C while they ran",
        &format!(
            "filled: it crossed and traded, a stop (UnexpectedFill, 0151 item 9); {}",
            stage_two::CROSS_FILLED
        ),
        "refused locally or met by a provider rate refusal, so not every order was seen to end \
         (0151 item 11)",
        "; the span bound stopped it, its cancels begun 300 ms before the bound",
        stage_two::HAND_CANCEL_CROSS,
        "- What followed: the run's stop, UnexpectedFill: nothing more is placed",
        "- No other order of ours (decision 0148 item 6): kept back: synthetic reason",
        "- No other order of ours (decision 0148 item 6): none: it may be placed",
        "answered other than accepted, so not every order was seen to end",
        "the cross registry still counts an order of ours resting, Unknown and never resent",
        "not known: the account was not read after the session",
        "not every order they named was seen to end in time",
        "- The Stop's cancels: none: the session's connection ended first",
        "- Ended by: it did not open",
        "- How QFEX refused it: not measured: it was not placed",
        &format!("- What followed: {}", stage_two::LAST_CROSS),
        "| the crossing post-only | 1 | 5 ms | agreed |",
    ] {
        assert!(report.contains(said), "{said}: {report}");
    }
    let shut = stage_two::StageTwo {
        gate: Some(false),
        ..stage_two::StageTwo::default()
    };
    let mut report = String::new();
    stage_two::markdown(&mut report, &shut);
    assert!(report.contains(&format!("- The exact-echo gate: {}", EchoGate::shut_said())));
    let json = stage_two::to_json(&two);
    assert_eq!(json["exact_echo_gate"], true);
    assert_eq!(json["characters"], serde_json::Value::Null);
    let crossings = json["crossings"].as_array().unwrap();
    assert_eq!(crossings.len(), 5);
    let cancelled = Crossed::Cancelled { exact: true };
    assert_eq!(crossings[0]["refused"], cancelled.says());
    assert_eq!(crossings[0]["cancels"]["blocked"], false);
    assert_eq!(crossings[1]["cancels_lead_ms"], 300);
    assert_eq!(crossings[1]["hand_cancel"], stage_two::HAND_CANCEL_CROSS);
    assert_eq!(crossings[2]["blocked"], "synthetic reason");
    assert_eq!(crossings[4]["cancels"], serde_json::Value::Null);
}

#[test]
fn stage_two_reads_what_the_whole_runs_frames_show() {
    let cid = text_of(minted(1)[0]);
    let mut model = QfexModel::default();
    session(&mut model, 0);
    let (mut tee, tap) = teed();
    exchange(&mut model, &mut tee, &add_order(&cid, "BUY", "99.50"));
    let mut two = stage_two::StageTwo::default();
    two.read(&tap.kept());
    assert_eq!(two.echoes.acks.exact, 1);
    assert_eq!(two.as_encoded, Some(AsEncoded::Accepted));
    let chars = two.characters.as_ref().unwrap();
    assert_eq!(chars.classes.len(), 3);
    assert_eq!(two.statuses, Statuses::default());
}

#[test]
fn the_crossing_report_says_what_the_reads_and_the_check_showed_not_the_sessions_shape() {
    // FBC-77r8's review, carried: each line comes from what was read or checked. A session that
    // ended before the check for another order of ours says it was not checked, not that none
    // was found; and the reads after the session say whether they listed an order or a TWAP not
    // ours, or that one of them could not be made.
    let crosses = vec![
        // Its arm refused: the session ended before the check, and the reads found nothing.
        stage_two::CrossRun {
            read: true,
            others: Some(false),
            ..cross_run("primary")
        },
        // Checked and placed; the reads after it listed an order not ours.
        stage_two::CrossRun {
            checked: true,
            read: true,
            others: Some(true),
            stop: Some("ReadDisagrees"),
            ..cross_run("subaccount")
        },
        // A named account not read after it.
        stage_two::CrossRun {
            checked: true,
            ..cross_run("primary")
        },
    ];
    let two = stage_two::StageTwo {
        gate: Some(true),
        crosses,
        ..stage_two::StageTwo::default()
    };
    let mut report = String::new();
    stage_two::markdown(&mut report, &two);
    let lines = |n: usize| {
        let heading = "### The crossing post-only on ";
        report.split(heading).nth(n + 1).unwrap().to_owned()
    };
    let other = "- No other order of ours (decision 0148 item 6): ";
    let others = "- An order or a TWAP not ours in the reads after the session: ";
    let first = lines(0);
    assert!(
        first.contains(&format!("{other}not checked: the session ended before it")),
        "{first}"
    );
    assert!(first.contains(&format!("{others}none\n")), "{first}");
    let second = lines(1);
    assert!(
        second.contains(&format!("{other}none: it may be placed")),
        "{second}"
    );
    assert!(
        second.contains(&format!("{others}{}", stage_two::OTHERS_CROSS)),
        "{second}"
    );
    let third = lines(2);
    assert!(
        third.contains(&format!("{others}not known: a named account was not read")),
        "{third}"
    );
    let json = stage_two::to_json(&two);
    let crossings = json["crossings"].as_array().unwrap();
    assert_eq!(crossings[0]["other_order_checked"], false);
    assert_eq!(crossings[0]["others_after"], false);
    assert_eq!(crossings[1]["other_order_checked"], true);
    assert_eq!(crossings[1]["others_after"], true);
    assert!(crossings[2]["others_after"].is_null());
}
