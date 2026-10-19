//! FBC-xx0's done line: three more of design §6's fault scripts, played against fbc-runtime's
//! order-entry session with the conformance toy, every order command built and authorized by
//! fbc-oms and every event the session reports applied to it as a live consumer applies them
//! (`order_entry`; decisions 0005, 0006, 0021, 0025).
//!
//! - **An error frame that names no request.** A venue's error with no id (the toy's `error`
//!   record, as Paradex sends one) arrives while a placement is in flight. It reaches the handler
//!   as `UncorrelatedError` and resolves nothing: the placement stays pending, and its own answer,
//!   arriving later, still accepts it.
//! - **An idle close with a venue code.** The venue closes an order connection with nothing
//!   resting on it with a close code of its own (Paradex's 4031 "no active subscriptions", which
//!   the Java library met on 2026-09-23). The session reconnects through its pacing,
//!   authenticates again, arms and resyncs again, and writes no order frame of the first
//!   connection again.
//! - **A stalled journal writer.** The journal's writer thread is stalled (its drain not yet
//!   spawned, which `fbc-journal` documents as the same as a writer that stopped) until the sink's
//!   Safety reserve is spent. A cancel is still written to the stub though its record is dropped,
//!   every dropped record is counted, and once the writer resumes the next record offered is
//!   preceded by a `Degraded` marker counting them (decision 0006).
//!
//! The clock is paused and moved only by the scenario: past the reconnect pacing after the
//! close, and nowhere else, so no deadline passes and every outcome is an answer's.

mod order_entry;

use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fbc_conformance::Frame;
use fbc_conformance::toy::{self, SYMBOL_A, reject_kind};
use fbc_core::{
    CancelReason, ClientOrderId, ExecEvent, Lots, Reject, SubmitOutcome, TrafficClass, encode_cid,
};
use fbc_journal::{
    CloseRec, Entry, JournalReader, JournalWriter, Marker, QueueSink, Record, RedactionKey,
    SinkConfig, WsControl, journal_queue,
};
use fbc_oms::{OrdState, TerminalKind};
use fbc_runtime::Journal;
use order_entry::{LeaseDir, Played, Script, answer, field, play, play_journaled};

/// The venue id the toy's stub gives every order placed here.
const VID: &str = "toy-1";
/// The shard the journal is written under.
const SHARD: u16 = 3;
/// The close code and reason Paradex closes an idle order connection with.
const IDLE_CODE: u16 = 4031;
const IDLE_REASON: &str = "no active subscriptions";
/// The margin refusal an error with no id carries: one that would end the placement had it
/// been taken as the placement's answer.
const MARGIN: &str = "1005";
/// How long, by the wall clock, a wait on the journal's writer thread lasts before the test
/// fails.
const WATCHDOG: Duration = Duration::from_secs(60);

/// An order update of our buy of 25 at 130 865 as the toy's wire reports it.
fn order(cid: &str, st: &str, seq: u64) -> String {
    format!(
        "order|cid={cid}|vid={VID}|sym={SYMBOL_A}|side=B|st={st}|cum=0|px=130865|qty=25\
         |po=1|ro=0|seq={seq}"
    )
}

/// The acceptance of item 0 of request `rpc`.
fn accepted(rpc: &str) -> String {
    format!("item|rpc={rpc}|i=0|res=ok|vid={VID}")
}

/// An error naming no request.
fn id_less_error(msg: &str) -> String {
    format!("error|code={MARGIN}|msg={msg}")
}

/// The kind of a request the toy's wire carries: the first field of its first record.
fn kind(frame: &Frame) -> &str {
    let Frame::Text(text) = frame else {
        panic!("the toy sends text");
    };
    text.split('|').next().unwrap_or_default()
}

/// The kinds of the requests `frames` carry, in order.
fn kinds(frames: &[Frame]) -> Vec<&str> {
    frames.iter().map(kind).collect()
}

/// `cid` as the toy's wire spells it.
fn wire(cid: ClientOrderId) -> String {
    let caps = toy::caps().exec.unwrap().order;
    encode_cid(&caps.client_id, cid).unwrap().to_string()
}

/// What the stub learnt of the placement it read: its request id and our client id.
#[derive(Clone, Default)]
struct Placed(Arc<Mutex<Option<(String, String)>>>);

impl Placed {
    fn read(&self, frame: &Frame) -> Result<(String, String), String> {
        let read = (field(frame, "rpc")?, field(frame, "cid")?);
        *self.0.lock().unwrap() = Some(read.clone());
        Ok(read)
    }

    fn get(&self) -> Result<(String, String), String> {
        let read = self.0.lock().unwrap().clone();
        read.ok_or_else(|| "no placement read".into())
    }
}

/// A placement answered as accepted and resting.
fn place_rests() -> fbc_conformance::Responder {
    answer("place", |frame| {
        let (rpc, cid) = (field(frame, "rpc")?, field(frame, "cid")?);
        Ok(vec![accepted(&rpc), order(&cid, "open", 1)])
    })
}

/// A cancel answered as accepted, its order then reported cancelled as requested.
fn cancel_ends(seq: u64) -> fbc_conformance::Responder {
    answer("cancel", move |frame| {
        let (rpc, cid) = (field(frame, "rpc")?, field(frame, "cid")?);
        Ok(vec![
            format!("item|rpc={rpc}|i=0|res=ok"),
            order(&cid, "canceled|why=requested", seq),
        ])
    })
}

fn redaction_key() -> Arc<RedactionKey> {
    // Synthetic: of no account and no venue.
    Arc::new(RedactionKey::new(&[7; 32]).unwrap())
}

/// Every entry of the journal under `root`.
fn read_back(root: &Path) -> Vec<Entry> {
    let reader = JournalReader::open(root, SHARD).unwrap();
    reader.entries().collect::<Result<_, _>>().unwrap()
}

/// Whether `entry` is an outbound frame of the toy's request kind `kind`.
fn outbound(entry: &Entry, kind: &str) -> bool {
    let Record::Outbound { frame, .. } = &entry.record else {
        return false;
    };
    frame.bytes().starts_with(format!("{kind}|").as_bytes())
}

/// Every drop the sink counted, both classes.
fn drops(sink: &QueueSink) -> u64 {
    sink.dropped(TrafficClass::Normal) + sink.dropped(TrafficClass::Safety)
}

// ---------------------------------------------------------------------------------------------
// An error frame that names no request.
// ---------------------------------------------------------------------------------------------

/// What the registry and the glue held of the placement once the error had been handled, and
/// before its answer.
struct AfterError {
    cid: ClientOrderId,
    state: OrdState,
    outcomes: usize,
    uncorrelated: Vec<Reject>,
}

#[test]
fn an_id_less_error_frame_reaches_the_handler_as_uncorrelated_and_resolves_no_request() {
    // The placement is read and its answer held back; an error naming no request is pushed in
    // its place. Our cancel (by client id, the toy cancelling before an acknowledgement) is
    // then answered with the placement's acceptance, its open update, the cancel's acceptance
    // and the CANCELED.
    let placed = Placed::default();
    let read = placed.clone();
    let place = answer("place", move |frame| {
        read.read(frame)?;
        Ok(vec![id_less_error("insufficient margin")])
    });
    let cancel = answer("cancel", move |frame| {
        let (place_rpc, cid) = placed.get()?;
        let rpc = field(frame, "rpc")?;
        Ok(vec![
            accepted(&place_rpc),
            order(&cid, "open", 1),
            format!("item|rpc={rpc}|i=0|res=ok"),
            order(&cid, "canceled|why=requested", 2),
        ])
    });
    let script = Script::default().reply(place).reply(cancel);
    let p: Played<AfterError> = play(script, async |run| {
        run.started().await;
        let (cid, place_rpc) = run.place();
        run.replied().await;
        let after = {
            let st = run.st.borrow();
            let outcomes = st.events.iter();
            let outcomes = outcomes.filter(|e| e.answers() == Some(place_rpc));
            let uncorrelated = st.events.iter().filter_map(|e| match e {
                ExecEvent::UncorrelatedError(r) => Some(r.clone()),
                _ => None,
            });
            AfterError {
                cid,
                state: st.reg.get(cid).unwrap().state(),
                outcomes: outcomes.count(),
                uncorrelated: uncorrelated.collect(),
            }
        };
        run.cancel(cid);
        run.replied().await;
        after
    });
    let after = p.out;
    let margin = Reject {
        kind: reject_kind(MARGIN),
        venue_code: Some(MARGIN.into()),
        raw: "insufficient margin".into(),
    };
    // The error reached the handler as uncorrelated, and was decoded (no decode error).
    assert_eq!(after.uncorrelated, [margin]);
    assert_eq!(p.counters.decode_errors, 0);
    // It resolved no request: the placement is still pending, and nothing answered it.
    assert_eq!(after.state, OrdState::PendingNew);
    assert_eq!(after.outcomes, 0);
    // Its own answer, arriving after the error, still accepted it, and our cancel ended it.
    let accepted = p.st.events.iter().any(|e| {
        matches!(
            e,
            ExecEvent::Outcome {
                outcome: SubmitOutcome::Accepted { .. },
                ..
            }
        )
    });
    assert!(accepted, "{:?}", p.st.events);
    let rec = p.st.reg.get(after.cid).unwrap();
    let canceled = OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested));
    assert_eq!(rec.state(), canceled);
    assert_eq!(rec.resting(), Lots::ZERO);
    // The placement and the cancel, each written once.
    assert_eq!(
        kinds(&p.received),
        ["auth", "cod", "resync", "place", "cancel"]
    );
}

// ---------------------------------------------------------------------------------------------
// An idle close with a venue code.
// ---------------------------------------------------------------------------------------------

#[test]
fn an_idle_close_with_a_venue_code_reconnects_authenticates_resyncs_and_writes_no_order_again() {
    // On the first connection a placement rests and our cancel ends it, so nothing rests and the
    // connection is idle; the stub then closes it with Paradex's idle code. The second
    // connection's opening is answered as the first's was: authenticated, armed, resynced flat.
    let script = Script::default()
        .reply(place_rests())
        .reply(cancel_ends(2))
        .close_with(IDLE_CODE, IDLE_REASON)
        .reopen();
    let dir = LeaseDir::new();
    let (queue, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 1 << 20,
            soft_limit_pct: 85,
        },
        redaction_key(),
    )
    .unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&dir.0, SHARD, redaction_key()).unwrap())
        .unwrap();
    let sink = Rc::new(RefCell::new(queue));
    let journal = Journal::new(Rc::clone(&sink));
    let p = play_journaled(script, Some(journal), async |run| {
        run.started().await;
        let (cid, _) = run.place();
        run.replied().await;
        run.cancel(cid);
        run.replied().await;
        // The close ends the first epoch; the session reconnects once the pacing allows.
        run.reopened(1).await;
        cid
    });
    drop(Rc::into_inner(sink).unwrap().into_inner());
    writer.close().unwrap();
    let cid = p.out;
    // Two connections: the first carried the order frames, the second only its opening, with a
    // new authentication and resync; no order frame of the first was written again.
    assert_eq!(p.conns.len(), 2);
    assert_eq!(
        kinds(&p.conns[0].received),
        ["auth", "cod", "resync", "place", "cancel"]
    );
    assert_eq!(kinds(&p.conns[1].received), ["auth", "cod", "resync"]);
    let carries = |f: &&Frame| matches!(f, Frame::Text(t) if t.contains(&wire(cid)));
    assert_eq!(p.received.iter().filter(carries).count(), 2);
    assert_eq!(p.st.authenticated, 2);
    assert_eq!(p.st.resynced, 2);
    // The close ended the first epoch, and the stop the second.
    assert_eq!(p.st.ended.len(), 2);
    assert_ne!(p.st.ended[0], p.st.ended[1]);
    assert_eq!(p.counters.attempts, 2);
    assert_eq!(p.counters.arm_failures, 0);
    // The order ended, and stayed ended across the reconnect.
    let canceled = OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested));
    assert_eq!(p.st.reg.get(cid).unwrap().state(), canceled);
    // The session heard the venue's code: its close frame is journaled with it.
    let entries = read_back(&dir.0);
    let closes: Vec<&CloseRec> = entries
        .iter()
        .filter_map(|e| match &e.record {
            Record::InboundControl {
                frame: WsControl::Close(Some(close)),
                ..
            } => Some(close),
            _ => None,
        })
        .collect();
    assert_eq!(closes.len(), 1);
    assert_eq!(closes[0].code, IDLE_CODE);
}

// ---------------------------------------------------------------------------------------------
// A stalled journal writer.
// ---------------------------------------------------------------------------------------------

/// How many id-less errors the stub pushes while the writer is stalled: their records, each a
/// Safety record, take far more than the whole queue, so the reserve is spent by them.
const FILLERS: usize = 200;

/// The drops counted at each point of the stalled run.
struct Stalled {
    /// Before the cancel: Normal, Safety.
    before_cancel: (u64, u64),
    /// Once the cancel was written: Normal, Safety.
    after_cancel: (u64, u64),
    /// Once the resumed writer had emptied the queue, before anything more was offered.
    at_resume: u64,
}

#[test]
fn a_stalled_journal_writer_spends_the_reserve_a_cancel_still_goes_out_and_degraded_is_marked() {
    // The queue: 8 KiB, the soft limit half of it, so the reserve is 4 KiB; the writer stalled
    // from the start. A placement rests; the stub pushes FILLERS errors naming no request, every
    // one journaled under Safety, until the reserve is spent; our cancel is read and left
    // unanswered. Once the writer resumes and has emptied the queue, a second placement is
    // answered: the first record it offers finds room again.
    let script = Script::default()
        .reply(place_rests())
        .push((0..FILLERS).map(|n| id_less_error(&format!("f{n}"))))
        .read()
        .reply(place_rests());
    let dir = LeaseDir::new();
    let (queue, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 8 << 10,
            soft_limit_pct: 50,
        },
        redaction_key(),
    )
    .unwrap();
    let sink = Rc::new(RefCell::new(queue));
    let journal = Journal::new(Rc::clone(&sink));
    let held = Rc::clone(&sink);
    let root = dir.0.clone();
    let mut writer = None;
    let resumed = &mut writer;
    let p = play_journaled(script, Some(journal), async move |run| {
        let counted = |s: &QueueSink| {
            let n = s.dropped(TrafficClass::Normal);
            (n, s.dropped(TrafficClass::Safety))
        };
        run.started().await;
        let (first, _) = run.place();
        run.replied().await;
        // The fillers: every one handled before the cancel is submitted.
        run.replied().await;
        let before_cancel = counted(&held.borrow());
        run.cancel(first);
        // The stub has read the cancel.
        run.replied().await;
        let after_cancel = counted(&held.borrow());
        // The writer resumes, and empties the queue before anything more is offered.
        let thread = drain
            .spawn(JournalWriter::create(&root, SHARD, redaction_key()).unwrap())
            .unwrap();
        *resumed = Some(thread);
        let give_up = Instant::now() + WATCHDOG;
        while held.borrow().queued_bytes() > 0 {
            assert!(
                Instant::now() < give_up,
                "the writer never emptied the queue"
            );
            tokio::task::yield_now().await;
        }
        let at_resume = drops(&held.borrow());
        run.place();
        run.replied().await;
        Stalled {
            before_cancel,
            after_cancel,
            at_resume,
        }
    });
    let sink = Rc::into_inner(sink).unwrap().into_inner();
    let total = drops(&sink);
    drop(sink);
    writer.expect("resumed").close().unwrap();
    let s = p.out;
    // The reserve was spent before the cancel: Safety records were being dropped, the Normal
    // ones long before them.
    assert!(s.before_cancel.0 > 0, "no Normal record dropped");
    assert!(s.before_cancel.1 > 0, "the reserve was never spent");
    // The cancel's own records were dropped and counted, Safety among them, and the cancel was
    // still written to the stub.
    assert!(
        s.after_cancel.1 > s.before_cancel.1,
        "the cancel found room"
    );
    let cancels = p.conns[0].received.iter().filter(|f| kind(f) == "cancel");
    assert_eq!(cancels.count(), 1);
    assert_eq!(
        kinds(&p.received),
        ["auth", "cod", "resync", "place", "cancel", "place"]
    );
    // Nothing was dropped once the writer had resumed.
    assert_eq!(total, s.at_resume);
    // The journal: one Degraded marker, counting every drop, written after every record kept
    // while the writer was stalled and before the second placement's frame. The cancel's frame
    // is not in it.
    let entries = read_back(&dir.0);
    let degraded: Vec<(usize, u64)> = entries
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match e.record {
            Record::Marker(Marker::Degraded { dropped, .. }) => Some((i, dropped)),
            _ => None,
        })
        .collect();
    let [(at, dropped)] = degraded[..] else {
        panic!("expected one Degraded marker, got {degraded:?}");
    };
    assert_eq!(dropped, total);
    let places: Vec<usize> = (0..entries.len())
        .filter(|&i| outbound(&entries[i], "place"))
        .collect();
    assert_eq!(places.len(), 2, "both placements journaled");
    assert!(places[0] < at && at < places[1], "{at} {places:?}");
    assert!(!entries.iter().any(|e| outbound(e, "cancel")));
}
