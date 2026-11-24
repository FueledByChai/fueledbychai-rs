//! `fbc_live::link::LinkHandler` hands each of a session handler's five calls to its `Link` and
//! then calls the consumer's wake, once per call, with the link no longer borrowed (decision 0155
//! item 2). The same calls on `Link` itself, for a consumer's own handler, note the same.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use fbc_core::{
    ConnKey, ConnState, Envelope, ExchTsKind, ExecEvent, MonoNs, NotSentReason, RpcId, Stamp,
    StreamId, SubmitHandle, WallNs,
};
use fbc_live::link::{ENDED_UNSAID, Link, LinkHandler, Note};
use fbc_oms::{FillLedger, LadderConfig, LedgerConfig, Registry};
use fbc_runtime::ExecHandler;

/// A link over a fresh registry under the conformance toy's order caps.
fn link() -> Link {
    let caps = fbc_conformance::toy::caps().exec.unwrap().order;
    let ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1_000,
        },
        WallNs(0),
    )
    .unwrap();
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::from_secs(1),
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    Link::new(Registry::new(), ledger, caps, ladder)
}

const KEY: ConnKey = ConnKey { conn: 0, epoch: 0 };

/// The connection opening, the shard's first input, received at 100 ns.
fn opened() -> Envelope<ExecEvent> {
    Envelope {
        stamp: Stamp {
            ingest_seq: 0,
            kernel_rx: None,
            recv_mono: MonoNs(100),
            recv_wall: WallNs(100),
            conn: KEY,
        },
        exch_ts: None,
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: None,
        body: ExecEvent::Conn {
            stream: StreamId(0),
            state: ConnState::Open,
        },
    }
}

/// Request 7, which the session reports not sent.
fn not_sent() -> SubmitHandle {
    SubmitHandle {
        rpc: RpcId(7),
        receipt: Err(NotSentReason::Disconnected),
    }
}

/// Whether `note` is what the `n`th of the five calls, in the tests' order, notes.
fn expected(n: usize, note: &Note) -> bool {
    match (n, note) {
        (0, Note::Conn(ConnState::Open)) => true,
        (1, Note::EpochEnd { key, why }) => *key == KEY && why == ENDED_UNSAID,
        (2, Note::Submitted { rpc, sent }) => {
            *rpc == RpcId(7) && *sent == Err(NotSentReason::Disconnected)
        }
        (3, Note::Unprotected { told, rpcs, left }) => {
            told.is_empty() && rpcs.is_empty() && left.is_empty()
        }
        (4, Note::Problem(what)) => what.contains("could not decode"),
        _ => false,
    }
}

#[test]
fn the_handler_hands_each_call_to_the_link_then_wakes_the_consumer_once() {
    let link = Rc::new(RefCell::new(link()));
    // Each wake records how many notes the link held then, borrowing it: the handler has let
    // it go before it wakes.
    let woken = Rc::new(RefCell::new(Vec::new()));
    let mut h = LinkHandler {
        link: Rc::clone(&link),
        wake: Box::new({
            let (link, woken) = (Rc::clone(&link), Rc::clone(&woken));
            move || woken.borrow_mut().push(link.borrow_mut().notes().len())
        }),
    };
    h.on_exec(opened());
    assert_eq!(*woken.borrow(), [1]);
    h.on_epoch_end(KEY);
    assert_eq!(*woken.borrow(), [1, 2]);
    h.on_submitted(not_sent());
    assert_eq!(*woken.borrow(), [1, 2, 3]);
    h.on_unprotected(&[]);
    assert_eq!(*woken.borrow(), [1, 2, 3, 4]);
    h.on_decode_error(KEY);
    assert_eq!(*woken.borrow(), [1, 2, 3, 4, 5]);

    let link = link.borrow();
    assert_eq!(link.notes().len(), 5);
    for (n, note) in link.notes().iter().enumerate() {
        assert!(expected(n, note), "note {n}: {note:?}");
    }
    // The epoch's end was reported to the registry at its last envelope.
    assert_eq!(link.registry().order_entry_epoch_end(), Some(MonoNs(100)));
}

#[test]
fn the_links_own_entry_points_note_what_the_handlers_calls_note() {
    let mut link = link();
    link.on_exec(opened());
    link.on_epoch_end(KEY);
    link.on_submitted(not_sent());
    link.on_unprotected(&[]);
    link.on_decode_error(KEY);
    assert_eq!(link.notes().len(), 5);
    for (n, note) in link.notes().iter().enumerate() {
        assert!(expected(n, note), "note {n}: {note:?}");
    }
    assert_eq!(link.registry().order_entry_epoch_end(), Some(MonoNs(100)));
}
