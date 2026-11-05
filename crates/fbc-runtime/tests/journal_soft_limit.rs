//! FBC-8pm's done line (decision 0139, the owner's answer to OQ-journal-cancel-only): the
//! journal filled to its soft limit during an order-entry session moves every Quoting market of
//! the shard to Cancel-only, the move journaled from the reserve, and only the owner's Start,
//! Flatten or Wind-down leaves it; a market in the owner's Exit stays in Exit, its reducing
//! orders still written (decision 0006); cancels are still written.
//!
//! The shard's journal is a `QueueSink` whose writer is not running yet, as behind a stalled
//! disk, so it fills; it is given the `CancelOnlyTrip` of each of the shard's three registries:
//! two Quoting on the conformance toy's market, the third long on it and in the owner's Flatten.
//! The consumer fills the journal to its soft limit with its own Normal records, which it offers
//! to the sink itself (decision 0101), so the runtime has not seen the journal reach it and
//! nothing moves. Then a place, an amend, a batch and a cancel, all built and authorized while
//! Quoting, and a reduce-only sell built and authorized in Exit, are submitted: the place's
//! context is the first record of the runtime's that the full journal drops, which moves both
//! Quoting registries' markets to Cancel-only and journals `Marker::CancelOnly` under Safety,
//! from the reserve. The place, already past its check, is written (the journal never blocks,
//! decision 0006); the amend and the batch are refused at their encode,
//! `NotSent(StaleAuthorization)`, nothing reserved or written; the cancel and the Exit's
//! reducing sell are written, and the Exit market stays in Exit, building reducing orders. In
//! Cancel-only no place is built and a cancel is built and written; the kill switch put on and
//! lifted leaves the market Cancel-only, and the owner's Start returns it to Quoting while the
//! other registry stays Cancel-only; with the journal still at its soft limit nothing moves
//! again, so a place built after the Start is written. Read back, the journal holds the marker
//! once, between the place's nonce and the cancel's, the cancels' and the reducing sell's
//! frames, no dropped Safety record, and the gap's `Degraded` marker last.
//!
//! Every order command is one fbc-oms built and authorized through `Registry::authorize`
//! (`armed_oms`). The venue is the conformance toy.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use armed_oms::{Oms, Reserved, churn, order_caps, session_config, settle, watermark};
use common::{Peer, ScriptedWs};
use exec_toy::INST_A;
use fbc_core::{
    Channel, Envelope, ExecEvent, Lots, MonoNs, NewOrder, NotSentReason, OrderKind, RpcId, Side,
    SubmitHandle, Ticks, Tif, TrafficClass, WallNs,
};
use fbc_journal::{
    JournalReader, JournalSink, JournalWriter, Marker, Opaque, QueueSink, Record, Recorded,
    RedactionKey, SinkConfig, journal_queue,
};
use fbc_oms::{CancelChoice, EntryState, ExitKind, Leases, OmsError, StateRefusal};
use fbc_runtime::{ExecHandler, ExecOrders, Journal};

type Handles = Rc<RefCell<Vec<SubmitHandle>>>;
type Events = Rc<RefCell<Vec<ExecEvent>>>;

/// Keeps each submission's handle and the events it hears.
struct Keep {
    handles: Handles,
    events: Events,
}

impl ExecHandler for Keep {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.events.borrow_mut().push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.handles.borrow_mut().push(handle);
    }
}

fn redaction_key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[3; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Answers the session's login, its cancel-on-disconnect arm (request 1) and its resync on
/// `peer`, then waits until the epoch takes places.
async fn armed_and_resynced(peer: &mut Peer, orders: &ExecOrders, events: &Events) {
    assert!(peer.recv().await.starts_with("auth|ts="));
    peer.send("auth|ok=1|token=toy-session-token");
    assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
    let wm = watermark(&peer.recv().await);
    peer.send("item|rpc=1|i=0|res=ok");
    peer.send(&format!("rsbegin|wm={wm}"));
    peer.send("rsend");
    settle(|| events.borrow().contains(&ExecEvent::ResyncEnd)).await;
    assert!(orders.may_place());
}

/// The consumer's own annotation of `len` bytes, which it offers the sink itself.
fn annotation(len: usize) -> Record {
    Record::Consumer {
        at: MonoNs(1),
        kind: 9,
        input: false,
        ingest_seq: None,
        bytes: Opaque(vec![0x5a; len]),
    }
}

/// Fills the journal to its soft limit with the consumer's Normal annotations, large ones and
/// then empty ones, each until the sink drops one, so that no Normal record fits under the soft
/// limit any more: how many it dropped.
fn fill(sink: &RefCell<QueueSink>) -> u64 {
    let mut dropped = 0;
    for len in [512, 0] {
        loop {
            let offered =
                sink.borrow_mut()
                    .record(TrafficClass::Normal, WallNs(1), &annotation(len));
            if offered == Recorded::DroppedCounted {
                dropped += 1;
                break;
            }
        }
    }
    dropped
}

/// A reduce-only sell of five lots well over the market: it reduces a long position.
fn reducing_ask() -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(131_200) },
        qty: Lots::new(5).unwrap(),
        post_only: false,
        reduce_only: true,
        ..bid()
    }
}

/// The rpc a frame names, for one that starts with `kind|rpc=`.
fn rpc_of(line: &str, kind: &str) -> RpcId {
    let rest = line
        .strip_prefix(&format!("{kind}|rpc="))
        .unwrap_or_else(|| panic!("not a {kind}: {line}"));
    RpcId(rest.split('|').next().unwrap().parse().unwrap())
}

/// A post-only buy of one lot well under the market.
fn bid() -> NewOrder {
    NewOrder {
        cid: armed_oms::cid(),
        inst: INST_A,
        side: Side::Buy,
        kind: OrderKind::Limit { px: Ticks(130_800) },
        qty: Lots::new(1).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

#[tokio::test(start_paused = true)]
async fn the_journals_soft_limit_moves_every_quoting_market_of_the_shard_to_cancel_only_journaled_from_the_reserve_leaving_exit_and_cancels_written()
 {
    // A blocked thread keeps the paused clock from auto-advancing while the sockets are idle.
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let mut server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let (handles, events) = (Handles::default(), Events::default());
    let keep = Keep {
        handles: Rc::clone(&handles),
        events: Rc::clone(&events),
    };

    // The shard's journal, its writer not started: half of 16 KiB for Normal records, the rest
    // the Safety reserve.
    let config = SinkConfig {
        budget_bytes: 16 * 1024,
        soft_limit_pct: 50,
    };
    let (queue, drain) = journal_queue(config, redaction_key()).unwrap();
    let sink = Rc::new(RefCell::new(queue));
    let journal = Journal::new(Rc::clone(&sink));
    // The shard's three registries, each on the toy's market for an account of its own, all
    // given to the shard's journal: two Quoting, and one long 20 lots and in the owner's
    // Flatten.
    let mut oms = Oms::armed();
    let other = Oms::armed();
    let mut exiting = Oms::armed_holding(20);
    let exit = (true, EntryState::Exit(ExitKind::Flatten));
    let flattened = exiting.reg.flatten(INST_A, Leases::none()).unwrap();
    assert_eq!((flattened.armed(), flattened.state()), exit);
    for reg in [&oms.reg, &other.reg, &exiting.reg] {
        journal.cancel_only_at_soft_limit(reg.cancel_only_trip());
    }

    let config = session_config(&server.url(), &reserved);
    let (mut session, control) = fbc_runtime::ExecSession::new(config, keep).unwrap();
    session.set_journal(journal.clone());
    let orders = session.orders();
    let watch = Rc::clone(&handles);
    let fill_sink = Rc::clone(&sink);
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &orders, &events).await;
        let quoting = |e: fbc_oms::MarketEntry| (e.armed(), e.state());
        assert_eq!(quoting(oms.reg.entry(INST_A)), (true, EntryState::Quoting));
        assert_eq!(fill_sink.borrow().dropped(TrafficClass::Normal), 0);

        // Built and authorized while Quoting, and in Exit.
        let (place, amend, batch, cancel) = (oms.place(), oms.amend(), oms.batch(2), oms.cancel());
        let reduce = exiting.reg.place(reducing_ask()).unwrap();
        let reduce = exiting.reg.authorize(armed_oms::ACCT, reduce).unwrap();

        // The consumer fills the journal to its soft limit with records of its own, which the
        // runtime does not see: nothing has moved yet.
        assert!(fill(&fill_sink) >= 2);
        assert_eq!(quoting(oms.reg.entry(INST_A)), (true, EntryState::Quoting));
        assert_eq!(
            quoting(other.reg.entry(INST_A)),
            (true, EntryState::Quoting)
        );

        let submitted = [
            orders.submit(place).unwrap(),
            orders.submit(amend).unwrap(),
            orders.submit(batch).unwrap(),
            orders.submit(cancel).unwrap(),
            orders.submit(reduce).unwrap(),
        ];
        // The place's context is the first record of the runtime's the full journal drops: it
        // moves the shard, and the place, past its check already, is written. The amend and the
        // batch built before the move are not; the cancel and the Exit's reducing sell are.
        assert_eq!(rpc_of(&peer.recv().await, "place"), submitted[0]);
        assert_eq!(rpc_of(&peer.recv().await, "cancel"), submitted[3]);
        assert_eq!(rpc_of(&peer.recv().await, "place"), submitted[4]);
        settle(|| watch.borrow().len() == 5).await;

        // Every Quoting market of the shard is Cancel-only, armed as it was; the owner's exit
        // is still Exit, and builds its reducing orders.
        for reg in [&oms.reg, &other.reg] {
            assert_eq!(quoting(reg.entry(INST_A)), (true, EntryState::CancelOnly));
        }
        assert_eq!(quoting(exiting.reg.entry(INST_A)), exit);
        assert!(exiting.reg.place(reducing_ask()).is_ok());
        assert_eq!(quoting(exiting.reg.entry(INST_A)), exit);
        // In Cancel-only no place is built, and a cancel is, and is written.
        assert_eq!(
            oms.reg.place(bid()),
            Err(OmsError::State(StateRefusal::CancelOnly(INST_A)))
        );
        let CancelChoice::Send(cmd) = oms
            .reg
            .cancellable(oms.amendable)
            .unwrap()
            .cancel(&order_caps())
        else {
            panic!("a cancel of an acknowledged order is sent");
        };
        let cancelled = orders
            .submit(oms.reg.authorize(armed_oms::ACCT, cmd).unwrap())
            .unwrap();
        assert_eq!(rpc_of(&peer.recv().await, "cancel"), cancelled);

        // The kill switch put on and lifted leaves it Cancel-only; only the owner's Start
        // leaves it, and only on the market it is pressed for.
        assert_eq!(quoting(oms.reg.kill(INST_A)), (true, EntryState::Killed));
        assert_eq!(
            quoting(oms.reg.lift_kill(INST_A)),
            (true, EntryState::CancelOnly)
        );
        assert_eq!(
            quoting(oms.reg.start(INST_A, Leases::none()).unwrap()),
            (true, EntryState::Quoting)
        );
        assert_eq!(
            quoting(other.reg.entry(INST_A)),
            (true, EntryState::CancelOnly)
        );
        // The journal is still at its soft limit, which moves nothing again: a place built
        // after the owner's Start is written.
        let placed = orders.submit(oms.place()).unwrap();
        assert_eq!(rpc_of(&peer.recv().await, "place"), placed);
        assert_eq!(quoting(oms.reg.entry(INST_A)), (true, EntryState::Quoting));

        settle(|| watch.borrow().len() == 7).await;
        churn().await;
        assert!(peer.quiet());
        drop(control);
        (submitted, cancelled, placed)
    };
    let (run, (submitted, cancelled, placed)) = tokio::join!(session.run(), script);
    run.unwrap();
    drop(thaw);

    let handles = handles.borrow();
    let rpcs: Vec<RpcId> = handles.iter().map(|h| h.rpc).collect();
    assert_eq!(
        rpcs,
        [
            submitted[0],
            submitted[1],
            submitted[2],
            submitted[3],
            submitted[4],
            cancelled,
            placed
        ]
    );
    let stale = Err(NotSentReason::StaleAuthorization);
    assert!(handles[0].receipt.is_ok(), "{:?}", handles[0]);
    assert_eq!(handles[1].receipt, stale);
    assert_eq!(handles[2].receipt, stale);
    for sent in &handles[3..] {
        assert!(sent.receipt.is_ok(), "{sent:?}");
    }
    // The arm's nonce, the first place's, the first cancel's, the reducing sell's, the second
    // cancel's and the last place's: none for the amend and the batch.
    assert_eq!(
        *reserved.lock().unwrap(),
        [vec![0], vec![1], vec![2], vec![3], vec![4], vec![5]]
    );

    // No Safety record was dropped: the move, the cancels and the reducing sell were kept from
    // the reserve.
    assert_eq!(sink.borrow().dropped(TrafficClass::Safety), 0);
    assert!(sink.borrow().dropped(TrafficClass::Normal) >= 3);
    drop(journal);
    drop(session);
    let root = fresh_dir("journal_soft_limit");
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, redaction_key()).unwrap())
        .unwrap();
    drop(sink);
    writer.close().unwrap();
    let read: Vec<Record> = JournalReader::open(&root, 1)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let at = |want: &dyn Fn(&Record) -> bool| {
        let found: Vec<usize> = (0..read.len()).filter(|&i| want(&read[i])).collect();
        assert_eq!(found.len(), 1, "{read:#?}");
        found[0]
    };
    let marker = at(&|r| matches!(r, Record::Marker(Marker::CancelOnly)));
    let nonce =
        |value| move |r: &Record| matches!(r, Record::Nonce { value: v, .. } if *v == value);
    // After the first place's nonce, which the reserve kept, and before the cancel's.
    assert!(at(&nonce(1)) < marker);
    assert!(marker < at(&nonce(2)));
    // The cancels' and the reducing sell's frames were kept; the places' were dropped.
    let outbound = |rpc: RpcId| move |r: &Record| matches!(r, Record::Outbound { rpc: Some(x), .. } if *x == rpc);
    for kept in [submitted[3], submitted[4], cancelled] {
        assert!(marker < at(&outbound(kept)), "{kept:?}");
    }
    for dropped in [submitted[0], placed] {
        assert!(!read.iter().any(outbound(dropped)), "{dropped:?}");
    }
    // The gap the soft limit opened is still open at the end: its marker is the last record.
    assert!(
        matches!(read.last(), Some(Record::Marker(Marker::Degraded { .. }))),
        "{:?}",
        read.last()
    );
}
