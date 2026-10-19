//! FBC-ct0's done line (decisions 0002, 0023, 0057, 0060, 0100): one shard runs the toy
//! market-data venue and the conformance toy's order entry on the shard host. One decide pass
//! submits a ladder of fbc-oms authorizations; reference frames that arrive while it goes out
//! are handed to the market-data handler between two of its commands' encodes; a cancel the
//! handler then submits is written before the ladder's remaining commands; a kill switch the
//! handler turns on there instead leaves every remaining place and amend unwritten
//! (`NotSent(StaleAuthorization)`) while the ladder's cancels still go out; and one `Cycle`
//! record is journaled per decide pass, after every input stamped before it reached the
//! handlers.
//!
//! The reference frames are written from the shard's own thread during the decide pass, on a
//! socket the test holds, after the pass drained every input; a byte then sent through a second
//! loopback connection and read back tells the test they have reached the market-data session's
//! socket (loopback delivers in order), so the host's first poll between two commands reads
//! them. The assertions hold wherever between two commands they are read.

mod armed_oms;
mod common;
#[path = "../../fbc-conformance/src/toy/mod.rs"]
#[allow(dead_code, unused_imports)]
mod exec_toy;

use std::cell::RefCell;
use std::io::{Read, Write};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use armed_oms::{Oms, Reserved, churn, session_config, settle, watermark};
use common::toy::{self, ToyVenue};
use common::{Peer, ScriptedWs};
use exec_toy::INST_A;
use fbc_core::{
    AccountKey, EndpointPlan, Envelope, ExecEvent, InstrumentId, MdEvent, MdTransport,
    NotSentReason, RpcId, SubmitHandle, TrafficClass, VenueConfig, WallNs, WireUrl,
};
use fbc_journal::{JournalSink, Record, Recorded};
use fbc_oms::ControlCommand;
use fbc_runtime::{
    Changed, Connector, Decide, ExecHandler, ExecOrders, ExecSession, ExecSessionError,
    IngestClock, Journal, Ladder, Liveness, MdSession, MdSessionConfig, MdVenue, MdVenueConfig,
    Member, ProxyConfig, ReconnectPacing, ShardError, ShardHost, Unsubmitted, WriteStall,
};

/// The price of the toy's trade the market-data server sends with its upgrade answer.
const FIRST_PX: i64 = 100;
/// The prices of the reference trades written during the ladder's decide pass; the handler acts
/// on the first.
const REFERENCE_PX: [i64; 2] = [101, 102];

/// What the handler does on the first reference trade.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum OnReference {
    /// Submits a cancel of the resting `V-1` through the session's `ExecOrders`.
    Cancel,
    /// Turns the kill switch on for the toy's market.
    Kill,
}

/// A command of the ladder, by kind.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Kind {
    Place,
    Amend,
    Cancel,
}

/// What reached the consumer, in order.
#[derive(Clone, Eq, PartialEq, Debug)]
enum Step {
    /// A trade handed to the market-data handler.
    Trade { seq: u64, px: i64 },
    /// An order-entry event handed to its handler.
    Exec { seq: u64 },
    /// A decide pass, with the instruments it was given.
    Decided { instruments: Vec<InstrumentId> },
    /// What became of a submitted command.
    Submitted {
        rpc: RpcId,
        receipt: Result<(), NotSentReason>,
    },
}

/// The consumer's state, shared by its handlers and its decide pass.
struct World {
    steps: Vec<Step>,
    events: Vec<ExecEvent>,
    oms: Oms,
    orders: ExecOrders,
    changed: Changed,
    on_reference: OnReference,
    /// The request id of the cancel the handler submitted, once it has.
    handler_cancel: Option<RpcId>,
    /// Whether the ladder was decided.
    laddered: bool,
}

type Shared = Rc<RefCell<World>>;

/// The order-entry handler: every event and every submission's fate, in order.
struct ExecLog(Shared);

impl ExecHandler for ExecLog {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let mut world = self.0.borrow_mut();
        world.steps.push(Step::Exec {
            seq: env.stamp.ingest_seq,
        });
        world.events.push(env.body);
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        let receipt = handle.receipt.map(|_| ());
        let rpc = handle.rpc;
        let step = Step::Submitted { rpc, receipt };
        self.0.borrow_mut().steps.push(step);
    }
}

/// A journal sink that keeps the `Cycle` records it is offered, with their class.
#[derive(Default)]
struct Cycles(Vec<(TrafficClass, u64, Vec<InstrumentId>)>);

impl JournalSink for Cycles {
    fn record(&mut self, class: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        if let Record::Cycle {
            last_ingest_seq,
            instruments,
        } = record
        {
            self.0.push((class, *last_ingest_seq, instruments.clone()));
        }
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::Ok
    }
}

/// An unmasked text frame, as a server sends one.
fn text_frame(text: &str) -> Vec<u8> {
    let len = u8::try_from(text.len()).unwrap();
    assert!(len < 126);
    let mut frame = vec![0x81, len];
    frame.extend_from_slice(text.as_bytes());
    frame
}

/// The toy's trade of instrument `A` at `px`.
fn trade(px: i64, seq: i64) -> String {
    format!("trade|sym=A|px={px}|qty=1|seq={seq}")
}

/// A market-data server for one connection on a 127.0.0.1 ephemeral port: it answers the
/// upgrade by hand, with the trade at [`FIRST_PX`] in the same write, then leaves the socket in
/// the returned slot as a blocking stream, so the test writes what else it sends from the
/// shard's own thread. Its URL and the slot.
async fn md_server() -> (String, Arc<Mutex<Option<std::net::TcpStream>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/md", listener.local_addr().unwrap());
    let slot = Arc::new(Mutex::new(None));
    let filled = Arc::clone(&slot);
    tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        tcp.set_nodelay(true).unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            tcp.read_exact(&mut byte).await.unwrap();
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        let key = head.lines().find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("sec-websocket-key")
                .then(|| value.trim().to_owned())
        });
        let accept = derive_accept_key(key.unwrap().as_bytes());
        let mut reply = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\n\
             Upgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        .into_bytes();
        reply.extend(text_frame(&trade(FIRST_PX, 1)));
        tcp.write_all(&reply).await.unwrap();
        let std = tcp.into_std().unwrap();
        std.set_nonblocking(false).unwrap();
        *filled.lock().unwrap() = Some(std);
    });
    (url, slot)
}

/// The two ends of a loopback connection: a byte written on one and read back on the other
/// tells the writer that what it wrote before on another loopback socket has been delivered
/// too, since loopback delivers in order.
struct Delivered {
    tx: std::net::TcpStream,
    rx: std::net::TcpStream,
}

impl Delivered {
    fn new() -> Delivered {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let tx = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        tx.set_nodelay(true).unwrap();
        let (rx, _) = listener.accept().unwrap();
        Delivered { tx, rx }
    }

    /// Returns once what was written on loopback before it has been delivered.
    fn pass(&mut self) {
        self.tx.write_all(&[1]).unwrap();
        let mut byte = [0u8; 1];
        self.rx.read_exact(&mut byte).unwrap();
    }
}

/// The market-data session of the toy at `url`, wanting trades on `A`, stamping from `clock`.
fn md_config(url: &str, clock: &IngestClock) -> MdSessionConfig {
    let venue = ToyVenue::leak();
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: EndpointPlan {
            stream: toy::STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(url),
            },
            subs: vec![toy::sub(1)],
        },
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap(),
        clock: clock.clone(),
        http_max_body: 1024,
        conn: 3,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

/// Answers the session's login, its cancel-on-disconnect arm (request 1) and its resync on
/// `peer`, then waits until the epoch takes places.
async fn armed_and_resynced(peer: &mut Peer, world: &Shared) {
    assert!(peer.recv().await.starts_with("auth|ts="));
    peer.send("auth|ok=1|token=toy-session-token");
    assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
    let wm = watermark(&peer.recv().await);
    peer.send("item|rpc=1|i=0|res=ok");
    peer.send(&format!("rsbegin|wm={wm}"));
    peer.send("rsend");
    settle(|| world.borrow().events.contains(&ExecEvent::ResyncEnd)).await;
}

/// The request id a frame the order-entry peer heard names (`<kind>|rpc=<n>|...`).
fn rpc_of(frame: &str) -> RpcId {
    let first = frame.lines().next().unwrap();
    let field = first.split('|').find_map(|f| f.strip_prefix("rpc="));
    RpcId(field.unwrap().parse().unwrap())
}

/// What one shard run showed: every step in order, the `Cycle` records journaled, the frames
/// the order-entry peer heard after the epoch took places, the passes the host counted and the
/// cancel the handler submitted.
struct Shown {
    steps: Vec<Step>,
    cycles: Vec<(TrafficClass, u64, Vec<InstrumentId>)>,
    heard: Vec<String>,
    passes: u64,
    handler_cancel: Option<RpcId>,
}

/// Runs the shard: the toy's market data and the conformance toy's order entry on one host.
/// The first pass that sees the epoch take places and the first trade decides `plan`, built
/// and authorized by fbc-oms, and writes the reference trades; the market-data handler does
/// `on_reference` on the first of them.
async fn shard(plan: &[Kind], on_reference: OnReference) -> Shown {
    // A blocked thread keeps the paused clock from auto-advancing while the sockets are idle.
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let clock = IngestClock::new();
    let mut host = ShardHost::new(clock.clone());
    let cycles = Rc::new(RefCell::new(Cycles::default()));
    host.set_journal(Journal::new(Rc::clone(&cycles)));

    let mut server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let mut exec_config = session_config(&server.url(), &reserved);
    exec_config.clock = clock.clone();
    // The session is built with a handler that needs the world, which needs its orders: the
    // world is filled in once the session exists.
    let slot: Rc<RefCell<Option<Shared>>> = Rc::default();
    let (md_url, md_socket) = md_server().await;

    struct Late(Rc<RefCell<Option<Shared>>>);
    impl ExecHandler for Late {
        fn on_exec(&mut self, env: Envelope<ExecEvent>) {
            let world = self.0.borrow().clone().unwrap();
            ExecLog(world).on_exec(env);
        }
        fn on_submitted(&mut self, handle: SubmitHandle) {
            let world = self.0.borrow().clone().unwrap();
            ExecLog(world).on_submitted(handle);
        }
    }
    let (mut exec, exec_control) = ExecSession::new(exec_config, Late(Rc::clone(&slot))).unwrap();
    let world: Shared = Rc::new(RefCell::new(World {
        steps: Vec::new(),
        events: Vec::new(),
        oms: Oms::armed(),
        orders: exec.orders(),
        changed: host.changed(),
        on_reference,
        handler_cancel: None,
        laddered: false,
    }));
    *slot.borrow_mut() = Some(Rc::clone(&world));

    let md_world = Rc::clone(&world);
    let md_handler = move |env: Envelope<MdEvent>| {
        let MdEvent::Trade { inst, px, .. } = env.body else {
            return;
        };
        let mut world = md_world.borrow_mut();
        world.changed.mark(inst);
        let seq = env.stamp.ingest_seq;
        world.steps.push(Step::Trade { seq, px: px.0 });
        if px.0 == REFERENCE_PX[0] {
            match world.on_reference {
                OnReference::Cancel => {
                    let cancel = world.oms.cancel();
                    let rpc = world.orders.submit(cancel).unwrap();
                    world.handler_cancel = Some(rpc);
                }
                OnReference::Kill => {
                    world.oms.reg.kill(INST_A);
                }
            }
        }
    };
    let (mut md, md_control) = MdSession::new(md_config(&md_url, &clock), md_handler).unwrap();

    let mut delivered = Delivered::new();
    let decide_world = Rc::clone(&world);
    // The commands reported in all: the ladder's, and the handler's cancel when it submits one.
    let expected = plan.len() + usize::from(on_reference == OnReference::Cancel);
    let plan = plan.to_vec();
    let mut decide = move |instruments: &[InstrumentId], ladder: &mut Ladder| {
        let mut world = decide_world.borrow_mut();
        let instruments = instruments.to_vec();
        world.steps.push(Step::Decided { instruments });
        let traded = world
            .steps
            .iter()
            .any(|s| matches!(s, Step::Trade { px, .. } if *px == FIRST_PX));
        if world.laddered || !traded || !world.orders.may_place() {
            return;
        }
        world.laddered = true;
        for kind in &plan {
            let auth = match kind {
                Kind::Place => world.oms.place(),
                Kind::Amend => world.oms.amend(),
                Kind::Cancel => world.oms.cancel(),
            };
            let orders = world.orders.clone();
            ladder.push(&orders, auth).unwrap();
        }
        assert_eq!(ladder.len(), plan.len());
        // The reference trades arrive now, after this pass drained every input.
        let mut socket = md_socket.lock().unwrap();
        let socket = socket.as_mut().unwrap();
        let mut frames = text_frame(&trade(REFERENCE_PX[0], 2));
        frames.extend(text_frame(&trade(REFERENCE_PX[1], 3)));
        socket.write_all(&frames).unwrap();
        delivered.pass();
    };

    let watch = Rc::clone(&world);
    let script = async move {
        let mut peer = server.accept().await;
        armed_and_resynced(&mut peer, &watch).await;
        // Every command reported, then what was written read off the socket.
        let reported = || {
            let world = watch.borrow();
            let submitted = |s: &&Step| matches!(s, Step::Submitted { .. });
            world.laddered && world.steps.iter().filter(submitted).count() == expected
        };
        settle(reported).await;
        let sent = watch
            .borrow()
            .steps
            .iter()
            .filter(|s| {
                matches!(
                    s,
                    Step::Submitted {
                        receipt: Ok(()),
                        ..
                    }
                )
            })
            .count();
        let mut heard = Vec::new();
        for _ in 0..sent {
            heard.push(peer.recv().await);
        }
        churn().await;
        assert!(peer.quiet());
        drop((md_control, exec_control));
        heard
    };
    let members = vec![Member::md(&mut md), Member::exec(&mut exec)];
    let (ran, heard) = tokio::join!(host.run(members, &mut decide), script);
    ran.unwrap();
    drop(thaw);
    let world = world.borrow();
    Shown {
        steps: world.steps.clone(),
        cycles: cycles.borrow().0.clone(),
        heard,
        passes: host.passes(),
        handler_cancel: world.handler_cancel,
    }
}

/// Each decide pass has its `Cycle` record, under Normal, with the instruments it was given;
/// every input stamped before a pass reached the handlers before it, and none after it was.
fn one_cycle_per_pass(shown: &Shown) {
    let passes: Vec<_> = shown
        .steps
        .iter()
        .filter_map(|s| match s {
            Step::Decided { instruments } => Some(instruments.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(passes.len(), shown.cycles.len());
    assert_eq!(u64::try_from(passes.len()).unwrap(), shown.passes);
    for ((class, _, instruments), decided) in shown.cycles.iter().zip(&passes) {
        assert_eq!(*class, TrafficClass::Normal);
        assert_eq!(instruments, decided);
    }
    // The pass the ladder came from was given the first trade's instrument.
    assert!(passes.iter().any(|p| p == &[InstrumentId::new(1)]));
    let mut pass = 0;
    for step in &shown.steps {
        let seq = match step {
            Step::Decided { .. } => {
                pass += 1;
                continue;
            }
            Step::Trade { seq, .. } | Step::Exec { seq } => *seq,
            Step::Submitted { .. } => continue,
        };
        // Every input before pass `pass` is covered by its Cycle; none after an earlier one is.
        if let Some((_, last, _)) = shown.cycles.get(pass) {
            assert!(
                seq <= *last,
                "input {seq} after the pass covering up to {last}"
            );
        }
        if pass > 0 {
            let (_, last, _) = &shown.cycles[pass - 1];
            assert!(seq > *last, "input {seq} handed after the pass covering it");
        }
    }
}

/// A command's request id and what became of it.
type Reported = (RpcId, Result<(), NotSentReason>);

/// The request ids the ladder's commands were reported under, in order (the handler's cancel
/// left out), and how many of them were reported before the first reference trade reached the
/// handler.
fn ladder_around_reference(shown: &Shown) -> (Vec<Reported>, usize) {
    let mut ladder = Vec::new();
    let mut before = None;
    for step in &shown.steps {
        match step {
            Step::Submitted { rpc, receipt } if Some(*rpc) != shown.handler_cancel => {
                ladder.push((*rpc, *receipt));
            }
            Step::Trade { px, .. } if *px == REFERENCE_PX[0] => before = Some(ladder.len()),
            _ => {}
        }
    }
    (
        ladder,
        before.expect("the reference trade reached the handler"),
    )
}

/// Both reference trades reached the handler between the ladder's `k`th command and its next.
fn references_between(shown: &Shown, k: usize) {
    let mut ladder = 0;
    for step in &shown.steps {
        match step {
            Step::Submitted { rpc, .. } if Some(*rpc) != shown.handler_cancel => ladder += 1,
            Step::Trade { px, .. } if REFERENCE_PX.contains(px) => assert_eq!(ladder, k),
            _ => {}
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_cancel_the_handler_submits_on_reference_frames_read_between_two_ladder_commands_is_written_before_the_rest_of_the_ladder()
 {
    let plan = [Kind::Place, Kind::Amend, Kind::Place, Kind::Place];
    let shown = shard(&plan, OnReference::Cancel).await;
    one_cycle_per_pass(&shown);
    let (ladder, k) = ladder_around_reference(&shown);
    assert_eq!(ladder.len(), plan.len());
    // Read between two of the ladder's encodes: after its first at the earliest, before its
    // last.
    assert!(k >= 1 && k < plan.len(), "{k}");
    references_between(&shown, k);
    let cancel = shown.handler_cancel.unwrap();
    // The handler's cancel is the next command encoded, ahead of the ladder's next.
    let after: Vec<RpcId> = shown
        .steps
        .iter()
        .skip_while(|s| !matches!(s, Step::Trade { px, .. } if *px == REFERENCE_PX[0]))
        .filter_map(|s| match s {
            Step::Submitted { rpc, .. } => Some(*rpc),
            _ => None,
        })
        .collect();
    assert_eq!(after[..2], [cancel, ladder[k].0]);
    // Everything was sent, and written in that order.
    assert!(ladder.iter().all(|(_, receipt)| receipt.is_ok()));
    let mut order: Vec<RpcId> = ladder[..k].iter().map(|(rpc, _)| *rpc).collect();
    order.push(cancel);
    order.extend(ladder[k..].iter().map(|(rpc, _)| *rpc));
    let heard: Vec<RpcId> = shown.heard.iter().map(|f| rpc_of(f)).collect();
    assert_eq!(heard, order);
    assert!(shown.heard[k].starts_with("cancel|"), "{}", shown.heard[k]);
}

#[tokio::test(start_paused = true)]
async fn a_kill_switch_the_handler_turns_on_between_two_ladder_commands_leaves_every_remaining_place_and_amend_unwritten_while_the_cancels_go_out()
 {
    let plan = [
        Kind::Place,
        Kind::Amend,
        Kind::Place,
        Kind::Cancel,
        Kind::Place,
    ];
    let shown = shard(&plan, OnReference::Kill).await;
    one_cycle_per_pass(&shown);
    let (ladder, k) = ladder_around_reference(&shown);
    assert_eq!(ladder.len(), plan.len());
    // Read between two of the ladder's encodes, with its cancel still to come.
    assert!((1..=3).contains(&k), "{k}");
    references_between(&shown, k);
    for (j, ((_, receipt), kind)) in ladder.iter().zip(&plan).enumerate() {
        let want = match (j < k, kind) {
            (true, _) | (false, Kind::Cancel) => Ok(()),
            (false, Kind::Place | Kind::Amend) => Err(NotSentReason::StaleAuthorization),
        };
        assert_eq!(*receipt, want, "command {j}, a {kind:?}");
    }
    // Written: the commands before the kill switch, then only the ladder's cancel.
    let mut written: Vec<RpcId> = ladder[..k].iter().map(|(rpc, _)| *rpc).collect();
    written.push(ladder[3].0);
    let heard: Vec<RpcId> = shown.heard.iter().map(|f| rpc_of(f)).collect();
    assert_eq!(heard, written);
    assert!(shown.heard.last().unwrap().starts_with("cancel|"));
}

/// A member's error ends the run at once, the other members' runs dropped; a run with no
/// member, or whose members all ended, returns at once.
#[tokio::test]
async fn a_members_error_ends_the_run_at_once_and_a_run_ends_once_every_member_has() {
    let clock = IngestClock::new();
    let mut host = ShardHost::new(clock.clone());
    let mut decide = |_: &[InstrumentId], _: &mut Ladder| panic!("no pass");
    host.run(Vec::new(), &mut decide).await.unwrap();

    // An order-entry session that has run once refuses to run again: an error.
    let server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let (mut exec, control) =
        ExecSession::new(session_config(&server.url(), &reserved), |_| {}).unwrap();
    drop(control);
    exec.run().await.unwrap();
    // A market-data session that would otherwise run until its control drops.
    let md_server = ScriptedWs::start().await;
    let (mut md, _md_control) =
        MdSession::new(md_config(&md_server.url(), &clock), |_| {}).unwrap();
    let members = vec![Member::md(&mut md), Member::exec(&mut exec)];
    let ran = host.run(members, &mut decide).await;
    assert_eq!(ran, Err(ShardError::Exec(ExecSessionError::Ended)));
    assert_eq!(host.passes(), 0);
}

/// What a decide pass of the hand-back test decided and was handed back.
struct HandBack {
    passes: Rc<RefCell<Vec<Vec<InstrumentId>>>>,
    unsubmitted: Rc<RefCell<Vec<Unsubmitted>>>,
    oms: Oms,
    /// The orders of a session that has run and ended.
    ended: ExecOrders,
    /// The orders of a session that never runs: what is submitted to it stays there.
    unrun: ExecOrders,
}

impl Decide for HandBack {
    fn decide(&mut self, instruments: &[InstrumentId], ladder: &mut Ladder) {
        self.passes.borrow_mut().push(instruments.to_vec());
        if instruments.is_empty() {
            return;
        }
        ladder.push_control(&self.ended, ControlCommand::FeeQuery);
        ladder.push(&self.unrun, self.oms.place()).unwrap();
        ladder.push_control(&self.unrun, ControlCommand::RefreshDeadMan);
        assert_eq!(ladder.len(), 3);
    }

    fn unsubmitted(&mut self, command: Unsubmitted) {
        self.unsubmitted.borrow_mut().push(command);
    }
}

/// A ladder command whose session has ended when its turn comes, and every command the run's
/// end leaves, are handed back unsubmitted, an authorization unspent, and counted; an
/// authorization for another account than the session's is never planned.
#[tokio::test]
async fn a_ladder_command_whose_session_ended_or_that_the_runs_end_left_is_handed_back_and_another_accounts_is_never_planned()
 {
    let clock = IngestClock::new();
    let mut host = ShardHost::new(clock.clone());
    let changed = host.changed();

    // An order-entry session that has run and ended: it takes no more commands.
    let server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let mut config = session_config(&server.url(), &reserved);
    config.acct = AccountKey::new(9);
    let (mut ended, control) = ExecSession::new(config, |_| {}).unwrap();
    drop(control);
    ended.run().await.unwrap();
    // One that is never run, for the account the registry authorizes for.
    let (unrun, _unrun_control) =
        ExecSession::new(session_config(&server.url(), &reserved), |_| {}).unwrap();

    // Another account's authorization is handed back, nothing planned.
    let mut oms = Oms::armed();
    let mut ladder = Ladder::default();
    let refused = ladder.push(&ended.orders(), oms.place());
    assert!(refused.is_err_and(|auth| auth.account() == armed_oms::ACCT));
    assert!(ladder.is_empty());

    // A venue's market data, whose trades call for passes.
    let mut md_server = ScriptedWs::start().await;
    let mut cfg = VenueConfig::new();
    cfg.insert("toy.url.0", &md_server.url());
    let venue = ToyVenue::leak();
    let venue_config = MdVenueConfig {
        venue,
        cfg,
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(
            Duration::from_millis(10),
            Duration::from_millis(100),
            100,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap(),
        clock: clock.clone(),
        http_max_body: 1024,
        conns: 10..20,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), Duration::from_millis(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    };
    let marks = changed.clone();
    let handler = move |env: Envelope<MdEvent>| {
        if let MdEvent::Trade { inst, .. } = env.body {
            marks.mark(inst);
        }
    };
    let (mut md, md_control) = MdVenue::new(venue_config, handler).unwrap();
    md_control.set_desired(vec![toy::sub(1)]).unwrap();

    let mut decide = HandBack {
        passes: Rc::default(),
        unsubmitted: Rc::default(),
        oms,
        ended: ended.orders(),
        unrun: unrun.orders(),
    };
    let (watch, back) = (Rc::clone(&decide.passes), Rc::clone(&decide.unsubmitted));
    let script = async move {
        let mut peer = md_server.accept().await;
        assert!(peer.recv().await.starts_with("hello|"));
        peer.send(&trade(FIRST_PX, 1));
        // The pass decides, the ended session's command is handed back at once and the next is
        // submitted to the session that never runs, which keeps it: the run waits on it.
        let decided = || !back.borrow().is_empty();
        for _ in 0..10_000 {
            if decided() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(decided());
        assert!(watch.borrow().contains(&vec![InstrumentId::new(1)]));
        assert_eq!(back.borrow().len(), 1);
        // Every member ends: the rest of the ladder goes back, and so does the command the
        // session never took, withdrawn from it (Codex P1 r4309119897 on PR #164).
        drop(md_control);
        peer
    };
    let members = vec![Member::md_venue(&mut md)];
    let (ran, _peer) = tokio::join!(host.run(members, &mut decide), script);
    ran.unwrap();
    let back = decide.unsubmitted.borrow();
    assert!(
        matches!(
            back[..],
            [
                Unsubmitted::Control(ControlCommand::FeeQuery),
                Unsubmitted::Authorized(_),
                Unsubmitted::Control(ControlCommand::RefreshDeadMan)
            ]
        ),
        "{back:?}"
    );
    assert_eq!(host.unsubmitted(), 3);
    assert_eq!(
        u64::try_from(decide.passes.borrow().len()).unwrap(),
        host.passes()
    );
}

/// Codex P1 r4309119903 on PR #164: the host's drain and its poll between two commands yield to
/// the runtime so that its I/O driver is polled before the host resumes. tokio is pinned exactly
/// (decision 0019), and its `yield_now` hands the task's waker to the scheduler, which wakes it
/// only once it has run out of ready tasks and polled the driver; this pins that, so a tokio
/// that stops doing it fails here rather than leaving a delivered frame unread between commands.
#[tokio::test]
async fn one_yield_to_the_runtime_polls_the_io_driver_before_the_task_resumes() {
    use std::io::ErrorKind::WouldBlock;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    peer.set_nodelay(true).unwrap();
    let (stream, _) = listener.accept().await.unwrap();
    let mut buf = [0u8; 8];
    // Nothing to read: tokio holds the stream not readable.
    assert_eq!(stream.try_read(&mut buf).unwrap_err().kind(), WouldBlock);
    let mut delivered = Delivered::new();
    peer.write_all(b"frame").unwrap();
    delivered.pass();
    // Delivered to the socket, but unseen until the driver is polled.
    assert_eq!(stream.try_read(&mut buf).unwrap_err().kind(), WouldBlock);
    tokio::task::yield_now().await;
    assert_eq!(stream.try_read(&mut buf).unwrap(), 5);
}

/// A nonce source that reserves none once `short` is set, and counts up from 0 until then.
struct ShortOnce {
    next: u64,
    short: Arc<std::sync::atomic::AtomicBool>,
}

impl fbc_core::NonceSource for ShortOnce {
    fn reserve(&mut self, len: u16) -> fbc_core::NonceBlock {
        if self.short.load(std::sync::atomic::Ordering::Relaxed) {
            return fbc_core::NonceBlock::EMPTY;
        }
        let block = fbc_core::NonceBlock::consecutive(self.next, len).unwrap();
        self.next += u64::from(len);
        block
    }
}

/// A decide pass that, once its session takes places, plans one place and makes the session's
/// nonce source short, keeping what it is handed back.
struct ShortPlace {
    orders: ExecOrders,
    oms: Oms,
    short: Arc<std::sync::atomic::AtomicBool>,
    planned: bool,
    unsubmitted: Vec<Unsubmitted>,
}

impl Decide for ShortPlace {
    fn decide(&mut self, _: &[InstrumentId], ladder: &mut Ladder) {
        if self.planned || !self.orders.may_place() {
            return;
        }
        self.planned = true;
        self.short.store(true, std::sync::atomic::Ordering::Relaxed);
        ladder.push(&self.orders, self.oms.place()).unwrap();
    }

    fn unsubmitted(&mut self, command: Unsubmitted) {
        self.unsubmitted.push(command);
    }
}

/// Codex P1 r4309350872 on PR #164: a ladder command its session took and then failed to
/// encode, its nonce source reserving another count, which ends the session with nothing
/// reported (0057), is handed back to the consumer: it reaches the consumer once.
#[tokio::test(start_paused = true)]
async fn a_ladder_command_whose_session_fails_before_reporting_it_is_handed_back() {
    let (thaw, frozen) = std::sync::mpsc::channel::<()>();
    tokio::task::spawn_blocking(move || frozen.recv());
    let clock = IngestClock::new();
    let mut host = ShardHost::new(clock.clone());
    let mut server = ScriptedWs::start().await;
    let reserved = Reserved::default();
    let mut config = session_config(&server.url(), &reserved);
    config.clock = clock.clone();
    let short = Arc::new(std::sync::atomic::AtomicBool::new(false));
    config.nonces = Box::new(ShortOnce {
        next: 0,
        short: Arc::clone(&short),
    });
    let log: Rc<RefCell<(Vec<ExecEvent>, Vec<SubmitHandle>)>> = Rc::default();
    struct Log(Rc<RefCell<(Vec<ExecEvent>, Vec<SubmitHandle>)>>);
    impl ExecHandler for Log {
        fn on_exec(&mut self, env: Envelope<ExecEvent>) {
            self.0.borrow_mut().0.push(env.body);
        }
        fn on_submitted(&mut self, handle: SubmitHandle) {
            self.0.borrow_mut().1.push(handle);
        }
    }
    let (mut exec, _control) = ExecSession::new(config, Log(Rc::clone(&log))).unwrap();
    let mut decide = ShortPlace {
        orders: exec.orders(),
        oms: Oms::armed(),
        short,
        planned: false,
        unsubmitted: Vec::new(),
    };
    let watch = Rc::clone(&log);
    let script = async move {
        let mut peer = server.accept().await;
        assert!(peer.recv().await.starts_with("auth|ts="));
        peer.send("auth|ok=1|token=toy-session-token");
        assert_eq!(peer.recv().await, "cod|rpc=1|on=1");
        let wm = watermark(&peer.recv().await);
        peer.send("item|rpc=1|i=0|res=ok");
        peer.send(&format!("rsbegin|wm={wm}"));
        peer.send("rsend");
        settle(|| watch.borrow().0.contains(&ExecEvent::ResyncEnd)).await;
        // The session ends, having written nothing more.
        assert_eq!(peer.next().await, None);
    };
    let members = vec![Member::exec(&mut exec)];
    let (ran, ()) = tokio::join!(host.run(members, &mut decide), script);
    drop(thaw);
    assert!(
        matches!(
            ran,
            Err(ShardError::Exec(ExecSessionError::Nonces {
                reserved: 0,
                ..
            }))
        ),
        "{ran:?}"
    );
    assert!(log.borrow().1.is_empty(), "{:?}", log.borrow().1);
    assert!(
        matches!(decide.unsubmitted[..], [Unsubmitted::Authorized(_)]),
        "{:?}",
        decide.unsubmitted
    );
    assert_eq!(host.unsubmitted(), 1);
}
