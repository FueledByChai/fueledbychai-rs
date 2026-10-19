//! FBC-hvqi's done line (decisions 0006, 0023, 0101, 0103): during a toy market-data session on
//! the shard host, consumer inputs pushed from another thread and a timer the input handler
//! arms each reach the handler stamped with the shard's next ingest sequence, among the
//! session's frames, and journaled as an input consumer record before the handler sees them;
//! decoder replay of that journal hands the same inputs and the timer's firing back to the
//! handler at the same ingest positions, through timers that arm nothing; and a push into a full
//! ring is refused and counted.

mod common;

use std::cell::RefCell;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use common::ScriptedWs;
use common::toy::{self, ToyVenue};
use fbc_core::{
    EndpointPlan, Envelope, InstrumentId, MdEvent, MdTransport, TrafficClass, VenueConfig, WallNs,
    WireUrl,
};
use fbc_journal::{
    JournalReader, JournalSink, JournalWriter, Opaque, Record, RecordRef, Recorded, RedactionKey,
    SinkConfig, journal_queue,
};
use fbc_runtime::{
    Connector, ConsumerInput, ConsumerInputs, ConsumerInputsConfig, ConsumerTimers, IngestClock,
    InputHandler, InputRefusal, Journal, Ladder, Liveness, MdReplay, MdReplayConfig, MdSession,
    MdSessionConfig, Member, ProxyConfig, ReconnectPacing, ShardHost, WriteStall,
};

const CONN: u16 = 6;
const SHARD: u16 = 4;

/// The consumer's own numbers for its inputs: what the library never reads.
const START: u16 = 1;
const RELOAD: u16 = 2;
const FLATTEN: u16 = 3;
const STOP: u16 = 4;
/// The kind the input handler arms its timer with.
const WAKE: u16 = 9;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[5; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn plan(url: &str) -> EndpointPlan {
    EndpointPlan {
        stream: toy::STREAM,
        transport: MdTransport::Socket {
            url: WireUrl::plain(url.to_owned()),
        },
        subs: vec![toy::sub(1)],
    }
}

fn session(venue: &'static ToyVenue, url: &str, clock: &IngestClock) -> MdSessionConfig {
    MdSessionConfig {
        venue,
        cfg: VenueConfig::new(),
        plan: plan(url),
        specs: toy::specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(ms(10), ms(100), 100, ms(60_000), ms(5_000)).unwrap(),
        clock: clock.clone(),
        http_max_body: 1024,
        conn: CONN,
        limiter: venue.limiter(0),
        liveness: Liveness::new(Duration::from_secs(3_600), ms(1)).unwrap(),
        write_stall: WriteStall::new(Duration::from_secs(3_600)).unwrap(),
    }
}

async fn until(done: impl Fn() -> bool) {
    while !done() {
        tokio::time::sleep(ms(2)).await;
    }
}

/// What reached the consumer's handlers, in order.
#[derive(Clone, Debug, PartialEq)]
enum Step {
    Md(Envelope<MdEvent>),
    Input(ConsumerInput),
}

type Steps = Rc<RefCell<Vec<Step>>>;

/// The records offered to the journal so far, as the sink was offered them.
type Offered = Rc<RefCell<Vec<Record>>>;

/// A sink that keeps a copy of every record it is offered and hands it on to the journal's.
struct Tee<S> {
    inner: S,
    offered: Offered,
}

impl<S: JournalSink> JournalSink for Tee<S> {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.offered.borrow_mut().push(record.clone());
        self.inner.record(class, now, record)
    }

    fn record_ref(&mut self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) -> Recorded {
        self.offered.borrow_mut().push(record.to_record());
        self.inner.record_ref(class, now, record)
    }

    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded {
        self.inner.omit(class, now)
    }
}

/// The consumer's input handler: on Start it arms a wake timer, as a model's wake request or a
/// resume hold would; live, it notes whether each input's record was the last consumer record
/// offered to the journal when it was handed the input.
struct Operator {
    steps: Steps,
    timers: ConsumerTimers,
    /// Live only: the journal's offered records, and for each input whether its record was
    /// already there.
    journaled: Option<(Offered, Rc<RefCell<Vec<bool>>>)>,
}

impl InputHandler for Operator {
    fn on_input(&mut self, input: ConsumerInput) {
        if let Some((offered, first)) = &self.journaled {
            let last = offered
                .borrow()
                .iter()
                .rev()
                .find(|r| matches!(r, Record::Consumer { .. }))
                .cloned();
            let own = Record::Consumer {
                at: input.at,
                kind: input.kind,
                input: true,
                ingest_seq: Some(input.ingest_seq),
                bytes: Opaque(input.bytes.clone()),
            };
            first.borrow_mut().push(last == Some(own));
        }
        if input.kind == START {
            let wake = b"wake".to_vec();
            self.timers.arm(ms(20), TrafficClass::Normal, WAKE, wake);
        }
        self.steps.borrow_mut().push(Step::Input(input));
    }
}

fn md_handler(steps: &Steps) -> impl FnMut(Envelope<MdEvent>) + use<> {
    let steps = steps.clone();
    move |env| steps.borrow_mut().push(Step::Md(env))
}

/// The ingest sequence a journaled record was stamped with, if it is an input.
fn seq_of(record: &Record) -> Option<u64> {
    match record {
        Record::Inbound { stamp, .. }
        | Record::InboundControl { stamp, .. }
        | Record::Timer { stamp, .. }
        | Record::HttpResult { stamp, .. }
        | Record::RpcTimeout { stamp, .. } => Some(stamp.ingest_seq),
        Record::Consumer {
            input: true,
            ingest_seq,
            ..
        } => *ingest_seq,
        _ => None,
    }
}

fn inputs_of(steps: &[Step]) -> Vec<(u16, Vec<u8>)> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Input(i) => Some((i.kind, i.bytes.clone())),
            Step::Md(_) => None,
        })
        .collect()
}

#[tokio::test]
async fn consumer_inputs_and_a_handler_timer_are_stamped_journaled_then_handled_and_replay_hands_them_back_in_place()
 {
    let root = fresh_dir("consumer_inputs_session");
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (sink, drain) = journal_queue(config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, SHARD, key()).unwrap())
        .unwrap();
    let offered = Offered::default();
    let sink = Rc::new(RefCell::new(Tee {
        inner: sink,
        offered: offered.clone(),
    }));
    let journal = Journal::new(sink.clone());

    let clock = IngestClock::new();
    let mut ws = ScriptedWs::start().await;
    let steps = Steps::default();
    let venue = ToyVenue::leak();
    let (mut md, control) =
        MdSession::new(session(venue, &ws.url(), &clock), md_handler(&steps)).unwrap();
    md.set_journal(journal.clone());
    let timers = ConsumerTimers::new();
    let first = Rc::new(RefCell::new(Vec::new()));
    let operator = Operator {
        steps: steps.clone(),
        timers: timers.clone(),
        journaled: Some((offered.clone(), first.clone())),
    };
    let inputs_config = ConsumerInputsConfig {
        clock: clock.clone(),
        capacity: 2,
        timers: timers.clone(),
    };
    let (mut inputs, sender) = ConsumerInputs::new(inputs_config, operator).unwrap();
    inputs.set_journal(journal.clone());

    // The operator's thread: Start and a reload fill the ring of two before the shard runs, so
    // a Flatten pushed then is refused and counted; it then waits to be told to push Stop.
    let (go, wait) = mpsc::channel::<()>();
    let (filled, filled_rx) = mpsc::channel();
    let operator_thread = std::thread::spawn(move || {
        sender
            .push(TrafficClass::Safety, START, b"Start".to_vec())
            .unwrap();
        sender
            .push(TrafficClass::Normal, RELOAD, b"calendar v2".to_vec())
            .unwrap();
        let full = sender.push(TrafficClass::Normal, FLATTEN, b"Flatten".to_vec());
        filled.send((full, sender.refused())).unwrap();
        wait.recv().unwrap();
        let stop = sender.push(TrafficClass::Safety, STOP, vec![0xff, 0x00, 0xc3]);
        (stop, sender.refused())
    });
    // The ring was full before the shard took anything: the third push was refused, counted.
    let (full, refused) = filled_rx.recv().unwrap();
    assert_eq!(full, Err(InputRefusal::Full));
    assert_eq!(refused, 1);

    let mut host = ShardHost::new(clock.clone());
    host.set_journal(journal.clone());
    let watch = steps.clone();
    let script = async move {
        let mut peer = ws.accept().await;
        assert_eq!(peer.recv().await, "hello|codec=0|plan=1");
        assert_eq!(peer.recv().await, "sub|add=A");
        peer.send("trade|sym=A|px=100|qty=1|seq=1");
        // Start and the reload reach the handler; Start's wake timer fires after them.
        until(|| inputs_of(&watch.borrow()).len() == 3).await;
        peer.send("trade|sym=A|px=101|qty=1|seq=2");
        until(|| watch.borrow().len() == 5).await;
        go.send(()).unwrap();
        until(|| inputs_of(&watch.borrow()).len() == 4).await;
        peer.send("trade|sym=A|px=102|qty=1|seq=3");
        until(|| watch.borrow().len() == 7).await;
        drop(control);
        assert_eq!(peer.next().await, None);
    };
    let mut decide = |_: &[InstrumentId], _: &mut Ladder| {};
    let members = vec![Member::md(&mut md), Member::inputs(&mut inputs)];
    let (ran, ()) = tokio::join!(host.run(members, &mut decide), script);
    ran.unwrap();
    let (stop, refused_after) = operator_thread.join().unwrap();
    stop.unwrap();
    assert_eq!(refused_after, 1);
    drop((md, inputs, journal));
    writer.close().unwrap();

    // Each input and the firing reached the handler in order, Flatten never.
    let live = steps.take();
    assert_eq!(
        inputs_of(&live),
        [
            (START, b"Start".to_vec()),
            (RELOAD, b"calendar v2".to_vec()),
            (WAKE, b"wake".to_vec()),
            (STOP, vec![0xff, 0x00, 0xc3]),
        ]
    );
    assert_eq!(live.len(), 7);
    // Each was journaled before the handler was handed it.
    assert_eq!(*first.borrow(), [true; 4]);
    assert_eq!(timers.arms(), 1);
    assert_eq!(timers.pending(), 0);

    // In the journal, every input of the shard took the next ingest sequence, the consumer's
    // among the session's frames, each record where the handler saw it.
    let read: Vec<Record> = JournalReader::open(&root, SHARD)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let seqs: Vec<u64> = read.iter().filter_map(seq_of).collect();
    assert_eq!(seqs, (0..seqs.len() as u64).collect::<Vec<_>>());
    let handled: Vec<u64> = live
        .iter()
        .map(|s| match s {
            Step::Md(env) => env.stamp.ingest_seq,
            Step::Input(i) => i.ingest_seq,
        })
        .collect();
    assert!(handled.windows(2).all(|w| w[0] < w[1]), "{handled:?}");
    let journaled_inputs: Vec<u64> = read
        .iter()
        .filter(|r| matches!(r, Record::Consumer { input: true, .. }))
        .filter_map(seq_of)
        .collect();
    let handed_inputs: Vec<u64> = live
        .iter()
        .filter_map(|s| match s {
            Step::Input(i) => Some(i.ingest_seq),
            Step::Md(_) => None,
        })
        .collect();
    assert_eq!(journaled_inputs, handed_inputs);
    // An input between two trades took a sequence between theirs: a frame of the session
    // came between the firing and Stop.
    let at = |kind: u16| {
        live.iter()
            .position(|s| matches!(s, Step::Input(i) if i.kind == kind))
            .unwrap()
    };
    assert!(
        at(WAKE) < at(STOP)
            && live[at(WAKE)..at(STOP)]
                .iter()
                .any(|s| matches!(s, Step::Md(_)))
    );
    // Each input called for a decide pass, as a frame does: a Cycle record after it covers it.
    for (i, record) in read.iter().enumerate() {
        if let Record::Consumer {
            ingest_seq: Some(seq),
            ..
        } = record
        {
            let covered = read[i..].iter().any(
                |r| matches!(r, Record::Cycle { last_ingest_seq, .. } if last_ingest_seq >= seq),
            );
            assert!(covered, "input {seq} in {read:#?}");
        }
    }
    assert!(host.passes() >= 1);

    // Replay hands the same inputs and the firing back at the same positions, through timers
    // that arm nothing.
    for _ in 0..2 {
        let replayed = Steps::default();
        let inert = ConsumerTimers::inert();
        let config = MdReplayConfig {
            venue: ToyVenue::leak(),
            cfg: VenueConfig::new(),
            plan: plan("ws://127.0.0.1:1/replayed"),
            specs: toy::specs(),
            conn: CONN,
        };
        let mut replay = MdReplay::new(config, md_handler(&replayed)).unwrap();
        replay.set_inputs(Operator {
            steps: replayed.clone(),
            timers: inert.clone(),
            journaled: None,
        });
        replay
            .run(JournalReader::open(&root, SHARD).unwrap())
            .unwrap();
        assert_eq!(replay.counters().inputs, 4);
        drop(replay);
        assert_eq!(replayed.take(), live);
        assert_eq!(
            (inert.arms(), inert.not_armed(), inert.pending()),
            (1, 1, 0)
        );
    }
    fs::remove_dir_all(&root).unwrap();
}
