//! Decoder replay (decision 0006, design §10.1): a market-data session's journal fed back to the
//! venue's current decoders through the same calls the live [`MdSession`](crate::MdSession)
//! made, so a recorded day rebuilds the same events, and the same books from them.
//!
//! An [`MdReplay`] takes the records of one session, its connection number, in journal order,
//! with no socket and no clock:
//!
//! - each `Opened` control record begins an epoch with a fresh codec from
//!   [`VenueFactory::md_codec`], whose `on_open` is called; `Closed` ends it, and the handler
//!   is told the epoch ended ([`MdHandler::on_epoch_end`]) as live, so the books built from a
//!   replay are invalidated where the live ones were (decision 0039); an `Opened` with an epoch
//!   still open (its `Closed` was dropped) ends that one first;
//! - each `Subscribe` control record calls the codec's `subscribe` with its differences;
//! - each inbound frame, HTTP result and timer firing of the open epoch goes to the codec with
//!   its recorded [`Stamp`] (its ingest sequence, both clocks and its connection epoch), inside
//!   the venue's decode scope, and every event the codec pushes is stamped with it and handed
//!   to the consumer's [`MdHandler`], as live;
//! - one of an epoch that is not open (it ended, or the journal lost its opening) reaches no
//!   codec and is counted stale, as live a late arrival is.
//!
//! Effects a codec asks for are not executed, and the outbound frames, write results and HTTP
//! requests the journal holds are outputs, not fed to anything: exact replay of outbound bytes
//! is BT-402's. A ping, pong or close frame the session received reached no codec live, so its
//! record (FBC-drf, decision 0041) is fed to none either. A consumer's own record (FBC-hxkm,
//! decision 0101) reaches no codec: one whose input flag is false is the consumer's annotation,
//! passed over as an outbound frame is. A consumer input the shard stamped (FBC-hvqi, decision
//! 0103) is handed back to the input handler set with [`MdReplay::set_inputs`] at its place,
//! with its recorded ingest sequence, time, kind and bytes, as live; with none set, or for an
//! input the consumer journaled itself (no ingest sequence), it is passed over. A replay arms
//! no timer: the consumer's handlers arm through [`ConsumerTimers::inert`](crate::ConsumerTimers::inert),
//! and each firing the live shard took comes back from the journal.
//!
//! The caller supplies what the session ran with: the [`VenueConfig`], the
//! [`SpecTable`] and the endpoint's plan. Whether the journal's `SessionStart` header carries
//! them is the consumer's choice (design §9).
//!
//! The journal does not hold the subscriptions an epoch's codec was built for. Live, they are
//! those of the epoch's opening `subscribe` call, which comes after `on_open` and before any
//! frame or timer, so replay builds the codec for the additions of the epoch's first
//! `Subscribe` record when it comes before the epoch's first frame or timer, and for none
//! otherwise; an HTTP result in between is held and answered once the codec is built, before
//! that call, as live. Format version 4 cannot tell that opening call from a change of an
//! empty desired set made before any frame or timer, nor show the subscriptions of an epoch
//! whose opening write failed (FBC-11m). A frame, timer firing or HTTP result the session took
//! as its control dropped reached no codec; it is journaled after the epoch's `Closed`, so
//! replay feeds it to none.
//! Records the sink dropped leave a `Degraded` marker, counted as a gap: a replay across one
//! is not exact.
//!
//! **Closed loop** (FBC-066n, decision 0107). A replay can also run as a member of a shard host
//! ([`Member::md_replay`](crate::Member::md_replay)) with a [`ClosedLoop`]: each record it feeds
//! waits for its recorded instant on the shard's replayed clock
//! ([`IngestClock::replayed`], on a paused runtime, so the wait is virtual), and each input is
//! stamped with the shard's next ingest sequence, keeping its recorded instant, so the consumer's
//! handlers, decide passes and order-entry sessions (SimVenue's, per bracket) run among the
//! records as they run live. Once the records end it waits the `settle` time and ends the
//! sessions.

use std::fmt;

use fbc_core::{
    ConfigError, ConnKey, Effects, EndpointPlan, Envelope, HttpFailure, HttpResponse, HttpTag,
    MdCodec, MdEvent, MdSink, MonoNs, RawFrame, SpecTable, Stamp, Subscription, VenueCaps,
    VenueConfig, VenueFactory, VenueMeta,
};
use fbc_journal::{ControlEvent, HttpResponseRec, JournalError, Marker, Opcode, Record};

use crate::exec_session::ExecControl;
use crate::inputs::{ConsumerInput, InputHandler};
use crate::session::{MdHandler, feed_frame, feed_http, feed_timer};
use crate::session_core::IngestClock;

/// What a replay needs, all from the consumer: what the session ran with.
pub struct MdReplayConfig {
    pub venue: &'static dyn VenueFactory,
    pub cfg: VenueConfig,
    /// The endpoint; each epoch's codec is built for its stream and transport, and for the
    /// subscriptions the journal shows that epoch opened with (the module docs say how).
    pub plan: EndpointPlan,
    pub specs: SpecTable,
    /// The session's connection number: records of every other connection are skipped.
    pub conn: u16,
}

/// What a replay counted.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct MdReplayCounters {
    /// Epochs replayed, each with a fresh codec.
    pub epochs: u64,
    /// Inbound frames fed to a codec.
    pub frames: u64,
    /// HTTP results fed to the codec that asked.
    pub http: u64,
    /// Timer firings fed to the codec that set them.
    pub timers: u64,
    /// Inputs and subscribe calls of an epoch that was not open, which reached no codec.
    pub stale: u64,
    /// Frames and results the codec could not decode.
    pub decode_errors: u64,
    /// Subscribe calls the codec refused.
    pub refused_subscribes: u64,
    /// `Degraded` markers: places where the journal dropped records.
    pub gaps: u64,
    /// Consumer inputs the shard stamped, handed back to the input handler at their place
    /// ([`MdReplay::set_inputs`]).
    pub inputs: u64,
}

/// Why a replay could not start, or stopped.
#[derive(Debug)]
pub enum ReplayError {
    /// The venue refused the configuration.
    Config(ConfigError),
    /// The journal could not be read.
    Journal(JournalError),
    /// A text frame's bytes are not UTF-8, which no session journals: the record is damaged.
    NotUtf8 { ingest_seq: u64 },
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplayError::Config(e) => write!(f, "venue configuration refused: {e}"),
            ReplayError::Journal(e) => write!(f, "the journal cannot be read: {e}"),
            ReplayError::NotUtf8 { ingest_seq } => {
                write!(
                    f,
                    "the text frame of ingest sequence {ingest_seq} is not UTF-8"
                )
            }
        }
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReplayError::Config(e) => Some(e),
            ReplayError::Journal(e) => Some(e),
            ReplayError::NotUtf8 { .. } => None,
        }
    }
}

/// What a closed-loop replay runs on a shard with (FBC-066n): [`Member::md_replay`](crate::Member::md_replay).
pub struct ClosedLoop<I> {
    /// The shard's clock, [`IngestClock::replayed`]: the host and every member's session share
    /// it.
    pub clock: IngestClock,
    /// The journal's records in order, as [`JournalReader`](fbc_journal::JournalReader) reads
    /// them.
    pub records: I,
    /// How long the replay runs on past the instant of the journal's last record, so the
    /// answers due by then are read.
    pub settle: std::time::Duration,
    /// The controls of the shard's order-entry sessions, dropped once the replay has settled:
    /// which ends them, and the shard's run with them.
    pub stops: Vec<ExecControl>,
}

impl<I> fmt::Debug for ClosedLoop<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClosedLoop")
            .field("settle", &self.settle)
            .field("stops", &self.stops.len())
            .finish_non_exhaustive()
    }
}

/// Why a closed-loop replay stopped: the shard's run ends with it
/// ([`ShardError::Replay`](crate::ShardError::Replay)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplayFailure {
    /// The clock is a live shard's, not [`IngestClock::replayed`]: it would read the wall clock.
    LiveClock,
    /// The journal could not be read: the reader's error, as it reads.
    Journal(String),
    /// A text frame's bytes are not UTF-8, which no session journals: the record is damaged.
    NotUtf8 { ingest_seq: u64 },
}

impl fmt::Display for ReplayFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReplayFailure::LiveClock => {
                f.write_str("a closed-loop replay needs a replayed clock, not a live one")
            }
            ReplayFailure::Journal(e) => write!(f, "the journal cannot be read: {e}"),
            ReplayFailure::NotUtf8 { ingest_seq } => {
                write!(
                    f,
                    "the text frame of ingest sequence {ingest_seq} is not UTF-8"
                )
            }
        }
    }
}

impl std::error::Error for ReplayFailure {}

/// One session's journal, replayed through its venue's decoders into a handler.
pub struct MdReplay<H> {
    venue: &'static dyn VenueFactory,
    cfg: VenueConfig,
    plan: EndpointPlan,
    caps: VenueCaps,
    specs: SpecTable,
    conn: u16,
    open: Option<Epoch>,
    counters: MdReplayCounters,
    handler: H,
    inputs: Option<Box<dyn InputHandler>>,
    /// The shard's replayed clock while a closed-loop replay plays ([`MdReplay::play`]): every
    /// input takes its next ingest sequence.
    shard: Option<IngestClock>,
}

/// The open epoch: its codec once built, and until then the HTTP results that came back
/// before the epoch's opening call.
struct Epoch {
    key: ConnKey,
    codec: Option<Box<dyn MdCodec>>,
    held: Vec<Held>,
}

/// An HTTP result as journaled: its stamp, its tag, and the response or why none came.
type Held = (Stamp, HttpTag, Result<HttpResponseRec, HttpFailure>);

impl<H: MdHandler> MdReplay<H> {
    /// A replay of `config`'s session, handing events to `handler`.
    pub fn new(config: MdReplayConfig, handler: H) -> Result<MdReplay<H>, ReplayError> {
        let caps = config
            .venue
            .caps(&config.cfg)
            .map_err(ReplayError::Config)?;
        Ok(MdReplay {
            venue: config.venue,
            cfg: config.cfg,
            plan: config.plan,
            caps,
            specs: config.specs,
            conn: config.conn,
            open: None,
            counters: MdReplayCounters::default(),
            handler,
            inputs: None,
            shard: None,
        })
    }

    /// Hands each consumer input the shard stamped back to `handler` at its place in the
    /// journal, with its recorded ingest sequence, time, kind and bytes (FBC-hvqi). Set it on
    /// one replay of a shard's journal: every session's replay reads the same inputs.
    pub fn set_inputs<I: InputHandler + 'static>(&mut self, handler: I) {
        self.inputs = Some(Box::new(handler));
    }

    pub fn counters(&self) -> MdReplayCounters {
        self.counters
    }

    /// Feeds every record of `records` in order ([`MdReplay::feed`]), then [`MdReplay::finish`]:
    /// a whole journal, as [`JournalReader`](fbc_journal::JournalReader) reads it.
    pub fn run(
        &mut self,
        records: impl IntoIterator<Item = Result<Record, JournalError>>,
    ) -> Result<(), ReplayError> {
        for record in records {
            self.feed(&record.map_err(ReplayError::Journal)?)?;
        }
        self.finish();
        Ok(())
    }

    /// Drives the closed-loop replay of `closed` on a shard ([`Member::md_replay`](crate::Member::md_replay)).
    ///
    /// Each record this replay feeds waits for its recorded instant on the shard's replayed
    /// clock (the runtime's, paused, so the wait moves it to the earliest of that instant and
    /// every other timer of the shard due before it), then is fed as [`MdReplay::feed`] feeds
    /// it, each input stamped with the shard's next ingest sequence. Once the records end,
    /// the replay waits `settle` past the journal's last recorded instant, lets the shard read
    /// what is due then and decide on every input, and drops the order-entry sessions'
    /// controls. A live clock, a record the journal cannot read and a damaged text frame each
    /// stop it with nothing more fed, the controls dropped all the same.
    pub(crate) async fn play<I>(&mut self, closed: ClosedLoop<I>) -> Result<(), ReplayFailure>
    where
        I: IntoIterator<Item = Result<Record, JournalError>>,
    {
        let ClosedLoop {
            clock,
            records,
            settle,
            stops,
        } = closed;
        if !clock.is_replayed() {
            return Err(ReplayFailure::LiveClock);
        }
        let last = self.play_records(&clock, records).await?;
        let settled = u64::try_from(settle.as_nanos()).unwrap_or(u64::MAX);
        until(&clock, MonoNs(last.0.saturating_add(settled))).await;
        // What is due at the settle instant is read, and decided on, before the end (Codex P2
        // r4315555356 on PR #178).
        drained(&clock).await;
        drop(stops);
        Ok(())
    }

    /// Calls `f` with the shard's clock set for it to stamp with, and clears it before
    /// returning: no await lies between, so a replay whose run is dropped while it waits stamps
    /// with the recorded stamps again when fed on its own (Codex P2 r4315555339 on PR #178).
    fn on_shard<R>(&mut self, clock: &IngestClock, f: impl FnOnce(&mut Self) -> R) -> R {
        self.shard = Some(clock.clone());
        let r = f(self);
        self.shard = None;
        r
    }

    /// Feeds `records`, each at its recorded instant on `clock`, then finishes: the latest
    /// instant the journal records, of a record fed or passed over (Codex P2 r4315555312 on PR
    /// #178).
    async fn play_records<I>(
        &mut self,
        clock: &IngestClock,
        records: I,
    ) -> Result<MonoNs, ReplayFailure>
    where
        I: IntoIterator<Item = Result<Record, JournalError>>,
    {
        let mut last = MonoNs(0);
        for record in records {
            let record = record.map_err(|e| ReplayFailure::Journal(e.to_string()))?;
            last = last.max(recorded_at(&record).unwrap_or(last));
            if let Some(at) = self.due(&record) {
                until(clock, at).await;
            }
            // A record refused is a damaged text frame: the only refusal of a feed.
            if let Err(ReplayError::NotUtf8 { ingest_seq }) =
                self.on_shard(clock, |replay| replay.feed(&record))
            {
                return Err(ReplayFailure::NotUtf8 { ingest_seq });
            }
        }
        self.on_shard(clock, MdReplay::finish);
        Ok(last)
    }

    /// The recorded instant of `record` when this replay feeds it: an input or control record
    /// of its connection, or a consumer input the shard stamped while an input handler is set.
    fn due(&self, record: &Record) -> Option<MonoNs> {
        match record {
            Record::Inbound { stamp, .. }
            | Record::InboundControl { stamp, .. }
            | Record::Timer { stamp, .. }
            | Record::HttpResult { stamp, .. }
                if self.owns(stamp.conn) =>
            {
                Some(stamp.recv_mono)
            }
            Record::Control { at, ev } if self.owns(control_conn(ev)) => Some(*at),
            Record::Consumer {
                at,
                input: true,
                ingest_seq: Some(_),
                ..
            } if self.inputs.is_some() => Some(*at),
            _ => None,
        }
    }

    /// The stamp of a replayed input recorded as `recorded`: the shard's next ingest sequence
    /// in a closed-loop replay, the recorded one otherwise.
    fn stamp(&self, recorded: Stamp) -> Stamp {
        self.shard
            .as_ref()
            .map_or(recorded, |clock| clock.restamp(recorded))
    }

    /// Feeds one record; records of other connections and outputs are skipped.
    pub fn feed(&mut self, record: &Record) -> Result<(), ReplayError> {
        match record {
            Record::Marker(Marker::Degraded { .. }) => self.counters.gaps += 1,
            Record::Control { ev, .. } => self.control(ev),
            Record::Inbound {
                stamp,
                opcode,
                bytes,
                ..
            } if self.owns(stamp.conn) => {
                let raw = match opcode {
                    Opcode::Text => {
                        RawFrame::Text(std::str::from_utf8(&bytes.0).map_err(|_| {
                            ReplayError::NotUtf8 {
                                ingest_seq: stamp.ingest_seq,
                            }
                        })?)
                    }
                    Opcode::Binary => RawFrame::Binary(&bytes.0),
                };
                let stamp = &self.stamp(*stamp);
                if self.ready(stamp.conn)
                    && let Some(codec) = self.open.as_mut().and_then(|e| e.codec.as_deref_mut())
                {
                    let mut sink = Deliver {
                        handler: &mut self.handler,
                        stamp: *stamp,
                        shard: self.shard.as_ref(),
                    };
                    let fed = feed_frame(codec, &self.caps, &self.specs, raw, &mut sink, &mut fx());
                    self.counters.frames += 1;
                    self.counters.decode_errors += u64::from(fed.is_err());
                }
            }
            Record::Timer { stamp, tag } if self.owns(stamp.conn) => {
                let stamp = &self.stamp(*stamp);
                if self.ready(stamp.conn)
                    && let Some(codec) = self.open.as_mut().and_then(|e| e.codec.as_deref_mut())
                {
                    let mut sink = Deliver {
                        handler: &mut self.handler,
                        stamp: *stamp,
                        shard: self.shard.as_ref(),
                    };
                    feed_timer(codec, *stamp, *tag, &mut sink, &mut fx());
                    self.counters.timers += 1;
                }
            }
            // A ping, pong or close reaches no codec, but the live session stamped it: in a
            // closed-loop replay it takes its place in the ingest order (Codex P2 r4315555320 on
            // PR #178).
            Record::InboundControl { stamp, .. } if self.owns(stamp.conn) => {
                self.stamp(*stamp);
            }
            Record::HttpResult { stamp, tag, result } if self.owns(stamp.conn) => {
                // Stamped as it is answered, so its sequence and its events reach the shard
                // together (Codex P2 r4315555331 on PR #178).
                let held = (*stamp, *tag, result.clone());
                match &mut self.open {
                    // Before the epoch's opening call: held until its codec is built.
                    Some(epoch) if epoch.key == stamp.conn && epoch.codec.is_none() => {
                        epoch.held.push(held);
                    }
                    Some(epoch) if epoch.key == stamp.conn => self.answer(held),
                    _ => {
                        self.stamp(*stamp);
                        self.counters.stale += 1;
                    }
                }
            }
            // An input the shard stamped goes back to the consumer at its place (FBC-hvqi).
            Record::Consumer { .. } => hand_back(
                &mut self.inputs,
                &mut self.counters,
                self.shard.as_ref(),
                record,
            ),
            _ => {}
        }
        Ok(())
    }

    /// Ends the replay: an epoch still waiting for its opening call gets its codec, which
    /// answers the results it held.
    pub fn finish(&mut self) {
        self.build(Vec::new());
    }

    /// Whether `key` is this session's connection.
    fn owns(&self, key: ConnKey) -> bool {
        key.conn == self.conn
    }

    fn control(&mut self, ev: &ControlEvent) {
        match ev {
            ControlEvent::Opened(key) if self.owns(*key) => {
                // An opening with no close before it (the close was dropped) ends the open epoch.
                if let Some(open) = self.open.as_ref().map(|e| e.key) {
                    self.end(open);
                }
                self.open = Some(Epoch {
                    key: *key,
                    codec: None,
                    held: Vec::new(),
                });
            }
            // A close of an epoch that is not open (its opening was dropped) fed nothing, but
            // ended all the same, as live.
            ControlEvent::Closed(key) if self.owns(*key) => self.end(*key),
            ControlEvent::Subscribe { conn, add, remove } if self.owns(*conn) => {
                if self.open.as_ref().is_some_and(|e| e.key == *conn) {
                    self.build(add.clone());
                }
                if self.ready(*conn)
                    && let Some(codec) = self.open.as_mut().and_then(|e| e.codec.as_deref_mut())
                {
                    let asked = codec.subscribe(add, remove, &self.specs, &mut fx());
                    self.counters.refused_subscribes += u64::from(asked.is_err());
                }
            }
            _ => {}
        }
    }

    /// Ends epoch `key`: the open one, if it is, answers what it held first; then the handler is
    /// told, as live ([`MdHandler::on_epoch_end`], decision 0039).
    fn end(&mut self, key: ConnKey) {
        if self.open.as_ref().is_some_and(|e| e.key == key) {
            self.finish();
            self.open = None;
        }
        self.handler.on_epoch_end(key);
    }

    /// Builds the open epoch's codec for `subs`, if it has none yet, calls its `on_open` and
    /// answers the results it held.
    fn build(&mut self, subs: Vec<Subscription>) {
        let Some(epoch) = self.open.as_mut().filter(|e| e.codec.is_none()) else {
            return;
        };
        let plan = EndpointPlan {
            subs,
            ..self.plan.clone()
        };
        let mut codec = self.venue.md_codec(&self.cfg, &plan);
        codec.on_open(&mut fx());
        epoch.codec = Some(codec);
        let held = std::mem::take(&mut epoch.held);
        self.counters.epochs += 1;
        for held in held {
            self.answer(held);
        }
    }

    /// Whether epoch `key` is the open one, its codec built if it was not yet; when it is not,
    /// the input is counted stale.
    fn ready(&mut self, key: ConnKey) -> bool {
        if !self.open.as_ref().is_some_and(|e| e.key == key) {
            self.counters.stale += 1;
            return false;
        }
        self.build(Vec::new());
        true
    }

    /// Hands an HTTP result of the open epoch to its codec, which is built, stamped now.
    fn answer(&mut self, (recorded, tag, result): Held) {
        let stamp = self.stamp(recorded);
        if let Some(codec) = self.open.as_mut().and_then(|e| e.codec.as_deref_mut()) {
            let mut sink = Deliver {
                handler: &mut self.handler,
                stamp,
                shard: self.shard.as_ref(),
            };
            let (caps, specs) = (&self.caps, &self.specs);
            let fed = match &result {
                Ok(rec) => with_response(rec, |resp| {
                    feed_http(codec, caps, specs, tag, Ok(resp), &mut sink, &mut fx())
                }),
                Err(e) => feed_http(codec, caps, specs, tag, Err(*e), &mut sink, &mut fx()),
            };
            self.counters.http += 1;
            self.counters.decode_errors += u64::from(fed.is_err());
        }
    }
}

/// Hands a consumer input the shard stamped back to `inputs`, counted, with the shard's next
/// ingest sequence in a closed-loop replay (`shard`) and its recorded one otherwise. The
/// consumer's annotations and the inputs it journaled itself are no codec's input (decision
/// 0101) and nothing the shard handed on: passed over, as is every input with no input handler
/// set.
fn hand_back(
    inputs: &mut Option<Box<dyn InputHandler>>,
    counters: &mut MdReplayCounters,
    shard: Option<&IngestClock>,
    record: &Record,
) {
    if let (
        Some(inputs),
        Record::Consumer {
            at,
            kind,
            input: true,
            ingest_seq: Some(ingest_seq),
            bytes,
        },
    ) = (inputs, record)
    {
        let (ingest_seq, at) = shard.map_or((*ingest_seq, *at), |clock| clock.retick(*at));
        inputs.on_input(ConsumerInput {
            ingest_seq,
            at,
            kind: *kind,
            bytes: bytes.0.clone(),
        });
        counters.inputs += 1;
    }
}

/// Effects a replayed codec asks for: collected and never executed.
fn fx() -> Effects {
    Effects::new()
}

/// Calls `f` with a journaled response as its codec is handed it: its status, headers in order
/// (a secret one's value as the journal reads it back, blanked) and body.
fn with_response<R>(rec: &HttpResponseRec, f: impl FnOnce(HttpResponse<'_>) -> R) -> R {
    let headers: Vec<(&str, &str)> = rec
        .headers
        .iter()
        .map(|h| (h.name.as_str(), h.value.as_str()))
        .collect();
    f(HttpResponse {
        status: rec.status,
        headers: &headers,
        body: &rec.body.0,
    })
}

/// Stamps each event a replayed codec pushes with its input's stamp and hands it to the
/// handler: the codec is always the open epoch's, so every event passes. In a closed-loop
/// replay the shard's clock names the input as being handled meanwhile, as a live session's
/// does (decision 0100), so what the handler submits is attributed to it.
struct Deliver<'a, H> {
    handler: &'a mut H,
    stamp: Stamp,
    shard: Option<&'a IngestClock>,
}

impl<H: MdHandler> MdSink for Deliver<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        let env = Envelope::new(self.stamp, meta, ev);
        match self.shard {
            Some(clock) => clock.attributing(self.stamp, || self.handler.on_md(env)),
            None => self.handler.on_md(env),
        }
    }
}

/// The recorded instant a journal record carries, on the shard's monotonic clock: every kind
/// but the untimed ones is named, so a kind added later is placed here (an encode context's is
/// the time it gave the codec, Codex P2 r4316220952 on PR #178).
fn recorded_at(record: &Record) -> Option<MonoNs> {
    match record {
        Record::Inbound { stamp, .. }
        | Record::InboundControl { stamp, .. }
        | Record::HttpResult { stamp, .. }
        | Record::Timer { stamp, .. }
        | Record::RpcTimeout { stamp, .. } => Some(stamp.recv_mono),
        Record::Control { at, .. }
        | Record::Outbound { at, .. }
        | Record::WriteResult { at, .. }
        | Record::HttpRequest { at, .. }
        | Record::Consumer { at, .. } => Some(*at),
        Record::EncodeCtx { ctx, .. } => Some(ctx.mono),
        Record::Marker(_) | Record::Nonce { .. } | Record::Cycle { .. } => None,
    }
}

/// Waits until the shard has read what was due as it woke and decided on every input stamped
/// so far: it yields once, so every other member woken at the same instant is polled, then until
/// its host has run a decide pass, and taken that pass's ladder, since the last input was
/// stamped.
async fn drained(clock: &IngestClock) {
    tokio::task::yield_now().await;
    while clock.undecided() {
        tokio::task::yield_now().await;
    }
}

/// The connection a control record names.
fn control_conn(ev: &ControlEvent) -> ConnKey {
    match ev {
        ControlEvent::Opened(key) | ControlEvent::Closed(key) => *key,
        ControlEvent::Subscribe { conn, .. } => *conn,
    }
}

/// Waits until `at` on the replayed clock `clock`: at once when it is not later than now or
/// lies past the end of the clock.
async fn until(clock: &IngestClock, at: MonoNs) {
    if let Some(when) = clock.instant(at)
        && when > tokio::time::Instant::now()
    {
        tokio::time::sleep_until(when).await;
    }
}
