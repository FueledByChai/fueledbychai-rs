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
//! record (FBC-drf, decision 0041) is fed to none either. The caller supplies what the session ran with: the [`VenueConfig`], the
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

use std::fmt;

use fbc_core::{
    ConfigError, ConnKey, Effects, EndpointPlan, Envelope, HttpFailure, HttpResponse, HttpTag,
    MdCodec, MdEvent, MdSink, RawFrame, SpecTable, Stamp, Subscription, VenueCaps, VenueConfig,
    VenueFactory, VenueMeta,
};
use fbc_journal::{ControlEvent, HttpResponseRec, JournalError, Marker, Opcode, Record};

use crate::session::{MdHandler, feed_frame, feed_http, feed_timer};

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
        })
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
                if self.ready(stamp.conn)
                    && let Some(codec) = self.open.as_mut().and_then(|e| e.codec.as_deref_mut())
                {
                    let mut sink = Deliver {
                        handler: &mut self.handler,
                        stamp: *stamp,
                    };
                    let fed = feed_frame(codec, &self.caps, &self.specs, raw, &mut sink, &mut fx());
                    self.counters.frames += 1;
                    self.counters.decode_errors += u64::from(fed.is_err());
                }
            }
            Record::Timer { stamp, tag } if self.owns(stamp.conn) => {
                if self.ready(stamp.conn)
                    && let Some(codec) = self.open.as_mut().and_then(|e| e.codec.as_deref_mut())
                {
                    let mut sink = Deliver {
                        handler: &mut self.handler,
                        stamp: *stamp,
                    };
                    feed_timer(codec, *stamp, *tag, &mut sink, &mut fx());
                    self.counters.timers += 1;
                }
            }
            Record::HttpResult { stamp, tag, result } if self.owns(stamp.conn) => {
                let held = (*stamp, *tag, result.clone());
                match &mut self.open {
                    // Before the epoch's opening call: held until its codec is built.
                    Some(epoch) if epoch.key == stamp.conn && epoch.codec.is_none() => {
                        epoch.held.push(held);
                    }
                    Some(epoch) if epoch.key == stamp.conn => self.answer(held),
                    _ => self.counters.stale += 1,
                }
            }
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

    /// Hands an HTTP result of the open epoch to its codec, which is built.
    fn answer(&mut self, (stamp, tag, result): Held) {
        if let Some(codec) = self.open.as_mut().and_then(|e| e.codec.as_deref_mut()) {
            let mut sink = Deliver {
                handler: &mut self.handler,
                stamp,
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

/// Stamps each event a replayed codec pushes with its input's recorded stamp and hands it to
/// the handler: the codec is always the open epoch's, so every event passes.
struct Deliver<'a, H> {
    handler: &'a mut H,
    stamp: Stamp,
}

impl<H: MdHandler> MdSink for Deliver<'_, H> {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.handler.on_md(Envelope::new(self.stamp, meta, ev));
    }
}
