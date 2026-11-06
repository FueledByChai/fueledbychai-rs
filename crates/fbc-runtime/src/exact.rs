//! Exact replay of an order-entry session (FBC-qwq; decisions 0006, 0024, 0028, 0138, and 0142,
//! which states the rules [`ExactReport`] gives): every frame and HTTP request a replayed codec
//! asks for, compared with the ones the journal holds of its session, outside their redaction
//! spans, signatures included. [`ExactReport`] says what the comparison finds;
//! [`ExecReplay`](crate::ExecReplay) hands this module what each of its calls asked for and each
//! outbound frame and HTTP request the journal holds of its session, in journal order.

use fbc_core::{ConnKey, Effect, Effects, MonoNs, StreamId};
use fbc_journal::{HttpRequestRec, Marker, Opcode, Record};

/// What exact replay found of a session's outbound frames and HTTP requests (decision 0006; 0142
/// states the rules below). The replay is exact when it found no difference and nothing
/// unwritten ([`ExactReport::is_exact`]); what a `Degraded` span may explain is excluded from
/// both, and counted.
///
/// A frame is compared on the connection epoch its stream's endpoint had open when the codec
/// asked for it, and an HTTP request on the epoch whose call asked for it, as live the session
/// writes and journals them (0115). Both are compared as the journal's reader returns a record,
/// every span blanked ([`Record::blanked`]) and its write instant aside: what lies inside a span
/// (a credential, or the blank a replayed codec was handed back for one, 0028 item 4) is not
/// compared, though the span's place and length are, and every other byte is, a signature's
/// included (0006). A secret header's value is blanked at its length the same way. What the
/// journal does not hold of an effect, its traffic class, rate charge and timeout, is not
/// compared.
///
/// - A journaled frame or request is **matched** with the first one asked for on its epoch that
///   equals it. The session writes an epoch's frames in the order they were asked for, so a frame
///   asked for before the matched one that the journal does not hold was never written. A request
///   is matched wherever it lies: one asked for while a write waits starts at once, ahead of those
///   queued behind the write.
/// - One the replay did not reproduce is a **difference**, with what was asked for in its place,
///   if anything: the first frame on its epoch, or the first request on its epoch under its tag,
///   still unmatched and in the place of no earlier difference. That one stays asked for, so a
///   later record may still match it and one record not reproduced leaves the rest of its epoch
///   to match; it is never counted unwritten, its difference standing for it.
/// - One asked for that no journaled record matches is **unwritten**. A frame is found so once a
///   later frame of its epoch is matched, or at once when no epoch of its stream is open; either,
///   once its epoch ends, or at the end. Live, a session writes nothing of a frame its buckets
///   refuse as its turn comes, of what an encode, `on_open` or the resync asked for that they
///   refuse all together, of an encode the codec refused or whose effects do not carry its
///   request, of what was still unwritten when its epoch ended or its control dropped, or of a
///   frame whose stream has no connection open; an HTTP request any other call asks for is
///   journaled as it starts, refused or not. But a codec that asks for something the live one
///   did not leaves the same count, so an unwritten one counts against exactness, and a caller
///   that expects some reads the count.
/// - **Excluded**, and counted: what a `Degraded` span may explain, since the gap may have dropped
///   the input that would have reproduced it, or its own record. The sink numbers every record
///   offered to it from 0, and a marker names the first it dropped and how many (decisions 0006
///   and 0139; 0142 item 8). Counting every record read but the markers, each marker moving the
///   count past the records it says were dropped, numbers each record before a gap as the sink
///   did, and each one the sink kept from its reserve while the gap was open at or past the first
///   it dropped. A difference found there is excluded, and so is a frame or request found
///   unwritten there, or asked for before a marker read since. That count is the sink's only when
///   the replay is fed one sink's records from its first, every drop marked.
///   [`JournalReader`](fbc_journal::JournalReader) reads every run written under a root and shard
///   as one sequence, each run's sink numbering from 0 again, with nothing journaled where one
///   begins: fed an earlier run's records first, the count runs past the sink's and a marker can
///   exclude what its gap does not explain. Fed a later run of one sink alone, or past a drop no
///   marker names, it runs below the sink's, and a marker reports some of what its gap may
///   explain, never excluding what it does not. So an exact report with `excluded` above zero,
///   or of a replay across a gap, does not show the session reproduced (0138).
///
/// Two cases read as differences that the session did not cause:
///
/// - **One a gap explains, found after its marker.** The sink may drop an input, or a call's
///   context, whose effect shows only in a later record: the frame of a Normal command whose
///   context it dropped (decision 0138), which may be the first record after the marker, or a
///   frame the codec writes otherwise for an input it never saw. That record is numbered past
///   the gap and is reported, since a replay across a gap is not exact (0138).
/// - **A frame one endpoint asks for on another endpoint's stream** (0115). The session writes
///   it once the asking call's own effects ran, and so possibly after a frame that stream's own
///   calls asked for later. The replay then counts it unwritten where that later frame is
///   matched, and reports its record a difference. A venue that plans one order-entry endpoint,
///   or whose calls ask only for their own endpoint's stream, has no such frame.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct ExactReport {
    /// Frames and requests the journal holds of the session that the replay's codec asked for,
    /// byte for byte outside their redaction spans.
    pub matched: u64,
    /// Frames and requests the replay's codec asked for that the journal holds none of, outside
    /// every `Degraded` span: the live session wrote nothing of them, whether it refused them or
    /// the codec asked for more than the live one did.
    pub unwritten: u64,
    /// Differences and unwritten frames or requests a `Degraded` span may explain, counted
    /// rather than reported.
    pub excluded: u64,
    /// Each frame or request the journal holds of the session that the replay did not reproduce,
    /// in journal order, outside every `Degraded` span.
    pub differences: Vec<OutboundDiff>,
}

impl ExactReport {
    /// Whether the replay reproduced every outbound frame and HTTP request the journal holds and
    /// asked for nothing more, outside what a `Degraded` span may explain: no difference and
    /// nothing unwritten.
    pub fn is_exact(&self) -> bool {
        self.differences.is_empty() && self.unwritten == 0
    }

    /// Settles what was found: a difference, or a frame or request unwritten.
    fn settle(&mut self, diff: Option<OutboundDiff>) {
        match diff {
            Some(diff) => self.differences.push(diff),
            None => self.unwritten += 1,
        }
    }
}

/// A frame or HTTP request the journal holds that the replay did not reproduce.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct OutboundDiff {
    /// The journal's record, an outbound frame or an HTTP request, its spans blanked.
    pub journaled: Record,
    /// What the replay's codec asked for in its place on that connection epoch, as the journal
    /// would hold it, at instant zero since the replay writes nothing; `None` when it asked for
    /// nothing there still unmatched.
    pub replayed: Option<Record>,
}

/// A frame or HTTP request the replay's codec asked for, as the journal would hold it.
struct Asked {
    /// The connection epoch it would be written on, or asked under.
    key: ConnKey,
    /// Its record, at instant zero, its spans blanked.
    record: Record,
    /// The `Degraded` markers read before it was asked for.
    gaps: u64,
    /// Whether a difference names it as what was asked for in its place: still asked for, so a
    /// later record may match it, but never counted unwritten.
    paired: bool,
}

/// What was found at the record numbered `at` that a `Degraded` marker read later may cover: a
/// difference, or a frame or request unwritten.
struct Found {
    at: u64,
    diff: Option<OutboundDiff>,
}

/// The comparison of one replay's outbound frames and HTTP requests with the journal's.
#[derive(Default)]
pub(crate) struct Exact {
    /// The number the sink gave the record being read.
    at: u64,
    /// The number it gave the next record.
    next: u64,
    /// The `Degraded` markers read.
    gaps: u64,
    /// What the codec asked for that no journaled record has matched yet.
    asked: Vec<Asked>,
    /// What was found since the last `Degraded` marker.
    held: Vec<Found>,
    /// What is settled.
    report: ExactReport,
}

impl Exact {
    /// Numbers `record` ([`ExactReport`] says how): every record but a `Degraded` marker takes
    /// the next number, and a marker moves the count past the records it says were dropped. A
    /// marker excludes what was found at or past the first record it names and settles the rest.
    pub(crate) fn read(&mut self, record: &Record) {
        self.at = self.next;
        let Record::Marker(Marker::Degraded { from_seq, dropped }) = record else {
            self.next += 1;
            return;
        };
        for found in std::mem::take(&mut self.held) {
            if found.at >= *from_seq {
                self.report.excluded += 1;
            } else {
                self.report.settle(found.diff);
            }
        }
        self.gaps += 1;
        self.next = self.next.saturating_add(*dropped);
    }

    /// What one call on epoch `origin` asked for, `fx`: each frame for the open epoch of the
    /// endpoint whose stream it names (`streams`, `open`, by the endpoints' places in the plan),
    /// unwritten when there is none, and each HTTP request under `origin`.
    pub(crate) fn asked(
        &mut self,
        fx: &mut Effects,
        origin: ConnKey,
        streams: &[StreamId],
        open: &[Option<ConnKey>],
    ) {
        for effect in fx.take() {
            let (key, record) = match effect {
                Effect::Send {
                    stream, frame, rpc, ..
                } => {
                    let at = streams.iter().position(|s| *s == stream);
                    let Some(key) = at.and_then(|at| open[at]) else {
                        self.held.push(Found {
                            at: self.at,
                            diff: None,
                        });
                        continue;
                    };
                    // Sent as text when its bytes are UTF-8, as the session sends it.
                    let opcode = Opcode::of(frame.bytes());
                    let rpc = rpc.map(|call| call.id);
                    let (at, conn) = (MonoNs(0), key);
                    let record = Record::Outbound {
                        at,
                        conn,
                        rpc,
                        opcode,
                        frame,
                    };
                    (key, record)
                }
                Effect::Http { tag, req, rpc, .. } => {
                    let (at, conn, req) = (MonoNs(0), origin, HttpRequestRec::from(&req));
                    let record = Record::HttpRequest {
                        at,
                        conn,
                        tag,
                        rpc,
                        req,
                    };
                    (origin, record)
                }
                Effect::Timer { .. } | Effect::Reconnect { .. } | Effect::RateLimited { .. } => {
                    continue;
                }
            };
            let (record, gaps, paired) = (record.blanked(), self.gaps, false);
            self.asked.push(Asked {
                key,
                record,
                gaps,
                paired,
            });
        }
    }

    /// `record`, an outbound frame or HTTP request the journal holds of epoch `key`, and `wire`,
    /// the same at instant zero: matched with the first frame or request asked for on `key` that
    /// equals it, else a difference, paired with the first alike one in the place of no earlier
    /// difference, which stays asked for.
    pub(crate) fn journaled(&mut self, key: ConnKey, wire: Record, record: &Record) {
        let wire = wire.blanked();
        let on = |a: &Asked| a.key == key;
        let Some(i) = self.asked.iter().position(|a| on(a) && a.record == wire) else {
            let alike = |a: &&mut Asked| on(a) && !a.paired && alike(&a.record, &wire);
            let replayed = self.asked.iter_mut().find(alike).map(|asked| {
                asked.paired = true;
                asked.record.clone()
            });
            let journaled = record.blanked();
            let diff = Some(OutboundDiff {
                journaled,
                replayed,
            });
            self.held.push(Found { at: self.at, diff });
            return;
        };
        self.report.matched += 1;
        let frame = matches!(wire, Record::Outbound { .. });
        let mut kept = Vec::with_capacity(self.asked.len());
        for (j, asked) in std::mem::take(&mut self.asked).into_iter().enumerate() {
            // The match, and on a frame's epoch each frame asked for before it, never written.
            let passed = frame && on(&asked) && matches!(asked.record, Record::Outbound { .. });
            match j.cmp(&i) {
                std::cmp::Ordering::Equal => {}
                std::cmp::Ordering::Less if passed => self.unwritten(asked),
                _ => kept.push(asked),
            }
        }
        self.asked = kept;
    }

    /// Epoch `key` ended: nothing it asked for that is still unmatched is written now.
    pub(crate) fn ended(&mut self, key: ConnKey) {
        let asked = std::mem::take(&mut self.asked);
        let (ended, kept): (Vec<Asked>, Vec<Asked>) = asked.into_iter().partition(|a| a.key == key);
        self.asked = kept;
        ended.into_iter().for_each(|asked| self.unwritten(asked));
    }

    /// `asked` will not be written: nothing when a difference stands for it; excluded when a
    /// `Degraded` marker was read since it was asked for, its record possibly among those
    /// dropped; otherwise found unwritten here.
    fn unwritten(&mut self, asked: Asked) {
        if asked.paired {
            return;
        }
        if asked.gaps < self.gaps {
            self.report.excluded += 1;
        } else {
            self.held.push(Found {
                at: self.at,
                diff: None,
            });
        }
    }

    /// What was found so far, everything asked for and still unmatched counted unwritten (or
    /// excluded, or nothing, as [`Exact::unwritten`] says).
    pub(crate) fn report(&self) -> ExactReport {
        let mut report = self.report.clone();
        for found in &self.held {
            report.settle(found.diff.clone());
        }
        for asked in self.asked.iter().filter(|a| !a.paired) {
            if asked.gaps < self.gaps {
                report.excluded += 1;
            } else {
                report.unwritten += 1;
            }
        }
        report
    }
}

/// Whether `asked` stands in the place of `journaled`: a frame for a frame, a request for a
/// request under the same tag.
fn alike(asked: &Record, journaled: &Record) -> bool {
    match (asked, journaled) {
        (Record::Outbound { .. }, Record::Outbound { .. }) => true,
        (Record::HttpRequest { tag, .. }, Record::HttpRequest { tag: journaled, .. }) => {
            tag == journaled
        }
        _ => false,
    }
}
