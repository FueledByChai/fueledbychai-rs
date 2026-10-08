//! The toy's market data (FBC-u1d): its two book channels on one connection and its keepalive.
//! [`BOOK`] takes its snapshot in one frame; [`ANCHORED_BOOK`] declares `rest_anchor` and takes
//! its snapshot over HTTP, holding its deltas until the snapshot comes. Both are sequenced plus
//! one per instrument and channel.
//!
//! Out: `sub|add=<sym>@<book>,...|remove=...` per subscribe call (an empty part left out), one
//! `Subscribe` unit counted against its instrument when the call names one; a `GET
//! <anchor>/book/<sym>` per anchor ([`ANCHOR_TIMEOUT`], one `Rest` unit); the frame `ping` every
//! [`KEEPALIVE_EVERY`] ([`MdCodec::keepalive`]).
//!
//! In, one record per frame, `<levels>` being `bid=<px>:<qty>,...|ask=<px>:<qty>,...` (prices
//! ticks, sizes lots, either list possibly empty):
//!
//! - `snap|sym|book|seq|ts?|<levels>`: [`BOOK`]'s whole snapshot, decoded whole or not at all
//!   (record 0014 item 2). One before a sequence the channel had already seen, applied or
//!   dropped while it waited (the delta that broke it included), is dropped: sequences never
//!   go back on a connection (Codex r4216289341, r4216898616).
//! - `delta|sym|book|seq|ts?|<levels>`: levels set (zero removes one) on either channel. A
//!   `seq` other than the channel's last plus one is a gap: `Health { feed: Feed::Book(book),
//!   h: Gap }` names the channel that broke, the other is untouched, and the broken one waits
//!   for its next snapshot ([`ANCHORED_BOOK`] asks for one at once).
//! - The anchor's response body: `anchor|sym|seq|<levels>`, the snapshot at `seq`. Held deltas
//!   up to `seq` are dropped and the rest must follow it plus one, or the anchor is asked for
//!   again and nothing is pushed, those after it still held. Past [`MAX_HELD`] deltas, or at a
//!   gap, the anchor is asked for again holding the delta that overflowed or broke the
//!   sequence, the first the new anchor must reach, and an anchor before the highest delta it
//!   held is asked for again (Codex r4216289348). After a gap an anchor before the sequence
//!   the channel had reached is asked for again too: sequences never go back on a connection.
//!   No sequence follows `u64::MAX`: a delta after an anchor or delta there is a gap. A
//!   failure, a status other than 200 or a body it cannot decode asks again after
//!   [`ANCHOR_RETRY`].
//!
//! A record with more levels on a side than its channel's declared `max_depth` is malformed,
//! nothing of it pushed (Codex r4216289357).
//!
//! A frame for a channel not subscribed is dropped; an anchor answered after its subscription
//! was removed, or superseded, is ignored.

use std::collections::BTreeMap;

use fbc_core::{
    BookId, BookSide, ConfigError, DecodeError, DecodeScope, Effect, Effects, Feed, FeedHealth,
    HttpFailure, HttpMethod, HttpRequest, HttpResponse, HttpTag, Inbound, InboundSpans,
    InstrumentId, Keepalive, KeepaliveKind, Lots, MdCodec, MdEvent, MdSink, MonoNs, OpKind,
    RateCharge, RawFrame, SpecTable, StreamId, Subscription, Ticks, TimerTag, TrafficClass,
    VenueError, VenueMeta, WallNs, WireSlice, WireUrl,
};

use super::decode::{Record, record};
use super::url::{echoed, join, occurrences, secrets};
use super::{
    ANCHOR_RETRY, ANCHOR_TIMEOUT, ANCHOR_URL_KEY, ANCHORED_BOOK, BOOK, KEEPALIVE_EVERY, MAX_HELD,
    caps,
};
use DecodeError::Malformed;

/// Where one subscribed channel stands.
#[derive(Clone, Debug)]
enum Chan {
    /// No snapshot yet, or a gap since the last: deltas are dropped, and a snapshot before
    /// `floor`, the highest sequence the channel had seen, too (Codex r4216289341,
    /// r4216898616).
    Waiting { floor: u64 },
    /// An anchor is asked for at `url` under `tag`; deltas are held in order until it comes.
    /// An anchor before `floor`, the sequence the channel had already reached, is asked for
    /// again: it would move the book back (Codex r4216139627).
    Anchoring {
        tag: u64,
        url: WireUrl,
        held: Vec<Levels>,
        floor: u64,
    },
    /// Anchored at this sequence.
    Live { seq: u64 },
}

/// One `snap`, `delta` or `anchor` record, decoded whole.
#[derive(Clone, Debug)]
struct Levels {
    meta: VenueMeta,
    seq: u64,
    levels: Vec<(BookSide, Ticks, Lots)>,
}

/// The conformance toy's market-data codec, one per connection epoch.
#[derive(Clone, Debug)]
pub struct ToyMd {
    stream: StreamId,
    /// The base URL anchors are asked under, with its credential spans; without one an
    /// anchored channel is refused.
    anchor: Option<WireUrl>,
    chans: BTreeMap<(InstrumentId, BookId), Chan>,
    /// The next tag an anchor's request and its retry timer share.
    next_tag: u64,
    /// The snapshots pushed, each one's `epoch`.
    snapshots: u32,
    /// The credentials of the URLs it was given, named wherever a frame or response echoes
    /// them (Codex r4219753519, r4219929319).
    secrets: Vec<String>,
}

impl ToyMd {
    /// A codec on `stream` with no anchor URL: [`ANCHORED_BOOK`] is refused as configuration
    /// ([`ANCHOR_URL_KEY`]).
    pub fn new(stream: StreamId) -> ToyMd {
        ToyMd {
            stream,
            anchor: None,
            chans: BTreeMap::new(),
            next_tag: 1,
            snapshots: 0,
            secrets: Vec::new(),
        }
    }

    /// A codec on `stream` asking [`ANCHORED_BOOK`]'s anchors under `base`, which holds no
    /// credential.
    pub fn with_anchor(stream: StreamId, base: impl Into<String>) -> ToyMd {
        ToyMd::with_anchor_url(stream, WireUrl::plain(base))
    }

    /// A codec on `stream` asking [`ANCHORED_BOOK`]'s anchors under `base`, every anchor's URL
    /// keeping its credential spans (FBC-ja3, Codex r4172917294).
    pub fn with_anchor_url(stream: StreamId, base: WireUrl) -> ToyMd {
        ToyMd {
            secrets: secrets(&base),
            anchor: Some(base),
            ..ToyMd::new(stream)
        }
    }

    /// This codec, on the connection at `url`: its credentials are named wherever a frame
    /// echoes them.
    pub fn socket_url(mut self, url: &WireUrl) -> ToyMd {
        self.secrets.extend(secrets(url));
        self
    }

    /// `sub` is a declared channel of an instrument in `specs`; its wire spelling.
    fn check(&self, sub: &Subscription, specs: &SpecTable) -> Result<String, VenueError> {
        let spec = specs.get(sub.inst);
        let spec = spec.ok_or(VenueError::UnknownInstrument(sub.inst))?;
        let Feed::Book(book) = sub.feed else {
            return Err(VenueError::UnsupportedFeed(*sub));
        };
        let declared = caps().md.books.get(usize::from(book.0)).copied();
        let declared = declared.ok_or(VenueError::UnsupportedFeed(*sub))?;
        if declared.rest_anchor && self.anchor.is_none() {
            return Err(VenueError::Config(ConfigError::Invalid {
                key: ANCHOR_URL_KEY,
                reason: "no anchor URL is configured for the anchored book channel",
            }));
        }
        Ok(format!("{}@{}", spec.venue_symbol.as_wire(), book.0))
    }

    /// Asks for `inst`'s anchor at `url` under a fresh tag, which it returns.
    fn ask_anchor(&mut self, inst: InstrumentId, url: &WireUrl, fx: &mut Effects) -> u64 {
        let tag = self.next_tag;
        self.next_tag += 1;
        fx.push(Effect::Http {
            tag: HttpTag(tag),
            req: HttpRequest {
                method: HttpMethod::Get,
                url: url.clone(),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: ANCHOR_TIMEOUT,
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Rest, Some(inst)),
        });
        tag
    }

    /// The anchored channel waiting on `tag`.
    fn anchoring(&self, tag: u64) -> Option<InstrumentId> {
        self.chans.iter().find_map(|(&(inst, _), chan)| match chan {
            Chan::Anchoring { tag: t, .. } if *t == tag => Some(inst),
            _ => None,
        })
    }

    /// Pushes a snapshot of `book` from `s`, under the next epoch.
    fn push_snapshot(
        &mut self,
        inst: InstrumentId,
        book: BookId,
        s: &Levels,
        sink: &mut dyn MdSink,
    ) {
        let epoch = self.snapshots;
        self.snapshots = self.snapshots.wrapping_add(1);
        sink.push(s.meta, MdEvent::BookSnapshotBegin { inst, book, epoch });
        push_levels(inst, book, s, sink);
        sink.push(s.meta, MdEvent::BookSnapshotEnd { inst, book });
    }

    /// A gap on `book`, live at `seq`, at the delta `d`: reported, and the channel waits for
    /// its next snapshot, an anchor having to reach both.
    fn gap(
        &mut self,
        (inst, book): (InstrumentId, BookId),
        seq: u64,
        d: Levels,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        let feed = Feed::Book(book);
        sink.push(
            d.meta,
            MdEvent::Health {
                inst,
                feed,
                h: FeedHealth::Gap,
            },
        );
        let next = self.restart(inst, book, specs, (vec![d], seq), fx);
        self.chans.insert((inst, book), next);
    }

    /// Where a channel starts, or starts again: [`ANCHORED_BOOK`] asks for its anchor, holding
    /// `held`, deltas already seen that the anchor must reach (Codex r4203051298), and refusing
    /// an anchor before `floor`; a channel snapshotted in its own frames drops them, raising
    /// its floor to the highest (Codex r4216898616). Subscribe checked that the instrument is
    /// in `specs` and the anchor's base is set.
    fn restart(
        &mut self,
        inst: InstrumentId,
        book: BookId,
        specs: &SpecTable,
        (held, floor): (Vec<Levels>, u64),
        fx: &mut Effects,
    ) -> Chan {
        let base = self.anchor.as_ref().filter(|_| book == ANCHORED_BOOK);
        match (base, specs.get(inst)) {
            (Some(base), Some(spec)) => {
                let url = join(base, &format!("/book/{}", spec.venue_symbol.as_wire()));
                let tag = self.ask_anchor(inst, &url, fx);
                Chan::Anchoring {
                    tag,
                    url,
                    held,
                    floor,
                }
            }
            _ => Chan::Waiting {
                floor: held.iter().map(|h| h.seq).fold(floor, u64::max),
            },
        }
    }

    fn delta(
        &mut self,
        key: (InstrumentId, BookId),
        d: Levels,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        match self.chans.get_mut(&key) {
            None => {}
            // Dropped, but seen: a snapshot must reach it (Codex r4216898616).
            Some(Chan::Waiting { floor }) => *floor = d.seq.max(*floor),
            Some(Chan::Anchoring { held, .. }) if held.len() < MAX_HELD => held.push(d),
            // Held past the bound: ask again rather than grow, holding `d` as the first delta
            // the new anchor must reach (Codex r4203051298), and refusing one before the
            // highest delta held, which `d` may be behind (Codex r4216289348).
            Some(Chan::Anchoring { held, floor, .. }) => {
                let floor = held.iter().map(|h| h.seq).fold(*floor, u64::max);
                let next = self.restart(key.0, key.1, specs, (vec![d], floor), fx);
                self.chans.insert(key, next);
            }
            Some(Chan::Live { seq }) if seq.checked_add(1) == Some(d.seq) => {
                *seq = d.seq;
                push_levels(key.0, key.1, &d, sink);
            }
            Some(&mut Chan::Live { seq }) => self.gap(key, seq, d, specs, sink, fx),
        }
    }

    /// The anchor `tag` asked for answered with `resp`.
    fn anchored(
        &mut self,
        inst: InstrumentId,
        resp: HttpResponse<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let key = (inst, ANCHORED_BOOK);
        let body = core::str::from_utf8(resp.body).map_err(|_| Malformed("body"))?;
        let r = record(RawFrame::Text(body))?;
        if r.kind != "anchor" || specs.by_symbol(r.get("sym")?).map(|s| s.id) != Some(inst) {
            return Err(Malformed("anchor"));
        }
        let snap = levels(&r, depth(ANCHORED_BOOK))?;
        let Some(&Chan::Anchoring {
            ref held, floor, ..
        }) = self.chans.get(&key)
        else {
            return Ok(());
        };
        let after: Vec<Levels> = held.iter().filter(|d| d.seq > snap.seq).cloned().collect();
        // Each held delta follows the one before plus one, checked so that nothing overflows:
        // no sequence follows u64::MAX (Codex r4203051299).
        let mut last = snap.seq;
        let chained = after.iter().all(|d| {
            let follows = last.checked_add(1) == Some(d.seq);
            last = d.seq;
            follows
        });
        if !chained || snap.seq < floor {
            // The snapshot is older than a delta already missed, or than the book had reached:
            // ask again, push nothing, and keep holding the deltas after it.
            let next = self.restart(inst, ANCHORED_BOOK, specs, (after, floor), fx);
            self.chans.insert(key, next);
            return Ok(());
        }
        self.push_snapshot(inst, ANCHORED_BOOK, &snap, sink);
        let mut seq = snap.seq;
        for d in &after {
            push_levels(inst, ANCHORED_BOOK, d, sink);
            seq = d.seq;
        }
        self.chans.insert(key, Chan::Live { seq });
        Ok(())
    }
}

/// Pushes `s`'s levels on `book`.
fn push_levels(inst: InstrumentId, book: BookId, s: &Levels, sink: &mut dyn MdSink) {
    for &(side, px, qty) in &s.levels {
        sink.push(
            s.meta,
            MdEvent::Level {
                inst,
                book,
                side,
                px,
                qty,
            },
        );
    }
}

/// The most levels per side `book` declares; zero for a book it does not declare.
fn depth(book: BookId) -> usize {
    let declared = caps().md.books.get(usize::from(book.0)).copied();
    declared.map_or(0, |b| usize::from(b.max_depth))
}

/// The sequence, time and levels of a `snap`, `delta` or `anchor` record, all or nothing, no
/// side past `depth` levels.
fn levels(r: &Record<'_>, depth: usize) -> Result<Levels, DecodeError> {
    let meta = r.meta(true)?;
    let seq = r.num("seq")?;
    let mut levels = Vec::new();
    for (key, side) in [("bid", BookSide::Bid), ("ask", BookSide::Ask)] {
        let listed = r.get(key)?.split(',').filter(|l| !l.is_empty());
        if listed.clone().count() > depth {
            return Err(Malformed("depth"));
        }
        for level in listed {
            let (px, qty) = level.split_once(':').ok_or(Malformed(key))?;
            let px = Ticks(px.parse().map_err(|_| Malformed(key))?);
            let qty = qty.parse().ok().and_then(Lots::new).ok_or(Malformed(key))?;
            levels.push((side, px, qty));
        }
    }
    Ok(Levels { meta, seq, levels })
}

impl MdCodec for ToyMd {
    /// Nothing: the subscribe call says what the stream carries.
    fn on_open(&mut self, _fx: &mut Effects) {}

    /// Checks every subscription before asking for anything: an instrument missing from
    /// `specs`, a feed other than a declared book channel, or [`ANCHORED_BOOK`] without an
    /// anchor URL is refused, nothing sent (Codex r4172917339). Every instrument, added or
    /// removed, is checked before any feed, so a missing one is named first (Codex
    /// r4216704345).
    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        let unknown = add
            .iter()
            .chain(remove)
            .find(|s| specs.get(s.inst).is_none());
        if let Some(s) = unknown {
            return Err(VenueError::UnknownInstrument(s.inst));
        }
        let spell = |subs: &[Subscription]| -> Result<Vec<String>, VenueError> {
            subs.iter().map(|s| self.check(s, specs)).collect()
        };
        let (adds, removes) = (spell(add)?, spell(remove)?);
        if adds.is_empty() && removes.is_empty() {
            return Ok(());
        }
        let mut text = String::from("sub");
        for (part, syms) in [("add", &adds), ("remove", &removes)] {
            if !syms.is_empty() {
                text += &format!("|{part}={}", syms.join(","));
            }
        }
        let mut insts = add.iter().chain(remove).map(|s| s.inst);
        let inst = insts.next().filter(|first| insts.all(|i| i == *first));
        fx.push(Effect::Send {
            stream: self.stream,
            frame: WireSlice::plain(text.into_bytes()),
            rpc: None,
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Subscribe, inst),
        });
        for sub in remove {
            if let Feed::Book(book) = sub.feed {
                self.chans.remove(&(sub.inst, book));
            }
        }
        for sub in add {
            if let Feed::Book(book) = sub.feed
                && !self.chans.contains_key(&(sub.inst, book))
            {
                let chan = self.restart(sub.inst, book, specs, (Vec::new(), 0), fx);
                self.chans.insert((sub.inst, book), chan);
            }
        }
        Ok(())
    }

    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let r = record(f)?;
        let inst = specs.by_symbol(r.get("sym")?);
        let inst = inst.ok_or(DecodeError::UnknownInstrument)?.id;
        let book = BookId(r.num("book")?);
        if caps().md.books.len() <= usize::from(book.0) {
            return Err(Malformed("book"));
        }
        // The whole record is read before anything is pushed.
        let s = levels(&r, depth(book))?;
        match r.kind {
            "snap" if book == BOOK => {
                // A snapshot before the sequence the channel had reached is dropped.
                let floor = match self.chans.get(&(inst, book)) {
                    Some(&Chan::Waiting { floor }) => Some(floor),
                    Some(&Chan::Live { seq }) => Some(seq),
                    _ => None,
                };
                if floor.is_some_and(|floor| floor <= s.seq) {
                    self.push_snapshot(inst, book, &s, sink);
                    self.chans.insert((inst, book), Chan::Live { seq: s.seq });
                }
                Ok(())
            }
            "snap" => Err(Malformed("an anchored book's snapshot comes over HTTP")),
            "delta" => {
                self.delta((inst, book), s, specs, sink, fx);
                Ok(())
            }
            _ => Err(Malformed("kind")),
        }
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let Some(inst) = self.anchoring(tag.0) else {
            return Ok(());
        };
        let retry = Effect::Timer {
            tag: TimerTag(tag.0),
            after: ANCHOR_RETRY,
        };
        match resp {
            Ok(r) if r.status == 200 => {
                let anchored = self.anchored(inst, r, specs, sink, fx);
                if anchored.is_err() {
                    fx.push(retry);
                }
                anchored
            }
            _ => {
                fx.push(retry);
                Ok(())
            }
        }
    }

    /// An anchor's retry fell due: asked again, unless the channel moved on meanwhile. The toy
    /// times no feed's cadence, so it reports nothing here.
    fn on_timer(
        &mut self,
        tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        _sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        let Some(inst) = self.anchoring(tag.0) else {
            return;
        };
        if let Some(Chan::Anchoring { url, .. }) = self.chans.get(&(inst, ANCHORED_BOOK)) {
            let url = url.clone();
            let fresh = self.ask_anchor(inst, &url, fx);
            if let Some(Chan::Anchoring { tag, .. }) = self.chans.get_mut(&(inst, ANCHORED_BOOK)) {
                *tag = fresh;
            }
        }
    }

    fn keepalive(&self) -> Option<Keepalive> {
        Some(Keepalive {
            interval: KEEPALIVE_EVERY,
            kind: KeepaliveKind::Frame(WireSlice::plain(b"ping".to_vec())),
            charge: RateCharge::one(OpKind::Control, None),
        })
    }

    /// Public market data names no credential of its own; a frame or an anchor's response
    /// echoing the credentials of the URLs it was given names them.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Http(_, resp) => echoed(&resp, &self.secrets),
            Inbound::Frame(frame) => {
                InboundSpans::frame(occurrences(frame.bytes(), &self.secrets, &[]))
            }
        }
    }
}
