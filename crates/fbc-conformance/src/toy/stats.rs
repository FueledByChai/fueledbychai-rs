//! The toy's stats poll (FBC-z2s): volume and open interest, which the toy offers only over REST
//! (`MdCaps::stats` is `FeedSource::Poll`), so its factory plans them on an
//! `MdTransport::Poll` endpoint of their own and [`ToyStats`] gets them only through the HTTP
//! requests it asks for, paced by its own timer (record 0014 item 9, decision 0027; Codex
//! r4172456290, r4172752101).
//!
//! Out: a `GET <base>/stats/<sym>` per subscribed instrument ([`STATS_TIMEOUT`], one `Rest` unit
//! charged to that instrument): at once for each instrument a subscribe call adds, then for
//! every one each [`STATS_EVERY`] on the codec's timer, armed when the first stats subscription
//! comes. Once a subscribe call leaves none the timer is disarmed: it fires into nothing and
//! nothing more is asked for (Codex r4172917328); a subscription taken again arms a fresh one.
//! A poll endpoint has no socket, so the codec writes no frame.
//!
//! In: each response's body, one `stats` record (`feeds.rs`) for the instrument asked. One
//! answered after its subscription was removed is ignored; one that failed, or came back with a
//! status other than 200, pushes nothing and asks for nothing, the timer being the retry; one
//! it cannot decode whole, negative open interest or volume included, pushes nothing (Codex
//! r4172917318).
//!
//! It takes only a polled feed: a streamed one (a book, trades, mark, index or funding) is
//! refused with nothing asked for, as the socket codec refuses stats (Codex r4172917344,
//! r4172649862).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use fbc_core::{
    DecodeError, DecodeScope, Effect, Effects, FeedSource, HttpFailure, HttpMethod, HttpRequest,
    HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId, Keepalive, MdCodec, MdSink, MonoNs,
    OpKind, RateCharge, RawFrame, SpecTable, StreamId, Subscription, TimerTag, TrafficClass,
    VenueError, WallNs, WireSlice, WireUrl,
};

use super::decode::record;
use super::url::{echoed, join, occurrences, secrets};
use super::{STATS_EVERY, STATS_TIMEOUT, feed_source, feeds};
use DecodeError::Malformed;

/// The conformance toy's stats poll codec, one per poll endpoint. Its `Debug` counts the
/// credentials its base holds and shows none.
#[derive(Clone)]
pub struct ToyStats {
    stream: StreamId,
    /// The base every request is asked under, with its credential spans.
    base: WireUrl,
    /// Each instrument subscribed, with the URL its stats are asked at.
    subscribed: BTreeMap<InstrumentId, WireUrl>,
    /// The requests in flight, by tag, each for its instrument.
    asked: BTreeMap<u64, InstrumentId>,
    /// The next tag a request is asked under.
    next_tag: u64,
    /// The timer that polls, while a subscription stands.
    armed: Option<TimerTag>,
    /// The tag the next timer armed fires under.
    next_timer: u64,
    /// The credentials its base holds, named wherever a response or frame echoes them.
    secrets: Vec<String>,
}

impl core::fmt::Debug for ToyStats {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ToyStats")
            .field("stream", &self.stream)
            .field("base", &self.base)
            .field("subscribed", &self.subscribed.keys())
            .field("asked", &self.asked)
            .field("armed", &self.armed)
            .field("secrets", &self.secrets.len())
            .finish()
    }
}

impl ToyStats {
    /// A codec for the poll endpoint on `stream`, asking under `base` (the endpoint's
    /// `MdTransport::Poll` base), every request keeping its credential spans.
    pub fn new(stream: StreamId, base: WireUrl) -> ToyStats {
        ToyStats {
            stream,
            secrets: secrets(&base),
            base,
            subscribed: BTreeMap::new(),
            asked: BTreeMap::new(),
            next_tag: 1,
            armed: None,
            next_timer: 1,
        }
    }

    /// Asks for `inst`'s stats at `url` under a fresh tag.
    fn ask(&mut self, inst: InstrumentId, url: WireUrl, fx: &mut Effects) {
        let tag = self.next_tag;
        self.next_tag += 1;
        self.asked.insert(tag, inst);
        fx.push(Effect::Http {
            tag: HttpTag(tag),
            req: HttpRequest {
                method: HttpMethod::Get,
                url,
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: STATS_TIMEOUT,
            class: TrafficClass::Normal,
            charge: RateCharge::one(OpKind::Rest, Some(inst)),
        });
    }
}

impl MdCodec for ToyStats {
    /// Nothing: the subscribe call says what is polled.
    fn on_open(&mut self, _fx: &mut Effects) {}

    /// Checks every subscription before asking for anything: an instrument missing from
    /// `specs` is named first, then a feed that is not polled is refused, nothing asked for.
    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError> {
        let all = || add.iter().chain(remove);
        if let Some(s) = all().find(|s| specs.get(s.inst).is_none()) {
            return Err(VenueError::UnknownInstrument(s.inst));
        }
        if let Some(s) = all().find(|s| feed_source(s.feed) != FeedSource::Poll) {
            return Err(VenueError::UnsupportedFeed(*s));
        }
        let mut added = Vec::with_capacity(add.len());
        for sub in add {
            let spec = specs
                .get(sub.inst)
                .ok_or(VenueError::UnknownInstrument(sub.inst))?;
            let url = join(
                &self.base,
                &format!("/stats/{}", spec.venue_symbol.as_wire()),
            );
            added.push((sub.inst, url));
        }
        for sub in remove {
            self.subscribed.remove(&sub.inst);
            // An answer still in flight for it is ignored when it comes.
            self.asked.retain(|_, inst| *inst != sub.inst);
        }
        for (inst, url) in added {
            // One already subscribed is asked for no more often.
            if let Entry::Vacant(at) = self.subscribed.entry(inst) {
                at.insert(url.clone());
                self.ask(inst, url, fx);
            }
        }
        match (self.subscribed.is_empty(), self.armed) {
            // The last removed: the timer fires into nothing.
            (true, _) => self.armed = None,
            (false, Some(_)) => {}
            (false, None) => {
                let tag = TimerTag(self.next_timer);
                self.next_timer += 1;
                self.armed = Some(tag);
                fx.push(Effect::Timer {
                    tag,
                    after: STATS_EVERY,
                });
            }
        }
        Ok(())
    }

    /// A poll endpoint has no socket: a frame is a defect, refused.
    fn on_frame(
        &mut self,
        _f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(Malformed("a poll endpoint takes no frame"))
    }

    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let Some(inst) = self.asked.remove(&tag.0) else {
            return Ok(());
        };
        let resp = match resp {
            Ok(resp) if resp.status == 200 => resp,
            // The timer asks again.
            _ => return Ok(()),
        };
        let body = core::str::from_utf8(resp.body).map_err(|_| Malformed("body"))?;
        let r = record(RawFrame::Text(body))?;
        let (meta, ev) = feeds::stats(&r, inst, specs)?;
        sink.push(meta, ev);
        Ok(())
    }

    /// The poll fell due: every subscribed instrument is asked for, and the timer armed again;
    /// a timer disarmed since, or of no poll of its, fires into nothing.
    fn on_timer(
        &mut self,
        tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        _sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        if self.armed != Some(tag) {
            return;
        }
        let due: Vec<(InstrumentId, WireUrl)> = self
            .subscribed
            .iter()
            .map(|(&inst, url)| (inst, url.clone()))
            .collect();
        for (inst, url) in due {
            self.ask(inst, url, fx);
        }
        fx.push(Effect::Timer {
            tag,
            after: STATS_EVERY,
        });
    }

    /// No socket, so nothing to keep alive.
    fn keepalive(&self) -> Option<Keepalive> {
        None
    }

    /// Public stats name no credential of their own; a response or a frame echoing the
    /// credentials of its base names them.
    fn redact_inbound(&self, input: Inbound<'_>) -> InboundSpans {
        match input {
            Inbound::Http(_, resp) => echoed(&resp, &self.secrets),
            Inbound::Frame(frame) => {
                InboundSpans::frame(occurrences(frame.bytes(), &self.secrets, &[]))
            }
        }
    }
}
