//! The diff-depth book (`<symbol>@depth@100ms`, [`BOOK_DIFF`]), anchored on a REST snapshot as
//! Binance documents for a local order book ("How to manage a local order book correctly"):
//!
//! 1. From its subscription, a book buffers its events. The first event asks for the snapshot,
//!    `GET /fapi/v1/depth?symbol=<SYMBOL>&limit=<limit>`, as an [`Effect::Http`] with the
//!    configured timeout and the limit's documented weight against the REST budget.
//! 2. The response pushes `BookSnapshotBegin` under the book's next epoch, the snapshot's levels
//!    and `BookSnapshotEnd`, then the buffered events and those that follow as `Level` deltas:
//!    an event whose `u` is below the snapshot's `lastUpdateId` is dropped; the first applied
//!    must straddle it (`U <= lastUpdateId <= u`); each one after must name the last applied
//!    `u` as its `pu`.
//! 3. An event whose `u` is not above the last applied `u` was already applied: it is ignored as
//!    a duplicate. Any other break (a gap, events out of order, a snapshot older than the first
//!    event after it) pushes `Health { feed: Book(BOOK_DIFF), h: Gap }` with that event's
//!    metadata, asks for a new snapshot at once, and buffers from that event: a fresh anchor
//!    under a new epoch.
//! 4. A request that fails (no response, or a response that is refused or cannot be read) asks
//!    for a timer of the configured retry interval, which asks again. The buffer is emptied on
//!    a failure, since the next snapshot is taken after the events it held; it holds at most what
//!    arrives during one request and one retry interval.
//!
//! Every response and frame is decoded whole before anything is pushed (decision 0014 item 2).
//! A response to a request this codec no longer waits for (a retried or unsubscribed book) is
//! refused with nothing pushed; a frame for an instrument whose diff-depth book is not
//! subscribed on this connection (one still in flight after an unsubscribe) is read and dropped.

use std::collections::BTreeMap;

use fbc_core::{
    BookSide, DecodeError, Effect, Effects, ExchNs, ExchTsKind, Feed, FeedHealth, HttpFailure,
    HttpMethod, HttpRequest, HttpResponse, HttpTag, InstrumentId, InstrumentSpec, Lvl, MdEvent,
    MdSink, OpKind, RateCharge, SpecTable, TimerTag, TrafficClass, VenueMeta, WireSlice, WireUrl,
};
use serde_json::{Map, Value};

use crate::BOOK_DIFF;
use crate::config::SnapshotSettings;
use crate::md::level_list;

/// One diff-depth event, decoded.
pub(crate) struct DiffEvent {
    meta: VenueMeta,
    /// `U`, its first update id.
    first: u64,
    /// `u`, its final update id.
    last: u64,
    /// `pu`, the previous event's `u`.
    prev: u64,
    levels: Vec<(BookSide, Lvl)>,
}

fn id(data: &Map<String, Value>, key: &'static str) -> Result<u64, DecodeError> {
    data.get(key)
        .and_then(Value::as_u64)
        .ok_or(DecodeError::Malformed(key))
}

/// Milliseconds in `data[key]` as an exchange time.
fn millis(data: &Map<String, Value>, key: &'static str) -> Result<ExchNs, DecodeError> {
    let ms = data.get(key).and_then(Value::as_i64);
    let ns = ms.and_then(|ms| ms.checked_mul(1_000_000));
    ns.map(ExchNs).ok_or(DecodeError::Malformed(key))
}

/// Both sides of `data`, bids then asks, from `bids_key` and `asks_key`, each at most `max`
/// levels when given.
fn sides(
    spec: &InstrumentSpec,
    data: &Map<String, Value>,
    [bids_key, asks_key]: [&'static str; 2],
    max: Option<(u16, [&'static str; 2])>,
) -> Result<Vec<(BookSide, Lvl)>, DecodeError> {
    let mut out = Vec::new();
    for (n, (side, key)) in [(BookSide::Bid, bids_key), (BookSide::Ask, asks_key)]
        .into_iter()
        .enumerate()
    {
        let list = data.get(key).and_then(Value::as_array);
        let list = list.ok_or(DecodeError::Malformed(key))?;
        if let Some((max, too_many)) = max
            && list.len() > usize::from(max)
        {
            return Err(DecodeError::Malformed(too_many[n]));
        }
        out.extend(level_list(spec, list, key)?.into_iter().map(|l| (side, l)));
    }
    Ok(out)
}

/// The diff-depth fields of `data`, whose event type, symbol and `meta` (`u`, `T`) the caller
/// has read.
pub(crate) fn decode_event(
    spec: &InstrumentSpec,
    data: &Map<String, Value>,
    meta: VenueMeta,
) -> Result<DiffEvent, DecodeError> {
    let last = id(data, "u")?;
    let first = id(data, "U")?;
    if first > last {
        return Err(DecodeError::Malformed("U"));
    }
    let prev = id(data, "pu")?;
    let levels = sides(spec, data, ["b", "a"], None)?;
    Ok(DiffEvent {
        meta,
        first,
        last,
        prev,
        levels,
    })
}

/// A `GET /fapi/v1/depth` response: `lastUpdateId`, `E`, `T`, `bids`, `asks`.
struct Snapshot {
    meta: VenueMeta,
    id: u64,
    levels: Vec<(BookSide, Lvl)>,
}

fn decode_snapshot(
    spec: &InstrumentSpec,
    body: &[u8],
    limit: u16,
) -> Result<Snapshot, DecodeError> {
    let value: Value =
        serde_json::from_slice(body).map_err(|_| DecodeError::Malformed("not JSON"))?;
    let Value::Object(data) = value else {
        return Err(DecodeError::Malformed("not a JSON object"));
    };
    let id = id(&data, "lastUpdateId")?;
    let ts = millis(&data, "T")?;
    let too_many = [
        "bids: more levels than the limit asked for",
        "asks: more levels than the limit asked for",
    ];
    let levels = sides(spec, &data, ["bids", "asks"], Some((limit, too_many)))?;
    Ok(Snapshot {
        meta: VenueMeta {
            exch_ts: Some(ts),
            exch_ts_kind: ExchTsKind::MatchingEngine,
            venue_seq: Some(id),
        },
        id,
        levels,
    })
}

/// Where a book's anchor stands.
enum Anchor {
    /// Subscribed; no event yet, so nothing asked for.
    Subscribed,
    /// A snapshot request `tag` is out; events are buffered.
    Requested {
        tag: HttpTag,
        buffer: Vec<DiffEvent>,
    },
    /// A request failed; `timer` asks again; events are buffered.
    Retrying {
        timer: TimerTag,
        buffer: Vec<DiffEvent>,
    },
    /// Anchored: `last` is the last applied `u`, or the snapshot's `lastUpdateId` while
    /// `fresh` (no event applied since it).
    Live { last: u64, fresh: bool },
}

/// One instrument's diff-depth book on this connection.
struct Book {
    /// Its snapshot request's URL.
    url: String,
    /// How its snapshot is asked for.
    cfg: SnapshotSettings,
    anchor: Anchor,
}

/// Every diff-depth book subscribed on one connection.
#[derive(Default)]
pub(crate) struct DiffBooks {
    books: BTreeMap<InstrumentId, Book>,
    /// Each instrument's last snapshot epoch on this connection, kept across an unsubscribe so
    /// an epoch is never reused.
    epochs: BTreeMap<InstrumentId, u32>,
    /// The last tag given to a request or a timer.
    tags: u64,
}

/// What a book needs to ask for its snapshot.
struct Asker<'a> {
    inst: InstrumentId,
    url: &'a str,
    cfg: &'a SnapshotSettings,
}

impl<'a> Asker<'a> {
    fn of(inst: InstrumentId, book: &'a Book) -> Asker<'a> {
        Asker {
            inst,
            url: &book.url,
            cfg: &book.cfg,
        }
    }

    /// Asks for the snapshot under the next tag.
    fn ask(&self, tags: &mut u64, fx: &mut Effects) -> HttpTag {
        *tags += 1;
        let tag = HttpTag(*tags);
        fx.push(Effect::Http {
            tag,
            req: HttpRequest {
                method: HttpMethod::Get,
                url: WireUrl::plain(self.url),
                headers: Vec::new(),
                body: WireSlice::plain(Vec::new()),
            },
            rpc: None,
            timeout: self.cfg.timeout,
            class: TrafficClass::Normal,
            charge: RateCharge {
                op: OpKind::Rest,
                inst: Some(self.inst),
                weight: self.cfg.weight,
            },
        });
        tag
    }
}

fn push_levels(
    inst: InstrumentId,
    meta: VenueMeta,
    levels: &[(BookSide, Lvl)],
    sink: &mut dyn MdSink,
) {
    for &(side, Lvl { px, qty }) in levels {
        let book = BOOK_DIFF;
        sink.push(
            meta,
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

/// Applies `ev` to `anchor`: buffers it, applies it, drops it, or breaks the anchor.
fn apply(
    anchor: &mut Anchor,
    asker: &Asker<'_>,
    tags: &mut u64,
    ev: DiffEvent,
    sink: &mut dyn MdSink,
    fx: &mut Effects,
) {
    let (last, fresh) = match anchor {
        Anchor::Subscribed => {
            let tag = asker.ask(tags, fx);
            *anchor = Anchor::Requested {
                tag,
                buffer: vec![ev],
            };
            return;
        }
        Anchor::Requested { buffer, .. } | Anchor::Retrying { buffer, .. } => {
            buffer.push(ev);
            return;
        }
        Anchor::Live { last, fresh } => (last, fresh),
    };
    let follows = if *fresh {
        if ev.last < *last {
            // Before the snapshot: already in it.
            return;
        }
        ev.first <= *last
    } else {
        if ev.last <= *last {
            // Already applied: a duplicate.
            return;
        }
        ev.prev == *last
    };
    if follows {
        push_levels(asker.inst, ev.meta, &ev.levels, sink);
        (*last, *fresh) = (ev.last, false);
        return;
    }
    let (inst, feed, h) = (asker.inst, Feed::Book(BOOK_DIFF), FeedHealth::Gap);
    sink.push(ev.meta, MdEvent::Health { inst, feed, h });
    let tag = asker.ask(tags, fx);
    *anchor = Anchor::Requested {
        tag,
        buffer: vec![ev],
    };
}

impl DiffBooks {
    /// Starts buffering `inst`'s book, unless it is subscribed already; its snapshot is asked
    /// for at `cfg`'s limit from `cfg`'s base URL, the instrument spelled `symbol`.
    pub fn subscribe(&mut self, inst: InstrumentId, symbol: &str, cfg: &SnapshotSettings) {
        self.books.entry(inst).or_insert_with(|| Book {
            url: format!(
                "{}/fapi/v1/depth?symbol={symbol}&limit={}",
                cfg.base_url, cfg.limit
            ),
            cfg: cfg.clone(),
            anchor: Anchor::Subscribed,
        });
    }

    /// Forgets `inst`'s book: its buffer, and any request or timer it waits for.
    pub fn unsubscribe(&mut self, inst: InstrumentId) {
        self.books.remove(&inst);
    }

    /// One event of `inst`'s book, dropped when the book is not subscribed.
    pub fn on_event(
        &mut self,
        inst: InstrumentId,
        ev: DiffEvent,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        if let Some(book) = self.books.get_mut(&inst) {
            let Book { url, cfg, anchor } = book;
            let asker = Asker { inst, url, cfg };
            apply(anchor, &asker, &mut self.tags, ev, sink, fx);
        }
    }

    /// The response to snapshot request `tag`, or why none came.
    pub fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        let waiting = self.books.iter_mut().find(
            |(_, book)| matches!(book.anchor, Anchor::Requested { tag: out, .. } if out == tag),
        );
        let Some((&inst, book)) = waiting else {
            return Err(DecodeError::Malformed(
                "a response to no pending snapshot request",
            ));
        };
        let Book { url, cfg, anchor } = book;
        // A failure is retried: with `Ok` when no response came, with the error when one came
        // and was refused or could not be read.
        let snapshot = match resp {
            Err(_) => Err(Ok(())),
            Ok(resp) if resp.status != 200 => Err(Err(DecodeError::Malformed(
                "the venue refused the depth snapshot request",
            ))),
            Ok(resp) => specs
                .get(inst)
                .ok_or(DecodeError::UnknownInstrument)
                .and_then(|spec| decode_snapshot(spec, resp.body, cfg.limit))
                .map_err(Err),
        };
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(result) => {
                self.tags += 1;
                let timer = TimerTag(self.tags);
                fx.push(Effect::Timer {
                    tag: timer,
                    after: cfg.retry,
                });
                *anchor = Anchor::Retrying {
                    timer,
                    buffer: Vec::new(),
                };
                return result;
            }
        };
        let epoch = self.epochs.entry(inst).or_insert(0);
        *epoch = epoch.wrapping_add(1);
        let (book_id, epoch, meta) = (BOOK_DIFF, *epoch, snapshot.meta);
        sink.push(
            meta,
            MdEvent::BookSnapshotBegin {
                inst,
                book: book_id,
                epoch,
            },
        );
        push_levels(inst, meta, &snapshot.levels, sink);
        sink.push(
            meta,
            MdEvent::BookSnapshotEnd {
                inst,
                book: book_id,
            },
        );
        let mut buffer = Vec::new();
        if let Anchor::Requested { buffer: held, .. } = anchor {
            buffer = core::mem::take(held);
        }
        *anchor = Anchor::Live {
            last: snapshot.id,
            fresh: true,
        };
        let asker = Asker { inst, url, cfg };
        for ev in buffer {
            apply(anchor, &asker, &mut self.tags, ev, sink, fx);
        }
        Ok(())
    }

    /// Retry timer `tag` fired: its book asks for its snapshot again.
    pub fn on_timer(&mut self, tag: TimerTag, fx: &mut Effects) {
        let retrying = self.books.iter_mut().find(
            |(_, book)| matches!(book.anchor, Anchor::Retrying { timer, .. } if timer == tag),
        );
        if let Some((&inst, book)) = retrying {
            let request = Asker::of(inst, book).ask(&mut self.tags, fx);
            if let Anchor::Retrying { buffer, .. } = &mut book.anchor {
                let buffer = core::mem::take(buffer);
                book.anchor = Anchor::Requested {
                    tag: request,
                    buffer,
                };
            }
        }
    }
}
