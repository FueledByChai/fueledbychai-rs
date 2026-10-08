//! Paradex order book channels: `BookEvent` (template 3) decoded into book events, with every
//! seq_no discontinuity detected (decision 0022).
//!
//! A frame does not name its channel, only its market, so each connection carries at most one
//! book channel per market (the factory's `plan_md` spreads them; [`ParadexMd`]'s `subscribe`
//! refuses a second), and a frame's book is the one its market holds on the connection. The
//! venue enforces the same: a second `order_book` channel of a market "cannot share an SBE
//! session" with the first (decision 0074).
//!
//! seq_no is tracked per book. A snapshot (`pkgType` SNAPSHOT, the channel's update type `s`)
//! becomes `BookSnapshotBegin`, its levels and `BookSnapshotEnd`, and anchors the sequence. A
//! delta applies only when its seq_no is the last plus one; any other (a skipped seq_no, a
//! repeat, a backwards one) pushes `Health { feed: Book(id), h: Gap }` and asks for
//! `Effect::Reconnect` of the stream, so the runtime's reconciler subscribes once on the new
//! epoch and the limiter charges the connection and the subscription: the codec never writes a
//! subscribe frame of its own from a frame. Until the next snapshot no delta is applied, and
//! deltas before the first snapshot are dropped the same way, without a gap.
//!
//! [`ParadexMd`]: super::ParadexMd

use fbc_core::{
    BookId, BookSide, DecodeError, Effect, Effects, ExchTsKind, Feed, FeedHealth, InstrumentId,
    Lvl, MdEvent, MdSink, SpecTable, StreamId, VenueMeta,
};

use super::sbe::{Block, Message};
use super::{lots, market, micros, price, required, seq};

/// The `order_book.{market}.deltas` channel: index 0 of the caps' `books`.
pub const DELTAS: BookId = BookId(0);
/// The `order_book.{market}.interactive_deltas` channel: index 1 of the caps' `books`.
pub const INTERACTIVE_DELTAS: BookId = BookId(1);

/// Each book channel's feed type, the channel being `order_book.{market_symbol}.{feed_type}`,
/// indexed by [`BookId`]. No `@{depth}@{refresh_rate}` suffix: docs.paradex.trade spells one,
/// but on the SBE socket the venue refuses it ("no parameters expected for deltas topic.") and
/// acknowledges the bare names, which carry the whole book on every change (decision 0074).
pub const BOOK_CHANNELS: [&str; 2] = ["deltas", "interactive_deltas"];

/// What the codec asks the runtime when a book's sequence breaks.
pub const RESYNC_REASON: &str = "Paradex book seq_no discontinuity";

/// The schema's `PkgType`.
const SNAPSHOT: u8 = 0;
const DELTA: u8 = 1;

/// The channel name of `book` of the market spelled `symbol`, or `None` for an undeclared one.
pub(crate) fn book_channel(book: BookId, symbol: &str) -> Option<String> {
    let suffix = BOOK_CHANNELS.get(usize::from(book.0))?;
    Some(format!("order_book.{symbol}.{suffix}"))
}

/// One `BookEvent`, decoded whole before anything is pushed (decision 0014 item 2).
pub(crate) struct BookFrame {
    inst: InstrumentId,
    meta: VenueMeta,
    seq: u64,
    snapshot: bool,
    levels: Vec<(BookSide, Lvl)>,
}

impl BookFrame {
    /// The market the frame is for.
    pub(crate) fn inst(&self) -> InstrumentId {
        self.inst
    }
}

/// `BookEvent`: ts@0, seq@8, pkgType@16, then the interactive and API best prices this decoder
/// does not read; groups `bids` and `asks` (price@0, size@8 per entry); then `market`.
pub(crate) fn decode_book(msg: &Message<'_>, specs: &SpecTable) -> Result<BookFrame, DecodeError> {
    let block = msg.block();
    let snapshot = match block.u8_at(16) {
        Some(SNAPSHOT) => true,
        Some(DELTA) => false,
        _ => return Err(DecodeError::Malformed("book pkgType")),
    };
    let mut tail = msg.tail();
    let (bids, asks) = (tail.group()?, tail.group()?);
    let symbol = tail
        .var_str()?
        .ok_or(DecodeError::Malformed("SBE market"))?;
    let spec = market(symbol, specs)?;
    let mut levels = Vec::with_capacity(bids.len() + asks.len());
    for (side, group) in [(BookSide::Bid, bids), (BookSide::Ask, asks)] {
        for entry in group.entries() {
            levels.push((side, book_level(spec, &entry)?));
        }
    }
    let seq = seq(required(&block, 8, "book seq")?)?;
    let meta = VenueMeta {
        exch_ts: Some(micros(required(&block, 0, "book ts")?)?),
        // BookEvent's one timestamp, as BboEvent's: the schema calls TradeEvent's the "Feed
        // publish timestamp".
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: Some(seq),
    };
    Ok(BookFrame {
        inst: spec.id,
        meta,
        seq,
        snapshot,
        levels,
    })
}

/// One group entry: its price and size, zero size removing the level.
fn book_level(spec: &fbc_core::InstrumentSpec, entry: &Block<'_>) -> Result<Lvl, DecodeError> {
    let px = required(entry, 0, "book level")?;
    let qty = required(entry, 8, "book level")?;
    Ok(Lvl {
        px: price(spec, px)?,
        qty: lots(spec, qty)?,
    })
}

/// The market and seq_no of a `BookEvent` frame, or `None` for a frame of another template;
/// refused as the codec would refuse it. A frame's seq_no is there even when it decodes to no
/// event (a delta with no levels), which an offline check aligning the book with a snapshot
/// or a bbo sample by seq_no needs.
pub fn frame_seq(
    frame: &[u8],
    specs: &SpecTable,
) -> Result<Option<(InstrumentId, u64)>, DecodeError> {
    let msg = Message::parse(frame)?;
    if msg.header().template_id != super::TEMPLATE_BOOK {
        return Ok(None);
    }
    let frame = decode_book(&msg, specs)?;
    Ok(Some((frame.inst, frame.seq)))
}

/// One market's book channel on a connection, and where its sequence stands.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct BookFeed {
    book: BookId,
    /// The seq_no of the last frame applied; `None` while awaiting a snapshot.
    last: Option<u64>,
    /// Snapshots applied on this connection: each starts the next epoch of the book.
    snapshots: u32,
}

impl BookFeed {
    /// A book awaiting its first snapshot.
    pub(crate) fn new(book: BookId) -> BookFeed {
        BookFeed {
            book,
            last: None,
            snapshots: 0,
        }
    }

    /// Applies `frame`: a snapshot, a delta that follows the last seq_no, or a gap.
    pub(crate) fn apply(
        &mut self,
        frame: BookFrame,
        stream: StreamId,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) {
        let (inst, book, meta) = (frame.inst, self.book, frame.meta);
        if frame.snapshot {
            self.snapshots = self.snapshots.wrapping_add(1);
            let epoch = self.snapshots;
            sink.push(meta, MdEvent::BookSnapshotBegin { inst, book, epoch });
            push_levels(&frame, book, sink);
            sink.push(meta, MdEvent::BookSnapshotEnd { inst, book });
            self.last = Some(frame.seq);
            return;
        }
        let Some(last) = self.last else {
            // Awaiting a snapshot: the first, or the one a gap asked for.
            return;
        };
        if last.checked_add(1) == Some(frame.seq) {
            push_levels(&frame, book, sink);
            self.last = Some(frame.seq);
            return;
        }
        self.last = None;
        let h = FeedHealth::Gap;
        sink.push(
            meta,
            MdEvent::Health {
                inst,
                feed: Feed::Book(book),
                h,
            },
        );
        fx.push(Effect::Reconnect {
            stream,
            reason: RESYNC_REASON,
        });
    }
}

fn push_levels(frame: &BookFrame, book: BookId, sink: &mut dyn MdSink) {
    for &(side, Lvl { px, qty }) in &frame.levels {
        let inst = frame.inst;
        sink.push(
            frame.meta,
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
