//! The journal: everything that crosses the shard boundary, recorded so a recorded day
//! replays through the same code that ran live (decision 0006, design §4.8 as 0014 refines it).
//!
//! The library writes and reads the journal; where its files live and how long they are kept
//! is the consumer's choice. It has:
//!
//! - [`Record`], the records the runtime writes: inbound frames with their stamps, outbound
//!   frames with their redaction spans and write results, HTTP requests and results with
//!   headers, timer firings, control records (connections opened and closed, subscription
//!   calls), markers (a session's start with the consumer's header, `Degraded`,
//!   `Recovered`), the nonces each source reserved, the context (wall and monotonic time,
//!   nonces) each encode was given, and decide-cycle boundaries.
//! - [`JournalWriter`], which writes one shard's records into a directory the consumer
//!   supplies, one subdirectory per UTC day (`YYYYMMDD`), segments named
//!   `<shard>-<seq>.fbcj`, rolled at every UTC hour; each segment it closes is compressed with
//!   zstd into `<shard>-<seq>.fbcj.zst` (decision 0026). Time comes from the caller.
//! - [`JournalReader`], which returns one shard's records in write order across segments
//!   and days, the closed compressed segments and the open uncompressed one alike.
//! - [`JournalSink`], what the shard journals through, which never blocks (0006): a
//!   [`QueueSink`] feeds a bounded queue with a soft limit for Normal records and a Safety
//!   reserve, counts what it drops by class and marks the gap with `Degraded` once space
//!   returns; a [`WriterThread`] drains the queue into a [`JournalWriter`] ([`journal_queue`]).
//!
//! **Format.** Length-prefixed records after a header holding the format version, in a
//! hand-written little-endian encoding ([`format`]). There is no serialization dependency:
//! adding one would need a decision record and the licence gate (FBC-cu0).
//!
//! **Redaction (0006, 0009, 0024, 0028).** No byte of a redaction span is written: the spans
//! of a [`WireSlice`](fbc_core::WireSlice) or [`WireUrl`](fbc_core::WireUrl), header values
//! (and, in a response, names) a codec marked redacted, the values of the [`SECRET_HEADERS`] by
//! name, in requests and results alike, and the spans a codec named in an inbound frame or a
//! response body ([`InboundSpans`](fbc_core::InboundSpans), through
//! [`Record::inbound_redacted`] and [`HttpResponseRec::redacted`]). Each is written as its
//! HMAC-SHA-256 under the consumer's [`RedactionKey`], so equal secrets hash equally and replay
//! compares bytes modulo spans ([`redact`]). The record keeps each span's place and length, and
//! the reader returns [`BLANK`] bytes there ([`Record::blanked`] gives what the reader will
//! return) with the hashes beside it ([`JournalReader::entries`], [`Record::digests`]); replay
//! hands a codec the blanked input. Order signatures are not redacted: bytes outside spans are
//! written verbatim.
//!
//! **What is still written verbatim.** Whatever in inbound bytes the codec did not mark: a
//! record built with [`Record::inbound`] or [`HttpResponseRec::from`] marks nothing, for a codec
//! whose input holds no credential. The runtime journaling a session through its codec's
//! `redact_inbound` is FBC-s69.

mod error;
pub mod format;
mod reader;
mod record;
pub mod redact;
mod sink;
mod writer;

pub use error::JournalError;
pub use reader::{Entries, Entry, JournalReader};
pub use record::{
    BLANK, ControlEvent, HeaderRec, HttpRequestRec, HttpResponseRec, Marker, NonceSourceId, Opaque,
    Opcode, Record, SECRET_HEADERS, WriteRes, is_secret_header,
};
pub use redact::{RedactionKey, SpanDigest};
pub use sink::{
    JournalDrain, JournalSink, QueueSink, Recorded, SinkConfig, WriterThread, journal_queue,
};
pub use writer::{COMPRESSED_EXT, JournalWriter, SEGMENT_EXT};
