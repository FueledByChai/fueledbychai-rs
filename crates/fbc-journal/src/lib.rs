//! The journal: everything that crosses the shard boundary, recorded so a recorded day
//! replays through the same code that ran live (decision 0006, design §4.8 as 0014 refines it).
//!
//! The library writes and reads the journal; where its files live and how long they are kept
//! is the consumer's choice. It has:
//!
//! - [`Record`], the records the runtime writes: inbound frames with their stamps, outbound
//!   frames with their redaction spans and write results, HTTP requests and results with
//!   headers, timer firings, control records (connections opened and closed, subscription
//!   calls) and markers (a session's start with the consumer's header, `Degraded`,
//!   `Recovered`).
//! - [`JournalWriter`], which writes one shard's records into a directory the consumer
//!   supplies, one subdirectory per UTC day (`YYYYMMDD`), segments named
//!   `<shard>-<seq>.fbcj`. Time comes from the caller.
//! - [`JournalReader`], which returns one shard's records in write order across segments
//!   and days.
//! - [`JournalSink`], what the shard journals through, which never blocks (0006): a
//!   [`QueueSink`] feeds a bounded queue with a soft limit for Normal records and a Safety
//!   reserve, counts what it drops by class and marks the gap with `Degraded` once space
//!   returns; a [`WriterThread`] drains the queue into a [`JournalWriter`] ([`journal_queue`]).
//!
//! **Format.** Length-prefixed records after a header holding the format version, in a
//! hand-written little-endian encoding ([`format`]). There is no serialization dependency:
//! adding one would need a decision record and the licence gate (FBC-cu0).
//!
//! **Redaction (0006, 0009).** No byte of a redaction span is written: the spans of a
//! [`WireSlice`](fbc_core::WireSlice) or [`WireUrl`](fbc_core::WireUrl), header values a codec
//! marked redacted, and the values of the [`SECRET_HEADERS`] by name, in requests and results
//! alike. The record keeps each span's place and length, and the reader returns [`BLANK`]
//! bytes there ([`Record::blanked`] gives what the reader will return). Keyed hashes in their
//! place are FBC-apz's, under `src/redact*`. Order signatures are not redacted.
//!
//! **What is still written verbatim.** Inbound frames, HTTP response bodies, and response
//! header names and values other than the [`SECRET_HEADERS`] carry no redaction metadata in
//! `fbc-core` yet (a [`RawFrame`](fbc_core::RawFrame) or an
//! [`HttpResponse`](fbc_core::HttpResponse) marks nothing), so they are written as they came:
//! decoder replay needs their bytes. A codec marking credential spans there, and the journal
//! blanking them, is FBC-7lm. Until it lands, journal no traffic whose inbound side carries a
//! credential (an auth response holding a JWT, a frame echoing a key).

mod error;
pub mod format;
mod reader;
mod record;
mod sink;
mod writer;

pub use error::JournalError;
pub use reader::JournalReader;
pub use record::{
    BLANK, ControlEvent, HeaderRec, HttpRequestRec, HttpResponseRec, Marker, Opaque, Opcode,
    Record, SECRET_HEADERS, WriteRes, is_secret_header,
};
pub use sink::{
    JournalDrain, JournalSink, QueueSink, Recorded, SinkConfig, WriterThread, journal_queue,
};
pub use writer::{JournalWriter, SEGMENT_EXT};
