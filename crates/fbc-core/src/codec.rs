//! The sans-IO codec boundary every venue adapter implements (decisions 0002 and 0014, design §4.7).
//!
//! An adapter owns no socket, thread, queue or clock. Its codecs take bytes, HTTP results and
//! timer firings in, and give normalized events (to a sink) and [`Effect`]s out: send a frame,
//! make an HTTP request, set a timer, reconnect. The runtime executes and journals the effects.
//!
//! - [`MdCodec`] decodes market data; [`ExecCodec`] encodes and signs [`VenueCommand`]s and
//!   decodes an account's order-entry traffic. Both decode only inside a
//!   [`DecodeScope`](crate::DecodeScope), which alone builds venue order ids, fill ids, venue
//!   symbols and fees (decision 0004).
//! - [`EncodeCtx`] is the only source of wall time and nonces an encode sees. It is a value the
//!   runtime fills (from its clock and its [`NonceSource`]) and journals, and replay reads back,
//!   so the same command encoded under the same context gives the same bytes at any real time.
//! - [`OrderSigner`] signs the normalized wire view of an order ([`PlaceWire`], [`AmendWire`],
//!   [`CancelWire`]); a venue's signer lives in its `src/sign` (decision 0009).
//! - Frames carry redaction spans ([`WireSlice`]) and HTTP headers a redaction flag, so the
//!   journal can keep credentials only as keyed hashes (decision 0006).

use core::fmt;
use core::ops::Range;
use core::time::Duration;
use std::collections::BTreeMap;

use arrayvec::ArrayVec;
use compact_str::CompactString;

use crate::command::{NotSentReason, OrderKind, Tif, VenueCommand};
use crate::event::{BookId, ExecEvent, MdEvent, RpcId, StreamId, TouchSourceId, VenueMeta};
use crate::fee::FeeError;
use crate::ids::{IdError, InstrumentId, VenueOrderId};
use crate::instrument::InstrumentSpec;
use crate::scope::DecodeScope;
use crate::time::{MonoNs, WallNs};
use crate::units::{Channel, Lots, Side, Ticks};
use crate::venue::VenueError;

/// A frame as it came off a stream. Its `Debug` shows the kind and length only, since a venue
/// can echo a credential back in a frame.
#[derive(Copy, Clone, Eq, PartialEq)]
pub enum RawFrame<'a> {
    /// A text frame.
    Text(&'a str),
    /// A binary frame.
    Binary(&'a [u8]),
}

impl fmt::Debug for RawFrame<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            RawFrame::Text(_) => "Text",
            RawFrame::Binary(_) => "Binary",
        };
        f.debug_struct(kind)
            .field("len", &self.bytes().len())
            .finish()
    }
}

impl<'a> RawFrame<'a> {
    /// The frame's bytes, whatever its kind.
    pub fn bytes(&self) -> &'a [u8] {
        match *self {
            RawFrame::Text(text) => text.as_bytes(),
            RawFrame::Binary(bytes) => bytes,
        }
    }
}

/// A tag a codec puts on an HTTP request to recognize its response.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct HttpTag(pub u64);

/// A tag a codec puts on a timer to recognize its firing.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct TimerTag(pub u64);

/// Bytes for the wire, with the spans that hold credentials (a bearer token, an API key) so the
/// journal stores those spans only as keyed hashes and replay compares bytes modulo them.
///
/// Its `Debug` shows the bytes as text with every span replaced by `<redacted n bytes>`, so a
/// log line or panic message that formats an [`Effect`] never carries a credential (0009).
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WireSlice {
    bytes: Vec<u8>,
    redact: Vec<Range<u32>>,
}

/// Checks redaction spans against content of `len` bytes: each non-empty, inside, after the
/// one before it, and with both ends where `boundary` allows.
fn check_spans(
    len: usize,
    redact: &[Range<u32>],
    boundary: impl Fn(usize) -> bool,
) -> Result<(), RedactError> {
    let mut end = 0;
    for span in redact {
        let past = usize::try_from(span.end).map_or(true, |e| e > len);
        if span.start >= span.end || past {
            return Err(RedactError::OutOfBounds);
        }
        // In bounds, so both ends fit in usize.
        if !boundary(span.start as usize) || !boundary(span.end as usize) {
            return Err(RedactError::OutOfBounds);
        }
        if span.start < end {
            return Err(RedactError::Unordered);
        }
        end = span.end;
    }
    Ok(())
}

/// A URL for the wire, with the spans that hold credentials (a key or token in the path,
/// query or user information), so the journal stores those spans only as keyed hashes and
/// replay compares URLs modulo them, as it does frames ([`WireSlice`]) and headers.
///
/// Its `Debug` shows the URL with every span replaced by `<redacted n bytes>`, and its user
/// information, query and fragment only by length whatever the spans, so no log line or panic
/// message that formats a request or an endpoint plan carries a credential (0009).
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct WireUrl {
    text: String,
    redact: Vec<Range<u32>>,
}

impl WireUrl {
    /// A URL holding no credential.
    pub fn plain(text: impl Into<String>) -> WireUrl {
        WireUrl {
            text: text.into(),
            redact: Vec::new(),
        }
    }

    /// A URL whose `redact` spans hold credentials: each non-empty, inside the text, on
    /// character boundaries, and after the one before it.
    pub fn redacted(text: String, redact: Vec<Range<u32>>) -> Result<WireUrl, RedactError> {
        check_spans(text.len(), &redact, |at| text.is_char_boundary(at))?;
        Ok(WireUrl { text, redact })
    }

    /// The URL.
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// The spans holding credentials.
    pub fn redactions(&self) -> &[Range<u32>] {
        &self.redact
    }
}

impl PartialEq<str> for WireUrl {
    fn eq(&self, other: &str) -> bool {
        self.text == other
    }
}

impl PartialEq<&str> for WireUrl {
    fn eq(&self, other: &&str) -> bool {
        self.text == *other
    }
}

impl fmt::Debug for WireUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(
            &ShownUrl {
                url: &self.text,
                redact: &self.redact,
            },
            f,
        )
    }
}

/// Why a [`WireSlice`]'s or [`WireUrl`]'s redaction spans were refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum RedactError {
    /// A span is empty, reversed or reaches past the end of the bytes.
    OutOfBounds,
    /// Two spans overlap, or the spans are not in ascending order.
    Unordered,
}

impl fmt::Display for RedactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RedactError::OutOfBounds => "a redaction span lies outside the frame",
            RedactError::Unordered => "redaction spans overlap or are out of order",
        })
    }
}

impl std::error::Error for RedactError {}

impl fmt::Debug for WireSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WireSlice")
            .field("len", &self.bytes.len())
            .field("bytes", &Shown(self))
            .field("redact", &self.redact)
            .finish()
    }
}

/// A [`WireSlice`]'s bytes as escaped text, each redaction span replaced by its length.
struct Shown<'a>(&'a WireSlice);

impl fmt::Debug for Shown<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = &self.0.bytes;
        let mut at = 0;
        f.write_str("\"")?;
        for span in &self.0.redact {
            // In bounds and ordered: `WireSlice::redacted` checked every span.
            let (start, end) = (span.start as usize, span.end as usize);
            write_escaped(f, &bytes[at..start])?;
            write!(f, "<redacted {} bytes>", end - start)?;
            at = end;
        }
        write_escaped(f, &bytes[at..])?;
        f.write_str("\"")
    }
}

/// Writes `bytes` as escaped text: valid UTF-8 escaped as `str`'s `Debug` does, any other byte
/// as `\xNN`.
fn write_escaped(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for chunk in bytes.utf8_chunks() {
        write!(f, "{}", chunk.valid().escape_debug())?;
        for byte in chunk.invalid() {
            write!(f, "\\x{byte:02x}")?;
        }
    }
    Ok(())
}

impl WireSlice {
    /// Bytes holding no credential.
    pub fn plain(bytes: Vec<u8>) -> WireSlice {
        WireSlice {
            bytes,
            redact: Vec::new(),
        }
    }

    /// Bytes whose `redact` spans hold credentials: each span non-empty, inside the bytes, and
    /// after the one before it.
    pub fn redacted(bytes: Vec<u8>, redact: Vec<Range<u32>>) -> Result<WireSlice, RedactError> {
        check_spans(bytes.len(), &redact, |_| true)?;
        Ok(WireSlice { bytes, redact })
    }

    /// The bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The spans holding credentials.
    pub fn redactions(&self) -> &[Range<u32>] {
        &self.redact
    }
}

/// An HTTP method.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

/// One HTTP header; `redact` marks a credential the journal keeps only as a keyed hash. Its
/// `Debug` never shows a redacted value.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Header {
    pub name: &'static str,
    pub value: String,
    pub redact: bool,
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Header");
        d.field("name", &self.name);
        if self.redact {
            d.field(
                "value",
                &format_args!("<redacted {} bytes>", self.value.len()),
            );
        } else {
            d.field("value", &self.value);
        }
        d.field("redact", &self.redact).finish()
    }
}

/// An HTTP request for the runtime to make, through the consumer's proxy (decision 0002). Its
/// `Debug` shows the URL's scheme, host and path, but its user information, query and fragment
/// only by length, since a venue may take a key there.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: WireUrl,
    pub headers: Vec<Header>,
    pub body: WireSlice,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("headers", &self.headers)
            .field("body", &self.body)
            .finish()
    }
}

/// A URL with its user information, query and fragment shown by length only, and its
/// redaction spans replaced inside what is shown. The structure is found in the URL as written,
/// before any span is replaced, so a span covering a `?`, `#` or `@` cannot expose what follows.
struct ShownUrl<'a> {
    url: &'a str,
    redact: &'a [Range<u32>],
}

impl ShownUrl<'_> {
    /// Writes `url[from..to]` escaped, with every span's part inside it redacted. `from` and
    /// `to` are character boundaries (ASCII delimiters or the ends), and so is every span end.
    fn write_part(&self, f: &mut fmt::Formatter<'_>, from: usize, to: usize) -> fmt::Result {
        let mut at = from;
        for span in self.redact {
            let (start, end) = ((span.start as usize).max(from), (span.end as usize).min(to));
            if start < end {
                write!(f, "{}", self.url[at..start].escape_debug())?;
                write!(f, "<redacted {} bytes>", end - start)?;
                at = end;
            }
        }
        write!(f, "{}", self.url[at..to].escape_debug())
    }
}

impl fmt::Debug for ShownUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let url = self.url;
        let tail_at = url.find(['?', '#']).unwrap_or(url.len());
        let head = &url[..tail_at];
        f.write_str("\"")?;
        let after_scheme = head.find("://").map_or(0, |at| at + 3);
        let authority_end = head[after_scheme..]
            .find('/')
            .map_or(head.len(), |at| after_scheme + at);
        match head[after_scheme..authority_end].rfind('@') {
            Some(at) => {
                self.write_part(f, 0, after_scheme)?;
                write!(f, "<redacted {at} bytes>")?;
                self.write_part(f, after_scheme + at, tail_at)?;
            }
            None => self.write_part(f, 0, tail_at)?,
        }
        if let Some(sep) = url[tail_at..].chars().next() {
            write!(f, "{sep}<redacted {} bytes>", url.len() - tail_at - 1)?;
        }
        f.write_str("\"")
    }
}

/// An HTTP response, handed back to the codec that asked for it. Its `Debug` shows the status,
/// the header names and the body's length only: a response can carry a credential (a JWT in
/// the body, a cookie in a header) that nothing marks.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct HttpResponse<'a> {
    pub status: u16,
    pub headers: &'a [(&'a str, &'a str)],
    pub body: &'a [u8],
}

impl fmt::Debug for HttpResponse<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.headers.iter().map(|(name, _)| *name).collect();
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("header_names", &names)
            .field("body_len", &self.body.len())
            .finish()
    }
}

/// Why an HTTP request got no response.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum HttpFailure {
    /// No byte of the request was written (connect, TLS or proxy failure): the venue never saw
    /// it.
    NotSent,
    /// The request was written and its timeout passed with no response.
    TimedOut,
    /// The connection failed after the request was written: the venue may have acted on it.
    Lost,
}

/// Which traffic a frame is: safety traffic (cancels, reducing orders, keepalives,
/// authentication) keeps flowing at a rate scope's safety floor when normal traffic stops.
/// A request is safety traffic only when every item in it is: a batch of reducing orders is
/// `Safety`, a batch that mixes in one non-reducing order is `Normal`, so the planner keeps
/// reducing orders out of mixed batches rather than let normal orders ride the safety floor.
/// "Reducing" is the fact the OMS states ([`NewOrder::reducing`](crate::NewOrder::reducing)),
/// not only the venue's reduce-only flag; [`VenueCommand::traffic_class`] is the one rule.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum TrafficClass {
    Safety,
    Normal,
}

/// A request that awaits an answer, and how long the runtime waits for it. An RPC always has a
/// deadline, so an unanswered order-entry request always reaches Unknown (0005).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RpcCall {
    pub id: RpcId,
    pub timeout: Duration,
}

/// Something a codec asks the runtime to do. The codec does none of it itself.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Effect {
    /// Write `frame` to `stream`; with `rpc`, the runtime reports its timeout passing without
    /// an answer to [`ExecCodec::on_rpc_timeout`].
    Send {
        stream: StreamId,
        frame: WireSlice,
        rpc: Option<RpcCall>,
        class: TrafficClass,
    },
    /// Make `req`, handing the codec's `on_http` the response with `tag`, or the
    /// [`HttpFailure`] when none came: `timeout`, which every request has, passing without a
    /// response is [`HttpFailure::TimedOut`]. `rpc` names an order-entry request (journal and
    /// rate scope); unlike [`Effect::Send`], its timeout comes back to `on_http`, not
    /// `on_rpc_timeout`.
    Http {
        tag: HttpTag,
        req: HttpRequest,
        rpc: Option<RpcId>,
        timeout: Duration,
        class: TrafficClass,
    },
    /// Call the codec's `on_timer` with `tag` after `after`.
    Timer { tag: TimerTag, after: Duration },
    /// Close `stream` and open it again, under a new connection epoch.
    Reconnect {
        stream: StreamId,
        reason: &'static str,
    },
}

/// The effects one codec call asked for, in order.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct Effects {
    buf: Vec<Effect>,
}

impl Effects {
    /// No effects.
    pub fn new() -> Effects {
        Effects::default()
    }

    /// Asks for `effect`, after those already asked for.
    pub fn push(&mut self, effect: Effect) {
        self.buf.push(effect);
    }

    /// The effects asked for, in order.
    pub fn as_slice(&self) -> &[Effect] {
        &self.buf
    }

    /// Takes the effects asked for, in order, leaving none.
    pub fn take(&mut self) -> Vec<Effect> {
        core::mem::take(&mut self.buf)
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Whether these effects, asked for by an encode of request `rpc` whose command's class is
    /// `class` ([`VenueCommand::traffic_class`]), send it as a request the runtime can time out
    /// and rate-limit rightly: at least one [`Effect::Send`] or [`Effect::Http`], every one
    /// naming `rpc` (a `Send` with an [`RpcCall`] for `rpc`, an `Http` with `rpc: Some(rpc)`)
    /// and labelled `class`. The runtime executes an encode's effects only when this holds;
    /// otherwise it writes nothing and the command is `NotSent(Unencodable)`, a codec defect, so
    /// no order is sent outside the Unknown ladder (0005) and no request rides the wrong
    /// traffic class.
    pub fn carry_request(&self, rpc: RpcId, class: TrafficClass) -> bool {
        let mut requests = self.buf.iter().filter_map(|effect| match effect {
            Effect::Send {
                rpc: call,
                class: labelled,
                ..
            } => Some((call.map(|c| c.id), *labelled)),
            Effect::Http {
                rpc: call,
                class: labelled,
                ..
            } => Some((*call, *labelled)),
            Effect::Timer { .. } | Effect::Reconnect { .. } => None,
        });
        let mut any = false;
        let all = requests.all(|named| {
            any = true;
            named == (Some(rpc), class)
        });
        any && all
    }
}

/// The nonces reserved for one encode, one per item in item order. The values are whatever the
/// venue's [`NonceScope`](crate::NonceScope) needs: increasing for a monotonic scope,
/// independent for a random one. They are stated one by one, so the journal and replay carry
/// them exactly.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct NonceBlock {
    values: Vec<u64>,
}

impl NonceBlock {
    /// No nonces.
    pub const EMPTY: NonceBlock = NonceBlock { values: Vec::new() };

    /// `values`, the nonce for item `i` at index `i`.
    pub fn new(values: Vec<u64>) -> NonceBlock {
        NonceBlock { values }
    }

    /// `len` consecutive nonces from `first`, or `None` when they would pass `u64::MAX`.
    pub fn consecutive(first: u64, len: u16) -> Option<NonceBlock> {
        if len > 0 {
            first.checked_add(u64::from(len) - 1)?;
        }
        Some(NonceBlock::new(
            (0..u64::from(len)).map(|i| first + i).collect(),
        ))
    }

    /// The nonce for item `item`, or `None` past the block.
    pub fn get(&self, item: u16) -> Option<u64> {
        self.values.get(usize::from(item)).copied()
    }

    /// Every nonce, in item order.
    pub fn as_slice(&self) -> &[u64] {
        &self.values
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// The wall time, monotonic time and nonces for one encode: the only time and nonces a codec
/// sees. In live trading the runtime fills it from its clock and its [`NonceSource`] and
/// journals it; in replay it is read back from the journal. Every timestamp, expiry, deadline
/// and nonce in a payload comes from here.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct EncodeCtx {
    pub wall: WallNs,
    pub mono: MonoNs,
    pub nonces: NonceBlock,
}

impl EncodeCtx {
    /// The nonce for item `item` of the command, or `None` past the reserved block.
    pub fn nonce(&self, item: u16) -> Option<u64> {
        self.nonces.get(item)
    }
}

/// The nonces an encode used, per item, for the OMS to keep as each order's placement nonce.
/// A codec fills it only through [`use_nonce`](EncodeReceipt::use_nonce), which takes each
/// value from the [`EncodeCtx`], once per item, so a receipt never names a nonce the context
/// did not reserve for that item.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct EncodeReceipt {
    nonces: Vec<(u16, u64)>,
}

impl EncodeReceipt {
    /// No nonces used.
    pub fn new() -> EncodeReceipt {
        EncodeReceipt::default()
    }

    /// Takes item `item`'s nonce from `ctx`, records it, and returns it for the payload;
    /// `None` past the reserved block or when `item` already took its nonce.
    pub fn use_nonce(&mut self, ctx: &EncodeCtx, item: u16) -> Option<u64> {
        if self.nonces.iter().any(|(used, _)| *used == item) {
            return None;
        }
        let nonce = ctx.nonce(item)?;
        self.nonces.push((item, nonce));
        Some(nonce)
    }

    /// The nonces used, as `(item, nonce)` in the order they were taken.
    pub fn nonces(&self) -> &[(u16, u64)] {
        &self.nonces
    }
}

/// Where the runtime gets nonces, scoped as the venue's
/// [`NonceScope`](crate::NonceScope) says; every reservation is journaled.
pub trait NonceSource: Send {
    /// Reserves `len` nonces, one per item of a command, as the venue's scope needs: for a
    /// monotonic scope each above every nonce reserved before and increasing in item order;
    /// for a random scope each drawn independently.
    fn reserve(&mut self, len: u16) -> NonceBlock;
}

/// Receives the market-data events a codec decodes; the runtime stamps each one into an
/// [`Envelope`](crate::Envelope).
///
/// **A call that returns `Err` has pushed nothing.** A codec decodes the whole frame or response
/// before its first push, so the runtime never receives part of one: no snapshot begun and never
/// ended, no half-applied resync. The failed input is then lost to decoding as a whole. Effects
/// the call asked for still stand, so a failing codec can ask for recovery (a resync, an anchor,
/// a reconnect). Replay feeds the journaled input (D7) to the same codec and gets the same error
/// and, again, no events.
pub trait MdSink {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent);
}

/// Receives the execution events a codec decodes; the runtime stamps each one into an
/// [`Envelope`](crate::Envelope).
///
/// **A call that returns `Err` has pushed nothing.** A codec decodes the whole frame or response
/// before its first push, so the runtime never receives part of one: no snapshot begun and never
/// ended, no half-applied resync. The failed input is then lost to decoding as a whole. Effects
/// the call asked for still stand, so a failing codec can ask for recovery (a resync, an anchor,
/// a reconnect). Replay feeds the journaled input (D7) to the same codec and gets the same error
/// and, again, no events.
pub trait ExecSink {
    fn push(&mut self, meta: VenueMeta, ev: ExecEvent);
}

/// Why a frame or response could not be decoded. It names what was wrong, never the frame's
/// content, so no credential echoed by a venue reaches a log through it.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum DecodeError {
    /// The frame is not what the protocol says; names the part.
    Malformed(&'static str),
    /// The frame names an instrument missing from the spec table.
    UnknownInstrument,
    /// A venue order id, fill id or symbol was refused.
    IdRefused(IdError),
    /// A fee was refused.
    FeeRefused(FeeError),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Malformed(what) => write!(f, "malformed frame: {what}"),
            DecodeError::UnknownInstrument => f.write_str("the frame names an unknown instrument"),
            DecodeError::IdRefused(err) => write!(f, "venue id refused: {err:?}"),
            DecodeError::FeeRefused(err) => write!(f, "fee refused: {err}"),
        }
    }
}

impl std::error::Error for DecodeError {}

impl From<IdError> for DecodeError {
    fn from(err: IdError) -> DecodeError {
        DecodeError::IdRefused(err)
    }
}

impl From<FeeError> for DecodeError {
    fn from(err: FeeError) -> DecodeError {
        DecodeError::FeeRefused(err)
    }
}

/// The instrument specs a codec decodes and encodes against, by id and by venue symbol.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct SpecTable {
    specs: BTreeMap<InstrumentId, InstrumentSpec>,
    by_symbol: BTreeMap<CompactString, InstrumentId>,
}

impl SpecTable {
    /// An empty table.
    pub fn new() -> SpecTable {
        SpecTable::default()
    }

    /// Adds `spec`, replacing (and returning) the spec it shares an id with. A spec already
    /// holding its venue symbol under another id is removed too, so a symbol names one spec.
    pub fn insert(&mut self, spec: InstrumentSpec) -> Option<InstrumentSpec> {
        let (id, symbol) = (spec.id, CompactString::from(spec.venue_symbol.as_wire()));
        if let Some(other) = self.by_symbol.get(&symbol).copied()
            && other != id
        {
            self.specs.remove(&other);
        }
        let old = self.specs.insert(id, spec);
        if let Some(old) = &old {
            self.by_symbol.remove(old.venue_symbol.as_wire());
        }
        self.by_symbol.insert(symbol, id);
        old
    }

    /// The spec with id `id`.
    pub fn get(&self, id: InstrumentId) -> Option<&InstrumentSpec> {
        self.specs.get(&id)
    }

    /// The spec the venue spells `wire`.
    pub fn by_symbol(&self, wire: &str) -> Option<&InstrumentSpec> {
        self.by_symbol.get(wire).and_then(|id| self.specs.get(id))
    }

    /// Every spec, by id.
    pub fn iter(&self) -> impl Iterator<Item = &InstrumentSpec> {
        self.specs.values()
    }

    pub fn len(&self) -> usize {
        self.specs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }
}

/// A kind of market data for one instrument.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Feed {
    /// The best bid and offer from one touch channel.
    Touch(TouchSourceId),
    /// One book channel ([`BookId`]: an index into [`MdCaps::books`](crate::MdCaps::books)).
    /// An instrument may be subscribed to several at once, on one connection (a recorder keeps
    /// both a public and an interactive book, design §10); every book event
    /// ([`MdEvent::Level`] and the rest) names its channel. Which channel
    /// is the trading book stays a configuration choice recorded in the journal header and
    /// treated as a strategy change (design §7.2).
    Book(BookId),
    Trades,
    Mark,
    Index,
    Funding,
    Stats,
}

/// One market-data subscription.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Subscription {
    pub inst: InstrumentId,
    pub feed: Feed,
}

/// How a stream is kept alive.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum KeepaliveKind {
    /// A WebSocket ping.
    WsPing,
    /// A frame of the venue's own.
    Frame(WireSlice),
}

/// A keepalive the runtime sends every `interval`.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Keepalive {
    pub interval: Duration,
    pub kind: KeepaliveKind,
}

/// A market-data codec: one per connection epoch. Deterministic given its inputs (frames,
/// responses, the times passed to it) and its prior state.
pub trait MdCodec: Send {
    /// The stream opened.
    fn on_open(&mut self, fx: &mut Effects);
    /// Subscribe to `add` and unsubscribe from `remove`, spelling each instrument as `specs`
    /// says. `Err` names an instrument missing from `specs`; nothing is pushed then.
    fn subscribe(
        &mut self,
        add: &[Subscription],
        remove: &[Subscription],
        specs: &SpecTable,
        fx: &mut Effects,
    ) -> Result<(), VenueError>;
    /// Decode one frame. `Err` means nothing was pushed ([`MdSink`]).
    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError>;
    /// Decode the response to an HTTP request the codec asked for (a book anchor, a stats
    /// poll), or learn why none came, to retry or recover by asking for effects. `Err` means
    /// nothing was pushed ([`MdSink`]).
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError>;
    /// A timer the codec set fired at `now` (`wall` on the wall clock).
    fn on_timer(&mut self, tag: TimerTag, now: MonoNs, wall: WallNs, fx: &mut Effects);
    /// How the stream is kept alive, or `None` where the venue needs nothing.
    fn keepalive(&self) -> Option<Keepalive>;
}

/// An [`ExecCodec`] callback other than `encode` that takes an [`EncodeCtx`].
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CtxCall {
    /// [`ExecCodec::on_open`] for this stream.
    Open(StreamId),
    /// [`ExecCodec::on_timer`] for this tag.
    Timer(TimerTag),
    /// [`ExecCodec::resync`].
    Resync,
}

/// An order-entry codec for one account session. Deterministic given its inputs and prior
/// state; it reads no clock and draws no nonce except through [`EncodeCtx`].
pub trait ExecCodec: Send {
    /// How many nonces the runtime reserves into the [`EncodeCtx`] it passes to `call`, asked
    /// right before the call from the codec's current state; 0 when the call signs nothing.
    /// (For `encode` the count is [`VenueCommand::items`].) The runtime reserves and journals
    /// exactly that many, so replay hands the call the same context.
    fn nonces_for(&self, call: CtxCall) -> u16;
    /// `stream` opened: authenticate and subscribe, as effects.
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects);
    /// Encode and sign `cmd` as request `rpc`. `Err` means not sent: no byte reached a socket
    /// buffer and no effect was pushed. `Ok` effects carry the request
    /// ([`Effects::carry_request`]): every frame or HTTP request names `rpc`, so it has a
    /// deadline, and is labelled with `cmd`'s traffic class. Never retries.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason>;
    /// Decode one frame from `stream`. `Err` means nothing was pushed ([`ExecSink`]).
    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError>;
    /// Decode the response to an HTTP request the codec asked for, or learn why none came. For
    /// an order-entry request (one with an `rpc`), the call always reports that request's
    /// outcome: a failure is [`HttpFailure::NotSent`] for `NotSent`, the others `Unknown`, never
    /// resent; and a response the codec cannot decode is `Unknown` too (the venue may have
    /// acted), pushed with `Ok`, since the request's timeout is spent and nothing else will
    /// settle it. `Err` means nothing was pushed ([`ExecSink`]), and is only for a response to a
    /// request without an `rpc`.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: Result<HttpResponse<'_>, HttpFailure>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError>;
    /// A timer the codec set fired (token refresh, keepalive, dead-man refresh).
    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects);
    /// Request `rpc`, sent as a frame ([`Effect::Send`]), timed out unanswered: report
    /// `Unknown` for every item still unanswered, and keep every answer already decoded. That is
    /// `Outcome { item: None, Unknown }` when the codec holds no item outcome for `rpc`. For a
    /// batch whose items the venue answers in separate frames, the codec has held the outcomes
    /// it decoded (the one-call contract, [`ExecEvent::answers`]): it pushes those, then
    /// `Unknown` for each other item by its index, so an acknowledged item keeps its venue id.
    /// The runtime calls it only when no event answering `rpc` was pushed before the deadline.
    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink);
    /// Read the venue's open orders and positions (reads only), reported as the `Resync*`
    /// events.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects);
}

/// The most bytes a [`Sig`] holds.
pub const MAX_SIG_LEN: usize = 128;

/// A signature, as the venue's wire carries it.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Sig(ArrayVec<u8, MAX_SIG_LEN>);

impl Sig {
    /// The signature `bytes`, or `None` when longer than [`MAX_SIG_LEN`].
    pub fn new(bytes: &[u8]) -> Option<Sig> {
        ArrayVec::try_from(bytes).ok().map(Sig)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Why a signer did not sign. It never carries key material.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SignError {
    /// The order cannot be expressed in the venue's signed message; names the part.
    Unsignable(&'static str),
    /// The signing backend failed; names how.
    Backend(&'static str),
}

impl fmt::Display for SignError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignError::Unsignable(what) => write!(f, "cannot sign: {what}"),
            SignError::Backend(what) => write!(f, "signer failed: {what}"),
        }
    }
}

impl std::error::Error for SignError {}

/// A new order as it goes on the wire: what a signer signs.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct PlaceWire<'a> {
    pub spec: &'a InstrumentSpec,
    /// The client id in the venue's wire format.
    pub cid: &'a str,
    pub side: Side,
    pub kind: OrderKind,
    pub qty: Lots,
    pub tif: Tif,
    pub channel: Channel,
    pub post_only: bool,
    pub reduce_only: bool,
    /// The signature time, from [`EncodeCtx`].
    pub wall: WallNs,
    /// The order's nonce, from [`EncodeCtx`], where the venue uses one.
    pub nonce: Option<u64>,
}

/// An amend as it goes on the wire: the full post-amend order.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct AmendWire<'a> {
    pub spec: &'a InstrumentSpec,
    /// The reference the request carries, and only that one.
    pub target: AmendRef<'a>,
    pub side: Side,
    pub px: Ticks,
    /// The quantity as the venue's wire means it: [`AmendOrder::wire_qty`](crate::AmendOrder::wire_qty)
    /// under the venue's [`AmendQty`](crate::AmendQty).
    pub qty: Lots,
    pub tif: Tif,
    pub channel: Channel,
    pub post_only: bool,
    pub reduce_only: bool,
    pub wall: WallNs,
    pub nonce: Option<u64>,
}

/// The one reference an amend request names its order by, as the codec chose it from the
/// command's [`OrderRef`](crate::OrderRef): the signer signs exactly what is sent.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AmendRef<'a> {
    /// The venue's order id.
    Venue(&'a VenueOrderId),
    /// Our client id, already in the venue's wire format.
    Client(&'a str),
}

/// The one reference a cancel request names its order by, as the codec chose it from the
/// command's [`OrderRef`](crate::OrderRef) and placement nonce: the signer signs exactly what is sent.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CancelRef<'a> {
    /// The venue's order id.
    Venue(&'a VenueOrderId),
    /// Our client id, already in the venue's wire format.
    Client(&'a str),
    /// The nonce the order was placed with.
    PlacementNonce(u64),
}

/// A cancel as it goes on the wire.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct CancelWire<'a> {
    pub spec: &'a InstrumentSpec,
    /// The reference the request carries, and only that one.
    pub target: CancelRef<'a>,
    pub side: Side,
    pub wall: WallNs,
    pub nonce: Option<u64>,
}

/// Signs orders for one venue account. Implemented in a venue crate's `src/sign`, reviewed by
/// the owner (decision 0009).
pub trait OrderSigner: Send {
    fn sign_place(&mut self, w: &PlaceWire<'_>) -> Result<Sig, SignError>;
    fn sign_amend(&mut self, w: &AmendWire<'_>) -> Result<Sig, SignError>;
    /// `None` for a venue whose cancels are not signed.
    fn sign_cancel(&mut self, w: &CancelWire<'_>) -> Result<Option<Sig>, SignError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cid::ClientIdFormat;
    use crate::fee::VenueFeeSign;
    use crate::grid::PriceGrid;
    use crate::ids::{Namespace, UnderlyingId, VenueId};
    use crate::instrument::{FundingSpec, InstrumentKind, SizeStep, TradingStatus};
    use crate::scope::dispatch;
    use crate::units::AssetSym;
    use rust_decimal::Decimal;

    fn spec(id: u32, symbol: &str) -> InstrumentSpec {
        let venue_symbol = dispatch(
            &ClientIdFormat::Uuid,
            Namespace::new(1),
            VenueFeeSign::PositiveIsCost,
            |scope| scope.venue_symbol(symbol),
        )
        .unwrap();
        let usd = AssetSym::new("USD").unwrap();
        InstrumentSpec {
            id: InstrumentId::new(id),
            venue: VenueId::new(1),
            venue_symbol,
            native_id: None,
            underlying: UnderlyingId::new(1),
            kind: InstrumentKind::Perpetual,
            price_grid: PriceGrid::fixed(Decimal::ONE).unwrap(),
            quote_grid: None,
            size_step: SizeStep::new(Decimal::ONE).unwrap(),
            min_size: Lots::new(1).unwrap(),
            min_notional: None,
            max_order_size: None,
            position_limit: None,
            price_band: None,
            max_open_orders: None,
            multiplier: Decimal::ONE,
            quote_ccy: usd,
            settle_ccy: usd,
            funding: FundingSpec::Unknown,
            public_fees: None,
            status: TradingStatus::Trading,
            version: 1,
            fetched_at: WallNs(0),
        }
    }

    #[test]
    fn a_spec_table_finds_specs_by_id_and_by_symbol() {
        let mut table = SpecTable::new();
        assert!(table.is_empty());
        assert_eq!(table.insert(spec(1, "A-PERP")), None);
        assert_eq!(table.insert(spec(2, "B-PERP")), None);
        assert_eq!(table.len(), 2);
        assert_eq!(
            table
                .get(InstrumentId::new(2))
                .unwrap()
                .venue_symbol
                .as_wire(),
            "B-PERP"
        );
        assert_eq!(table.by_symbol("A-PERP").unwrap().id, InstrumentId::new(1));
        assert!(table.by_symbol("C-PERP").is_none());

        // A new spec for an id replaces the old one and its symbol.
        let old = table.insert(spec(1, "A2-PERP")).unwrap();
        assert_eq!(old.venue_symbol.as_wire(), "A-PERP");
        assert!(table.by_symbol("A-PERP").is_none());
        assert_eq!(table.by_symbol("A2-PERP").unwrap().id, InstrumentId::new(1));

        // A symbol moving to another id leaves one spec under it.
        assert_eq!(table.insert(spec(3, "B-PERP")), None);
        assert!(table.get(InstrumentId::new(2)).is_none());
        assert_eq!(table.by_symbol("B-PERP").unwrap().id, InstrumentId::new(3));
        let ids: Vec<u32> = table.iter().map(|s| s.id.get()).collect();
        assert_eq!(ids, [1, 3]);
    }

    #[test]
    fn redaction_spans_must_lie_inside_the_frame_in_order() {
        let bytes = b"auth|token=SYNTHETIC|end".to_vec();
        let ok = WireSlice::redacted(bytes.clone(), vec![11..20, 21..24]).unwrap();
        assert_eq!(ok.redactions(), [11..20, 21..24]);
        assert_eq!(ok.bytes(), bytes.as_slice());
        let past = WireSlice::redacted(bytes.clone(), std::iter::once(20..25).collect());
        assert_eq!(past, Err(RedactError::OutOfBounds));
        let empty = WireSlice::redacted(bytes.clone(), std::iter::once(3..3).collect());
        assert_eq!(empty, Err(RedactError::OutOfBounds));
        let overlap = WireSlice::redacted(bytes.clone(), vec![2..6, 5..8]);
        assert_eq!(overlap, Err(RedactError::Unordered));
        let reversed = WireSlice::redacted(bytes, vec![10..12, 2..4]);
        assert_eq!(reversed, Err(RedactError::Unordered));
        assert!(WireSlice::plain(b"x".to_vec()).redactions().is_empty());
        assert!(RedactError::OutOfBounds.to_string().contains("outside"));
        assert!(RedactError::Unordered.to_string().contains("order"));
    }

    #[test]
    fn debug_output_never_shows_a_credential() {
        let secret = "SYNTHETIC-CREDENTIAL-VALUE";
        let start = u32::try_from("auth|token=".len()).unwrap();
        let end = start + u32::try_from(secret.len()).unwrap();
        let frame = format!("auth|token={secret}|end").into_bytes();
        let body = WireSlice::redacted(frame, std::iter::once(start..end).collect()).unwrap();
        let req = HttpRequest {
            method: HttpMethod::Post,
            url: WireUrl::plain(format!(
                "https://venue.invalid/auth?key={secret}&v=2#{secret}"
            )),
            headers: vec![
                Header {
                    name: "Authorization",
                    value: format!("Bearer {secret}"),
                    redact: true,
                },
                Header {
                    name: "Content-Type",
                    value: "application/json".to_owned(),
                    redact: false,
                },
            ],
            body: body.clone(),
        };
        let effects = [
            Effect::Http {
                tag: HttpTag(1),
                req,
                rpc: None,
                timeout: Duration::from_secs(5),
                class: TrafficClass::Safety,
            },
            Effect::Send {
                stream: StreamId(0),
                frame: body.clone(),
                rpc: None,
                class: TrafficClass::Safety,
            },
        ];
        let mut fx = Effects::new();
        effects.iter().cloned().for_each(|e| fx.push(e));
        let keepalive = Keepalive {
            interval: Duration::from_secs(1),
            kind: KeepaliveKind::Frame(body),
        };
        let echoed = format!("{{\"jwt\":\"{secret}\"}}");
        let resp = HttpResponse {
            status: 200,
            headers: &[("Set-Cookie", secret)],
            body: echoed.as_bytes(),
        };
        let shown = [
            format!("{fx:?}"),
            format!("{effects:#?}"),
            format!("{keepalive:?}"),
            format!("{resp:?}"),
            format!("{:?}", RawFrame::Text(&echoed)),
            format!("{:?}", RawFrame::Binary(echoed.as_bytes())),
        ];
        for text in &shown {
            assert!(!text.contains(secret), "a credential reached Debug: {text}");
        }
        // What is not a credential is still shown, so the output stays useful.
        assert!(shown[0].contains("auth|token=") && shown[0].contains("|end"));
        assert!(shown[0].contains("Authorization") && shown[0].contains("application/json"));
        // A URL shows its scheme, host and path; its query and fragment only by length.
        assert!(shown[0].contains("https://venue.invalid/auth?<redacted 61 bytes>"));
        assert!(shown[0].contains("redacted"));
        assert!(shown[3].contains("200") && shown[3].contains("Set-Cookie"));
        assert!(shown[4].contains(&echoed.len().to_string()));
        // Bytes that are not UTF-8 are escaped one by one.
        let binary = format!("{:?}", WireSlice::plain(vec![b'a', 0xff, b'"']));
        assert!(binary.contains(r#""a\xff\"""#), "{binary}");
        // User information in a URL is a credential too; a URL without a query shows whole.
        let shown = |url: &str| format!("{:?}", WireUrl::plain(url));
        assert_eq!(
            shown("wss://user:pw@venue.invalid/ws#frag"),
            r#""wss://<redacted 7 bytes>@venue.invalid/ws#<redacted 4 bytes>""#
        );
        assert_eq!(
            shown("https://venue.invalid/a/b"),
            r#""https://venue.invalid/a/b""#
        );
        assert_eq!(shown("no-scheme?q=1"), r#""no-scheme?<redacted 3 bytes>""#);
    }

    #[test]
    fn effects_keep_their_order_until_taken() {
        let mut fx = Effects::new();
        assert!(fx.is_empty());
        let timer = |n| Effect::Timer {
            tag: TimerTag(n),
            after: Duration::from_millis(n),
        };
        fx.push(timer(1));
        fx.push(Effect::Reconnect {
            stream: StreamId(2),
            reason: "stale",
        });
        assert_eq!(fx.len(), 2);
        assert_eq!(fx.as_slice()[0], timer(1));
        let taken = fx.take();
        assert_eq!(taken.len(), 2);
        assert!(fx.is_empty());
    }

    #[test]
    fn an_encode_is_sent_only_as_a_request_the_runtime_can_time_out() {
        // Codex r4173031192: an encode that sends its command with no RpcCall would leave the
        // order outside the Unknown ladder. Its effects carry the request only when there is a
        // frame or HTTP request and every one names the encode's rpc.
        let (rpc, other) = (RpcId(7), RpcId(8));
        let frame = |call: Option<RpcId>| Effect::Send {
            stream: StreamId(1),
            frame: WireSlice::plain(b"x".to_vec()),
            rpc: call.map(|id| RpcCall {
                id,
                timeout: Duration::from_secs(1),
            }),
            class: TrafficClass::Safety,
        };
        let http = |call: Option<RpcId>| Effect::Http {
            tag: HttpTag(1),
            req: HttpRequest {
                method: HttpMethod::Post,
                url: WireUrl::plain("https://venue.invalid/order"),
                headers: vec![],
                body: WireSlice::plain(Vec::new()),
            },
            rpc: call,
            timeout: Duration::from_secs(1),
            class: TrafficClass::Safety,
        };
        let timer = Effect::Timer {
            tag: TimerTag(1),
            after: Duration::from_secs(1),
        };
        let carries = |effects: Vec<Effect>| {
            let mut fx = Effects::new();
            effects.into_iter().for_each(|e| fx.push(e));
            fx.carry_request(rpc, TrafficClass::Safety)
        };
        assert!(carries(vec![frame(Some(rpc))]));
        assert!(carries(vec![http(Some(rpc)), timer.clone()]));
        assert!(carries(vec![frame(Some(rpc)), frame(Some(rpc))]));
        assert!(!carries(vec![frame(None)]));
        assert!(!carries(vec![http(None)]));
        assert!(!carries(vec![frame(Some(rpc)), frame(Some(other))]));
        assert!(!carries(vec![frame(Some(rpc)), frame(None)]));
        assert!(!carries(vec![timer]));
        assert!(!carries(vec![]));
        // Codex r4173103646: every request effect is labelled with the command's own class
        // (VenueCommand::traffic_class), so a cancel is never rate-limited as normal traffic
        // and a normal order never rides the safety floor.
        let mut fx = Effects::new();
        fx.push(frame(Some(rpc)));
        assert!(!fx.carry_request(rpc, TrafficClass::Normal));
    }

    #[test]
    fn a_nonce_block_gives_one_nonce_per_item_and_none_past_it() {
        let ctx = EncodeCtx {
            wall: WallNs(1),
            mono: MonoNs(2),
            nonces: NonceBlock::consecutive(10, 2).unwrap(),
        };
        assert_eq!(
            (ctx.nonce(0), ctx.nonce(1), ctx.nonce(2)),
            (Some(10), Some(11), None)
        );
        assert_eq!(ctx.nonces.as_slice(), [10, 11]);
        assert_eq!(NonceBlock::EMPTY.get(0), None);
        assert!(NonceBlock::EMPTY.is_empty());
        assert_eq!(NonceBlock::consecutive(3, 0), Some(NonceBlock::EMPTY));
        // A consecutive block that would pass u64::MAX is refused, not wrapped.
        let top = NonceBlock::consecutive(u64::MAX, 1).unwrap();
        assert_eq!((top.get(0), top.get(1)), (Some(u64::MAX), None));
        assert_eq!(NonceBlock::consecutive(u64::MAX, 2), None);
        // Random nonces are independent values, one per item.
        let random = NonceBlock::new(vec![u64::MAX, 7, 7_000]);
        assert_eq!(random.len(), 3);
        assert_eq!(
            (random.get(0), random.get(1), random.get(2), random.get(3)),
            (Some(u64::MAX), Some(7), Some(7_000), None)
        );
    }

    #[test]
    fn errors_name_what_was_wrong_and_nothing_else() {
        assert_eq!(
            DecodeError::Malformed("px").to_string(),
            "malformed frame: px"
        );
        assert!(
            DecodeError::UnknownInstrument
                .to_string()
                .contains("unknown")
        );
        let id: DecodeError = IdError::Empty.into();
        assert_eq!(id, DecodeError::IdRefused(IdError::Empty));
        assert!(id.to_string().contains("Empty"));
        let fee: DecodeError = FeeError::OutOfRange.into();
        assert!(fee.to_string().starts_with("fee refused"));
        assert_eq!(SignError::Unsignable("px").to_string(), "cannot sign: px");
        assert_eq!(SignError::Backend("rng").to_string(), "signer failed: rng");
    }

    #[test]
    fn a_signature_holds_at_most_max_sig_len_bytes() {
        let sig = Sig::new(&[7; MAX_SIG_LEN]).unwrap();
        assert_eq!(sig.as_bytes().len(), MAX_SIG_LEN);
        assert_eq!(Sig::new(&[7; MAX_SIG_LEN + 1]), None);
    }

    #[test]
    fn a_raw_frame_gives_its_bytes_whatever_its_kind() {
        assert_eq!(RawFrame::Text("ab").bytes(), b"ab");
        assert_eq!(RawFrame::Binary(&[1, 2]).bytes(), [1, 2]);
    }

    #[test]
    fn a_url_keeps_its_credential_spans_and_never_shows_them() {
        // A venue may put a credential in a URL's path; the URL keeps the span that holds it,
        // as frames and headers do, so the journal can hash it, and Debug never prints it.
        let secret = "SYNTHETIC-URL-TOKEN";
        let text = format!("wss://venue.invalid/ws/{secret}/stream");
        let start = u32::try_from("wss://venue.invalid/ws/".len()).unwrap();
        let span = start..start + u32::try_from(secret.len()).unwrap();
        let url = WireUrl::redacted(text.clone(), vec![span.clone()]).unwrap();
        assert_eq!(
            (url.as_str(), url.redactions()),
            (text.as_str(), &[span][..])
        );
        assert!(url == text.as_str() && url == *text.as_str());
        let shown = format!("{url:?}");
        assert!(
            !shown.contains(secret) && shown.contains("venue.invalid/ws/"),
            "{shown}"
        );
        assert!(
            WireUrl::plain("https://venue.invalid")
                .redactions()
                .is_empty()
        );
        // Spans lie inside the text, in order, on character boundaries.
        let refused = |spans| WireUrl::redacted("https://é.invalid".to_owned(), spans);
        assert_eq!(
            refused(std::iter::once(0..99).collect()),
            Err(RedactError::OutOfBounds)
        );
        assert_eq!(
            refused(std::iter::once(9..10).collect()),
            Err(RedactError::OutOfBounds)
        );
        assert_eq!(refused(vec![4..6, 2..3]), Err(RedactError::Unordered));
    }

    #[test]
    fn a_url_span_over_a_delimiter_still_leaves_the_rest_of_the_url_hidden() {
        // The query, fragment and user information are found in the URL as written, before
        // any span is replaced: a span that swallows the `?`, `#` or `@` cannot expose what
        // follows. The path is shown (only a span hides it); the query and fragment never are.
        let cases = [
            (
                "https://venue.invalid/x?api_key=SECRET&other=TOKEN2",
                "?api_key=SECRET",
                0,
            ),
            ("https://venue.invalid/x#SECRET&TOKEN2", "#SECRET", 0),
            (
                "https://user:SECRET@venue.invalid/TOKEN2?TOKEN2",
                ":SECRET@",
                1,
            ),
        ];
        for (text, marked, path_tokens) in cases {
            let start = text.find(marked).unwrap();
            let end = u32::try_from(start + marked.len()).unwrap();
            let span = u32::try_from(start).unwrap()..end;
            let url = WireUrl::redacted(text.to_owned(), vec![span]).unwrap();
            let shown = format!("{url:?}");
            assert!(
                !shown.contains("SECRET") && shown.contains("venue.invalid"),
                "{shown}"
            );
            assert_eq!(shown.matches("TOKEN2").count(), path_tokens, "{shown}");
        }
    }

    #[test]
    fn an_encode_receipt_holds_only_nonces_taken_from_the_context() {
        // The OMS keeps a receipt's nonces as placement nonces; each comes from the reserved
        // block, once per item, so a receipt cannot name a nonce the request did not use.
        let ctx = EncodeCtx {
            wall: WallNs(5),
            mono: MonoNs(6),
            nonces: NonceBlock::consecutive(700, 2).unwrap(),
        };
        let mut receipt = EncodeReceipt::new();
        assert_eq!(receipt.use_nonce(&ctx, 1), Some(701));
        assert_eq!(
            receipt.use_nonce(&ctx, 1),
            None,
            "an item takes its nonce once"
        );
        assert_eq!(receipt.use_nonce(&ctx, 2), None, "past the reserved block");
        assert_eq!(receipt.use_nonce(&ctx, 0), Some(700));
        assert_eq!(receipt.nonces(), [(1, 701), (0, 700)]);
    }
}
