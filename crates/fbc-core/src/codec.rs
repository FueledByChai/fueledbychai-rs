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

use crate::cid::ClientIdFormat;
use crate::command::{NotSentReason, OrderKind, Tif, VenueCommand};
use crate::event::{ExecEvent, MdEvent, RpcId, StreamId, TouchSourceId, VenueMeta};
use crate::fee::FeeError;
use crate::ids::{IdError, InstrumentId, OrderRef, VenueOrderId};
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

/// Why a [`WireSlice`]'s redaction spans were refused.
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
        let mut end = 0;
        for span in &redact {
            let past = usize::try_from(span.end).map_or(true, |e| e > bytes.len());
            if span.start >= span.end || past {
                return Err(RedactError::OutOfBounds);
            }
            if span.start < end {
                return Err(RedactError::Unordered);
            }
            end = span.end;
        }
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
    pub url: String,
    pub headers: Vec<Header>,
    pub body: WireSlice,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &ShownUrl(&self.url))
            .field("headers", &self.headers)
            .field("body", &self.body)
            .finish()
    }
}

/// A URL with its user information, query and fragment shown by length only.
struct ShownUrl<'a>(&'a str);

impl fmt::Debug for ShownUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let url = self.0;
        let (head, tail) = match url.find(['?', '#']) {
            Some(at) => url.split_at(at),
            None => (url, ""),
        };
        f.write_str("\"")?;
        let after_scheme = head.find("://").map_or(0, |at| at + 3);
        let authority_end = head[after_scheme..]
            .find('/')
            .map_or(head.len(), |at| after_scheme + at);
        match head[after_scheme..authority_end].rfind('@') {
            Some(at) => {
                write!(f, "{}", head[..after_scheme].escape_debug())?;
                write!(f, "<redacted {at} bytes>")?;
                write!(f, "{}", head[after_scheme + at..].escape_debug())?;
            }
            None => write!(f, "{}", head.escape_debug())?,
        }
        if let Some(sep) = tail.chars().next() {
            write!(f, "{sep}<redacted {} bytes>", tail.len() - 1)?;
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

/// Which traffic a frame is: safety traffic (cancels, reducing orders, keepalives,
/// authentication) keeps flowing at a rate scope's safety floor when normal traffic stops.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum TrafficClass {
    Safety,
    Normal,
}

/// Something a codec asks the runtime to do. The codec does none of it itself.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Effect {
    /// Write `frame` to `stream`; with `rpc`, the runtime reports `timeout` passing without an
    /// answer to [`ExecCodec::on_rpc_timeout`].
    Send {
        stream: StreamId,
        frame: WireSlice,
        rpc: Option<RpcId>,
        timeout: Option<Duration>,
        class: TrafficClass,
    },
    /// Make `req`, handing the response to the codec's `on_http` with `tag`; with `rpc`, as for
    /// [`Effect::Send`].
    Http {
        tag: HttpTag,
        req: HttpRequest,
        rpc: Option<RpcId>,
        timeout: Option<Duration>,
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
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct EncodeReceipt {
    pub nonces: Vec<(u16, u64)>,
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
pub trait MdSink {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent);
}

/// Receives the execution events a codec decodes; the runtime stamps each one into an
/// [`Envelope`](crate::Envelope).
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
    /// One book channel: an index into [`MdCaps::books`](crate::MdCaps::books). An instrument
    /// subscribes to at most one book channel: which one is a configuration choice recorded in
    /// the journal header and treated as a strategy change (design §7.2, `includes_channels`),
    /// so the book events ([`MdEvent::Level`](crate::MdEvent::Level) and the rest) name the
    /// instrument and not the channel.
    Book(u8),
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
    /// Decode one frame.
    fn on_frame(
        &mut self,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn MdSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError>;
    /// Decode the response to an HTTP request the codec asked for (a book anchor, a stats
    /// poll).
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: HttpResponse<'_>,
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

/// An order-entry codec for one account session. Deterministic given its inputs and prior
/// state; it reads no clock and draws no nonce except through [`EncodeCtx`].
pub trait ExecCodec: Send {
    /// `stream` opened: authenticate and subscribe, as effects.
    fn on_open(&mut self, stream: StreamId, ctx: &EncodeCtx, fx: &mut Effects);
    /// Encode and sign `cmd` as request `rpc`. `Err` means not sent: no byte reached a socket
    /// buffer and no effect was pushed. Never retries.
    fn encode(
        &mut self,
        cmd: &VenueCommand,
        rpc: RpcId,
        specs: &SpecTable,
        ctx: &EncodeCtx,
        fx: &mut Effects,
    ) -> Result<EncodeReceipt, NotSentReason>;
    /// Decode one frame from `stream`.
    fn on_frame(
        &mut self,
        stream: StreamId,
        f: RawFrame<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError>;
    /// Decode the response to an HTTP request the codec asked for.
    fn on_http(
        &mut self,
        tag: HttpTag,
        resp: HttpResponse<'_>,
        scope: &DecodeScope<'_>,
        specs: &SpecTable,
        sink: &mut dyn ExecSink,
        fx: &mut Effects,
    ) -> Result<(), DecodeError>;
    /// A timer the codec set fired (token refresh, keepalive, dead-man refresh).
    fn on_timer(&mut self, tag: TimerTag, ctx: &EncodeCtx, fx: &mut Effects);
    /// Request `rpc` timed out unanswered: report `Outcome { item: None, Unknown }`.
    fn on_rpc_timeout(&mut self, rpc: RpcId, sink: &mut dyn ExecSink);
    /// Read the venue's open orders and positions (reads only), reported as the `Resync*`
    /// events.
    fn resync(&mut self, ctx: &EncodeCtx, fx: &mut Effects);
    /// How the venue spells our client ids.
    fn client_id_format(&self) -> &ClientIdFormat;
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
    /// The venue's order id, when the target names it.
    pub vid: Option<&'a VenueOrderId>,
    /// The client id in the venue's wire format, when the target names it.
    pub cid: Option<&'a str>,
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

/// A cancel as it goes on the wire.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct CancelWire<'a> {
    pub spec: &'a InstrumentSpec,
    pub target: &'a OrderRef,
    pub side: Side,
    pub placement_nonce: Option<u64>,
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
            url: format!("https://venue.invalid/auth?key={secret}&v=2#{secret}"),
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
                timeout: None,
                class: TrafficClass::Safety,
            },
            Effect::Send {
                stream: StreamId(0),
                frame: body.clone(),
                rpc: None,
                timeout: None,
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
        let shown = |url: &str| format!("{:?}", ShownUrl(url));
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
}
