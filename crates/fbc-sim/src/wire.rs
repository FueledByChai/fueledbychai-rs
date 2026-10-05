//! The frames on SimVenue's simulated order-entry stream (decision 0043): what [`SimCodec`]
//! writes for each command and what [`SimEngine`] answers, one record per frame,
//! `kind|key=value|...`, in text. Both ends build and read them here, so the format has one
//! source.
//!
//! [`SimCodec`]: crate::SimCodec
//! [`SimEngine`]: crate::SimEngine

use core::fmt::{Display, Write as _};
use core::str::FromStr;

use fbc_core::{
    AssetSym, CancelReason, InstrumentId, Liquidity, Lots, MonoNs, RejectKind, Side, TerminalHint,
    Ticks, TifTag, WallNs,
};

/// A field a frame lacks or cannot be read as.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct WireError(pub &'static str);

/// One record: its kind and its fields, in order, each value unescaped.
struct Record<'a> {
    kind: &'a str,
    fields: Vec<(&'a str, String)>,
}

impl<'a> Record<'a> {
    fn parse(bytes: &'a [u8]) -> Result<Record<'a>, WireError> {
        let text = core::str::from_utf8(bytes).map_err(|_| WireError("utf-8"))?;
        let mut parts = text.split('|');
        let kind = parts.next().unwrap_or_default();
        let mut fields = Vec::new();
        for part in parts {
            let (key, value) = part.split_once('=').ok_or(WireError("field"))?;
            fields.push((key, unescape(value)?));
        }
        Ok(Record { kind, fields })
    }

    fn opt(&self, key: &str) -> Option<&str> {
        let found = self.fields.iter().find(|(k, _)| *k == key);
        found.map(|(_, v)| v.as_str())
    }

    fn str(&self, key: &'static str) -> Result<&str, WireError> {
        self.opt(key).ok_or(WireError(key))
    }

    fn num<T: FromStr>(&self, key: &'static str) -> Result<T, WireError> {
        self.str(key)?.parse().map_err(|_| WireError(key))
    }

    fn lots(&self, key: &'static str) -> Result<Lots, WireError> {
        Lots::new(self.num(key)?).ok_or(WireError(key))
    }

    fn ticks(&self, key: &'static str) -> Result<Ticks, WireError> {
        self.num(key).map(Ticks)
    }

    fn flag(&self, key: &'static str) -> Result<bool, WireError> {
        match self.str(key)? {
            "1" => Ok(true),
            "0" => Ok(false),
            _ => Err(WireError(key)),
        }
    }

    fn side(&self) -> Result<Side, WireError> {
        match self.str("side")? {
            "B" => Ok(Side::Buy),
            "S" => Ok(Side::Sell),
            _ => Err(WireError("side")),
        }
    }

    fn inst(&self) -> Result<InstrumentId, WireError> {
        self.num("inst").map(InstrumentId::new)
    }

    /// One of `table`'s names, read back as its value.
    fn named<T: Copy>(&self, key: &'static str, table: &[(&str, T)]) -> Result<T, WireError> {
        let name = self.str(key)?;
        let found = table.iter().find(|(n, _)| *n == name);
        found.map(|(_, value)| *value).ok_or(WireError(key))
    }
}

/// A value as [`Writer::field`] escaped it, read back; refused for an escape it never writes.
fn unescape(value: &str) -> Result<String, WireError> {
    let mut out = String::with_capacity(value.len());
    let mut rest = value;
    while let Some(at) = rest.find('%') {
        out.push_str(&rest[..at]);
        out.push(match rest.get(at + 1..at + 3) {
            Some("25") => '%',
            Some("7C") => '|',
            Some("3D") => '=',
            _ => return Err(WireError("escape")),
        });
        rest = &rest[at + 3..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Builds one record.
struct Writer(String);

impl Writer {
    fn new(kind: &str) -> Writer {
        Writer(kind.to_owned())
    }

    /// Adds `key=value`, the value escaped (Codex r4182154750): `%`, `|` and `=` are written
    /// `%25`, `%7C` and `%3D`, so no value can split a record or a field.
    fn field(mut self, key: &str, value: impl Display) -> Writer {
        let value = value.to_string();
        let escaped = value
            .replace('%', "%25")
            .replace('|', "%7C")
            .replace('=', "%3D");
        // Writing to a String cannot fail.
        let _ = write!(self.0, "|{key}={escaped}");
        self
    }

    fn side(self, side: Side) -> Writer {
        self.field("side", if side == Side::Buy { "B" } else { "S" })
    }

    fn flag(self, key: &str, on: bool) -> Writer {
        self.field(key, u8::from(on))
    }

    fn name<T: Copy + PartialEq>(self, key: &str, table: &[(&str, T)], value: T) -> Writer {
        let found = table.iter().find(|(_, v)| *v == value);
        self.field(key, found.map_or("", |(name, _)| *name))
    }

    fn finish(self) -> Vec<u8> {
        self.0.into_bytes()
    }
}

const TIFS: &[(&str, TifTag)] = &[
    ("gtc", TifTag::Gtc),
    ("ioc", TifTag::Ioc),
    ("fok", TifTag::Fok),
];

const LIQUIDITY: &[(&str, Liquidity)] = &[("M", Liquidity::Maker), ("T", Liquidity::Taker)];

const STATES: &[(&str, SimState)] = &[
    ("open", SimState::Open),
    ("filled", SimState::Filled),
    ("canceled", SimState::Canceled(CancelReason::Requested)),
    ("unfilled", SimState::Canceled(CancelReason::Unfilled)),
    ("venue", SimState::Canceled(CancelReason::Venue)),
];

const REFUSALS: &[(&str, Refusal)] = &[
    ("post_only", Refusal::PostOnlyWouldCross),
    ("not_found", Refusal::NotFound),
    ("filled", Refusal::Terminal(TerminalHint::Filled)),
    ("canceled", Refusal::Terminal(TerminalHint::Canceled)),
    ("duplicate", Refusal::DuplicateClientId),
    ("no_book", Refusal::NoBook),
    ("no_fee", Refusal::NoFee),
    ("invalid_qty", Refusal::InvalidQty),
];

/// When a command left the client: its encode's wall and monotonic time.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct Sent {
    pub mono: MonoNs,
    pub wall: WallNs,
}

/// A command frame, codec to engine.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Command {
    Place(Place),
    Cancel(Cancel),
}

/// A new order. `px` is `None` for a market order.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Place {
    pub rpc: u64,
    pub sent: Sent,
    pub cid: String,
    pub inst: InstrumentId,
    pub side: Side,
    pub px: Option<Ticks>,
    pub qty: Lots,
    pub tif: TifTag,
    pub post_only: bool,
    pub reduce_only: bool,
}

/// A cancel naming its order by the venue's id or our wire client id.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Cancel {
    pub rpc: u64,
    pub sent: Sent,
    pub target: Target,
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Target {
    Venue(String),
    Client(String),
}

impl Command {
    pub(crate) fn sent(&self) -> Sent {
        match self {
            Command::Place(p) => p.sent,
            Command::Cancel(c) => c.sent,
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Command::Place(p) => {
                let w = Writer::new("place")
                    .field("rpc", p.rpc)
                    .field("mono", p.sent.mono.0)
                    .field("wall", p.sent.wall.0)
                    .field("cid", &p.cid)
                    .field("inst", p.inst.get())
                    .side(p.side);
                let w = match p.px {
                    Some(px) => w.field("px", px.0),
                    None => w,
                };
                w.field("qty", p.qty.get())
                    .name("tif", TIFS, p.tif)
                    .flag("po", p.post_only)
                    .flag("ro", p.reduce_only)
                    .finish()
            }
            Command::Cancel(c) => {
                let (key, id) = match &c.target {
                    Target::Venue(vid) => ("vid", vid),
                    Target::Client(cid) => ("cid", cid),
                };
                Writer::new("cancel")
                    .field("rpc", c.rpc)
                    .field("mono", c.sent.mono.0)
                    .field("wall", c.sent.wall.0)
                    .field(key, id)
                    .finish()
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Command, WireError> {
        let r = Record::parse(bytes)?;
        let rpc = r.num("rpc")?;
        let sent = Sent {
            mono: MonoNs(r.num("mono")?),
            wall: WallNs(r.num("wall")?),
        };
        match r.kind {
            "place" => Ok(Command::Place(Place {
                rpc,
                sent,
                cid: r.str("cid")?.to_owned(),
                inst: r.inst()?,
                side: r.side()?,
                px: r.opt("px").map(|_| r.ticks("px")).transpose()?,
                qty: r.lots("qty")?,
                tif: r.named("tif", TIFS)?,
                post_only: r.flag("po")?,
                reduce_only: r.flag("ro")?,
            })),
            "cancel" => {
                let target = match (r.opt("vid"), r.opt("cid")) {
                    (Some(vid), _) => Target::Venue(vid.to_owned()),
                    (None, Some(cid)) => Target::Client(cid.to_owned()),
                    (None, None) => return Err(WireError("target")),
                };
                Ok(Command::Cancel(Cancel { rpc, sent, target }))
            }
            _ => Err(WireError("kind")),
        }
    }
}

/// The states an order event of the simulated venue reports.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum SimState {
    Open,
    Filled,
    Canceled(CancelReason),
}

/// Why the simulated venue refused a command.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Refusal {
    PostOnlyWouldCross,
    NotFound,
    Terminal(TerminalHint),
    /// A placement reusing a client id the venue already knows.
    DuplicateClientId,
    /// The venue's book cannot place the order: no spec or no trading book for the instrument,
    /// the book is not valid, or it does not know the size at the order's price (decision 0038).
    NoBook,
    /// The consumer's fee book has no current rate for a fill the order would take.
    NoFee,
    /// A placement of zero lots, which is never an order, or outside the spec's size limits.
    InvalidQty,
}

impl Refusal {
    /// The reject kind it maps to.
    pub(crate) fn kind(self) -> RejectKind {
        match self {
            Refusal::PostOnlyWouldCross => RejectKind::PostOnlyWouldCross,
            Refusal::NotFound => RejectKind::NotFound,
            Refusal::Terminal(hint) => RejectKind::AlreadyTerminal(hint),
            Refusal::InvalidQty => RejectKind::InvalidQty,
            Refusal::DuplicateClientId | Refusal::NoBook | Refusal::NoFee => RejectKind::Other,
        }
    }

    /// Its name on the wire, which is the venue code the codec reports.
    pub(crate) fn code(self) -> &'static str {
        let found = REFUSALS.iter().find(|(_, r)| *r == self);
        found.map_or("", |(name, _)| *name)
    }
}

/// An answer frame, engine to codec. Every one carries the engine's sequence number.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum Reply {
    /// A command accepted, naming the order it placed or cancelled.
    Accepted {
        rpc: u64,
        cid: String,
        vid: String,
    },
    Rejected {
        rpc: u64,
        refusal: Refusal,
    },
    Order(OrderEvent),
    Fill(FillRecord),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct OrderEvent {
    pub cid: String,
    pub vid: String,
    pub inst: InstrumentId,
    pub side: Side,
    pub state: SimState,
    pub cum: Lots,
    pub px: Option<Ticks>,
    pub qty: Lots,
    pub post_only: bool,
    pub reduce_only: bool,
}

/// A fill; `fee` is the raw amount in the simulated venue's declared fee sign.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct FillRecord {
    pub fid: String,
    pub cid: String,
    pub vid: String,
    pub inst: InstrumentId,
    pub side: Side,
    pub px: Ticks,
    pub qty: Lots,
    pub cum: Lots,
    pub liquidity: Liquidity,
    pub fee: i128,
    pub asset: AssetSym,
}

impl Reply {
    pub(crate) fn encode(&self, seq: u64) -> Vec<u8> {
        match self {
            Reply::Accepted { rpc, cid, vid } => Writer::new("ack")
                .field("seq", seq)
                .field("rpc", rpc)
                .field("cid", cid)
                .field("vid", vid),
            Reply::Rejected { rpc, refusal } => Writer::new("reject")
                .field("seq", seq)
                .field("rpc", rpc)
                .name("code", REFUSALS, *refusal),
            Reply::Order(o) => {
                let w = Writer::new("order")
                    .field("seq", seq)
                    .field("cid", &o.cid)
                    .field("vid", &o.vid)
                    .field("inst", o.inst.get())
                    .side(o.side)
                    .name("state", STATES, o.state)
                    .field("cum", o.cum.get());
                let w = match o.px {
                    Some(px) => w.field("px", px.0),
                    None => w,
                };
                w.field("qty", o.qty.get())
                    .flag("po", o.post_only)
                    .flag("ro", o.reduce_only)
            }
            Reply::Fill(f) => Writer::new("fill")
                .field("seq", seq)
                .field("fid", &f.fid)
                .field("cid", &f.cid)
                .field("vid", &f.vid)
                .field("inst", f.inst.get())
                .side(f.side)
                .field("px", f.px.0)
                .field("qty", f.qty.get())
                .field("cum", f.cum.get())
                .name("liq", LIQUIDITY, f.liquidity)
                .field("fee", f.fee)
                .field("asset", f.asset.as_str()),
        }
        .finish()
    }

    /// The reply and its sequence number.
    pub(crate) fn decode(bytes: &[u8]) -> Result<(u64, Reply), WireError> {
        let r = Record::parse(bytes)?;
        let seq = r.num("seq")?;
        let reply = match r.kind {
            "ack" => Reply::Accepted {
                rpc: r.num("rpc")?,
                cid: r.str("cid")?.to_owned(),
                vid: r.str("vid")?.to_owned(),
            },
            "reject" => Reply::Rejected {
                rpc: r.num("rpc")?,
                refusal: r.named("code", REFUSALS)?,
            },
            "order" => Reply::Order(OrderEvent {
                cid: r.str("cid")?.to_owned(),
                vid: r.str("vid")?.to_owned(),
                inst: r.inst()?,
                side: r.side()?,
                state: r.named("state", STATES)?,
                cum: r.lots("cum")?,
                px: r.opt("px").map(|_| r.ticks("px")).transpose()?,
                qty: r.lots("qty")?,
                post_only: r.flag("po")?,
                reduce_only: r.flag("ro")?,
            }),
            "fill" => Reply::Fill(FillRecord {
                fid: r.str("fid")?.to_owned(),
                cid: r.str("cid")?.to_owned(),
                vid: r.str("vid")?.to_owned(),
                inst: r.inst()?,
                side: r.side()?,
                px: r.ticks("px")?,
                qty: r.lots("qty")?,
                cum: r.lots("cum")?,
                liquidity: r.named("liq", LIQUIDITY)?,
                fee: r.num("fee")?,
                asset: AssetSym::new(r.str("asset")?).ok_or(WireError("asset"))?,
            }),
            _ => return Err(WireError("kind")),
        };
        Ok((seq, reply))
    }
}
