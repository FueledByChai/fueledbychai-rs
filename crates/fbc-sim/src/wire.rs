//! The frames on SimVenue's simulated order-entry stream (decision 0046): what [`SimCodec`]
//! writes for each command and what [`SimEngine`] answers, in text: one record per line,
//! `kind|key=value|...`, and one line per frame except for a batch, whose items follow its
//! header line, and a query's answer, whose order follows it. Both ends build and read them
//! here, so the format has one source.
//!
//! [`SimCodec`]: crate::SimCodec
//! [`SimEngine`]: crate::SimEngine

use core::fmt::{Display, Write as _};
use core::str::FromStr;

use fbc_core::{
    AssetSym, CancelReason, InstrumentId, Liquidity, Lots, MonoNs, NotAmendable, RejectKind, Side,
    TerminalHint, Ticks, TifTag, WallNs,
};

/// A field a frame lacks or cannot be read as.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct WireError(pub &'static str);

/// One record: its kind and its fields, in order, each value unescaped.
struct Record<'a> {
    kind: &'a str,
    fields: Vec<(&'a str, String)>,
}

/// A frame's records, one per line.
fn records(bytes: &[u8]) -> Result<Vec<Record<'_>>, WireError> {
    let text = core::str::from_utf8(bytes).map_err(|_| WireError("utf-8"))?;
    text.split('\n').map(Record::parse).collect()
}

/// A frame's one record: refused when it has more.
fn record(bytes: &[u8]) -> Result<(Record<'_>, Vec<Record<'_>>), WireError> {
    let mut lines = records(bytes)?.into_iter();
    // `split` gives at least one line.
    let head = lines.next().ok_or(WireError("kind"))?;
    Ok((head, lines.collect()))
}

impl<'a> Record<'a> {
    fn parse(text: &'a str) -> Result<Record<'a>, WireError> {
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
            Some("0A") => '\n',
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

    /// Adds `key=value`, the value escaped (Codex r4182154750): `%`, `|`, `=` and a newline
    /// are written `%25`, `%7C`, `%3D` and `%0A`, so no value can split a line, a record or a
    /// field.
    fn field(mut self, key: &str, value: impl Display) -> Writer {
        let value = value.to_string();
        let escaped = value
            .replace('%', "%25")
            .replace('|', "%7C")
            .replace('=', "%3D")
            .replace('\n', "%0A");
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

    /// Starts the next record of the frame, on its own line.
    fn line(mut self, kind: &str) -> Writer {
        self.0.push('\n');
        self.0.push_str(kind);
        self
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
    ("invalid_px", Refusal::InvalidPrice),
    (
        "not_amendable",
        Refusal::NotAmendable(NotAmendable::Unsupported),
    ),
    (
        "partially_filled",
        Refusal::NotAmendable(NotAmendable::PartiallyFilled),
    ),
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
    /// A batch of placements, each item carrying the batch's rpc and send time.
    Batch(Head, Vec<Place>),
    Amend(Amend),
    Cancel(Cancel),
    /// A batch of cancels.
    Cancels(Head, Vec<Target>),
    /// Every order on one instrument.
    CancelAll(Head, InstrumentId),
    Query(Query),
}

/// What every command frame starts with: its request and when it left the client.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct Head {
    pub rpc: u64,
    pub sent: Sent,
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

/// An amend naming its order, with the order's full values after it. `qty` is the venue's
/// wire quantity, read under the stood-in venue's `AmendQty`.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Amend {
    pub head: Head,
    pub target: Target,
    pub inst: InstrumentId,
    pub side: Side,
    pub px: Ticks,
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

/// A query naming its order by `target`, and every identifier the query's own reference
/// carries (`vid`, and our wire client id `cid`), which its answer echoes so the codec can name
/// the order the query asked about.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Query {
    pub head: Head,
    pub target: Target,
    pub vid: Option<String>,
    pub cid: Option<String>,
}

impl Writer {
    fn head(kind: &str, head: Head) -> Writer {
        Writer::new(kind)
            .field("rpc", head.rpc)
            .field("mono", head.sent.mono.0)
            .field("wall", head.sent.wall.0)
    }

    fn target(self, target: &Target) -> Writer {
        match target {
            Target::Venue(vid) => self.field("vid", vid),
            Target::Client(cid) => self.field("cid", cid),
        }
    }

    fn opt(self, key: &str, value: Option<impl Display>) -> Writer {
        match value {
            Some(value) => self.field(key, value),
            None => self,
        }
    }

    /// A placement's order fields.
    fn order(self, p: &Place) -> Writer {
        self.field("cid", &p.cid)
            .field("inst", p.inst.get())
            .side(p.side)
            .opt("px", p.px.map(|px| px.0))
            .field("qty", p.qty.get())
            .name("tif", TIFS, p.tif)
            .flag("po", p.post_only)
            .flag("ro", p.reduce_only)
    }

    /// An order event's fields.
    fn event(self, o: &OrderEvent) -> Writer {
        self.field("cid", &o.cid)
            .field("vid", &o.vid)
            .field("inst", o.inst.get())
            .side(o.side)
            .name("state", STATES, o.state)
            .field("cum", o.cum.get())
            .opt("px", o.px.map(|px| px.0))
            .field("qty", o.qty.get())
            .flag("po", o.post_only)
            .flag("ro", o.reduce_only)
    }
}

impl Record<'_> {
    fn head(&self) -> Result<Head, WireError> {
        Ok(Head {
            rpc: self.num("rpc")?,
            sent: Sent {
                mono: MonoNs(self.num("mono")?),
                wall: WallNs(self.num("wall")?),
            },
        })
    }

    fn target(&self) -> Result<Target, WireError> {
        match (self.opt("vid"), self.opt("cid")) {
            (Some(vid), _) => Ok(Target::Venue(vid.to_owned())),
            (None, Some(cid)) => Ok(Target::Client(cid.to_owned())),
            (None, None) => Err(WireError("target")),
        }
    }

    fn px(&self) -> Result<Option<Ticks>, WireError> {
        self.opt("px").map(|_| self.ticks("px")).transpose()
    }

    /// A placement under `head`.
    fn place(&self, head: Head) -> Result<Place, WireError> {
        Ok(Place {
            rpc: head.rpc,
            sent: head.sent,
            cid: self.str("cid")?.to_owned(),
            inst: self.inst()?,
            side: self.side()?,
            px: self.px()?,
            qty: self.lots("qty")?,
            tif: self.named("tif", TIFS)?,
            post_only: self.flag("po")?,
            reduce_only: self.flag("ro")?,
        })
    }

    fn event(&self) -> Result<OrderEvent, WireError> {
        Ok(OrderEvent {
            cid: self.str("cid")?.to_owned(),
            vid: self.str("vid")?.to_owned(),
            inst: self.inst()?,
            side: self.side()?,
            state: self.named("state", STATES)?,
            cum: self.lots("cum")?,
            px: self.px()?,
            qty: self.lots("qty")?,
            post_only: self.flag("po")?,
            reduce_only: self.flag("ro")?,
        })
    }
}

/// Each of `lines` read as an item of kind `item`.
fn items<'a, T>(
    lines: Vec<Record<'a>>,
    item: &str,
    read: impl Fn(&Record<'a>) -> Result<T, WireError>,
) -> Result<Vec<T>, WireError> {
    let read = |r: &Record<'a>| {
        if r.kind == item {
            read(r)
        } else {
            Err(WireError("item"))
        }
    };
    lines.iter().map(read).collect()
}

impl Command {
    pub(crate) fn sent(&self) -> Sent {
        match self {
            Command::Place(p) => p.sent,
            Command::Cancel(c) => c.sent,
            Command::Amend(Amend { head, .. })
            | Command::Query(Query { head, .. })
            | Command::Batch(head, _)
            | Command::Cancels(head, _)
            | Command::CancelAll(head, _) => head.sent,
        }
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Command::Place(p) => {
                let head = Head {
                    rpc: p.rpc,
                    sent: p.sent,
                };
                Writer::head("place", head).order(p)
            }
            Command::Batch(head, places) => places
                .iter()
                .fold(Writer::head("batch", *head), |w, p| w.line("item").order(p)),
            Command::Amend(a) => Writer::head("amend", a.head)
                .target(&a.target)
                .field("inst", a.inst.get())
                .side(a.side)
                .field("px", a.px.0)
                .field("qty", a.qty.get())
                .name("tif", TIFS, a.tif)
                .flag("po", a.post_only)
                .flag("ro", a.reduce_only),
            Command::Cancel(c) => {
                let head = Head {
                    rpc: c.rpc,
                    sent: c.sent,
                };
                Writer::head("cancel", head).target(&c.target)
            }
            Command::Cancels(head, targets) => {
                targets.iter().fold(Writer::head("cancels", *head), |w, t| {
                    w.line("item").target(t)
                })
            }
            Command::CancelAll(head, inst) => {
                Writer::head("cancelall", *head).field("inst", inst.get())
            }
            Command::Query(q) => Writer::head("query", q.head)
                .target(&q.target)
                .opt("qvid", q.vid.as_ref())
                .opt("qcid", q.cid.as_ref()),
        }
        .finish()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Command, WireError> {
        let (r, lines) = record(bytes)?;
        let head = r.head()?;
        let single = |command: Command| {
            if lines.is_empty() {
                Ok(command)
            } else {
                Err(WireError("item"))
            }
        };
        match r.kind {
            "place" => single(Command::Place(r.place(head)?)),
            "batch" => Ok(Command::Batch(
                head,
                items(lines, "item", |i| i.place(head))?,
            )),
            "amend" => single(Command::Amend(Amend {
                head,
                target: r.target()?,
                inst: r.inst()?,
                side: r.side()?,
                px: r.ticks("px")?,
                qty: r.lots("qty")?,
                tif: r.named("tif", TIFS)?,
                post_only: r.flag("po")?,
                reduce_only: r.flag("ro")?,
            })),
            "cancel" => single(Command::Cancel(Cancel {
                rpc: head.rpc,
                sent: head.sent,
                target: r.target()?,
            })),
            "cancels" => Ok(Command::Cancels(
                head,
                items(lines, "item", Record::target)?,
            )),
            "cancelall" => single(Command::CancelAll(head, r.inst()?)),
            "query" => single(Command::Query(Query {
                head,
                target: r.target()?,
                vid: r.opt("qvid").map(str::to_owned),
                cid: r.opt("qcid").map(str::to_owned),
            })),
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
    /// A limit price the instrument's price grid does not accept.
    InvalidPrice,
    /// An amend the stood-in venue's amend capabilities do not allow.
    NotAmendable(NotAmendable),
}

impl Refusal {
    /// The reject kind it maps to.
    pub(crate) fn kind(self) -> RejectKind {
        match self {
            Refusal::PostOnlyWouldCross => RejectKind::PostOnlyWouldCross,
            Refusal::NotFound => RejectKind::NotFound,
            Refusal::Terminal(hint) => RejectKind::AlreadyTerminal(hint),
            Refusal::InvalidQty => RejectKind::InvalidQty,
            Refusal::InvalidPrice => RejectKind::InvalidPrice,
            Refusal::NotAmendable(why) => RejectKind::NotAmendable(why),
            Refusal::DuplicateClientId | Refusal::NoBook | Refusal::NoFee => RejectKind::Other,
        }
    }

    /// Its name on the wire, which is the venue code the codec reports.
    pub(crate) fn code(self) -> &'static str {
        let found = REFUSALS.iter().find(|(_, r)| *r == self);
        found.map_or("", |(name, _)| *name)
    }
}

/// What became of one item of a request: accepted, naming the order it placed, amended or
/// cancelled, or refused.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) enum ItemResult {
    Accepted { cid: String, vid: String },
    Rejected(Refusal),
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
    /// Each item of a batch, in the batch's order, in one frame.
    Items {
        rpc: u64,
        items: Vec<ItemResult>,
    },
    /// A request of no items accepted whole: a cancel-all.
    Done {
        rpc: u64,
    },
    /// A query's answer, echoing the identifiers it named its order by, with the order when the
    /// venue knows it.
    Query {
        rpc: u64,
        vid: Option<String>,
        cid: Option<String>,
        found: Option<OrderEvent>,
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
            Reply::Items { rpc, items } => {
                let w = Writer::new("items").field("seq", seq).field("rpc", rpc);
                items.iter().fold(w, |w, item| match item {
                    ItemResult::Accepted { cid, vid } => {
                        w.line("ack").field("cid", cid).field("vid", vid)
                    }
                    ItemResult::Rejected(refusal) => {
                        w.line("reject").name("code", REFUSALS, *refusal)
                    }
                })
            }
            Reply::Done { rpc } => Writer::new("done").field("seq", seq).field("rpc", rpc),
            Reply::Query {
                rpc,
                vid,
                cid,
                found,
            } => {
                let w = Writer::new("query")
                    .field("seq", seq)
                    .field("rpc", rpc)
                    .opt("qvid", vid.as_ref())
                    .opt("qcid", cid.as_ref());
                match found {
                    Some(o) => w.line("order").event(o),
                    None => w,
                }
            }
            Reply::Order(o) => Writer::new("order").field("seq", seq).event(o),
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
        let (r, lines) = record(bytes)?;
        let seq = r.num("seq")?;
        let single = |reply: Reply| {
            if lines.is_empty() {
                Ok(reply)
            } else {
                Err(WireError("item"))
            }
        };
        let reply = match r.kind {
            "ack" => single(Reply::Accepted {
                rpc: r.num("rpc")?,
                cid: r.str("cid")?.to_owned(),
                vid: r.str("vid")?.to_owned(),
            }),
            "reject" => single(Reply::Rejected {
                rpc: r.num("rpc")?,
                refusal: r.named("code", REFUSALS)?,
            }),
            "items" => {
                let item = |i: &Record<'_>| match i.kind {
                    "ack" => Ok(ItemResult::Accepted {
                        cid: i.str("cid")?.to_owned(),
                        vid: i.str("vid")?.to_owned(),
                    }),
                    "reject" => Ok(ItemResult::Rejected(i.named("code", REFUSALS)?)),
                    _ => Err(WireError("item")),
                };
                let items = lines.iter().map(item).collect::<Result<_, _>>()?;
                Ok(Reply::Items {
                    rpc: r.num("rpc")?,
                    items,
                })
            }
            "done" => single(Reply::Done { rpc: r.num("rpc")? }),
            "query" => {
                let mut found = items(lines, "order", Record::event)?;
                if found.len() > 1 {
                    return Err(WireError("item"));
                }
                Ok(Reply::Query {
                    rpc: r.num("rpc")?,
                    vid: r.opt("qvid").map(str::to_owned),
                    cid: r.opt("qcid").map(str::to_owned),
                    found: found.pop(),
                })
            }
            "order" => single(Reply::Order(r.event()?)),
            "fill" => single(Reply::Fill(FillRecord {
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
            })),
            _ => Err(WireError("kind")),
        }?;
        Ok((seq, reply))
    }
}
