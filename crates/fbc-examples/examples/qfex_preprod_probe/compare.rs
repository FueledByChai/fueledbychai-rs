//! The place stages' comparisons of the streams with the reads (FBC-23ch, a slice of FBC-9sk2;
//! decisions 0109, 0132 item 7, 0135 item 1, 0148 item 8 and 0151 item 4): after each place,
//! cancel and fill, what a never-arming read shows of the account is compared with what the
//! session's streams showed, until the two agree or the wait ends.
//!
//! - **The reads** ([`Socket`]): one trade socket per comparison that never arms, its
//!   authentication and then, each read, `get_user_orders` from offset 0 until a page that is not
//!   full, plus REST `GET /user/positions`, all through the adapter's `account_reads` on the
//!   guarded configuration, decoded under the run's `--namespace`. Every frame and request is
//!   charged to the run's shared provider-user limiter first; one it has no room for is not sent
//!   and skips that read ([`Attempt::Skipped`]), which is not waited for (decision 0135 item 1).
//!   A `RateLimited` err answering the authentication or a page holds the shared budget, as the
//!   adapter asks of its caller (decision 0126), and is a stop, [`Stop::RateRefused`] on
//!   [`RefusedOn::ReadSocket`]; so is every other refusal the frame's count shows, one the
//!   journal keeps whole, which holds no budget, included (decision 0135 item 2,
//!   [`Refusals::shown`]), and any rise in the limiter's `rejected.user` count during a
//!   read; a REST answer with status 429 or 418 is one on [`RefusedOn::Rest`] (decision 0148
//!   item 8). Each frame the socket reads is counted as a read socket's ([`Refusals`]). A read
//!   whose socket failed drops it, and the next read opens another.
//! - **The cadence** ([`Cadence`], [`compare`]): the first read at once, then each one
//!   `--read-interval-ms` after the one before began, so a read that runs long delays the rest,
//!   at most [`MAX_READS`] and none beginning past `--agree-wait-ms` from the comparison's start
//!   (0151 item 4); the comparison ends at the first read that agrees.
//! - **Agreement** ([`judge`]): every order a read lists is one of ours, open or in flight in the
//!   streams; every order of ours the streams show open is listed; and the REST position on the
//!   traded symbol equals the registry's. An order of ours in flight may be listed or not. The
//!   time from the step to the first read that agrees is kept ([`Compared::first_agreed`]).
//! - **At the end of the wait**, judged on the last read made: an order listed that is not ours
//!   or that the streams showed ended, or a position that still differs, is a stop,
//!   [`Stop::ReadDisagrees`] (decision 0148 item 8); a read still missing an order of ours is
//!   recorded as lag ([`Ended::Lagged`]); and a comparison none of whose reads was made is
//!   recorded as unread ([`Ended::Unread`]), which the reads after the session then judge.

use std::future::Future;
use std::time::Duration;

use fbc_core::{
    CidMatch, ClientOrderId, EncodeCtx, HttpResponse, Inbound, Namespace, VenueCaps, VenueConfig,
    VenueOrderId, Via, dispatch,
};
use fbc_runtime::ws::WebSocket;
use fbc_runtime::{Connector, RateLimiter};
use fbc_venue_qfex::account_reads::{self, NotAPage, PageAsk, PageOrder, SocketErr};
use fbc_venue_qfex::auth::ReadAuth;
use rust_decimal::Decimal;
use tokio::time::Instant;

use crate::auth_qfex;
use crate::places::{RefusedOn, Stop};
use crate::probe_args::{self, MAX_READS, Places};
use crate::refusals::{self, Refusals};
use crate::stage::{self, Data, NO_CONN};

/// When a comparison reads: one read at once, then each one `interval` after the one before
/// began, at most `cap` and none beginning past `wait` from its start (0151 item 4).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Cadence {
    pub interval: Duration,
    pub wait: Duration,
    pub cap: u64,
}

impl Cadence {
    /// `--read-interval-ms` and `--agree-wait-ms`, at most [`MAX_READS`] reads.
    pub fn of(places: &Places) -> Cadence {
        Cadence {
            interval: places.read_interval,
            wait: places.agree_wait,
            cap: MAX_READS,
        }
    }

    /// How many reads it allows: as `probe_args::reads` counts them, at most `cap`.
    pub fn reads(&self) -> u64 {
        probe_args::reads(self.wait, self.interval).min(self.cap)
    }
}

/// Where the streams show one order of ours.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Seen {
    /// Sent, its outcome not in yet (PendingNew), or `Unknown`.
    InFlight,
    /// Acknowledged and resting.
    Open,
    /// Ended: filled, cancelled or refused.
    Ended,
}

/// One order of ours as the streams show it: its client id, its venue id once known, and where
/// it stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ours {
    pub cid: ClientOrderId,
    pub venue: Option<VenueOrderId>,
    pub seen: Seen,
}

/// What the streams show of the account on the traded market: our orders, and the registry's
/// position in the venue's units.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Streams {
    pub orders: Vec<Ours>,
    pub position: Decimal,
}

/// One order a read listed: its venue id and its client id as decoded under our namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    pub order: VenueOrderId,
    pub client: CidMatch,
}

impl Listed {
    fn of(row: &PageOrder) -> Listed {
        Listed {
            order: row.order.clone(),
            client: row.client,
        }
    }
}

/// What one read showed: every order its pages listed, once each, the TWAPs they listed, and the
/// REST position on the traded symbol (zero when no row names it).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Read {
    pub listed: Vec<Listed>,
    pub twaps: usize,
    pub position: Decimal,
}

/// How one read compared with the streams.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Judged {
    /// Orders and TWAPs listed that are not ours.
    pub foreign: usize,
    /// Orders of ours listed that the streams showed ended.
    pub ended: usize,
    /// Orders of ours the streams show open that the read did not list.
    pub missing: usize,
    /// Whether the REST position differs from the registry's.
    pub position_differs: bool,
}

impl Judged {
    pub fn agrees(&self) -> bool {
        *self == Judged::default()
    }

    /// Whether it shows what the streams did not: an order not ours or ended, or another
    /// position. A read that only misses an order of ours lags; it does not disagree.
    pub fn disagrees(&self) -> bool {
        self.foreign > 0 || self.ended > 0 || self.position_differs
    }
}

/// `read` against `streams` ([`compare`]'s module): each order listed is matched to one of ours
/// by its venue id, or by its client id where the streams know no venue id for it yet.
pub fn judge(streams: &Streams, read: &Read) -> Judged {
    let ours_of = |listed: &Listed| {
        let by_venue = (streams.orders.iter()).find(|o| o.venue.as_ref() == Some(&listed.order));
        let by_cid = || {
            (streams.orders.iter())
                .find(|o| o.venue.is_none() && listed.client == CidMatch::Ours(o.cid))
        };
        by_venue.or_else(by_cid)
    };
    let mut judged = Judged {
        foreign: read.twaps,
        ..Judged::default()
    };
    let mut listed_ours = Vec::new();
    for listed in &read.listed {
        match ours_of(listed) {
            None => judged.foreign += 1,
            Some(o) if o.seen == Seen::Ended => judged.ended += 1,
            Some(o) => listed_ours.push(o.cid),
        }
    }
    let open = streams.orders.iter().filter(|o| o.seen == Seen::Open);
    judged.missing = open.filter(|o| !listed_ours.contains(&o.cid)).count();
    judged.position_differs = read.position != streams.position;
    judged
}

/// What one read of a comparison came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Attempt {
    Read(Read),
    /// The shared limiter had no room for one of its frames or its request, so it was not
    /// sent and the read not made.
    Skipped,
    /// It could not be made: what failed, never a value.
    Failed(String),
    /// A provider rate refusal, and where it was met.
    Refused(RefusedOn),
}

/// What makes a comparison's reads ([`Socket`] on a run; a test's own otherwise).
pub trait Reader {
    fn read(&mut self) -> impl Future<Output = Attempt>;
}

/// One read of a comparison, as recorded.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Took {
    Agreed,
    /// Made, missing an order of ours and nothing else.
    Lagged,
    /// Made, showing what the streams did not.
    Disagreed,
    Skipped,
    Failed,
    Refused,
}

/// How a comparison ended.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Ended {
    /// A read agreed.
    Agreed,
    /// The wait ended with the last read made missing an order of ours, and nothing else.
    Lagged,
    /// The wait ended with the last read made showing what the streams did not: a stop.
    Disagreed,
    /// The wait ended with no read made.
    Unread,
    /// A provider rate refusal stopped it.
    Refused,
}

/// One comparison, for the report and the evidence: names and times alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Compared {
    /// The step it followed (a place, a cancel or a fill), as the driver names it.
    pub step: &'static str,
    /// Each read, when it began from the comparison's start, and what it came to.
    pub reads: Vec<(Duration, Took)>,
    /// From the step to the end of the first read that agreed.
    pub first_agreed: Option<Duration>,
    pub ended: Ended,
    /// The stop it raised: a provider rate refusal, or [`Stop::ReadDisagrees`].
    pub stop: Option<Stop>,
}

/// Compares `streams` (read afresh for each read) with what `reader` reads, at `cadence`, after
/// the step named `step`, made at `since` ([`compare`]'s module).
pub async fn compare<R: Reader>(
    step: &'static str,
    reader: &mut R,
    streams: impl Fn() -> Streams,
    cadence: Cadence,
    since: Instant,
) -> Compared {
    let start = Instant::now();
    let mut compared = Compared {
        step,
        reads: Vec::new(),
        first_agreed: None,
        ended: Ended::Unread,
        stop: None,
    };
    let mut last = None;
    // When the read before began: the next is due one interval after it, so no two reads begin
    // closer than the interval, however long one took (0151 item 4).
    let mut before: Option<Instant> = None;
    for _ in 0..cadence.reads() {
        if let Some(before) = before {
            tokio::time::sleep_until(before + cadence.interval).await;
        }
        let now = Instant::now();
        let began = now.duration_since(start);
        if before.is_some() && began > cadence.wait {
            break;
        }
        before = Some(now);
        let took = match reader.read().await {
            Attempt::Read(read) => {
                let judged = judge(&streams(), &read);
                last = Some(judged);
                match (judged.agrees(), judged.disagrees()) {
                    (true, _) => Took::Agreed,
                    (false, true) => Took::Disagreed,
                    (false, false) => Took::Lagged,
                }
            }
            Attempt::Skipped => Took::Skipped,
            Attempt::Failed(_) => Took::Failed,
            Attempt::Refused(on) => {
                compared.reads.push((began, Took::Refused));
                compared.ended = Ended::Refused;
                compared.stop = Some(Stop::RateRefused { on });
                return compared;
            }
        };
        compared.reads.push((began, took));
        if took == Took::Agreed {
            compared.first_agreed = Some(Instant::now().duration_since(since));
            compared.ended = Ended::Agreed;
            return compared;
        }
    }
    compared.ended = match last {
        None => Ended::Unread,
        Some(judged) if judged.disagrees() => {
            compared.stop = Some(Stop::ReadDisagrees);
            Ended::Disagreed
        }
        Some(_) => Ended::Lagged,
    };
    compared
}

/// What a comparison's socket reads with, the run's own: its connector, its shared limiter, the
/// probe mode's caps, the namespace reads decode under, the page size, the waits and the traded
/// symbol as QFEX writes it.
#[derive(Clone, Copy)]
pub struct Io<'a> {
    pub connector: &'a Connector,
    pub limiter: &'a RateLimiter,
    pub caps: &'a VenueCaps,
    pub ns: Namespace,
    pub page_size: u16,
    pub connect_wait: Duration,
    pub read_wait: Duration,
    pub symbol: &'a str,
}

/// A comparison's one never-arming trade socket, on the account `reader` reads under `cfg`, the
/// configuration the probe's guard admitted ([`compare`]'s module).
pub struct Socket<'a> {
    io: Io<'a>,
    cfg: &'a VenueConfig,
    reader: &'a ReadAuth,
    /// The context of each signed frame or request: now, and two nonces from the run's source.
    ctx: &'a mut dyn FnMut() -> EncodeCtx,
    ws: Option<WebSocket>,
    authenticated: bool,
    /// The provider rate refusals its frames showed, as a read socket's are counted, and, as its
    /// limiter count, each one it held the shared budget for: never one of the session it runs
    /// beside, which counts its own (`stage.rs`, `Run::drop_session`).
    pub heard: Refusals,
}

impl<'a> Socket<'a> {
    /// The socket, not yet open: its first read opens and authenticates it.
    pub fn new(
        io: Io<'a>,
        cfg: &'a VenueConfig,
        reader: &'a ReadAuth,
        ctx: &'a mut dyn FnMut() -> EncodeCtx,
    ) -> Socket<'a> {
        Socket {
            io,
            cfg,
            reader,
            ctx,
            ws: None,
            authenticated: false,
            heard: Refusals::default(),
        }
    }

    /// Closes it, waiting at most `--read-wait-ms`, and gives back what its frames showed.
    pub async fn close(mut self) -> Refusals {
        if let Some(mut ws) = self.ws.take() {
            let _ = tokio::time::timeout(self.io.read_wait, ws.close(None)).await;
        }
        self.heard
    }

    /// Charges one frame or request to the shared limiter: `false`, nothing sent, with no room.
    fn admit(&self, charge: fbc_core::RateCharge, via: Via, class: fbc_core::TrafficClass) -> bool {
        stage::admit(self.io.limiter, charge, via, class).is_ok()
    }

    /// Counts `frame` as a read socket's ([`Refusals::hear`]), and holds the shared budget for a
    /// `RateLimited` err, as the adapter asks: `true` for a provider rate refusal, that one or
    /// any other its count shows ([`Refusals::shown`]), one in a frame the journal keeps whole
    /// included, from which the adapter reads no hold (decision 0134 items 2 and 6).
    fn heard(&mut self, frame: &Data) -> bool {
        let raw = frame.raw();
        let spans = account_reads::redact_inbound(self.reader, Inbound::Frame(raw));
        let kept = refusals::kept(raw, &spans);
        let shown = self.heard.shown();
        (self.heard).hear(&kept, &String::from_utf8_lossy(raw.bytes()));
        match account_reads::socket_err(self.reader, raw) {
            Some(SocketErr::RateLimited(hold)) => {
                let now = Instant::now();
                (self.io.limiter).provider_rejected(now, NO_CONN, hold.charge, hold.retry_after);
                self.heard.limiter += 1;
                true
            }
            _ => self.heard.shown() > shown,
        }
    }

    /// Writes `write` on the open socket and reads its answer, within `--read-wait-ms` each: the
    /// read [`Attempt::Skipped`], nothing written, when the limiter has no room, and failed, the
    /// socket dropped, when either does not go.
    async fn ask(&mut self, write: &account_reads::SocketWrite) -> Result<Data, Attempt> {
        if !self.admit(write.charge, Via::Frame, write.class) {
            return Err(Attempt::Skipped);
        }
        let wait = self.io.read_wait;
        let ws = self.ws.as_mut().expect("an open socket");
        let written = tokio::time::timeout(wait, auth_qfex::write(ws, &write.frame)).await;
        let written = written.unwrap_or_else(|_| Err("the read socket took no frame".to_owned()));
        let answered = match written {
            Ok(()) => stage::next(ws, wait).await,
            Err(e) => Err(e),
        };
        answered.map_err(|e| self.failed(e))
    }

    /// One read, as [`Reader::read`] makes it, before the limiter's count is looked at.
    async fn attempt(&mut self) -> Attempt {
        match self.reading().await {
            Ok(read) => Attempt::Read(read),
            Err(attempt) => attempt,
        }
    }

    /// One read's frames and request in turn: the socket opened and authenticated when it is
    /// not yet, every page from offset 0, then REST positions.
    async fn reading(&mut self) -> Result<Read, Attempt> {
        let failed = |e: String| Attempt::Failed(e);
        let (cfg, reader) = (self.cfg, self.reader);
        if self.ws.is_none() {
            let url = account_reads::trade_url(cfg, reader).map_err(|e| failed(e.to_string()))?;
            let opened = auth_qfex::open(self.io.connector, &url, self.io.connect_wait).await;
            self.ws = Some(opened.map_err(failed)?);
        }
        if !self.authenticated {
            let ctx = (self.ctx)();
            let auth = account_reads::authentication(cfg, reader, &ctx);
            let reply = self.ask(&auth.map_err(|e| failed(e.to_string()))?).await?;
            if self.heard(&reply) {
                return Err(Attempt::Refused(RefusedOn::ReadSocket));
            }
            if !account_reads::authenticated(reader, reply.raw()) {
                return Err(self.failed("QFEX did not authenticate the read socket".to_owned()));
            }
            self.authenticated = true;
        }
        let mut read = Read::default();
        let mut offset = 0;
        loop {
            let ask = PageAsk::new(self.io.page_size, offset);
            let ask = ask.map_err(|e| failed(e.to_string()))?;
            let write = account_reads::user_orders(cfg, ask).map_err(|e| failed(e.to_string()))?;
            let reply = self.ask(&write).await?;
            if self.heard(&reply) {
                return Err(Attempt::Refused(RefusedOn::ReadSocket));
            }
            let (caps, ns) = (self.io.caps, self.io.ns);
            let page = dispatch(caps, ns, |scope| {
                account_reads::orders_page(reader, reply.raw(), ask, scope)
            });
            let page = page.map_err(|e| match e {
                NotAPage::RateLimited(_) => Attempt::Refused(RefusedOn::ReadSocket),
                e => self.failed(format!("get_user_orders: {e}")),
            })?;
            let before = read.listed.len();
            for row in page.orders() {
                match read.listed.iter_mut().find(|l| l.order == row.order) {
                    Some(listed) => *listed = Listed::of(row),
                    None => read.listed.push(Listed::of(row)),
                }
            }
            read.twaps += page.twaps();
            if !page.is_full() {
                break;
            }
            if read.listed.len() == before && page.twaps() == 0 {
                let why = "a full get_user_orders page named no new order".to_owned();
                return Err(self.failed(why));
            }
            offset += u32::from(ask.limit());
        }
        let ctx = (self.ctx)();
        let rest =
            account_reads::positions(cfg, reader, &ctx).map_err(|e| failed(e.to_string()))?;
        if !self.admit(rest.charge, Via::Http, rest.class) {
            return Err(Attempt::Skipped);
        }
        let sent = auth_qfex::send(self.io.connector, &rest.req, self.io.read_wait).await;
        let (status, body) = sent.map_err(failed)?;
        if matches!(status, 429 | 418) {
            return Err(Attempt::Refused(RefusedOn::Rest));
        }
        let resp = HttpResponse {
            status,
            headers: &[],
            body: &body,
        };
        let positions = dispatch(self.io.caps, self.io.ns, |scope| {
            account_reads::decode_positions(&resp, scope)
        });
        let positions = positions.map_err(|e| failed(format!("GET /user/positions: {e}")))?;
        let on = positions
            .rows()
            .iter()
            .filter(|r| r.symbol.as_wire() == self.io.symbol);
        read.position = on.map(|r| r.position).sum();
        Ok(read)
    }

    /// A read whose socket failed: the socket is dropped, so the next read opens another.
    fn failed(&mut self, why: String) -> Attempt {
        self.ws = None;
        self.authenticated = false;
        Attempt::Failed(why)
    }
}

impl Reader for Socket<'_> {
    /// One read: a provider rate refusal its frames or its request show, or any rise in the
    /// shared limiter's `rejected.user` count while it ran, is [`Attempt::Refused`].
    async fn read(&mut self) -> Attempt {
        let before = self.io.limiter.counts().rejected.user;
        let attempt = self.attempt().await;
        let rose = self.io.limiter.counts().rejected.user > before;
        match attempt {
            Attempt::Refused(on) => Attempt::Refused(on),
            _ if rose => Attempt::Refused(RefusedOn::ReadSocket),
            attempt => attempt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::VecDeque;
    use std::str::FromStr;
    use std::sync::OnceLock;

    use fbc_core::{AccountKey, CidMint, NamespaceLease, VenueFactory, WallNs};

    /// The caps the probe's reads decode under, on loopback origins nothing listens on.
    fn caps() -> VenueCaps {
        let mut cfg = VenueConfig::new();
        cfg.insert(
            fbc_venue_qfex::probe::KEY_EXEC_MODE,
            fbc_venue_qfex::probe::PREPROD_PROBE,
        );
        cfg.insert(fbc_venue_qfex::auth::TRADE_URL, "ws://127.0.0.1:1");
        cfg.insert(fbc_venue_qfex::KEY_REST_URL, "http://127.0.0.1:2");
        cfg.insert(fbc_venue_qfex::KEY_MDS_URL, "ws://127.0.0.1:3");
        cfg.insert(fbc_venue_qfex::probe::KEY_PAGE_SIZE, "1000");
        cfg.insert(fbc_venue_qfex::probe::KEY_RESYNC_TIMEOUT, "30s");
        cfg.insert(fbc_venue_qfex::probe::KEY_RPC_TIMEOUT, "600ms");
        fbc_venue_qfex::Qfex.caps(&cfg).unwrap()
    }

    /// The `seq`th of four client ids minted under a lease of the tests' own, from 1.
    fn cid(seq: usize) -> ClientOrderId {
        static MINTED: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
        let minted = MINTED.get_or_init(|| {
            let dir = std::env::temp_dir().join(format!("fbc-compare-{}", std::process::id()));
            let ns = Namespace::new(7);
            std::fs::create_dir_all(&dir).unwrap();
            let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), ns).unwrap();
            let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
            let cids = (0..4).map(|_| mint.mint().unwrap()).collect();
            let _ = std::fs::remove_dir_all(&dir);
            cids
        });
        minted[seq - 1]
    }

    fn vid(n: u8) -> VenueOrderId {
        let id = format!("{n:08x}-1111-4111-8111-111111111111");
        dispatch(&caps(), Namespace::new(7), |scope| {
            scope.venue_order_id(&id)
        })
        .unwrap()
    }

    fn dec(text: &str) -> Decimal {
        Decimal::from_str(text).unwrap()
    }

    fn ours(seq: usize, venue: Option<u8>, seen: Seen) -> Ours {
        Ours {
            cid: cid(seq),
            venue: venue.map(vid),
            seen,
        }
    }

    fn listed(n: u8, client: CidMatch) -> Listed {
        Listed {
            order: vid(n),
            client,
        }
    }

    #[test]
    fn a_read_agrees_when_it_lists_exactly_our_open_orders_and_the_registrys_position() {
        let streams = Streams {
            orders: vec![
                ours(1, Some(1), Seen::Open),
                ours(2, None, Seen::InFlight),
                ours(3, Some(3), Seen::Ended),
            ],
            position: dec("0.01"),
        };
        // X listed by its venue id; the order in flight may be listed, by its client id, or not.
        let read = Read {
            listed: vec![listed(1, CidMatch::Unparseable)],
            twaps: 0,
            position: dec("0.010"),
        };
        assert!(judge(&streams, &read).agrees());
        let with_flight = Read {
            listed: vec![
                listed(1, CidMatch::Ours(cid(1))),
                listed(2, CidMatch::Ours(cid(2))),
            ],
            ..read.clone()
        };
        assert!(judge(&streams, &with_flight).agrees());
        // Missing X is lag, not disagreement.
        let missing = Read {
            listed: Vec::new(),
            ..read.clone()
        };
        let judged = judge(&streams, &missing);
        assert_eq!((judged.missing, judged.disagrees()), (1, false));
        // An order not ours, a TWAP, an order the streams showed ended and another position each
        // disagree.
        let foreign = Read {
            listed: vec![
                listed(1, CidMatch::Ours(cid(1))),
                listed(9, CidMatch::Foreign(Namespace::new(3))),
            ],
            twaps: 1,
            ..read.clone()
        };
        assert_eq!(judge(&streams, &foreign).foreign, 2);
        let ended = Read {
            listed: vec![
                listed(1, CidMatch::Unparseable),
                listed(3, CidMatch::Ours(cid(3))),
            ],
            ..read.clone()
        };
        assert_eq!(judge(&streams, &ended).ended, 1);
        let moved = Read {
            position: dec("0"),
            ..read
        };
        let judged = judge(&streams, &moved);
        assert!(judged.position_differs && judged.disagrees());
        // An order whose venue id the streams know is matched by it alone: its client id names
        // one of ours, but another venue id is not ours.
        let other = Streams {
            orders: vec![ours(1, Some(1), Seen::Open)],
            position: Decimal::ZERO,
        };
        let by_cid = Read {
            listed: vec![listed(4, CidMatch::Ours(cid(1)))],
            ..Read::default()
        };
        let judged = judge(&other, &by_cid);
        assert_eq!((judged.foreign, judged.missing), (1, 1));
    }

    /// A reader playing `attempts` in order, recording when each read began; each read takes
    /// the next of `lengths`, then `each`.
    struct Played {
        attempts: VecDeque<Attempt>,
        at: Vec<Instant>,
        lengths: VecDeque<Duration>,
        each: Duration,
    }

    impl Reader for Played {
        async fn read(&mut self) -> Attempt {
            self.at.push(Instant::now());
            let length = self.lengths.pop_front().unwrap_or(self.each);
            tokio::time::sleep(length).await;
            self.attempts.pop_front().unwrap_or(Attempt::Skipped)
        }
    }

    fn played(attempts: Vec<Attempt>) -> Played {
        Played {
            attempts: attempts.into(),
            at: Vec::new(),
            lengths: VecDeque::new(),
            each: Duration::from_millis(30),
        }
    }

    const CADENCE: Cadence = Cadence {
        interval: Duration::from_millis(1_000),
        wait: Duration::from_millis(5_000),
        cap: MAX_READS,
    };

    fn streams() -> Streams {
        Streams {
            orders: vec![ours(1, Some(1), Seen::Open)],
            position: Decimal::ZERO,
        }
    }

    fn agreeing() -> Attempt {
        Attempt::Read(Read {
            listed: vec![listed(1, CidMatch::Ours(cid(1)))],
            ..Read::default()
        })
    }

    fn lagging() -> Attempt {
        Attempt::Read(Read::default())
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_agreeing_read_ends_the_comparison_and_is_timed_from_the_step() {
        let since = Instant::now();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut reader = played(vec![lagging(), Attempt::Skipped, agreeing(), agreeing()]);
        let compared = compare("place X", &mut reader, streams, CADENCE, since).await;
        assert_eq!(compared.ended, Ended::Agreed);
        assert_eq!(compared.stop, None);
        let took: Vec<Took> = compared.reads.iter().map(|(_, t)| *t).collect();
        assert_eq!(took, [Took::Lagged, Took::Skipped, Took::Agreed]);
        // The step, 200 ms before the comparison, then two intervals, then the read itself.
        assert_eq!(compared.first_agreed, Some(Duration::from_millis(2_230)));
        assert_eq!(compared.step, "place X");
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_longer_than_the_interval_delays_the_next_and_none_begins_past_the_wait() {
        let mut reader = played(Vec::new());
        reader.each = Duration::from_millis(1_400);
        let start = Instant::now();
        let compared = compare("cancel X", &mut reader, streams, CADENCE, start).await;
        // A read longer than the interval delays the next; none starts closer than the interval
        // to the one before, and none past the wait.
        let gaps: Vec<Duration> = reader.at.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(gaps.iter().all(|g| *g >= CADENCE.interval), "{gaps:?}");
        let began: Vec<Duration> = compared.reads.iter().map(|(at, _)| *at).collect();
        assert!(began.iter().all(|at| *at <= CADENCE.wait), "{began:?}");
        assert_eq!(began.len(), 4);
        assert_eq!(compared.ended, Ended::Unread);
        assert_eq!(compared.stop, None);
    }

    #[tokio::test(start_paused = true)]
    async fn each_read_begins_one_interval_after_the_last_began_however_long_it_took() {
        let mut reader = played(Vec::new());
        reader.lengths = [1_400, 100].map(Duration::from_millis).into();
        reader.each = Duration::from_millis(100);
        let start = Instant::now();
        let compared = compare("cancel X", &mut reader, streams, CADENCE, start).await;
        // The first read runs past the interval, so the second begins as it ends; the third
        // begins one interval after the second began, not at its slot from the start.
        let began: Vec<Duration> = reader.at.iter().map(|at| *at - start).collect();
        let want = [0, 1_400, 2_400, 3_400, 4_400].map(Duration::from_millis);
        assert_eq!(began, want);
        let gaps: Vec<Duration> = reader.at.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(gaps.iter().all(|g| *g >= CADENCE.interval), "{gaps:?}");
        assert_eq!(compared.reads.len(), 5);
        assert_eq!(compared.ended, Ended::Unread);
    }

    #[tokio::test(start_paused = true)]
    async fn a_comparison_makes_no_more_reads_than_its_cap() {
        let cadence = Cadence {
            interval: Duration::from_millis(1_000),
            wait: Duration::from_millis(60_000),
            cap: MAX_READS,
        };
        assert_eq!(cadence.reads(), MAX_READS);
        let mut reader = played(Vec::new());
        let compared = compare("fill", &mut reader, streams, cadence, Instant::now()).await;
        assert_eq!(reader.at.len(), 10);
        assert_eq!(compared.reads.len(), 10);
        // A short wait allows fewer: one read, then one each interval within it.
        let short = Cadence {
            wait: Duration::from_millis(2_500),
            ..cadence
        };
        assert_eq!(short.reads(), 3);
        let mut reader = played(Vec::new());
        compare("fill", &mut reader, streams, short, Instant::now()).await;
        assert_eq!(reader.at.len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn at_the_end_of_the_wait_a_disagreeing_read_stops_and_a_lagging_one_is_lag() {
        let disagreeing = Attempt::Read(Read {
            listed: vec![listed(1, CidMatch::Ours(cid(1)))],
            position: dec("0.01"),
            ..Read::default()
        });
        let mut reader = played(vec![
            lagging(),
            disagreeing.clone(),
            Attempt::Failed("x".into()),
        ]);
        let compared = compare("place X", &mut reader, streams, CADENCE, Instant::now()).await;
        assert_eq!(compared.ended, Ended::Disagreed);
        assert_eq!(compared.stop, Some(Stop::ReadDisagrees));
        assert_eq!(compared.first_agreed, None);
        // The last read made is the one judged: a disagreement that lags at the end is lag.
        let mut reader = played(vec![disagreeing, lagging()]);
        let compared = compare("place X", &mut reader, streams, CADENCE, Instant::now()).await;
        assert_eq!((compared.ended, compared.stop), (Ended::Lagged, None));
        let took: Vec<Took> = compared.reads.iter().map(|(_, t)| *t).collect();
        assert_eq!(&took[..2], [Took::Disagreed, Took::Lagged]);
        assert_eq!(took.len(), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn a_provider_rate_refusal_stops_the_comparison_at_once() {
        for on in [RefusedOn::ReadSocket, RefusedOn::Rest] {
            let mut reader = played(vec![lagging(), Attempt::Refused(on), agreeing()]);
            let compared = compare("place X", &mut reader, streams, CADENCE, Instant::now()).await;
            assert_eq!(compared.ended, Ended::Refused);
            assert_eq!(compared.stop, Some(Stop::RateRefused { on }));
            assert_eq!(reader.at.len(), 2);
        }
    }
}
