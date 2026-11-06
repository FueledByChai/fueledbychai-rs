//! Stage one of the QFEX pre-production probe (decision 0132 items 1, 3, 6 and 7; FBC-l931):
//! one session per account, each arming cancel-on-disconnect once, so the journal captures
//! whatever answers the arm. It places nothing and sends no command but each session's arm and
//! the probe's own reads.
//!
//! - **Before anything connects** ([`run`]): every credential the run reads is set
//!   (`src/auth_qfex.rs`), and each session's configuration passes decision 0132 item 6's guard
//!   and decision 0122 item 1's account check (the adapter's `probe::connections`): any QFEX
//!   origin but the pre-production ones, a `qfex.com` host, a mix of loopback and other
//!   origins, or an account id that is not `primary` or a lowercase subaccount UUID is refused,
//!   naming the setting and never its value. Loopback origins with a SOCKS5 proxy are refused
//!   too: the proxy would reach its own host's loopback.
//! - **The accounts** ([`Run::listing`], [`Run::check_accounts`]): `GET /user/subaccounts`, then
//!   before each session, for the primary account and every listed subaccount, `GET
//!   /user/positions` and one `get_user_orders` page on a trade socket of its own that sends
//!   only its authentication and that read, and never arms. Every request takes its origin from
//!   the guarded configuration, through the adapter's `account_reads`, and is charged to the
//!   run's one provider-user rate limiter first (refused, not waited for, when it has no room).
//!   An account that cannot be read, a listing that does not name the subaccount armed on, an
//!   open order, a TWAP or a position that is not flat stops the probe before the session, and
//!   so does a REST `open_orders` count above zero ([`AccountRead::refusal`]). Every read, write
//!   and close on a read socket waits at most `--read-wait-ms`.
//! - **The session** ([`Run::session`]): fbc-runtime's `ExecSession`, unchanged, with the
//!   consumer's `AccountWideCodAuthority` for the account it arms on. The program submits
//!   nothing: the session sends its own arm once the codec reports the stream authenticated
//!   (decision 0058), and the probe codec's resync asks for nothing, so nothing places. The
//!   handler stops the session through its control at the arm's outcome (its answer, or
//!   `Unknown` at its deadline) or at the connection's end, whichever comes first, and the
//!   program at `--session-wait`; the reconnect pacing allows one attempt a day, so no second
//!   attempt starts meanwhile. The journal keeps every frame as the codec names its spans, so
//!   the arm's window is kept as decision 0132 item 3 says. The sessions run one after the
//!   other: no two arms are in flight across the user.
//! - **The files** ([`crate::capture`]): the report, the evidence, the journal and the scrubbed
//!   fixture slice, which scripts/privacy-check.sh must pass. A record the journal dropped,
//!   marked in it or only counted by its sink, one its writer never wrote, and a journal that
//!   cannot be read to its end each make the run incomplete and the capture of every session
//!   that could have lost a record partial (decision 0006).

use std::cell::RefCell;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fbc_core::{
    AccountKey, ConnKey, ConnState, EncodeCtx, Envelope, ExecEvent, HttpResponse, MonoNs,
    Namespace, NonceBlock, NonceSource, RateCharge, RawFrame, SpecTable, TrafficClass, VenueCaps,
    VenueConfig, VenueFactory, Via, WallNs, dispatch,
};
use fbc_journal::{
    JournalError, JournalWriter, NonceSourceId, QueueSink, RedactionKey, SinkConfig, journal_queue,
};
use fbc_runtime::ws::{Message, WebSocket};
use fbc_runtime::{
    AccountWideCodAuthority, Connector, ExecControl, ExecHandler, ExecSession, ExecSessionConfig,
    InFlight, IngestClock, Journal, ProxyConfig, RateLimiter, RateUserKey, ReconnectPacing,
    ReservedBound, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::Qfex;
use fbc_venue_qfex::account_reads::{
    self, OrdersPage, PageAsk, RestRead, SocketWrite, Subaccounts,
};
use fbc_venue_qfex::auth::{ACCOUNT_KEY, AccountSelection, ReadAuth, TRADE_URL, account_selection};
use fbc_venue_qfex::probe::{
    self, KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT, Origins, PREPROD_PROBE,
};
use fbc_venue_qfex::sign::QfexNonces;
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_URL};
use futures_util::StreamExt;

use crate::auth_qfex::{self, Armed, Lookup, SUB_ID_VAR};
use crate::capture::{self, AccountRead, Evidence, SessionRun};
use crate::evidence;
use crate::probe_args::Options;

/// Where the lines go: standard output when run, a buffer under test.
pub type Out = Rc<RefCell<dyn Write>>;

/// The venue, for the sessions' `&'static dyn VenueFactory`.
static QFEX: Qfex = Qfex;

/// The client-id namespace the sessions' decode scope reads ours by: stage one mints none.
const NS: Namespace = Namespace::new(1);

/// The journal's shard.
const SHARD: u16 = 0;

/// The probe codec's resync deadline, which nothing reads until FBC-jxau lands the resync.
const RESYNC_TIMEOUT: &str = "30s";

/// The provider user the run's rate limiter counts for: one user, its accounts sharing QFEX's
/// buckets (decision 0123).
const USER: RateUserKey = RateUserKey::new(1);

/// The reconnect pacing's floor, ceiling and window: one connection attempt a day.
const DAY: Duration = Duration::from_secs(86_400);

/// The connection an HTTP request or a read socket's frame is charged under: QFEX's buckets are
/// per user, and these are on no session's connection.
const NO_CONN: ConnKey = ConnKey {
    conn: u16::MAX,
    epoch: 0,
};

/// The consumer's account key for each account a session arms on (decision 0122): the program's
/// own numbering.
fn account_key(armed: Armed) -> AccountKey {
    match armed {
        Armed::Primary => AccountKey::new(1),
        Armed::Subaccount => AccountKey::new(2),
    }
}

/// How an account is named in the output: never by its id.
fn name(armed: Armed) -> &'static str {
    match armed {
        Armed::Primary => "primary",
        Armed::Subaccount => "subaccount",
    }
}

/// What a run ended with (the evidence is read by the integration test; the program prints the
/// result on its DONE line).
#[allow(dead_code)]
#[derive(Debug)]
pub struct Outcome {
    pub ok: bool,
    pub evidence: Evidence,
}

/// The run's nonces: QFEX's random source, one for every read and session, so no nonce repeats
/// among those it remembers (the adapter's `QfexNonces`).
#[derive(Clone)]
struct Nonces(Arc<Mutex<QfexNonces>>);

impl NonceSource for Nonces {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        self.0.lock().expect("the nonce source").reserve(len)
    }
}

/// Runs stage one: the guard, the reads, one session per account in `opts.arms`, then the
/// files. An error is a refusal before anything connected; once the evidence directory exists,
/// the outcome is in the report.
pub async fn run(opts: &Options, lookup: Lookup<'_>, out: Out) -> Result<Outcome, String> {
    auth_qfex::check(lookup, &opts.arms)?;
    let base = base_config(opts);
    let mut origins = Vec::new();
    for armed in &opts.arms {
        let (cfg, _) = configured(&base, lookup, *armed)?;
        let to = probe::connections(&cfg).map_err(|e| format!("refused, nothing sent: {e}"))?;
        origins.push(to.origins);
    }
    if origins.contains(&Origins::Loopback) && opts.proxy != ProxyConfig::Direct {
        return Err(
            "--socks5 with loopback origins: the proxy would reach its own host's \
                    loopback, not this machine's; connect to a stub directly"
                .to_owned(),
        );
    }
    let dir = &opts.evidence_dir;
    fs::create_dir_all(dir).map_err(|e| format!("--evidence-dir: {e}"))?;
    let mut listing = fs::read_dir(dir).map_err(|e| format!("--evidence-dir: {e}"))?;
    if listing.next().is_some() {
        return Err("--evidence-dir is not empty: give a new directory".to_owned());
    }
    let (cfg, _) = configured(&base, lookup, opts.arms[0])?;
    let caps = QFEX.caps(&cfg).map_err(|e| e.to_string())?;
    let limits = caps.exec_limits.clone().unwrap_or_default();
    let reserve = SafetyReserve::percent(10).expect("10% is a reserve");
    let limiter = RateLimiter::for_user(USER, &limits, reserve);
    let limiter = limiter.map_err(|e| format!("the rate limiter: {e:?}"))?;
    let mut nonces = Nonces(Arc::new(Mutex::new(QfexNonces::os())));
    let key: Vec<u8> = nonces
        .reserve(4)
        .as_slice()
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let key = Arc::new(RedactionKey::new(&key).map_err(|e| format!("the journal: {e}"))?);
    let sink = SinkConfig {
        budget_bytes: 16 << 20,
        soft_limit_pct: 90,
    };
    let (sink, drain) =
        journal_queue(sink, Arc::clone(&key)).map_err(|e| format!("the journal: {e}"))?;
    // Kept to read its drop counts once the writer has closed.
    let sink = Rc::new(RefCell::new(sink));
    let writer = JournalWriter::create(dir.join("journal"), SHARD, key);
    let writer = writer.map_err(|e| format!("the journal: {e}"))?;
    let writing = drain
        .spawn(writer)
        .map_err(|e| format!("the journal: {e}"))?;
    let target = match origins[0] {
        Origins::PreProduction => "QFEX pre-production",
        Origins::Loopback => "a loopback test stub",
    };
    let mut run = Run {
        opts,
        lookup,
        out,
        base,
        caps,
        connector: Connector::new(opts.proxy.clone()),
        limiter,
        nonces,
        clock: IngestClock::new(),
        journal: Journal::new(Rc::clone(&sink)),
        origin: Instant::now(),
        ev: Evidence {
            started: evidence::utc(wall_secs()),
            target,
            ..Evidence::default()
        },
    };
    run.line(format_args!(
        "PROBE stage one against {target}: arms on {:?}",
        opts.arms
    ));
    if let Err(why) = run.stage().await {
        run.line(format_args!("STOPPED {why}"));
        run.ev.stopped = Some(why);
    }
    let Run {
        journal,
        mut ev,
        out,
        ..
    } = run;
    // Every session has dropped its handle on the journal; this one goes too, and the writer
    // thread writes what is queued, the marker of a gap still open, and closes its segment.
    // Then the sink's drop counts, every class's, which nothing offers to now: a drop the
    // journal holds no marker for counts as much as one it does (decision 0006).
    drop(journal);
    let closed = writing.close();
    journal_closed(&mut ev, closed, &sink.borrow());
    drop(sink);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    let failed = write_slice(dir, &mut ev, &script).err();
    ev.errors.extend(failed);
    let (report, json) = capture::outputs(&mut ev);
    fs::write(dir.join("report.md"), report).map_err(|e| format!("report.md: {e}"))?;
    fs::write(dir.join("evidence.json"), json).map_err(|e| format!("evidence.json: {e}"))?;
    let ok = ev.ok();
    let done = if ok { "ok" } else { "INCOMPLETE" };
    let _ = writeln!(
        out.borrow_mut(),
        "DONE {done}: {} ({} fixtures)",
        dir.display(),
        ev.fixtures.len()
    );
    Ok(Outcome { ok, evidence: ev })
}

/// The configuration every session and read shares: the probe mode, its settings and the
/// origins. Each session's account and public key are added from the environment, and its
/// account key from the program's numbering ([`configured`]).
fn base_config(opts: &Options) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_EXEC_MODE, PREPROD_PROBE);
    cfg.insert(TRADE_URL, &opts.trade_url);
    cfg.insert(KEY_REST_URL, &opts.rest_url);
    cfg.insert(KEY_MDS_URL, &opts.mds_url);
    cfg.insert(KEY_PAGE_SIZE, &opts.page_size.to_string());
    cfg.insert(KEY_RESYNC_TIMEOUT, RESYNC_TIMEOUT);
    cfg.insert(KEY_RPC_TIMEOUT, &format!("{}ms", opts.rpc_timeout_ms));
    cfg
}

/// The configuration of the session arming on `armed`, and its credentials: `base` with the
/// account and public key the environment gives it (`auth_qfex::session`) and the account key
/// the program numbers it by (decision 0122 item 1).
fn configured(
    base: &VenueConfig,
    lookup: Lookup<'_>,
    armed: Armed,
) -> Result<(VenueConfig, fbc_core::Secrets), String> {
    let (mut cfg, creds) = auth_qfex::session(base, lookup, armed)?;
    cfg.insert(ACCOUNT_KEY, &account_key(armed).get().to_string());
    Ok((cfg, creds))
}

/// The seconds since the epoch now.
fn wall_secs() -> i64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH);
    now.map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// The records `sink` dropped since it was made, Normal then Safety ([`Evidence::dropped`]).
pub fn journal_dropped(sink: &QueueSink) -> [u64; 2] {
    [TrafficClass::Normal, TrafficClass::Safety].map(|class| sink.dropped(class))
}

/// What the run knows of its journal once the writer thread has closed (`closed`, what
/// `WriterThread::close` returned), which the slice holds the journal it reads to (decision
/// 0006): the error that stopped the writer, if one did ([`Evidence::writer_failed`], and among
/// the errors), and `sink`'s drop counts ([`Evidence::dropped`]), which nothing offers to now.
pub fn journal_closed(ev: &mut Evidence, closed: Result<(), JournalError>, sink: &QueueSink) {
    if let Err(e) = closed {
        ev.errors.push(format!("the journal's writer: {e}"));
        ev.writer_failed = Some(e.to_string());
    }
    ev.dropped = journal_dropped(sink);
}

/// Writes the fixture slice of the journal under `dir` into `dir/fixtures`, then runs the
/// privacy check `script` over it: fixtures it refuses are removed, never left to copy.
pub fn write_slice(dir: &Path, ev: &mut Evidence, script: &Path) -> Result<(), String> {
    let files = capture::slice(&dir.join("journal"), SHARD, ev)?;
    let fixtures = dir.join("fixtures");
    capture::write_fixtures(&fixtures, files, ev)?;
    let checked = evidence::privacy_check(script, &fixtures);
    if checked.is_err() {
        let _ = fs::remove_dir_all(&fixtures);
        ev.fixtures.clear();
    }
    checked.map_err(|e| format!("{e}; the fixtures directory was removed"))
}

/// One run's shared state.
struct Run<'a> {
    opts: &'a Options,
    lookup: Lookup<'a>,
    out: Out,
    base: VenueConfig,
    /// The probe mode's caps, whose scope decodes the reads.
    caps: VenueCaps,
    connector: Connector,
    limiter: RateLimiter,
    nonces: Nonces,
    clock: IngestClock,
    journal: Journal,
    origin: Instant,
    ev: Evidence,
}

impl Run<'_> {
    fn line(&self, args: fmt::Arguments<'_>) {
        let _ = writeln!(self.out.borrow_mut(), "{args}");
    }

    /// The listing, then each session after its accounts' checks.
    async fn stage(&mut self) -> Result<(), String> {
        let arms = self.opts.arms.clone();
        let (cfg, _) = configured(&self.base, self.lookup, arms[0])?;
        let subs = self.listing(&cfg).await?;
        self.ev.listed = Some(subs.len());
        self.line(format_args!("LISTED {} subaccounts", subs.len()));
        for (n, armed) in arms.into_iter().enumerate() {
            let (cfg, creds) = configured(&self.base, self.lookup, armed)?;
            let account = account_selection(&cfg).map_err(|e| e.to_string())?;
            if !subs.admits(account) {
                return Err(format!(
                    "{SUB_ID_VAR} names no subaccount GET /user/subaccounts lists"
                ));
            }
            self.check_accounts(&cfg, &subs, account, name(armed))
                .await?;
            let run = self.session(n, armed, cfg, creds).await?;
            self.ev.sessions.push(run);
        }
        Ok(())
    }

    /// The context of one signed read: now, and two nonces from the run's source.
    fn ctx(&mut self) -> EncodeCtx {
        let wall = SystemTime::now().duration_since(UNIX_EPOCH);
        let wall = wall.map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        let mono = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX);
        EncodeCtx {
            wall: WallNs(wall),
            mono: MonoNs(mono),
            nonces: self.nonces.reserve(ReadAuth::NONCES),
        }
    }

    /// Charges one request to the run's limiter, or refuses it, nothing sent, when the buckets
    /// have no room for it now.
    fn admit(&self, charge: RateCharge, via: Via, class: TrafficClass) -> Result<(), String> {
        let request = fbc_runtime::Request { charge, via, class };
        let charged = self
            .limiter
            .charge(tokio::time::Instant::now(), NO_CONN, &[request]);
        charged
            .map(|_| ())
            .map_err(|_| "not sent: the rate limit has no room".to_owned())
    }

    async fn rest(&self, read: &RestRead) -> Result<(u16, Vec<u8>), String> {
        self.admit(read.charge, Via::Http, read.class)?;
        auth_qfex::send(&self.connector, &read.req, self.opts.read_wait).await
    }

    /// Writes one of the adapter's frames on a read socket, waiting at most `--read-wait-ms`
    /// for a peer that stopped reading.
    async fn write(&self, ws: &mut WebSocket, write: &SocketWrite) -> Result<(), String> {
        self.admit(write.charge, Via::Frame, write.class)?;
        let wait = self.opts.read_wait;
        let written = tokio::time::timeout(wait, auth_qfex::write(ws, &write.frame)).await;
        written.map_err(|_| {
            format!(
                "the read socket took no frame within {} ms",
                wait.as_millis()
            )
        })?
    }

    /// `GET /user/subaccounts`, with the primary credential.
    async fn listing(&mut self, cfg: &VenueConfig) -> Result<Subaccounts, String> {
        let lister = auth_qfex::reader(self.lookup, AccountSelection::Primary)?;
        let ctx = self.ctx();
        let read = account_reads::subaccounts(cfg, &lister, &ctx).map_err(|e| e.to_string())?;
        let (status, body) = self.rest(&read).await?;
        let resp = HttpResponse {
            status,
            headers: &[],
            body: &body,
        };
        account_reads::decode_subaccounts(&resp).map_err(|e| format!("GET /user/subaccounts: {e}"))
    }

    /// Reads every account before the session on `armed`; stops the probe when one cannot be
    /// read, or holds an open order, a TWAP or a position that is not flat.
    async fn check_accounts(
        &mut self,
        cfg: &VenueConfig,
        subs: &Subaccounts,
        armed: AccountSelection<'_>,
        before: &'static str,
    ) -> Result<(), String> {
        for (n, account) in subs.accounts().enumerate() {
            let mut label = if n == 0 {
                "primary".to_owned()
            } else {
                format!("subaccount {n}")
            };
            label.push_str(if account == armed { " (armed)" } else { "" });
            let read = self.read_account(cfg, account).await;
            let read = read.map_err(|e| format!("{label} could not be read: {e}"))?;
            let refusal = read.refusal();
            let shown = format!(
                "{} open orders, {} TWAPs, flat {}, REST open_orders {}",
                read.page_orders, read.page_twaps, read.flat, read.rest_open_orders
            );
            self.line(format_args!("READ before {before}: {label}: {shown}"));
            self.ev.reads.push(AccountRead {
                before,
                label: label.clone(),
                ..read
            });
            if let Some(why) = refusal {
                return Err(format!(
                    "{label} {why}: no session starts (decision 0132 item 7)"
                ));
            }
        }
        Ok(())
    }

    /// One account's positions, and its open orders on a trade socket of its own.
    async fn read_account(
        &mut self,
        cfg: &VenueConfig,
        account: AccountSelection<'_>,
    ) -> Result<AccountRead, String> {
        let reader = auth_qfex::reader(self.lookup, account)?;
        let ctx = self.ctx();
        let read = account_reads::positions(cfg, &reader, &ctx).map_err(|e| e.to_string())?;
        let (status, body) = self.rest(&read).await?;
        let resp = HttpResponse {
            status,
            headers: &[],
            body: &body,
        };
        let positions = dispatch(&self.caps, NS, |scope| {
            account_reads::decode_positions(&resp, scope)
        });
        let positions = positions.map_err(|e| format!("GET /user/positions: {e}"))?;
        let url = account_reads::trade_url(cfg, &reader).map_err(|e| e.to_string())?;
        let mut ws = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await?;
        let page = self.page(cfg, &reader, &mut ws).await;
        let wait = self.opts.read_wait;
        let closed = tokio::time::timeout(wait, ws.close(None)).await;
        let page = page?;
        // A close the peer refuses is passed over (it may have closed first); one it never
        // takes fails the read, so the run cannot hang on it.
        let _ = closed.map_err(|_| {
            format!(
                "the read socket did not close within {} ms",
                wait.as_millis()
            )
        })?;
        Ok(AccountRead {
            rows: positions.rows().len(),
            flat: positions.is_flat(),
            rest_open_orders: positions.rows().iter().map(|row| row.open_orders).sum(),
            page_orders: page.orders().len(),
            page_twaps: page.twaps(),
            page_full: page.is_full(),
            ..AccountRead::default()
        })
    }

    /// The socket's authentication, its success, then one `get_user_orders` page from offset 0.
    async fn page(
        &mut self,
        cfg: &VenueConfig,
        reader: &ReadAuth,
        ws: &mut WebSocket,
    ) -> Result<OrdersPage, String> {
        let ctx = self.ctx();
        let auth = account_reads::authentication(cfg, reader, &ctx).map_err(|e| e.to_string())?;
        self.write(ws, &auth).await?;
        let reply = next(ws, self.opts.read_wait).await?;
        if !account_reads::authenticated(reader, reply.raw()) {
            return Err("QFEX did not authenticate the read socket".to_owned());
        }
        let ask = PageAsk::new(self.opts.page_size, 0).map_err(|e| e.to_string())?;
        let ask_frame = account_reads::user_orders(cfg, ask).map_err(|e| e.to_string())?;
        self.write(ws, &ask_frame).await?;
        let reply = next(ws, self.opts.read_wait).await?;
        let page = dispatch(&self.caps, NS, |scope| {
            account_reads::orders_page(reader, reply.raw(), ask, scope)
        });
        page.map_err(|e| format!("get_user_orders: {e}"))
    }

    /// One session on `armed`, connection `n` of the run: it arms once and places nothing.
    async fn session(
        &mut self,
        n: usize,
        armed: Armed,
        cfg: VenueConfig,
        creds: fbc_core::Secrets,
    ) -> Result<SessionRun, String> {
        let acct = account_key(armed);
        let seen = Rc::new(RefCell::new(Seen::default()));
        let control = Rc::new(RefCell::new(None));
        let watch = Watch {
            seen: Rc::clone(&seen),
            control: Rc::clone(&control),
        };
        let pacing = ReconnectPacing::new(DAY, DAY, 1, DAY, self.opts.connect_wait);
        let bound = ReservedBound::new(4, 4).expect("a bound with Normal room");
        let config = ExecSessionConfig {
            venue: &QFEX,
            cfg,
            creds,
            acct,
            account_wide_cod_authority: Some(AccountWideCodAuthority::new(acct)),
            rpc_ids: RpcIds::default(),
            ns: NS,
            specs: SpecTable::new(),
            connector: self.connector.clone(),
            pacing: pacing.expect("one attempt a day, each within --connect-wait-ms"),
            clock: self.clock.clone(),
            nonces: Box::new(self.nonces.clone()),
            nonce_source: NonceSourceId(0),
            conn: u16::try_from(n).expect("one or two sessions"),
            limiter: self.limiter.clone(),
            write_stall: WriteStall::new(Duration::from_secs(10)).expect("a ten-second window"),
            http_max_body: 1 << 20,
            outbound: bound,
            in_flight: InFlight::new(bound),
        };
        let label = name(armed);
        let built = ExecSession::new(config, watch);
        let (mut session, ctl) = built.map_err(|e| format!("the {label} session: {e}"))?;
        *control.borrow_mut() = Some(ctl);
        session.set_journal(self.journal.clone());
        self.line(format_args!(
            "SESSION {label}: it arms once and places nothing"
        ));
        let ran = {
            let run = session.run();
            tokio::pin!(run);
            tokio::select! {
                ran = &mut run => ran,
                () = tokio::time::sleep(self.opts.session_wait) => {
                    Watch::stop(&seen, &control, "--session-wait");
                    run.await
                }
            }
        };
        let counters = session.counters();
        drop(session);
        let seen = seen.borrow();
        let arm = seen.arm.as_deref().unwrap_or("none");
        let ended_by = seen.stopped.unwrap_or("the session's own end");
        self.line(format_args!(
            "SESSION {label}: arm outcome {arm}, ended by {ended_by}"
        ));
        Ok(SessionRun {
            account: label,
            authenticated: seen.authenticated,
            arm: seen.arm.clone(),
            ended_by,
            attempts: counters.attempts,
            failed_attempts: counters.failed_attempts,
            arm_failures: counters.arm_failures,
            decode_errors: counters.decode_errors,
            error: ran.err().map(|e| e.to_string()),
            ..SessionRun::default()
        })
    }
}

/// A data frame off a read socket.
enum Data {
    Text(String),
    Binary(Vec<u8>),
}

impl Data {
    /// The frame as the adapter reads one.
    fn raw(&self) -> RawFrame<'_> {
        match self {
            Data::Text(text) => RawFrame::Text(text),
            Data::Binary(bytes) => RawFrame::Binary(bytes),
        }
    }
}

/// The next data frame on a read socket within `wait`; pings and pongs are passed over.
async fn next(ws: &mut WebSocket, wait: Duration) -> Result<Data, String> {
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return Ok(Data::Text(text.as_str().to_owned())),
                Some(Ok(Message::Binary(bytes))) => return Ok(Data::Binary(bytes.to_vec())),
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                _ => return Err("the read socket closed before it answered".to_owned()),
            }
        }
    };
    let waited = tokio::time::timeout(wait, read).await;
    waited.map_err(|_| format!("no answer within {} ms", wait.as_millis()))?
}

/// What a session's handler saw.
#[derive(Default)]
struct Seen {
    authenticated: bool,
    /// The arm's outcome: the first outcome the session reports, since the program submits
    /// nothing and the arm is the session's only request.
    arm: Option<String>,
    /// What stopped the session, when the program did.
    stopped: Option<&'static str>,
}

/// The session's handler: it stops the session at the arm's outcome or the connection's end.
struct Watch {
    seen: Rc<RefCell<Seen>>,
    control: Rc<RefCell<Option<ExecControl>>>,
}

impl Watch {
    /// Records `why` when nothing stopped the session yet, and drops its control: the stop.
    fn stop(seen: &RefCell<Seen>, control: &RefCell<Option<ExecControl>>, why: &'static str) {
        seen.borrow_mut().stopped.get_or_insert(why);
        drop(control.borrow_mut().take());
    }
}

impl ExecHandler for Watch {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let answered = {
            let mut seen = self.seen.borrow_mut();
            let state = ConnState::Authenticated;
            seen.authenticated |=
                matches!(env.body, ExecEvent::Conn { state: s, .. } if s == state);
            match env.body {
                ExecEvent::Outcome { outcome, .. } if seen.arm.is_none() => {
                    seen.arm = Some(format!("{outcome:?}"));
                    true
                }
                _ => false,
            }
        };
        if answered {
            Watch::stop(&self.seen, &self.control, "the arm's outcome");
        }
    }

    fn on_epoch_end(&mut self, _key: ConnKey) {
        Watch::stop(&self.seen, &self.control, "the connection's end");
    }
}
