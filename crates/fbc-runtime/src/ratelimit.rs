//! Rate-limit buckets (decisions 0002, 0018, 0030): one per declared [`RateLimit`] and scope key,
//! charged by each request's [`RateCharge`], with a safety reserve only safety traffic may use.
//!
//! A [`RateLimiter`] holds the buckets of one venue's declared limits. A request is charged its
//! weight in every bucket whose limit [counts](RateLimit::counts) it, in the bucket its scope
//! picks: the one bucket of an [`Account`](LimitScope::Account), [`User`](LimitScope::User) or [`Ip`](LimitScope::Ip)
//! limit, the named instrument's of a [`Pair`](LimitScope::Pair) limit, the connection epoch's
//! of a [`Connection`](LimitScope::Connection) limit. A bucket is a sliding window: it holds
//! what was charged in the last `per`, so no window of that length ever holds more than
//! `units`, which a bucket that refills at a steady rate would allow at its edges.
//!
//! [`TrafficClass::Normal`] traffic stops where a bucket would drop into its
//! [`SafetyReserve`], the share the consumer configures; [`TrafficClass::Safety`] traffic may
//! use it until the bucket is empty. A request is charged in all its buckets or in none, and a
//! refusal is counted once under each scope whose bucket refused it ([`RateCounts`]); so is a
//! venue's HTTP 429 or 418 to a request charged to that scope ([`RateLimiter::rejected`]).
//!
//! [`LimitScope::AddressVolume`] (requests earned by traded volume) is not supported yet: a
//! limiter refuses limits that declare it, naming the scope, rather than leave them uncounted.
//!
//! Every limiter also samples Tokio's instant under its lock, so a delayed shard cannot charge
//! at a stale time. Clones share their buckets, and independently built user-scoped limiters
//! share by provider-user key within this process.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};

use fbc_core::{ConnKey, Effect, InstrumentId, LimitScope, RateCharge, RateLimit, TrafficClass};
use fbc_core::{OpKind, Via};
use tokio::time::Instant;

/// The share of every bucket kept for safety traffic, from the consumer's configuration
/// (0009): a bucket of `units` keeps `units × percent / 100` units, rounded down, which normal
/// traffic never uses.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SafetyReserve {
    percent: u8,
}

impl SafetyReserve {
    /// Keeps `percent` of every bucket for safety traffic; below 100, so normal traffic has
    /// some of every bucket.
    pub fn percent(percent: u8) -> Result<SafetyReserve, RateError> {
        if percent >= 100 {
            return Err(RateError::Reserve(percent));
        }
        Ok(SafetyReserve { percent })
    }

    /// The units of a bucket of `units` that normal traffic may use.
    fn normal_cap(self, units: u32) -> u64 {
        let units = u64::from(units);
        units - units * u64::from(self.percent) / 100
    }
}

/// Why a limiter could not be built, or a session would not take one.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum RateError {
    /// A safety reserve of 100% or more, which would stop all normal traffic.
    Reserve(u8),
    /// A limit of a scope the runtime cannot count yet.
    Unsupported(LimitScope),
    /// A limit that allows no units, counts over no time or counts no operation.
    Empty(RateLimit),
    /// A limit that lists an operation its scope never counts: [`OpKind::Connect`] per pair or
    /// per connection (a connection attempt names no instrument and is on no connection yet),
    /// or [`OpKind::Rest`] per connection (an HTTP request is on none).
    Unreachable(RateLimit, OpKind),
    /// A subscribe call's frame, charged this, weighs more than its buckets ever admit.
    NeverFits(RateCharge),
    /// A limiter built for limits other than the venue's own.
    OtherLimits,
    /// A user-scoped limit needs an explicit provider-user key.
    UserKeyRequired,
    /// A keyed user limiter may contain only user-scoped limits.
    MixedUserScopes,
    /// The live limiter for this provider user has different limits or reserve.
    UserBudgetMismatch,
}

impl fmt::Display for RateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RateError::Reserve(p) => write!(f, "a safety reserve of {p}% leaves no normal traffic"),
            RateError::Unsupported(scope) => {
                write!(f, "rate limits of scope {scope:?} are not supported yet")
            }
            RateError::Empty(limit) => write!(
                f,
                "a rate limit of {} units per {:?} counts nothing",
                limit.units, limit.per
            ),
            RateError::Unreachable(limit, op) => write!(
                f,
                "a rate limit of scope {:?} never counts {op:?}",
                limit.scope
            ),
            RateError::NeverFits(charge) => write!(
                f,
                "a subscribe call's frame of {:?} weighs {}, more than its buckets ever admit",
                charge.op, charge.weight
            ),
            RateError::OtherLimits => {
                f.write_str("the rate limiter was built for other limits than the venue's")
            }
            RateError::UserKeyRequired => {
                f.write_str("a user-scoped rate limit needs a provider-user key")
            }
            RateError::MixedUserScopes => {
                f.write_str("a keyed provider-user limiter needs only user-scoped limits")
            }
            RateError::UserBudgetMismatch => {
                f.write_str("this provider user's live budget has different limits or reserve")
            }
        }
    }
}

impl std::error::Error for RateError {}

/// One count per scope the runtime keeps buckets for.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct ScopeCounts {
    pub account: u64,
    pub user: u64,
    pub ip: u64,
    pub pair: u64,
    pub connection: u64,
}

impl ScopeCounts {
    /// The count of `scope`; zero for a scope no limiter keeps.
    pub fn get(&self, scope: LimitScope) -> u64 {
        let mut copy = *self;
        copy.slot(scope).map_or(0, |n| *n)
    }

    fn slot(&mut self, scope: LimitScope) -> Option<&mut u64> {
        match scope {
            LimitScope::Account => Some(&mut self.account),
            LimitScope::User => Some(&mut self.user),
            LimitScope::Ip => Some(&mut self.ip),
            LimitScope::Pair => Some(&mut self.pair),
            LimitScope::Connection => Some(&mut self.connection),
            LimitScope::AddressVolume { .. } => None,
        }
    }

    fn bump(&mut self, scopes: Scopes) {
        for scope in SCOPES {
            if scopes.has(scope)
                && let Some(n) = self.slot(scope)
            {
                *n += 1;
            }
        }
    }
}

/// What a limiter counted, by scope.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct RateCounts {
    /// Requests the limiter refused, once under each scope whose bucket refused them.
    pub refused: ScopeCounts,
    /// HTTP 429 and 418 responses, once under each scope the request was charged to.
    pub rejected: ScopeCounts,
}

/// The scopes buckets are kept for, in the order [`Scopes`] numbers them.
const SCOPES: [LimitScope; 5] = [
    LimitScope::Account,
    LimitScope::User,
    LimitScope::Ip,
    LimitScope::Pair,
    LimitScope::Connection,
];

/// A set of scopes.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
struct Scopes(u8);

impl Scopes {
    fn bit(scope: LimitScope) -> u8 {
        SCOPES
            .iter()
            .position(|s| *s == scope)
            .map_or(0, |i| 1 << i)
    }

    fn add(&mut self, scope: LimitScope) {
        self.0 |= Scopes::bit(scope);
    }

    fn has(self, scope: LimitScope) -> bool {
        self.0 & Scopes::bit(scope) != 0
    }
}

/// The scopes a request was charged to, for counting the venue's answer to it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Charged(Scopes);

impl Charged {
    /// Whether the request was charged to a bucket of `scope`.
    pub fn includes(&self, scope: LimitScope) -> bool {
        self.0.has(scope)
    }
}

/// A request was not charged: it would have taken a bucket past its cap.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Refused {
    /// When the buckets that refused it will have room for it, if nothing else is charged
    /// meanwhile; `None` when they never will, its weight being over their cap.
    pub ready_at: Option<Instant>,
}

/// One request to charge: its charge, how it goes out and its traffic class.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Request {
    pub charge: RateCharge,
    pub via: Via,
    pub class: TrafficClass,
}

impl Request {
    /// The request a frame or an HTTP effect makes; `None` for a timer or a reconnect.
    pub fn of(effect: &Effect) -> Option<Request> {
        let class = match effect {
            Effect::Send { class, .. } | Effect::Http { class, .. } => *class,
            Effect::Timer { .. } | Effect::Reconnect { .. } => return None,
        };
        let (charge, via) = effect.charge()?;
        Some(Request { charge, via, class })
    }
}

/// Which bucket of a limit a request falls in.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum BucketKey {
    /// The one bucket of an account or IP limit.
    Shared,
    /// An instrument's bucket of a per-pair limit.
    Pair(InstrumentId),
    /// A connection epoch's bucket of a per-connection limit.
    Connection(ConnKey),
}

/// What was charged in the last window: each charge's time and weight, oldest first.
#[derive(Default, Debug)]
struct Bucket {
    log: VecDeque<(Instant, u32)>,
    used: u64,
}

impl Bucket {
    /// Forgets what was charged a whole window `per` before `now`.
    fn expire(&mut self, now: Instant, per: std::time::Duration) {
        while let Some(&(at, weight)) = self.log.front() {
            // A charge whose window ends past the end of the clock never expires.
            if at.checked_add(per).is_none_or(|end| end > now) {
                break;
            }
            self.log.pop_front();
            self.used -= u64::from(weight);
        }
    }

    /// When `need` more units fit under `cap` as charges expire, for a bucket they do not fit
    /// in now; `None` when they never will.
    fn ready_at(&self, per: std::time::Duration, need: u64, cap: u64) -> Option<Instant> {
        if need > cap {
            return None;
        }
        let mut used = self.used;
        let fits = self.log.iter().find(|&&(_, weight)| {
            used -= u64::from(weight);
            used + need <= cap
        });
        fits.and_then(|&(at, _)| at.checked_add(per))
    }
}

#[derive(Debug)]
struct Inner {
    limits: Vec<RateLimit>,
    reserve: SafetyReserve,
    buckets: HashMap<(usize, BucketKey), Bucket>,
    counts: RateCounts,
    /// Serialized charge time. A shard may sample its clock before another shard but take
    /// this mutex later; never append an older timestamp behind a newer charge.
    last_now: Option<Instant>,
}

impl Inner {
    fn at(&mut self, now: Instant) -> Instant {
        let now = now.max(Instant::now());
        let at = self.last_now.map_or(now, |last| last.max(now));
        self.last_now = Some(at);
        at
    }
}

/// Opaque, consumer-assigned identity of one provider user. Every API key and subaccount of
/// that user must use the same key; different users must use different keys. It contains no
/// provider credential or account identifier.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RateUserKey(u64);

impl RateUserKey {
    pub const fn new(n: u64) -> Self {
        Self(n)
    }
    pub const fn get(self) -> u64 {
        self.0
    }
}

type UserRegistry = HashMap<RateUserKey, Arc<Mutex<Inner>>>;

fn user_registry() -> &'static Mutex<UserRegistry> {
    static REGISTRY: OnceLock<Mutex<UserRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The buckets of one venue's declared limits, shared by its clones.
#[derive(Clone, Debug)]
pub struct RateLimiter {
    inner: Arc<Mutex<Inner>>,
}

/// One bucket a batch needs: the limit, the key and the units.
struct Need {
    limit: usize,
    key: BucketKey,
    units: u64,
}

impl RateLimiter {
    /// Buckets for `limits`, with `reserve` of each kept for safety traffic. Refuses a limit of
    /// a scope it cannot count ([`LimitScope::AddressVolume`]), one that counts nothing (no
    /// units, no window or no operation) and one that lists an operation its scope never
    /// counts ([`RateError::Unreachable`]).
    pub fn new(limits: &[RateLimit], reserve: SafetyReserve) -> Result<RateLimiter, RateError> {
        if limits.iter().any(|limit| limit.scope == LimitScope::User) {
            return Err(RateError::UserKeyRequired);
        }
        Self::build(limits, reserve)
    }

    /// Returns the process-wide budget for one provider user. Independently built sessions
    /// with the same key share its buckets, even across shard threads or different venue
    /// instance IDs. The caller must assign the same key to the primary account and every
    /// subaccount of that user, distinct keys to distinct users, and ensure only this process
    /// owns their private traffic. A different declaration or reserve for a live key is refused
    /// instead of silently creating another budget.
    pub fn for_user(
        user: RateUserKey,
        limits: &[RateLimit],
        reserve: SafetyReserve,
    ) -> Result<RateLimiter, RateError> {
        if limits.is_empty() || limits.iter().any(|limit| limit.scope != LimitScope::User) {
            return Err(RateError::MixedUserScopes);
        }
        let built = Self::build(limits, reserve)?;
        let mut registry = user_registry().lock().expect("user rate registry poisoned");
        if let Some(existing) = registry.get(&user) {
            let inner = existing.lock().expect("user rate budget poisoned");
            if inner.limits != limits || inner.reserve != reserve {
                return Err(RateError::UserBudgetMismatch);
            }
            drop(inner);
            return Ok(RateLimiter {
                inner: Arc::clone(existing),
            });
        }
        registry.insert(user, Arc::clone(&built.inner));
        Ok(built)
    }

    fn build(limits: &[RateLimit], reserve: SafetyReserve) -> Result<RateLimiter, RateError> {
        for limit in limits {
            if let LimitScope::AddressVolume { .. } = limit.scope {
                return Err(RateError::Unsupported(limit.scope));
            }
            if limit.units == 0 || limit.per.is_zero() || limit.ops.is_empty() {
                return Err(RateError::Empty(*limit));
            }
            // A connection attempt names no instrument and is on no connection yet (Codex
            // r4179648042); an HTTP request is on no connection (Codex r4179682241).
            let never: &[OpKind] = match limit.scope {
                LimitScope::Pair => &[OpKind::Connect],
                LimitScope::Connection => &[OpKind::Connect, OpKind::Rest],
                _ => &[],
            };
            if let Some(op) = never.iter().find(|op| limit.ops.contains(**op)) {
                return Err(RateError::Unreachable(*limit, *op));
            }
        }
        let inner = Inner {
            limits: limits.to_vec(),
            reserve,
            buckets: HashMap::new(),
            counts: RateCounts::default(),
            last_now: None,
        };
        Ok(RateLimiter {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Whether this limiter was built for exactly `limits`.
    pub fn serves(&self, limits: &[RateLimit]) -> bool {
        self.inner.lock().expect("rate limiter poisoned").limits == limits
    }

    /// Refuses [`RateError::OtherLimits`] unless this limiter was built for exactly `limits`.
    pub fn check(&self, limits: &[RateLimit]) -> Result<(), RateError> {
        self.serves(limits)
            .then_some(())
            .ok_or(RateError::OtherLimits)
    }

    /// What it has counted.
    pub fn counts(&self) -> RateCounts {
        self.inner.lock().expect("rate limiter poisoned").counts
    }

    /// The units charged at `now` in the bucket `key` of the `limit`th declared limit, within
    /// its window.
    pub fn used(&self, now: Instant, limit: usize, key: BucketKey) -> u64 {
        let mut inner = self.inner.lock().expect("rate limiter poisoned");
        let now = inner.at(now);
        let Some(per) = inner.limits.get(limit).map(|l| l.per) else {
            return 0;
        };
        inner.buckets.get_mut(&(limit, key)).map_or(0, |b| {
            b.expire(now, per);
            b.used
        })
    }

    /// Charges `requests`, made on connection `conn`, at `now`, all or none: each in every
    /// bucket that counts it. When one would take a bucket past its cap (the reserve when any
    /// request of the batch is normal traffic, empty when all are safety traffic), none is
    /// charged and the refusal is counted.
    pub fn charge(
        &self,
        now: Instant,
        conn: ConnKey,
        requests: &[Request],
    ) -> Result<Charged, Refused> {
        self.charge_on(now, Some(conn), requests)
    }

    /// Charges opening a connection ([`OpKind::Connect`]) at `now`, as normal traffic; no
    /// per-connection limit counts it.
    pub fn connect(&self, now: Instant) -> Result<Charged, Refused> {
        let request = Request {
            charge: RateCharge::one(OpKind::Connect, None),
            // Not a frame on any connection.
            via: Via::Http,
            class: TrafficClass::Normal,
        };
        self.charge_on(now, None, &[request])
    }

    /// Charges `request`, made on connection `conn`, at `now` in every bucket that counts it,
    /// past its cap if need be: for a frame already on its way that the runtime did not choose
    /// to send (a pong the WebSocket layer writes on its own), so that later traffic waits for
    /// it rather than the venue's count running ahead of the buckets.
    pub fn record(&self, now: Instant, conn: ConnKey, request: Request) {
        let mut inner = self.inner.lock().expect("rate limiter poisoned");
        let now = inner.at(now);
        let Inner {
            limits, buckets, ..
        } = &mut *inner;
        let weight = request.charge.weight.get();
        for (i, key) in matches(limits, Some(conn), &request) {
            let bucket = buckets.entry((i, key)).or_default();
            bucket.expire(now, limits[i].per);
            bucket.log.push_back((now, weight));
            bucket.used += u64::from(weight);
        }
    }

    /// The scopes `request`, made on connection `conn`, is charged to on its own: what a
    /// venue's 429 or 418 to it is counted under when it was charged together with another,
    /// such as the connection an HTTP request opens (Codex r4179558360).
    pub fn scopes(&self, conn: ConnKey, request: &Request) -> Charged {
        let inner = self.inner.lock().expect("rate limiter poisoned");
        let mut scopes = Scopes::default();
        for (i, _) in matches(&inner.limits, Some(conn), request) {
            scopes.add(inner.limits[i].scope);
        }
        Charged(scopes)
    }

    /// Counts a venue's HTTP 429 or 418 to a request charged `charged`.
    pub fn rejected(&self, charged: Charged) {
        self.inner
            .lock()
            .expect("rate limiter poisoned")
            .counts
            .rejected
            .bump(charged.0);
    }

    /// Forgets the per-connection buckets of `conn`, an epoch that ended.
    pub fn closed(&self, conn: ConnKey) {
        let mut inner = self.inner.lock().expect("rate limiter poisoned");
        inner
            .buckets
            .retain(|(_, key), _| *key != BucketKey::Connection(conn));
    }

    fn charge_on(
        &self,
        now: Instant,
        conn: Option<ConnKey>,
        requests: &[Request],
    ) -> Result<Charged, Refused> {
        let mut inner = self.inner.lock().expect("rate limiter poisoned");
        let now = inner.at(now);
        let Inner {
            limits,
            reserve,
            buckets,
            counts,
            ..
        } = &mut *inner;
        let needs = needs(limits, conn, requests);
        // A batch with any normal request in it keeps out of the reserve of every bucket it
        // charges, not only of those its normal requests fall in (Codex r4179380153).
        let normal = requests.iter().any(|r| r.class == TrafficClass::Normal);
        let (mut refused, mut ready_at, mut charged) =
            (Scopes::default(), Some(now), Scopes::default());
        for need in &needs {
            let limit = limits[need.limit];
            let cap = match normal {
                true => reserve.normal_cap(limit.units),
                false => u64::from(limit.units),
            };
            let bucket = buckets.entry((need.limit, need.key)).or_default();
            bucket.expire(now, limit.per);
            charged.add(limit.scope);
            if bucket.used + need.units > cap {
                refused.add(limit.scope);
                let at = bucket.ready_at(limit.per, need.units, cap);
                ready_at = ready_at.zip(at).map(|(a, b)| a.max(b));
            }
        }
        if refused != Scopes::default() {
            counts.refused.bump(refused);
            return Err(Refused { ready_at });
        }
        for request in requests {
            for (i, key) in matches(limits, conn, request) {
                let bucket = buckets.entry((i, key)).or_default();
                bucket.log.push_back((now, request.charge.weight.get()));
                bucket.used += u64::from(request.charge.weight.get());
            }
        }
        Ok(Charged(charged))
    }
}

/// The buckets `request` falls in: each limit that counts it, with the key its scope picks.
fn matches<'a>(
    limits: &'a [RateLimit],
    conn: Option<ConnKey>,
    request: &'a Request,
) -> impl Iterator<Item = (usize, BucketKey)> + 'a {
    limits.iter().enumerate().filter_map(move |(i, limit)| {
        if !limit.counts(&request.charge, request.via) {
            return None;
        }
        let key = match limit.scope {
            LimitScope::Pair => BucketKey::Pair(request.charge.inst?),
            LimitScope::Connection => BucketKey::Connection(conn?),
            LimitScope::Account
            | LimitScope::User
            | LimitScope::Ip
            | LimitScope::AddressVolume { .. } => BucketKey::Shared,
        };
        Some((i, key))
    })
}

/// What `requests` need of each bucket, together.
fn needs(limits: &[RateLimit], conn: Option<ConnKey>, requests: &[Request]) -> Vec<Need> {
    let mut needs: Vec<Need> = Vec::new();
    for request in requests {
        let units = u64::from(request.charge.weight.get());
        for (limit, key) in matches(limits, conn, request) {
            match needs.iter_mut().find(|n| n.limit == limit && n.key == key) {
                Some(need) => need.units += units,
                None => needs.push(Need { limit, key, units }),
            }
        }
    }
    needs
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{AccountKey, TagSet};
    use std::num::NonZeroU32;
    use std::time::Duration;

    fn limit(scope: LimitScope, ops: &[OpKind], units: u32, secs: u64) -> RateLimit {
        RateLimit {
            scope,
            ops: TagSet::of(ops),
            per: Duration::from_secs(secs),
            units,
        }
    }

    fn req(op: OpKind, inst: Option<u32>, weight: u32, class: TrafficClass) -> Request {
        Request {
            charge: RateCharge {
                op,
                inst: inst.map(InstrumentId::new),
                weight: NonZeroU32::new(weight).unwrap(),
            },
            via: Via::Frame,
            class,
        }
    }

    const CONN: ConnKey = ConnKey { conn: 1, epoch: 0 };
    const N: TrafficClass = TrafficClass::Normal;
    const S: TrafficClass = TrafficClass::Safety;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn a_reserve_is_a_share_below_the_whole_bucket_rounded_down() {
        assert_eq!(SafetyReserve::percent(100), Err(RateError::Reserve(100)));
        let r = SafetyReserve::percent(15).unwrap();
        assert_eq!([1, 7, 10, 20].map(|u| r.normal_cap(u)), [1, 6, 9, 17]);
        assert_eq!(SafetyReserve::percent(0).unwrap().normal_cap(3), 3);
    }

    #[test]
    fn a_limit_it_cannot_count_is_refused_naming_its_scope() {
        let r = SafetyReserve::percent(10).unwrap();
        let volume = LimitScope::AddressVolume { usdc_per_req: 1 };
        let err = RateLimiter::new(&[limit(volume, &[OpKind::Place], 1, 1)], r).unwrap_err();
        assert_eq!(err, RateError::Unsupported(volume));
        assert_eq!(
            err.to_string(),
            "rate limits of scope AddressVolume { usdc_per_req: 1 } are not supported yet"
        );
        let none = limit(LimitScope::Ip, &[OpKind::Rest], 0, 1);
        let err = RateLimiter::new(&[none], r).unwrap_err();
        assert_eq!(
            err.to_string(),
            "a rate limit of 0 units per 1s counts nothing"
        );
        let instant = limit(LimitScope::Ip, &[OpKind::Rest], 1, 0);
        assert_eq!(
            RateLimiter::new(&[instant], r).unwrap_err(),
            RateError::Empty(instant)
        );
        // A limit of no operation matches no request (Codex r4179474180).
        let nothing = limit(LimitScope::Ip, &[], 1, 1);
        assert_eq!(
            RateLimiter::new(&[nothing], r).unwrap_err(),
            RateError::Empty(nothing)
        );
        // A connection attempt names no instrument and is on no connection yet, so a limit per
        // pair or per connection never counts one; listing it is refused (Codex r4179648042).
        // Nor does a limit per connection ever count a REST request, an HTTP request (Codex
        // r4179682241).
        let never = [
            (LimitScope::Connection, OpKind::Connect),
            (LimitScope::Pair, OpKind::Connect),
            (LimitScope::Connection, OpKind::Rest),
        ];
        for (scope, op) in never {
            let unreachable = limit(scope, &[OpKind::Subscribe, op], 1, 1);
            let err = RateLimiter::new(&[unreachable], r).unwrap_err();
            assert_eq!(err, RateError::Unreachable(unreachable, op));
            let want = format!("a rate limit of scope {scope:?} never counts {op:?}");
            assert_eq!(err.to_string(), want);
        }
        // A REST request names its instrument where a limit is per pair.
        RateLimiter::new(&[limit(LimitScope::Pair, &[OpKind::Rest], 1, 1)], r).unwrap();
        assert_eq!(
            RateError::Reserve(100).to_string(),
            "a safety reserve of 100% leaves no normal traffic"
        );
        assert_eq!(
            RateError::OtherLimits.to_string(),
            "the rate limiter was built for other limits than the venue's"
        );
    }

    #[test]
    fn a_bucket_is_a_sliding_window_and_says_when_it_has_room() {
        let limits = [limit(LimitScope::Account, &[OpKind::Place], 4, 10)];
        let rates = RateLimiter::new(&limits, SafetyReserve::percent(25).unwrap()).unwrap();
        assert!(rates.serves(&limits) && !rates.serves(&[]));
        let t0 = Instant::now();
        let place = |w| [req(OpKind::Place, None, w, N)];
        rates.charge(t0, CONN, &place(1)).unwrap();
        rates.charge(t0 + secs(2), CONN, &place(2)).unwrap();
        // Normal traffic stops at 3 of 4: one more waits until the first charge expires.
        let refused = rates.charge(t0 + secs(3), CONN, &place(1)).unwrap_err();
        let ready = refused.ready_at.unwrap();
        assert!(ready >= t0 + secs(10) && ready <= Instant::now() + secs(10));
        // Two more wait for the second too; four never fit under the normal cap.
        let refused = rates.charge(t0 + secs(3), CONN, &place(3)).unwrap_err();
        assert_eq!(refused.ready_at, Some(t0 + secs(12)));
        assert_eq!(
            rates.charge(t0, CONN, &place(4)).unwrap_err().ready_at,
            None
        );
        assert_eq!(rates.used(t0 + secs(9), 0, BucketKey::Shared), 3);
        assert_eq!(rates.used(t0 + secs(11), 0, BucketKey::Shared), 2);
        rates.charge(t0 + secs(11), CONN, &place(1)).unwrap();
        assert_eq!(rates.counts().refused.account, 3);
        // Nothing of a limit that is not declared, or of a bucket never charged.
        assert_eq!(rates.used(t0, 1, BucketKey::Shared), 0);
        assert_eq!(rates.used(t0, 0, BucketKey::Pair(InstrumentId::new(1))), 0);
    }

    #[test]
    fn a_batch_is_charged_whole_or_not_at_all_and_its_normal_part_keeps_out_of_the_reserve() {
        let limits = [
            limit(LimitScope::Connection, &[OpKind::Subscribe], 4, 10),
            limit(
                LimitScope::Ip,
                &[OpKind::Subscribe, OpKind::Cancel],
                100,
                10,
            ),
        ];
        let rates = RateLimiter::new(&limits, SafetyReserve::percent(50).unwrap()).unwrap();
        let t0 = Instant::now();
        let sub = req(OpKind::Subscribe, None, 1, S);
        let normal = req(OpKind::Subscribe, None, 1, N);
        // Three safety frames fit in 4, but with one normal frame the batch keeps under 2.
        let refused = rates.charge(t0, CONN, &[sub, sub, normal]).unwrap_err();
        assert_eq!(refused.ready_at, None);
        assert_eq!(rates.used(t0, 1, BucketKey::Shared), 0);
        let charged = rates.charge(t0, CONN, &[sub, sub, sub]).unwrap();
        assert!(charged.includes(LimitScope::Connection) && charged.includes(LimitScope::Ip));
        assert!(!charged.includes(LimitScope::Pair));
        // Safety traffic uses the reserve until the bucket is empty.
        rates.charge(t0, CONN, &[sub]).unwrap();
        assert!(rates.charge(t0, CONN, &[sub]).is_err());
        // Another epoch of the connection has a bucket of its own; a closed one is forgotten.
        let next = ConnKey { epoch: 1, ..CONN };
        rates.charge(t0, next, &[sub]).unwrap();
        rates.closed(CONN);
        let key = BucketKey::Connection(CONN);
        assert_eq!(rates.used(t0, 0, key), 0);
        assert_eq!(rates.used(t0, 0, BucketKey::Connection(next)), 1);
        assert_eq!(rates.used(t0, 1, BucketKey::Shared), 5);
        let counts = rates.counts();
        assert_eq!((counts.refused.connection, counts.refused.ip), (2, 0));
    }

    #[test]
    fn a_batch_with_any_normal_request_keeps_out_of_every_reserve_it_charges() {
        // The normal subscription and the safety cancels fall in different buckets (Codex
        // r4179380153).
        let limits = [
            limit(LimitScope::Connection, &[OpKind::Subscribe], 2, 10),
            limit(LimitScope::Ip, &[OpKind::Cancel], 2, 10),
        ];
        let rates = RateLimiter::new(&limits, SafetyReserve::percent(50).unwrap()).unwrap();
        let t0 = Instant::now();
        let cancel = req(OpKind::Cancel, None, 1, S);
        let mixed = [req(OpKind::Subscribe, None, 1, N), cancel, cancel];
        assert!(rates.charge(t0, CONN, &mixed).is_err());
        assert_eq!(rates.counts().refused.ip, 1);
        assert_eq!(rates.counts().refused.connection, 0);
        // Alone, the cancels may use the IP's reserve.
        rates.charge(t0, CONN, &[cancel, cancel]).unwrap();
    }

    #[test]
    fn a_connect_counts_only_where_connect_is_listed_and_never_per_connection() {
        let limits = [
            limit(LimitScope::Ip, &[OpKind::Connect], 1, 10),
            limit(LimitScope::Connection, &[OpKind::Subscribe], 1, 10),
            limit(LimitScope::Account, &[OpKind::Place], 1, 10),
        ];
        let rates = RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap();
        let t0 = Instant::now();
        let charged = rates.connect(t0).unwrap();
        assert!(charged.includes(LimitScope::Ip) && !charged.includes(LimitScope::Connection));
        let ready = rates.connect(t0 + secs(1)).unwrap_err().ready_at.unwrap();
        assert!(ready >= t0 + secs(10) && ready <= Instant::now() + secs(10));
        rates.rejected(charged);
        let counts = rates.counts();
        assert_eq!(counts.rejected.get(LimitScope::Ip), 1);
        assert_eq!(counts.refused.get(LimitScope::Ip), 1);
        assert_eq!(counts.rejected.get(LimitScope::Account), 0);
        for scope in SCOPES {
            let ip = u64::from(scope == LimitScope::Ip);
            assert_eq!(
                (counts.rejected.get(scope), counts.refused.get(scope)),
                (ip, ip)
            );
        }
        let volume = LimitScope::AddressVolume { usdc_per_req: 1 };
        assert_eq!(counts.rejected.get(volume), 0);
    }

    #[test]
    fn a_frame_already_on_its_way_is_recorded_past_the_cap_and_later_traffic_waits_for_it() {
        let limits = [
            limit(LimitScope::Connection, &[OpKind::Control], 1, 10),
            limit(LimitScope::Pair, &[OpKind::Control], 1, 10),
        ];
        let rates = RateLimiter::new(&limits, SafetyReserve::percent(0).unwrap()).unwrap();
        let t0 = Instant::now();
        let pong = req(OpKind::Control, None, 1, S);
        rates.charge(t0, CONN, &[pong]).unwrap();
        rates.record(t0 + secs(1), CONN, pong);
        assert_eq!(rates.used(t0 + secs(1), 0, BucketKey::Connection(CONN)), 2);
        // Nothing names an instrument, so the per-pair limit holds nothing.
        assert_eq!(rates.used(t0, 1, BucketKey::Shared), 0);
        let refused = rates.charge(t0 + secs(2), CONN, &[pong]).unwrap_err();
        assert_eq!(refused.ready_at, Some(t0 + secs(11)));
    }

    #[test]
    fn a_request_is_read_off_its_effect() {
        let charge = RateCharge::one(OpKind::Control, None);
        let reconnect = Effect::Reconnect {
            stream: fbc_core::StreamId(0),
            reason: "x",
        };
        assert_eq!(Request::of(&reconnect), None);
        let send = Effect::Send {
            stream: fbc_core::StreamId(0),
            frame: fbc_core::WireSlice::plain(Vec::new()),
            rpc: None,
            class: S,
            charge,
        };
        let request = Request::of(&send).unwrap();
        assert_eq!((request.via, request.class), (Via::Frame, S));
    }

    #[test]
    fn one_provider_user_budget_covers_primary_and_subaccounts_across_threads() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<RateLimiter>();

        let user = RateUserKey::new(0xe5bd_0001);
        let other_user = RateUserKey::new(0xe5bd_0002);
        let reserve = SafetyReserve::percent(50).unwrap();
        let limits = [
            limit(
                LimitScope::User,
                &[OpKind::Place, OpKind::Amend, OpKind::Query],
                4,
                60,
            ),
            limit(LimitScope::User, &[OpKind::Cancel], 2, 60),
        ];
        assert_eq!(
            RateLimiter::new(&limits, reserve).unwrap_err(),
            RateError::UserKeyRequired
        );

        // Each account has its own key and API key, and independently constructs its limiter.
        // The provider-user key binds all three to one budget.
        let sessions = [
            (
                AccountKey::new(1),
                "primary-key",
                RateLimiter::for_user(user, &limits, reserve).unwrap(),
            ),
            (
                AccountKey::new(2),
                "sub-key-a",
                RateLimiter::for_user(user, &limits, reserve).unwrap(),
            ),
            (
                AccountKey::new(3),
                "sub-key-b",
                RateLimiter::for_user(user, &limits, reserve).unwrap(),
            ),
        ];
        assert_ne!(sessions[0].0, sessions[1].0);
        assert_ne!(sessions[0].1, sessions[1].1);
        let other = RateLimiter::for_user(other_user, &limits, reserve).unwrap();
        let t0 = Instant::now();
        let place = req(OpKind::Place, None, 1, N);
        let cancel = req(OpKind::Cancel, None, 1, S);
        let query = req(OpKind::Query, None, 1, S);
        let first = sessions[0].2.charge(t0, CONN, &[place]).unwrap();
        assert!(first.includes(LimitScope::User));
        let on_another_shard = sessions[1].2.clone();
        std::thread::spawn(move || {
            on_another_shard
                .charge(t0, ConnKey { conn: 2, epoch: 0 }, &[place])
                .unwrap()
        })
        .join()
        .unwrap();
        let ready = sessions[2]
            .2
            .charge(t0, ConnKey { conn: 3, epoch: 0 }, &[place])
            .unwrap_err()
            .ready_at
            .unwrap();
        assert!(ready >= t0 + secs(60) && ready <= Instant::now() + secs(60));
        assert_eq!(sessions[0].2.used(t0, 0, BucketKey::Shared), 2);
        // Safety traffic can use the reserved half of the general bucket. The cancel bucket
        // remains independent, but is shared across the same three sessions.
        sessions[2].2.charge(t0, CONN, &[query, query]).unwrap();
        assert!(sessions[0].2.charge(t0, CONN, &[query]).is_err());
        sessions[0].2.charge(t0, CONN, &[cancel]).unwrap();
        sessions[1].2.charge(t0, CONN, &[cancel]).unwrap();
        assert!(sessions[2].2.charge(t0, CONN, &[cancel]).is_err());
        assert_eq!(sessions[2].2.used(t0, 1, BucketKey::Shared), 2);

        // A venue RateLimited reply reaches the same counters whichever account sees it.
        sessions[1].2.rejected(first);
        assert_eq!(sessions[2].2.counts().rejected.user, 1);
        assert_eq!(sessions[0].2.counts().refused.user, 3);
        other.charge(t0, CONN, &[place]).unwrap();
        assert_eq!(other.used(t0, 0, BucketKey::Shared), 1);
        assert_eq!(sessions[0].2.used(t0 + secs(61), 0, BucketKey::Shared), 0);
    }

    #[test]
    fn stale_shard_time_cannot_expire_a_newly_admitted_user_request() {
        let user = RateUserKey::new(0xe5bd_0004);
        let limits = [limit(LimitScope::User, &[OpKind::Place], 2, 60)];
        let rates =
            RateLimiter::for_user(user, &limits, SafetyReserve::percent(0).unwrap()).unwrap();
        let t0 = Instant::now();
        let place = req(OpKind::Place, None, 1, N);
        rates.charge(t0, CONN, &[place]).unwrap();
        // A later timestamp acquires the lock first and expires the original charge.
        rates.charge(t0 + secs(61), CONN, &[place]).unwrap();
        // This shard sampled time earlier, but its charge happens after the above write.
        rates.charge(t0 + secs(1), CONN, &[place]).unwrap();
        assert_eq!(rates.used(t0 + secs(62), 0, BucketKey::Shared), 2);
        assert_eq!(
            rates
                .charge(t0 + secs(62), CONN, &[place])
                .unwrap_err()
                .ready_at,
            Some(t0 + secs(121))
        );
    }

    #[test]
    fn delayed_charge_uses_time_after_acquiring_the_shared_lock() {
        let user = RateUserKey::new(0xe5bd_0005);
        let reserve = SafetyReserve::percent(0).unwrap();
        let user_limits = [limit(LimitScope::User, &[OpKind::Place], 2, 60)];
        let account_limits = [limit(LimitScope::Account, &[OpKind::Place], 2, 60)];
        for rates in [
            RateLimiter::for_user(user, &user_limits, reserve).unwrap(),
            RateLimiter::new(&account_limits, reserve).unwrap(),
        ] {
            let sampled = Instant::now();
            let guard = rates.inner.lock().unwrap();
            let worker_rates = rates.clone();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                worker_rates
                    .charge(sampled, CONN, &[req(OpKind::Place, None, 1, N)])
                    .unwrap();
            });
            started_rx.recv().unwrap();
            std::thread::sleep(Duration::from_millis(20));
            let released_at = Instant::now();
            drop(guard);
            worker.join().unwrap();
            let inner = rates.inner.lock().unwrap();
            let charged_at = inner.buckets[&(0, BucketKey::Shared)].log.back().unwrap().0;
            assert!(charged_at >= released_at);
        }
    }

    #[test]
    fn a_live_user_key_refuses_an_inconsistent_budget() {
        let user = RateUserKey::new(0xe5bd_0003);
        let reserve = SafetyReserve::percent(20).unwrap();
        let limits = [limit(LimitScope::User, &[OpKind::Place], 10, 60)];
        let held = RateLimiter::for_user(user, &limits, reserve).unwrap();
        assert_eq!(
            RateLimiter::for_user(user, &limits, SafetyReserve::percent(0).unwrap()).unwrap_err(),
            RateError::UserBudgetMismatch
        );
        assert_eq!(
            RateLimiter::for_user(
                user,
                &[limit(LimitScope::User, &[OpKind::Place], 11, 60)],
                reserve
            )
            .unwrap_err(),
            RateError::UserBudgetMismatch
        );
        assert_eq!(
            RateLimiter::for_user(
                user,
                &[limit(LimitScope::Account, &[OpKind::Place], 10, 60)],
                reserve
            )
            .unwrap_err(),
            RateError::MixedUserScopes
        );
        drop(held);
        assert_eq!(
            RateLimiter::for_user(
                user,
                &[limit(LimitScope::User, &[OpKind::Place], 11, 60)],
                reserve,
            )
            .unwrap_err(),
            RateError::UserBudgetMismatch
        );
    }
}
