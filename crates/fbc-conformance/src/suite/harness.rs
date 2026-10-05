//! What the checks share: the venue's caps and first instrument under the assumed setup,
//! codecs built fresh from the factory, client ids minted under a lease of the suite's own,
//! venue ids made in the venue's decode scope, and the commands built from them.

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::{
    AccountKey, AmendOrder, CancelOrder, Channel, CidMint, ClientOrderId, Effects, EncodeCtx,
    EncodeReceipt, ExecCodec, Feature, InstrumentId, Lots, MonoNs, Namespace, NamespaceLease,
    NewOrder, NonceBlock, NotSentReason, OrderCaps, OrderKind, OrderKindTag, OrderRef, PathStamps,
    QueryOrder, RefKind, RpcId, Side, SpecTable, TagSet, Ticks, TifTag, VenueCaps, VenueCommand,
    VenueOrderId, WallNs, dispatch,
};

use super::{Failure, Subject};

/// The request id every probed command is encoded as.
pub(crate) const RPC: RpcId = RpcId(1);
/// The account and namespace the suite mints its client ids under, in a directory of its own.
const ACCOUNT: AccountKey = AccountKey::new(1);
const NAMESPACE: Namespace = Namespace::new(1);
/// The fixed encode time.
const WALL: WallNs = WallNs(1_759_363_200_000_000_000);
/// The first placement nonce an order carries; item `i`'s is this plus `i`.
const PLACEMENT_NONCE: u64 = 1_000;
/// A limit price; codecs encode it without judging it.
const PX: Ticks = Ticks(100);

/// What an encode gave.
#[derive(Debug)]
pub(crate) struct Encoded {
    pub result: Result<EncodeReceipt, NotSentReason>,
    pub fx: Effects,
}

/// The venue under the assumed setup.
pub(crate) struct Harness<'s> {
    check: &'static str,
    subject: &'s Subject<'s>,
    pub caps: VenueCaps,
    specs: SpecTable,
    pub inst: InstrumentId,
    qty: Lots,
}

impl<'s> Harness<'s> {
    /// The venue's caps and its first instrument under the assumed setup.
    pub fn new(check: &'static str, subject: &'s Subject<'s>) -> Result<Harness<'s>, Failure> {
        let setup = subject.setup();
        let caps = subject.factory().caps(&setup.cfg).map_err(|e| {
            Failure::one(
                check,
                "VenueFactory::caps",
                format!("refused the setup: {e}"),
            )
        })?;
        let Some(spec) = setup.specs.iter().next() else {
            return Err(Failure::one(
                check,
                "setup",
                "the spec table lists no instrument",
            ));
        };
        // At least one lot, so an amend always leaves something to rest.
        let qty = Lots::new(spec.min_size.get().max(1)).expect("a positive count");
        let inst = spec.id;
        Ok(Harness {
            check,
            subject,
            caps,
            specs: setup.specs,
            inst,
            qty,
        })
    }

    /// A failure of this check with the one breach of `capability`.
    pub fn fail(&self, capability: &str, what: impl Into<String>) -> Failure {
        Failure::one(self.check, capability, what)
    }

    /// A codec built fresh under the setup: `None` when the factory builds none.
    pub fn codec(&self) -> Result<Option<Box<dyn ExecCodec>>, Failure> {
        let setup = self.subject.setup();
        match self.subject.factory().exec_codec(&setup.cfg, setup.creds) {
            None => Ok(None),
            Some(Ok(codec)) => Ok(Some(codec)),
            Some(Err(e)) => Err(self.fail(
                "VenueFactory::exec_codec",
                format!("refused the setup: {e}"),
            )),
        }
    }

    /// A codec built fresh, for a venue whose caps declare order entry.
    pub fn exec_codec(&self) -> Result<Box<dyn ExecCodec>, Failure> {
        self.codec()?.ok_or_else(|| {
            self.fail(
                "VenueCaps.exec",
                "declares order entry, yet exec_codec builds no codec",
            )
        })
    }

    /// `cmd` encoded by `codec` as request `rpc` at the fixed time, with one nonce per item, and
    /// the effects it asked for.
    pub fn encode(&self, codec: &mut dyn ExecCodec, cmd: &VenueCommand, rpc: RpcId) -> Encoded {
        let items = u64::from(cmd.items().unwrap_or(u16::MAX));
        let nonces = NonceBlock::new((1..=items).collect());
        let ctx = EncodeCtx {
            wall: WALL,
            mono: MonoNs(1),
            nonces,
        };
        let mut fx = Effects::new();
        let stamps = &mut PathStamps::off();
        let result = codec.encode(cmd, rpc, &self.specs, &ctx, stamps, &mut fx);
        Encoded { result, fx }
    }

    /// Our first `n` client ids.
    pub fn cids(&self, n: usize) -> Result<Vec<ClientOrderId>, Failure> {
        mint(n).map_err(|why| self.fail("setup", why))
    }

    /// The venue's order ids `conformance-0` to `conformance-<n-1>`.
    pub fn vids(&self, n: usize) -> Vec<VenueOrderId> {
        dispatch(&self.caps, NAMESPACE, |scope| {
            (0..n)
                .map(|i| scope.venue_order_id(&format!("conformance-{i}")))
                .collect::<Result<_, _>>()
        })
        .expect("a short non-empty venue id")
    }
}

/// The parts of an order a venue's caps refuse or allow: its kind, time in force, channel and
/// flags.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct Shape {
    pub kind: OrderKindTag,
    pub tif: TifTag,
    pub channel: Channel,
    pub post_only: bool,
    pub reduce_only: bool,
}

impl Shape {
    /// The plainest order the caps declare: a limit order where they offer one, their first
    /// time in force and channel, no flag.
    pub fn plain(o: &OrderCaps) -> Shape {
        let kind = if o.kinds.contains(OrderKindTag::Limit) {
            OrderKindTag::Limit
        } else {
            o.kinds.iter().next().unwrap_or(OrderKindTag::Limit)
        };
        Shape {
            kind,
            tif: o.tifs.iter().next().unwrap_or(TifTag::Gtc),
            channel: o.channels.iter().next().unwrap_or(Channel::Public),
            post_only: false,
            reduce_only: false,
        }
    }

    /// Whether an order of this shape has `feature`.
    pub fn has(self, feature: Feature) -> bool {
        let features = [
            (Feature::PostOnly, self.post_only),
            (Feature::ReduceOnly, self.reduce_only),
            (Feature::Ioc, self.tif == TifTag::Ioc),
            (Feature::Fok, self.tif == TifTag::Fok),
            (Feature::Rpi, self.channel == Channel::Rpi),
        ];
        features.contains(&(feature, true))
    }

    /// This shape with `feature` too.
    pub fn with(self, feature: Feature) -> Shape {
        match feature {
            Feature::PostOnly => Shape {
                post_only: true,
                ..self
            },
            Feature::ReduceOnly => Shape {
                reduce_only: true,
                ..self
            },
            Feature::Ioc => Shape {
                tif: TifTag::Ioc,
                ..self
            },
            Feature::Fok => Shape {
                tif: TifTag::Fok,
                ..self
            },
            Feature::Rpi => Shape {
                channel: Channel::Rpi,
                ..self
            },
        }
    }

    /// Why the caps refuse an order of this shape: `Unsupported` for a kind, time in force,
    /// channel or flag they do not declare, then `FlagConflict` for a pair of features they
    /// declare in conflict; `None` when they allow it.
    pub fn refusal(self, o: &OrderCaps) -> Option<NotSentReason> {
        let offered = o.kinds.contains(self.kind)
            && o.tifs.contains(self.tif)
            && o.channels.contains(self.channel)
            && (!self.post_only || o.post_only)
            && (!self.reduce_only || o.reduce_only);
        if !offered {
            return Some(NotSentReason::Unsupported);
        }
        let conflict = o
            .flag_conflicts
            .iter()
            .any(|&(a, b)| self.has(a) && self.has(b));
        conflict.then_some(NotSentReason::FlagConflict)
    }
}

/// The ids the checks build commands from: item `i` of a batch is order `i`.
pub(crate) struct Ids {
    pub cids: Vec<ClientOrderId>,
    pub vids: Vec<VenueOrderId>,
}

impl Ids {
    /// Ids for `n` orders.
    pub fn new(h: &Harness<'_>, n: usize) -> Result<Ids, Failure> {
        Ok(Ids {
            cids: h.cids(n)?,
            vids: h.vids(n),
        })
    }

    /// Order `i` placed with `shape` on the harness's instrument.
    pub fn order(&self, h: &Harness<'_>, i: usize, shape: Shape) -> NewOrder {
        let kind = match shape.kind {
            OrderKindTag::Limit => OrderKind::Limit { px: PX },
            OrderKindTag::Market => OrderKind::Market,
        };
        NewOrder {
            cid: self.cids[i],
            inst: h.inst,
            side: Side::Buy,
            qty: h.qty,
            kind,
            tif: shape.tif,
            channel: shape.channel,
            post_only: shape.post_only,
            reduce_only: shape.reduce_only,
            reducing: false,
        }
    }

    /// An amend of order `i`, to `shape` as a limit order (an amend targets a resting limit
    /// order), naming it by `target`.
    pub fn amend(&self, h: &Harness<'_>, i: usize, shape: Shape, target: OrderRef) -> AmendOrder {
        let kind = OrderKindTag::Limit;
        let order = self.order(h, i, Shape { kind, ..shape });
        AmendOrder {
            target,
            inst: order.inst,
            side: order.side,
            tif: order.tif,
            channel: order.channel,
            post_only: order.post_only,
            reduce_only: order.reduce_only,
            reducing: false,
            px: PX,
            qty: order.qty,
            cum_filled: Lots::new(0).expect("zero is a count"),
        }
    }

    /// Order `i`'s references of the kinds in `kinds`, as a target and a placement nonce, or
    /// `None` when `kinds` holds neither our id nor the venue's: every target names one.
    pub fn carrying(&self, i: usize, kinds: &[RefKind]) -> Option<(OrderRef, Option<u64>)> {
        let (cid, vid) = (self.cids[i], self.vids[i].clone());
        let target = match (
            kinds.contains(&RefKind::Client),
            kinds.contains(&RefKind::Venue),
        ) {
            (true, true) => OrderRef::Both(cid, vid),
            (true, false) => OrderRef::Client(cid),
            (false, true) => OrderRef::Venue(vid),
            (false, false) => return None,
        };
        let nonce = kinds
            .contains(&RefKind::PlacementNonce)
            .then(|| placement_nonce(i));
        Some((target, nonce))
    }

    /// A cancel of order `i` carrying `refs`.
    pub fn cancel(&self, h: &Harness<'_>, refs: (OrderRef, Option<u64>)) -> CancelOrder {
        let (target, placement_nonce) = refs;
        CancelOrder {
            target,
            inst: h.inst,
            side: Side::Buy,
            placement_nonce,
        }
    }

    /// A query for an order carrying `refs`.
    pub fn query(&self, h: &Harness<'_>, refs: (OrderRef, Option<u64>)) -> QueryOrder {
        let (target, placement_nonce) = refs;
        QueryOrder {
            target,
            inst: h.inst,
            placement_nonce,
        }
    }
}

/// The nonce order `i` was placed with.
pub(crate) fn placement_nonce(i: usize) -> u64 {
    PLACEMENT_NONCE + i as u64
}

/// The reference kinds of `all` not in `declared`.
pub(crate) fn undeclared(declared: TagSet<RefKind>, all: &[RefKind]) -> Vec<RefKind> {
    all.iter()
        .copied()
        .filter(|&k| !declared.contains(k))
        .collect()
}

/// `n` client ids minted under the suite's own lease, in a directory made for them and removed
/// after.
fn mint(n: usize) -> Result<Vec<ClientOrderId>, String> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let k = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("fbc-conformance-{}-{k}", std::process::id()));
    let cids = mint_in(&dir, n);
    let _ = fs::remove_dir_all(&dir);
    cids
}

/// `n` client ids minted under a lease taken in `dir`, created if missing.
fn mint_in(dir: &Path, n: usize) -> Result<Vec<ClientOrderId>, String> {
    let shown = dir.display();
    fs::create_dir_all(dir).map_err(|e| format!("cannot create {shown}: {e}"))?;
    let lease = NamespaceLease::acquire(dir, ACCOUNT, NAMESPACE);
    let lease = lease.map_err(|e| format!("cannot mint client ids in {shown}: {e}"))?;
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    (0..n)
        .map(|_| mint.mint().map_err(|e| format!("cannot mint: {e:?}")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minting_reports_a_directory_it_cannot_create() {
        let file =
            std::env::temp_dir().join(format!("fbc-conformance-file-{}", std::process::id()));
        fs::write(&file, b"not a directory").unwrap();
        let err = mint_in(&file.join("under"), 1).unwrap_err();
        let _ = fs::remove_file(&file);
        assert!(err.starts_with("cannot create"), "{err}");
    }

    #[test]
    fn minting_reports_a_lease_held_elsewhere() {
        let dir = std::env::temp_dir().join(format!("fbc-conformance-held-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let held = NamespaceLease::acquire(&dir, ACCOUNT, NAMESPACE).unwrap();
        let err = mint_in(&dir, 1).unwrap_err();
        drop(held);
        let _ = fs::remove_dir_all(&dir);
        assert!(err.starts_with("cannot mint client ids"), "{err}");
    }
}
