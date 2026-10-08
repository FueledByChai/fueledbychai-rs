//! What the conformance toy's fixtures (`fixtures/conformance-toy`) assume, shared by the suite's
//! tests on the toy: its two instruments, no configuration, no credentials, and the golden
//! commands whose encodings `signing_golden/` holds.

use std::sync::OnceLock;

use fbc_conformance::suite::{Golden, Setup};
use fbc_conformance::toy::{self, INST_A, INST_B, OWN_NS};
use fbc_core::{
    AccountKey, AmendOrder, CancelOrder, Channel, CidMint, ClientOrderId, EncodeCtx, Lots, MonoNs,
    NamespaceLease, NewOrder, NonceBlock, OrderKind, OrderRef, RpcId, Secrets, Side, Ticks, TifTag,
    VenueCommand, VenueConfig, VenueOrderId, WallNs,
};

/// The toy's fixture directory.
pub const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/conformance-toy"
);

/// The wall time the goldens are signed at.
const WALL: i64 = 1_759_363_200_000_000_000;

/// What the toy's fixtures assume.
pub fn assumed() -> Setup {
    Setup {
        specs: toy::specs(),
        cfg: VenueConfig::new(),
        creds: Secrets::new(),
        goldens: goldens(),
    }
}

/// One golden command for each kind of request the toy signs: a placement, a batch
/// placement, an amend, a cancel and a batch cancel.
pub fn goldens() -> Vec<Golden> {
    vec![
        golden("place", 21, &[500], VenueCommand::Place(order(0, INST_A))),
        golden(
            "place-batch",
            22,
            &[501, 502],
            VenueCommand::PlaceBatch(vec![order(1, INST_A), order(2, INST_B)]),
        ),
        golden(
            "amend",
            23,
            &[503],
            VenueCommand::Amend(amend(OrderRef::Venue(vid("toy-7001")))),
        ),
        golden(
            "cancel",
            24,
            &[504],
            VenueCommand::Cancel(cancel(OrderRef::Client(cid(0)), None)),
        ),
        golden(
            "cancel-batch",
            25,
            &[505, 506],
            VenueCommand::CancelMany(vec![
                cancel(OrderRef::Venue(vid("toy-7002")), None),
                cancel(OrderRef::Client(cid(2)), Some(502)),
            ]),
        ),
    ]
}

fn golden(name: &'static str, rpc: u64, nonces: &[u64], cmd: VenueCommand) -> Golden {
    Golden {
        name,
        rpc: RpcId(rpc),
        ctx: EncodeCtx {
            wall: WallNs(WALL),
            mono: MonoNs(1),
            nonces: NonceBlock::new(nonces.to_vec()),
        },
        cmd,
    }
}

/// A post-only buy of 25 lots at 130 865 ticks (13 086.5 on the toy's grid of 0.5).
fn order(n: usize, inst: fbc_core::InstrumentId) -> NewOrder {
    NewOrder {
        cid: cid(n),
        inst,
        side: Side::Buy,
        qty: Lots::new(25).unwrap(),
        kind: OrderKind::Limit { px: Ticks(130_865) },
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// An amend to 10 lots at 130 860 ticks of an order 4 lots of which have filled.
fn amend(target: OrderRef) -> AmendOrder {
    AmendOrder {
        target,
        inst: INST_A,
        side: Side::Buy,
        tif: TifTag::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        px: Ticks(130_860),
        qty: Lots::new(10).unwrap(),
        cum_filled: Lots::new(4).unwrap(),
    }
}

fn cancel(target: OrderRef, placement_nonce: Option<u64>) -> CancelOrder {
    CancelOrder {
        target,
        inst: INST_A,
        side: Side::Buy,
        placement_nonce,
    }
}

fn vid(wire: &str) -> VenueOrderId {
    toy::with_scope(|scope| scope.venue_order_id(wire)).unwrap()
}

/// Our `n`th client id, minted once per test binary under a lease in a fresh directory at a
/// fixed time, so every run mints the same ids.
fn cid(n: usize) -> ClientOrderId {
    static CIDS: OnceLock<Vec<ClientOrderId>> = OnceLock::new();
    let cids = CIDS.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("fbc-conformance-goldens-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let lease = NamespaceLease::acquire(&dir, AccountKey::new(1), OWN_NS).unwrap();
        let mut mint = CidMint::new(lease, 0, 0, WallNs(WALL));
        let cids = (0..3).map(|_| mint.mint().unwrap()).collect();
        drop(mint);
        let _ = std::fs::remove_dir_all(&dir);
        cids
    });
    cids[n]
}
