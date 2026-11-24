//! Which orders of ours the cancels of ours the link submitted cover (`fbc_live::link::Coverage`,
//! FBC-aoh5): moved from `testnet_quote`'s test with the link (decision 0155 item 9).

use std::cell::RefCell;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::{AccountKey, CidMint, ExchNs, Namespace, NamespaceLease, RpcId, Side, WallNs};
use fbc_live::link;

/// A directory removed when it is dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

thread_local! {
    /// The lease directories the test on this thread made, removed when its thread ends.
    static MADE: RefCell<Vec<Removed>> = const { RefCell::new(Vec::new()) };
}

/// A lease directory of this test's own.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-live-coverage-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    MADE.with(|made| made.borrow_mut().push(Removed(dir.clone())));
    dir
}

#[test]
fn a_cancel_of_ours_covers_its_orders_only_once_the_session_reports_it_sent() {
    // Codex r4308625182 / RB-aoh5-3 on PR #159: the session decodes a frame waiting in its turn
    // before it writes what was queued, so a cancel of ours submitted but not yet reported sent
    // covers nothing (another program's cancel read then is not ours); reported sent, it covers
    // its orders; reported not sent (Codex r4308561813), it never does. One another request of
    // ours still covers stays covered.
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let mut cid = || mint.mint().unwrap();
    let (a, b, c) = (cid(), cid(), cid());
    let mut covered = link::Coverage::default();
    covered.submitted(RpcId(1), [a]);
    covered.submitted(RpcId(2), [a, b]);
    assert!(!covered.covers(a) && !covered.covers(b), "pending: not yet");
    covered.reported(RpcId(1), true);
    assert!(covered.covers(a));
    assert!(!covered.covers(b));
    // Request 2 never written: b never covered, a still is (request 1).
    covered.reported(RpcId(2), false);
    assert!(covered.covers(a));
    assert!(!covered.covers(b));
    // A report of no request pending changes nothing.
    covered.reported(RpcId(2), true);
    assert!(!covered.covers(b));
    // Forgotten once ended, pending or covered.
    covered.submitted(RpcId(3), [b, c]);
    covered.reported(RpcId(3), true);
    covered.submitted(RpcId(4), [a, c]);
    covered.retain(|x| *x == b);
    assert!(!covered.covers(a) && covered.covers(b) && !covered.covers(c));
    covered.reported(RpcId(4), true);
    assert!(
        !covered.covers(a) && !covered.covers(c),
        "ended while pending"
    );

    // Codex r4309154204 / RB-aoh5-4: a cancel the venue refused (for its rate, say) no longer
    // covers the order it named, while another request of ours still does; a whole request
    // refused covers nothing it covered; a refusal while pending leaves nothing to cover.
    let mut covered = link::Coverage::default();
    covered.submitted(RpcId(5), [a, b]);
    covered.submitted(RpcId(6), [b]);
    covered.reported(RpcId(5), true);
    covered.reported(RpcId(6), true);
    covered.refused(RpcId(5), Some(a));
    assert!(!covered.covers(a), "refused for a");
    covered.refused(RpcId(5), Some(b));
    assert!(covered.covers(b), "request 6 still covers b");
    covered.refused(RpcId(6), None);
    assert!(!covered.covers(b));
    covered.submitted(RpcId(7), [a, c]);
    covered.submitted(RpcId(8), [c]);
    covered.reported(RpcId(8), true);
    covered.refused(RpcId(7), Some(a));
    covered.reported(RpcId(7), true);
    assert!(!covered.covers(a) && covered.covers(c));
    covered.refused(RpcId(7), None);
    assert!(covered.covers(c), "request 8 still covers c");

    // Codex r4310042612: an order a user cancel ended while only unanswered requests of ours
    // covered it is held. Every one refused: released, a cancel not ours; one accepted: ours.
    let (buy, sell) = (Side::Buy, Side::Sell);
    let mut covered = link::Coverage::default();
    covered.submitted(RpcId(9), [a]);
    covered.submitted(RpcId(10), [a, b]);
    covered.reported(RpcId(9), true);
    covered.reported(RpcId(10), true);
    assert!(covered.hold(a, buy, Some(ExchNs(7))));
    covered.retain(|_| false);
    assert!(
        covered.refused(RpcId(9), Some(a)).is_empty(),
        "10 still covers a"
    );
    assert_eq!(
        covered.refused(RpcId(10), None),
        vec![link::Released {
            cid: a,
            side: buy,
            at: Some(ExchNs(7))
        }]
    );
    assert!(covered.refused(RpcId(10), None).is_empty(), "released once");
    covered.submitted(RpcId(11), [b]);
    covered.reported(RpcId(11), true);
    assert!(covered.hold(b, sell, None));
    covered.accepted(RpcId(11), Some(b));
    assert!(
        covered.refused(RpcId(11), Some(b)).is_empty(),
        "accepted: ours"
    );
    // Accepted before the order's end: ours, nothing held.
    covered.submitted(RpcId(12), [c]);
    covered.reported(RpcId(12), true);
    covered.accepted(RpcId(12), None);
    assert!(!covered.hold(c, buy, None));
    assert!(!covered.hold(a, buy, None), "covered by nothing");
}
