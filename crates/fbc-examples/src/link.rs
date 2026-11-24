//! The re-export shim for the QFEX probe (decision 0155 item 9): the link moved into fbc-live as
//! `fbc_live::link`, and this file, which `qfex_preprod_probe`'s example and test include by
//! path, re-exports it with a [`LinkHandler`] of its own on the `Rc<Notify>` wake the probe's
//! files build, so they compile unchanged. The Paradex samples use `fbc_live::link` itself.
//! FBC-bddu's own commit moves the probe onto the library and deletes this file.

use std::cell::RefCell;
use std::rc::Rc;

use fbc_core::{ConnKey, Envelope, ExecEvent, SubmitHandle, VenueOrderSnapshot};
use fbc_runtime::ExecHandler;
use tokio::sync::Notify;

pub use fbc_live::link::*;

/// The session's handler: hands each call to the [`Link`], as `fbc_live::link::LinkHandler`
/// does, then wakes whoever waits on `wake`.
pub struct LinkHandler {
    pub link: Rc<RefCell<Link>>,
    pub wake: Rc<Notify>,
}

impl ExecHandler for LinkHandler {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        self.link.borrow_mut().on_exec(env);
        self.wake.notify_waiters();
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.link.borrow_mut().on_epoch_end(key);
        self.wake.notify_waiters();
    }

    fn on_submitted(&mut self, handle: SubmitHandle) {
        self.link.borrow_mut().on_submitted(handle);
        self.wake.notify_waiters();
    }

    fn on_unprotected(&mut self, orders: &[VenueOrderSnapshot]) {
        self.link.borrow_mut().on_unprotected(orders);
        self.wake.notify_waiters();
    }

    /// A frame or HTTP answer the codec could not decode: [`Link::on_decode_error`].
    fn on_decode_error(&mut self, key: ConnKey) {
        self.link.borrow_mut().on_decode_error(key);
        self.wake.notify_waiters();
    }
}

/// The shim's handler hands each of its five calls to the link and then wakes a waiter that was
/// waiting before the call (decision 0155 item 9). These run in the QFEX probe's test binary,
/// which includes this file by path.
#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::pin;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use fbc_core::{
        ConnKey, ConnState, Envelope, ExchTsKind, ExecEvent, MonoNs, NotSentReason, RpcId, Stamp,
        StreamId, SubmitHandle, WallNs,
    };
    use fbc_oms::{FillLedger, LadderConfig, LedgerConfig, Registry};
    use fbc_runtime::ExecHandler;
    use tokio::sync::Notify;

    use super::{ENDED_UNSAID, Link, LinkHandler, Note};

    const KEY: ConnKey = ConnKey { conn: 0, epoch: 0 };

    /// A link over a fresh registry under the conformance toy's order caps.
    fn link() -> Link {
        let caps = fbc_conformance::toy::caps().exec.unwrap().order;
        let ledger = FillLedger::new(
            LedgerConfig {
                max_age: Duration::from_secs(3600),
                max_entries: 1_000,
            },
            WallNs(0),
        )
        .unwrap();
        let ladder = LadderConfig::new(
            Duration::from_secs(5),
            Duration::from_secs(1),
            Duration::from_secs(60),
            2,
        )
        .unwrap();
        Link::new(Registry::new(), ledger, caps, ladder)
    }

    /// The connection opening, received at 100 ns.
    fn opened() -> Envelope<ExecEvent> {
        Envelope {
            stamp: Stamp {
                ingest_seq: 0,
                kernel_rx: None,
                recv_mono: MonoNs(100),
                recv_wall: WallNs(100),
                conn: KEY,
            },
            exch_ts: None,
            exch_ts_kind: ExchTsKind::Unknown,
            venue_seq: None,
            body: ExecEvent::Conn {
                stream: StreamId(0),
                state: ConnState::Open,
            },
        }
    }

    /// Runs `call` on the handler with a waiter registered on its wake beforehand, and says
    /// whether the call woke it.
    fn woke(h: &mut LinkHandler, call: impl FnOnce(&mut LinkHandler)) -> bool {
        let wake = Rc::clone(&h.wake);
        let mut waiter = pin!(wake.notified());
        waiter.as_mut().enable();
        call(h);
        let mut cx = Context::from_waker(Waker::noop());
        matches!(waiter.as_mut().poll(&mut cx), Poll::Ready(()))
    }

    #[test]
    fn the_shims_handler_hands_each_call_to_the_link_then_wakes_its_waiters() {
        let link = Rc::new(RefCell::new(link()));
        let mut h = LinkHandler {
            link: Rc::clone(&link),
            wake: Rc::new(Notify::new()),
        };

        assert!(
            woke(&mut h, |h| h.on_exec(opened())),
            "on_exec woke no waiter"
        );
        assert_eq!(link.borrow().notes().len(), 1);
        assert!(matches!(
            &link.borrow().notes()[0],
            Note::Conn(ConnState::Open)
        ));

        assert!(
            woke(&mut h, |h| h.on_epoch_end(KEY)),
            "on_epoch_end woke no waiter"
        );
        assert!(matches!(
            &link.borrow().notes()[1],
            Note::EpochEnd { key, why } if *key == KEY && why == ENDED_UNSAID
        ));
        // The epoch's end reached the registry at its last envelope (FBC-9svb).
        assert_eq!(
            link.borrow().registry().order_entry_epoch_end(),
            Some(MonoNs(100))
        );

        let not_sent = SubmitHandle {
            rpc: RpcId(7),
            receipt: Err(NotSentReason::Disconnected),
        };
        assert!(
            woke(&mut h, |h| h.on_submitted(not_sent)),
            "on_submitted woke no waiter"
        );
        assert!(matches!(
            &link.borrow().notes()[2],
            Note::Submitted {
                rpc: RpcId(7),
                sent: Err(NotSentReason::Disconnected)
            }
        ));

        assert!(
            woke(&mut h, |h| h.on_unprotected(&[])),
            "on_unprotected woke no waiter"
        );
        assert!(matches!(
            &link.borrow().notes()[3],
            Note::Unprotected { told, rpcs, left }
                if told.is_empty() && rpcs.is_empty() && left.is_empty()
        ));

        assert!(
            woke(&mut h, |h| h.on_decode_error(KEY)),
            "on_decode_error woke no waiter"
        );
        assert!(matches!(
            &link.borrow().notes()[4],
            Note::Problem(what) if what.contains("could not decode")
        ));
        assert_eq!(link.borrow().notes().len(), 5);
    }
}
