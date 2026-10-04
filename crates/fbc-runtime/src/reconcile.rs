//! The subscription reconciler of one stream (decision 0002): the difference between the
//! subscriptions the consumer wants and the ones the current connection epoch has sent, and
//! nothing else.
//!
//! The Java stack resubscribed on every reconnect without asking what the connection already
//! had, so duplicate subscriptions piled up and so did the work behind them. Here the consumer
//! states the whole set it wants ([`Reconciler::set_desired`]), and the reconciler yields a
//! [`SubscribeCall`] whose `add` and `remove` are the difference from what the current epoch
//! has subscribed. The runtime passes them to the codec's `subscribe` and settles the call:
//!
//! - [`Reconciler::sent`] when the codec took it: the call's subscriptions are active in this
//!   epoch and are never sent again in it.
//! - [`Reconciler::refused`] when the codec refused it (it then sends nothing): its
//!   subscriptions stay pending, never dropped, and go again on the next
//!   [`Reconciler::set_desired`], [`Reconciler::retry`] or epoch.
//!
//! One call is outstanding at a time; a change made meanwhile goes in the call that follows it,
//! whether the outstanding call was sent or refused. A call settles only on the reconciler that
//! made it, and only in the epoch it was made for.
//! A new epoch ([`Reconciler::begin_epoch`]) starts with nothing subscribed, and once it opens
//! ([`Reconciler::opened`]) it subscribes the desired set exactly once. Until then every
//! desired subscription waits, pending. Every call yielded must be settled: an unsettled call
//! holds back the rest of its epoch.
//!
//! This is logic only: no socket, no codec, no task.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::Subscription;

/// The next reconciler's origin: every reconciler in the process has its own, so a call made
/// by one is never taken for another's.
static NEXT_ORIGIN: AtomicU64 = AtomicU64::new(0);

/// The subscriptions to add and to remove on the current epoch's connection: the difference
/// between the desired set and the active one. Settle it with [`Reconciler::sent`] or
/// [`Reconciler::refused`].
#[derive(Eq, PartialEq, Debug)]
#[must_use = "a subscribe call holds back its epoch until it is settled"]
pub struct SubscribeCall {
    /// The reconciler that made it.
    origin: u64,
    epoch: u32,
    add: Vec<Subscription>,
    remove: Vec<Subscription>,
}

impl SubscribeCall {
    /// The epoch the call was made for.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// Subscriptions to add, in order.
    pub fn add(&self) -> &[Subscription] {
        &self.add
    }

    /// Subscriptions to remove, in order.
    pub fn remove(&self) -> &[Subscription] {
        &self.remove
    }
}

/// A rule of reconciliation that a caller broke. Nothing changes when one is returned.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ReconcileError {
    /// A call made for an older epoch was settled after a new one began: that connection is
    /// gone, so the call is dropped.
    StaleCall { call: u32, current: u32 },
    /// [`Reconciler::begin_epoch`] named an epoch that is not after the current one.
    NotNewer { current: u32, given: u32 },
    /// [`Reconciler::opened`] named an epoch other than the current one.
    NotCurrent { current: u32, given: u32 },
    /// A call made by another reconciler.
    ForeignCall,
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReconcileError::StaleCall { call, current } => write!(
                f,
                "a subscribe call of epoch {call} was settled in epoch {current}; dropped"
            ),
            ReconcileError::NotNewer { current, given } => write!(
                f,
                "epoch {given} cannot begin after epoch {current}; epochs only rise"
            ),
            ReconcileError::NotCurrent { current, given } => {
                write!(
                    f,
                    "epoch {given} opened, but the current epoch is {current}"
                )
            }
            ReconcileError::ForeignCall => {
                f.write_str("a subscribe call made by another reconciler was settled here")
            }
        }
    }
}

impl std::error::Error for ReconcileError {}

/// The subscription state of one stream: what the consumer wants, and what the current epoch
/// has subscribed. It is not `Clone`: a copy would share its origin, and so its calls.
#[derive(Debug)]
pub struct Reconciler {
    origin: u64,
    epoch: u32,
    open: bool,
    desired: BTreeSet<Subscription>,
    active: BTreeSet<Subscription>,
    /// A call of the current epoch is outstanding.
    in_flight: bool,
    /// The desired set changed while the outstanding call was out.
    changed: bool,
    /// A refused call waits for a retry, a change or an epoch.
    held: bool,
}

impl Reconciler {
    /// A stream at `epoch`, not yet open, wanting nothing. Its calls settle only on it.
    pub fn new(epoch: u32) -> Reconciler {
        Reconciler {
            origin: NEXT_ORIGIN.fetch_add(1, Ordering::Relaxed),
            epoch,
            open: false,
            desired: BTreeSet::new(),
            active: BTreeSet::new(),
            in_flight: false,
            changed: false,
            held: false,
        }
    }

    /// The current epoch.
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// What the consumer wants subscribed.
    pub fn desired(&self) -> &BTreeSet<Subscription> {
        &self.desired
    }

    /// What the current epoch has subscribed.
    pub fn active(&self) -> &BTreeSet<Subscription> {
        &self.active
    }

    /// Desired subscriptions the current epoch has not sent yet: waiting, refused or in the
    /// outstanding call.
    pub fn pending(&self) -> Vec<Subscription> {
        self.desired.difference(&self.active).copied().collect()
    }

    /// Want exactly `subs`. Yields the difference from what this epoch has subscribed, when
    /// the epoch is open, there is one, and no call is outstanding. A refused call's
    /// subscriptions go again.
    pub fn set_desired(
        &mut self,
        subs: impl IntoIterator<Item = Subscription>,
    ) -> Option<SubscribeCall> {
        let desired: BTreeSet<_> = subs.into_iter().collect();
        if self.in_flight && desired != self.desired {
            self.changed = true;
        }
        self.desired = desired;
        self.held = false;
        self.next_call()
    }

    /// Send a refused call's subscriptions again, without changing what is desired.
    pub fn retry(&mut self) -> Option<SubscribeCall> {
        self.held = false;
        self.next_call()
    }

    /// The connection closed or reconnects as `epoch`: nothing is subscribed on it yet, the
    /// epoch is not open, and an outstanding call of the old epoch no longer counts.
    pub fn begin_epoch(&mut self, epoch: u32) -> Result<(), ReconcileError> {
        if epoch <= self.epoch {
            return Err(ReconcileError::NotNewer {
                current: self.epoch,
                given: epoch,
            });
        }
        self.epoch = epoch;
        self.open = false;
        self.active.clear();
        self.in_flight = false;
        self.changed = false;
        self.held = false;
        Ok(())
    }

    /// The current epoch's connection is open: yields the desired set, once.
    pub fn opened(&mut self, epoch: u32) -> Result<Option<SubscribeCall>, ReconcileError> {
        if epoch != self.epoch {
            return Err(ReconcileError::NotCurrent {
                current: self.epoch,
                given: epoch,
            });
        }
        self.open = true;
        Ok(self.next_call())
    }

    /// The codec took `call`: its additions are active and its removals are not. Yields the
    /// next difference, if the desired set moved while the call was outstanding.
    pub fn sent(&mut self, call: SubscribeCall) -> Result<Option<SubscribeCall>, ReconcileError> {
        self.settle(&call)?;
        self.active.extend(call.add);
        for sub in &call.remove {
            self.active.remove(sub);
        }
        Ok(self.next_call())
    }

    /// The codec refused `call` and sent nothing: its subscriptions stay pending. When the
    /// desired set changed while it was out, yields the next difference, the refused
    /// subscriptions with it; otherwise they wait for the next [`Reconciler::set_desired`],
    /// [`Reconciler::retry`] or epoch.
    pub fn refused(
        &mut self,
        call: SubscribeCall,
    ) -> Result<Option<SubscribeCall>, ReconcileError> {
        let changed = self.settle(&call)?;
        self.held = !changed;
        Ok(self.next_call())
    }

    /// End the outstanding call, if `call` is it, and say whether the desired set changed
    /// while it was out. A call of this reconciler and of the current epoch is the outstanding
    /// one: an epoch has one call out at a time, a call cannot be copied, and settling it
    /// consumes it.
    fn settle(&mut self, call: &SubscribeCall) -> Result<bool, ReconcileError> {
        if call.origin != self.origin {
            return Err(ReconcileError::ForeignCall);
        }
        if call.epoch != self.epoch {
            return Err(ReconcileError::StaleCall {
                call: call.epoch,
                current: self.epoch,
            });
        }
        self.in_flight = false;
        Ok(std::mem::take(&mut self.changed))
    }

    /// The difference between the desired and the active set, as the outstanding call, when
    /// the epoch is open, no call is outstanding, no refusal waits for a retry, and there is
    /// one.
    fn next_call(&mut self) -> Option<SubscribeCall> {
        if !self.open || self.in_flight || self.held {
            return None;
        }
        let add: Vec<_> = self.desired.difference(&self.active).copied().collect();
        let remove: Vec<_> = self.active.difference(&self.desired).copied().collect();
        if add.is_empty() && remove.is_empty() {
            return None;
        }
        self.in_flight = true;
        Some(SubscribeCall {
            origin: self.origin,
            epoch: self.epoch,
            add,
            remove,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{BookId, Feed, InstrumentId};

    fn sub(inst: u32) -> Subscription {
        Subscription {
            inst: InstrumentId::new(inst),
            feed: Feed::Book(BookId(0)),
        }
    }

    fn set(insts: &[u32]) -> Vec<Subscription> {
        insts.iter().copied().map(sub).collect()
    }

    /// The call's epoch, additions and removals, by instrument number.
    fn shape(call: &SubscribeCall) -> (u32, Vec<u32>, Vec<u32>) {
        let insts = |subs: &[Subscription]| subs.iter().map(|s| s.inst.get()).collect();
        (call.epoch(), insts(call.add()), insts(call.remove()))
    }

    fn at(epoch: u32) -> Reconciler {
        Reconciler::new(epoch)
    }

    /// Everything a reconciler holds, to show that a refused settlement changed none of it.
    fn state(rec: &Reconciler) -> String {
        format!("{rec:?}")
    }

    fn open_at(epoch: u32) -> Reconciler {
        let mut rec = at(epoch);
        assert_eq!(rec.opened(epoch), Ok(None));
        rec
    }

    /// Set `insts` desired and send the call it yields, which must have this shape.
    fn send(rec: &mut Reconciler, insts: &[u32], add: &[u32], remove: &[u32]) {
        let call = rec.set_desired(set(insts)).expect("a call");
        assert_eq!(shape(&call), (rec.epoch(), add.to_vec(), remove.to_vec()));
        assert_eq!(rec.sent(call), Ok(None));
    }

    #[test]
    fn within_an_epoch_only_the_difference_is_yielded() {
        let mut rec = open_at(0);
        send(&mut rec, &[1, 2], &[1, 2], &[]);
        send(&mut rec, &[1, 2, 3], &[3], &[]);
        send(&mut rec, &[1, 3], &[], &[2]);
        send(&mut rec, &[4, 1], &[4], &[3]);
        assert_eq!(rec.active(), &set(&[1, 4]).into_iter().collect());
        assert!(rec.pending().is_empty());
    }

    #[test]
    fn an_active_subscription_is_never_sent_twice_in_an_epoch() {
        let mut rec = open_at(0);
        send(&mut rec, &[1, 2], &[1, 2], &[]);
        assert_eq!(rec.set_desired(set(&[2, 1])), None);
        assert_eq!(rec.set_desired(set(&[1, 1, 2])), None);
        assert_eq!(rec.retry(), None);
        assert_eq!(rec.opened(0), Ok(None));
        // Removed and then re-added: each change is a difference once.
        send(&mut rec, &[2], &[], &[1]);
        assert_eq!(rec.set_desired(set(&[2])), None);
        send(&mut rec, &[1, 2], &[1], &[]);
        assert_eq!(rec.set_desired(set(&[1, 2])), None);
    }

    #[test]
    fn each_new_epoch_subscribes_the_desired_set_exactly_once() {
        let mut rec = open_at(0);
        send(&mut rec, &[1, 2], &[1, 2], &[]);
        for epoch in 1..=3 {
            assert_eq!(rec.begin_epoch(epoch), Ok(()));
            assert_eq!(rec.epoch(), epoch);
            assert!(rec.active().is_empty());
            assert_eq!(rec.pending(), set(&[1, 2]));
            let call = rec.opened(epoch).unwrap().expect("the desired set");
            assert_eq!(shape(&call), (epoch, vec![1, 2], vec![]));
            assert_eq!(rec.sent(call), Ok(None));
            assert_eq!(rec.opened(epoch), Ok(None));
            assert_eq!(rec.set_desired(set(&[1, 2])), None);
            assert_eq!(rec.retry(), None);
        }
    }

    #[test]
    fn a_new_epoch_sends_the_desired_set_with_no_removals_of_the_old_one() {
        let mut rec = open_at(0);
        send(&mut rec, &[1, 2], &[1, 2], &[]);
        rec.begin_epoch(1).unwrap();
        // Changed while the new epoch waits to open: only the set desired now goes.
        assert_eq!(rec.set_desired(set(&[2, 3])), None);
        let call = rec.opened(1).unwrap().expect("the desired set");
        assert_eq!(shape(&call), (1, vec![2, 3], vec![]));
        assert_eq!(rec.sent(call), Ok(None));
    }

    #[test]
    fn subscriptions_wait_pending_until_the_epoch_opens() {
        let mut rec = at(5);
        assert_eq!(rec.set_desired(set(&[7, 8])), None);
        assert_eq!(rec.retry(), None);
        assert_eq!(rec.pending(), set(&[7, 8]));
        assert_eq!(rec.desired(), &set(&[7, 8]).into_iter().collect());
        let call = rec.opened(5).unwrap().expect("the waiting set");
        assert_eq!(shape(&call), (5, vec![7, 8], vec![]));
        assert_eq!(rec.pending(), set(&[7, 8]), "in flight is still pending");
        assert_eq!(rec.sent(call), Ok(None));
        assert!(rec.pending().is_empty());
    }

    #[test]
    fn a_refused_subscription_stays_pending_until_it_is_sent() {
        let mut rec = open_at(0);
        let call = rec.set_desired(set(&[1, 2])).unwrap();
        assert_eq!(rec.refused(call), Ok(None));
        assert!(rec.active().is_empty());
        assert_eq!(rec.pending(), set(&[1, 2]));
        assert_eq!(
            rec.opened(0),
            Ok(None),
            "a refusal is not retried by itself"
        );

        // A new desired set sends the refused subscriptions again with the change.
        let call = rec.set_desired(set(&[1, 2, 3])).unwrap();
        assert_eq!(shape(&call), (0, vec![1, 2, 3], vec![]));
        assert_eq!(rec.refused(call), Ok(None));

        // So does a retry, with nothing changed.
        let call = rec.retry().expect("the refused set");
        assert_eq!(shape(&call), (0, vec![1, 2, 3], vec![]));
        assert_eq!(rec.refused(call), Ok(None));

        // And so does the next epoch once it opens.
        rec.begin_epoch(1).unwrap();
        let call = rec.opened(1).unwrap().expect("the refused set");
        assert_eq!(shape(&call), (1, vec![1, 2, 3], vec![]));
        assert_eq!(rec.sent(call), Ok(None));
        assert!(rec.pending().is_empty());
        assert_eq!(rec.retry(), None);
    }

    #[test]
    fn a_refused_removal_stays_active_until_it_is_sent() {
        let mut rec = open_at(0);
        send(&mut rec, &[1, 2], &[1, 2], &[]);
        let call = rec.set_desired(set(&[1])).unwrap();
        assert_eq!(shape(&call), (0, vec![], vec![2]));
        assert_eq!(rec.refused(call), Ok(None));
        assert_eq!(rec.active(), &set(&[1, 2]).into_iter().collect());
        let call = rec.retry().expect("the refused removal");
        assert_eq!(shape(&call), (0, vec![], vec![2]));
        assert_eq!(rec.sent(call), Ok(None));
        assert_eq!(rec.active(), &set(&[1]).into_iter().collect());
    }

    #[test]
    fn a_change_made_while_a_call_is_outstanding_follows_it() {
        let mut rec = open_at(0);
        let first = rec.set_desired(set(&[1])).unwrap();
        assert_eq!(rec.set_desired(set(&[1, 2])), None);
        assert_eq!(rec.retry(), None);
        assert_eq!(rec.opened(0), Ok(None));
        let next = rec.sent(first).unwrap().expect("the change");
        assert_eq!(shape(&next), (0, vec![2], vec![]));
        assert_eq!(rec.sent(next), Ok(None));
    }

    #[test]
    fn a_call_of_an_older_epoch_is_dropped_and_changes_nothing() {
        let mut rec = open_at(0);
        let old = rec.set_desired(set(&[1])).unwrap();
        rec.begin_epoch(1).unwrap();
        let before = state(&rec);
        assert_eq!(
            rec.sent(old),
            Err(ReconcileError::StaleCall {
                call: 0,
                current: 1
            })
        );
        assert_eq!(state(&rec), before);
        let call = rec.opened(1).unwrap().expect("the desired set");
        assert_eq!(shape(&call), (1, vec![1], vec![]));

        // A refusal from the old epoch is dropped too, even while a new call is outstanding.
        let mut other = open_at(0);
        let old = other.set_desired(set(&[2])).unwrap();
        other.begin_epoch(1).unwrap();
        let current = other.opened(1).unwrap().unwrap();
        let err = other.refused(old).unwrap_err();
        assert_eq!(
            err.to_string(),
            "a subscribe call of epoch 0 was settled in epoch 1; dropped"
        );
        assert_eq!(other.sent(current), Ok(None));
        assert_eq!(other.active(), &set(&[2]).into_iter().collect());
    }

    #[test]
    fn epochs_only_rise_and_only_the_current_one_opens() {
        let mut rec = at(2);
        let err = rec.begin_epoch(2).unwrap_err();
        assert_eq!(
            err,
            ReconcileError::NotNewer {
                current: 2,
                given: 2
            }
        );
        assert_eq!(
            err.to_string(),
            "epoch 2 cannot begin after epoch 2; epochs only rise"
        );
        assert_eq!(
            rec.begin_epoch(1),
            Err(ReconcileError::NotNewer {
                current: 2,
                given: 1
            })
        );
        assert_eq!(rec.set_desired(set(&[1])), None, "not open");
        for given in [1, 3] {
            let err = rec.opened(given).unwrap_err();
            assert_eq!(err, ReconcileError::NotCurrent { current: 2, given });
        }
        assert_eq!(
            ReconcileError::NotCurrent {
                current: 2,
                given: 3
            }
            .to_string(),
            "epoch 3 opened, but the current epoch is 2"
        );
        assert_eq!(rec.pending(), set(&[1]), "a refused open leaves it waiting");
        assert!(rec.opened(2).unwrap().is_some());
    }

    #[test]
    fn a_change_made_while_a_refused_call_was_outstanding_follows_the_refusal() {
        let mut rec = open_at(0);
        let first = rec.set_desired(set(&[1])).unwrap();
        assert_eq!(rec.set_desired(set(&[1, 2])), None);
        let next = rec
            .refused(first)
            .unwrap()
            .expect("the change, with the refused one");
        assert_eq!(shape(&next), (0, vec![1, 2], vec![]));
        // Refused again with no change meanwhile: held until a retry, a change or an epoch.
        assert_eq!(rec.refused(next), Ok(None));
        assert_eq!(rec.opened(0), Ok(None));
        let again = rec.retry().expect("the refused set");
        assert_eq!(shape(&again), (0, vec![1, 2], vec![]));
        assert_eq!(rec.sent(again), Ok(None));
    }

    #[test]
    fn a_call_settles_only_on_the_reconciler_that_made_it() {
        // Two reconcilers at the same epoch, each with its first call out: the calls agree on
        // everything but the reconciler that made them.
        let mut a = open_at(0);
        let mut b = open_at(0);
        let from_a = a.set_desired(set(&[1])).unwrap();
        let from_b = b.set_desired(set(&[2])).unwrap();
        let (a_before, b_before) = (state(&a), state(&b));
        let err = b.sent(from_a).unwrap_err();
        assert_eq!(err, ReconcileError::ForeignCall);
        assert_eq!(
            err.to_string(),
            "a subscribe call made by another reconciler was settled here"
        );
        assert_eq!(state(&b), b_before);
        // b still settles its own call, and its refusal is refused on a.
        assert_eq!(b.sent(from_b), Ok(None));
        assert_eq!(b.active(), &set(&[2]).into_iter().collect());
        let from_b = b.set_desired(set(&[])).unwrap();
        assert_eq!(a.refused(from_b), Err(ReconcileError::ForeignCall));
        assert_eq!(state(&a), a_before);
    }
}
