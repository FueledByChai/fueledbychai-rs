//! The audit sink (decision 0005, I8, library form): every raw order event the registry
//! receives is handed to the consumer's sink in receive order, before it is applied, even if
//! it is then ignored, and the verdict follows it. Writing the sink to the consumer's
//! `order_audit` table, that table's schema, the legacy status names and backfilling gaps from
//! the journal stay with the consumer (decision 0001).

use std::fmt;

use fbc_core::OrderUpdate;

use crate::record::OrderKey;
use crate::registry::Routed;

/// One entry the registry hands its [`AuditSink`].
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum AuditEntry<'a> {
    /// A raw order event as the registry received it, before any of it is applied: the
    /// `seq`-th event [`Registry::apply_update`](crate::Registry::apply_update) received
    /// (from 0, counting the events received before a sink was set), with the key it was
    /// applied under.
    Received {
        seq: u64,
        update: &'a OrderUpdate,
        key: OrderKey,
    },
    /// What the registry did with the event `seq`: where it routed it and, for our order,
    /// what the order did with it ([`Applied`](crate::Applied)), an ignored late, stale or
    /// superseded one included. It follows that event's `Received` before the next event is
    /// received. For our order it carries the consumer's tag of the last place or amend the
    /// execution planner built for it and the removal reason of the last cancel it built for
    /// it ([`OrderRecord::tag`](crate::OrderRecord::tag),
    /// [`OrderRecord::removal`](crate::OrderRecord::removal); decision 0098): both
    /// `None` for an event routed to no order of ours, and for an order the planner did not
    /// place. fbc-oms gives them no meaning (decision 0001).
    Verdict {
        seq: u64,
        routed: &'a Routed,
        tag: Option<u16>,
        removal: Option<u16>,
    },
}

/// The consumer's audit sink: it is handed every [`AuditEntry`] in the order the registry
/// produces them. It cannot refuse an event: one it cannot keep is the consumer's to backfill
/// from the journal (decision 0005, I8). It runs on the registry's thread, between an event's
/// receipt and its application, so it should only hand the entry on (to a queue, say). It is
/// `Send` and `Sync` so the registry holding it stays both.
pub trait AuditSink: Send + Sync {
    /// Takes one entry; it borrows the event and verdict, so a sink keeps what it needs.
    fn record(&mut self, entry: AuditEntry<'_>);
}

/// The registry's audit trail: the consumer's sink, when it set one, and how many raw order
/// events the registry received.
#[derive(Default)]
pub(crate) struct Audit {
    sink: Option<Box<dyn AuditSink>>,
    received: u64,
}

impl fmt::Debug for Audit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Audit")
            .field("sink", &self.sink.as_ref().map(|_| "AuditSink"))
            .field("received", &self.received)
            .finish()
    }
}

impl Audit {
    /// Sets the sink, handing back the one it replaces.
    pub(crate) fn set(&mut self, sink: Box<dyn AuditSink>) -> Option<Box<dyn AuditSink>> {
        self.sink.replace(sink)
    }

    /// Records the receipt of `update` under `key`, before any of it is applied, and returns
    /// its sequence number for its verdict.
    pub(crate) fn received(&mut self, update: &OrderUpdate, key: OrderKey) -> u64 {
        let seq = self.received;
        self.received += 1;
        if let Some(sink) = self.sink.as_mut() {
            sink.record(AuditEntry::Received { seq, update, key });
        }
        seq
    }

    /// Records the verdict on the event `seq`, with the consumer's tag and removal reason of
    /// the order it was routed to.
    pub(crate) fn verdict(
        &mut self,
        seq: u64,
        routed: &Routed,
        tag: Option<u16>,
        removal: Option<u16>,
    ) {
        if let Some(sink) = self.sink.as_mut() {
            sink.record(AuditEntry::Verdict {
                seq,
                routed,
                tag,
                removal,
            });
        }
    }
}
