//! The classifier of cancels not ours (FBC-aoh5, decision 0155 item 2): what each order of ours
//! that a cancel the consumer did not send ended ([`Note::CancelNotOurs`]) is to the
//! consumer's rule, "Warn once, Stop on repeat".
//!
//! A venue may not say who sent a user cancel (the owner's 2026-10-18 mainnet session: another
//! program on the same account cancelled every order on its ticker it did not place, every 3 s,
//! and the quoting loop just requoted). The link notes an order of ours ended by one that no
//! cancel of ours covers; [`CancelsNotOurs`] tells those of one action of the other program's,
//! one event, from the next action's: two are one event when the venue stamped the second within
//! the consumer's span of the event's first ([`NotOursConfig`]) and its order was placed before
//! the event was seen; a requote cancelled again, or a cancel seconds later of any order, however
//! old, is another. A new event within the consumer's window of the window's first is a repeat,
//! as is one of the consumer's requote of a side (the first order it placed on the side after the
//! event before, a place that never rested aside: RB-aoh5-10), or of a replace of it, within the
//! window of that requote's place, however long the cycle that placed it (Codex r4309154208 on PR
//! #159). Past the window the next one starts a new window ([`NotOurs`]).
//!
//! It only classifies. The `WARN`, the Stop and every text a classification leads to are the
//! consumer's, as is what it sends once one was seen ([`CancelsNotOurs::seen`]: another program
//! may be on the account, and an instrument cancel-all would cancel its orders too). One serves
//! one market: its requote rule keeps one requote per side. It reads no clock: each call takes
//! `now` from its caller, on the shard's monotonic clock ([`Link::now`], a stamp, or a consumer
//! input's or timer's `at`; decision 0155 item 3), and the span and the window are the
//! consumer's, each refused zero, with no default (decision 0155 item 8).
//!
//! [`Note::CancelNotOurs`]: crate::link::Note::CancelNotOurs
//! [`Link::now`]: crate::link::Link::now

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use fbc_core::{ClientOrderId, ExchNs, MonoNs, Side};

/// The consumer's two bounds over cancels not ours (decision 0155 item 8): the span within which
/// a cancel the venue stamped after an event's first is still the same event, and the window
/// within which a new event repeats the window's first.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct NotOursConfig {
    span: Duration,
    window: Duration,
}

/// Why a [`NotOursConfig`] was refused.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NotOursConfigError {
    /// A zero span would make no two cancels one event unless the venue stamped them at once.
    ZeroSpan,
    /// A zero window would make no event a repeat, so the rule would never Stop.
    ZeroWindow,
}

impl fmt::Display for NotOursConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            NotOursConfigError::ZeroSpan => "the span of an event of cancels not ours is zero",
            NotOursConfigError::ZeroWindow => "the window over cancels not ours is zero",
        })
    }
}

impl std::error::Error for NotOursConfigError {}

impl NotOursConfig {
    /// A cancel the venue stamped within `span` of an event's first (or, with no venue time on
    /// either, seen within `span` of it) is of the same event when its order was placed before
    /// the event was seen; a new event within `window` of the window's first, or of the place of
    /// the requote it cancelled, is a repeat. Refused when either is zero.
    pub fn new(span: Duration, window: Duration) -> Result<NotOursConfig, NotOursConfigError> {
        if span.is_zero() {
            return Err(NotOursConfigError::ZeroSpan);
        }
        if window.is_zero() {
            return Err(NotOursConfigError::ZeroWindow);
        }
        Ok(NotOursConfig { span, window })
    }
}

/// What a cancel not ours is to the consumer's rule ([`CancelsNotOurs::on_cancel`]).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum NotOurs {
    /// Of an order placed before the event before it was seen, stamped by the venue within the
    /// span of that event's first: the same event (one reconcile, or one cancel-all in the
    /// venue's UI, cancels what rested when it ran: both sides).
    SameEvent,
    /// A new event with no other within the window before it: it starts the window.
    First,
    /// A new event within the window of the window's first, or of the consumer's requote within
    /// the window of its place.
    Repeat,
}

/// The cancels not ours the consumer saw on one market and the consumer's window over them
/// (module documentation): when the window's first event was seen, and the latest event.
///
/// Two are one event only when the second's order was placed before the first was seen and the
/// venue stamped the second within the span of the event's first. The consumer's "a second one
/// on a later cycle" is so judged by the venue's times and the order, not by when an order event
/// happens to be read (between two of the consumer's cycles, or during the next: one reconcile's
/// events read apart stay one event): a cancel not ours of a quote the consumer placed after it
/// saw one is another action, as is one seconds later of an order that rested since before it
/// (Codex r4308625185, RB-aoh5-1 on PR #159). An event the venue stamped no time on is judged by
/// when it was seen instead.
///
/// A new event is a repeat when it comes within the window of the window's first event, or when
/// it cancels the consumer's requote of a side (the first order of ours placed on that side
/// after the latest event was seen, a place refused, not sent or that would cross aside: it
/// never rested, [`Self::unrested`]), or a replace of it, within the window of that requote's
/// place. The second is measured from our place, not from the event before: a consumer requotes
/// a side another program cancelled only on its next cycle, and its reads, settle waits and
/// rate-limit waits can make that cycle longer than the window, so by the first measure alone a
/// co-tenant cancelling every requote would only warn each cycle (Codex r4309154208, RB-aoh5-9
/// on PR #159). A replace of the requote is timed from the requote's place too, so a consumer
/// replacing its requote before another program acts keeps the rule (Codex r4310042604). One
/// that cancels a requote or its replace more than the window after the requote's place is
/// judged by the first measure.
#[derive(Clone, Debug)]
pub struct CancelsNotOurs {
    cfg: NotOursConfig,
    orders: usize,
    first: Option<MonoNs>,
    latest: Option<Event>,
    /// When each order of ours not ended was placed by this consumer.
    placed: HashMap<ClientOrderId, MonoNs>,
    /// The consumer's requote of each side (bid, ask) since the latest event ([`Requote`]).
    requotes: [Option<Requote>; 2],
}

/// The consumer's requote of a side since the latest event: the first order of ours placed on it
/// after that event was seen that did not end unrested ([`CancelsNotOurs::unrested`]), timed
/// from its place, and every order placed on the side since, not ended: its replaces (Codex
/// r4310042604 on PR #159), so a consumer replacing its requote before another program acts does
/// not take the requote rule away, and a fill of it does not either.
#[derive(Clone, Debug)]
struct Requote {
    since: MonoNs,
    founder: ClientOrderId,
    chain: Vec<ClientOrderId>,
}

/// The index of `side` in [`CancelsNotOurs::requotes`].
fn side_index(side: Side) -> usize {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

/// An event of cancels not ours: when it was seen, and the venue's time on its first cancel.
#[derive(Copy, Clone, Debug)]
struct Event {
    seen: MonoNs,
    at: Option<ExchNs>,
}

impl Event {
    /// Whether a cancel stamped `at` by the venue, seen at `now`, is within `span` of this
    /// event's first, by the venue's times when both have one.
    fn spans(&self, span: Duration, at: Option<ExchNs>, now: MonoNs) -> bool {
        let nanos = i128::try_from(span.as_nanos()).unwrap_or(i128::MAX);
        match (self.at, at) {
            (Some(first), Some(at)) => (i128::from(at.0) - i128::from(first.0)).abs() <= nanos,
            _ => now - self.seen <= span,
        }
    }
}

impl CancelsNotOurs {
    /// None seen yet, under the consumer's `cfg`.
    pub fn new(cfg: NotOursConfig) -> CancelsNotOurs {
        CancelsNotOurs {
            cfg,
            orders: 0,
            first: None,
            latest: None,
            placed: HashMap::new(),
            requotes: [None, None],
        }
    }

    /// The consumer placed our order `cid` on `side` at `now`: its requote of that side when it
    /// is the first placed on it since the latest event, otherwise a replace of it, judged from
    /// the requote's place ([`Requote`]).
    pub fn placed(&mut self, cid: ClientOrderId, side: Side, now: MonoNs) {
        self.placed.insert(cid, now);
        if self.latest.is_none() {
            return;
        }
        match &mut self.requotes[side_index(side)] {
            Some(requote) => requote.chain.push(cid),
            slot @ None => {
                *slot = Some(Requote {
                    since: now,
                    founder: cid,
                    chain: vec![cid],
                });
            }
        }
    }

    /// Our place `cid` ended without resting: refused (for its rate, or a post-only place that
    /// would cross), or never sent. It held its side's requote only as the first place after the
    /// latest event, and another program cannot cancel it, so the side's requote is the next
    /// order placed on it: the one that rests (RB-aoh5-10 on PR #159). A replace of a requote
    /// that rested frees nothing: the requote stays timed from its place.
    pub fn unrested(&mut self, cid: ClientOrderId) {
        for slot in &mut self.requotes {
            if let Some(requote) = slot {
                requote.chain.retain(|c| *c != cid);
                if requote.founder == cid && requote.chain.is_empty() {
                    *slot = None;
                }
            }
        }
    }

    /// Keeps the placing times of the orders `keep` takes (those not ended).
    pub fn retain(&mut self, keep: impl Fn(&ClientOrderId) -> bool) {
        self.placed.retain(|cid, _| keep(cid));
        for requote in self.requotes.iter_mut().flatten() {
            requote.chain.retain(|cid| keep(cid));
        }
    }

    /// Our order `cid` on `side` (placed by this consumer, [`Self::placed`], or before it ran),
    /// ended by a cancel not ours the venue stamped `at`, seen at `now`: the same event as the
    /// one before it when its order was placed before that one was seen and the venue stamped it
    /// within the span of that one's first; otherwise a repeat when within the window of the
    /// window's first event or of its place as the consumer's requote of `side` (type
    /// documentation), or the first of a new window.
    pub fn on_cancel(
        &mut self,
        cid: ClientOrderId,
        side: Side,
        at: Option<ExchNs>,
        now: MonoNs,
    ) -> NotOurs {
        self.orders += 1;
        let placed = self.placed.remove(&cid);
        if let Some(latest) = self.latest
            && placed.is_none_or(|placed| placed <= latest.seen)
            && latest.spans(self.cfg.span, at, now)
        {
            return NotOurs::SameEvent;
        }
        let window = self.cfg.window;
        let requote = self.requotes[side_index(side)]
            .as_ref()
            .is_some_and(|requote| requote.chain.contains(&cid) && now - requote.since <= window);
        self.latest = Some(Event { seen: now, at });
        self.requotes = [None, None];
        match self.first {
            _ if requote => NotOurs::Repeat,
            Some(first) if now - first <= window => NotOurs::Repeat,
            _ => {
                self.first = Some(now);
                NotOurs::First
            }
        }
    }

    /// How many orders of ours cancels not ours ended.
    pub fn orders(&self) -> usize {
        self.orders
    }

    /// Whether any was seen: another program may be on the account.
    pub fn seen(&self) -> bool {
        self.orders > 0
    }

    /// The consumer's window, for what it prints.
    pub fn window(&self) -> Duration {
        self.cfg.window
    }
}
