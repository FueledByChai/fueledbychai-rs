//! `caps_truthful` (decision 0003): a venue does only what its caps declare.
//!
//! Each probe is one command, encoded by a codec built fresh from the factory, that the caps
//! either refuse or declare; it changes one thing from the plainest order the caps allow
//! ([`Shape::plain`]: the first combination of their kinds, times in force and channels that
//! no declared conflict refuses), so the refusal it should meet is unambiguous:
//!
//! - every order kind, time in force, channel and flag the caps leave out, on a placement, on
//!   an item of a batch placement and on an amend, is refused `NotSent(Unsupported)`;
//! - every declared flag conflict, the pair set together on one order, is refused
//!   `NotSent(FlagConflict)` (a pair no one order can carry, such as IOC with FOK, is not
//!   probed);
//! - an absent operation (amend, batch placement, batch cancel, an account or instrument
//!   cancel-all, cancel-on-disconnect, or a dead-man refresh without a dead-man timer), a
//!   batch longer than its `max_items` (one item, for a `max_items` of zero), and an amend, cancel, batch-cancel item or query
//!   naming only references the caps leave out for it are refused `NotSent(Unsupported)`;
//! - a refusal asks for no effect: no frame, no HTTP request, no timer;
//! - an instrument cancel-all the caps declare is sent charged as a cancel-all of that
//!   instrument and nothing wider (an account-wide request's charge names no instrument), and
//!   one they leave out is refused rather than widened to the account.
//!
//! Beside the probes, one control per operation the caps declare (the plainest allowed
//! placement, batch, amend, cancel, batch cancel and query, an account cancel-all, arming and
//! disarming cancel-on-disconnect, a dead-man refresh) must be sent, carrying its request: a
//! codec that refuses everything would otherwise pass (Codex r4188835150). Which amend fields may change (`AmendCaps`'s
//! `price`, `qty` and `flags`) is not probed: an amend carries the whole amended order and no
//! codec holds the original to compare it with, so those are the OMS's permits to hold.
//!
//! A venue whose caps declare no order entry passes when its factory builds no order-entry
//! codec, and fails when it builds one.

use fbc_core::{
    CancelBatch, CancelOnDisconnect, CancelScope, CapTag, Channel, Effect, Feature, NotSentReason,
    OpKind, OrderCaps, OrderKindTag, RefKind, Support, TifTag, VenueCommand,
};

use super::harness::{Encoded, Harness, Ids, RPC, Shape, undeclared};
use super::{Breach, Failure, Subject, Verdict};

use NotSentReason::Unsupported;

const CHECK: &str = "caps_truthful";

/// The amend's references: an amend carries no placement nonce.
const AMEND_REFS: [RefKind; 2] = [RefKind::Venue, RefKind::Client];

/// What a probe must meet.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
enum Expect {
    /// Refused for this reason, with no effect.
    Refused(NotSentReason),
    /// Sent, carrying its request: a control.
    Sent,
    /// Sent, carrying its request, every charge a cancel-all of the probe's instrument.
    SentOnInstrument,
}

/// One command and what it must meet, named by the capability it holds the venue to.
struct Probe {
    capability: String,
    cmd: VenueCommand,
    expect: Expect,
}

/// Runs `caps_truthful` against `subject`.
pub fn caps_truthful(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let Some(exec) = h.caps.exec.clone() else {
        return match h.codec()? {
            None => Ok(Verdict::Passed {
                check: CHECK,
                probed: vec!["VenueCaps.exec is None: no order-entry codec".to_owned()],
            }),
            Some(_) => Err(h.fail(
                "VenueCaps.exec",
                "declares no order entry, yet exec_codec builds a codec",
            )),
        };
    };
    let probes = probes(&h, &exec.order)?;
    let mut breaches = Vec::new();
    let mut probed = Vec::new();
    for p in probes {
        let mut codec = h.exec_codec()?;
        let encoded = h.encode(codec.as_mut(), &p.cmd, RPC);
        if let Some(what) = judge(&h, &p, &encoded) {
            breaches.push(Breach {
                capability: p.capability.clone(),
                what,
            });
        }
        probed.push(p.capability);
    }
    if breaches.is_empty() {
        Ok(Verdict::Passed {
            check: CHECK,
            probed,
        })
    } else {
        Err(Failure {
            check: CHECK,
            breaches,
        })
    }
}

/// What, if anything, `p` met that it must not have.
fn judge(h: &Harness<'_>, p: &Probe, e: &Encoded) -> Option<String> {
    let n = e.fx.len();
    match (p.expect, &e.result) {
        (Expect::Refused(want), Err(got)) if *got == want => (n > 0).then(|| {
            format!(
                "refused as NotSent({want:?}) but asked for {n} effect(s): a refusal asks for none"
            )
        }),
        (Expect::Refused(want), Err(got)) => Some(format!(
            "refused as NotSent({got:?}), not NotSent({want:?})"
        )),
        (Expect::Refused(want), Ok(_)) => Some(format!(
            "sent ({n} effect(s)) where the caps make it NotSent({want:?})"
        )),
        (Expect::Sent | Expect::SentOnInstrument, Err(got)) => Some(format!(
            "refused as NotSent({got:?}) though the caps declare it, so the probes beside it \
             prove nothing"
        )),
        (Expect::Sent | Expect::SentOnInstrument, Ok(_))
            if !e.fx.carry_request(RPC, p.cmd.traffic_class()) =>
        {
            Some(format!(
                "sent, but its effects do not carry request {} as {:?} traffic",
                RPC.0,
                p.cmd.traffic_class()
            ))
        }
        (Expect::Sent, Ok(_)) => None,
        (Expect::SentOnInstrument, Ok(_)) => {
            let wider =
                e.fx.as_slice()
                    .iter()
                    .filter_map(Effect::charge)
                    .find(|(c, _)| c.op != OpKind::CancelAll || c.inst != Some(h.inst));
            wider.map(|(c, _)| {
                format!(
                    "sent charged as {:?} on {:?}, not as a cancel-all of instrument {}: \
                     widened beyond its instrument",
                    c.op,
                    c.inst.map(|i| i.get()),
                    h.inst.get()
                )
            })
        }
    }
}

/// Every probe the caps call for.
fn probes(h: &Harness<'_>, o: &OrderCaps) -> Result<Vec<Probe>, Failure> {
    // Enough orders for the longest batch probed: one past either batch's `max_items`.
    let longest = [
        o.batch_place.map(|b| b.max_items),
        o.batch_cancel.map(|b| b.max_items),
    ]
    .into_iter()
    .flatten()
    .max()
    .unwrap_or(0);
    let ids = Ids::new(h, usize::from(longest) + 2)?;
    let mut out = Vec::new();
    let mut push = |capability: String, cmd: VenueCommand, expect: Expect| {
        out.push(Probe {
            capability,
            cmd,
            expect,
        });
    };
    shapes(h, o, &ids, &mut push);
    references(h, o, &ids, &mut push);
    operations(h, o, &mut push);
    Ok(out)
}

/// An order shape that changes one thing from the plainest declared order, named by the
/// capability it holds the venue to, and the refusal the caps make it.
type Variant = (String, Shape, NotSentReason);

/// What a probe-recording closure looks like.
type Push<'a> = &'a mut dyn FnMut(String, VenueCommand, Expect);

/// The order shapes: kinds, times in force, channels, flags and flag conflicts, each on a
/// placement, a batch item and an amend, beside a control of each.
fn shapes(h: &Harness<'_>, o: &OrderCaps, ids: &Ids, push: Push<'_>) {
    let place = |shape| VenueCommand::Place(ids.order(h, 0, shape));
    let Some(plain) = Shape::plain(o) else {
        // The caps allow no order: the first they name is refused, and every variant of it
        // would be refused for more than the one thing it changes.
        let first = Shape::first(o);
        let why = first.refusal(o).unwrap_or(Unsupported);
        let label = "OrderCaps allows no order: the first it names".to_owned();
        return push(label, place(first), Expect::Refused(why));
    };
    let mut variants: Vec<(String, Shape)> = Vec::new();
    for &kind in OrderKindTag::ALL.iter().filter(|&&k| !o.kinds.contains(k)) {
        let label = format!("OrderCaps.kinds lacks {kind:?}");
        variants.push((label, Shape { kind, ..plain }));
    }
    for &tif in TifTag::ALL.iter().filter(|&&t| !o.tifs.contains(t)) {
        variants.push((
            format!("OrderCaps.tifs lacks {tif:?}"),
            Shape { tif, ..plain },
        ));
    }
    for &channel in Channel::ALL.iter().filter(|&&c| !o.channels.contains(c)) {
        let label = format!("OrderCaps.channels lacks {channel:?}");
        variants.push((label, Shape { channel, ..plain }));
    }
    if !o.post_only {
        let shape = plain.with(Feature::PostOnly);
        variants.push(("OrderCaps.post_only is false".to_owned(), shape));
    }
    if !o.reduce_only {
        let shape = plain.with(Feature::ReduceOnly);
        variants.push(("OrderCaps.reduce_only is false".to_owned(), shape));
    }
    for &(a, b) in &o.flag_conflicts {
        let shape = plain.with(a).with(b);
        if shape.has(a) && shape.has(b) {
            let label = format!("OrderCaps.flag_conflicts has ({a:?}, {b:?})");
            variants.push((label, shape));
        }
    }

    // Every variant changes one thing from an order the caps allow, so each is refused.
    let variants: Vec<Variant> = variants
        .into_iter()
        .filter_map(|(label, shape)| shape.refusal(o).map(|why| (label, shape, why)))
        .collect();
    push(
        "control: a placement".to_owned(),
        place(plain),
        Expect::Sent,
    );
    for (label, shape, why) in &variants {
        push(label.clone(), place(*shape), Expect::Refused(*why));
    }
    batch_shapes(h, o, ids, plain, &variants, push);
    amend_shapes(h, o, ids, plain, &variants, push);
}

/// The shapes as an item of a batch placement, beside a plain item where the batch has room.
fn batch_shapes(
    h: &Harness<'_>,
    o: &OrderCaps,
    ids: &Ids,
    plain: Shape,
    variants: &[Variant],
    push: Push<'_>,
) {
    let Some(batch) = o.batch_place else {
        let cmd = VenueCommand::PlaceBatch(vec![ids.order(h, 0, plain)]);
        let label = "OrderCaps.batch_place is None".to_owned();
        return push(label, cmd, Expect::Refused(Unsupported));
    };
    let max = usize::from(batch.max_items);
    let batch_of = |last: Shape| {
        let lead = (max >= 2).then(|| ids.order(h, 0, plain));
        let items = lead.into_iter().chain([ids.order(h, 1, last)]).collect();
        VenueCommand::PlaceBatch(items)
    };
    // A batch of no item has room for nothing but the one-item probe past its limit.
    if max > 0 {
        let control = "control: a batch placement".to_owned();
        push(control, batch_of(plain), Expect::Sent);
        for (label, shape, why) in variants {
            let label = format!("{label} (a batch item)");
            push(label, batch_of(*shape), Expect::Refused(*why));
        }
    }
    if batch.max_items < u16::MAX {
        let items = (0..=max).map(|i| ids.order(h, i, plain)).collect();
        let label = format!("Batch.max_items is {max}");
        push(
            label,
            VenueCommand::PlaceBatch(items),
            Expect::Refused(Unsupported),
        );
    }
}

/// The shapes as an amend (always of a limit order, so the kind is not varied), and an amend
/// naming only references the caps leave out for amends.
fn amend_shapes(
    h: &Harness<'_>,
    o: &OrderCaps,
    ids: &Ids,
    plain: Shape,
    variants: &[Variant],
    push: Push<'_>,
) {
    let both = ids.carrying(0, &AMEND_REFS).expect("both ids").0;
    let amend = |shape, target| VenueCommand::Amend(ids.amend(h, 0, shape, target));
    let Some(caps) = o.amend else {
        let label = "OrderCaps.amend is None".to_owned();
        return push(label, amend(plain, both), Expect::Refused(Unsupported));
    };
    let Some(limit) = Shape::sendable(o, &[OrderKindTag::Limit]) else {
        let first = Shape {
            kind: OrderKindTag::Limit,
            ..Shape::first(o)
        };
        let why = first.refusal(o).unwrap_or(Unsupported);
        let label = "OrderCaps allows no limit order to amend".to_owned();
        return push(label, amend(first, both), Expect::Refused(why));
    };
    let named = AMEND_REFS.iter().any(|&k| caps.refs.contains(k));
    if named {
        push(
            "control: an amend".to_owned(),
            amend(limit, both.clone()),
            Expect::Sent,
        );
    }
    // An amend is of a limit order: the kind is not varied.
    for (label, shape, why) in variants.iter().filter(|(_, s, _)| s.kind == limit.kind) {
        let label = format!("{label} (an amend)");
        push(label, amend(*shape, both.clone()), Expect::Refused(*why));
    }
    let left_out = undeclared(caps.refs, &AMEND_REFS);
    if let Some((target, _)) = ids.carrying(0, &left_out) {
        let label = format!("AmendCaps.refs lacks {left_out:?}");
        push(label, amend(limit, target), Expect::Refused(Unsupported));
    }
}

/// Cancels, batch cancels and queries: a control naming every reference, and a command naming
/// only references the caps leave out for it.
fn references(h: &Harness<'_>, o: &OrderCaps, ids: &Ids, push: Push<'_>) {
    let full = |i| ids.carrying(i, RefKind::ALL).expect("every reference");
    let cancel = |i| ids.cancel(h, full(i));

    if o.cancel_refs.is_empty() {
        let cmd = VenueCommand::Cancel(cancel(0));
        push(
            "OrderCaps.cancel_refs is empty".to_owned(),
            cmd,
            Expect::Refused(Unsupported),
        );
    } else {
        push(
            "control: a cancel".to_owned(),
            VenueCommand::Cancel(cancel(0)),
            Expect::Sent,
        );
        let left_out = undeclared(o.cancel_refs, RefKind::ALL);
        if let Some(refs) = ids.carrying(0, &left_out) {
            let cmd = VenueCommand::Cancel(ids.cancel(h, refs));
            let label = format!("OrderCaps.cancel_refs lacks {left_out:?}");
            push(label, cmd, Expect::Refused(Unsupported));
        }
    }

    match o.batch_cancel {
        None => {
            let cmd = VenueCommand::CancelMany(vec![cancel(0)]);
            push(
                "OrderCaps.batch_cancel is None".to_owned(),
                cmd,
                Expect::Refused(Unsupported),
            );
        }
        Some(batch) => batch_cancels(h, ids, batch, push),
    }

    let query = VenueCommand::Query(ids.query(h, full(0)));
    if o.query_refs.is_empty() {
        push(
            "OrderCaps.query_refs is empty".to_owned(),
            query,
            Expect::Refused(Unsupported),
        );
    } else {
        push("control: a query".to_owned(), query, Expect::Sent);
        let left_out = undeclared(o.query_refs, RefKind::ALL);
        if let Some(refs) = ids.carrying(0, &left_out) {
            let cmd = VenueCommand::Query(ids.query(h, refs));
            let label = format!("OrderCaps.query_refs lacks {left_out:?}");
            push(label, cmd, Expect::Refused(Unsupported));
        }
    }
}

/// Batch cancels the caps declare: where the batch has room, a control naming every reference
/// and an item naming only references the caps leave out for batch items; and one item past
/// its limit, so a batch of no item is probed too.
fn batch_cancels(h: &Harness<'_>, ids: &Ids, batch: CancelBatch, push: Push<'_>) {
    let cancel = |i| ids.cancel(h, ids.carrying(i, RefKind::ALL).expect("every reference"));
    let max = usize::from(batch.max_items);
    if max > 0 {
        let batch_of = |last| {
            let lead = (max >= 2).then(|| cancel(0));
            VenueCommand::CancelMany(lead.into_iter().chain([last]).collect())
        };
        if batch.refs.is_empty() {
            let label = "CancelBatch.refs is empty".to_owned();
            push(label, batch_of(cancel(1)), Expect::Refused(Unsupported));
        } else {
            let control = "control: a batch cancel".to_owned();
            push(control, batch_of(cancel(1)), Expect::Sent);
            let left_out = undeclared(batch.refs, RefKind::ALL);
            if let Some(refs) = ids.carrying(1, &left_out) {
                let label = format!("CancelBatch.refs lacks {left_out:?}");
                let cmd = batch_of(ids.cancel(h, refs));
                push(label, cmd, Expect::Refused(Unsupported));
            }
        }
    }
    if batch.max_items < u16::MAX {
        let cmd = VenueCommand::CancelMany((0..=max).map(cancel).collect());
        let label = format!("CancelBatch.max_items is {max}");
        push(label, cmd, Expect::Refused(Unsupported));
    }
}

/// Cancel-alls and cancel-on-disconnect.
fn operations(h: &Harness<'_>, o: &OrderCaps, push: Push<'_>) {
    let refused = Expect::Refused(Unsupported);
    let account = VenueCommand::CancelAll(CancelScope::Account);
    match o.cancel_all_account {
        Support::Unsupported => {
            let label = "OrderCaps.cancel_all_account is Unsupported".to_owned();
            push(label, account, refused);
        }
        Support::Native => push(
            "control: an account cancel-all".to_owned(),
            account,
            Expect::Sent,
        ),
    }
    let cmd = VenueCommand::CancelAll(CancelScope::Instrument(h.inst));
    match o.cancel_all_instrument {
        Support::Unsupported => {
            let label = "OrderCaps.cancel_all_instrument is Unsupported".to_owned();
            push(label, cmd, refused);
        }
        Support::Native => {
            let label = "OrderCaps.cancel_all_instrument is Native: never widened".to_owned();
            push(label, cmd, Expect::SentOnInstrument);
        }
    }
    // Arming and disarming cancel-on-disconnect, and refreshing a dead-man timer: sent where
    // the caps declare them, refused where they do not.
    let (protection, dead_man) = match o.cancel_on_disconnect {
        CancelOnDisconnect::None => (false, false),
        CancelOnDisconnect::PerConnection { .. } => (true, false),
        CancelOnDisconnect::DeadMan { .. } => (true, true),
    };
    let expect = |declared| if declared { Expect::Sent } else { refused };
    let label = |declared, what| match declared {
        true => format!("control: {what}"),
        false => format!("OrderCaps.cancel_on_disconnect has no {what}"),
    };
    let arm = VenueCommand::ArmCancelOnDisconnect(true);
    push(
        label(protection, "protection (arm)"),
        arm,
        expect(protection),
    );
    let disarm = VenueCommand::ArmCancelOnDisconnect(false);
    push(
        label(protection, "protection (disarm)"),
        disarm,
        expect(protection),
    );
    let refresh = VenueCommand::RefreshDeadMan;
    push(
        label(dead_man, "dead-man timer (refresh)"),
        refresh,
        expect(dead_man),
    );
}
