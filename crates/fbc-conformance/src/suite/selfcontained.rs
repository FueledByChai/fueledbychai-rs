//! `commands_selfcontained` (decisions 0005, 0014 item 5): an amend, a cancel or a query
//! carries everything its venue needs, so a codec encodes it from the command and the spec
//! table alone. The OMS resolves every field from its record; a codec keeps no order registry,
//! and after a reconnect or a restart it has seen nothing.
//!
//! For each reference the caps declare for amends, cancels, batch-cancel items and queries,
//! a command naming its order by that reference is encoded by a codec built fresh from the
//! factory, which has seen nothing: it must be sent, carrying its request on its traffic
//! class. The same command is then encoded by another fresh codec that first saw the order
//! placed, and must give the same result and the same effects: a codec that answers
//! differently once it has seen the placement depends on state the command does not carry.
//! A placement nonce is probed only where some command can name its order by it, behind a
//! target the operation does not take. The placement the warm codec sees is the plainest order
//! the caps allow, and must itself be sent, carrying its request, on each warm codec, or the
//! command is a breach left uncompared; where they allow no order there is no
//! warm codec, and amends, which target a resting limit order, are probed only where the caps
//! allow a limit order.
//!
//! Skipped for a venue whose caps declare no order entry, or no reference any amend, cancel or
//! query can name its order by.

use fbc_core::{OrderCaps, OrderKindTag, RefKind, RpcId, VenueCommand};

use super::harness::{Harness, Ids, RPC, Shape, placement_nonce};
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "commands_selfcontained";

/// The request id the placement a warm codec saw first was encoded as.
const PLACE_RPC: RpcId = RpcId(2);

/// Runs `commands_selfcontained` against `subject`.
pub fn commands_selfcontained(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let Some(exec) = h.caps.exec.clone() else {
        return Ok(Verdict::Skipped {
            check: CHECK,
            why: "the caps declare no order entry (VenueCaps.exec is None): no amend, cancel or \
                  query to encode",
        });
    };
    let ids = Ids::new(&h, 1)?;
    let commands = commands(&h, &exec.order, &ids);
    if commands.is_empty() {
        return Ok(Verdict::Skipped {
            check: CHECK,
            why: "the caps declare no reference an amend, cancel or query can name an order by",
        });
    }
    let mut breaches = Vec::new();
    let mut probed = Vec::new();
    // The placement a warm codec sees first; none where the caps allow no order.
    let place = Shape::plain(&exec.order).map(|shape| VenueCommand::Place(ids.order(&h, 0, shape)));
    for (label, cmd) in commands {
        let fresh = h.encode(h.exec_codec()?.as_mut(), &cmd, RPC);
        // Each warm codec's placement must itself be sent, carrying its request, or the codec
        // has seen nothing and the comparison proves nothing (Codex r4188991867).
        let warm = match &place {
            Some(place) => {
                let mut warm = h.exec_codec()?;
                let placed = h.encode(warm.as_mut(), place, PLACE_RPC);
                let sent = placed.result.is_ok()
                    && placed.fx.carry_request(PLACE_RPC, place.traffic_class());
                Some(sent.then(|| h.encode(warm.as_mut(), &cmd, RPC)))
            }
            None => None,
        };
        let class = cmd.traffic_class();
        let what = match &fresh.result {
            Err(why) => Some(format!(
                "refused as NotSent({why:?}) by a freshly built codec"
            )),
            Ok(_) if !fresh.fx.carry_request(RPC, class) => Some(format!(
                "encoded by a freshly built codec, but its effects do not carry request {} as \
                 {class:?} traffic",
                RPC.0
            )),
            Ok(_) => match warm {
                Some(None) => Some(
                    "not compared: the placement a codec was to see first was refused or sent \
                     without its request, though the caps allow it"
                        .to_owned(),
                ),
                Some(Some(after)) if after.result != fresh.result || after.fx != fresh.fx => Some(
                    "encoded differently by a codec that first saw the order placed: it \
                     depends on state the command does not carry"
                        .to_owned(),
                ),
                _ => None,
            },
        };
        if let Some(what) = what {
            breaches.push(Breach {
                capability: label.clone(),
                what,
            });
        }
        probed.push(label);
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

/// Every command to encode, named by the operation and the reference it names its order by.
pub(crate) fn commands(h: &Harness<'_>, o: &OrderCaps, ids: &Ids) -> Vec<(String, VenueCommand)> {
    let mut out = Vec::new();
    // An amend targets a resting limit order: none to amend where the caps allow none.
    let limit = Shape::sendable(o, &[OrderKindTag::Limit]);
    if let Some((amend, limit)) = o.amend.zip(limit) {
        for kind in amend.refs.iter().filter(|&k| k != RefKind::PlacementNonce) {
            let (target, _) = naming(ids, kind, amend.refs).expect("our id or the venue's");
            let cmd = VenueCommand::Amend(ids.amend(h, 0, limit, target));
            out.push((format!("AmendCaps.refs has {kind:?}: an amend"), cmd));
        }
    }
    for kind in o.cancel_refs.iter() {
        if let Some(refs) = naming(ids, kind, o.cancel_refs) {
            let cmd = VenueCommand::Cancel(ids.cancel(h, refs));
            out.push((format!("OrderCaps.cancel_refs has {kind:?}: a cancel"), cmd));
        }
    }
    if let Some(batch) = o.batch_cancel.filter(|b| b.max_items > 0) {
        for kind in batch.refs.iter() {
            if let Some(refs) = naming(ids, kind, batch.refs) {
                let cmd = VenueCommand::CancelMany(vec![ids.cancel(h, refs)]);
                out.push((
                    format!("CancelBatch.refs has {kind:?}: a batch cancel"),
                    cmd,
                ));
            }
        }
    }
    for kind in o.query_refs.iter() {
        if let Some(refs) = naming(ids, kind, o.query_refs) {
            let cmd = VenueCommand::Query(ids.query(h, refs));
            out.push((format!("OrderCaps.query_refs has {kind:?}: a query"), cmd));
        }
    }
    out
}

/// Order 0's references such that, of `declared`, the operation picks `kind`: that reference
/// alone, or for a placement nonce the nonce behind a target of a kind the operation does not
/// take; `None` when no command can be named by `kind` (every target kind is declared, and
/// tried before the nonce).
fn naming(
    ids: &Ids,
    kind: RefKind,
    declared: fbc_core::TagSet<RefKind>,
) -> Option<(fbc_core::OrderRef, Option<u64>)> {
    match kind {
        RefKind::Venue | RefKind::Client => ids.carrying(0, &[kind]),
        RefKind::PlacementNonce => {
            let behind = [RefKind::Venue, RefKind::Client]
                .into_iter()
                .find(|&k| !declared.contains(k))?;
            let (target, _) = ids.carrying(0, &[behind])?;
            Some((target, Some(placement_nonce(0))))
        }
    }
}
