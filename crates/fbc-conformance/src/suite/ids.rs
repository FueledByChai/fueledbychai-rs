//! `ids_roundtrip` and `restart_cid` (design §4.3 and §6, decision 0004): the core's client-id
//! codec under the venue's wire format, and a mint restarted while our orders rest.
//!
//! # `ids_roundtrip`
//!
//! The suite mints ids at both ends of the sequence space and at a realistic start time
//! ([`ROUNDTRIP_SEEDS`]), spells each in the venue's
//! [`ClientIdFormat`](fbc_core::ClientIdFormat) (`OrderCaps.client_id`) with
//! [`encode_cid`], and reads the spelling back through the decode scope the core lends for the
//! venue's caps: in the suite's namespace it must be that id ([`CidMatch::Ours`]), and in
//! another namespace another engine's ([`CidMatch::Foreign`]). A format too small for the
//! canonical payload fails here, not on the first order.
//!
//! Then every id in `<fixtures>/ids_roundtrip/java_era.txt` (one per line, each trimmed; blank
//! lines and lines starting with `#` are ignored) must read as [`CidMatch::Unparseable`]: an
//! order the Java stack left resting is never taken for one of ours. The file lists the ids
//! the Java brokers sent this venue (their millisecond counters, UUIDs), or, for a venue the
//! Java stack never traded, ids another system would send it; a file missing or listing no id
//! fails, as a pass that probed nothing would prove nothing.
//!
//! # `restart_cid`
//!
//! Before the restart, the suite's mint issued our first [`MINTED_BEFORE`] ids (sequence
//! numbers 1 to 8 in the suite's namespace, 1). The case
//! `<fixtures>/restart_cid/resting.frames` ([`frames`](super::frames)) is what the venue shows
//! a restarted process of them: a resync's answer with our orders resting, the newest id among
//! them, and any other frames the venue sends at startup. It is decoded by a codec built fresh
//! from the factory.
//!
//! The restarted mint is seeded as [`CidMint::new`] is, in the worst case: its high-water mark
//! was lost and its clock stands at the epoch, so only the highest sequence number of ours the
//! venue's frames show keeps it above the ids it issued before. It must mint none of the
//! [`MINTED_BEFORE`] ids again, live (resting in the resync's answer) or recent (the others). A
//! codec that loses a client id from a resync, or reads it as another's, lets the mint issue it
//! again, and an id the venue might still match is sent twice.
//!
//! The case must decode with no frame refused and show at least one order of ours resting in
//! a resync's answer, or it proves nothing.
//!
//! Both are skipped for a venue whose caps declare no order entry: it sends no client id.

use std::collections::BTreeSet;
use std::fs;

use fbc_core::{
    CidMatch, ClientOrderId, ExecEvent, Namespace, VenueOrderState, WallNs, decode_cid, encode_cid,
};

use super::frames::{self, EXT};
use super::harness::{Harness, NAMESPACE, WALL};
use super::{Breach, Failure, Subject, Verdict};

const ROUNDTRIP: &str = "ids_roundtrip";
const RESTART: &str = "restart_cid";

/// The Java-era id file, under the fixture directory.
const JAVA_ERA: &str = "ids_roundtrip/java_era.txt";

/// The case `restart_cid` reads, under the fixture directory.
const RESTING: &str = "restart_cid/resting";

/// Another engine's namespace, for the ids it must read as foreign.
const OTHER: Namespace = Namespace::new(2);

/// How many ids the suite's mint issued before the restart.
pub const MINTED_BEFORE: usize = 8;

/// The mints `ids_roundtrip` draws from: the high-water mark, the start time and how many ids
/// each issues. The first sequence numbers, those at the fixed encode time's wall floor, and
/// the last three a namespace can issue.
const ROUNDTRIP_SEEDS: [(u64, WallNs, usize); 3] = [
    (0, WallNs(0), 3),
    (0, WALL, 3),
    (u64::MAX - 3, WallNs(0), 3),
];

/// Runs `ids_roundtrip` against `subject`.
pub fn ids_roundtrip(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(ROUNDTRIP, subject)?;
    let Some(exec) = h.caps.exec.as_ref() else {
        return Ok(skipped(ROUNDTRIP));
    };
    let fmt = exec.order.client_id;
    let mut cids = Vec::new();
    for (hwm, start, n) in ROUNDTRIP_SEEDS {
        cids.extend(h.cids_from(hwm, 0, start, n)?);
    }
    let mut breaches = Vec::new();
    let capability = format!("OrderCaps.client_id is {fmt:?}");
    for cid in &cids {
        let read = encode_cid(&fmt, *cid).map(|wire| {
            let ours = h.decode_scope(|scope| scope.client_order_id(&wire));
            (ours, decode_cid(&fmt, OTHER, &wire))
        });
        let want = (CidMatch::Ours(*cid), CidMatch::Foreign(NAMESPACE));
        if read != Ok(want) {
            let what = format!(
                "our id of sequence number {} does not round-trip: read back as {read:?}",
                cid.seq()
            );
            breaches.push(Breach::new(&capability, what));
        }
    }
    let mut probed = vec![format!("{capability}: {} minted ids", cids.len())];
    match java_era(subject) {
        Err(breach) => breaches.push(breach),
        Ok(ids) => {
            let before = breaches.len();
            for id in &ids {
                let read = h.decode_scope(|scope| scope.client_order_id(id));
                if read != CidMatch::Unparseable {
                    let what = format!("reads the Java-era id {id} as {read:?}, not Unparseable");
                    breaches.push(Breach::new(&capability, what));
                }
            }
            if breaches.len() == before {
                probed.push(format!("{JAVA_ERA}: {} Java-era ids", ids.len()));
            }
        }
    }
    finish(ROUNDTRIP, breaches, probed)
}

/// The ids the Java-era file lists, or why it lists none.
fn java_era(subject: &Subject<'_>) -> Result<Vec<String>, Breach> {
    let text = fs::read_to_string(subject.fixtures().join(JAVA_ERA))
        .map_err(|e| Breach::new(JAVA_ERA, format!("cannot read: {e}")))?;
    let ids: Vec<String> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect();
    if ids.is_empty() {
        return Err(Breach::new(JAVA_ERA, "lists no id"));
    }
    Ok(ids)
}

/// Runs `restart_cid` against `subject`.
pub fn restart_cid(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(RESTART, subject)?;
    if h.caps.exec.is_none() {
        return Ok(skipped(RESTART));
    }
    let file = format!("{RESTING}.{EXT}");
    let case = frames::read(&subject.fixtures().join(&file)).map_err(|what| h.fail(&file, what))?;
    let mut breaches = Vec::new();
    // Every id of ours the frames show, and those resting in a resync's answer.
    let (mut shown, mut live) = (BTreeSet::new(), BTreeSet::new());
    for step in frames::decode(&h, &case)? {
        if let Err(e) = step.result {
            let what = format!("line {} refused: {e}", step.line);
            breaches.push(Breach::new(&file, what));
        }
        for (_, event) in &step.events {
            let Some((cid, resting)) = ours(event) else {
                continue;
            };
            shown.insert(cid);
            if resting {
                live.insert(cid);
            }
        }
    }
    if live.is_empty() {
        let what = "shows no order of ours resting in a resync's answer: a restart with nothing \
                    resting proves nothing";
        breaches.push(Breach::new(&file, what));
    }
    if !breaches.is_empty() {
        return Err(Failure {
            check: RESTART,
            breaches,
        });
    }
    let before = h.cids(MINTED_BEFORE)?;
    let snapshot_max = shown.iter().map(|cid| cid.seq()).max().unwrap_or(0);
    // Its high-water mark lost and its clock at the epoch: only the venue's frames hold it up.
    let after = h.cids_from(0, snapshot_max, WallNs(0), MINTED_BEFORE)?;
    for cid in after.iter().filter(|cid| before.contains(cid)) {
        let what = format!(
            "a mint restarted from {file} issues our id of sequence number {} again: the \
             highest of ours the frames show is {snapshot_max}",
            cid.seq()
        );
        breaches.push(Breach::new("ExecCodec::resync", what));
    }
    let probed = vec![format!(
        "{file}: {} of ours resting, {} shown, restarted above {snapshot_max}",
        live.len(),
        shown.len()
    )];
    finish(RESTART, breaches, probed)
}

/// The id of ours `event` carries and whether it rests in a resync's answer.
fn ours(event: &ExecEvent) -> Option<(ClientOrderId, bool)> {
    let (cid, resting) = match event {
        ExecEvent::ResyncOrder(snap) => (snap.cid, snap.state == VenueOrderState::Open),
        ExecEvent::Order(update) => (update.cid, false),
        ExecEvent::Fill(fill) => (fill.cid, false),
        _ => return None,
    };
    match cid {
        Some(CidMatch::Ours(cid)) => Some((cid, resting)),
        _ => None,
    }
}

/// Why a check is skipped for a venue without order entry.
fn skipped(check: &'static str) -> Verdict {
    Verdict::Skipped {
        check,
        why: "the caps declare no order entry (VenueCaps.exec is None): it sends no client id",
    }
}

/// A pass probing `probed`, or a failure with `breaches`.
fn finish(
    check: &'static str,
    breaches: Vec<Breach>,
    probed: Vec<String>,
) -> Result<Verdict, Failure> {
    if breaches.is_empty() {
        Ok(Verdict::Passed { check, probed })
    } else {
        Err(Failure { check, breaches })
    }
}
