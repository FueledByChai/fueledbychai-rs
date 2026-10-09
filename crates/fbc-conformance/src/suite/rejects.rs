//! `reject_coverage` (design §6 step 9, BT-502; decisions 0014 item 6, 0085): every venue code in the
//! fixture's reject table maps to the `RejectKind` the table gives it, `Other` only where the
//! table says so. It runs fbc-runtime's order-entry session against the stub server as the
//! other order-entry checks do (`live.rs`): one placement per code, in the table's order, each
//! refused by the stub under that code ([`Answer::RejectCode`]).
//!
//! The table is `<fixtures>/reject_coverage/table.txt`: one code per line, then whitespace, then
//! the kind it maps to as `RejectKind`'s `Debug` spells it (`InvalidPrice`,
//! `RateLimited { retry_after: None }`, `AlreadyTerminal(Unspecified)`, `Other`); blank lines
//! and lines starting with `#` are ignored. Each placement's outcome must be one `Rejected` of
//! its single item (or of the whole request) carrying the code as `Reject::venue_code` and the
//! table's kind. A table that is missing, lists no code, or has a line with no kind fails: a
//! pass that probed nothing would prove nothing.

use std::fs;

use fbc_core::{ItemRef, OpKind, SubmitOutcome};

use super::live::Live;
use super::orders::verdict;
use super::stub::Answer;
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "reject_coverage";

/// The reject table, under the fixture directory.
const FILE: &str = "reject_coverage/table.txt";

/// Runs `reject_coverage` against `subject`.
pub fn reject_coverage(subject: &Subject<'static>) -> Result<Verdict, Failure> {
    let live = match Live::new(CHECK, subject)? {
        Ok(live) => live,
        Err(skipped) => return Ok(skipped),
    };
    let table = table(subject)?;
    let requests = table
        .iter()
        .map(|(code, _)| vec![Answer::RejectCode(code.clone())])
        .collect();
    let live = live.orders(table.len());
    let breaches = live.run(requests, async |c| {
        c.ready().await?;
        let mut sent = Vec::new();
        for _ in &table {
            let (_, auth) = c.oms.place(c.h)?;
            sent.push(c.send(auth, OpKind::Place).await?);
        }
        c.answered().await?;
        let mut breaches = Vec::new();
        for (rpc, (code, kind)) in sent.into_iter().zip(&table) {
            let outcomes = c.outcomes(rpc);
            let whole = |it: &Option<ItemRef>| it.as_ref().is_none_or(|it| it.idx == 0);
            let [(item, SubmitOutcome::Rejected(reject))] = outcomes.as_slice() else {
                let what = format!(
                    "the placement the stub refused under code {code} was reported \
                     {outcomes:?}, not Rejected once"
                );
                breaches.push(Breach::new("ExecCodec::on_frame", what));
                continue;
            };
            let capability = format!("{FILE}: {code}");
            if !whole(item) {
                let what = format!("the refusal names {item:?}, not the placement's one item");
                breaches.push(Breach::new(&capability, what));
            }
            if reject.venue_code.as_deref() != Some(code.as_str()) {
                let what = "the refusal does not carry the code the stub sent as \
                            Reject::venue_code";
                breaches.push(Breach::new(&capability, what));
            }
            let mapped = format!("{:?}", reject.kind);
            if mapped != *kind {
                let what = format!("the code maps to {mapped}; the table says {kind}");
                breaches.push(Breach::new(&capability, what));
            }
        }
        Ok(breaches)
    })?;
    verdict(CHECK, breaches, || {
        table
            .iter()
            .map(|(code, kind)| format!("{FILE}: {code} is {kind}"))
            .collect()
    })
}

/// The fixture's reject table: each code and the kind it maps to, in order.
fn table(subject: &Subject<'_>) -> Result<Vec<(String, String)>, Failure> {
    let path = subject.fixtures().join(FILE);
    let text = fs::read_to_string(&path);
    let text = text.map_err(|e| Failure::one(CHECK, FILE, format!("cannot read: {e}")))?;
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'));
    let mut table = Vec::new();
    for line in lines {
        let Some((code, kind)) = line.split_once(char::is_whitespace) else {
            let what = format!("the line {line:?} names no kind after its code");
            return Err(Failure::one(CHECK, FILE, what));
        };
        table.push((code.to_owned(), kind.trim().to_owned()));
    }
    if table.is_empty() {
        return Err(Failure::one(CHECK, FILE, "lists no code"));
    }
    Ok(table)
}
