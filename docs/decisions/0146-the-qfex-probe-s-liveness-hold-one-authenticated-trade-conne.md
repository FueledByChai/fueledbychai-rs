# 0146 — The QFEX probe's liveness hold, one authenticated trade connection that never arms, places or reads, may run on production under production_probe behind --production while no session is open, augmenting 0132, 0143 and 0144

Status: accepted
Date: 2026-11-13

## Context

**The question.** Nothing in this repository says how QFEX's trade WebSocket stays alive,
whether QFEX closes an idle one, or how long one may live (FBC-39ks). It matters before FBC-9sk2
rests orders: an armed connection that drops unexpectedly cancels every open order of the account
(0121, 0132 item 2). QFEX's authenticate page, as FBC-39ks read it, says only that the server
pings "from time to time" after authentication. The QFEX codec arms no keepalive timer of its
own (0056).

**The hold.** FBC-39ks adds a hold phase to the probe (`qfex_preprod_probe`): one authenticated
trade connection that never arms and places nothing, held for a duration the owner sets,
recording each ping and close it receives. As written, it runs on pre-production.

**Why production.** The owner's keys are production keys, and pre-production refused them
(0143's Context). 0143 says "This record decides stage one only", and 0144 changed only how
stage one reads accounts. Neither admits the hold.

**What production has shown.** FBC-291a's production sessions lasted about 11 s each and neither
was closed early (`fixtures/qfex/exec-recorded/arm-production/` and its report). The run's
journal, which the owner keeps, shows one server ping about 10 s into each (FBC-39ks's comment of
2026-11-13). One ping settles no interval, idle timeout or lifetime.

**An unarmed connection.** QFEX's cancel-on-disconnect page, as 0132's Context quotes it, says
enabling the flag cancels the account's open orders, says the engine cancels them "if the
connection drops unexpectedly", and scopes the flag to one connection. 0132 reads the drop
sentence as an armed connection's. The page says nothing explicit about a connection that never
enables the flag, and no run has measured one.

**The owner's choice.** On 2026-11-13 the owner was asked "Should a short decision record allow
FBC-39ks's never-arming hold to run on production?" and answered "Yes, write the record". Asked
"Approve records 0145 and 0146 as drafted?", the owner answered "Approve both".

## Decision

1. **Production is admitted for the hold.** The hold may run on QFEX production under
   `production_probe`, behind the probe's `--production` flag, with 0143 item 2's origins and
   refusals unchanged. On pre-production it runs under `preprod_probe`. It lives in
   `qfex_preprod_probe` alone, and every origin it uses passes the mode's guard through the
   venue-crate functions the probe's read sockets use (0132 item 7). This record gives no amount
   (0009).
2. **What the hold is.** One connection, on one account the configuration names (0144 item 1):
   the primary account unless the owner names the configured subaccount.
   - It sends its authentication, then the three subscriptions a session sends
     (`exec::CHANNELS`), each built by the venue crate as the codec builds it. It holds only once
     QFEX has accepted the authentication and confirmed all three within its 60 s deadline.
   - After that it sends nothing but the pong or close reply the WebSocket layer writes for each
     ping or close it receives (0140's Context), and at its end its own close. It never arms,
     places, cancels or reads, and sends no ping of its own.
   - It builds no `ExecSession` and no registry, and needs no `AccountWideCodAuthority`.
   - Its frames, and each reply the WebSocket layer writes for it, are charged to the run's shared
     limiter as a session charges them (0030). It provokes no rate limit (0135 item 1), and the
     refusals it meets are counted (0135 item 2).
3. **When it runs.** Before the run's sessions, while no session and no other connection of the
   probe is open. A run may hold without arming.
   - Before it opens, and again after it ends if its connection opened, the probe reads every
     named account as it does before a session (0132 item 7, as 0144 amends it). It holds only
     when each shows no open order and flat positions. The report gives both reads.
   - The owner confirms before the run that the user has no subaccount the configuration leaves
     out (0144 item 2). The owner also asserts that nothing trades on any account of the user from
     the first read to the last. The probe can check neither, and its report says both are
     conditions of the run.
4. **How long, and how it ends.**
   - `--hold-secs` sets its duration in whole seconds from the connection's opening, at most a
     day, with no default; the hold runs only when it is given. `--hold-account` names its
     account and is refused without `--hold-secs`. A run that holds needs no `--arm`, and a run
     without `--arm` refuses a registry or ledger flag. A run with neither the hold nor `--arm` is
     refused.
   - It ends at its duration, at the owner's interrupt (Ctrl-C), at a server close, when the
     connection ends, or at a refusal: the authentication refused, an err frame, or a
     subscription unconfirmed at the deadline. Nothing reconnects.
   - At its duration, the owner's interrupt or a refusal it closes, and the reads and the report
     follow. When the shared limiter has no room for its close, it drops the connection instead.
     A hold that ends at a refusal stops the run, and its report says it ended there.
   - The owner's interrupt goes to the hold from its connection's opening until its close is
     over. One in that time stops a run that also arms, so no session starts. Ctrl-C at any
     other time ends the program at once, with no report.
5. **What it records.** Each ping, pong and close it receives, with its time since the
   connection opened. A payload and a close reason are recorded by length only, and a close code
   in full. Data frames are counted by kind, never kept or printed; an order or fill frame
   received on the hold is reported. The report and the evidence say:
   - what the hold wrote, in order;
   - whether any ping came before authentication;
   - the intervals between pings after authentication;
   - each server close, with its code and its time since open, or that none came within the
     hold;
   - how the hold ended, and that a lifetime longer than the hold was not measured;
   - the environment the mode names, as 0143 item 3 asks of a run.
6. **What is committed.** The owner reads the report before it is committed. A production hold's
   report goes in `fixtures/qfex/exec-recorded/hold-production/` and a pre-production one's in
   `fixtures/qfex/exec-recorded/hold/`, beside a README that records the result. This follows
   0143 item 4's directories. No frame is committed.
7. **How the earlier records read.** 0143 and 0144 admit the hold beside stage one, and their
   checks and falsifiers apply to a run that holds. From 0132 item 7 the hold keeps the
   owner-assisted run, the origins, the accounts and the reads before a session. Item 7's
   registry, commands, stops on places and fills, and cancels after a session do not apply.
8. **What the result decides.** The result settles FBC-39ks's step 2 or step 3, through a
   follow-up ticket, for the environment it was measured on. Whatever that step writes names the
   environment measured and says the other was not. A production hold therefore settles liveness
   for FBC-9sk2 only if FBC-9sk2 runs on production, which 0143 item 5 leaves to FBC-9sk2's
   record. Stages two and three on production, and production's admission (FBC-cnyh), are not
   decided here.

## Alternatives

- **Read liveness from FBC-291a's journal:** not enough. Each session held one ping in about
  11 s and ended at the arm's deadline.
- **Hold an `ExecSession`:** rejected. The session arms itself on every epoch (0058, 0121), the
  unanswered arm ends the epoch (0132 item 1), and an armed hold would put the account's orders
  at stake.
- **Send a client ping on the hold:** rejected. It would hide whether QFEX needs one, which is
  the case 0056 says would want a new record.
- **Hold on every named account, or on two at once:** not taken. The question is about a
  connection, not an account, and one connection keeps the hold apart from everything else the
  probe opens (item 3).

## Consequences

- **The proof.** `crates/fbc-examples/tests/qfex_preprod_probe.rs`, against the QFEX-shaped
  stub, shows a hold:
  - on `--production`, its origins refused as 0143 item 3 says;
  - opened only after the named accounts are read, with none open while it holds, and the
    accounts read again after it;
  - not opened when a named account shows an open order or a position;
  - writing its authentication and the three subscriptions and nothing after them;
  - ending at its duration, at an interrupt, at a server close and at each refusal;
  - counting data frames by kind without keeping them, and reporting an order or fill frame;
  - charging its frames and its replies to the shared limiter;
  - with its flags refused when missing, zero or over a day;
  - with its report and evidence naming production and passing `scripts/privacy-check.sh`.

  `crates/venues/fbc-venue-qfex/tests/liveness.rs` covers the codec's side, as FBC-39ks says.
- **The docs.** `crates/fbc-examples/README.md` and the probe's `--help` say how the owner runs
  the hold on production. `AGENTS.md`'s Layout entry for the probe names it.
- **What the owner does.** The owner runs the hold, reads its report, and commits it with its
  README.

## What would show this was wrong

- A named account read after the hold showing an order or a position its read before did not, or
  an order or fill frame received on the hold.
- A hold that arms, places, cancels or reads, or writes a data frame after its subscriptions.
- A hold opened while a session or another connection of the probe is open.
- A committed hold report holding an identifying string or an amount of the account.
- QFEX documenting that a connection that never enabled the flag cancels orders when it closes
  or drops.
- A liveness result from one environment applied to the other without a hold measured there.
