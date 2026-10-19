# 0112 — A venue-flagged reduce-only order that strictly shrinks its side's worst case passes the inventory cap even past it, amending 0005's I6, 0013 rule 2, 0052 and 0110

Status: accepted
Date: 2026-10-19

## Context

0005's I6 (the inventory cap, as 0052 names it) admits an order only if
`|pos + Σ resting same side + new| ≤ inventory cap`, and 0052's resting cap admits it only if
`Σ resting same side + new ≤ resting cap`. 0013 rule 2 and 0052 apply both to every order
command, reducing ones and batch items included, with nothing exempt as reducing. Reviewer B
found on PR #180 (FBC-7aw4) that together they leave a hole: a reducing order of q on a
position P, with nothing else on its side, passes the inventory cap only if `P - q` is within
it and the resting cap only if q is within it, so a reducing order exists only while
`P ≤ inventory cap + resting cap`. Past that no reducing order of any size is admitted, in
Quoting or in Exit by Flatten or Wind-down, and 0110's Consequences records that such a
position "cannot be crossed by this library and is flattened on the venue". A position gets
there only when something has gone wrong (orphan or race fills), which is when Flatten and a
consumer's disaster stop are needed most.

The owner was asked (FBC-ugto) and answered on 2026-10-19, to the coordinator, quoted
exactly: to "Once a position is past $61 (inventory cap + resting cap), how should fbc-oms
judge a reducing order?" he chose "Exempt venue reduce-only (Recommended)" (option text: "A
reduce-only order flagged to the venue that only shrinks the worst-case position passes the
inventory cap. The venue stops it crossing zero, and the $11 resting cap still applies, so the
position comes down in $11 steps. Flatten and force-close keep working past $61."). The dollar
figures in the question are the cap values configured then; the owner removed those values on
2026-10-19 (FBC-thks), and caps are the consumer's configuration with no default (0052), so
this record states the rule for any caps.

## Decision

1. **The exemption.** A place, an amend or replace, or a batch item that carries the venue's
   reduce-only flag (`NewOrder::reduce_only`, the placement's flag for an amend) passes the
   inventory cap whenever its admission leaves the side's worst case strictly smaller than the
   side's worst case without it, `|pos + Σ others same side + new| < |pos + Σ others same side|`,
   even when the result is still past the cap. Within the cap the formula admits it as before.
   An order carrying the flag reaches the wire only on a venue whose `OrderCaps::reduce_only`
   declares one (a codec refuses it otherwise), and the venue keeps it from taking the position
   past zero.
2. **Not exempt.** An order the OMS only classifies reducing (`NewOrder::reducing` without the
   flag, as an emulated reduce-only order goes out on a venue refusing the flag with post-only,
   FBC-01w) passes only by the formula, since nothing at the venue stops it overshooting if a
   fill races its cancel. Neither is an order, flagged or not, that would leave the worst case
   the same or larger, such as a bid on a long or an offer beside offers already taking the
   worst case past zero.
3. **Everything else still applies.** The resting cap has no exemption, so a position past the
   inventory cap comes down in orders the resting cap bounds; the market's state (Killed and
   Cancel-only build nothing; Exit admits only exit orders, 0063), an orphan cooldown's
   suppression (0108) and the duplicate-id check are judged as before. The same rule is judged
   again at authorization: on each side of the command, the exemption holds only if every one
   of the command's orders on that side carries the flag, and the worst case is compared with
   the side's worst case without the command's orders, as they are now; a flagged order that no
   longer shrinks it is refused there.
4. **Where.** In the one pre-trade path (`Exposure::admit` in `fbc-oms`'s `caps.rs`), so
   `Registry::place`, `Registry::place_batch`, `Live::amend` and `Registry::authorize` judge it
   the same way.

This amends 0005's I6 and 0052 for this case only, reads 0013 rule 2's "nothing is exempt as
reducing" as unchanged for the OMS's classification (it still exempts nothing), and amends
0110's rule 4 ("no exemption"); the sentence of 0110's Consequences saying a position past the
inventory cap plus the resting cap cannot be crossed by this library is superseded: such a
position is crossed in steps the resting cap bounds.

## Alternatives

- Keep the rule and leave the position to be flattened on the venue: not chosen by the owner.
  Flatten and the disaster stop fail exactly when needed.
- Admit any reducer whose worst case does not grow, emulated reduce-only included: not chosen.
  An emulated order can overshoot zero when a fill races its cancel, and nothing at the venue
  bounds it.
- Exempt reduce-only orders from both caps: rejected. The resting cap is what bounds each step
  and the order stack on the reducing side (0052).
- Clip a crossing intent to the resting cap: out of scope; 0110 keeps every cap applied as the
  cap's refusal and the consumer sizes its cross within it.

## Consequences

- Flatten, Wind-down, a consumer's force-close and any venue-flagged reduce-only order keep
  working on a position of any size past the inventory cap, one order of at most the resting
  cap at a time (and less what else rests on the side).
- A venue without a reduce-only flag gets no such exit past the cap: its emulated reducers and
  crossing intents (refused there by 0110) are judged by the formula alone.
- `scripts/check.sh`'s tests pin the rule: `tests/crossing.rs` crosses a long past both caps
  down in resting-cap steps in Quoting and Exit, and `tests/caps.rs` covers places, amends,
  batch items and authorization.

## What would show this was wrong

- A venue-flagged reduce-only order of ours that took a position past zero, or one whose
  admission grew a side's worst case.
- An emulated or classified-only reducer built past the inventory cap.
- A venue that accepts the reduce-only flag but does not enforce it against the position.
