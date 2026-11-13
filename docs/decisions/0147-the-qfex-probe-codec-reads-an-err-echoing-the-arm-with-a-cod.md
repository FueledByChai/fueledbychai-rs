# 0147 — The QFEX probe codec reads an err echoing the arm with a code other than RateLimited as the arm's rejection, and skips and counts what the consumer's spec table does not hold, augmenting 0132 and 0134

Status: accepted
Date: 2026-11-13

## Context

FBC-jxau decodes, under either probe mode, what 0132 items 1, 5 and 8 say: the arm's answer,
the resync, cancel settlement, and the order, fill, page and position frames. FBC-291a's
production capture shows the answer 0132 item 1 expects: an `ack` echoing the arm, on both
accounts (`fixtures/qfex/exec-recorded/arm-production/`). Two readings are left open.

**The records disagree on an err echoing the arm.**

- 0132 item 1: "An `err` with such an echo is `Rejected`, unless its code is `RateLimited`."
  0134 item 7 says the same: "An `err` with another code is the arm's rejection, as 0132 item 1
  says".
- 0134 item 5: "No err settles a request … This holds for every err, whether or not it
  echoes".
- 0132 item 8 says an err settles no place and no cancel. It says nothing of the arm.

**Symbols outside the consumer's spec table.** A private frame or a resync row can name a symbol
the consumer's `SpecTable` does not hold.

- The probe runs its sessions with an empty table (`stage.rs`, `specs: SpecTable::new()`), and
  production's capture holds eleven `position_response` frames on such a symbol, flat.
- An account may also hold orders or positions on markets the consumer does not trade.
- No record says what the QFEX codec does with them. Refusing the frame would end no epoch
  (0132 keeps it open on private state), but refusing the resync would ask to reconnect, so an
  account with any such row could never finish a resync.

**The owner's choice.** On 2026-11-13 the owner was asked to approve this record as drafted, and
answered "Approve 0147".

## Decision

1. **An err echoing the arm.** An err whose kept echo equals the arm, as 0132 item 1 defines it,
   answers the arm `Rejected` when its code is anything but `RateLimited`, documented or not.
   A `RateLimited` one holds every frame budget for its hint and settles nothing (0126, 0134
   item 7), and nothing later answers the arm on that epoch, so it fails at its deadline as
   0132 item 1 says. 0134 item 5's "No err settles a request" reads as covering every request but the
   arm. Every other err settles nothing, as 0132 item 8 and 0134 item 5 say, and what each does
   to the epoch is unchanged.
2. **Symbols the spec table does not hold.**
   - A live order, fill or position frame naming such a symbol pushes nothing and is counted.
     The epoch stays open.
   - A resync row naming one is left out of the bracket and counted. An order row's id still
     counts toward the paging rule's new ids (0132 item 5).
   - A frame or row on a held symbol whose price is off the grid or whose size is off the step
     is still refused: a live frame is a decode error with the epoch open, and a resync row asks
     to reconnect.
3. **An order whose end came before a resync ended.** An order the codec placed on an earlier
   connection epoch, and saw end before the current epoch's resync pushed `ResyncEnd`, still
   counts for that resync and for each later one until one pushes `ResyncEnd`. Its send time
   bounds the trades read's start time, and a trade row naming it is replayed as ours. So 0132
   item 5's "the oldest order it placed and has not seen end" reads as including such an order:
   its missed fill is not lost when it ends, by a cancel the consumer sends on reconnecting or
   otherwise, before a resync has read the trades. An order placed and ended on an epoch whose
   resync had already ended is forgotten as before, since that epoch's own stream carried its
   fills.

## Alternatives

- **Read 0134 item 5 as written, so no err answers the arm:** rejected. 0132 item 1 and 0134 item
  7 both name the rejection, and an arm QFEX refuses would then wait for its deadline.
- **Refuse what the spec table does not hold:** rejected. The primary account's trade socket
  carried eleven flat `position_response` frames on such a symbol (FBC-291a's capture), so stage
  one's sessions, and any consumer that trades fewer markets than the account has rows for,
  could never finish a resync.

## Consequences

- **The proof.** `crates/venues/fbc-venue-qfex/tests/probe_settle.rs` shows an err echoing the
  arm rejecting it under every code but `RateLimited`, which holds for its hint and settles
  nothing. `tests/probe_resync.rs` and `tests/probe_settle.rs` show rows and frames on a symbol
  the table does not hold left out and counted, and off-grid or off-step ones on a held symbol
  refused. `tests/probe_resync.rs` shows an order ended live before its epoch's resync ends, and
  one ended while a resync was left unfinished, still bounding the next trades read and having
  its missed fill replayed, and such an order forgotten once a resync has ended.
- **What production sees.** As 0143 item 4 says, what FBC-jxau lands under `preprod_probe` it
  lands under `production_probe` too. A production stage-one session now has its arm accepted
  and runs the resync's reads: the signed `GET /user/positions` and the first `get_user_orders`
  page, sent together, then any later pages. It sends `get_user_trades` only once the codec has
  a start time, which stage one, placing nothing, never has. Places stay refused there (0145).

## What would show this was wrong

- QFEX answering an arm it accepted with an err echoing it, or an arm it refused with an ack.
- A position or order on a market the consumer trades left out because its symbol was missing
  from the spec table it gave.
- A fill of an order this codec placed, made while no connection was open, and missing from
  every resync bracket after it.
