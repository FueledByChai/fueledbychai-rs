# 0064 — A remaining-quantity amend counts its wire quantity in full until the venue settles it

Status: accepted
Date: 2026-10-07

## Context

On a venue whose amend states the quantity still to fill (`AmendQty::Remaining`), 0014 item 5
has `AmendOrder::wire_qty` send the new total less the fills the OMS's record held when it built
the amend. Fills the OMS has not seen can reach the venue first: they come out of the old
resting quantity, and the venue then rests the whole wire quantity on top of them. In the
example on PR #8 (Codex r4172303663), 4 lots filled, the total amended to 10 sends 6; 2 more
fill first, and the venue rests 6 with 6 filled, a total of 12. An OMS that recomputes resting
as `total - filled` after seeing the 2 fills counts 4 while 6 rest. 0014 item 5 already says
the OMS counts such an amend's resting as the wire quantity it sent until the venue reports
the order's total (FBC-w5n); FBC-2e4 refused every such amend until it was built
(`AmendRefusal::RemainingQty`, FBC-b0z9). No venue in the workspace declares `Remaining` today
(Paradex states the total, 0054), so this lands before any venue that does, and before the
planner emits amends (FBC-0j3).

The inventory cap (0005's I6, 0052) needs more than the resting count. Before the amend
applies, unseen fills may take all but the last lot of what rests, and the venue then rests
the whole wire quantity: the order may add to the position what rests now and the wire
quantity together, not the larger of the two.

## Decision

`fbc-oms` builds amends on a `Remaining` venue and records each one's wire quantity (built,
in flight, or replaced in flight before anything confirmed it). Until the venue settles it:

1. The order's resting quantity (`OrderRecord::resting`, for the resting cap) is at least the
   largest such wire quantity, whatever fills arrive meanwhile.
2. What the order may add to the position (`OrderRecord::exposure`, for the inventory cap) is
   what it may add without them plus every such wire quantity; an amend is built only when
   that, with the rest of the side and the position, stays within the cap.
3. A `Remaining` amend's own target total is not counted as a total the venue may hold, and
   fills never complete the order while one is unsettled; the venue's terminal event does.

The amend is settled as any amend is (0005, design §4.9), except that its total is the
venue's to state: an amended update tied to it by a later venue ordering key confirms it at
the total it states, or, stating none, at the record's filled quantity (already raised to the
update's cumulative fill) plus the wire quantity, so later fills reduce the count from there;
an amended update stating a total whose remainder at its own cumulative fill is the wire
quantity confirms it too. An update that is not an amended one never confirms it. A refusal of
it, a later-keyed total stated with nothing in flight, or the order's end retires its wire
quantity, as they retire an amend's total today.

## Alternatives

- Keep refusing every amend on a `Remaining` venue (FBC-2e4's interim): safe, but a venue that
  amends by remaining quantity could then only cancel and replace.
- Count the larger of the old resting quantity and the wire quantity for the inventory cap,
  as the resting cap does: undercounts I6 when unseen fills precede the amend (old resting 6,
  wire 6: 5 unseen fills and the amend's 6 move the position by 11, not 6).
- Refuse amends of partly filled orders: does not close the race, since the unseen fill can
  be the first.

## Consequences

An amend on a `Remaining` venue costs up to twice its quantity of inventory-cap headroom until
the venue confirms it, so near the cap such amends are refused where an amend stating the
total would be built. An amended update's cumulative fill becomes load-bearing on such a venue:
a codec that turns an RPC reply into the amended event (`AmendAck::RpcReplyOnly`) must report
the venue's cumulative fill, never a guess, or state the total.

## What would show this was wrong

A sequence of fills and amend acknowledgements, in any order the venue and the feeds can
deliver them, after which the OMS counts less resting on a side than the venue rests, or the
venue's position plus its resting orders on a side exceeds the inventory cap that admitted
them.
