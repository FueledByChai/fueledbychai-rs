# 0042 — A codec reports a subscription the venue refused as the feed's health Refused, naming its instrument and feed, and asks for nothing to retry it

Status: accepted
Date: 2026-10-05

## Context

FBC-jlr's Paradex codec reported a JSON-RPC subscribe error as the frame's
`DecodeError::Malformed("the venue refused a subscribe")`, the only report `MdCodec::on_frame`
had. The runtime counts a decode error and nothing more, so neither it nor the consumer
learned which subscription was dead, and the one existing per-feed report, `MdEvent::Health`
with `FeedHealth::Stale`, invites a reconnect that would resubscribe into the same refusal.
FBC-50m asks fbc-core for a way to report a refused subscription by instrument and feed, with
nothing retried. It grows the venue boundary (0003, 0014).

## Decision

- **The report.** `FeedHealth` gains `Refused`. A codec that reads the venue refusing a
  subscription pushes `MdEvent::Health { inst, feed, h: FeedHealth::Refused }` under
  `VenueMeta::NONE`, one per subscription the refused request was to carry (Paradex's
  `markets_summary` channel carries a market's mark and its funding, so both are reported), and
  the call returns `Ok`: the frame was understood.
- **Nothing retried.** The codec asks for no effect with it: no subscribe resent, no
  reconnect. Neither a snapshot nor a reconnect heals a refusal, so asking again is the
  consumer's call. The codec stops holding what it refused (its frames are not pushed or
  applied), so a new subscription sends the channel again; a refusal that answers an older
  request while a later subscribe to the same channel is unanswered names its subscription and
  changes nothing else.
- **What stays an error.** A refused unsubscribe and an error answering no request the codec
  sent are still `DecodeError::Malformed`: no subscription of the consumer's is lost.
- **The runtime.** The session hands the event to its handler as any other. Holding the
  subscription back in its reconciler across epochs, so a later epoch does not resubscribe it
  and a removal sends no unsubscribe, is FBC-oy7.

## Alternatives

- A `DecodeError` variant naming the subscription: rejected. `Err` means nothing was pushed,
  and the runtime only counts decode errors, so the handler would still not learn it; and one
  refused request can stand for several subscriptions.
- A new `MdEvent` variant: rejected. `Health` already names an instrument and a feed and says
  one feed's state changed, which a refusal is; a consumer's existing `Health` arm sees it
  without a new match arm on the boundary every codec reports through.
- Reporting it `Stale` or `Gap`: rejected. A reconnect or a snapshot is what those invite, and
  neither heals a refusal; that is the loop this ticket exists to prevent.

## Consequences

- A consumer that matches `FeedHealth` exhaustively gets a fourth arm. `fbc-book`'s books act
  on `Gap` only, so a refused book feed leaves its book as it is (empty until a snapshot, or
  gapped by its epoch's end, 0039).
- Every codec that reads a venue's subscribe refusal reports it this way; Binance USD-M's
  codec, which still returns a refused SUBSCRIBE as a decode error, follows in FBC-92n.
- Until FBC-oy7 lands, a reconnect for another reason subscribes a refused subscription once
  more per epoch; a refusal itself never causes one.

## What would show this was wrong

- A venue whose refusal is transient (a rate limit, a market not yet listed) and heals on its
  own, so a codec would need to retry it on a timer rather than leave it to the consumer.
- A consumer that needs the venue's reason (code or message) to decide whether to ask again.
