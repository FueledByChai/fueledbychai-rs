# 0022 — A Paradex book's seq_no advances by one per frame, bbo shares it, and a break resyncs by reconnecting the book's stream

Status: accepted
Date: 2026-10-04

## Context

FBC-70f (BT-401, design §14 M0) decodes Paradex's order book channel, carried since 2026-09-21
as the SBE `BookEvent` (template 3) of `paradex_1_0.xml` (tradeparadex/paradex-py at
`b8248fb747e278d2167ac2f056b339a287d5ef30`). The ticket asks for every seq_no discontinuity to
be detected and resynced, and for three choices to be stated in the capability declaration:
how seq_no advances (`BookCaps::continuity`), whether bbo shares the book's sequence
(`TouchSourceCaps::seq_domain`), and the resync path. 0014 fixes the codec boundary (a failed
decode pushes nothing; `Health` names its feed; codecs ask for effects and never do IO) but
says nothing about how a codec recovers a book.

What the sources say. docs.paradex.trade names the channel
`order_book.{market_symbol}.{feed_type}@15@{refresh_rate}[@{price_tick}]`, with feed types
`snapshot`, `deltas`, `interactive` and `interactive_deltas`, refresh rates 50ms and 100ms,
update types `s` and `d`, and an integer `seq_no` it does not describe further. The schema
says `BookEvent.seq` is a "Sequence number; DELTA must be applied in order", that a SNAPSHOT
package is the full top-N levels and a DELTA package incremental updates where size 0 removes
a level; that `BboEvent.seq` is the orderbook sequence and `TradeEvent.seq` "the same counter
BboEvent.seq reports". FueledByChaiTrading's recorder (`BookEpochSequencer`, used for Paradex
through `RecordingBookEventListener`) treats a Paradex delta as in sequence exactly when its
seq is the last plus one. Neither source states the step outright. The schema also says a frame
names its market but not its channel.

## Decision

1. **Continuity is `PlusOne`** on both declared book channels, `deltas@15@50ms` and
   `interactive_deltas@15@50ms`: a snapshot anchors the sequence at its seq_no, and a delta
   applies only when its seq_no is the last plus one. A skipped seq_no, a repeat or a backwards
   one is a discontinuity. Deltas before the first snapshot are dropped without a gap.
2. **bbo shares the book's sequence** (`SeqDomain::SharedWithBook`): bbo, trades and the book
   carry the per-market orderbook sequence. The codec does not order bbo against the book by
   it; the value says the two can be ordered together.
3. **A discontinuity is resynced by reconnecting the book's stream.** The codec pushes
   `Health { inst, feed: Book(id), h: Gap }` with the offending frame's metadata, asks for
   `Effect::Reconnect { stream, reason }` once, and applies no delta of that book until a
   snapshot arrives. The runtime's reconciler subscribes the desired set once on the new epoch
   and the limiter charges the connection and the subscription (0018). The codec never writes a
   subscribe or unsubscribe frame from `on_frame`, which would bypass both. The reconnect takes
   the connection's bbo and trades with it; the channel starts again with a snapshot, so no
   REST anchor is needed (`rest_anchor: false`).
4. **At most one book channel per market and connection**, since a frame does not say which
   book channel it is on: `plan_md` puts a market's second book channel on a second connection,
   and the codec's `subscribe` refuses a second book channel of a market on its connection with
   `VenueError::UnsupportedFeed`, sending nothing. A frame's book is the one its market holds
   on the connection; a frame for a market with none is skipped. A market's book channel on a
   connection is fixed by its first book subscription there and kept after an unsubscribe, so
   a swap to another channel is refused too: the old channel's frames still in flight would
   be taken for the new one's (Codex r4176866128). A swap takes a new connection or epoch.
5. **The capability model states it** (0003): `ConnTopology` gains
   `SharedOneBookPerInstrument { max_subscriptions }`, sharing like `Shared` but with at most
   one book channel per instrument and connection, and Paradex declares it, so code reading
   `VenueCaps` sees the constraint (Codex r4176866133).

This augments 0014 with a resync path and 0003's capability model with a topology; it changes
none of 0014's items.

## Alternatives

- A codec that unsubscribes and resubscribes the book from `on_frame`: rejected, as the ticket
  recommends. The frame would bypass the reconciler (which would no longer know what the
  connection has) and the limiter's subscription charge.
- A REST `/orderbook` snapshot to re-anchor without a reconnect: not taken. It costs an HTTP
  request and a merge of a REST snapshot with in-flight deltas by seq_no, while the channel
  already sends a snapshot on subscribe; FBC-9cf adds the REST snapshot for the oracles.
- `Continuity::Unsequenced` until a recording settles the step: rejected for now. The ticket
  asks for every discontinuity to be detected, and Unsequenced detects none.

## Consequences

- A book on Paradex is invalid from a gap until the snapshot of the reconnected stream, and
  the stream's bbo and trades pause for the reconnect.
- A consumer recording both `deltas` and `interactive_deltas` of one market gets two
  connections; any number of markets share them.
- FBC-3by checks the step against a recorded session once FBC-64r has one.

## What would show this was wrong

- A recorded `deltas@15@50ms` session (FBC-64r, FBC-3by) in which consecutive BookEvent
  seq_nos skip numbers without a lost frame. The captured BTC delta of 2026-09-23 already
  hints at it: its seq_no, 7,678,386,728, is far more frames than a 50ms channel could have
  sent, so the seq may be the orderbook counter, which a coalescing channel would skip. If so,
  every frame would push a gap and a reconnect, bounded only by the limiter's 20 connections
  per second per IP; a record superseding this one would make continuity monotone only.
- A Paradex frame that names its channel, which would let book channels share a connection.
