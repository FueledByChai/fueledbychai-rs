# 0074 — Paradex's book channels are the bare order_book.{market}.deltas and .interactive_deltas, the whole book on every change, one per market per SBE session

Status: accepted
Date: 2026-10-07

## Context

FBC-yj56. The owner ran `md_watch` against Paradex mainnet on 2026-10-07 and saw `HEALTH paradex
BTC-USD-PERP deltas refused` while bbo and trades streamed. The adapter subscribed the channel
names decision 0022 took from docs.paradex.trade, `order_book.{market}.deltas@15@50ms` and
`order_book.{market}.interactive_deltas@15@50ms`. A probe of the public SBE 1:1 socket
(`wss://ws.api.prod.paradex.trade/v1?sbeSchemaId=1&sbeSchemaVersion=1`, public channels only)
found:

- `order_book.BTC-USD-PERP.deltas@15@50ms`, `...deltas@15@100ms` and
  `...interactive_deltas@15@50ms` are refused with JSON-RPC error -32600 "invalid subscribe
  request", data "no parameters expected for deltas topic." (or "... interactive_deltas
  topic.").
- The bare `order_book.BTC-USD-PERP.deltas` and `order_book.BTC-USD-PERP.interactive_deltas` are
  acknowledged (the reply's `result.channel` is the bare name) and stream `BookEvent` frames
  (template 3), about 19 a second on mainnet; testnet accepts them too.
- A second `order_book` channel of the same market on one SBE session is refused: "channel
  '...snapshot@15@100ms' cannot share an SBE session with 'order_bo...'". The interactive
  (snapshot) channels take only 100ms, 200ms or 400ms. The probe paired only channels of one
  market (BTC), and the venue's error text is cut short, so it does not show whether the rule
  is one `order_book` channel per market or one per session. Book channels of different
  markets on one SBE session have not been tried against the venue.

Two captures of the bare channels (BTC-USD-PERP, 2026-10-08 UTC, about 30 s each, kept in
`fixtures/paradex/md/`) show what they carry: each opens with a SNAPSHOT of the whole book (115
bids and 60 asks; 120 and 64), then DELTAs whose seq_no is the last plus one in every frame (289
and 299 frames, no gap), some with no levels, frames as little as 0 ms apart, and a book that is
never crossed.

## Decision

1. **The book channels are `order_book.{market}.deltas` and
   `order_book.{market}.interactive_deltas`**, with no `@{depth}@{refresh_rate}` suffix
   (`BOOK_CHANNELS`). This replaces the spellings in 0022 item 1; 0022's continuity, shared
   sequence, resync and topology items stand, and the captures bear out its `PlusOne`.
2. **Their caps say what they carry**: `max_depth: u16::MAX` (no depth limit: the whole book),
   `cadence: Realtime` (a frame per change of the sequence), `windowed: false`, `PlusOne`, no
   REST anchor. The REST `/orderbook` oracle keeps asking for depth 15 and compares it with the
   top 15 of the stream-built book.
3. **One `order_book` channel per market per connection stays as 0022 item 4 built it:
   separate connections.** `plan_md` puts a market's second book channel on a second
   connection and the codec's `subscribe` refuses a second book channel of a market locally,
   sending nothing. This was the smaller change (none): the venue's own rule now matches it.
   Book channels of different markets still share a connection, as 0022 item 4 has them
   ("any number of markets share them"). That part is untested against the venue (Context);
   FBC-2976 is the read-only probe that settles it, and a multi-market Paradex book consumer
   (the chaiwala-rs recorder redeploy) waits for it.

## Alternatives

- Keep the documented suffix and add a fallback to the bare name on refusal: rejected. The venue
  refuses every suffixed form on the SBE socket, so the fallback is the only path, and a retry
  on refusal is what 0042 rules out.
- Subscribe the interactive snapshot channels (`...snapshot@15@100ms` and the like), which do
  take parameters: not taken. They are periodic depth-15 snapshots, not deltas; the delta
  channels give a gap-checked whole book.
- Refuse the second channel locally in `plan_md` instead of planning another connection:
  rejected. The split already exists, is tested, and costs one connection per extra channel.

## Consequences

- Paradex books are the whole book. Memory and frame sizes grow with the venue's depth (frames
  up to about 3 KB in the captures), not with 15 levels.
- The chaiwala-rs recorder built on an earlier tag recorded no Paradex order book; it needs a
  fueledbychai-rs tag with this change, a pin bump and a redeploy.
- docs.paradex.trade's channel spelling is not authoritative for the SBE socket; the captured
  acknowledgements are this adapter's evidence for the names (`tests/md_live_capture.rs`).

## What would show this was wrong

- The venue refusing a bare name, or acknowledging a suffixed one on the SBE socket.
- A recording of a bare channel in which seq_no skips without a lost frame, or a SNAPSHOT that
  is not the whole book (a capped depth would make `max_depth: u16::MAX` false).
- A Paradex frame that names its channel, which would let book channels share a connection.
- The venue refusing a book channel of a second market on an SBE session that already carries
  another market's book channel (for example `order_book.ETH-USD-PERP.deltas` after
  `order_book.BTC-USD-PERP.deltas`). The rule would then be one book channel per session, and
  `plan_md` would have to give every market's book its own connection.
