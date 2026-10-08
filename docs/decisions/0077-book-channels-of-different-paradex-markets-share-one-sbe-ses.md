# 0077 — Book channels of different Paradex markets share one SBE session, as a captured production session shows, augmenting 0022 and 0074

Status: accepted
Date: 2026-10-07

## Context

FBC-2976, from Reviewer B's RB-yj56-1 on PR #116. Decision 0074 found that Paradex refuses a
second `order_book` channel of one market on an SBE session. Its probe paired only BTC channels,
and the venue's error text is cut short, so 0074 left open whether the rule is one book channel
per market or one per session. 0022 item 4, `plan_md` and `tests/replay.rs` put book channels of
different markets on one connection. 0074 Decision 3 called that untested and named FBC-2976 as
the probe that settles it. The multi-market chaiwala-rs recorder redeploy waits on the answer.

The coordinator ran the probe on 2026-10-08, from 02:53:08 UTC. It used one session of the
public production socket, `wss://ws.api.prod.paradex.trade/v1?sbeSchemaId=1&sbeSchemaVersion=1`,
with public channels only. It sent three subscribes in turn:

- id 1, `order_book.BTC-USD-PERP.deltas`
- id 2, `order_book.ETH-USD-PERP.deltas`
- id 3, `order_book.SOL-USD-PERP.interactive_deltas`

The venue acknowledged all three. Each reply's `result.channel` is the channel its request named,
and no error came back. In about 15 s the session carried 89 BTC, 63 ETH and 3 SOL `BookEvent`
frames (template 3). Each frame names its market, and each market's frames open with a snapshot
of its whole book and run gap-free from there. The capture is kept in `fixtures/paradex/md/`.

Two smaller faults in 0074's text came from the same review (RB-yj56-2, RB-yj56-3):

- **The frame rate.** 0074's Context says the bare channels stream "about 19 a second on mainnet",
  over captures of "about 30 s each". No committed capture shows 19 a second. The two committed
  captures run 289 frames in 30.1 s (about 9.6 a second, `deltas`) and 299 frames in 23.9 s
  (about 12.5 a second, `interactive_deltas`).
- **The pointers to 0022.** 0074 Decision 1 replaces the channel spellings of 0022 item 1, but
  0074's title, and so its index line, does not name 0022. 0074 Decision 3 also cites "any
  number of markets share them" as "0022 item 4". That sentence is in 0022's Consequences; item 4
  is the one-book-channel-per-market rule.

Records are never edited in place, so 0074 stays as merged and this record carries the
corrections.

## Decision

1. **The venue's rule is one `order_book` channel per market per SBE session.** Book channels of
   different markets share a session, whichever book channel each market holds. 0022 item 4 and
   0074 Decision 3 stand as built. `plan_md` is unchanged: it puts each market's first book
   channel on the first connection and a market's second book channel on another. 0074's open
   question is closed, and a multi-market Paradex book consumer no longer waits on FBC-2976.
2. **Read 0074's Context with the fixtures' rates.** The bare channels ran about 9.6 frames a
   second (`deltas`, 30.1 s) and about 12.5 a second (`interactive_deltas`, 23.9 s) in the
   committed captures. The "about 19 a second" figure has no committed capture behind it.
3. **Read 0074's pointers to 0022 this way.** 0074 Decision 1 replaces the channel spellings in
   0022 item 1 (`deltas@15@50ms`, `interactive_deltas@15@50ms`) with the bare
   `order_book.{market}.deltas` and `order_book.{market}.interactive_deltas`. 0022's other items
   stand. "Any number of markets share them" is from 0022's Consequences; 0022 item 4 is the
   one-book-channel-per-market rule. This record's title names 0022 and 0074 so that the index
   points from 0022's number to the names now in force.

## Alternatives

- Correct 0074 in place: rejected. `docs/decisions/README.md` and AGENTS.md forbid editing an
  accepted record, as Codex held for 0065 on PR #117 (decision 0075).
- Give every market's book its own connection anyway, in case the venue tightens its rule:
  rejected. That costs one connection per market, and the venue has now accepted the shared
  form. The falsifier below names the evidence that would reopen it.

## Consequences

- A consumer recording one book channel for each of N Paradex markets uses one connection for
  them, plus one more for any market's second channel. The chaiwala-rs recorder can take its
  multi-market Paradex books on this layout once its pin is bumped.
- `tests/md_live_multimarket.rs` replays the capture through the codec. It checks that the plan
  is one connection, that each acknowledgement answers its own subscribe, and that each market's
  frames reach only its own book, gap-free and uncrossed after every frame.
- The probe tried three markets. No capture shows how many book channels one session takes.
  Paradex declares no cap (`max_subscriptions: None`), because docs.paradex.trade states none.

## What would show this was wrong

- The venue refusing a book channel of a market on a session that already carries another
  market's book channel, or acknowledging it and then not streaming it.
- A frame on a shared session carrying levels of a market other than the one it names.
- A refusal or a silent drop once a session carries more than some number of book channels. A cap
  would go into `max_subscriptions`, and `plan_md` would open another connection beyond it.
