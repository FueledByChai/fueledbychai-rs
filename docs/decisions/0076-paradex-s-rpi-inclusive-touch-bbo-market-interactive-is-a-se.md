# 0076 — Paradex's RPI-inclusive touch bbo.{market}.interactive is a second touch source, one touch source per market per connection

Status: accepted
Date: 2026-10-07

## Context

FBC-taxd. The owner asked on 2026-10-07: "is there an 'interactive' for BBO as well? that would
include RPI". The adapter subscribed only `bbo.{market}`, the public book's touch, though decision
0074's book channels already include the interactive (RPI-inclusive) book. A probe of the public
SBE 1:1 socket (`wss://ws.api.prod.paradex.trade/v1?sbeSchemaId=1&sbeSchemaVersion=1`, public
channels only) on 2026-10-07 found:

- `bbo.BTC-USD-PERP.interactive` is acknowledged (the reply's `result.channel` is that name) and
  streams `BboEvent` frames, template 2, the same template as `bbo.{market}`, about 11.5 a second.
- `bbo.interactive.BTC-USD-PERP` and `bbo.BTC-USD-PERP@interactive` are acknowledged too but
  stream nothing; `bbo_interactive.BTC-USD-PERP`, `interactive_bbo.BTC-USD-PERP` and
  `rpi_bbo.BTC-USD-PERP` are refused as an invalid channel. So the exact spelling matters, and an
  acknowledgement alone does not prove a channel is the right one.
- The REST twin is `GET /v1/bbo/{market}/interactive`.

Two captures (BTC-USD-PERP, 2026-10-08 UTC, kept in `fixtures/paradex/md/`), one channel each and
taken one after the other, not side by side: `bbo.BTC-USD-PERP.interactive` (299 frames in about
25 s, seq 7687292177 to 7687292528) and `bbo.BTC-USD-PERP` (299 frames in about 12 s, seq
7687292570 to 7687292898). Both are the same `BboEvent`, uncrossed in every frame, and their seqs
run on in one increasing series, as the orderbook sequence would; neither steps by one (bbo
publishes on best-price changes, not on every book change). The interactive touch's median
spread was about 2.35 bps, the plain one's about 3.62 bps.

A `BboEvent` names its market, not its channel. Two touch channels of one market on one
connection could not be told apart, as two book channels could not (decisions 0022, 0074).
Whether the venue lets `bbo.{market}` and `bbo.{market}.interactive` share an SBE session, or
lets the interactive touch share one with a book channel or another market's interactive touch,
has not been tried (FBC-yka8).

## Decision

1. **The interactive touch is a second touch source**, `md::BBO_INTERACTIVE`
   (`TouchSourceId(1)`), subscribed as exactly `bbo.{market}.interactive`. Its caps entry,
   `touch_sources[1]`, is named `bbo.interactive`, shows the public and RPI order channels (as the
   `interactive_deltas` book does), and otherwise declares what the plain bbo does: realtime,
   publish timestamps, and the orderbook sequence (`SharedWithBook`). Its frames decode into
   `MdEvent::Touch { source: BBO_INTERACTIVE }`. `md::BBO` (`TouchSourceId(0)`, `bbo.{market}`,
   public only) is unchanged.
2. **One touch source per market per connection**, as one book channel per market (0022 item 4,
   0074 Decision 3). `plan_md` puts each touch source on the connection of its own index,
   whatever else is subscribed: `BBO` on the first (`MD_STREAM`, with the trades, mark, funding
   and each market's first book channel, as before), `BBO_INTERACTIVE` on the second (with each
   market's second book channel, when there is one). A connection with nothing to carry is not
   planned. So a change of the desired set never moves a touch source between connections
   (Codex r4214305885 on PR #118: an ordinal split moved the interactive touch off the first
   connection once the plain one was added, and the first connection's codec, still holding the
   interactive touch, refused the plain one). The codec's `subscribe` refuses a second touch
   source of a market locally, sending nothing, even after the first is removed. A bbo frame is
   the touch of the source its market holds on the connection (`BBO` where it holds none, as
   before). `ConnTopology::SharedOneBookPerInstrument` says so for touch sources where a venue
   declares more than one.
3. **The REST twin is not built.** Nothing asks for a REST touch yet.
4. **md_watch's `--paradex-touch bbo|interactive|both`** chooses the Paradex touch it prints;
   `bbo` stays the default, and the interactive touch prints as `bbo.interactive`.

## Alternatives

- Put both touch channels of a market on one connection and tell their frames apart by price or
  size: rejected. Nothing in a frame says which channel it is on, and at a quiet moment the two
  touches are equal.
- Replace the plain bbo with the interactive one: rejected. The owner asked for a feed beside it,
  and a consumer that quotes the public book needs the public touch.
- Spell the channel `bbo.interactive.{market}`, which the venue acknowledges: rejected. It streams
  nothing.

## Consequences

- The interactive touch always costs a second connection, even alone: a consumer that asks only
  for it and the trades opens two. A market's second book channel shares that connection.
- Book channels keep 0022's ordinal split, which can move a book channel between connections
  the same way when the desired set changes; FBC-xpah (follow-up) is that.
- The chaiwala-rs recorder can record the interactive touch once it takes a fueledbychai-rs tag
  with this change.
- `TouchSourceId(1)` is no longer an undeclared Paradex touch source; tests that used it as one use
  `TouchSourceId(2)`.

## What would show this was wrong

- The venue streaming nothing on `bbo.{market}.interactive`, or a capture of it whose seq is not
  in the orderbook sequence of the market's books.
- A `BboEvent` of the interactive channel whose touch shows less than the `interactive_deltas`
  book at the same seq, which would make "shows the RPI order channel" false.
- A Paradex frame that names its channel, which would let the two touches share a connection.
- The venue refusing `bbo.{market}.interactive` beside the market's book channel, or beside
  another market's interactive touch, on one SBE session, which would make `plan_md`'s sharing
  of a connection fail. FBC-yka8 is the read-only probe that settles it.
