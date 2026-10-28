# fbc-examples: owner-run sample programs

Each program here is an example target the owner runs by hand. CI builds every one and tests its
wiring against the conformance kit's stub server; nothing in CI runs one against a venue. The
library target is empty. `cargo run -p fbc-examples --example <name> -- --help` lists each
program's flags.

| Program | What it does |
| --- | --- |
| `md_watch` | Prints live Paradex, Binance USD-M and Hibachi touches and books through fbc-runtime. No orders, no credentials. See below. |
| `testnet_trade` | Paradex TESTNET only: places one post-only order well away from the touch through fbc-oms, cancels it, and Stops (FBC-x69b). |
| `testnet_quote` | A plumbing test, not a strategy: keeps one post-only order a side a fixed offset behind the touch through fbc-oms's execution planner, cancel-and-replace only, until Ctrl-C, `--duration` or `--cycles`, then Stops (FBC-elg7). Paradex TESTNET, or with `--mainnet` a supervised short session on Paradex MAINNET (FBC-tyft). See below. |
| `paradex_testnet_acceptance` | Paradex TESTNET only: FBC-8xr's acceptance run. It writes the evidence report that the snapshot-trust decision (FBC-2f7w) cites (FBC-xji8). |
| `mainnet_smoke` | Paradex MAINNET, real money: testnet_trade's round trip (one post-only order of `--order-usd`, the operator's own size, well away from the touch, its cancel, Stop) on a separate small account, behind `--mainnet` and `--sole-trader` (FBC-p0aa). It Starts only after a trustworthy resync seeded the market (Paradex's snapshot source is trustworthy, decision 0094) and the account is flat. See below. |

## md_watch

Public market data only: md_watch reads no credential and sends no order. Each venue named runs
on connections of its own, and `--help` lists every flag and every kind of line. Every line
after the first is led by a UTC time: a `TOUCH`, `TRADE`, `HEALTH` or `BOOK` line by the time
the runtime stamped the input that brought it (a frame's receive time, the instant a codec's
timer fired, or, for a `stale` line, the instant the runtime's silence alarm went off), and a
`MARKET`, `COUNTS`, `RECONNECT` or `CLOSED` line, which no input carries, by the time md_watch
printed it. Nothing in the repository runs it against a venue: its test (`tests/md_watch.rs`)
drives each venue's wiring against the conformance stub on 127.0.0.1 with hand-built frames.

```text
cargo run -p fbc-examples --example md_watch -- --paradex BTC-USD-PERP --binance BTCUSDT
cargo run -p fbc-examples --example md_watch -- --hibachi ETH/USDT-P --binance ETHUSDT
```

**Hibachi** (FBC-9o7e). `--hibachi <SYMBOL>` first reads the contract from
`GET /market/exchange-info` on `--hibachi-rest`, through `fbc-venue-hibachi`'s discovery: its
tick, step, book granularity, status and session calendar, so nothing is decoded on a grid of
md_watch's own. Then it subscribes the contract's `ask_bid_price`, `trades` and `orderbook` on
the anonymous `/ws/market` of `--hibachi-ws`, and prints the two touches apart:

- `TOUCH hibachi <symbol> ask_bid_price`: the venue's own touch, on the contract's tick. It
  carries no server timestamp and can sit frozen for long stretches, so read each line's receive
  time. A frame without both sides' sizes gives no line and is counted (`unsized_touches`); one
  that follows a touch in force also prints `HEALTH hibachi <symbol> ask_bid_price gap`, since
  that touch is no longer confirmed.
- `BOOK hibachi <symbol> orderbook`: the book's touch and, with `--top`, its best levels. The
  levels are buckets of the subscription's granularity (the contract's quote grid, which
  md_watch's first line names), so the book's touch can sit up to one bucket from the venue's.

`MARKET` says whether the contract is open. It is closed when the read lists it as anything but
trading (an FX pair `PAUSED` for its weekend, the exchange in maintenance), or while its session
calendar closes it (an FX pair from Friday 17:00 to Sunday 17:00 New York time). Whether a closed
contract's feed stays live is not known yet: a stream silent for 30 s is reported stale and
reconnected, so should Hibachi stop sending a closed contract's frames, `HEALTH hibachi <symbol>
<channel> stale` and `RECONNECT` lines would repeat about every 30 s while `MARKET` says closed.
The owner's first run over a closure (an FX pair at its weekend) tells which; a feed that falls
silent is then a ticket for md_watch to expect it.

Only the session calendar is judged again, at each flush. The listed status is the read's, made
once at the start, and holds for the whole run in both directions:

- A contract the read finds closed stays closed: one listed `PAUSED`, or the exchange in
  maintenance, prints `MARKET hibachi <symbol> closed: listed Halted` for the whole run, even
  after Hibachi reopens it. An FX pair started during its weekend still reads closed after Sunday
  17:00 New York time, while its `TOUCH` and `BOOK` lines flow.
- A contract the read finds trading stays open, but for its calendar: a halt Hibachi lists later
  is not seen, and nor is a maintenance window, which the calendar does not hold yet (FBC-ord5).

Restart md_watch to read the status again. Reading it again while running is FBC-1632's.

`COUNTS` prints what the market-data codec counted on the connection (decision 0129): each kind
as it is first counted, and all of them before every `RECONNECT` or `CLOSED` line, `none` when
it counted nothing. The defaults are `--hibachi-rest https://data-api.hibachi.xyz`,
`--hibachi-ws wss://data-api.hibachi.xyz`, `--hibachi-timeout 5000`, `--hibachi-depth 15` and
`--hibachi-retry 2000`; the orderbook is subscribed at the contract's quote grid unless
`--hibachi-granularity` names another, a whole number of its ticks, asked for in its shortest
spelling (`0.10` as `0.1`), the tick itself asking for the book on the tick.

**Does Hibachi honour the orderbook subscription's `granularity` and `depth`?** That is PR
#210's open point (FBC-2kmp; decision 0129, "What would show this was wrong"). No source shows
where the two fields go in a subscription, so the codec writes them beside `symbol` and `topic`
and holds every orderbook frame's own `granularity` and `depth` to them, before anything else
in the frame. The owner's first live run settles it, each outcome below told by its own
signature in the `hibachi` lines:

```text
cargo run -p fbc-examples --example md_watch -- --hibachi ETH/USDT-P --binance ETHUSDT --top 3 --seconds 60
```

- **Not honoured**: the book gaps on every frame. Each Snapshot comes back at the venue's own
  granularity or depth, so `HEALTH hibachi ETH/USDT-P orderbook gap` repeats about every
  `--hibachi-retry` (each resubscribe brings another such Snapshot), no `BOOK hibachi` line
  prints, and the `COUNTS` lines name `param_mismatches`, with `resubscribes` (and
  `updates_without_book`, the Updates passed over meanwhile) beside it. The fields then go
  somewhere else, or the topic takes none: a ticket of its own, under a record superseding
  0129's first item.
- **Not answered**: no Snapshot came at all, so nothing gaps the book until the codec's wait for
  one runs out. The first `HEALTH hibachi ETH/USDT-P orderbook gap` prints `--hibachi-retry`
  after a connection subscribes the orderbook, on no frame; the codec then resubscribes and,
  when that brings no Snapshot either within another `--hibachi-retry`, asks for a reconnect, so
  a `RECONNECT` line follows on every connection. The `COUNTS` lines name no orderbook counter but `resubscribes`
  and `reconnects` (no `param_mismatches`, `thin_sides`, `bounds_failures`, `off_granularity`,
  `empty_snapshots`, `malformed_books`, `null_sides`, `locked_books` or
  `updates_without_book`), and no `BOOK hibachi` line prints, while `ask_bid_price` touches can
  still print. Hibachi refused the orderbook subscription that carries the two fields (a
  refusal is a frame md_watch does not print) or ignored it: the topic does not take the fields
  where the codec writes them. As for "Not honoured", a ticket of its own under a record
  superseding 0129's first item. Any other orderbook counter means an orderbook frame did come:
  read the run against the other outcomes.
- **Honoured, or the venue's defaults happen to be what was asked**: `BOOK hibachi` lines print,
  and the closing `COUNTS` line names no `param_mismatches`. To tell the two apart, run once more
  with another depth and another granularity the contract lists, coarser than the one
  md_watch's first line names: `--hibachi-depth 5 --hibachi-granularity <G>`, `<G>` taken from
  the contract's `orderbookGranularities` in `GET /market/exchange-info` on `--hibachi-rest`. A
  book that prints both times, its levels on `<G>` the second time, cannot have come at the
  venue's defaults both times. A `<G>` the contract does not list may gap the book for that
  reason alone, so take it from the live list: the granularities of the repository's ETH/USDT-P
  fixture (0.01 to 10) are the API docs' and the SDK's public example values
  (`fixtures/hibachi/rest/README.md`), not a reading of the live contract.
- **Taken, but the book fails another check**: the Snapshots come back at the granularity and
  depth asked for, yet each gaps the book. `HEALTH hibachi ETH/USDT-P orderbook gap` prints on
  each Snapshot, about every `--hibachi-retry`, no `BOOK hibachi` line prints, and the `COUNTS`
  lines name `thin_sides`, `bounds_failures`, `off_granularity` or `empty_snapshots`, with
  `resubscribes` (and `updates_without_book`) beside it, but no `param_mismatches` and no
  `reconnects`. A frame is counted under any of those four only once its `granularity` and
  `depth` were found to be the ones asked for, so Hibachi took the fields, or its defaults happen
  to be what was asked: for the open point this reads as the outcome above, and the same second
  run tells the two apart (no `param_mismatches` either time). A run whose `BOOK hibachi` lines
  print between such gaps reads the same way. The counter named is evidence against one of
  0129's bounds assumptions ("What would show this was wrong"), for a ticket of its own.
  `thin_sides` may only mean the live book is shallower than the depth asked for: at the
  default `--hibachi-depth 15`, a side with fewer than 15 non-empty buckets. A run at a smaller
  `--hibachi-depth` (`3`, say) shows whether the side is just shallow: its `BOOK hibachi` lines
  then print. `malformed_books` beside these says less, since a frame whose `messageType` or
  `timestamp_ms` cannot be read is counted malformed before its fields are.

A run that fits none of these, one whose only orderbook counters are `malformed_books` or
`updates_without_book` and that prints no `BOOK hibachi` line, say, settles nothing: it is a
ticket of its own.

Record only the conclusion in the queue. The run's lines, prices, sizes, counts and timings stay
in the owner's private notes, as the 2026-10-22 capture's did (decisions 0001, 0009).

## mainnet_smoke

The owner's first Paradex mainnet order (owner answer "Library smoke first"). Paradex's adapter
declares its snapshot source trustworthy (decision 0094, accepted by the owner 2026-10-16), so
the resync seeds the market's position; a mainnet market is armed only after a trustworthy
resync and is never seeded by hand, so a venue declaring otherwise is refused before anything
connects. Stop sends the kill switch and an instrument cancel-all of the market
(`order.cancel_all`), never an account cancel-all.

**Positions follow fills** (the owner's condition in decision 0094, FBC-hpg6). `testnet_trade`
(so `mainnet_smoke`) and `testnet_quote` read `GET /positions` over REST, through the rate
limiter the session charges, after the first fill the session sees and at Stop once Stop's
cancels are answered (every run, fills or not, while the market's lease is still held: before
the market is disarmed and its leases released), and compare the market's size with the registry's
position exactly in lots, printing a `POSITION` line. A mismatch is read once more after
`--step-timeout`, both readings printed with their times, the venue's size, the registry's and
the fills applied: a re-read that agrees goes on (`GET /positions` lagged the fills); one that
still disagrees Stops the run (kill switch + cancel all, nothing placed after) and fails it with
"check the position by hand". The check logs in over REST on its own (`src/auth_rest.rs`, the
acceptance program's reader), so the run makes one more `POST /auth` and reads its credentials
from the environment twice.

Prerequisites:

- A SEPARATE Paradex mainnet account from the one the Java quoter trades (one quoter per
  account), funded small, with no open orders and no position on any market.
- Its Paradex (Starknet) address and its main L2 (Stark) key, in the mainnet-only variables. The
  testnet samples' variables must be unset: either one refuses the run.

```text
unset PARADEX_ACCOUNT_ADDRESS PARADEX_PRIVATE_KEY
export PARADEX_MAINNET_ACCOUNT_ADDRESS=...   # the separate mainnet account
export PARADEX_MAINNET_PRIVATE_KEY=...       # its main L2 key
cargo run -p fbc-examples --example mainnet_smoke -- --mainnet --sole-trader --namespace <N> \
    --market BTC-USD-PERP --tick <price_tick_size> --step <order_size_increment> \
    --min-notional <min_notional> --resting-cap-usd <USD> --inventory-cap-usd <USD> \
    --order-usd <USD> --side buy --away-bps 300
```

Read the market's `price_tick_size`, `order_size_increment` and `min_notional` from
`GET https://api.prod.paradex.trade/v1/markets`. The program checks that `GET /system/config`
names `PRIVATE_SN_PARACLEAR_MAINNET`, reads the mark price from `GET /markets/summary`, and refuses
an order whose size at the mark is below the minimum notional (Paradex judges it there). The caps
are yours: `--resting-cap-usd` and `--inventory-cap-usd` are required, with no defaults and no
ceiling (owner 2026-10-19, FBC-thks, decision 0113), and each must be positive; the run prints
both (the `CAPS` line) before anything connects. The resting cap is judged at the order's own
price (the notional a fill costs), not at the mark. `--help` lists every flag and output line.

**A cancel not ours** (FBC-aoh5). Paradex does not say who sent a user cancel. When the order
ends cancelled by one that the program did not send (another program on the account, such as a
Java quoter's orphan reconcile, or you in the Paradex UI), it prints a `WARN` line saying so:
one quoter per account. The round trip did not happen, so the run fails (no repeat is possible
with one order).

## testnet_quote --mainnet

The fixed-offset loop on Paradex MAINNET, real money (FBC-tyft; owner answer "Yes, small and
supervised"): a short session the owner runs and watches, on the same SEPARATE small mainnet
account as `mainnet_smoke` (its variables, its lease directory `$HOME/.fueledbychai/mainnet_smoke`).
A plumbing test of requoting, reconnects and Stop on prod before any model; it prices nothing
from a model.

Refused before anything connects unless both `--mainnet` and `--sole-trader` are given, the URLs
are exactly mainnet's, `PARADEX_CHAIN_ID` (when set) is mainnet's, both caps are given and positive
(`--resting-cap-usd` and `--inventory-cap-usd` are yours: required, no defaults, no ceiling,
owner 2026-10-19, FBC-thks, decision 0113; the `SUPERVISED` line prints them before anything
connects), both `--bid-bps` and `--ask-bps` are at least 20, `--duration` is at most 3600 s
(one hour; `MAINNET_DURATION_MAX` in `examples/testnet_quote/args.rs`), `--interval-ms` is at most 20000 (two thirds of the 30 s
window for a cancel not ours, `mainnet_interval_max`: a conservative bound kept beside the
requote rule below, which Stops a co-tenant cancelling every requote however long a cycle
takes), and the venue
declares its snapshot trustworthy. Then, before
the order socket connects, `GET /system/config` must name `PRIVATE_SN_PARACLEAR_MAINNET` and
`GET /positions` must show no position on any other market. The resync must seed the market (never
by hand on mainnet). Every cycle reads the touch and the mark price together, both charged to the
session's rate limiter, and leaves out (and prints) a side whose size at the mark is below the
minimum notional. Every change is a cancel, acknowledged and ended, before the next place: no
amend frame is ever sent.

It Stops (kill switch + instrument cancel-all, nothing placed after) on Ctrl-C, at `--duration`
or `--cycles`, on a resync mismatch, an order of ours gone Unknown, a refused login, a permanent
refusal of a place or a cancel (or an asynchronous `ORDER_SIZE_BELOW_MIN`, a sizing error), a
position on another market, a `GET /positions` mismatch the re-read confirms, and a repeated
cancel not ours (below). A request refused for its rate waits the venue's retry time (2 s when
Paradex states none). `DONE ok` only when the loop ended as asked and Stop did all it must.

**Cancels not ours** (FBC-aoh5; owner answer "Warn once, Stop on repeat", 2026-10-18). The
owner's first supervised session found a Java chaiwala instance on the same account cancelling
every order on its ticker it did not place, every 3 s, while the loop just requoted: Paradex does
not say who sent a user cancel, and the Java instance was flat with no orders at the start, so
neither the resync nor `--sole-trader` could see it. The loop now counts a cancel not ours when
an order of ours ends cancelled by a user while no cancel of ours covering it was sent (no
replace's or side's cancel, no cancel-all, no start, reconnect or Stop sweep, in flight or
answered). Those of one action of the other program's (one reconcile, one cancel-all in the UI)
are one event: two are one when Paradex stamped the second within 1 s of the event's first and
its order was placed before the event was seen; a cancel seconds later, of any order however old,
or of a quote placed after the event, is another (so both quotes cancelled by hand one at a
time, seconds apart, are two: on mainnet that Stops the run). The first prints a `WARN` line (another program on the
account, or the Paradex UI; one quoter per account) and the loop requotes. A second on a later
cycle within 30 s of the first Stops the run and fails it ("another program cancelled our
orders: one quoter per account"), which catches a 3 s reconcile within about 6 s; so does one
that cancels the loop's requote (the first order placed on a side after the event before; a
place refused, not sent or that would cross never rested, so the next place on its side is the
requote: RB-aoh5-10), or a replace of it the loop made, within 30 s of that requote's place, however long the cycle that placed it took (its reads, settle and
rate-limit waits do not count: Codex r4309154208 on PR #159); after 30 s with no second the
window starts again. A cancel of ours that Paradex refuses (for its rate, say) stops covering its
order at the refusal, so another program's cancel of it while ours waits is counted (Codex
r4309154204); when the order's cancel event comes before the refusal, it is counted at the
refusal (Codex r4310042612). Once one was seen, the reconnect sweeps and Stop cancel
our orders explicitly and never send an instrument cancel-all, which would cancel the other
program's orders too. Without `--mainnet` each only warns. `SUMMARY` ends with `cancels not
ours N`, the orders of ours they ended.

```text
unset PARADEX_ACCOUNT_ADDRESS PARADEX_PRIVATE_KEY
export PARADEX_MAINNET_ACCOUNT_ADDRESS=...   # the separate mainnet account
export PARADEX_MAINNET_PRIVATE_KEY=...       # its main L2 key
cargo run -p fbc-examples --example testnet_quote -- --mainnet --sole-trader --namespace <N> \
    --market <MARKET> --tick <price_tick_size> --step <order_size_increment> \
    --min-notional <min_notional> --resting-cap-usd <USD> --inventory-cap-usd <USD> \
    --order-usd <USD> --bid-bps <N> --ask-bps <N> --duration <SECONDS>
```

Read the market's `price_tick_size`, `order_size_increment` and `min_notional` from
`GET https://api.prod.paradex.trade/v1/markets`; the caps, the size, the offsets and the duration
are yours, within the limits above (the caps have none but being positive). `--help` lists every
flag and output line.

## paradex_testnet_acceptance

This program runs FBC-8xr's script on Paradex testnet through fbc-oms. Each command type runs
once: place, amend, cancel, batch create, cancel-many, an IOC that expires, with
`--allow-fill` the two fill steps, and cancel-on-disconnect twice (once on a dropped socket,
once on a graceful close). Every order is
tiny (`--order-usd`, at most the resting cap per side). Every order is post-only, except the
IOC, which is priced where it cannot fill. Every order rests at least `--away-bps` behind a
touch read just before the order is built.

After each step, the program reads GET /orders and GET /positions over REST and compares them
with what the order stream showed. It also reads each new order's
GET /orders-history?client_id=.

The program refuses any URL or chain id other than the testnet's before it connects. It uses
the same flags, testnet guard, lease directory and client-id namespace as `testnet_trade`, plus
four flags of its own:

- `--evidence-dir` (required)
- `--cod-wait` (default 30 s)
- `--rest-wait` (default 15 s)
- `--allow-fill` (off by default): runs the two fill steps below

### The fill steps (`--allow-fill`, FBC-6fnc)

Testnet only, and only with `--allow-fill`. Each runs only when both sides of the book have
resting liquidity, so what fills can always be flattened at once; otherwise it is skipped with
the reason. No take is sent that could pass `--inventory-cap-usd`, and every order still goes
through fbc-oms under the caps.

- `fill`: one IOC of about `--order-usd` at the far touch (the ask for `--side buy`) takes
  resting liquidity. GET /positions is compared with the fills before and after it. A
  reduce-only IOC on the other side then flattens what filled (up to three, each priced where
  the book's depth covers what is left), and the comparison runs again.
- `fill-partial`: when the far touch's level is smaller than the order, a GTC limit that is not
  post-only, at that level, fills in part and rests the rest. That order is amended once, away
  from the touch, with its size sent as the total. The report records whether Paradex read
  order.modify's size as the total or the remainder (FBC-b0z9). The order is then cancelled and
  what filled is flattened. When no such level exists, the step is not exercised and the report
  says why. Paradex declares an amend of a partly filled order refused (decision 0054); this run
  admits it for that one amend only (every other order, B1 included, is judged under Paradex's
  caps), and only when the larger reading (the whole size resting on top of what filled) still
  fits the caps.

Nothing is flattened from an inventory the stream has not settled. If an order of a fill step
(the take, the partly filled order, or a flatten IOC) does not end with every fill its order
events state within the step timeout, or GET /positions disagrees after the take, or GET
/orders or GET /positions disagrees once the partly filled order ended, the step
fails with `FAIL <step> ... flatten by hand on testnet`, no further IOC is sized, and the
phase stops. Nothing is added to an inventory REST and the stream disagree on either: when GET
/orders or GET /positions never agrees with the stream before the take, no take is sent, and
when either disagrees while the partly filled order is partly filled (it is then cancelled,
not amended) or after a flatten (a take or partly filled order whose placement failed and was
settled and flattened included), no further order is sent; the step fails and the phase stops. A step whose order-book read failed never passes, even when a later read flattened what
filled.

For decision 0092 (2) the report counts only the refusals of the fill steps' own IOC
placements (the take and the flattens), never a rejected amend or cancel; a NOT_ENOUGH_MARGIN
refusal is reported as its own case, not as that question's risk refusal. The answer follows
the IOC placements the fill steps actually sent: with `--allow-fill` and none sent (the steps
skipped, or stopped before a placement), it is not exercised.

The report lists every fill as received (fill id, side, fee and fee currency, realized P&L and
funding) and every PositionEvent.

### Run it on your machine

```text
export PARADEX_ACCOUNT_ADDRESS=...   # the testnet account (the Paradex/Starknet address)
export PARADEX_PRIVATE_KEY=...       # its main Stark key on testnet
cargo run -p fbc-examples --example paradex_testnet_acceptance -- \
    --evidence-dir "$HOME/fbc-acceptance/$(date -u +%Y%m%dT%H%M%SZ)" \
    --sole-trader --namespace <N> \
    --market BTC-USD-PERP --tick <price_tick_size> --step <order_size_increment> \
    --min-notional <min_notional> \
    --resting-cap-usd 11 --inventory-cap-usd 50 --order-usd 11 \
    --side buy --away-bps 300
```

Read the tick, size step and minimum notional from the market's entry in
`GET https://api.testnet.paradex.trade/v1/markets`. A book price or size off that grid is
reported as "not on the --tick (or --step) grid; check --tick and --step against GET /markets".

With the fill steps, on HYPE-USD-PERP (tick 0.0001, size step 0.01, minimum notional 10). Its
book is wide, so the resting cap in lots, taken at the dearer side's price, is too small for an
$11 order at `--resting-cap-usd 11`; `--resting-cap-usd 40` lets each order stay about $11
(the conversion is deferred, FBC-iaft):

```text
cargo run -p fbc-examples --example paradex_testnet_acceptance -- \
    --evidence-dir "$HOME/fbc-acceptance/$(date -u +%Y%m%dT%H%M%SZ)" \
    --sole-trader --namespace 4242 \
    --market HYPE-USD-PERP --tick 0.0001 --step 0.01 --min-notional 10 \
    --resting-cap-usd 40 --inventory-cap-usd 50 --order-usd 11 \
    --side buy --away-bps 300 --allow-fill
```

Before you run it, check two things:

- The account has no open order on the market that another program placed. The run refuses one
  it does not own.
- The account has no position or open order on any other market. The resync refuses that
  whole.

Orders this program or `testnet_trade` left open from an earlier run are cancelled as each
connection opens (decision 0080).

Ctrl-C aborts at once. The socket closes, and Paradex's cancel-on-disconnect cancels what was
resting.

The exit status is 0 only when every step passed. A cancel-on-disconnect step passes once it
has been measured, whatever Paradex did: the report records whether the order was cancelled.
It is measured only when a GET /orders answer read after the disconnect showed the order gone,
or the last read of the wait answered and still listed it. Every read of the wait is recorded.
If none answered within `--cod-wait`, or a read after a listing failed, the step fails as not
measured: an early listing never stands for the whole wait.

A placement is recorded only once its final phase came on an order event (decision 0092), the
reply's provisional acceptance not enough; a step whose placement got none within
`--step-timeout` fails. A GET /orders answer naming a further page (a non-null `next`) is never
agreement: the program reads only the first page. The IOC step passes only when
GET /orders-history listed the IOC CLOSED with a cancel reason; otherwise it fails, and the
report's 0092 answer says not measured.

The evidence directory holds:

- `report.md`: the answers to FBC-2f7w's questions (a) to (e) (with three limits the report
  states: (b) gives no bound when any comparison never agreed; (c) is no evidence about fills
  when no fill event arrived, since each phase seeds its position from REST; and (e)'s two
  disconnect histories are timed from the socket's end, so they include cancel-on-disconnect's
  latency and the GET /orders wait); what cancel-on-disconnect did on
  a dropped socket and on a graceful close (FBC-gkdz); the IOC's cancel reason (decision 0092);
  request_info persistence and the requestId echo (FBC-g3bw); and the items the run does not
  exercise. For each step it then gives what was sent, every outcome, each order's states in the
  order lattice, and the REST-versus-stream table. It ends with every order event as received.
- `evidence.json`: the same, machine-readable.
- `journal/`: the run's journal. Credentials appear in it only as keyed hashes under a key the
  run draws and never writes. Keep it local.
- `fixtures/`: the scrubbed slice of the journal, with its README. Commit it as
  `fixtures/paradex/exec-recorded/`, together with `report.md`, in FBC-8xr's PR. Each file
  passed the privacy check's patterns before it was written, and the repository's
  `scripts/privacy-check.sh` then scanned every file of the directory. `report.md` and
  `evidence.json` pass the same patterns before they are written: one they find something in is
  withheld, a note naming what was found written in its place, and the run fails. Run the
  check again after copying.

### Run it from the deployment box (design §13.3, the per-IP limit check)

Paradex's per-IP rate limit is counted per IP address, so this question can only be answered
from the address that will trade. Run the same command once on singapore8, with:

- the testnet credentials exported in that shell only;
- a fresh `--evidence-dir` on that machine;
- a fresh `--namespace`, or the same lease directory as your other runs.

The report's last answer lists, for each comparison, the rate-limit headers Paradex returned on
its first GET /orders. Beside them it lists what was sent since the comparison before:

- this program's own REST requests: its reads, its logins and the order-book reads;
- the REST requests the order sessions asked for (each login, resync read and token refresh),
  counted from what their journal records them sending, and how many of those were never sent
  (refused by the limiter or failed before a byte was written);
- the WebSocket order methods: the steps' requests, each connection's cancel-on-disconnect arm,
  the Safety cancels a reconnect sends and the Stop cancels;
- the connections the sessions opened, reconnects included.

Every request the program sends, its own REST reads and logins and the order-book reads included,
is charged to the same rate limiter as the order sessions. A read waits for room in that limiter,
and if no room comes before the read's wait ends, it fails without being sent.

The per-IP limit covers WebSocket order methods only if the remaining count fell by more than
every one of those REST requests, the program's and the sessions'. If it fell by no more than them, the run says nothing about the
WebSocket methods. If Paradex returns no such header, the run says so. In either case the
question stays open, and the record names what would settle it (for example, a dedicated burst
of WebSocket methods with no REST request between two reads).

Agents never run this program, against testnet or anything else, and never touch singapore8.
