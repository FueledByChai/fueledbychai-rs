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
| `qfex_preprod_probe` | QFEX PRE-PRODUCTION, or with `--production` QFEX PRODUCTION (decision 0143): stage one of decision 0132 item 7's probe (FBC-l931). It arms cancel-on-disconnect once per account and records what answers the arm, placing nothing; with `--hold-secs` it first holds one trade connection that never arms and records each server ping and close (decision 0146). See below. |

## md_watch

Public market data only: md_watch reads no credential and sends no order. Each venue named runs
on connections of its own, and `--help` lists every flag and every kind of line. Every line
after the first is led by a UTC time: a `TOUCH`, `TRADE`, `HEALTH`, `BOOK` or `STATUS` line by
the time the runtime stamped the input that brought it (a frame's or an HTTP answer's receive
time, the instant a codec's timer fired, or, for a `stale` line, the instant the runtime's
silence alarm went off), and a
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

Only the session calendar is judged again for `MARKET`, at each flush. Its listed status is the
start read's, and holds there for the whole run in both directions:

- A contract the read finds closed stays closed: one listed `PAUSED`, or the exchange in
  maintenance, prints `MARKET hibachi <symbol> closed: listed Halted` for the whole run, even
  after Hibachi reopens it. An FX pair started during its weekend still reads closed after Sunday
  17:00 New York time, while its `TOUCH` and `BOOK` lines flow.
- A contract the read finds trading stays open, but for its calendar: a halt Hibachi lists later
  does not close it, and nor does a maintenance window published after the start read. A window
  the start read published is in the calendar, so `MARKET` reads closed through it (FBC-ord5).

The status poll re-reads the status while md_watch runs (FBC-1632, decision 0130): Hibachi's plan
holds it beside `/ws/market`, a `GET /market/exchange-info` every `--hibachi-status` seconds, and
md_watch prints each status it reports as `STATUS hibachi <symbol> <status>`, once each time it
changes. So a halt Hibachi lists later is seen there, a published maintenance window halts the
contract from its begin, and after each reopening the poll knows of it reports the contract
`halted` for `--hibachi-auction-halt` seconds, for the call auction Hibachi may run then, whose
status no source gives yet (FBC-vg2n). `MARKET` does not follow it. The poll's endpoint takes the
connection number after the socket's, and its end prints a `CLOSED` line of its own.

`COUNTS` prints what the market-data codec counted on the `/ws/market` connection (decision
0129): each kind as it is first counted, and all of them before each of that connection's
`RECONNECT` or `CLOSED` lines, `none` when it counted nothing. The defaults are
`--hibachi-rest https://data-api.hibachi.xyz`, `--hibachi-ws wss://data-api.hibachi.xyz`,
`--hibachi-timeout 5000`, `--hibachi-depth 15`, `--hibachi-retry 2000`, `--hibachi-status 60`
and `--hibachi-auction-halt 900`; the orderbook is subscribed at the contract's quote grid unless
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

## qfex_preprod_probe

Stage one of decision 0132 item 7's QFEX probe (FBC-l931): an owner-assisted run on QFEX's
PRE-PRODUCTION environment, with the credentials QFEX supplied for it, as a declared testnet run
(decision 0067), or, with `--production`, on QFEX PRODUCTION with your production keys and no
such declaration (decision 0143; below). It arms
cancel-on-disconnect once per account and records what answers the arm. It places nothing, and
it sends no command but each session's arm, its resync's reads and its own reads. FBC-9sk2's
place stages build on it now that FBC-jxau has landed; they are not decided for production. With
`--hold-secs` it first holds one trade connection that never arms, to record how QFEX keeps it
alive (the liveness hold, decision 0146; below), and a run may hold without arming.

Before anything connects, it refuses:

- any origin but the environment's own, or, for a test stub, all three loopback addresses, and
  any mix of the two (the guard of the adapter's probe mode: decision 0132 item 6, decision 0143
  item 2). Without `--production` that is QFEX's pre-production origins (the defaults:
  `wss://trade.qfex.io`, `https://api.qfex.io`, `wss://mds.qfex.io`), and any `qfex.com` host is
  refused. With it, QFEX's production origins (the defaults then: `wss://trade.qfex.com`,
  `https://api.qfex.com`, `wss://mds.qfex.com`), and any `qfex.io` host is refused;
- an account decision 0122 item 1 refuses, and a `QFEX_SUBACCOUNT_ID` that selects the primary
  account (decision 0144): when set, it must be a lowercase subaccount UUID, as QFEX writes one,
  whether or not a session arms on it;
- a missing or empty credential, and a subaccount credential with one half set;
- `--socks5` with loopback origins: the proxy would reach its own host's loopback;
- with `--arm`, a missing, zero or malformed `--registry-span`, `--ledger-max-age` or
  `--ledger-max-entries` (each required with it, with no default; below), a ledger age not longer
  than the span bound, a span bound not longer than `--connect-wait-ms`, QFEX's 60 s
  authentication deadline and the longer of `--rpc-timeout-ms` and the resync's 30 s deadline
  together, and a ledger count not above the fill ceiling; without `--arm`, any of the three;
- a run with neither `--arm` nor `--hold-secs`, a `--hold-secs` outside 1 to 86400, and a
  `--hold-account` without `--hold-secs`;
- an `--evidence-dir` that is not empty.

A refusal names the setting or the variable, never its value. Then:

1. It reads the accounts its configuration names, and no others (decision 0144): the primary
   account, and the subaccount `QFEX_SUBACCOUNT_ID` names when it is set. It sends no
   `GET /user/subaccounts`, which QFEX refused with HTTP 403 to the primary account's
   single-account key in FBC-291a's first production run; its docs suggest only an All-accounts
   key may list (decision 0144). So before each run, confirm in QFEX's app that the user has no
   subaccount the configuration does not name: the probe neither checks nor records it, and its
   report says so. `--arm subaccount` arms the subaccount `QFEX_SUBACCOUNT_ID` names.
2. Before each session, for each of those accounts, it reads `GET /user/positions` (a
   subaccount selected by `x-qfex-requested-account-id`) and one `get_user_orders` page, on a
   trade socket of that account's own that sends only its authentication and that read, and
   never arms. The subaccount is read with its own credential when that is set, and with the
   primary credential otherwise. A session starts only when every account can be read and holds
   no open order, no TWAP and only flat positions; otherwise the run stops there and writes its
   report. Decision 0132 item 7 asks only an empty page and flat positions. The run also refuses
   an account whose REST positions count any `open_orders` while its page lists none: an order
   the page does not list (no page says whether stop orders are) could be cancelled by the arm,
   and its report would confound the capture. Every read, write and close on a read socket waits
   at most `--read-wait-ms`.
3. It runs one session per `--arm`, in the order given, one after the other, so no two arms are
   ever in flight across the user. Each is fbc-runtime's `ExecSession`, unchanged, with the
   `AccountWideCodAuthority` for the account it arms on (decision 0121). The session sends the
   arm itself once QFEX has authenticated the stream and confirmed its three subscriptions.
   With the arm it asks for its resync, whose reads the adapter's probe codec sends (decision
   0132 item 5): one signed `GET /user/positions` of the session's account, on the REST origin,
   and `get_user_orders` pages on the session's own socket until a short page. It sends no
   `get_user_trades`, since the session places nothing and so has no start time. The program
   stops the session once the arm has its outcome (its answer, or `Unknown` at
   `--rpc-timeout-ms`) and the resync has ended, in either order, so the journal holds each read
   with its answer; or at the connection's end (the codec ends the epoch on a read that fails or
   outlasts the resync's 30 s deadline), at `--session-wait`, or at its registry's span bound
   (`--registry-span`), whichever comes first, always through the session's stop control.
   `--session-wait` and `--registry-span` must each be longer than `--connect-wait-ms`, QFEX's
   60 s authentication deadline and the longer of `--rpc-timeout-ms` and the resync's 30 s
   deadline together, so neither stops a session with its arm or its resync in flight.
   Its reconnect pacing allows one attempt a day, so no second attempt starts meanwhile.
   The codec answers the arm from its own echo (FBC-jxau, decision 0132 item 1): an ack
   accepts it at Final, as FBC-291a's capture of QFEX production shows, and an err rejects it
   unless its code is `RateLimited`, which answers nothing; an arm nothing answers, a
   `RateLimited` one included, ends `Unknown` at its deadline.
   Under `--production` an accepted arm opens decision 0058's gate, and the codec still refuses
   every place unsent (decision 0145).

Every request takes its origin from the guarded configuration, through the adapter's
`account_reads`, and is charged first to the one rate limiter the sessions share for the
provider user (decision 0123). A read the limiter has no room for is not sent.

**Provider rate refusals** (decision 0135 item 2, FBC-m9rj). The probe provokes no rate limit
(0135 item 1), but it reports every one it can count. For each read socket and each session, the
report and the evidence state how much the shared limiter's `counts().rejected` rose while it
ran, how many of its frames were an `err` whose `error_code` is `RateLimited`, how many an
`order_response` whose `status` is `RATE_LIMITED`, and how many were unread: kept whole by
decision 0134 item 2 (an echo of authentication, a JSON escape, a repeated member) or not JSON,
so that no code can be read from what the journal keeps. A session's frames are those its
journal kept. A read socket's, which are not journaled, are the two it reads (the
authentication reply and the page), each read as a journal would keep it, and each unread one
read once more in memory as it arrived, never written, for a refusal kept whole: the case
0135's "What would show this was wrong" names, since such a frame holds no budget (0134 item
6). A session's frames reach only its codec, so a refusal kept whole in one is not seen; its
unread count says how many such frames it had. A `RateLimited` err answering a read socket's
authentication or its page holds the shared budget, as a session's runtime does for its codec's
(decision 0126), and stops the run as any refused read does. What one met is at least the larger
of the limiter's count and the refusals its frames show: a lower bound, since each can miss one
the other sees. The REST reads' answers are not read for refusals: a 429 or 418 stops the run,
its status named where it stopped. A refusal does not fail a run by itself; a count above zero
is named in the report as provider rate-limit evidence to record under FBC-s5ox, which records a
session's refusal from its journal; a refusal met on a read socket has only its count.

**The fill ledger's bounds** (decision 0132 item 5, FBC-5hif). Each session runs on a registry
of its own (each arms its own account, and a registry is for one account), built as decision
0132 item 7 says on pre-production and with no testnet declaration on production (decision
0143), with one fill ledger kept across every session the registry serves. A ledger
forgets a fill older than its age bound or beyond its count bound, and a fill heard live that
it forgot is refused `Untimed` when a resync reads it again, which stops the probe (0132
item 7). So the three bounds are yours, with no defaults:

- `--registry-span <S>`: the longest from a registry's first session's start to its last
  session's end, 1 to 86400 seconds. A session that reaches it is stopped; stage one places
  nothing, so its stop control alone ends it, a moment after the bound (the report shows by how
  much).
- `--ledger-max-age <S>`: how long the ledger keeps a fill, 1 to 604800 seconds. It must be
  longer than `--registry-span`, so nothing a registry's sessions applied ages out before the
  last of them ends.
- `--ledger-max-entries <N>`: the most fills it keeps, 1 to 1000000. It must be above the fill
  ceiling: every order a registry's sessions may place, each at its largest size in size steps,
  since every fill moves at least one step. Stage one places nothing, so its ceiling is zero and
  any count is above it; FBC-9sk2's place stages add their orders to it.

The report and the evidence state each registry's bounds and, at each of its sessions' ends,
how long after the registry's first session's start it ended, by how much past the span bound,
and its ledger's horizon and replay counts. The bounds held when every horizon is still full
and every session ended within the age bound of its registry's first session's start: the
ledger forgot no fill, and none it applied could have aged out. A run where one did not is
incomplete.

Prerequisites, which the run cannot check (decision 0132 item 2, asserted by the
`AccountWideCodAuthority` it gives): nothing else is connected to or trades on any account of the
user. No QFEX web session, no other program, no other API client. And the user has no subaccount
the configuration does not name (decision 0144): confirm it in QFEX's app before each run, then
close that session. The accounts read must also be flat with no open order; the run checks that.

**The keys** (decision 0144). One key per account, each scoped to that account alone ("A
specific account" on QFEX's [API keys page](https://docs.qfex.com/api-reference/api-keys)), so
each can be revoked alone:

- the primary account's as `QFEX_PUBLIC_KEY` and `QFEX_SECRET_KEY`, and the subaccount's as
  `QFEX_SUBACCOUNT_PUBLIC_KEY` and `QFEX_SUBACCOUNT_SECRET_KEY`;
- each with Execute orders, View orders, View positions and View balance. `GET /user/positions`
  answers with the account's `balance` object, so a key without View balance is untested;
- never Deposit and withdraw, and no All-accounts key: the run lists no subaccounts, so it needs
  no key that reaches every account.

A subaccount with no pair of its own is read with the primary account's key. A key scoped to the
primary account is not expected to reach it: QFEX then refuses the read, and the run stops before
any session.

**On production** (`--production`, decision 0143, FBC-3prm). The owner chose on 2026-11-10 to
run stage one on QFEX production, keeping a minimal balance on the accounts it reads and arms:
QFEX supplies pre-production credentials, and a production key is refused there (HTTP 401 at
the first request of FBC-291a's first run, on pre-production).

- `--production` sets the adapter's `production_probe` mode, and `--trade-url`, `--rest-url` and
  `--mds-url` default to QFEX's production origins. A pre-production origin is then refused, and
  a production origin without `--production` is refused too, before anything connects.
- Every check above runs unchanged: the reads of the accounts the configuration names before
  every session, one session at a time, one arm each, the stop with no second attempt.
- Each registry is built with no testnet declaration (decision 0067: a consumer trading a live
  venue never makes it). Stage one seeds and arms no market, so it needs none. With none, and
  QFEX's resync seeding nothing (decision 0132 item 5), no market of the probe's can arm, so it
  can place nothing. Beneath that, the adapter's codec under `production_probe` refuses every
  place, batch place and amend unsent (decision 0145).
- `report.md`, `evidence.json` (`environment`, `exec_mode`, and each registry's `testnet_run`)
  and the fixtures' README name the environment. The fixtures are scrubbed exactly as a
  pre-production run's (decision 0009): read every file before committing it, as below.
- The keys: the same variables as on pre-production, set to production keys, one per account
  as **The keys** above says.
- If an order of ours is ever left resting, decision 0132 item 7's hand cancel is on QFEX's
  production site, outside any session and while nothing of the user is armed. Stage one places
  nothing, so it should never be needed.

```text
export QFEX_PUBLIC_KEY=...             # the primary account's key, for the environment run on
export QFEX_SECRET_KEY=...
export QFEX_SUBACCOUNT_ID=...          # the subaccount to read and arm on, lowercase
export QFEX_SUBACCOUNT_PUBLIC_KEY=...  # the key scoped to that subaccount
export QFEX_SUBACCOUNT_SECRET_KEY=...  # (both or neither; neither reads it with the primary's)
# pre-production:
cargo run -p fbc-examples --example qfex_preprod_probe -- \
    --evidence-dir "$HOME/fbc-qfex-probe/$(date -u +%Y%m%dT%H%M%SZ)" \
    --arm primary --arm subaccount \
    --registry-span <SECONDS> --ledger-max-age <SECONDS> --ledger-max-entries <N>
# production (decision 0143), with production keys:
cargo run -p fbc-examples --example qfex_preprod_probe -- --production \
    --evidence-dir "$HOME/fbc-qfex-probe/$(date -u +%Y%m%dT%H%M%SZ)" \
    --arm primary --arm subaccount \
    --registry-span <SECONDS> --ledger-max-age <SECONDS> --ledger-max-entries <N>
```

`--help` lists every flag. The evidence directory holds:

- `report.md`: the accounts as each check read them (counts only, never an id or an amount),
  how many the configuration named, and that they came from it and not from QFEX,
  and for each session whether the stream authenticated, whether the arm was written, its
  outcome, every frame QFEX sent placed against the arm (before it, in its window, after it),
  and what that shows under decision 0132 item 1. An ack echoing the arm is the shape 0132 reads
  as its Final acceptance, which the codec reports (FBC-jxau). Frames in the window that echo
  nothing of the arm are listed by kind (the resync's `{all_orders_response}` page among them),
  and an echo of the arm after the window is named late; neither, nor an empty window, is read
  as the arm's answer: a new record decides what answers the arm.
  It also names every member whose string value a fixture keeps, gives each registry's
  fill-ledger bounds, its horizon and replay counts at each session's end, and whether the
  bounds held, and states the provider rate refusals each read socket and session met, at least.
- `evidence.json`: the same, machine-readable.
- `journal/`: the sessions' journal. Credentials and account ids appear in it only as keyed
  hashes under a key the run draws and never writes. Keep it local.
- `fixtures/`: every frame QFEX sent on a session's socket, read back from the journal with
  its spans blanked, and a README. A frame that is not a top-level `ack` or `err` envelope
  echoing a request has its numbers zeroed, so no amount of the account is published. Each file
  passed the privacy check's patterns before it was written, and the run then ran
  `scripts/privacy-check.sh` over the directory, removing it when the check fails. Those
  patterns know credential and address shapes only, and string values are kept as QFEX sent
  them (but the codec's spans). So before committing, read every file for an identifying string
  (an email, a user name, a wallet address in another form, a name), starting from the members
  `report.md` names as keeping a string value; remove or blank any you find. Then commit the
  directory as `fixtures/qfex/exec-recorded/arm/`, or a production run's as
  `fixtures/qfex/exec-recorded/arm-production/` (decision 0143 item 4, so a capture from each
  environment can be compared), with `report.md`, in the PR that records the capture, and run
  the check again after copying.

If QFEX does not confirm a subscription in the shape the adapter reads, the stream is never
reported authenticated and no arm is sent. The report then says so, and the fixtures hold the
confirmations as QFEX sent them (FBC-bsou's note on FBC-l931).

The journal's sink never blocks, so a record it has no room for is dropped (decision 0006). A
session's capture is whole only when no loss the run found could hold its records: no gap a
`Degraded` marker shows, no drop the markers leave unaccounted, no record a stopped writer left
unwritten, and no end of the journal left unread. The report marks partial every session that
could have lost a record, and says why. A partial session's report concludes
nothing of what answered its arm and places none of its frames against the arm, and its fixture
files are named `<account>-<n>-partial.json`.

**The liveness hold** (decision 0146, FBC-39ks). Nothing in this repository says how QFEX's
trade WebSocket stays alive, whether QFEX closes an idle one, or how long one may live, and an
armed connection that drops cancels every open order of the account. QFEX's
[authenticate page](https://docs.qfex.com/websocket/channels/trade/authenticate) says only that
the server pings an authenticated connection from time to time. The hold measures it on one
connection that never arms.

With `--hold-secs <S>` (whole seconds, 1 to 86400, no default), the run holds one trade
connection before any session, while no other connection of the probe is open:

1. It reads every account the configuration names, as before a session (decision 0144), and
   holds only when each shows no open order and flat positions.
2. It opens one trade connection on the account `--hold-account` names: `primary` unless it
   says `subaccount` (the one `QFEX_SUBACCOUNT_ID` names). Its origin, authentication and rate
   charges are the read sockets': the adapter's `account_reads` and the shared limiter.
3. It sends its authentication and, once QFEX answers it with success, the three subscriptions a
   session sends (`order_responses`, `fills`, `positions`), and reads their confirmations, all
   within QFEX's 60 s authentication deadline. Then it sends nothing but the pongs and the close
   reply the WebSocket layer writes to each server ping and server close, each recorded on the
   shared limiter as a session records one (decision 0030). It never arms, places, cancels or
   reads, and sends no ping of its own.
4. It ends at `--hold-secs` from the connection's opening or at the owner's Ctrl-C, each with a
   close (charged as a control frame, as a session's is: when the limiter has no room, none is
   sent and the connection is dropped) and the server's echo read for at most `--read-wait-ms`;
   at a server close; at the connection's end; or at a refusal (the authentication refused, a
   confirmation missing at the deadline, an err such as `RateLimited`, which holds the shared
   budget as on a read socket; an err a journal would keep whole is read in memory as it
   arrived, for its code alone, and holds none), with a close as at its duration; whichever
   comes first. An err read while it closes, whatever ended it, is a refusal too, unless one
   already refused it. Nothing reconnects.
5. Once its connection is over, whatever ended it, it reads every named account again, as in
   step 1, each whatever another showed; the report gives both reads (the accounts table's
   `before the hold` and `after the hold` rows). A hold whose connection never opened held
   nothing and is not read after.

A hold that did not authenticate and confirm its three subscriptions, an order or fill frame on
it, an account read after it that shows an open order or a position, and Ctrl-C in a run that
also arms each stop the run, so no session starts. Ctrl-C from the hold's connection's opening
until its close and the read of the echo are over is the hold's: before the hold ended, it ends
the hold; while the hold closes, it is recorded as the owner's interrupt as it closed; either
way the reads and the report follow. At any other time Ctrl-C ends the program at once, writing
no report, as it always has.

It records each ping, pong and close it receives with its time since the connection opened, a
payload or a close reason by its length only and a close code in full (decision 0041). It counts
its data frames by kind (each kind's name read in memory from what a journal would keep of the
frame: `auth`, `subscribed`, `position_response`, `order_response`, `fill_response`, an err or
an ack, never a value), reads each for a provider rate refusal, and never keeps, prints or
writes one; an `order_response`, `stop_order_response` or `fill_response` on it is reported as
an order or fill frame. Its frames are not journaled. Its section of `report.md`, and `hold` in
`evidence.json`, state the account, the duration and the environment; that it never armed or
placed; each frame it wrote, as it recorded writing it (its authentication and three
subscriptions when nothing refused it); whether any ping came before authentication; the
intervals between the pings after it; each server close with its code and its time since the
connection opened, or that none came within the hold; how it ended; its own close, a close read
after it began one counted as the echo of it and flagged when its code is not an echo's (none,
or 1000); the replies the WebSocket layer wrote for it; its data frames by kind and any order or
fill frame; what the reads after it showed; and that a lifetime longer than the hold was not
measured. It states as conditions of the run, which the probe cannot check, that the owner
confirmed the user has no subaccount the configuration leaves out (decision 0144 item 2) and
asserts that nothing traded on any account of the user from the first read to the last. Its
provider rate refusals are reported under "the hold", beside the read sockets before it, and
those of the reads after it under "after the hold".

On production the owner runs it with no `--arm` (FBC-291a already captured the arm there), as an
owner-assisted run, under its two conditions (decision 0146 item 3): the owner confirms in
QFEX's app before the run that the user has no subaccount the configuration does not name, and
nothing else connects to or trades on any account of the user from the first read to the last.
Choose a `--hold-secs` long enough to show at least three server pings:

```text
export QFEX_PUBLIC_KEY=...             # the primary account's production key
export QFEX_SECRET_KEY=...
export QFEX_SUBACCOUNT_ID=...          # when set, read before and after the hold too
export QFEX_SUBACCOUNT_PUBLIC_KEY=...
export QFEX_SUBACCOUNT_SECRET_KEY=...
cargo run -p fbc-examples --example qfex_preprod_probe -- --production \
    --evidence-dir "$HOME/fbc-qfex-probe/$(date -u +%Y%m%dT%H%M%SZ)" \
    --hold-secs <SECONDS> --socks5 <HOST:PORT>
```

A run that only holds writes no frame: its `fixtures/` holds a README alone, and its journal
holds nothing of the hold. Read `report.md` before committing it, then commit it as
`fixtures/qfex/exec-recorded/hold-production/report.md` (a pre-production hold's as
`fixtures/qfex/exec-recorded/hold/`) beside a README that records the result (decision 0146 item
6). The result speaks for the environment it was measured on only.

The exit status is 0 only when the hold, when asked for, authenticated, had its three
subscriptions confirmed, received no order or fill frame and ended at its duration, the owner's
Ctrl-C, a server close or the connection's end, with nothing refusing it, and every account read
after it showed no open order and flat positions; every session authenticated, wrote its arm,
saw its outcome with one connection attempt and kept a whole capture; every registry's ledger bounds held; and every file
was written. Agents never run this program against QFEX, pre-production or production; its
test (`tests/qfex_preprod_probe.rs`) runs it against a QFEX-shaped conformance stub.

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
