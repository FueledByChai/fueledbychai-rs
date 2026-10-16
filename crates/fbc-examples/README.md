# fbc-examples: owner-run sample programs

Each program here is an example target the owner runs by hand. CI builds every one and tests its
wiring against the conformance kit's stub server; nothing in CI runs one against a venue. The
library target is empty. `cargo run -p fbc-examples --example <name> -- --help` lists each
program's flags.

| Program | What it does |
| --- | --- |
| `md_watch` | Prints live Paradex and Binance USD-M touches and books through fbc-runtime. No orders, no credentials. |
| `testnet_trade` | Paradex TESTNET only: places one post-only order well away from the touch through fbc-oms, cancels it, and Stops (FBC-x69b). |
| `testnet_quote` | Paradex TESTNET only, a plumbing test, not a strategy: keeps one post-only order a side a fixed offset behind the touch through fbc-oms's execution planner, cancel-and-replace only, until Ctrl-C, `--duration` or `--cycles`, then Stops (FBC-elg7). |
| `paradex_testnet_acceptance` | Paradex TESTNET only: FBC-8xr's acceptance run. It writes the evidence report that the snapshot-trust decision (FBC-2f7w) cites (FBC-xji8). |
| `mainnet_smoke` | Paradex MAINNET, real money: testnet_trade's round trip (one post-only order of about $11 well away from the touch, its cancel, Stop) on a separate small account, behind `--mainnet` and `--sole-trader` (FBC-p0aa). It refuses before anything connects until FBC-2f7w's record makes Paradex's snapshot source trustworthy. See below. |

## mainnet_smoke

The owner's first Paradex mainnet order (owner answer "Library smoke first"). Run it only once
FBC-2f7w's decision record is accepted and on main: until Paradex's adapter declares its snapshot
source trustworthy, the program refuses before anything connects, because a mainnet market is
armed only after a trustworthy resync and is never seeded by hand.

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
    --min-notional <min_notional> --resting-cap-usd 11 --inventory-cap-usd 50 --order-usd 11 \
    --side buy --away-bps 300
```

Read the market's `price_tick_size`, `order_size_increment` and `min_notional` from
`GET https://api.prod.paradex.trade/v1/markets`. The program checks that `GET /system/config`
names `PRIVATE_SN_PARACLEAR_MAINNET`, reads the mark price from `GET /markets/summary`, and refuses
an order whose size at the mark is below the minimum notional (Paradex judges it there). Caps above
$11 resting per side or $50 inventory are refused. The $11 resting cap is judged at the order's own
price (the notional a fill costs), not at the mark. `--help` lists every flag and output line.

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
