# fbc-examples: owner-run sample programs

Each program here is an example target the owner runs by hand. CI builds every one and tests its
wiring against the conformance kit's stub server; nothing in CI runs one against a venue. The
library target is empty. `cargo run -p fbc-examples --example <name> -- --help` lists each
program's flags.

| Program | What it does |
| --- | --- |
| `md_watch` | Prints live Paradex and Binance USD-M touches and books through fbc-runtime. No orders, no credentials. |
| `testnet_trade` | Paradex TESTNET only: places one post-only order well away from the touch through fbc-oms, cancels it, and Stops (FBC-x69b). |
| `paradex_testnet_acceptance` | Paradex TESTNET only: FBC-8xr's acceptance run. It writes the evidence report that the snapshot-trust decision (FBC-2f7w) cites (FBC-xji8). |

## paradex_testnet_acceptance

This program runs FBC-8xr's script on Paradex testnet through fbc-oms. Each command type runs
once: place, amend, cancel, batch create, cancel-many, an IOC that expires, and
cancel-on-disconnect twice (once on a dropped socket, once on a graceful close). Every order is
tiny (`--order-usd`, at most the resting cap per side). Every order is post-only, except the
IOC, which is priced where it cannot fill. Every order rests at least `--away-bps` behind a
touch read just before the order is built.

After each step, the program reads GET /orders and GET /positions over REST and compares them
with what the order stream showed. It also reads each new order's
GET /orders-history?client_id=.

The program refuses any URL or chain id other than the testnet's before it connects. It uses
the same flags, testnet guard, lease directory and client-id namespace as `testnet_trade`, plus
three flags of its own:

- `--evidence-dir` (required)
- `--cod-wait` (default 30 s)
- `--rest-wait` (default 15 s)

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
`GET https://api.testnet.paradex.trade/v1/markets`.

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
It is measured only when a GET /orders answer read after the disconnect showed the order gone
or still listed. If none answered within `--cod-wait`, the step fails as not measured.

The evidence directory holds:

- `report.md`: the answers to FBC-2f7w's questions (a) to (e); what cancel-on-disconnect did on
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
  `scripts/privacy-check.sh` then scanned every file of the directory. Run it again after
  copying.

### Run it from the deployment box (design §13.3, the per-IP limit check)

Paradex's per-IP rate limit is counted per IP address, so this question can only be answered
from the address that will trade. Run the same command once on singapore8, with:

- the testnet credentials exported in that shell only;
- a fresh `--evidence-dir` on that machine;
- a fresh `--namespace`, or the same lease directory as your other runs.

The report's last answer lists, for each comparison, the rate-limit headers Paradex returned on
its first GET /orders. Beside them it lists what was sent since the comparison before:

- this program's own REST requests: its reads, its logins and the order-book reads;
- the WebSocket order methods: the steps' requests, each connection's cancel-on-disconnect arm,
  the Safety cancels a reconnect sends and the Stop cancels;
- the connections opened. Each connection also makes a login and two resync reads, and a login
  every refresh interval (60 s) while it stays open.

The per-IP limit covers WebSocket order methods only if the remaining count fell by more than
every one of those REST requests. If it fell by no more than them, the run says nothing about the
WebSocket methods. If Paradex returns no such header, the run says so. In either case the
question stays open, and the record names what would settle it (for example, a dedicated burst
of WebSocket methods with no REST request between two reads).

Agents never run this program, against testnet or anything else, and never touch singapore8.
