# 0072 — Paradex's factory builds the order-entry or the read-only codec as a required mode key says, on one endpoint, signing orders with the login's main key

Status: accepted
Date: 2026-10-07

## Context

FBC-xzp wires Paradex's order entry into `ParadexFactory` (design §4.7, §6 steps 2-3 and 10):
`caps()` declares the `ExecCaps` decision 0054 cites, `plan_exec` plans the order socket and
`exec_codec` builds a codec from the consumer's configuration and `Secrets`. Four things were the
ticket's to choose, and no record fixed them:

- **Which codec.** Two exist: the order-entry codec (`ParadexExec`, 0071) and the read-only
  private-stream codec (`ReadOnlyExec`, 0061) a calibration session needs (design §10.2).
  `exec_codec(cfg, creds)` takes nothing else that could choose.
- **Where the order signer comes from.** `ParadexExec::new` takes a boxed `OrderSigner` and the
  `Secrets` its login takes (`Login::new` moves the account and key out of them). The order
  signer needs the same account and key. Decision 0009 keeps every reading of a credential under
  `src/auth`; nothing there gave the order signer.
- **Which settings.** The order socket's URL, and how long an order request awaits its reply
  (`ParadexEncoder`'s `rpc_timeout`, "the consumer's configuration").
- **Which stream.** `exec_codec` gets no endpoint, so the codec's stream must be the one
  `plan_exec` plans without being told.

## Decision

1. **A required mode key.** `paradex.exec.mode` is `orders` (the order-entry codec) or
   `read-only` (the read-only codec). It has no default: a missing or other value refuses the
   codec naming the key. `caps()` is the same in both modes (decision 0054's caps): the read-only
   codec refuses every command, so it never uses what the caps allow.
2. **The order signer from src/auth.** `auth::order_signer(cfg, &creds)` builds the order signer
   from the account address and the main key, reading them without taking them and parsing them
   exactly as the login does (one shared function), with the chain id from the configuration.
   The factory then hands `creds` to the codec, whose login takes them. The factory itself reads
   no credential: it passes `Secrets` straight to src/auth's functions. Orders are signed with the
   main key, the key the login uses (the owner's rule that Paradex logs in with the main key;
   a trading subkey is FBC-2vhg's).
3. **Two order-entry settings.** `paradex.exec.url` is the order socket's URL without a query,
   `wss://`, or `ws://` to a loopback test stub only, since the socket carries the session token
   and every signed order (as src/auth refuses plain `http://` for the REST base); the adapter
   appends `sbeSchemaId=1&sbeSchemaVersion=2` (0054), while market data stays on 1:1
   under its own key. `paradex.exec.rpc.timeout` (`<n>s` or `<n>ms`, positive) is how long an
   order request awaits its reply; it is read in the `orders` mode only. Both have no default.
4. **One stream.** `plan_exec` plans one endpoint, stream 0 (`EXEC_STREAM`), in either mode;
   the codec is built writing to that stream.

## Alternatives

- Choosing the codec by the credentials given (a read-only codec when no key is handed over):
  the read-only codec's login needs the key too, and a missing key would silently downgrade an
  order session to one that refuses every order.
- Choosing it by the caps (`exec: None` for read-only): the runtime's session refuses a venue
  whose caps declare no order entry, so the read-only codec could never be built.
- Defaulting the mode to `read-only`: safe for orders, but a consumer that meant `orders` and
  left the key out would get a session that refuses every order and says nothing of why; the
  OMS caps are likewise taken from consumer configuration with no defaults. A missing key is a
  mistake to report, not to guess at.
- Reading the credentials for the signer in the factory: puts credential code outside src/auth
  (0009), and the credential placement check exists to refuse it.
- Cloning the signer out of the `Login`: the signer holds the key, and a clone of key material
  is a copy that must be zeroed on its own; parsing the key twice from `Secrets` gives two
  owners, each zeroed when dropped, with no `Clone` on key types.
- Reusing `paradex.md.url` for order entry: the two negotiate different schema versions, and a
  consumer may point them at different hosts (a test stub for one).

## Consequences

- The consumer configures `paradex.exec.url`, `paradex.exec.mode` and, for orders,
  `paradex.exec.rpc.timeout`; a Paradex session without them is refused at build, naming the key.
- `ParadexFactory` now declares order entry, so a runtime order-entry session can be built for
  Paradex. Live use still waits as 0054 and the owner's answer C to RB-olg-3 say: the snapshot
  source stays `Untrustworthy`, so no resync seeds a position and every place and amend is
  refused until a market is seeded by hand, which only a registry built for the owner-assisted
  testnet run allows (0067). No mainnet Paradex session before a new record flips it.
- The read-only codec refuses the cancel-on-disconnect arm the runtime's order-entry session
  sends on every epoch (0058), so a `read-only` codec under that session ends each epoch at its
  arm. A read-only session type is FBC-t6r's; until it lands, the `read-only` mode is for a
  consumer driving the codec itself.
- `src/auth` gains `order_signer`, so this change is on the review path (0009) and waits for the
  owner's review.

## What would show this was wrong

- FBC-8xr's testnet run showing Paradex refuses orders signed with the main key on a session the
  main key logged in (then the signer needs the subkey path, FBC-2vhg).
- A consumer needing order entry and the read-only stream for one account at once: the mode key
  would then become a session-type choice in the runtime (FBC-t6r) rather than a venue setting.
