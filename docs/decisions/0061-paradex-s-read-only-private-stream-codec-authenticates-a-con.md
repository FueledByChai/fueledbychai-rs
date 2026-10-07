# 0061 — Paradex's read-only private-stream codec authenticates a connection with the latest refreshed token, logs in again after any refusal, and reconnects on a refused login, auth or private channel

Status: accepted
Date: 2026-10-06

## Context

FBC-dly builds the read-only Paradex `ExecCodec` design §10.2's calibration session needs: it
reads an account's private channels and sends no order method. Its login and refresh are
FBC-mz1's `LoginCycle` (0048's record covers the auth module). Three things were the ticket's
to choose, and no record fixed them:

- **What a refreshed token is for.** Paradex's WebSocket "Authentication" page: "After the
  initial authentication, you do not need to re-authenticate your WebSocket connection for the
  lifetime of the connection." A token is needed again only by the next connection. An
  order-entry codec lives across a session's epochs (0053), but its timers and HTTP results
  reach it only for the epoch that asked (0056), so the refresh timer of an ended connection
  fires into nothing. The Java library's order socket (`ParadexOrderWebSocketClient`) takes the
  latest refreshed token from a supplier when it connects; paradex-py's client re-authenticates
  with the account's current token on reconnect, logging in again only when that token has
  expired.
- **What a refusal does.** Paradex's "Error Handling" page: on an authentication error
  (40110 Malformed, 40111 Invalid), log in again and reconnect. A private channel the venue
  refuses would leave a calibration session without its fills while reporting nothing wrong.
- **What the codec can write.** The session must not be able to send an order method.

## Decision

1. **The first connection logs in; later ones reuse the latest token.** `on_open` asks for the
   login when the codec holds no reusable token, and the login's token goes into one `auth`
   frame. The refresh timer, set after every login answer for the configured interval and
   never from the token's bytes, logs in again; a new token is not sent on the open connection
   but kept, and the next connection's `on_open` writes its `auth` frame with it at once and
   sets the refresh timer for itself.
2. **Nothing refused is reused.** A refresh that gives no token, and an `auth` frame the venue
   refuses, leave no reusable token: the next connection logs in first. A refresh that gives no
   token leaves the open connection as it is.
3. **A refusal closes the connection.** A login that gives no token while a connection waits
   on it, a refused `auth` frame and a refused private channel report the stream `Closed` and
   ask for a reconnect, the venue's code first as an `UncorrelatedError` where it sent one. A
   login that cannot be signed asks for the reconnect alone (`on_open` has no sink).
4. **Only two methods.** The codec writes frames only through a type with two variants, `auth`
   and `subscribe`; `encode` refuses every command `NotSent(Unsupported)` with no effect.

## Alternatives

- Logging in on every `on_open`: no token is ever stale, but a refreshed token is never used,
  and each reconnect waits a REST round trip before authenticating; the Java library's order
  socket does not.
- Re-sending `auth` on the open connection after each refresh: Paradex says it is not needed,
  and an error reply to an unneeded frame would drop a healthy connection.
- Reading the token's expiry to decide whether to reuse it: replay blanks the token (0028), so
  a decision made from its bytes would differ in replay.
- Reporting a refused private channel as a decode error: nothing would be pushed and the
  session would go on without the channel.

## Consequences

- A reconnect within the token's lifetime authenticates without a login; one after a long
  outage may send a lapsed token, which Paradex refuses, costing one more reconnect before the
  next connection logs in. Reconnects stay paced by the session's `ReconnectPacing`.
- A private channel Paradex refuses for good makes the session reconnect until the pacing's
  attempt budget ends it, which is visible rather than silent.
- The full codec (FBC-xvf) builds on this one: its resync and query read REST with the current
  token in a redacted header.

## What would show this was wrong

- A Paradex connection that stops delivering private data, or is closed, when the token it
  authenticated with lapses: then the open connection must re-authenticate after each refresh.
- A testnet run (FBC-8xr) in which a reused token is refused on most reconnects: then each
  connection should log in first.
