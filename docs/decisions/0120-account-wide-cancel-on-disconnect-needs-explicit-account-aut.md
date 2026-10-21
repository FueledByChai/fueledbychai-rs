# 0120 — Account-wide cancel-on-disconnect needs explicit account authority and rearms each connection

Status: accepted
Date: 2026-10-20

## Context

QFEX's [cancel-on-disconnect specification](https://docs.qfex.com/websocket/channels/trade/cancel_on_disconnect)
says an armed trade WebSocket disconnect cancels every open order on the account,
including orders placed through other connections after the arm. `PerConnection` in 0080
covers only orders placed on that connection, plus those already open when armed if
`covers_open_orders` is true. It cannot state QFEX's continuing account-wide effect.
The current execution session arms protection automatically after authentication (0058),
before the consumer can inspect a fresh resync. Another process may be using the same
account; neither an fbc-oms market lease nor an account nonce lease proves that it is not.
This record governs FBC-m63n and augments 0015, 0058, 0080 and 0097.

## Decision

1. `CancelOnDisconnect::AccountWide` means that, while this connection is armed, its
   disconnect cancels all account orders: present at arm and opened later through any
   connection. Its arm lapses on disconnect and must be accepted anew on every epoch.
   `PerConnection` retains exactly its existing meaning; no venue is upgraded by inference.
2. An `ExecSession` for this cap requires the consumer to provide an explicit
   account-bound `AccountWideCodAuthority` in its configuration. Its absence or an account
   mismatch refuses construction before any endpoint is planned or opened. This value is
   the consumer's assertion that account-wide cancellation is authorized; the library
   does not infer it from a local lease or claim exclusive ownership of the account.
3. The session still waits for final arm acceptance and resync before place or amend.
   Because the accepted account-wide arm covers all open and future orders, 0080's
   unprotected-order quarantine does not apply to its snapshots. A rejected or unknown
   arm ends the epoch; reconnect sends a new arm and never re-sends orders.
4. SimVenue models same-account orders from another connection separately from injected
   public orders. Its armed account-wide connection's close cancels those orders too,
   including ones opened after arming. Disarm and reconnect remove the protection.

## Alternatives

- Call QFEX `PerConnection { covers_open_orders: true }`: rejected because it says
  nothing about orders opened later on other connections.
- Infer account authority from credentials or a local `AccountLease`: rejected because
  those do not prove the absence of other API clients or authorize canceling their orders.
- Give QFEX an account-wide cap but continue automatic arming: rejected because merely
  constructing a session would gain a cross-process cancellation effect.

## Consequences

QFEX order entry can declare its actual protection scope once its private adapter and
acceptance evidence exist. The consumer must make and audit the account-wide authority
decision for each account. FBC-lqa8 remains Readiness::Record until its UAT and human
review; this generic model does not establish QFEX live behavior by itself.

## What would show this was wrong

Provider evidence that QFEX's disconnection cancels only the armed connection's orders,
or a safe session that arms account-wide cancellation without explicit matching authority.
