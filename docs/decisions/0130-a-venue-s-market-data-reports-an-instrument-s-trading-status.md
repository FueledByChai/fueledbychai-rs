# 0130 — A venue's market data reports an instrument's trading status as MdEvent::Status each time it changes, a scheduled halt included, and Hibachi re-reads it on a poll endpoint, augmenting 0003, 0014, 0027, 0111 and 0125

Status: accepted
Date: 2026-10-27

## Context

An instrument's `TradingStatus` reaches the library once: `InstrumentSpec::status`, a snapshot
of the discovery answer, in a `SpecTable` that a market-data codec is handed immutably. What
reads it is `fbc-oms`'s execution planner, from the `PlanVenue::status` the consumer passes it
(0111): it plans no place or amend on a market whose status refuses them and still plans the
cancels. A command the consumer authorizes through the registry directly is not checked
against status.

Hibachi's contracts change status after discovery: an FX pair "transitions to PAUSED" for
every weekend (Hibachi's FX trading docs), the exchange-level status of `GET
/market/exchange-info` is `SCHEDULED_MAINTENANCE` or `UNSCHEDULED_MAINTENANCE` during a
maintenance (the Market API spec's `ExchangeStatus`), and a contract may advertise a close
(`marketCloseTimestamp`, which the spec leaves undescribed and unitless, and which the codec reads
as a temp market's "predefined expiration time", Hibachi's trading docs, Temp Markets; no source
shows one that is not null). `/ws/market` carries no status topic (the SDK's
`WebSocketSubscriptionTopic`), so the only source is re-reading that REST answer (Reviewer B
RB186F-3 on PR #186; FBC-1632).

No event carries a live status to the consumer. A market-data codec pushes only `MdEvent`s
through its `MdSink`, and none of them carries an instrument's status: `Health` is a feed's
continuity, not whether the market trades. `ExecEvent::Mode { scope, mode }` says what a venue
accepts, but it is an order-entry event, pushed by an `ExecCodec` of an account's session over
credentials, and Hibachi has no order entry here (BT-604); a market-data codec cannot push it,
and a public status is not an account's mode. A path beside the sink (a callback, a shared
cell the consumer reads) would be a second way for a codec to reach the consumer, outside the
stamping, the ingest order and the journal's replay (0006, 0014).

Some halts are known before they start: a published maintenance window's begin, the end of an
FX session in the contract's calendar (0125), an advertised close. Polling alone would report
each up to one interval late. A codec owns no clock (0002): it learns the wall time only from
`MdCodec::on_timer`, never from `on_open` or `on_http`, and an HTTP answer arrives whenever it
comes, after any timer that fired meanwhile (0027).

## Decision

1. **The event.** `fbc-core`'s `MdEvent` gains `Status { inst, status: TradingStatus }`: the
   instrument's status as the venue's market data now shows it, reported each time it differs
   from the status last reported for it, and a halt the codec applies on the venue's own
   schedule reported the same way. It is no book event and no feed's health: `fbc-book` and
   `fbc-runtime`'s `MdBooks` change no book for it and count it as feeding none (0039),
   `fbc-sim`'s engine moves no queue for it, the conformance suite's continuity and book checks
   judge it neither a book's event nor a gap, and `md_watch` prints it as a line of its own.
   Every match on `MdEvent` with a fallback arm names it. The journal records inputs, never
   `MdEvent`s, so its format does not change; decoder replay of the HTTP answers and timer
   firings gives the same events (0006).
2. **What it means to the consumer.** The status the consumer passes `PlanVenue::status` is the
   spec's until the venue reports one, then the latest reported. A venue that reports none
   leaves the spec's snapshot standing. A codec reports a status only through this event, to
   the consumer's handler.
3. **The calendar answers when it next closes.** `SessionCalendar::next_closed(from)` gives the
   first instant at or after `from` at which some part of the calendar closes the instrument
   (`from` itself while it is closed), reading no clock, so a codec can ask the runtime for a
   timer at a session's end (0125 gave the calendar only `is_open`).
4. **Hibachi's status poll.** `plan_md` plans, beside `/ws/market`, a poll endpoint (0027) on the
   market-data REST origin carrying the same subscriptions, its stream `STATUS_STREAM`. Its
   codec sends `GET /market/exchange-info` from a timer every `hibachi.status_interval` (whole
   seconds, required, no default, zero refused), the first from a timer it asks for in
   `on_open`, and judges each answer at the wall time of a timer it asks for once the answer
   arrives. It maps each contract's status by the function discovery maps it with (only `LIVE`
   while the exchange is `NORMAL` is trading; an absent, null or undocumented status refuses the
   answer), reads the advertised open and close as discovery does, and reads
   `maintenanceWindow`'s bounds as decimal Unix seconds in a number or a string, none after
   2100-01-01 UTC. A market is `Trading` after a read only if it traded throughout the read's
   span: listed `LIVE` with the exchange `NORMAL`, no advertised open after the send nor close
   at or before the judged time, no scheduled closure meeting the span, and no halt of that
   market after the send, whether or not it changed the status reported, on a schedule or by
   another read's answer or failure. A read held back only by such a halt changes nothing, and
   every other read halts it, a market a read leaves out included; a read that fails or is refused
   halts every market until a read sent after it succeeds. Between reads the codec halts a
   market on a timer at a published window's begin, at its calendar's next close and at its
   advertised close, the earlier of those two. Nothing makes a market trading but a read. The
   windows and closes it halts on are the latest-sent judged read's; a read answered after a
   later-sent one adds its windows and the earlier close and removes nothing, since which of the
   two shows the venue later is not known.
5. **The window after a reopening, until FBC-vg2n.** No source says which status a Hibachi
   contract shows during the call auction that follows a scheduled maintenance and the FX
   reopening. Until FBC-vg2n settles it from evidence, a market stays halted for the required
   `hibachi.auction_halt` (whole seconds, no default, zero refused) after every reopening the
   codec knows of: a published window's end, a reopening of its calendar, and an advertised
   open. A maintenance the exchange status shows with no published window has no end the codec
   knows, so no window follows it; the status alone halts the market until a read shows the
   exchange `NORMAL`.

## Alternatives

- Reuse `ExecEvent::Mode`: rejected. It is an order-entry event that a market-data codec cannot
  push, and a venue whose market data is all the library reads (Hibachi today) would have no
  session to push it from.
- Report the status through `Health`: rejected. A feed's health says whether its data can be
  trusted, not whether orders are accepted; a halted market's book is still the venue's book.
- A side channel from the codec to the consumer: rejected for the reasons above (unstamped,
  outside ingest order and replay).
- A status feed in `Feed` and a capability for it (0003): not taken now. The event says what the
  venue reported; a consumer passes the spec's status until one arrives, so it needs no flag to
  read it. If a consumer must know in advance whether a venue re-reads its status, the
  capability is a new mandatory field and a record.
- Judging an answer at the latest wall time any timer gave before it arrived: rejected (FBC-1632
  notes, 2026-10-22). No timer need fire between a read's send and its answer, and an advertised
  open judged at that later time would read a read sent before the open as made after it.
- Halting only at polls: rejected; each scheduled halt would arrive up to one interval late.

## Consequences

- Every exhaustive match on `MdEvent` grows an arm; a consumer's handler sees a new event.
- A consumer that quotes on Hibachi passes each reported status into `PlanVenue::status`; a
  command it authorizes through the registry directly is still not checked against status.
- A change no schedule announces (a pair paused early, an unscheduled maintenance) reaches the
  consumer at the next read, up to one interval late.
- Hibachi's plan has a second endpoint, which takes a connection number from the consumer's
  range and one REST request per interval against the IP's limit.
- `fbc-sim` does not yet refuse an order on a market a status shows halted; the planner withholds
  places and amends there from the status the consumer passes it.

## What would show this was wrong

- Evidence (FBC-vg2n) that a call auction shows its own status, or none of these, which would
  change the window after a reopening.
- A venue whose status changes faster than a REST interval can follow, or one that streams it,
  which would want a feed and a capability for it.
- A consumer that needs, before any event, to know whether a venue reports status at all.
