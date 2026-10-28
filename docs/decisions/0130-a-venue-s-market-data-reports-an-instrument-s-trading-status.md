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
   leaves the spec's snapshot standing, and its capabilities say so (point 6). A codec reports a
   status only through this event, to the consumer's handler.
3. **The calendar answers when it next closes.** `SessionCalendar::next_closed(from)` gives the
   first instant at or after `from` at which some part of the calendar closes the instrument
   (`from` itself while it is closed), reading no clock, so a codec can ask the runtime for a
   timer at a session's end (0125 gave the calendar only `is_open`). It never gives an instant
   `is_open` holds open: a weekly occurrence wholly inside a skipped daylight-saving stretch
   closes nothing (0125), so it is passed over for the next that closes something.
4. **Hibachi's status poll.** `plan_md` plans, beside `/ws/market`, a poll endpoint (0027) on the
   market-data REST origin carrying the same subscriptions, its stream `STATUS_STREAM`. It plans it
   for every set, alone and carrying none for the empty one, on the same stream and transport, so
   the runtime's `MdVenue`, which keeps an endpoint planned again on the same stream and transport,
   keeps it and its codec open while the venue's market data runs; with no market subscribed the
   codec sends no read. A codec of a newly opened endpoint (the venue's market data started again)
   knows nothing an earlier one read or held. The codec sends `GET /market/exchange-info` from a
   timer every `hibachi.status_interval` (whole seconds, required, no default, zero refused), the
   first from a timer it asks for in `on_open`, and judges each answer at the wall time of a timer
   it asks for once the answer arrives. It maps each contract's status by the function discovery
   maps it with (only `LIVE` while the exchange is `NORMAL` is trading; an absent, null or
   undocumented status refuses the answer), reads the advertised open and close as discovery does,
   and reads `maintenanceWindow`'s bounds as decimal Unix seconds in a number or a string, none
   after 2100-01-01 UTC. A market is `Trading` after a read only if it traded throughout the read's
   span: listed `LIVE` with the exchange `NORMAL`, no advertised open after the send nor close at
   or before the judged time, no scheduled closure meeting the span, the read sent after the market
   was subscribed, and no halt of that market after the send, whether or not it changed the status
   reported, on a schedule or by another read's answer or failure. A read held back only by such a
   halt or by the subscription changes nothing, and every other read halts it, a market a read
   leaves out included; a read that fails or is refused halts every market, and only a read sent
   after it can lift one, after the window of point 5, and so does subscribing a market once the
   reads have begun, for the first time on the endpoint or again, for that market, however long no
   market was subscribed meanwhile. The reads begin with the first read the endpoint sends, for
   whichever markets are subscribed then, not with its first timer: a timer that fires while no
   market is subscribed sends no read and begins nothing. A market subscribed before the reads
   began, however many timers had fired, is subscribed afresh, its status reported from the first
   read judged: no read had been sent, so none showed anything of it. Between reads the codec halts
   a market on a timer at a published window's begin, at its calendar's next close and at its
   advertised close, the earlier of those two. Nothing makes a market trading but a read.

   Its schedule, the published windows and each contract's advertised open and close, is the
   endpoint's, kept whichever markets are subscribed, so a market subscribed later is checked
   against it at once. An answer shows the venue at some instant between its read's send and its
   judged time, so a read sent no earlier than an answer was judged shows the venue no earlier than
   that answer does, and its answer replaces that one's. Of answers whose spans overlap (the later
   read sent before the earlier answer was judged), in whichever order they come, which shows the
   venue later is not known, so the schedule keeps each, and for each contract holds the earliest
   close and the latest open any of them advertises, and every window any publishes; a window that
   has begun stays, once the answer publishing it is replaced, until its end and the window of point
   5 after it have passed every read in flight. An advertised close holds a read's span when it is
   at or before the read's judged time, and an advertised open when the window of point 5 after it
   ends after the read's send, so the close or open kept holds every span another holds, and a close
   or an open by which any read halts a market, whichever read advertised it, stays in the schedule,
   or one holding every span it holds does, until an answer replaces the one that advertised it.
   Each such halt also shows the market halted at an instant that any reopening after it follows: a
   close the schedule holds the market by, at that close; a read whose own close holds its span, at
   its judged time (it may show the venue as late as that); a read whose own open holds its span, at
   that open or, if earlier, its judged time. The codec keeps the latest of these since a read last
   showed the market reopened (point 5). Such a halt ends on the schedule only through an open: an
   answer that replaces one of the schedule's advertises an open that holds its read's span, no
   earlier than that latest instant, and no close that holds it; that open is the venue's reopening
   (a moved open stays one while it is no earlier), and a read sent once the window after it has
   ended, held by no other close or open, can lift the market. Every other end of a halt by an
   advertised close or open comes at an answer that replaces one of the schedule's, advertises no
   close holding its read's span, and either, while the schedule before it held the span by a close
   or an open, no open holding it (no close or open, a close after its judged time, an open whose
   window ended by its send), or an open holding it earlier than that latest instant: that answer
   ends it as a halt no schedule announces (point 5). Its own answer judges this, since an answer it
   does not replace may show the venue before it. A read advertising a close that holds its span
   keeps the market halted by that close, an answer that replaces none (its read sent before every
   answer of the schedule was judged, as one answered after a later-sent read's) ends no halt, and
   an answer that stops advertising a close or an open not holding its read's span changes nothing.
5. **The window after a reopening, until FBC-vg2n.** No source says which status a Hibachi contract
   shows during the call auction that follows a scheduled maintenance and the FX reopening. Until
   FBC-vg2n settles it from evidence, a market stays halted for the required `hibachi.auction_halt`
   (whole seconds, no default, zero refused) after every reopening the codec knows of. A scheduled
   one is a published window's end, a reopening of its calendar or an advertised open, at its
   instant. The other follows a halt no schedule announces: a read that showed the market halted by
   its contract's status or the exchange's (a maintenance with no published window or past the end
   of the one published, an FX pair still `PAUSED` after its calendar reopens), a read that left it
   out, a read that failed or was refused, which shows nothing of the venue, so the venue may have
   halted and reopened the market meanwhile, a read that ended a halt by an advertised close or
   open other than through an open the venue advertises (point 4), since the codec cannot tell a
   close or an open the venue withdrew from one it passed and then reopened the market after, and
   an open earlier than an instant the market was shown halted is not the reopening after that
   halt, or the market subscribed once the reads had begun (point 4), for the first time or again,
   since the reads judged before showed its contract while no market of it was subscribed, so none
   was judged for it (it is reported halted then, as after a failed read). The venue reopened it,
   if it halted it, no later than the judged time of the first read that can lift it (sent after
   every halt of it and after its subscription) and shows it trading again, so the window is
   counted from that judged time, whatever already halts the market; a read sent before that halt
   is not such a read, since it may show the venue before the halt. What this guarantees: no read
   whose span may meet a reopening the codec knows of, or the window after it, reports the market
   trading, so a market is never reported trading while an auction may still run after a reopening
   the codec knows of. The codec knows what its own endpoint's reads showed and its schedule held,
   and that endpoint stays open while the venue's market data runs (point 4). What it does not
   guarantee is one reopening the codec may not know of: one after a halt that no schedule
   announces and no read shows, when no read leaves the market out, fails or is refused, and the
   market is not subscribed once the reads have begun, from the halt's begin until a read reports
   the market trading again. Such a halt begins and ends between what two reads show, or ends
   before what the first read judged shows of a market subscribed before the reads began (point
   4); it is never reported and may start no window.
6. **The capability.** `MdCaps` gains a mandatory `status: FeedSource`, as `trades.source`,
   `funding.source`, `stats`, `mark` and `index` say where their kinds of market data come
   from (0003, 0014): `None` where the adapter's market data reports no status, so a consumer
   knows from the capabilities alone that the spec's discovery snapshot is all it will have;
   `Poll` where it re-reads it from REST, Hibachi's; `Stream` where a streamed channel carries
   it. No subscription asks for it: a venue declaring a source reports it for every instrument
   its market data is subscribed to. Every adapter and toy declares it with its source, each
   venue's tests pin it, and the conformance kit's market-data checks count a status from a
   venue that declares `None` as a breach.

## Alternatives

- Reuse `ExecEvent::Mode`: rejected. It is an order-entry event that a market-data codec cannot
  push, and a venue whose market data is all the library reads (Hibachi today) would have no
  session to push it from.
- Report the status through `Health`: rejected. A feed's health says whether its data can be
  trusted, not whether orders are accepted; a halted market's book is still the venue's book.
- A side channel from the codec to the consumer: rejected for the reasons above (unstamped,
  outside ingest order and replay).
- A status feed in `Feed`, subscribed per instrument: not taken. A status concerns every
  instrument the venue's market data is subscribed to, as the planner reads it, not a channel a
  consumer chooses; the capability (point 6) says where it comes from.
- No capability, the event alone telling a consumer that a venue reports status: rejected
  (Reviewer B RB214-2 on PR #214). Every other kind of non-book market data declares its source
  as a `FeedSource`, and without one a consumer could not tell, before any event, a live status
  from a discovery snapshot that never refreshes except by the venue's name (0003), nor could the
  conformance kit hold an adapter to it.
- Counting the window after a reopening the reads show from the judged time of the last read
  that showed the market halted (Reviewer A RA214-2's first form): rejected. The venue may
  reopen any time up to the judged time of the first read showing it trading again, so a window
  counted from the earlier read can end while the auction still runs.
- Lifting a market at once on the first read after a failed or refused read, or after a read
  that left it out, to show it trading: rejected (Verifier V-1 on PR #214). Neither shows the
  market trading, so either may hide a halt and the reopening after it, and the market could be
  reported trading while that auction still runs. The window is counted as after a reopening a
  read shows; one failed read thus holds every market for at least the window. The same holds
  for a halt by an advertised close, or before an advertised open, that a read ends by no longer
  advertising it (Verifier V1C-2 on PR #214): reporting the market trading at that read, with a
  window only from an open it advertises, was rejected, since the venue may have passed that
  close or open and reopened the market before that read, its call auction still running.
- Counting that window from the read whenever a read advertises an open other than the one
  standing, or stops advertising one: rejected (V1C-2). An open that still holds the read's span,
  no earlier than every instant a close or an open showed the market halted (point 4), is the
  venue's schedule, and the window after it covers the auction then; and a market trading after
  an open's window, whose later reads drop the open, would be halted for no reopening.
- Taking as the venue's reopening every open that holds a read's span, or every one no earlier
  than the close standing: rejected (Verifier VE2-1 and VE2-2 on PR #214). An open earlier than
  the close that halted the market, or than an instant a read showed it still closed or not yet
  open, cannot be the reopening after that halt, so its window could end while the auction after
  the real one still runs; and a read still advertising a close it is judged after shows the
  market closed as late as its judged time, so the close alone is too early a bound. Each read
  counts at its judged time, not its send, since it may show the venue as late as that.
- Starting a market whose subscription is taken again afresh, or keeping its state across the
  removal: rejected (Verifier VE2-4 on PR #214). Afresh, a halt the schedule announced or a read
  showed before the removal is forgotten, and the first read listing the market `LIVE` reported
  it trading at once; kept, the state misses whatever the venue did while no read was judged for
  the market. It is held as across a failed read instead, from the moment it is subscribed
  again, as every market subscribed once the reads have begun is (Verifier VE5-1, below). VE3-2
  on PR #214 had one whose last subscription was removed before the reads began subscribed afresh
  when taken again after, since no read had been judged for it; VE5-1 holds it too.
- Subscribing a market afresh whenever it is subscribed for the first time on the endpoint, or
  counting a halt for it then so that only a read sent after its subscription can report it:
  rejected (Verifier VE5-1 on PR #214). The reads judged before showed its contract while no
  market of it was subscribed, and the codec judged none of it, so a pause or an omission a read
  showed, a failure, or a halt between reads went unknown: afresh, a read sent before the
  subscription and answered after it reported the market trading though a read judged since had
  shown it paused, left it out or failed. Counting a halt alone, the verifier's suggestion, keeps
  those reads out, but a read sent after the subscription still reported the market trading
  inside the window after a pause an earlier read had shown (the randomized oracle in
  `tests/status_oracle.rs` finds both on the earlier code). Keeping each contract's state whatever
  is subscribed was not taken: a contract never listed, a contract a read leaves out and a failure
  would need the same hold all the same, and a market's calendar comes only with its subscription.
  Every market subscribed once the reads have begun is held as across a failed read instead, so a
  market added while the venue's market data runs trades only from the window after the first
  read sent after its subscription to show it trading. One subscribed before the reads began is
  subscribed afresh, since no read had been sent.
- Taking the reads to begin with the endpoint's first timer: rejected (Verifier VE4-1 on PR #214).
  An endpoint opened with no market subscribed fires that timer and sends no read, so a market
  subscribed, removed and taken again before any read was sent was held as one subscribed again,
  halted when taken again and trading only from the window after the first read sent after that,
  though no read had been sent and the codec knew nothing of the market a first subscription would
  not. The reads begin with the first read sent, for whichever markets are subscribed then: one
  instant for the endpoint, as the docs had it, not one for each market.
- Planning nothing for an empty set of subscriptions, so the poll endpoint closes when the last
  is removed: rejected (Verifier VE3-1 on PR #214). A subscription taken after that would open a
  new endpoint whose codec starts afresh, so a halt the closed one's schedule announced or its
  reads showed would be forgotten, and the first read listing the market `LIVE` would report it
  trading at once, as VE2-4's afresh start did. Stating instead that the hold lasts while some
  subscription is kept was not taken: it would add a second exception. Kept open with no
  subscription, the endpoint costs a timer each interval and sends no read.
- Taking the answer of the latest-sent read judged as the venue's latest, whatever the spans:
  rejected (Verifier VE5-2 on PR #214). Of two answers judged in send order, the later shows the
  venue later only if its read was sent once the earlier was judged; otherwise it may show the
  venue before the earlier did, and replacing the earlier dropped a close, an open or a window the
  venue may have advertised last, so a close or a window's begin it held passed with no halt.
  Seeding a market subscribed later from the latest-sent judged read alone, the verifier's
  suggestion for a fresh market's schedule, would carry the same gap; the schedule is kept for the
  endpoint instead, whichever markets are subscribed, holding every answer no later read has
  certainly replaced. Judging an answer's marks (point 4) by that schedule after it, rather than
  by the answer itself, was not taken: an answer the replacing one does not replace may show the
  venue before it does, so an open or close it still advertises may be gone.
- Keeping out of the schedule an open that only a read answered after a later-sent one
  advertises: rejected (Verifier VE-1 on PR #214). That read halts the market before the open,
  and the next read that could lift it and no longer advertised the open reported it trading at
  once, before that open or inside the window after it. Marking the market as after a halt no
  schedule announces whenever such a read halts it was not taken: a read still advertising that
  open would then start a window of its own as well, where a read answered in order holds the
  market for the open's window alone.
- Judging an answer at the latest wall time any timer gave before it arrived: rejected (FBC-1632
  notes, 2026-10-22). No timer need fire between a read's send and its answer, and an advertised
  open judged at that later time would read a read sent before the open as made after it.
- Halting only at polls: rejected; each scheduled halt would arrive up to one interval late.

## Consequences

- Every exhaustive match on `MdEvent` grows an arm; a consumer's handler sees a new event.
- Every `MdCaps` literal states `status`; Paradex, Binance USD-M, QFEX and the toys declare
  `None`, Hibachi `Poll`.
- A market a read shows halted by status, a read leaves out, or a failed or refused read halts, one
  whose halt by an advertised close or open a read ends other than through an open the venue
  advertises (point 4), and one subscribed once the reads have begun, for the first time or again,
  stay halted until a read is sent one reopening window after the judged time of the first read
  sent after that halt to show it trading again. The reopening's instant is known only to lie
  between two reads, so this can hold it up to an interval and an answer's delay longer than a
  scheduled reopening would, one failed read holds every market for at least the window, as
  subscribing a market once the reads have begun holds it (a market a consumer adds while the
  venue's market data runs trades no sooner), and a temp market whose close is withdrawn, or
  which is listed again after it, trades only from the window after the read that stops
  advertising that close, or after an open that read advertises no earlier than every instant the
  close showed the market halted.
- An answer stays in the schedule until the answer of a read sent no earlier than its judged time
  comes, so when answers take longer than the interval a close, an open or a window the venue
  withdrew may halt a market until then.
- A consumer that quotes on Hibachi passes each reported status into `PlanVenue::status`; a
  command it authorizes through the registry directly is still not checked against status.
- A change no schedule announces (a pair paused early, an unscheduled maintenance) reaches the
  consumer at the next read, up to one interval late.
- Hibachi's plan has a second endpoint, which takes a connection number from the consumer's
  range and one REST request per interval against the IP's limit. It is planned for every set,
  so it opens with the venue's first plan, an empty one included, keeps its one connection
  number while the venue's market data runs, and sends no read while no market is subscribed.
- `fbc-sim` does not yet refuse an order on a market a status shows halted; the planner withholds
  places and amends there from the status the consumer passes it.

## What would show this was wrong

- Evidence (FBC-vg2n) that a call auction shows its own status, or none of these, which would
  change the window after a reopening.
- A venue whose status changes faster than a REST interval can follow, or one that streams it,
  which would want a streamed codec declaring `FeedSource::Stream`.
- A venue whose status source differs by instrument, which one `FeedSource` per venue cannot
  say.
