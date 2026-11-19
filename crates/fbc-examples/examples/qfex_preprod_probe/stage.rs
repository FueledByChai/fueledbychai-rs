//! Stage one of the QFEX probe (decision 0132 items 1, 3, 6 and 7; FBC-l931), on pre-production,
//! or with `--production` on production (decision 0143): one session per account, each arming
//! cancel-on-disconnect once, so the journal captures whatever answers the arm. Stage one places
//! nothing and sends no command but each session's arm, its resync's reads and the probe's own
//! reads. With `--hold-secs` it first holds one trade connection that never arms (decision 0146),
//! and a run may hold without arming at all. With `--place-stages` it runs the place stages'
//! preflight, each named account's drop test, which places X and Y, stage two's crossing
//! post-only and taker IOC, and stage three's pages (below).
//!
//! - **Before anything connects** ([`run`]): every credential the run reads is set
//!   (`src/auth_qfex.rs`), and the configuration of each account it reads, under the adapter's
//!   probe mode for the run's environment (`preprod_probe`, or `production_probe` with
//!   `--production`), passes that mode's guard and decision 0122 item 1's account check (the
//!   adapter's `probe::connections`): any QFEX origin but the environment's own, a host of the
//!   other environment's domain (`qfex.com` on pre-production, `qfex.io` on production), a mix
//!   of loopback and other origins, or an account id that is not `primary` or a lowercase
//!   subaccount UUID is refused, naming the setting and never its value, and so is a
//!   `QFEX_SUBACCOUNT_ID` that selects the primary account (decision 0144 item 1). Loopback
//!   origins with a SOCKS5 proxy are refused too: the proxy would reach its own host's loopback.
//! - **The accounts** ([`named`], [`Run::check_accounts`]): the ones the run's configuration
//!   names, the primary account and the subaccount `QFEX_SUBACCOUNT_ID` names when it is set
//!   (decision 0144). The probe sends no `GET /user/subaccounts`: the owner must confirm in
//!   QFEX's app before the run that the user has no other subaccount, which the probe neither
//!   checks nor records, and the report says so. Before each session, for each of them,
//!   `GET /user/positions` and one `get_user_orders` page on a trade socket of its own that
//!   sends only its authentication and that read, and never arms. Every request takes its
//!   origin from the guarded configuration, through the adapter's `account_reads`, and is
//!   charged to the run's one provider-user rate limiter first (refused, not waited for, when it
//!   has no room).
//!   A `RateLimited` err answering its authentication or its page holds that limiter's shared
//!   budget, as the adapter asks of its caller (decision 0126), and stops the probe as any
//!   refused read does.
//!   An account that cannot be read, an open order, a TWAP or a position that is not flat stops
//!   the probe before the session, and so does a REST `open_orders` count above zero
//!   ([`AccountRead::refusal`]). Every read, write and close on a read socket waits at most
//!   `--read-wait-ms`. The account a session arms on is one of them by construction: the
//!   primary account, or the subaccount `QFEX_SUBACCOUNT_ID` names.
//! - **The liveness hold** ([`Run::hold`], decision 0146, FBC-39ks): before the sessions, while no
//!   other connection of the probe is open, every named account is read as before a session,
//!   then one trade connection opens on the account `--hold-account` names (the primary unless
//!   it names the subaccount), through the same adapter functions and shared limiter as the read
//!   sockets. It sends its authentication and, once that succeeded, the three subscriptions a
//!   session sends (`account_reads::subscriptions`), whose confirmations it reads, all within
//!   QFEX's 60 s authentication deadline; then nothing but the pongs and the close reply the
//!   WebSocket layer writes to each server ping and server close, each recorded on the shared
//!   limiter as a session records one (`RateLimiter::record`, decision 0030). It never arms,
//!   places, cancels or reads, and sends no ping of its own. It ends at `--hold-secs` from its
//!   opening or at the owner's interrupt (Ctrl-C, [`Interrupt`]), each a close charged as a
//!   control frame (none, and the connection dropped, when the limiter has no room, as a
//!   session's) and then the server's echo read for at most `--read-wait-ms`; at a server close
//!   (read on until the close reply has gone out); or at the connection's end; nothing
//!   reconnects. Every ping, pong and close it receives is recorded with its time since the
//!   connection opened ([`crate::hold`]); every data frame is counted by kind and read for a
//!   provider rate refusal, never kept, and a `RateLimited` err among them holds the shared
//!   budget as on a read socket (an err a journal would keep whole is read in memory as it
//!   arrived, for its code alone, and holds none). A refusal (authentication refused, a
//!   confirmation missing at the deadline, an err) ends it with a close as its duration does,
//!   and an err read while it closes, whatever ended it, is its refusal too, unless one already
//!   refused it. Once its connection is over, whatever ended it, every named account is read
//!   again, each whatever another showed; an account that then shows an open order or a
//!   position, a hold that did not do what decision 0146 asks, an order or fill frame on it, or
//!   the owner's interrupt in a run that also arms stops the probe, so no session starts. The
//!   interrupt is the hold's from its connection's opening, when the hold polls it before
//!   writing anything, until its close and the read of the echo are over: one that comes as it
//!   closes is recorded as such and stops a run that arms too. An interrupt at any other time
//!   is not the hold's: the program (`main.rs`, [`crate::interrupt`]) then ends at once,
//!   writing no report, as the probe always has, but in a place-stage run, whose press from the
//!   hold's first read to its last is the hold's too ([`Run::hold`]).
//! - **The session** ([`Run::session`]): fbc-runtime's `ExecSession`, unchanged, with the
//!   consumer's `AccountWideCodAuthority` for the account it arms on. The program submits
//!   nothing: the session sends its own arm once the codec reports the stream authenticated
//!   (decision 0058), and asks for its resync, whose reads the probe codec sends (decision 0132
//!   item 5: one signed `GET /user/positions` on the REST origin and `get_user_orders` pages on
//!   the session's socket; no `get_user_trades`, since the session places nothing and decodes no
//!   fill before it), so nothing places. The codec answers the arm from its echo (an ack accepts
//!   it, an err rejects it, decision 0132 item 1; FBC-jxau). The handler stops the session
//!   through its control once the arm has its outcome (its answer, or `Unknown` at its deadline)
//!   and the resync has ended, so the journal holds each read with its answer, or at the
//!   connection's end (the codec ends the epoch on a read that fails or outlasts
//!   `qfex.exec.resync_timeout`), whichever comes first, and the program at `--session-wait` or at
//!   its registry's span bound (`--registry-span`), through the same control; the reconnect pacing
//!   allows one attempt a day, so no second attempt starts meanwhile. The journal keeps every frame
//!   as the codec names its spans, so the arm's window is kept as decision 0132 item 3 says. The
//!   sessions run one after the other: no two arms are in flight across the user.
//! - **The registries** ([`crate::ledger`]): each session's own, since each arms its own account
//!   (decision 0082), built as the session starts: as decision 0132 item 7 says on
//!   pre-production, and with no testnet declaration on production (decisions 0067 and 0143
//!   item 3), each with one fill ledger under the owner's bounds (decision 0132 item 5). Its span
//!   bound runs from then, and its ledger's horizon and replay counts are recorded as the session
//!   ends, for the report.
//! - **The provider rate refusals** ([`crate::refusals`]): each read socket's and each session's,
//!   counted as decision 0135 item 2 says, for the report: the limiter's count while it ran
//!   ([`Run::rejected`]), and its frames that were refusals or unread, a read socket's two as it
//!   reads them and a session's from its journal.
//! - **The resync's full pages** ([`crate::pages`]): each full `get_user_orders` page a session's
//!   resync read, with its page size and offset (decision 0132 item 5, FBC-4h1p), from the
//!   session's journal, since its codec's own count cannot be reached from outside the session.
//! - **The place stages' preflight** ([`run_places_until`], FBC-c6kw, the first slice of
//!   FBC-9sk2; decision 0148): a run with `--place-stages` prints a `CAPS` line with the
//!   environment and both caps first, before any request, the hold's included (decisions 0113
//!   items 1 and 2 and 0148 item 7); its origins are those the adapter's probe guard admits for
//!   production alone (`--production`: QFEX production's three, or all three loopback), as every
//!   run's are (decision 0143 item 2), FBC-4plj having lifted the loopback-only gate once every
//!   stop decisions 0132 item 7 and 0148 item 8 require had landed; it is refused, nothing sent,
//!   unless `--registry-span` is above the drop test's floor for the accounts the configuration
//!   names (`probe_args::check_place_span`, 0151 item 3). It holds first when
//!   `--hold-secs` asks, alone (the owner's Ctrl-C from the hold's first read to its last stops
//!   it before its preflight, with no stop marker, [`Run::hold`]),
//!   then runs [`crate::market`]'s preflight (the spec from this run's
//!   `GET /refdata`, the count bound, the MDS touch and the caps), then each named account's
//!   drop test ([`Run::drop_tests`], below). The MDS touch is driven from the preflight's first
//!   touch until the place stages end ([`market::Touch::driven`], FBC-kllr): beside every read,
//!   wait and close between two sessions as well as during them, so each server ping on its
//!   socket is answered and each touch reaches the view every place is priced from; a touch
//!   connection that ends is not opened again, and the next place it would price is refused.
//!   [`place_config`] is the configuration a place stage's sessions take, the only one with
//!   `qfex.exec.production_places` (decision 0148 item 4).
//!   Before anything connects it opens its lease directory ([`places::RunLeases::open`], FBC-23ch:
//!   the production tag, the stop markers held to `--reconciled`, the namespace lease and the
//!   run's mint; 0151 items 1 and 6). With `--reconciled` it reads every named account after the
//!   preflight and clears the marker only if every read is clean ([`Run::reconcile`]). The
//!   owner's Ctrl-C outside the hold ([`Presses`]) during the preflight or those reads stops the
//!   run, [`Stop::Interrupted`], and any stop of the place stages but `NoEcho` writes the stop
//!   marker with the report ([`Run::mark`]), keeping the stops of the marker it replaces unless
//!   the run cleared it.
//! - **The journal's tee** ([`crate::tee`], FBC-23ch): the run's journal sink is wrapped by a
//!   [`Tee`], which hands every record on unchanged and reads the sessions' frames as the journal
//!   keeps them, for the stops the drop sessions wait on.
//! - **The drop test** ([`Run::drop_tests`], FBC-23ch and FBC-77r8; decisions 0132 items 4 and 7,
//!   0148 and 0151) on each named account in turn, each starting only once the one before ended
//!   with no stop. **Drop session one**: every named account read clean (unless the reads that
//!   cleared a stop marker just did); the account's drop registry built, its `CancelOnlyTrip` given
//!   to the journal before any market arms (decision 0139), seeded flat (0148 item 3); one
//!   unchanged `ExecSession` on [`place_config`] with the discovered spec table, the run's
//!   `--namespace`, its one `RpcIds`, the run's next session number, an `AccountWideCodAuthority`
//!   for its own account (decisions 0121 and 0122) and one connection attempt, its handler a
//!   [`places::PlaceHandler`] over the registry's `Link`; once it is authenticated, its arm
//!   accepted and its resync applied, Start under the market lease, then X, a post-only GTC at the
//!   instrument's minimum size `--away-bps` behind the MDS touch, kept current meanwhile, minted
//!   under the run's namespace lease and authorized by fbc-oms, the one place before an ACK echoes
//!   our client id (decision 0132 item 7), spent from the registry's list first
//!   ([`places::Allowance`]); X's ACK or its deadline; the streams compared with never-arming reads
//!   ([`crate::compare`]); a graceful close; the read of every named account after it, and X and
//!   its trades read (FBC-be57). The first stop the tee, the handler, a comparison, the span bound
//!   or the owner's Ctrl-C raises ends the steps: only Ctrl-C and a fill of ours then cancel, by
//!   venue id (0151 item 2, [`Run::stop_cancels`]), and every session ends by a graceful close,
//!   cancels held included (0151 item 11), unless its connection ended first. The reads after it
//!   wait out a provider rate refusal's hold on the shared budget ([`Run::admitted`]), as does the
//!   close. A stop ends the run, its marker written, X still resting reported for the owner's hand
//!   cancel. **Drop session two** (FBC-77r8): the read before it of every named account
//!   ([`Run::read_before_two`]), which admits X alone, one open order on its symbol (decision 0132
//!   item 7, 0144 item 4); then a session on the same `Link` and registry, seeded nothing (0148
//!   item 3) and not Started again, its market armed from drop session one: its arm accepted; Y, as
//!   X was placed; Y's ACK or its deadline; the read before the drop, one never-arming read beside
//!   the session, a `CANCELLED` of X heard by its end being the arm's, reported, and a stop or a
//!   press met as its socket closes taken before the drop; then the abrupt drop, its run dropped
//!   unpolled, its connection with it, with no WebSocket close. From the first arm to the last read
//!   nothing cancels X or Y but Ctrl-C or a fill of ours, nothing reconnects, and no later arm is
//!   sent (decision 0132 item 4). **After the drop** ([`Run::judge_drop`]): reads of the account at
//!   the comparisons' cadence until X and Y are absent or `--agree-wait-ms` ends, then the trades
//!   since X's send and `GET /user/historic-orders`, and the verdict ([`crate::verdict`]), judged
//!   per account in memory and never carried to another account or run (decision 0148 item 2); then
//!   the read of every named account. A stop in drop session two ends it as one in drop session one
//!   does, before any drop, and a failure there that is no stop gains the stop the reads after it
//!   show ([`after_stop`]), or a stop for an order that may rest when X's account was not read; a
//!   read before the drop that was not made, or that showed what the streams did not, is a stop
//!   before the drop; a failed verdict, an arm-time cancel QFEX did not report, a fill of X or
//!   Y the reads show (the run stopping not flat, the position the owner's, 0151 item 9) or an
//!   order still resting after it is a stop, marked, so nothing more is placed on that account or
//!   any other. With no stop, stage two follows.
//! - **Stage two** ([`Run::cross_tests`], FBC-su9i; decisions 0005, 0132 item 7, 0148 items 2, 6
//!   and 8, 0151 items 5, 9 and 11), once every named account's drop test ran with no stop. The
//!   run's exact-echo gate ([`crate::stage_two::EchoGate`]) is judged on what the tee heard in this
//!   run, in memory: shut, the run ends [`Stop::NoEcho`], not a fault, with no stop marker. Open,
//!   each named account whose drop test passed in this run, in turn ([`Run::cross_test`]): every
//!   named account read clean, the account's cross registry built, tripped and seeded flat, its
//!   allowance the one its drop test spent from, the drop registry's market disarmed first so its
//!   market lease is free; one session ([`Run::cross_session`]), armed, resynced and Started under
//!   the market lease; no other order of ours ([`crate::stage_two::no_other_order`]: every registry
//!   of the run, X and Y counted ended only on 0151 item 5's three reads, and every named account's
//!   read before the session), else no crossing order and a stop; the crossing post-only, a
//!   post-only GTC at the minimum size at the far touch, as an fbc-oms authorization spent from the
//!   cross registry's list; its first answer (its ACK timed), then its end or its deadline,
//!   `--rpc-timeout-ms` from its send, a place never answered ending `Unknown`, never resent; how
//!   it came back ([`crate::stage_two::Crossed`]), read before any cancel of ours; the streams
//!   compared with the reads; then the session machinery's Stop: every order of ours cancelled by
//!   venue id, and a graceful close, or the abrupt drop when those cancels were held (below); then
//!   the read of every named account, the session's orders known as ours on its own account. A
//!   fill of it is [`Stop::UnexpectedFill`] (flattened first, below); a position not flat in those
//!   reads is [`Stop::NotFlat`], said to be the owner's whatever stop came first (0151 item 9); an
//!   order or a TWAP not ours in those reads is [`Stop::ReadDisagrees`]; an order of ours still
//!   resting after the session, listed by a read, counted open by REST and listed by no page, or
//!   counted resting by the registry (an `Unknown` one, decision 0005), is [`Stop::LeftResting`]
//!   ([`stage_after_stop`]). With no stop, the account's taker IOC session follows, before the next
//!   account's crossing session (decision 0148 item 1: the stages on each named account in turn).
//!   The report gives what the whole run's frames show of our client ids and QFEX's statuses
//!   ([`crate::stage_two::StageTwo`]), and the crossing and taker sessions' frames go in the
//!   `places-production/` slice ([`PLACES_SLICE`]).
//! - **The taker IOC** ([`Run::take_steps`], FBC-ou9g; decisions 0005, 0132 item 7, 0148 items 5,
//!   6, 8 and 10, 0151 items 5, 8 and 9; [`crate::taker`]), on the account's take registry, run as
//!   the crossing session is ([`Run::cross_test`] and [`Run::cross_session`] under
//!   [`Which::Take`]): every named account read clean, the registry built, tripped and seeded flat;
//!   one session armed, resynced and Started; no other order of ours; one limit IOC at the
//!   minimum size at the far touch, through it, spent from the take registry's list, sent only
//!   while both USD caps admit it at that touch ([`market::caps_admit`], else
//!   [`Stop::BeyondCaps`], decision 0113), its fills expected, so none is
//!   [`Stop::UnexpectedFill`]; its end with its fills settled, or its
//!   deadline, never resent; how it came back ([`crate::flatten::Ioc`]), its fees' signs and the
//!   order of its frames read; the streams compared with the reads. Then
//!   ([`crate::taker::after_ioc`]): one that left the registry's position off flat is flattened
//!   while the session is open (the owner's Flatten into Exit, then the flatten IOCs, REST flat
//!   before the Stop), its fill expected, so the run goes on once flat; one that expired unfilled
//!   flattens nothing; one left open with a fill, as the adapter decodes QFEX's
//!   `IOC_PARTIALLY_FILLED` with a remainder, is [`Stop::IocRemainder`], not flat (0151 item 8,
//!   until FBC-jdj7); one whose fills QFEX's count states but which had not all reached the
//!   registry by its deadline is [`Stop::IocUnsettled`], not flat, with no flatten, whatever the
//!   registry's position. Then the session's Stop and the read of every named account, as after a
//!   crossing session. With no stop, the account's stage-three session follows (below).
//! - **Stage three, the pages** ([`Run::pages_steps`], FBC-4plj; decision 0132 items 5 and 7,
//!   0148 items 7, 8 and 10; [`crate::pager`]), on the account's pages registry, after its taker
//!   IOC session, so only once its drop test passed and the run's exact-echo gate is open, run as
//!   the crossing session is ([`Run::cross_test`] and [`Run::cross_session`] under
//!   [`Which::Pages`]): every named account read clean, the registry built, tripped and seeded
//!   flat; one session armed, resynced and Started; then [`crate::pager::PLAN`]: P1 and P2, page
//!   0, P3, page 1, P1's cancel, pages 2 and 3, P2's and P3's cancels, then every page from offset
//!   0 until a short one ([`Run::walk`], cut at [`crate::pager::WALK_PATIENCE`]). Each order is a
//!   post-only GTC at the minimum size `--away-bps` behind the touch, an fbc-oms authorization
//!   spent from the pages registry's own list ([`Run::place_resting`]), held to both USD caps at
//!   the touch it is priced at; each cancel is by venue id ([`Run::cancel_paged`]); each place and
//!   cancel is compared with never-arming reads. The pages come from the pager, a second socket
//!   that never arms or places, on the session's account and the guarded configuration, at a page
//!   size of 1, every frame charged to the shared limiter first (`compare::Socket::page`); each
//!   is recorded with its size and offset, which of ours the streams showed open as it was asked
//!   and what it listed by name ([`Run::page_now`]). A provider rate refusal on the pager is the
//!   run's stop, [`Stop::RateRefused`] on [`places::RefusedOn::Pager`], marked, and so is a page
//!   listing an order that is none of P1 to P3, or a TWAP, [`Stop::ReadDisagrees`] (decision
//!   0148 item 8); a page that could not be made otherwise is recorded and stops nothing. A
//!   maker fill of a stage-three order is flattened while the session is open, then stops the
//!   run (the flatten, below). Then the
//!   session's Stop and the read of every named account, as after a crossing session; the
//!   session's frames and the pager's pages go in the `pages-production/` slice
//!   ([`PAGES_SLICE`]). With no stop the run ends after the last account's stage three
//!   ([`FINISHED`]), its outcome ok when its files and records are whole, and its report answers
//!   decision 0132's list for each named account ([`crate::answers`]).
//! - **The session machinery** ([`Run::end_session`], FBC-su9i): every place stage's session ends
//!   through it under its [`Policy`]. A drop session cancels only at Ctrl-C or once a fill of
//!   ours came and ends by a graceful close, or drop session two by its abrupt drop (above); a
//!   session outside the drop test cancels every order of ours by venue id before it stops, at
//!   every stop and once it ran its course (decision 0132 item 7, 0148 item 8), meets its span
//!   bound one `--rpc-timeout-ms` early ([`cancels_begin`]), and, its cancels refused locally or
//!   met by a provider rate refusal ([`Cancels::blocked`]), ends by the abrupt drop once its
//!   account's drop test passed in the run, and by a graceful close otherwise; either way, with
//!   no stop before them, those cancels stop the run, [`Stop::CancelsBlocked`], marked, the reads
//!   after the session still made ([`stage_session_stop`]; 0151 item 11, FBC-3xzn). A graceful
//!   close waits for the shared budget's room when a provider rate refusal holds it, and a
//!   Ctrl-C, or a stop that cancels under the policy, taken while it waits with no cancels run
//!   yet, runs its cancels first, then the flatten of a fill of ours not yet flattened (0151 items
//!   2, 9 and 11).
//! - **The flatten** ([`Run::flatten`], FBC-ou9g; decisions 0012, 0063, 0148 items 5, 6, 8 and 9,
//!   0151 items 5, 8 and 9; [`crate::flatten`]), reached from every registry: once a fill of ours
//!   other than a flatten IOC's reached a session (a maker fill of X or Y, a crossing post-only
//!   that fills), while the session is still open and before its Stop, every other order of ours on
//!   the market is cancelled by venue id and seen to end (else no flatten IOC, and the run stops
//!   not flat, `Flatten(NotEnded)`); the owner's Flatten puts the market in Exit (Start is never
//!   pressed again); then, while the account's one flatten budget lasts ([`places::Allowance`],
//!   carried across its registries), with no other order of ours open or `Unknown` on any registry
//!   or in the named accounts' reads before the session ([`crate::stage_two::no_other_order`]), one
//!   reducing limit IOC at a time, at the minimum size, priced within `--flatten-cap-bps` of a
//!   touch read just before it with nothing awaited between, sent only while both USD caps admit
//!   it at that touch ([`market::caps_admit`], since fbc-oms applies the caps to reducing orders
//!   too, 0013 rule 2; else `Flatten(BeyondCaps)`, decision 0113), waited on until it ends with
//!   its fills settled or its deadline, the streams compared with a read after it, which shows
//!   REST positions flat once the registry's position is. A fill of ours that reaches the session
//!   only during its Stop's cancels or after them is flattened then, before the close, the taker
//!   IOC's own included once it leaves the position off flat ([`Run::late_fill`]): the Stop's
//!   Kill lifted first, and Killed again after ([`Run::flatten_late`]). One that reaches it before
//!   the market was disarmed finds it still armed under its lease; one that reaches it after,
//!   while the close waits for the shared budget's room, finds it disarmed, so the owner's Flatten
//!   takes the market lease again to put it in Exit, and the market is disarmed again after.
//!   An IOC handed to the session is recorded as sent before its wait, so a connection that ends
//!   meanwhile leaves it recorded in flight. One whose fills had not all reached the registry by
//!   its deadline is the last placed, `Flatten(Unsettled)`, since the registry's position is then
//!   not QFEX's. A flatten that does not run its course always stops the run: its stops
//!   ([`Stop::Flatten`], a client id not minted and no touch to price from among them) replace the
//!   fill's as the run's, each marked; any other first stop stays, and a taker IOC session's, whose
//!   fill is expected, is the flatten's own. A drop session that flattened ends by a graceful
//!   close: its fill ended the drop test's measurement, and no drop test passed on its account to
//!   show a drop cancels (0151 items 2 and 11). A fill seen only when no session is open stops the
//!   run not flat.
//! - **The files** ([`crate::report`] for the report and the evidence, [`crate::capture`] for the
//!   journal and the scrubbed fixture slice), which scripts/privacy-check.sh must pass. A record
//!   the journal dropped, marked in it or only counted by its sink, one its writer never wrote, and
//!   a journal that cannot be read to its end each make the run incomplete and the capture of every
//!   session that could have lost a record partial (decision 0006).

use std::cell::RefCell;
use std::fmt;
use std::fs;
use std::future::Future;
use std::io::Write;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fbc_core::{
    AccountKey, ConnKey, ConnState, EncodeCtx, Envelope, ExecEvent, HttpResponse, Inbound, Lots,
    MonoNs, Namespace, NonceBlock, NonceSource, OpKind, RateCharge, RawFrame, SpecTable,
    SubmitOutcome, TrafficClass, VenueCaps, VenueConfig, VenueFactory, Via, WallNs, dispatch,
};
use fbc_journal::{
    JournalError, JournalWriter, NonceSourceId, QueueSink, RedactionKey, SinkConfig, journal_queue,
};
use fbc_oms::OrdState;
use fbc_runtime::ws::{Message, WebSocket};
use fbc_runtime::{
    AccountWideCodAuthority, Connector, ExecControl, ExecHandler, ExecSession, ExecSessionConfig,
    ExecSessionError, InFlight, IngestClock, Journal, ProxyConfig, RateLimiter, ReconnectPacing,
    ReservedBound, RpcIds, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::Qfex;
use fbc_venue_qfex::account_reads::{
    self, NotAPage, OrdersPage, PageAsk, RestRead, SocketErr, SocketWrite,
};
use fbc_venue_qfex::auth::{ACCOUNT_KEY, AccountSelection, ReadAuth, TRADE_URL, account_selection};
use fbc_venue_qfex::exec::{AUTH_DEADLINE, CHANNELS};
use fbc_venue_qfex::probe::{
    self, Environment, KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_PRODUCTION_PLACES, KEY_RESYNC_TIMEOUT,
    KEY_RPC_TIMEOUT, Origins, PROBE_PLACE_STAGES,
};
use fbc_venue_qfex::sign::QfexNonces;
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_URL};
use futures_util::{FutureExt, StreamExt};

use crate::auth_qfex::{self, Armed, Lookup, SUB_ID_VAR};
use crate::capture::{self, AccountRead, AllowedOrder, Evidence, SessionRun};
use crate::compare::Reader as _;
use crate::evidence;
use crate::flatten::{FlattenRun, Ioc, IocRun, Next};
use crate::hold::{self as held_frames, Control, End, Heard, HoldRun, ReadAfter};
use crate::interrupt::Presses;
use crate::ledger::{self, Opened, PlaceBounds, PlaceLists, PlaceRegistry, RegistryLedger, Step};
use crate::link::{Link, LinkHandler};
use crate::market::{self, Discovered, PlaceCaps, Preflight};
use crate::pager::PagesRun;
use crate::places::{
    self, Allowance, Cancels, DropRun, Flattening, PlaceHandler, RunLeases, Stop, Traded, TwoRun,
    Watched,
};
use crate::probe_args::{self as args, Hold, Options, Places, RESYNC_TIMEOUT};
use crate::refusals::{
    self, AFTER_CROSS, AFTER_DROP_ONE, AFTER_DROP_TWO, AFTER_HOLD, AFTER_PAGES, AFTER_TAKE, HOLD,
    ReadSocket, Refusals,
};
use crate::stage_two::{self as two, CrossRun, StageTwo};
use crate::tee::{Tap, Tee};

/// Where the lines go: standard output when run, a buffer under test.
pub type Out = Rc<RefCell<dyn Write>>;

/// The owner's interrupt, as the liveness hold waits on it: it resolves when the owner presses
/// Ctrl-C (`main.rs` wires tokio's `ctrl_c` through [`crate::interrupt`]; a test resolves its
/// own). Only the hold's connection polls it: once as the connection opens, before anything is
/// written, then while it holds, and once more when its close is over, before it drops it
/// (decision 0146 item 4).
pub type Interrupt = Pin<Box<dyn Future<Output = ()>>>;

/// What a pong, a close reply, the hold's own close or a read socket's close is charged: a
/// control frame, which safety traffic may send from the reserve, as a session charges one
/// (fbc-runtime's session core, decision 0030).
pub const CONTROL: fbc_runtime::Request = fbc_runtime::Request {
    charge: RateCharge::one(OpKind::Control, None),
    via: Via::Frame,
    class: TrafficClass::Safety,
};

/// The venue, for the sessions' `&'static dyn VenueFactory`.
static QFEX: Qfex = Qfex;

/// The client-id namespace the sessions' decode scope reads ours by: stage one mints none.
const NS: Namespace = Namespace::new(1);

/// The journal's shard.
const SHARD: u16 = 0;

/// The reconnect pacing's floor, ceiling and window: one connection attempt a day.
pub const DAY: Duration = Duration::from_secs(86_400);

/// The connection an HTTP request or a read socket's frame is charged under: QFEX's buckets are
/// per user, and these are on no session's connection.
pub const NO_CONN: ConnKey = ConnKey {
    conn: u16::MAX,
    epoch: 0,
};

/// The consumer's account key for each account a session arms on (decision 0122): the program's
/// own numbering.
fn account_key(armed: Armed) -> AccountKey {
    match armed {
        Armed::Primary => AccountKey::new(1),
        Armed::Subaccount => AccountKey::new(2),
    }
}

/// How an account is named in the output: never by its id.
fn name(armed: Armed) -> &'static str {
    match armed {
        Armed::Primary => "primary",
        Armed::Subaccount => "subaccount",
    }
}

/// The accounts the run's configuration names, which it reads before every session (decision
/// 0144 item 1): the primary account, then the subaccount [`SUB_ID_VAR`] names, when it is set
/// and not empty. Only whether it is set is read here; `src/auth_qfex.rs` reads its value. Never
/// a list QFEX gave: the probe sends no `GET /user/subaccounts`.
fn named(lookup: Lookup<'_>) -> Vec<Armed> {
    let sub = lookup(SUB_ID_VAR).is_some_and(|id| !id.is_empty());
    let mut named = vec![Armed::Primary];
    named.extend(sub.then_some(Armed::Subaccount));
    named
}

/// Where a run's origins pointed, as the report names it.
pub fn target(origins: Origins) -> &'static str {
    match origins {
        Origins::PreProduction => "QFEX pre-production",
        Origins::Production => "QFEX production",
        Origins::Loopback => "a loopback test stub",
    }
}

/// The environment a run's probe mode names, as the report names it (decision 0143 item 3).
pub fn environment(environment: Environment) -> &'static str {
    match environment {
        Environment::PreProduction => "pre-production",
        Environment::Production => "production",
    }
}

/// What a run ended with (the evidence is read by the integration test; the program prints the
/// result on its DONE line).
#[allow(dead_code)]
#[derive(Debug)]
pub struct Outcome {
    pub ok: bool,
    pub evidence: Evidence,
}

/// The run's nonces: QFEX's random source, one for every read and session, so no nonce repeats
/// among those it remembers (the adapter's `QfexNonces`).
#[derive(Clone)]
struct Nonces(Arc<Mutex<QfexNonces>>);

impl NonceSource for Nonces {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        self.0.lock().expect("the nonce source").reserve(len)
    }
}

/// Runs stage one as [`run_until`] does, with no interrupt: the integration test's entry, the
/// program passing the owner's.
#[allow(dead_code)]
pub async fn run(opts: &Options, lookup: Lookup<'_>, out: Out) -> Result<Outcome, String> {
    run_until(opts, lookup, out, Box::pin(std::future::pending())).await
}

/// Runs stage one: the guard, the reads, the hold when `opts.hold` asks for one (ended early by
/// `interrupt`), one session per account in `opts.arms`, then the files. An error is a refusal
/// before anything connected; once the evidence directory exists, the outcome is in the report.
pub async fn run_until(
    opts: &Options,
    lookup: Lookup<'_>,
    out: Out,
    interrupt: Interrupt,
) -> Result<Outcome, String> {
    run_with(opts, None, lookup, out, interrupt).await
}

/// Runs a place-stage run as [`run_places_until`] does, with no interrupt: the integration
/// test's entry.
#[allow(dead_code)]
pub async fn run_places(
    opts: &Options,
    places: &Places,
    lookup: Lookup<'_>,
    out: Out,
) -> Result<Outcome, String> {
    let pending = Box::pin(std::future::pending());
    run_places_until(opts, places, lookup, out, pending, Presses::none()).await
}

/// Runs a place-stage run (`--place-stages`, FBC-c6kw): the `CAPS` line, the guard and the span
/// bound's floor, the lease directory ([`RunLeases::open`]:
/// its tag, the stop markers and `--reconciled`, the namespace lease and the mint), the hold
/// when `opts.hold` asks for one (ended early by `interrupt`), then the place stages' preflight
/// ([`crate::market`]) and, with `--reconciled`, the reads of every named account that clear
/// the stop marker, then each named account's drop test, stage two and stage three
/// ([`Run::drop_tests`]), then the files; with no stop it ends after the last account's stage
/// three ([`FINISHED`]). A press of `presses` during the preflight or the reads stops it,
/// [`Stop::Interrupted`], its report and its stop marker written (decision 0148 item 8, 0151
/// item 6).
pub async fn run_places_until(
    opts: &Options,
    places: &Places,
    lookup: Lookup<'_>,
    out: Out,
    interrupt: Interrupt,
    presses: Presses,
) -> Result<Outcome, String> {
    run_with(opts, Some((places, presses)), lookup, out, interrupt).await
}

/// What a place-stage run says when it ends with no stop (FBC-4plj): every place stage ran on
/// every named account, each account's stage three last (decision 0132 item 7).
pub const FINISHED: &str = "every place stage ran: each named account's drop test, then, on each \
     whose drop test passed, its crossing post-only, its taker IOC and its stage three (decision \
     0132 item 7); the report answers decision 0132's list for each named account";

/// Why a place-stage run that holds stops when the owner's Ctrl-C comes before the hold's
/// connection opens, during the reads before it or its connect: it is the hold's interrupt, so
/// no preflight starts and no stop marker is written (decision 0146 item 4).
pub const INTERRUPTED_BEFORE_HOLD: &str = "the owner's interrupt (Ctrl-C) came before the \
     hold's connection opened: it never opened, and no preflight starts";

/// Why a place-stage run that holds stops when the owner's Ctrl-C comes during the reads after
/// the hold, which run to their end first (decision 0146 items 3 and 4): no preflight starts,
/// and no stop marker is written.
pub const INTERRUPTED_AFTER_HOLD: &str = "the owner's interrupt (Ctrl-C) came during the reads \
     after the hold: no preflight starts";

/// [`run_until`] or, with `places` and the presses it takes, [`run_places_until`].
async fn run_with(
    opts: &Options,
    places: Option<(&Places, Presses)>,
    lookup: Lookup<'_>,
    out: Out,
    interrupt: Interrupt,
) -> Result<Outcome, String> {
    let (places, presses) = match places {
        Some((places, presses)) => (Some(places), presses),
        None => (None, Presses::none()),
    };
    // The caps, first, before anything else, and so before any request (decisions 0113 items 1
    // and 2, 0148 item 7): on standard output alone, never in the report or the evidence
    // (0151 item 10).
    if let Some(places) = places {
        let _ = writeln!(
            out.borrow_mut(),
            "CAPS on QFEX {}: --resting-cap-usd {} USD, --inventory-cap-usd {} USD: the \
             operator's own (decision 0113), printed before any request and written to neither \
             the report nor the evidence",
            environment(opts.environment()),
            places.resting_cap_usd,
            places.inventory_cap_usd
        );
    }
    let named = named(lookup);
    // The accounts a session arms on, then the one the hold authenticates as, then, for the
    // place stages, every account the configuration names, each of which they run on in turn.
    let mut used = opts.arms.clone();
    used.extend(opts.hold.map(|hold| hold.account));
    used.extend(places.iter().flat_map(|_| named.iter().copied()));
    let first = *(used.first()).ok_or("--arm or --hold-secs is required: nothing to run")?;
    auth_qfex::check(lookup, &used)?;
    // Every account a session arms on or the hold authenticates as is among them: `check`
    // refuses the subaccount when no variable names it.
    assert!(used.iter().all(|a| named.contains(a)), "a named account");
    let base = base_config(opts);
    // What the guard admitted of the configuration of each account the run reads, every one a
    // session may arm on included: the environment its mode names and the side its origins are
    // on. Each is the base plus its account and keys, so they are all one pair, which the run's
    // environment and the report's target are read from; the proxy's refusal still looks at
    // every one.
    let mut admitted = Vec::new();
    for account in &named {
        let (cfg, _) = configured(&base, lookup, *account)?;
        let to = probe::connections(&cfg).map_err(|e| format!("refused, nothing sent: {e}"))?;
        // 0122 item 1 admits `primary` as an account id, but the subaccount slot must name a
        // subaccount: otherwise the primary account is read twice, counted as two accounts, and
        // armed under the subaccount's key (decision 0144 item 1).
        let selected = account_selection(&cfg);
        if *account == Armed::Subaccount && matches!(selected, Ok(AccountSelection::Primary)) {
            return Err(format!(
                "refused, nothing sent: {SUB_ID_VAR} selects the primary account, which the \
                 probe reads already: give a subaccount's UUID, or unset it"
            ));
        }
        admitted.push((to.environment, to.origins));
    }
    let (env, origins) = admitted[0];
    assert!(admitted.iter().all(|a| *a == (env, origins)), "one base");
    let loopback = admitted.iter().any(|(_, o)| *o == Origins::Loopback);
    if loopback && opts.proxy != ProxyConfig::Direct {
        return Err(
            "--socks5 with loopback origins: the proxy would reach its own host's \
                    loopback, not this machine's; connect to a stub directly"
                .to_owned(),
        );
    }
    if let Some(places) = places {
        args::check_place_span(opts, places, named.len())?;
    }
    // The lease directory, before anything connects or the evidence directory is made: its tag,
    // the stop markers and --reconciled, then the namespace lease for the run (0151 items 1 and
    // 6).
    let leases = match places {
        Some(places) => Some(RunLeases::open(places, wall_ns(), opts.read_wait)?),
        None => None,
    };
    let dir = &opts.evidence_dir;
    fs::create_dir_all(dir).map_err(|e| format!("--evidence-dir: {e}"))?;
    let mut listing = fs::read_dir(dir).map_err(|e| format!("--evidence-dir: {e}"))?;
    if listing.next().is_some() {
        return Err("--evidence-dir is not empty: give a new directory".to_owned());
    }
    let (cfg, _) = configured(&base, lookup, first)?;
    let caps = QFEX.caps(&cfg).map_err(|e| e.to_string())?;
    let limits = caps.exec_limits.clone().unwrap_or_default();
    let reserve = SafetyReserve::percent(10).expect("10% is a reserve");
    let limiter = RateLimiter::for_user(opts.rate_user, &limits, reserve);
    let limiter = limiter.map_err(|e| format!("the rate limiter: {e:?}"))?;
    let mut nonces = Nonces(Arc::new(Mutex::new(QfexNonces::os())));
    let key: Vec<u8> = nonces
        .reserve(4)
        .as_slice()
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let key = Arc::new(RedactionKey::new(&key).map_err(|e| format!("the journal: {e}"))?);
    let sink = SinkConfig {
        budget_bytes: 16 << 20,
        soft_limit_pct: 90,
    };
    let (sink, drain) =
        journal_queue(sink, Arc::clone(&key)).map_err(|e| format!("the journal: {e}"))?;
    // Read on its way through by the tee, whose stops the place stages wait on (FBC-23ch), and
    // kept to read the queue's drop counts once the writer has closed.
    let tee = Tee::new(sink);
    // A test's own journal past its soft limit (decision 0139), which the program never asks for.
    #[cfg(test)]
    let tee = match places.is_some_and(|p| p.test.refuse_normal) {
        true => tee.refusing_normal(),
        false => tee,
    };
    let sink = Rc::new(RefCell::new(tee));
    let tee = sink.borrow().tap();
    let writer = JournalWriter::create(dir.join("journal"), SHARD, key);
    let writer = writer.map_err(|e| format!("the journal: {e}"))?;
    let writing = drain
        .spawn(writer)
        .map_err(|e| format!("the journal: {e}"))?;
    let target = target(origins);
    // The sessions' clock, then the run's monotonic origin just after it, on the same runtime
    // clock ([`ledger::mono_since`]).
    let clock = IngestClock::new();
    let origin = tokio::time::Instant::now();
    let mut run = Run {
        opts,
        lookup,
        out,
        base,
        caps,
        connector: Connector::new(opts.proxy.clone()),
        limiter,
        nonces,
        clock,
        journal: Journal::new(Rc::clone(&sink)),
        origin,
        environment: env,
        interrupt: Some(interrupt),
        ev: Evidence {
            started: evidence::utc(wall_secs()),
            environment: environment(env),
            mode: env.mode(),
            target,
            accounts: named.iter().map(|a| name(*a)).collect(),
            arms: opts.arms.iter().map(|a| name(*a)).collect(),
            hold_asked: opts.hold.is_some(),
            places_asked: places.is_some(),
            marker: leases.as_ref().map(|l| l.marker.clone()),
            ..Evidence::default()
        },
        named,
        places,
        presses,
        leases,
        stop: None,
        tee,
        rpc_ids: RpcIds::default(),
        specs: None,
        patient: None,
        rest_refused: 0,
        unread: Vec::new(),
        built: Vec::new(),
        allowances: Vec::new(),
        taker_secs: None,
    };
    let held = opts.hold.map_or(String::new(), |hold| {
        format!(
            "; first holds one trade connection on {} for {} s",
            name(hold.account),
            hold.duration.as_secs()
        )
    });
    match places {
        None => run.line(format_args!(
            "PROBE stage one on QFEX {} ({}), against {target}: arms on {:?}{held}",
            environment(env),
            env.mode(),
            opts.arms
        )),
        Some(_) => run.line(format_args!(
            "PROBE place stages on QFEX {} ({}), against {target}: the preflight, then each named \
             account's drop test in turn (FBC-23ch and FBC-77r8): drop session one places X, one \
             post-only GTC at the minimum size behind the touch, and closes gracefully; drop \
             session two places Y and drops abruptly; the reads after the drop judge the account's \
             verdict; then stage two (FBC-su9i): with the run's exact-echo gate open, each named \
             account in turn places its crossing post-only, one post-only GTC at the minimum size \
             at the far touch, and cancels every order of ours by venue id before its session \
             stops, then, in a session of its own (FBC-ou9g), one limit IOC at the minimum size at \
             the far touch, whose fill is flattened through Exit before that session stops; with \
             the exact-echo gate shut the run ends NoEcho; then stage three (FBC-4plj), in a \
             session of its own on each account after its taker IOC: P1 and P2, each one post-only \
             GTC at the minimum size behind the touch, P3, then their cancels by venue id, while a \
             pager that never arms asks for get_user_orders at a page size of 1 between them; the \
             run ends after the last account's stage three, its report answering decision 0132's \
             list for each named account; a fill of ours other than the taker IOC's and a flatten \
             IOC's is flattened while its session is open: every other order of ours cancelled by \
             venue id, the owner's Flatten into Exit, then reducing limit IOCs at the minimum size \
             within --flatten-cap-bps under the account's one budget of --flatten-attempts, then \
             the run stops{held}",
            environment(env),
            env.mode(),
        )),
    }
    // A marker found is held to --reconciled as the lease directory opened, so both are here.
    if let Some(marker) = &run.ev.marker
        && let (false, Some(declared)) = (marker.found.is_empty(), marker.reconciled)
    {
        let found = places::stops_said(&marker.found);
        run.line(format_args!(
            "MARKER a stop marker from an earlier place-stage run ({found}): \
             --reconciled {declared}, the owner's declaration, which the probe does not check; \
             every named account is read after the preflight, and the marker is cleared only if \
             every read is clean (0151 item 6)"
        ));
    }
    if let Err(why) = run.stage().await {
        run.line(format_args!("STOPPED {why}"));
        run.ev.stopped = Some(why);
    }
    // What the whole run's frames show of our client ids and QFEX's statuses, for stage two's
    // report items (FBC-su9i), once a drop session ran.
    if !run.ev.drops.is_empty() {
        let kept = run.tee.kept();
        let two = run.ev.stage_two.get_or_insert_with(StageTwo::default);
        two.read(&kept);
    }
    run.mark();
    let Run {
        journal,
        mut ev,
        out,
        specs,
        ..
    } = run;
    // Every session has dropped its handle on the journal; this one goes too, and the writer
    // thread writes what is queued, the marker of a gap still open, and closes its segment.
    // Then the sink's drop counts, every class's, which nothing offers to now: a drop the
    // journal holds no marker for counts as much as one it does (decision 0006).
    drop(journal);
    let closed = writing.close();
    journal_closed(&mut ev, closed, sink.borrow().inner());
    drop(sink);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    let failed = match (places, &specs) {
        (Some(_), specs) => {
            let specs = specs.clone().unwrap_or_else(SpecTable::new);
            write_place_slice(dir, &mut ev, &specs, &script).err()
        }
        (None, _) => write_slice(dir, &mut ev, &script).err(),
    };
    ev.errors.extend(failed);
    let (report, json) = crate::report::outputs(&mut ev);
    fs::write(dir.join("report.md"), report).map_err(|e| format!("report.md: {e}"))?;
    fs::write(dir.join("evidence.json"), json).map_err(|e| format!("evidence.json: {e}"))?;
    let ok = ev.ok();
    let done = if ok { "ok" } else { "INCOMPLETE" };
    let _ = writeln!(
        out.borrow_mut(),
        "DONE {done}: {} ({} fixtures)",
        dir.display(),
        ev.fixtures.len()
    );
    Ok(Outcome { ok, evidence: ev })
}

/// The configuration every session and read shares: the probe mode of the run's environment
/// (`preprod_probe`, or `production_probe` with `--production`), its settings and the origins.
/// Each session's account and public key are added from the environment, and its account key
/// from the program's numbering ([`configured`]).
pub fn base_config(opts: &Options) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_EXEC_MODE, opts.environment().mode());
    cfg.insert(TRADE_URL, &opts.trade_url);
    cfg.insert(KEY_REST_URL, &opts.rest_url);
    cfg.insert(KEY_MDS_URL, &opts.mds_url);
    cfg.insert(KEY_PAGE_SIZE, &opts.page_size.to_string());
    let resync = format!("{}s", RESYNC_TIMEOUT.as_secs());
    cfg.insert(KEY_RESYNC_TIMEOUT, &resync);
    cfg.insert(KEY_RPC_TIMEOUT, &format!("{}ms", opts.rpc_timeout_ms));
    cfg
}

/// The configuration of a place stage's session on `armed` (decision 0148 item 4): `armed`'s
/// session configuration ([`configured`]) with `qfex.exec.production_places` set to
/// `probe_place_stages`, under which the production probe codec encodes a post-only GTC or a
/// limit IOC at the instrument's minimum size and refuses every other place. Only a place
/// stage's sessions take it: [`base_config`] never holds the key, so no stage-one session and no
/// read has it.
#[allow(dead_code)] // the place stages' driver opens its sessions on it (FBC-23ch)
pub fn place_config(
    base: &VenueConfig,
    lookup: Lookup<'_>,
    armed: Armed,
) -> Result<(VenueConfig, fbc_core::Secrets), String> {
    let (mut cfg, creds) = configured(base, lookup, armed)?;
    cfg.insert(KEY_PRODUCTION_PLACES, PROBE_PLACE_STAGES);
    Ok((cfg, creds))
}

/// The configuration of the session arming on `armed`, and its credentials: `base` with the
/// account and public key the environment gives it (`auth_qfex::session`) and the account key
/// the program numbers it by (decision 0122 item 1).
pub fn configured(
    base: &VenueConfig,
    lookup: Lookup<'_>,
    armed: Armed,
) -> Result<(VenueConfig, fbc_core::Secrets), String> {
    let (mut cfg, creds) = auth_qfex::session(base, lookup, armed)?;
    cfg.insert(ACCOUNT_KEY, &account_key(armed).get().to_string());
    Ok((cfg, creds))
}

/// Now on the local wall clock.
fn wall_ns() -> WallNs {
    let wall = SystemTime::now().duration_since(UNIX_EPOCH);
    WallNs(wall.map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)))
}

/// The seconds since the epoch now.
fn wall_secs() -> i64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH);
    now.map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// The records `sink` dropped since it was made, Normal then Safety ([`Evidence::dropped`]).
pub fn journal_dropped(sink: &QueueSink) -> [u64; 2] {
    [TrafficClass::Normal, TrafficClass::Safety].map(|class| sink.dropped(class))
}

/// What the run knows of its journal once the writer thread has closed (`closed`, what
/// `WriterThread::close` returned), which the slice holds the journal it reads to (decision
/// 0006): the error that stopped the writer, if one did ([`Evidence::writer_failed`], and among
/// the errors), and `sink`'s drop counts ([`Evidence::dropped`]), which nothing offers to now.
pub fn journal_closed(ev: &mut Evidence, closed: Result<(), JournalError>, sink: &QueueSink) {
    if let Err(e) = closed {
        ev.errors.push(format!("the journal's writer: {e}"));
        ev.writer_failed = Some(e.to_string());
    }
    ev.dropped = journal_dropped(sink);
}

/// Writes the fixture slice of the journal under `dir` into `dir/fixtures`, then runs the
/// privacy check `script` over it: fixtures it refuses are removed, never left to copy.
pub fn write_slice(dir: &Path, ev: &mut Evidence, script: &Path) -> Result<(), String> {
    let files = capture::slice(&dir.join("journal"), SHARD, ev)?;
    checked_fixtures(&dir.join("fixtures"), ev, script, |fixtures, ev| {
        capture::write_fixtures(fixtures, files, ev)
    })
}

/// The directory a place-stage run writes its fixture slice into (0151 item 7), to commit as
/// `fixtures/qfex/exec-recorded/drop-production/` once the owner has read every file.
pub const DROP_SLICE: &str = "drop-production";

/// The directory a place-stage run writes stage two's crossing sessions' frames into (0151 item
/// 7, FBC-su9i), to commit as `fixtures/qfex/exec-recorded/places-production/` once the owner
/// has read every file.
pub const PLACES_SLICE: &str = "places-production";

/// The directory a place-stage run writes stage three's sessions' frames and its pagers' pages
/// into (decision 0148 item 10, FBC-4plj), to commit as
/// `fixtures/qfex/exec-recorded/pages-production/` once the owner has read every file.
pub const PAGES_SLICE: &str = "pages-production";

/// A place-stage run's [`write_slice`]: the slice scrubbed by `capture::Scrub::Places`
/// (decision 0148 item 10), each resync page read on `specs`, the spec table its sessions held,
/// the drop test's sessions' frames written into `dir/drop-production/` with its README; when a
/// crossing session ran, stage two's into `dir/places-production/` with its own; and when a
/// stage-three session ran, its frames and its pager's pages into `dir/pages-production/` with
/// its own; each then checked by `script`.
pub fn write_place_slice(
    dir: &Path,
    ev: &mut Evidence,
    specs: &SpecTable,
    script: &Path,
) -> Result<(), String> {
    let files = capture::slice_places(&dir.join("journal"), SHARD, ev, specs)?;
    let (crossing, files) = capture::split_crossing(ev, files);
    let (mut paging, files) = capture::split_paging(ev, files);
    let crossed = ev.sessions.iter().any(capture::is_crossing);
    let paged = ev.sessions.iter().any(capture::is_paging);
    checked_fixtures(&dir.join(DROP_SLICE), ev, script, |fixtures, ev| {
        capture::write_fixtures_scrubbed(fixtures, files, ev, capture::Scrub::Places)
    })?;
    if crossed {
        checked_fixtures(&dir.join(PLACES_SLICE), ev, script, |fixtures, ev| {
            capture::write_crossing_fixtures(fixtures, crossing, ev)
        })?;
    }
    if !paged {
        return Ok(());
    }
    paging.extend(capture::pager_files(ev));
    checked_fixtures(&dir.join(PAGES_SLICE), ev, script, |fixtures, ev| {
        capture::write_pages_fixtures(fixtures, paging, ev)
    })
}

/// Writes `fixtures` with `write`, then runs the privacy check `script` over it: fixtures it
/// refuses are removed, never left to copy.
fn checked_fixtures(
    fixtures: &Path,
    ev: &mut Evidence,
    script: &Path,
    write: impl FnOnce(&Path, &mut Evidence) -> Result<(), String>,
) -> Result<(), String> {
    write(fixtures, ev)?;
    let checked = evidence::privacy_check(script, fixtures);
    if checked.is_err() {
        let _ = fs::remove_dir_all(fixtures);
        ev.fixtures.clear();
    }
    checked.map_err(|e| format!("{e}; the fixtures directory was removed"))
}

/// What a never-arming read socket of an account is made of ([`Run::socket_parts`]): its
/// configuration, which the guard admitted, its reader, and the context of each signed frame or
/// request.
struct SocketParts {
    cfg: VenueConfig,
    reader: ReadAuth,
    ctx: Box<dyn FnMut() -> EncodeCtx>,
}

/// One run's shared state.
struct Run<'a> {
    opts: &'a Options,
    lookup: Lookup<'a>,
    out: Out,
    base: VenueConfig,
    /// The probe mode's caps, whose scope decodes the reads.
    caps: VenueCaps,
    connector: Connector,
    limiter: RateLimiter,
    nonces: Nonces,
    clock: IngestClock,
    journal: Journal,
    /// The run's monotonic origin, on the runtime's clock, taken just after `clock`'s.
    origin: tokio::time::Instant,
    /// The environment the run's probe mode names, which its registries are built for.
    environment: Environment,
    /// The owner's interrupt, until the hold takes it.
    interrupt: Option<Interrupt>,
    ev: Evidence,
    /// The accounts the run's configuration names, read before every session ([`named`]).
    named: Vec<Armed>,
    /// What a place-stage run asks for (`--place-stages`): `None` for stage one.
    places: Option<&'a Places>,
    /// The owner's presses a place-stage run takes outside the hold ([`Presses`]): none for
    /// stage one.
    presses: Presses,
    /// A place-stage run's lease directory, namespace lease and mint ([`RunLeases`]).
    leases: Option<RunLeases>,
    /// The place stages' stop, once one stopped the run ([`Run::halt`]).
    stop: Option<Stop>,
    /// What the journal's tee keeps of the sessions' frames, and the stops it raises
    /// ([`crate::tee`]).
    tee: Tap,
    /// The request ids every session of the run shares, so no two requests of the run share an
    /// id in its one journal.
    rpc_ids: RpcIds,
    /// The spec table a place-stage run's sessions hold, once its preflight discovered it: the
    /// slice reads their pages on it.
    specs: Option<SpecTable>,
    /// Until when a read waits for the shared limiter's room ([`Run::admitted`]): set for the
    /// reads after a place stage's session alone.
    patient: Option<tokio::time::Instant>,
    /// The account reads' `GET /user/positions` answered HTTP 429 or 418 so far: provider rate
    /// refusals of REST, which the shared limiter does not count ([`Run::read_account`]).
    rest_refused: u64,
    /// The accounts whose reads could not be made so far, one entry per read ([`Run::read_one`]),
    /// which the evidence's reads do not hold.
    unread: Vec<Armed>,
    /// Every place-stage registry the run built, on every named account, for stage two's
    /// [`crate::stage_two::no_other_order`] (decision 0148 item 6).
    built: Vec<BuiltReg>,
    /// Each named account's allowance once its drop test's sessions ran, which its later
    /// registries spend from (decision 0148 item 7: one flatten budget per account).
    allowances: Vec<(Armed, Allowance)>,
    /// When the current taker IOC session's taker IOC was handed to the session, in whole Unix
    /// seconds: where its trades are read from after the session ([`Run::take_trades`]).
    taker_secs: Option<u64>,
}

/// A place-stage registry the run built ([`Run::built`]): its account, its step, its `Link`, and,
/// for a drop registry, X and Y as placed, which count ended only as 0151 item 5 rules.
struct BuiltReg {
    on: Armed,
    step: Step,
    link: Rc<RefCell<Link>>,
    xy: Vec<fbc_core::ClientOrderId>,
}

impl Run<'_> {
    fn line(&self, args: fmt::Arguments<'_>) {
        let _ = writeln!(self.out.borrow_mut(), "{args}");
    }

    /// Records `stop`, met `when`, as the run's: the first stop is the run's, by name in the
    /// evidence. Returns what the run stopped with, its name and what it is, never an id or an
    /// amount.
    fn halt(&mut self, stop: Stop, when: &str) -> String {
        self.halt_as(stop, when, None)
    }

    /// [`Run::halt`], `says` given in place of [`Stop::says`] when `stop` is the run's: what an
    /// order counted resting, or left unjudged, after a session is said as, never as seen
    /// resting (FBC-gisx). An earlier stop is said as itself.
    fn halt_as(&mut self, stop: Stop, when: &str, says: Option<&str>) -> String {
        let first = *self.stop.get_or_insert(stop);
        self.ev.stop = Some(first.name());
        stop_said(first, stop, when, says)
    }

    /// Writes the stop marker for the run's stop, when it marks one (0151 item 6), in its lease
    /// directory, keeping the stops of the marker it found there unless it cleared it, and
    /// records what it wrote, or why it could not, in the evidence.
    fn mark(&mut self) {
        let (Some(stop), Some(leases), Some(marker)) =
            (self.stop, &self.leases, self.ev.marker.as_mut())
        else {
            return;
        };
        let earlier = match marker.cleared {
            true => &[][..],
            false => &leases.carried[..],
        };
        match places::write_marker(&leases.dir, stop, earlier, &self.ev.started) {
            // A stop that is not a fault writes none.
            Ok(written) => {
                marker.written = written.then_some(stop.name());
                if written {
                    let _ = writeln!(
                        self.out.borrow_mut(),
                        "MARKER written ({}): the next place-stage run needs --reconciled (0151 \
                         item 6)",
                        stop.name()
                    );
                }
            }
            Err(e) => {
                marker.error = Some(e.clone());
                self.ev.errors.push(e);
            }
        }
    }

    /// The hold, when the run asks for one, then each session, each after the checks of every
    /// account the configuration names; a place-stage run's preflight, then its reads that clear
    /// a stop marker and its place stages, the MDS touch driven beside them from the preflight's
    /// first touch until they end (FBC-kllr).
    async fn stage(&mut self) -> Result<(), String> {
        self.line(format_args!(
            "ACCOUNTS {} named by the run's configuration, not listed by QFEX (decision 0144): {}",
            self.ev.accounts.len(),
            self.ev.accounts.join(", ")
        ));
        if let Some(hold) = self.opts.hold {
            self.hold(hold).await?;
        }
        if let Some(places) = self.places {
            // After the hold, so the MDS touch is not open while it holds (decision 0146). A
            // press made before it, outside the hold, is taken as it starts.
            let mut pre = Preflight::default();
            let mut presses = std::mem::take(&mut self.presses);
            let passed = unless_pressed(&mut presses, self.preflight(places, &mut pre)).await;
            self.presses = presses;
            self.ev.preflight = Some(pre);
            let (market, mut touch) = match passed {
                Some(passed) => passed?,
                None => return Err(self.halt(Stop::Interrupted, "during the preflight")),
            };
            // The MDS touch driven from its first read until the place stages end, between the
            // sessions as well as during them, so no read, wait or close between two sessions
            // leaves its pings unanswered (FBC-kllr). The stages' future is put on the heap, so
            // every run's future, stage one's too, holds it by pointer: inline, beside the touch,
            // it overflowed a debug build's test thread's stack.
            let stages = Box::pin(async {
                let read = self.reconcile().await?;
                self.drop_tests(places, market, read).await
            });
            return touch.driven(stages).await;
        }
        let arms = self.opts.arms.clone();
        for (n, armed) in arms.into_iter().enumerate() {
            let (cfg, creds) = configured(&self.base, self.lookup, armed)?;
            self.check_accounts(name(armed), armed, " (armed)").await?;
            let bounds = (self.opts.ledger.as_ref()).ok_or("--arm without the ledger's bounds")?;
            // Stage one's registries serve one session each: each arms its own account.
            let opened = Opened::now(self.origin, wall_ns());
            let label = name(armed);
            let mut registry =
                RegistryLedger::new(label, account_key(armed), bounds, opened, self.environment);
            let ran = self
                .session(n, armed, cfg, creds, registry.deadline())
                .await;
            registry.session_ended(label, self.mono());
            self.ev.registries.push(registry.run().clone());
            self.ev.sessions.push(ran?);
        }
        Ok(())
    }

    /// With `--reconciled` and a stop marker found (0151 item 6), reads every named account, as
    /// before drop session one ([`places::DROP_ONE`]), and clears the marker only if every read
    /// is clean; a read that is not clean stops the run with the marker kept, and a provider rate
    /// refusal they met is the run's stop, marked before the stops kept ([`Run::unless_refused`]).
    /// Without them it
    /// reads nothing. Whether it read every named account, clean, so drop session one needs no
    /// reads of its own before it.
    async fn reconcile(&mut self) -> Result<bool, String> {
        let Some(leases) = self.leases.as_ref().filter(|l| l.reconciling()) else {
            return Ok(false);
        };
        let marked = leases.marked.clone();
        let mark = self.read_mark();
        let mut presses = std::mem::take(&mut self.presses);
        let on = self.named[0];
        let reads = self.check_accounts(places::DROP_ONE, on, " (drop)");
        let read = unless_pressed(&mut presses, reads).await;
        self.presses = presses;
        // A provider rate refusal they met is the run's stop, the marker kept with it.
        let when = "in the reads that clear the marker";
        let read = read.map(|reads| self.unless_refused(mark, reads, when));
        match read {
            None => Err(self.halt(Stop::Interrupted, "during the reads that clear the marker")),
            Some(Err(why)) => Err(format!("{why}; the stop marker is kept (0151 item 6)")),
            Some(Ok(())) => {
                let cleared = places::clear_markers(&marked);
                let marker = self.ev.marker.get_or_insert_with(Default::default);
                match cleared {
                    Ok(()) => {
                        marker.cleared = true;
                        self.line(format_args!(
                            "MARKER cleared: every named account read clean (0151 item 6)"
                        ));
                        Ok(true)
                    }
                    Err(e) => {
                        marker.error = Some(e.clone());
                        Err(e)
                    }
                }
            }
        }
    }

    /// The place stages' preflight ([`crate::market`]), each step it passes recorded in `pre`:
    /// the spec from this run's `GET /refdata` on the configuration the guard admitted, the count
    /// bound against each place registry's ceiling, the MDS touch, and the caps at it. No account
    /// connects, and nothing authenticated opens. What it gives the drop session: the spec, the
    /// registries' bounds, the caps in lots and the touch's view, and the touch itself, still
    /// open, for [`Run::stage`] to drive.
    async fn preflight(
        &mut self,
        places: &Places,
        pre: &mut Preflight,
    ) -> Result<(Market, market::Touch), String> {
        let (cfg, _) = configured(&self.base, self.lookup, self.named[0])?;
        let cfg = market::market_config(&cfg, self.opts.read_wait);
        self.line(format_args!(
            "PREFLIGHT the instrument's spec from GET /refdata on the guarded REST origin"
        ));
        let found = market::discover_spec(&cfg, &places.symbol, &self.connector, &self.limiter);
        let found = found.await?;
        pre.passed.push(market::DISCOVERED);
        let parsed =
            (self.opts.ledger.as_ref()).ok_or("--place-stages without the ledger's bounds")?;
        let bounds = market::count_bound(parsed, &found, places)?;
        pre.passed.push(market::COUNTED);
        self.line(format_args!(
            "PREFLIGHT the touch from the guarded MDS origin"
        ));
        let opened = market::Touch::open(
            &cfg,
            &found,
            &self.connector,
            &self.clock,
            self.opts.connect_wait,
        );
        let within = self.opts.connect_wait + self.opts.read_wait;
        let mut touch = opened?;
        let quote = touch.first(within, places.touch_max_age).await?;
        pre.passed.push(market::TOUCHED);
        let caps = market::place_caps(places, &found, quote)?;
        pre.passed.push(market::CAPPED);
        self.line(format_args!(
            "PREFLIGHT passed: the spec, the count bound, the touch and the caps"
        ));
        self.specs = Some(found.specs.clone());
        let market = Market {
            found,
            bounds,
            caps,
            touch: touch.view(),
        };
        Ok((market, touch))
    }

    /// The client-id namespace reads decode ours by: a place-stage run's `--namespace`, stage
    /// one's own otherwise.
    fn ns(&self) -> Namespace {
        self.places.map_or(NS, |p| Namespace::new(p.namespace))
    }

    /// Now on the run's monotonic clock: the runtime's, from just after the sessions'
    /// `IngestClock` started on it ([`ledger::mono_since`]).
    fn mono(&self) -> MonoNs {
        ledger::mono_since(self.origin)
    }

    /// The context of one signed read: now, and two nonces from the run's source.
    fn ctx(&mut self) -> EncodeCtx {
        EncodeCtx {
            wall: wall_ns(),
            mono: self.mono(),
            nonces: self.nonces.reserve(ReadAuth::NONCES),
        }
    }

    /// Charges one request to the run's limiter, or refuses it, nothing sent, when the buckets
    /// have no room for it now ([`admit`]).
    fn admit(&self, charge: RateCharge, via: Via, class: TrafficClass) -> Result<(), String> {
        admit(&self.limiter, charge, via, class)
    }

    /// [`Run::admit`], but while [`Run::patient`] is set, a request the buckets have no room
    /// for yet waits for the room, when it comes before then, and is charged then: the reads
    /// after a place stage's session, which must be made (decision 0132 item 7), wait out the
    /// hold a provider rate refusal put on the shared budget (decision 0126) rather than skip.
    async fn admitted(
        &self,
        charge: RateCharge,
        via: Via,
        class: TrafficClass,
    ) -> Result<(), String> {
        loop {
            let request = fbc_runtime::Request { charge, via, class };
            let now = tokio::time::Instant::now();
            let refused = match self.limiter.charge(now, NO_CONN, &[request]) {
                Ok(_) => return Ok(()),
                Err(refused) => refused,
            };
            match (refused.ready_at, self.patient) {
                (Some(ready), Some(until)) if ready <= until => {
                    tokio::time::sleep_until(ready).await;
                }
                _ => return Err("not sent: the rate limit has no room".to_owned()),
            }
        }
    }

    async fn rest(&self, read: &RestRead) -> Result<(u16, Vec<u8>), String> {
        self.admitted(read.charge, Via::Http, read.class).await?;
        auth_qfex::send(&self.connector, &read.req, self.opts.read_wait).await
    }

    /// Closes a read socket, its close frame charged to the shared limiter first as a control
    /// frame, as a session charges its own (decision 0030), and as every frame of the read is
    /// ([`Run::admitted`]); with no room, under a provider hold say (decision 0126), it is
    /// dropped with no close frame, as the hold's and a session's are. A close the peer refuses
    /// is passed over (it may have closed first); one it never takes within `--read-wait-ms` is
    /// an error, so the run cannot hang on it.
    async fn close_read(&self, mut ws: WebSocket) -> Result<(), String> {
        let admitted = self.admitted(CONTROL.charge, CONTROL.via, CONTROL.class);
        if admitted.await.is_err() {
            return Ok(());
        }
        let wait = self.opts.read_wait;
        let closed = tokio::time::timeout(wait, ws.close(None)).await;
        closed.map(|_| ()).map_err(|_| {
            format!(
                "the read socket did not close within {} ms",
                wait.as_millis()
            )
        })
    }

    /// Writes one of the adapter's frames on a read socket or the hold's connection, waiting at
    /// most `--read-wait-ms` for a peer that stopped reading.
    async fn write(&self, ws: &mut WebSocket, write: &SocketWrite) -> Result<(), String> {
        self.admitted(write.charge, Via::Frame, write.class).await?;
        let wait = self.opts.read_wait;
        let written = tokio::time::timeout(wait, auth_qfex::write(ws, &write.frame)).await;
        written.map_err(|_| format!("the socket took no frame within {} ms", wait.as_millis()))?
    }

    /// Reads every account the configuration names before `before`, the session on `on` or the
    /// hold ([`HOLD`]) on it, `on`'s label marked `mark`, each under its own configuration
    /// ([`configured`]), which the guard admitted before anything connected; stops the probe
    /// at the first that cannot be read, or holds an open order, a TWAP or a position that is
    /// not flat.
    async fn check_accounts(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
    ) -> Result<(), String> {
        self.check_accounts_allowing(before, on, mark, None).await
    }

    /// Reads every account the configuration names as [`Run::check_accounts`] does, but admits
    /// `allowed`, when given, on its account alone: a page listing it and a REST count of it are
    /// clean there ([`AccountRead::refusal`]). Decision 0132 item 7's one exception, X before the
    /// drop test's second session (FBC-77r8, [`Run::read_before_two`]); every other read admits
    /// none.
    async fn check_accounts_allowing(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        allowed: Option<&AllowedOrder>,
    ) -> Result<(), String> {
        for account in self.named.clone() {
            self.check_account(before, on, mark, account, allowed)
                .await?;
        }
        Ok(())
    }

    /// Reads every account the configuration names after the hold on `on` ([`AFTER_HOLD`]), as
    /// [`Run::check_accounts`] does, but each whatever another showed, so the evidence holds
    /// every account's read after the hold (decision 0146 item 3); stops the probe once all were
    /// read when any could not be read or is not clean, naming each.
    async fn check_accounts_after_hold(&mut self, on: Armed) -> Result<(), String> {
        let mut failed = Vec::new();
        for account in self.named.clone() {
            let checked = (self.check_account(AFTER_HOLD, on, " (held)", account, None)).await;
            failed.extend(checked.err());
        }
        match failed.is_empty() {
            true => Ok(()),
            false => Err(failed.join("; ")),
        }
    }

    /// Reads `account` before `before`, as [`Run::check_accounts_allowing`] reads each, admitting
    /// `allowed` when it is on `account`.
    async fn check_account(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        account: Armed,
        allowed: Option<&AllowedOrder>,
    ) -> Result<(), String> {
        let mut failed = places::AccountUnread::Positions;
        let admits = (allowed, &[][..]);
        let read = self.read_one(before, on, mark, account, admits, &mut failed);
        let read = read.await?;
        let label = &read.label;
        if let Some(why) = read.refusal() {
            let starts = match before {
                HOLD => "no hold starts (decision 0146 item 3)",
                AFTER_HOLD => {
                    "read after the hold, though every account showed none before it: what \
                     decision 0146 says would show it wrong; no session starts"
                }
                _ => "no session starts (decision 0132 item 7)",
            };
            return Err(format!("{label} {why}: {starts}"));
        }
        Ok(())
    }

    /// Reads `account` before `before` (on `on`, its label marked `mark`), admitting `allowed`
    /// when it is on it, and records the read in the evidence and on a `READ` line: the read, or
    /// why it could not be made, `account` then kept among the reads not made
    /// ([`Run::unread`]) and `failed` naming the step that failed (FBC-gisx). Its read socket's
    /// refusals are recorded either way.
    async fn read_one(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        account: Armed,
        admits: (Option<&AllowedOrder>, &[&AllowedOrder]),
        failed: &mut places::AccountUnread,
    ) -> Result<AccountRead, String> {
        let read = self.reading_one(before, on, mark, account, admits, failed);
        let read = read.await;
        // Whether the account's last read was made, for the hand actions the run leaves.
        self.ev.last_unread.retain(|a| *a != name(account));
        if read.is_err() {
            self.unread.push(account);
            self.ev.last_unread.push(name(account));
        }
        read
    }

    /// [`Run::read_one`]'s read.
    async fn reading_one(
        &mut self,
        before: &'static str,
        on: Armed,
        mark: &'static str,
        account: Armed,
        (allowed, known): (Option<&AllowedOrder>, &[&AllowedOrder]),
        failed: &mut places::AccountUnread,
    ) -> Result<AccountRead, String> {
        let mut label = name(account).to_owned();
        label.push_str(if account == on { mark } else { "" });
        let (cfg, _) = configured(&self.base, self.lookup, account)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let mut met = None;
        let admits = (allowed, account);
        let read = self.read_account(&cfg, selection, admits, known, (&mut met, failed));
        let read = read.await;
        // A read socket's refusals are reported whether or not its read succeeded.
        if let Some(refusals) = met {
            self.ev.read_sockets.push(ReadSocket {
                before,
                label: label.clone(),
                refusals,
            });
        }
        let read = read.map_err(|e| format!("{label} could not be read: {e}"))?;
        let shown = format!(
            "{} open orders, {} TWAPs, flat {}, REST open_orders {}",
            read.page_orders, read.page_twaps, read.flat, read.rest_open_orders
        );
        let when = refusals::read_when(before);
        self.line(format_args!("READ {when}: {label}: {shown}"));
        let read = AccountRead {
            before,
            label,
            ..read
        };
        self.ev.reads.push(read.clone());
        Ok(read)
    }

    /// One account's positions, and its open orders on a trade socket of its own, counting
    /// those that are `admits.0` when it is on `admits.1`, the account read
    /// ([`AccountRead::of`]), and those that are none of `known` on it
    /// ([`AccountRead::page_unknown`]); `met` is the provider rate refusals that socket met, once
    /// it opened, and `failed` the step it reached past `GET /user/positions`, which names the
    /// one that failed when it fails.
    async fn read_account(
        &mut self,
        cfg: &VenueConfig,
        account: AccountSelection<'_>,
        admits: (Option<&AllowedOrder>, Armed),
        known: &[&AllowedOrder],
        (met, failed): (&mut Option<Refusals>, &mut places::AccountUnread),
    ) -> Result<AccountRead, String> {
        let reader = auth_qfex::reader(self.lookup, account)?;
        let ctx = self.ctx();
        let read = account_reads::positions(cfg, &reader, &ctx).map_err(|e| e.to_string())?;
        let (status, body) = self.rest(&read).await?;
        if matches!(status, 429 | 418) {
            self.rest_refused += 1;
            return Err(format!(
                "GET /user/positions: HTTP {status}, a provider rate refusal"
            ));
        }
        let resp = HttpResponse {
            status,
            headers: &[],
            body: &body,
        };
        let positions = dispatch(&self.caps, self.ns(), |scope| {
            account_reads::decode_positions(&resp, scope)
        });
        let positions = positions.map_err(|e| format!("GET /user/positions: {e}"))?;
        *failed = places::AccountUnread::Socket;
        let url = account_reads::trade_url(cfg, &reader).map_err(|e| e.to_string())?;
        let mut ws = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await?;
        let before = self.rejected();
        let mut heard = Refusals::default();
        let page = self.page(cfg, &reader, &mut ws, &mut heard, failed).await;
        heard.limiter = self.rejected().saturating_sub(before);
        *met = Some(heard);
        let closed = self.close_read(ws).await;
        let page = page?;
        closed?;
        let mut read = AccountRead::of(&positions, &page, admits.0, admits.1);
        read.page_unknown = AllowedOrder::unknown(known, admits.1, page.orders());
        Ok(read)
    }

    /// The socket's authentication, its success, then one `get_user_orders` page from offset 0;
    /// each of the two frames it reads counted in `heard` ([`Run::heard`]), which holds the
    /// shared budget for a `RateLimited` err in either. `failed` is [`places::AccountUnread::Page`]
    /// when the page's answer came and did not decode, and is left as it was otherwise.
    async fn page(
        &mut self,
        cfg: &VenueConfig,
        reader: &ReadAuth,
        ws: &mut WebSocket,
        heard: &mut Refusals,
        failed: &mut places::AccountUnread,
    ) -> Result<OrdersPage, String> {
        let ctx = self.ctx();
        let auth = account_reads::authentication(cfg, reader, &ctx).map_err(|e| e.to_string())?;
        let ask = PageAsk::new(self.opts.page_size, 0).map_err(|e| e.to_string())?;
        self.write(ws, &auth).await?;
        let reply = next(ws, self.opts.read_wait, &self.limiter).await?;
        let _ = self.heard(heard, reader, &reply, ask);
        if !account_reads::authenticated(reader, reply.raw()) {
            return Err("QFEX did not authenticate the read socket".to_owned());
        }
        let ask_frame = account_reads::user_orders(cfg, ask).map_err(|e| e.to_string())?;
        self.write(ws, &ask_frame).await?;
        let reply = next(ws, self.opts.read_wait, &self.limiter).await?;
        let page = self.heard(heard, reader, &reply, ask);
        page.map_err(|e| {
            *failed = places::AccountUnread::Page;
            format!("get_user_orders: {e}")
        })
    }

    /// Counts `frame`, off `reader`'s read socket, in `heard` ([`Refusals::hear`]): as a journal
    /// would keep it, its spans the adapter's (`account_reads::redact_inbound`, blanked by
    /// [`refusals::kept`]), and, when that is unread, as it arrived, in memory only. Then reads it
    /// as the adapter reads a page, which reads an err before anything else: a `RateLimited` err,
    /// whether it answers the authentication or the page, holds the shared budget, as a session's
    /// runtime holds it for its codec's errs in every state (decision 0126); one the journal
    /// would keep whole holds none, as the codec holds none for it (0134 item 6).
    fn heard(
        &self,
        heard: &mut Refusals,
        reader: &ReadAuth,
        frame: &Data,
        ask: PageAsk,
    ) -> Result<OrdersPage, NotAPage> {
        let raw = frame.raw();
        let spans = account_reads::redact_inbound(reader, Inbound::Frame(raw));
        heard.hear(
            &refusals::kept(raw, &spans),
            &String::from_utf8_lossy(raw.bytes()),
        );
        let page = dispatch(&self.caps, self.ns(), |scope| {
            account_reads::orders_page(reader, raw, ask, scope)
        });
        if let Err(NotAPage::RateLimited(hold)) = &page {
            let now = tokio::time::Instant::now();
            (self.limiter).provider_rejected(now, NO_CONN, hold.charge, hold.retry_after);
        }
        page
    }

    /// The run's limiter's count of provider refusals so far: its user count, every one it
    /// counted, since its buckets are all user-scoped (decision 0123).
    fn rejected(&self) -> u64 {
        self.limiter.counts().rejected.user
    }

    /// The liveness hold (decision 0146) on `hold.account`: every named account read as before
    /// a session, then its one connection ([`Run::held`]), then, once that connection opened,
    /// every named account read again, whatever ended it and whatever another account's read
    /// after it showed ([`Run::check_accounts_after_hold`], item 3). What it heard is the
    /// evidence's whether or not it did what the record asks. One that did not, an account read
    /// after it that shows an open order or a position, and the owner's interrupt in a run that
    /// also arms each stop the probe, as a refused read does, so no session starts.
    ///
    /// In a place-stage run the owner's Ctrl-C outside the hold's connection ([`Presses`]) is
    /// the hold's too, never a stop of the place stages, so it writes no stop marker: one during
    /// the reads before the hold or its connect keeps the connection from opening
    /// ([`INTERRUPTED_BEFORE_HOLD`]), and one during the reads after it, which run to their end
    /// first, starts no preflight ([`INTERRUPTED_AFTER_HOLD`]). `main.rs`'s runtime has one
    /// thread, so from the connection's opening, before anything awaits, each press is the
    /// connection's.
    async fn hold(&mut self, hold: Hold) -> Result<(), String> {
        let mut presses = std::mem::take(&mut self.presses);
        let opening = unless_pressed(&mut presses, self.open_hold(hold)).await;
        self.presses = presses;
        let (mut run, before, connected) = match opening {
            None => return Err(INTERRUPTED_BEFORE_HOLD.to_owned()),
            Some(opening) => opening?,
        };
        let label = run.account;
        let held = self.held(&mut run, connected).await;
        run.refusals.limiter = self.rejected().saturating_sub(before);
        if let Err(why) = held {
            run.error.get_or_insert(why);
        }
        let pings = run.pings();
        let closes = run.server_closes().len();
        self.line(format_args!(
            "HOLD {label}: ended by {}; {} pings ({} before authentication), {closes} server \
             closes",
            run.ended_by.says(),
            pings.len(),
            run.pings_before_auth()
        ));
        let mut why = run.stops();
        if why.is_none() && (!self.opts.arms.is_empty() || self.places.is_some()) {
            // What the interrupt stops: a place-stage run's preflight (FBC-c6kw), or the sessions.
            let next = match self.places {
                Some(_) => "no preflight starts",
                None => "no session starts",
            };
            why = match (&run.ended_by, run.interrupted_closing) {
                (End::Interrupted, _) => Some(format!(
                    "the owner's interrupt (Ctrl-C) ended the hold: {next}"
                )),
                (_, true) => Some(format!(
                    "the owner's interrupt (Ctrl-C) came as the hold closed: {next}"
                )),
                _ => None,
            };
        }
        // The reads after it, once its connection opened: a hold that never opened held nothing.
        if run.opened {
            let after = self.check_accounts_after_hold(hold.account).await;
            run.read_after = match after {
                Ok(()) => ReadAfter::Clean,
                Err(e) => {
                    let after = format!("after the hold: {e}");
                    why = Some(match why {
                        Some(why) => format!("{why}; {after}"),
                        None => after,
                    });
                    ReadAfter::Stopped(e)
                }
            };
            if self.presses.pressed().now_or_never().is_some() {
                why = Some(match why {
                    Some(why) => format!("{why}; {INTERRUPTED_AFTER_HOLD}"),
                    None => INTERRUPTED_AFTER_HOLD.to_owned(),
                });
            }
        }
        self.ev.hold = Some(run);
        why.map_or(Ok(()), Err)
    }

    /// The hold's beginning: every named account read as before a session, its `HOLD` line,
    /// its record, the limiter's count of refusals before its connection, and that connection
    /// opened ([`Run::connect_hold`]). A read that is not clean is returned; an error opening the
    /// connection is the hold's, in the connection's place.
    async fn open_hold(
        &mut self,
        hold: Hold,
    ) -> Result<(HoldRun, u64, Result<Connected, String>), String> {
        self.check_accounts(HOLD, hold.account, " (held)").await?;
        let label = name(hold.account);
        self.line(format_args!(
            "HOLD {label}: one trade connection for {} s: its authentication and three \
             subscriptions, then nothing; it never arms or places",
            hold.duration.as_secs()
        ));
        let run = HoldRun {
            account: label,
            duration: hold.duration,
            ..HoldRun::default()
        };
        let before = self.rejected();
        let connected = self.connect_hold(hold).await;
        Ok((run, before, connected))
    }

    /// The hold's connection opened on `hold.account`'s guarded trade origin, with what it
    /// writes on it.
    async fn connect_hold(&self, hold: Hold) -> Result<Connected, String> {
        let (cfg, _) = configured(&self.base, self.lookup, hold.account)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let reader = auth_qfex::reader(self.lookup, selection)?;
        let url = account_reads::trade_url(&cfg, &reader).map_err(|e| e.to_string())?;
        let subscriptions = account_reads::subscriptions(&cfg).map_err(|e| e.to_string())?;
        let ws = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await;
        let ws = ws.map_err(of_the_hold)?;
        Ok(Connected {
            cfg,
            reader,
            subscriptions,
            ws,
        })
    }

    /// Charges the hold's own close to the run's limiter as a control frame, as a session
    /// charges its own: refused, nothing sent, when the buckets have no room for it.
    fn admit_close(&self) -> Result<(), String> {
        self.admit(CONTROL.charge, CONTROL.via, CONTROL.class)
    }

    /// The hold's connection, `connected` ([`Run::connect_hold`]), held until it ends
    /// ([`Run::holding`]), then closed: at its duration, the owner's interrupt or a
    /// refusal, a close and the server's echo read for at most `--read-wait-ms`; at a server
    /// close, read on until the close reply has gone out; an err read meanwhile is kept as its
    /// refusal, unless one already refused it. The owner's interrupt is the hold's
    /// from the connection's opening until then: a press that did not end it, made while it
    /// closed or ready as something else ended it, is recorded as one made as it closed
    /// (`run.interrupted_closing`). An error before it opened is returned; everything after is
    /// in `run`.
    async fn held(
        &mut self,
        run: &mut HoldRun,
        connected: Result<Connected, String>,
    ) -> Result<(), String> {
        let Connected {
            cfg,
            reader,
            subscriptions,
            mut ws,
        } = connected?;
        // The owner's interrupt, which only this connection waits on, and which goes with it.
        let mut interrupt =
            (self.interrupt.take()).unwrap_or_else(|| Box::pin(std::future::pending()));
        let opened = tokio::time::Instant::now();
        run.opened = true;
        // Polled now, before anything is written, so the interrupt listens from the
        // connection's opening (`main.rs` hands it each Ctrl-C from its first poll on).
        let ended = match interrupt.as_mut().now_or_never() {
            Some(()) => End::Interrupted,
            None => {
                self.holding(
                    &cfg,
                    &reader,
                    &subscriptions,
                    &mut ws,
                    opened,
                    &mut interrupt,
                    run,
                )
                .await
            }
        };
        run.ended_at = Some(opened.elapsed());
        let wait = self.opts.read_wait;
        let mut drain = ended != End::NoCloseFrame;
        if matches!(ended, End::Duration | End::Interrupted | End::Refused) {
            match self.admit_close() {
                Ok(()) => {
                    // From here a close read is the echo of its own, though the write may not
                    // finish: the next read flushes a close queued and not yet written.
                    run.close_begun = Some(opened.elapsed());
                    let closed = tokio::time::timeout(wait, ws.close(None)).await;
                    if matches!(closed, Ok(Ok(()))) {
                        run.close_sent = Some(opened.elapsed());
                    }
                }
                // No room for it: the connection is dropped with no close, as a session's is.
                Err(_) => {
                    run.close_refused = true;
                    drain = false;
                }
            }
        }
        if drain {
            // The server's echo of the close, or, after the server's close, the reply the
            // WebSocket layer writes on the next read; then the stream's end. A data frame
            // still in flight is read meanwhile: an err among them is a refusal as one that
            // ended the hold is (decision 0146 item 4), the first kept, so the hold is not what
            // the record asks and a run that also arms starts no session.
            let drained = async {
                while let Some(Ok(msg)) = ws.next().await {
                    if let Some(data) = hear(run, opened.elapsed(), msg, &self.limiter)
                        && let Some(err) = self.hold_data(run, &reader, &data)
                    {
                        run.error
                            .get_or_insert_with(|| format!("{err}, read as the hold closed"));
                    }
                }
            };
            let _ = tokio::time::timeout(wait, drained).await;
        }
        // A press while it closed, or one ready as its duration ended it and the wait took the
        // duration, is the owner's interrupt as much as one that ended it. An interrupt that
        // ended the hold is not polled again.
        run.interrupted_closing =
            ended != End::Interrupted && interrupt.as_mut().now_or_never().is_some();
        drop(ws);
        drop(interrupt);
        run.ended_by = ended;
        Ok(())
    }

    /// The hold's connection from its opening to its end: its authentication, then, once QFEX
    /// answers it with success, the three subscriptions, then nothing; every frame it receives
    /// recorded ([`hear`], [`Run::hold_data`]). It ends at its duration from `opened`, at
    /// `interrupt`, at a server close, at the connection's end, or at a refusal (`run.error`
    /// says which): the authentication refused, a frame that is an err, or the three
    /// confirmations not all in within QFEX's authentication deadline.
    #[allow(clippy::too_many_arguments)]
    async fn holding(
        &mut self,
        cfg: &VenueConfig,
        reader: &ReadAuth,
        subscriptions: &[SocketWrite],
        ws: &mut WebSocket,
        opened: tokio::time::Instant,
        interrupt: &mut Interrupt,
        run: &mut HoldRun,
    ) -> End {
        fn refused(run: &mut HoldRun, why: String) -> End {
            run.error = Some(why);
            End::Refused
        }
        let ctx = self.ctx();
        let sent = match account_reads::authentication(cfg, reader, &ctx) {
            Ok(auth) => self.write(ws, &auth).await.map_err(of_the_hold),
            Err(e) => Err(e.to_string()),
        };
        if let Err(why) = sent {
            return refused(run, why);
        }
        run.sent.push("authentication".to_owned());
        let end = opened + run.duration;
        let deadline = opened + AUTH_DEADLINE;
        let mut confirmed = [false; CHANNELS.len()];
        loop {
            let settled = run.subscribed.is_some();
            let msg = tokio::select! {
                msg = ws.next() => msg,
                () = tokio::time::sleep_until(end) => return End::Duration,
                () = &mut *interrupt => return End::Interrupted,
                () = tokio::time::sleep_until(deadline), if !settled => {
                    let why = format!(
                        "QFEX did not authenticate the hold's connection and confirm its three \
                         subscriptions within {} s",
                        AUTH_DEADLINE.as_secs()
                    );
                    return refused(run, why);
                }
            };
            let at = opened.elapsed();
            let msg = match msg {
                Some(Ok(msg)) => msg,
                // The stream failed, with its error, or ended.
                ended => {
                    run.dropped = ended.and_then(Result::err).map(|e| e.to_string());
                    return End::NoCloseFrame;
                }
            };
            let closing = matches!(msg, Message::Close(_));
            let Some(data) = hear(run, at, msg, &self.limiter) else {
                match closing {
                    true => return End::ServerClose,
                    false => continue,
                }
            };
            if let Some(err) = self.hold_data(run, reader, &data) {
                return refused(run, err);
            }
            let raw = data.raw();
            if run.authenticated.is_none() {
                if !account_reads::authenticated(reader, raw) {
                    let why = "QFEX did not authenticate the hold's connection".to_owned();
                    return refused(run, why);
                }
                run.authenticated = Some(at);
                for (subscription, channel) in subscriptions.iter().zip(CHANNELS) {
                    if let Err(why) = self.write(ws, subscription).await {
                        return refused(run, of_the_hold(why));
                    }
                    run.sent.push(format!("subscribe {channel}"));
                }
            } else if let Some(at_channel) = account_reads::confirmation(reader, raw)
                && !std::mem::replace(&mut confirmed[at_channel], true)
            {
                run.confirmed += 1;
                if run.confirmed == CHANNELS.len() {
                    run.subscribed = Some(at);
                }
            }
        }
    }

    /// Counts `frame`, a data frame off the hold's connection, by kind ([`held_frames::kind_of`])
    /// and reads it for a provider rate refusal as a read socket's ([`Refusals::hear`]): as a
    /// journal would keep it, and, when that is unread, as it arrived, in memory only; nothing of
    /// it is kept but its kind's name. Returns the refusal it is when it is an err, which ends
    /// the hold (decision 0146 item 4): read as the adapter reads one from what the journal
    /// keeps, having held the shared budget for a `RateLimited` one, as for a read socket's
    /// (decision 0126); or, for an err the journal would keep whole, read in memory as it
    /// arrived ([`held_frames::whole_err`]), holding no budget, as a session's codec holds none
    /// for one (decision 0134 item 6).
    fn hold_data(&self, run: &mut HoldRun, reader: &ReadAuth, frame: &Data) -> Option<String> {
        run.data_frames += 1;
        let raw = frame.raw();
        let spans = account_reads::redact_inbound(reader, Inbound::Frame(raw));
        let (kept, arrived) = (
            refusals::kept(raw, &spans),
            String::from_utf8_lossy(raw.bytes()),
        );
        let kind = match frame {
            Data::Binary(_) => held_frames::BINARY.to_owned(),
            Data::Text(_) => held_frames::kind_of(&kept, &arrived),
        };
        let whole = kind == format!("err {}", held_frames::KEPT_WHOLE);
        *run.kinds.entry(kind).or_default() += 1;
        (run.refusals).hear(&kept, &arrived);
        let err = account_reads::socket_err(reader, raw);
        if let Some(SocketErr::RateLimited(hold)) = &err {
            let now = tokio::time::Instant::now();
            (self.limiter).provider_rejected(now, NO_CONN, hold.charge, hold.retry_after);
        }
        match err {
            Some(err) => Some(err.to_string()),
            None if whole => held_frames::whole_err(&arrived),
            None => None,
        }
    }

    /// One session on `armed`, connection `n` of the run: it arms once and places nothing, and
    /// is stopped at `span_end` at the latest, its registry's span bound.
    async fn session(
        &mut self,
        n: usize,
        armed: Armed,
        cfg: VenueConfig,
        creds: fbc_core::Secrets,
        span_end: tokio::time::Instant,
    ) -> Result<SessionRun, String> {
        let acct = account_key(armed);
        let before = self.rejected();
        let seen = Rc::new(RefCell::new(Seen::default()));
        let control = Rc::new(RefCell::new(None));
        let watch = Watch {
            seen: Rc::clone(&seen),
            control: Rc::clone(&control),
        };
        let pacing = ReconnectPacing::new(DAY, DAY, 1, DAY, self.opts.connect_wait);
        let bound = ReservedBound::new(4, 4).expect("a bound with Normal room");
        let config = ExecSessionConfig {
            venue: &QFEX,
            cfg,
            creds,
            acct,
            account_wide_cod_authority: Some(AccountWideCodAuthority::new(acct)),
            rpc_ids: RpcIds::default(),
            ns: NS,
            specs: SpecTable::new(),
            connector: self.connector.clone(),
            pacing: pacing.expect("one attempt a day, each within --connect-wait-ms"),
            clock: self.clock.clone(),
            nonces: Box::new(self.nonces.clone()),
            nonce_source: NonceSourceId(0),
            conn: u16::try_from(n).expect("one or two sessions"),
            limiter: self.limiter.clone(),
            write_stall: WriteStall::new(Duration::from_secs(10)).expect("a ten-second window"),
            http_max_body: 1 << 20,
            outbound: bound,
            in_flight: InFlight::new(bound),
        };
        let label = name(armed);
        let built = ExecSession::new(config, watch);
        let (mut session, ctl) = built.map_err(|e| format!("the {label} session: {e}"))?;
        *control.borrow_mut() = Some(ctl);
        session.set_journal(self.journal.clone());
        self.line(format_args!(
            "SESSION {label}: it arms once and places nothing"
        ));
        let ran = {
            let run = session.run();
            tokio::pin!(run);
            tokio::select! {
                ran = &mut run => ran,
                () = tokio::time::sleep(self.opts.session_wait) => {
                    Watch::stop(&seen, &control, "--session-wait");
                    run.await
                }
                // Stage one places nothing, so a session at its span bound has nothing to
                // cancel: the stop control alone ends it, a moment later (the report's past the
                // span bound).
                () = tokio::time::sleep_until(span_end) => {
                    Watch::stop(&seen, &control, "--registry-span");
                    run.await
                }
            }
        };
        let counters = session.counters();
        drop(session);
        let seen = seen.borrow();
        let arm = seen.arm.as_deref().unwrap_or("none");
        let ended_by = seen.stopped.unwrap_or("the session's own end");
        self.line(format_args!(
            "SESSION {label}: arm outcome {arm}, ended by {ended_by}"
        ));
        Ok(SessionRun {
            account: label,
            authenticated: seen.authenticated,
            arm: seen.arm.clone(),
            ended_by,
            attempts: counters.attempts,
            failed_attempts: counters.failed_attempts,
            arm_failures: counters.arm_failures,
            decode_errors: counters.decode_errors,
            error: ran.err().map(|e| e.to_string()),
            // The holds its runtime made while it ran; its frames' counts are the slice's.
            refusals: Refusals {
                limiter: self.rejected().saturating_sub(before),
                ..Refusals::default()
            },
            ..SessionRun::default()
        })
    }
}

/// The hold's connection, opened, and what it writes on it ([`Run::connect_hold`]).
struct Connected {
    cfg: VenueConfig,
    reader: ReadAuth,
    subscriptions: Vec<SocketWrite>,
    ws: WebSocket,
}

/// Charges one request, `charge` sent `via` as `class` traffic, to `limiter` under [`NO_CONN`], or
/// refuses it, nothing sent, when the buckets have no room for it now: every read socket's frame
/// and every REST read of the run, the comparisons' included (decision 0135 item 1).
pub fn admit(
    limiter: &RateLimiter,
    charge: RateCharge,
    via: Via,
    class: TrafficClass,
) -> Result<(), String> {
    let request = fbc_runtime::Request { charge, via, class };
    let charged = limiter.charge(tokio::time::Instant::now(), NO_CONN, &[request]);
    charged
        .map(|_| ())
        .map_err(|_| "not sent: the rate limit has no room".to_owned())
}

/// `work`'s output, or `None` when a press of `presses` came first: a press already made is
/// taken before `work` is polled, and `work` is dropped at the press.
async fn unless_pressed<T>(presses: &mut Presses, work: impl Future<Output = T>) -> Option<T> {
    tokio::select! {
        biased;
        () = presses.pressed() => None,
        done = work => Some(done),
    }
}

/// `error`, from `auth_qfex`'s socket helpers, which the read sockets share and which name the
/// read socket, said of the hold's connection.
fn of_the_hold(error: String) -> String {
    error
        .replacen(
            "no read socket within",
            "the hold's connection did not open within",
            1,
        )
        .replacen("the read socket", "the hold's connection", 1)
}

/// Records `msg`, which the hold's connection received `at` after it opened: a ping, pong or
/// close in `run.heard` (a payload and a reason by length only, a code in full, a close after
/// the hold began its own as its echo); a data frame is handed back, for [`Run::hold_data`]. The
/// reply the WebSocket layer writes to a ping (its pong, none once either side began to close)
/// or to a server close (its close reply; none to the echo of the hold's own) is recorded on
/// `limiter` as a control frame, past its cap if need be, as a session records it (decision
/// 0030).
pub fn hear(run: &mut HoldRun, at: Duration, msg: Message, limiter: &RateLimiter) -> Option<Data> {
    let control = match msg {
        Message::Text(text) => return Some(Data::Text(text.as_str().to_owned())),
        Message::Binary(bytes) => return Some(Data::Binary(bytes.to_vec())),
        Message::Ping(payload) => Control::Ping { len: payload.len() },
        Message::Pong(payload) => Control::Pong { len: payload.len() },
        Message::Close(frame) => Control::Close {
            code: frame.as_ref().map(|f| u16::from(f.code)),
            reason_len: frame.as_ref().map_or(0, |f| f.reason.len()),
            echo: run.close_begun.is_some(),
        },
        // A raw frame is something to send: reading never yields one.
        Message::Frame(_) => return None,
    };
    // The WebSocket layer answers a ping only while the connection is open both ways: before
    // the hold began its own close and before any close of the server's (tungstenite answers a
    // ping only in its active state). A close of the server's is yielded only in that state,
    // and answered.
    let open = run.close_begun.is_none() && run.server_closes().is_empty();
    let answered = match control {
        Control::Ping { .. } => open,
        Control::Close { echo, .. } => !echo,
        Control::Pong { .. } => false,
    };
    if answered {
        limiter.record(tokio::time::Instant::now(), NO_CONN, CONTROL);
        run.replies += 1;
    }
    run.heard.push(Heard { at, control });
    None
}

/// A data frame off a read socket or the hold's connection.
pub enum Data {
    Text(String),
    Binary(Vec<u8>),
}

impl Data {
    /// The frame as the adapter reads one.
    pub fn raw(&self) -> RawFrame<'_> {
        match self {
            Data::Text(text) => RawFrame::Text(text),
            Data::Binary(bytes) => RawFrame::Binary(bytes),
        }
    }
}

/// The next data frame on a read socket within `wait`; pings and pongs are passed over. The pong
/// the WebSocket layer writes to each ping, which it answers while the socket is open both ways,
/// as it is while this reads, is recorded on `limiter` as a control frame, past its cap if need
/// be, as the hold's and a session's are (decision 0030).
pub async fn next(
    ws: &mut WebSocket,
    wait: Duration,
    limiter: &RateLimiter,
) -> Result<Data, String> {
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return Ok(Data::Text(text.as_str().to_owned())),
                Some(Ok(Message::Binary(bytes))) => return Ok(Data::Binary(bytes.to_vec())),
                Some(Ok(Message::Ping(_))) => {
                    limiter.record(tokio::time::Instant::now(), NO_CONN, CONTROL);
                }
                Some(Ok(Message::Pong(_))) => {}
                _ => return Err("the read socket closed before it answered".to_owned()),
            }
        }
    };
    let waited = tokio::time::timeout(wait, read).await;
    waited.map_err(|_| format!("no answer within {} ms", wait.as_millis()))?
}

/// What stops a session that ran its course: the arm's outcome and the resync's end.
pub const ARM_AND_RESYNC: &str = "the arm's outcome and the resync's end";

/// What a session's handler saw.
#[derive(Default)]
struct Seen {
    authenticated: bool,
    /// The arm's outcome: the first outcome the session reports, since the program submits
    /// nothing and the arm is the session's only request.
    arm: Option<String>,
    /// Whether the session's resync has ended (`ResyncEnd`).
    resynced: bool,
    /// What stopped the session, when the program did.
    stopped: Option<&'static str>,
}

/// The session's handler: it stops the session once the arm has its outcome and the resync has
/// ended, in either order, or at the connection's end.
struct Watch {
    seen: Rc<RefCell<Seen>>,
    control: Rc<RefCell<Option<ExecControl>>>,
}

impl Watch {
    /// Records `why` when nothing stopped the session yet, and drops its control: the stop.
    fn stop(seen: &RefCell<Seen>, control: &RefCell<Option<ExecControl>>, why: &'static str) {
        seen.borrow_mut().stopped.get_or_insert(why);
        drop(control.borrow_mut().take());
    }
}

impl ExecHandler for Watch {
    fn on_exec(&mut self, env: Envelope<ExecEvent>) {
        let done = {
            let mut seen = self.seen.borrow_mut();
            let state = ConnState::Authenticated;
            seen.authenticated |=
                matches!(env.body, ExecEvent::Conn { state: s, .. } if s == state);
            match env.body {
                ExecEvent::Outcome { outcome, .. } if seen.arm.is_none() => {
                    seen.arm = Some(format!("{outcome:?}"));
                }
                ExecEvent::ResyncEnd => seen.resynced = true,
                _ => {}
            }
            seen.arm.is_some() && seen.resynced
        };
        if done {
            Watch::stop(&self.seen, &self.control, ARM_AND_RESYNC);
        }
    }

    fn on_epoch_end(&mut self, _key: ConnKey) {
        Watch::stop(&self.seen, &self.control, "the connection's end");
    }
}

// ----- Drop session one (FBC-23ch; decisions 0132 items 4 and 7, 0148, 0151) -----

/// What the place stages' preflight gives the drop session ([`Run::preflight`]): the traded
/// instrument, the registries' bounds, the caps in lots and the MDS touch's view, which
/// [`market::Touch::driven`] keeps current from the preflight's first touch until the place
/// stages end ([`Run::stage`], FBC-kllr).
pub struct Market {
    found: Discovered,
    bounds: PlaceBounds,
    caps: PlaceCaps,
    touch: market::TouchView,
}

/// What ends a drop session's steps before their course is run: a stop of the place stages, or
/// a failure that is no stop (the arm not accepted, a refused Start, a touch too old to price X
/// from), each said with no id and no value.
enum Halt {
    Stop(Stop),
    Error(String),
}

impl From<Stop> for Halt {
    fn from(stop: Stop) -> Halt {
        Halt::Stop(stop)
    }
}

/// Which session of a place stage: one of a drop test's two (decision 0132 item 4), stage two's
/// crossing session (FBC-su9i) or taker IOC session (FBC-ou9g, decision 0132 item 7), or stage
/// three's session (FBC-4plj, decision 0132 item 7).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Which {
    One,
    Two,
    Cross,
    Take,
    Pages,
}

impl Which {
    /// Its name: what its session and the reads before it are reported under.
    fn name(self) -> &'static str {
        match self {
            Which::One => places::DROP_ONE,
            Which::Two => places::DROP_TWO,
            Which::Cross => places::CROSS,
            Which::Take => places::TAKE,
            Which::Pages => places::PAGES,
        }
    }

    /// What the reads after it are reported under.
    fn after(self) -> &'static str {
        match self {
            Which::One => AFTER_DROP_ONE,
            Which::Two => AFTER_DROP_TWO,
            Which::Cross => AFTER_CROSS,
            Which::Take => AFTER_TAKE,
            Which::Pages => AFTER_PAGES,
        }
    }

    /// The order it places: X, Y, the crossing post-only, the taker IOC, or stage three's.
    fn order(self) -> &'static str {
        match self {
            Which::One => "X",
            Which::Two => "Y",
            Which::Cross => "the crossing post-only",
            Which::Take => "the taker IOC",
            Which::Pages => "P1, P2 and P3",
        }
    }

    /// The step of the registry it runs on, whose list it spends from.
    fn step(self) -> Step {
        match self {
            Which::One | Which::Two => Step::Drop,
            Which::Cross => Step::Cross,
            Which::Take => Step::Take,
            Which::Pages => Step::Pages,
        }
    }

    /// How the reads around it mark the label of the account it runs on.
    fn mark(self) -> &'static str {
        match self {
            Which::One | Which::Two => " (drop)",
            Which::Cross => " (cross)",
            Which::Take => " (take)",
            Which::Pages => " (pages)",
        }
    }

    /// Which stage-two or stage-three session it is, for the stops and the report's words that
    /// differ between them; `None` for a drop session.
    fn stage(self) -> Option<two::Session> {
        match self {
            Which::One | Which::Two => None,
            Which::Cross => Some(two::Session::Cross),
            Which::Take => Some(two::Session::Take),
            Which::Pages => Some(two::Session::Pages),
        }
    }
}

/// One drop session's handles: its `Link`, what its handler saw, its orders, its account key,
/// its market and its registry's span bound, the account it runs on, which session it is, and
/// where its frames begin among those the tee heard and the `add_order`s it saw sent.
struct DropCtx {
    link: Rc<RefCell<Link>>,
    wake: Rc<tokio::sync::Notify>,
    watched: Rc<RefCell<Watched>>,
    orders: fbc_runtime::ExecOrders,
    acct: AccountKey,
    inst: fbc_core::InstrumentId,
    /// The registry's lot caps, fixed at the preflight's touch ([`market::caps_admit`]).
    caps: PlaceCaps,
    span_end: tokio::time::Instant,
    on: Armed,
    which: Which,
    heard_from: usize,
    sent_from: usize,
}

/// An order a place stage's session placed (X, Y, the crossing post-only or the taker IOC): its
/// client id, its request and when it was sent.
#[derive(Copy, Clone)]
struct Placed {
    cid: fbc_core::ClientOrderId,
    rpc: fbc_core::RpcId,
    at: tokio::time::Instant,
    /// Its send on the wall clock, in whole seconds: the trades read after X start there.
    wall_secs: u64,
}

/// How long the reads after a place stage's session may wait for the shared limiter's room
/// ([`Run::admitted`]): longer than any hold QFEX's rate page shows a refusal asking for.
pub const AFTER_PATIENCE: Duration = Duration::from_secs(60);

/// Why a read of the drop test's orders and their trades after a drop session
/// ([`Run::read_orders`]) failed once [`AFTER_PATIENCE`] from its start passed with its pages
/// not all read (FBC-8o7a): the run's bound on the reads after a session, which it gives each
/// such read whole, so a venue answering new full pages for ever cannot hold the run open.
pub const HISTORY_CUT: &str = "the get_user_trades and GET /user/historic-orders pages not all \
                               read within 60 s of the read's start (AFTER_PATIENCE)";

/// How long a graceful close may take before the session's socket is dropped, as
/// `paradex_testnet_acceptance` waits for one.
pub const CLOSE_WAIT: Duration = Duration::from_secs(5);

/// What a flatten says as it waits for its session to take places before its first IOC
/// ([`Run::place_gate`]): the gate shut until its resync's end is pushed (decision 0058).
pub const PLACE_GATE: &str = "waiting, up to --rpc-timeout-ms, for the session to take places \
                              (its arm accepted and its resync's end pushed, decision 0058) \
                              before the first IOC";

/// What ends drop session one when it ran its course: a graceful close (decision 0132 item 4).
pub const GRACEFUL: &str = "a graceful close";
/// What ends drop session two when it ran its course: its connection dropped abruptly, with no
/// WebSocket close (decision 0132 item 4), by dropping the session's run.
pub const DROPPED: &str = "the abrupt drop";
/// What ends one whose connection ended first: the server's close, the codec's end or a
/// transport error ([`Stop::ConnectionEnded`]).
pub const CONNECTION_ENDED: &str = "the connection's end";
/// What ends one whose close the shared budget had no room for within [`AFTER_PATIENCE`]: the
/// runtime drops such a close frame unsent, and the connection with it, so the close may not
/// have been graceful, and whether it cancelled X is not measured (0151 item 11).
pub const NO_ROOM_TO_CLOSE: &str =
    "a close the shared budget had no room for, which may not have been graceful";

/// A place stage's session pacing (decision 0148 item 7): one connection attempt a day, each
/// within `connect_wait` (`--connect-wait-ms`), so a connection's end is never followed by
/// another.
pub fn place_pacing(connect_wait: Duration) -> ReconnectPacing {
    let pacing = ReconnectPacing::new(DAY, DAY, 1, DAY, connect_wait);
    pacing.expect("one attempt a day, each within --connect-wait-ms")
}

/// How a place stage's session ends once its steps ended ([`Run::end_session`], the place
/// stages' session machinery, which every session of the place stages ends through).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Policy {
    /// A drop session (decision 0132 item 4, 0151 items 2 and 11): only a stop at the owner's
    /// Ctrl-C or once a fill of ours came cancels, by venue id ([`Stop::cancels_in_drop`]), and
    /// it ends by a graceful close, since no drop test has passed yet on its account; but with
    /// `abrupt`, drop session two, once it ran its course with no stop and no failure, ends by
    /// the abrupt drop.
    Drop { abrupt: bool },
    /// A session outside the drop test (decision 0132 item 7, 0148 item 8, 0151 item 11): before
    /// it stops, at every stop and once it ran its course alike, the owner's Ctrl-C and the span
    /// bound included, it cancels every order of ours by venue id, each an fbc-oms authorization
    /// through the `Link`, then Stops; it ends by a graceful close, but when those cancels were
    /// refused locally or met by a provider rate refusal ([`Cancels::blocked`], 0151 item 11's
    /// two causes; a cancel answered other than accepted alone is neither) by the abrupt drop if
    /// this account's drop test passed in this run (`drop_passed`), which showed the drop
    /// cancels.
    Stage { drop_passed: bool },
}

/// How a place stage's session ends, when its connection did not end first.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Ending {
    /// A graceful close: the WebSocket close frame, sent by dropping the session's control.
    Close,
    /// The abrupt drop: the session's run dropped unpolled, its connection with it, with no
    /// WebSocket close.
    Drop,
}

impl Policy {
    /// Whether a session under it, its steps ended with `stop` (none when it ran its course),
    /// cancels every order of ours before it stops, given whether a fill of ours reached it.
    pub fn cancels(self, stop: Option<Stop>, fill_of_ours: bool) -> bool {
        match self {
            Policy::Drop { .. } => stop.is_some_and(|s| s.cancels_in_drop(fill_of_ours)),
            Policy::Stage { .. } => true,
        }
    }

    /// How a session under it ends, its steps ended with `stop`, `failed` when they ended on a
    /// failure that is no stop, and `cancels` the stop's cancels when they ran.
    pub fn ending(self, stop: Option<Stop>, failed: bool, cancels: Option<&Cancels>) -> Ending {
        match self {
            Policy::Drop { abrupt: true } if stop.is_none() && !failed => Ending::Drop,
            Policy::Drop { .. } => Ending::Close,
            Policy::Stage { drop_passed: true } if cancels.is_some_and(|c| c.blocked) => {
                Ending::Drop
            }
            Policy::Stage { .. } => Ending::Close,
        }
    }
}

/// When a session outside the drop test meets its registry's span bound, `span_end`, as a stop
/// ([`Stop::SpanBound`]): one `--rpc-timeout-ms`, `rpc`, before it, so its cancels begin early
/// enough to be answered before the bound (decision 0148 item 8), or, when the bound is nearer
/// the clock's origin than that, so no instant one timeout before it can be named, `now` or the
/// bound, whichever comes first: at once, the earliest the cancels can begin, never later than
/// one timeout before the bound would have been. A drop session meets it at the bound itself and
/// cancels nothing (0151 item 2).
pub fn cancels_begin(
    span_end: tokio::time::Instant,
    rpc: Duration,
    now: tokio::time::Instant,
) -> tokio::time::Instant {
    span_end.checked_sub(rpc).unwrap_or(now.min(span_end))
}

/// What [`Run::end_session`] did: the session's stop, how the session ended, the stop's cancels
/// when they ran, the comparison after them when one was made, and whether the session's own
/// order rested as its close was sent.
struct Closed {
    /// The session's stop: the one its steps ended with, the late flatten's own in place of the
    /// fill it answered ([`Run::flatten_late`]), or, when none came: once the stop's cancels took
    /// a press of Ctrl-C, a stop the tee or the handler raised before that press, else
    /// [`Stop::Interrupted`]; the stop the comparison after them raised; or a press of Ctrl-C or
    /// a stop the tee or the handler raised as the close waited for room.
    stop: Option<Stop>,
    ended_by: &'static str,
    cancels: Option<Cancels>,
    compared: Option<(crate::compare::Compared, Option<ReadSocket>)>,
    rested_at_close: bool,
}

/// The stop and the error of a drop session whose session's run ended first, `ended`, given
/// `raised`, the stop its tee or handler raised: that stop, or, when none was raised (the run
/// ended before any epoch opened, so its handler heard no end), [`Stop::ConnectionEnded`], with
/// the run's error by its variant's name alone.
pub fn ended_first(
    raised: Option<Stop>,
    ended: Result<(), ExecSessionError>,
) -> (Option<Stop>, Option<String>) {
    let error = ended.err().map(|e| {
        format!(
            "the session ended before it ran its course: {}",
            places::variant_name(&e)
        )
    });
    match raised {
        Some(stop) => (Some(stop), error),
        None => (Some(Stop::ConnectionEnded), error),
    }
}

/// Whether the reads of `reads` were clean on every named account but `on`: `None` when `named`
/// names no other account (FBC-77r8: whether the other named accounts stayed clean through a
/// drop test).
pub fn others_clean(reads: &[AccountRead], named: &[Armed], on: Armed) -> Option<bool> {
    let others: Vec<&str> = (named.iter().filter(|a| **a != on))
        .map(|a| name(*a))
        .collect();
    if others.is_empty() {
        return None;
    }
    let theirs = reads.iter().filter(|r| others.contains(&r.label.as_str()));
    Some(theirs.into_iter().all(|r| r.refusal().is_none()))
}

impl Run<'_> {
    /// Each named account's drop test in turn ([`Run::drop_test`]; decision 0132 item 4, 0148
    /// items 1 and 2), each starting only once the one before ended with no stop, so no order
    /// of ours rests on another account while a session runs (decision 0132 items 4 and 7). The
    /// first account's reads before drop session one are skipped when `read` (the reads that
    /// cleared a stop marker just made them). The first stop ends the run, so a drop test that
    /// failed places nothing more on its account or any other; with none, stage two follows
    /// ([`Run::cross_tests`]).
    async fn drop_tests(
        &mut self,
        places: &Places,
        mut market: Market,
        read: bool,
    ) -> Result<(), String> {
        for (n, on) in self.named.clone().into_iter().enumerate() {
            self.drop_test(places, &mut market, on, read && n == 0)
                .await?;
        }
        self.cross_tests(places, market).await
    }

    /// The drop test on `on` (FBC-23ch, FBC-77r8; decision 0132 item 4), as [`Run::drop_sessions`]
    /// runs it, then, once it was built, what it says of the other named accounts' reads during
    /// it and the run's stop, if one came.
    async fn drop_test(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        read: bool,
    ) -> Result<(), String> {
        let (reads, made) = (self.ev.reads.len(), self.ev.drops.len());
        let unread = self.unread.len();
        let tested = self.drop_sessions(places, market, on, read).await;
        if self.ev.drops.len() > made {
            let clean = others_clean(&self.ev.reads[reads..], &self.named, on);
            // A read of another account that could not be made leaves its cleanness not known,
            // since the evidence's reads hold only those made (FBC-77r8's review).
            let missed = self.unread[unread..].iter().any(|a| *a != on);
            let others = places::OthersClean::of(clean, missed);
            let stop = self.stop.map(Stop::name);
            let run = self.drop_run();
            run.others_clean = others;
            run.stop = stop;
        }
        tested
    }

    /// The drop test's sessions on `on`, after the preflight gave `market` and, unless `read`
    /// (the reads that cleared a stop marker), every named account read clean: the drop registry
    /// built and its trip given to the journal before any market arms (decision 0139), seeded
    /// flat (0148 item 3), then drop session one ([`Run::drop_session`]), the after-session read
    /// of every named account, and X and its trades read (FBC-be57). Then, with no stop, the read
    /// before drop session two ([`Run::read_before_two`]), which admits X alone, and drop session
    /// two on the same `Link` and registry with no second seed, ended by the abrupt drop; the
    /// reads after it ([`Run::after_drop`]), X's and Y's trades and historic-orders rows, the
    /// verdict ([`crate::verdict::DropVerdict`]), and the read of every named account after it.
    /// From the first arm to the last read nothing cancels X or Y but Ctrl-C or a fill of ours,
    /// nothing reconnects, and no later arm is sent (decision 0132 item 4, 0151 item 2).
    ///
    /// Each way it ends the run is a stop, marked, the first that applies. After drop session
    /// one: the session's stop; a provider rate refusal in the reads after it, then one reading
    /// X; the owner's Ctrl-C during them; a position not flat or a trade naming X (0151 item 9);
    /// a trade naming another order or none ([`Stop::DropFailed`], decision 0132 item 4); X's
    /// account not read for a reason that is no provider rate refusal, X counted resting, unless
    /// the streams showed it end, as decision 0005 counts an order whose end nothing showed, then
    /// X's trades or historic orders not read whole, X left unjudged ([`Stop::LeftResting`] both;
    /// the owner's ruling under 0151 item 6 on FBC-gisx); then a failure of the session that
    /// raised no stop, with [`Stop::ReadDisagrees`] marked when an order none of ours or a TWAP
    /// shows, else [`Stop::LeftResting`] when an order of ours rests; then a stop of the read
    /// before drop session two. After drop session two: the session's stop; a failure that
    /// raised no stop, with the stop the reads after it show ([`after_stop`]: a provider rate
    /// refusal, Ctrl-C, a position not flat or a trade naming X or Y, an order none of ours or a
    /// TWAP, an order of ours still resting), or, with none, X's account not read for a reason
    /// that is no provider rate refusal, the orders sent whose end the streams did not show
    /// counted resting ([`places::Counted`], FBC-gisx), so an order still resting, or counted
    /// resting, is marked; the verdict's stop (0151 items 9 and 13), unless the reads' stop
    /// outranks it ([`outranks`]: a position not flat, or an order none of ours or a TWAP, over
    /// a stop after it in the verdict's own order; FBC-77r8's review); then the reads' stop.
    /// With none it ends `Ok`, the next account's drop test next.
    async fn drop_sessions(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        read: bool,
    ) -> Result<(), String> {
        let label = name(on);
        if !read {
            let mark = self.read_mark();
            let mut presses = std::mem::take(&mut self.presses);
            let reads = self.check_accounts(places::DROP_ONE, on, " (drop)");
            let reads = unless_pressed(&mut presses, reads).await;
            self.presses = presses;
            match reads {
                None => {
                    let when = "during the reads before drop session one";
                    return Err(self.halt(Stop::Interrupted, when));
                }
                Some(reads) => {
                    let when = "in the reads before drop session one";
                    self.unless_refused(mark, reads, when)?;
                }
            }
        }
        let (cfg, creds) = place_config(&self.base, self.lookup, on)?;
        // Drop session two's configuration, read before anything is placed, so no failure to
        // read it can come between the sessions while X rests.
        let two_config = place_config(&self.base, self.lookup, on)?;
        let acct = account_key(on);
        let order = (self.caps.exec.as_ref()).map(|e| e.order.clone());
        let order = order.ok_or("the probe's caps declare no order entry")?;
        let inst = market.found.inst;
        let symbol = market.found.spec.venue_symbol.clone();
        let keys = places::lease_keys(label, &order, inst, symbol);
        let opened = Opened::now(self.origin, wall_ns());
        let caps = market::pre_trade_caps(inst, market.caps);
        let built = PlaceRegistry::new(
            Step::Drop,
            label,
            acct,
            &market.bounds,
            opened,
            inst,
            caps,
            keys,
        );
        // Its trip, before any market of it arms (decision 0139).
        built.trip_on(&self.journal);
        let rpc = Duration::from_millis(self.opts.rpc_timeout_ms);
        let ladder = fbc_oms::LadderConfig::new(rpc, Duration::ZERO, rpc * 6, 2);
        let ladder = ladder.map_err(|e| format!("the ladder: {}", places::variant_name(&e)))?;
        let (link, mut placed) = built.into_link(order, ladder);
        let link = Rc::new(RefCell::new(link));
        let at = self.built.len();
        self.built.push(BuiltReg {
            on,
            step: Step::Drop,
            link: Rc::clone(&link),
            xy: Vec::new(),
        });
        // The flat seed, after clean reads of every named account (decision 0148 item 3): once,
        // for both of the drop test's sessions.
        let seeded = link
            .borrow_mut()
            .reg()
            .seed_position(inst, fbc_core::SignedLots(0));
        seeded.map_err(|e| format!("the flat seed was refused: {}", places::variant_name(&e)))?;
        self.line(format_args!(
            "REGISTRY drop on {label}: {} on its one market, its CancelOnlyTrip given to the \
             journal before any market arms (decision 0139), then seeded flat after clean reads \
             of every named account (decision 0148 item 3)",
            ledger::PLACES_DECLARATION
        ));
        let lists = PlaceLists::new(market.found.min_size, places.flatten_attempts)?;
        let mut allowance = Allowance::new(&lists);
        // A test's X spent before the session, so its place is beyond the list.
        #[cfg(test)]
        if places.test.x_spent {
            let _ = allowance.take(Step::Drop, ledger::X);
        }
        self.ev.drops.push(DropRun {
            account: label,
            ..DropRun::default()
        });
        let span_end = placed.deadline();
        let ctx = (cfg, creds, Rc::clone(&link), span_end);
        let ran = self
            .drop_session(places, market, on, ctx, &mut allowance, None)
            .await;
        placed.session_ended(&link.borrow(), places::DROP_ONE, self.mono());
        self.ev.place_registries.push(placed);
        let (stop, x, iocs_one) = match ran {
            Ok(ran) => ran,
            Err(why) => {
                self.drop_run().error = Some(why.clone());
                (None, None, Vec::new())
            }
        };
        let said = stop.map(|stop| self.halt(stop, "in drop session one"));
        let vid_of = |cid| (link.borrow().registry().get(cid)).and_then(|r| r.vid().cloned());
        let x_allowed = x.map(|x| AllowedOrder {
            account: on,
            cid: x.cid,
            venue: vid_of(x.cid),
        });
        let ioc_allowed = |cids: &[fbc_core::ClientOrderId]| -> Vec<AllowedOrder> {
            (cids.iter())
                .map(|&cid| AllowedOrder {
                    account: on,
                    cid,
                    venue: vid_of(cid),
                })
                .collect()
        };
        // The reads after the session wait out a hold the shared budget is under (decision
        // 0126), for at most AFTER_PATIENCE.
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        // X first, then each flatten IOC the session sent: one left resting is ours, for the
        // owner's hand cancel, not an order the streams did not show (the restack's review).
        let flattened_one = ioc_allowed(&iocs_one);
        let ours: Vec<&AllowedOrder> = x_allowed.iter().chain(&flattened_one).collect();
        let after = self.read_after(Which::One, on, &ours).await;
        let (mut x_refused, mut x_pressed, mut x_unread) = (None, false, false);
        let mut x_shown_ended = false;
        if let (Some(x), Some(allowed)) = (x, &x_allowed) {
            let read = self
                .read_orders(Which::One, on, x.wall_secs, &[allowed])
                .await;
            self.drop_run().after = Some(read.0.x_after());
            // A historic-orders row naming X shows X ended, whether or not a later page could be
            // read: what a page showed survives an unfinished read (FBC-77r8's review, carried).
            x_shown_ended = read.0.terminal.first().is_some_and(Option::is_some);
            x_unread = !read.0.trades_whole || !(read.0.historic_whole || x_shown_ended);
            (x_refused, x_pressed) = (read.1, read.2);
        }
        self.patient = None;
        // Whether the close cancelled X, measured on a read of X's own account alone (another
        // account's says nothing of X) and, for X absent, on that account flat and X's trades
        // read whole with no row since its send (decision 0132 item 4).
        let run = self.drop_run();
        let read = places::XAccountRead {
            read: after.x_read,
            listed: after.x_listed,
            flat: after.x_flat,
        };
        let verdict = places::close_verdict(run, read);
        (run.close_cancelled_x, run.close_not_measured) = (verdict.ok(), verdict.err());
        run.x_resting = after.x_listed;
        run.x_read = after.x_read;
        // The owner's hand cancel: every order of ours once a flatten IOC was sent and an order
        // of ours rests, and otherwise X when the read listed it (the restack's review).
        run.hand_cancel = match (after.ours_resting && !iocs_one.is_empty(), after.x_listed) {
            (true, _) => Some(places::HAND_CANCEL_ONE_ORDERS),
            (false, true) => Some(places::HAND_CANCEL),
            (false, false) => None,
        };
        // X placed and left unjudged with no provider rate refusal in the way: its account not
        // read while the streams did not show X end, X then counted resting as decision 0005
        // counts an order whose end nothing showed, or its trades or historic orders not read
        // whole (the owner's ruling under 0151 item 6 on FBC-gisx).
        // X's end is shown by the registry, or by a historic-orders row naming X (FBC-77r8's
        // review, carried).
        let x_ended = x.is_some_and(|x| ended_on(&link.borrow(), x.cid)) || x_shown_ended;
        let on_unread = after.on_unread.filter(|_| !x_ended);
        let unseen = match (on_unread, x_unread && x_refused.is_none()) {
            (Some(why), _) => Some(places::Unseen::Account(why)),
            (None, true) => Some(places::Unseen::History),
            (None, false) => None,
        };
        run.x_unseen = unseen.filter(|_| x.is_some());
        run.x_ended = x_ended;
        // X counted resting with its account not read: the owner's hand cancel says so, never
        // as seen resting (FBC-gisx). A flatten IOC is sent only once the flatten saw X end, so
        // none is counted beside X here.
        if run.hand_cancel.is_none() && matches!(run.x_unseen, Some(places::Unseen::Account(_))) {
            run.hand_cancel = Some(places::HAND_CANCEL_UNREAD);
        }
        // The run's end, the first that applies: the session's stop; a provider rate refusal in
        // the reads after it, then one reading X; the owner's Ctrl-C during those reads; a
        // position not flat, or a trade naming X; a trade naming another order or none, which
        // fails the drop test already; X's account not read, X counted resting, then X's trades
        // or historic orders not read whole, X left unjudged; then a failure of the session that
        // was no stop, kept in the evidence as the session's error whichever stop came after it,
        // with any order of ours still resting marked for the owner's hand cancel; then a stop of
        // the read before drop session two. A stop after the session is raised, and marked,
        // whether or not the session failed (0151 item 6). X still resting, or counted resting,
        // then is the owner's to cancel by hand (decision 0132 item 7).
        let ended = 'ended: {
            if let Some(said) = said {
                break 'ended Err(said);
            }
            if let Some(stop) = after.refused {
                break 'ended Err(self.halt(stop, "in the reads after drop session one"));
            }
            if let Some(stop) = x_refused {
                break 'ended Err(self.halt(stop, "reading X after drop session one"));
            }
            if after.pressed || x_pressed {
                let when = "during the reads after drop session one";
                break 'ended Err(self.halt(Stop::Interrupted, when));
            }
            // A trade naming X, seen with no session open, is a fill of ours: not flat, the
            // position the owner's, whatever the position read before it showed (0151 item 9),
            // so drop session two is not run.
            let x_traded = (self.drop_run().after.as_ref()).is_some_and(|a| a.trades_naming_x > 0);
            if after.not_flat || x_traded {
                break 'ended Err(self.halt(Stop::NotFlat, "after drop session one"));
            }
            // A row since X's send naming another order or none, on an account still flat:
            // something else traded on the account, which 0132 item 4 rules out from the first
            // arm to the read after the drop, so the drop test has failed already and Y is not
            // placed. The session is over, so nothing is cancelled (0151 item 2).
            let other =
                (self.drop_run().after.as_ref()).is_some_and(|a| a.trades > a.trades_naming_x);
            if other {
                let when = "after drop session one, a get_user_trades row since X's send naming \
                            another order or none (decision 0132 item 4)";
                break 'ended Err(self.halt(Stop::DropFailed, when));
            }
            // X's account not read after the session, with no provider rate refusal: X counts as
            // resting, its end not seen (decision 0005, the owner's ruling under 0151 item 6 on
            // FBC-gisx), so the run stops for the owner's hand cancel, marked. Nothing more is
            // placed, and nothing is cancelled with no session open (0151 item 2).
            // An order none of ours or a TWAP a read listed is what the streams did not show
            // (decision 0148 item 8): it outranks X counted resting or left unjudged, as an
            // order or a TWAP not ours outranks an order of ours resting after drop session two
            // (FBC-77r8's review, carried). X is still the owner's to cancel by hand.
            let unseen = self.drop_run().x_unseen;
            if after.unexpected && unseen.is_some() {
                let said = self.halt(Stop::ReadDisagrees, "after drop session one");
                break 'ended Err(match self.drop_run().error.clone() {
                    Some(why) => format!("{why}; {said}"),
                    None => said,
                });
            }
            if let Some(places::Unseen::Account(why)) = unseen {
                let when = format!(
                    "after drop session one, X's account not read after it ({}), so X counts as \
                     resting (decision 0005)",
                    why.name()
                );
                let says = Some(places::COUNTED_RESTING);
                let said = self.halt_as(Stop::LeftResting, &when, says);
                // A failure of the session that raised no stop is kept beside it, as after drop
                // session two.
                break 'ended Err(match self.drop_run().error.clone() {
                    Some(why) => format!("{why}; {said}"),
                    None => said,
                });
            }
            // A read of X's trades or historic orders that failed with no refusal leaves X
            // unjudged, as an account that could not be read does before drop session two: Y is
            // not placed beside an X whose fills were never read. It is said so, never that X was
            // seen resting (the owner's ruling under 0151 item 6 on FBC-gisx).
            if x_unread {
                let when = "reading X after drop session one: its get_user_trades or GET \
                            /user/historic-orders pages not all read, X left unjudged (decision \
                            0005)";
                let says = Some(places::LEFT_UNJUDGED);
                break 'ended Err(self.halt_as(Stop::LeftResting, when, says));
            }
            // A failure of the session that raised no stop, after every stop the reads showed:
            // an order none of ours or a TWAP is what the streams did not show (decision 0148
            // item 8), and otherwise an order of ours still resting is marked for the owner's
            // hand cancel (0151 item 6). X placed on an account that could not be read was
            // counted resting above (FBC-gisx), and a full page not listing X lists an order.
            // The stop is said after the failure, as after drop session two and every other
            // stop after a failed drop session one here (FBC-gisx's review).
            if let Some(why) = self.drop_run().error.clone() {
                let stop = match (after.unexpected, after.resting) {
                    (true, _) => Some(Stop::ReadDisagrees),
                    (false, true) => Some(Stop::LeftResting),
                    (false, false) => None,
                };
                break 'ended Err(match stop {
                    Some(stop) => format!("{why}; {}", self.halt(stop, "after drop session one")),
                    None => why,
                });
            }
            // X, placed and ended with no stop: the read before drop session two admits it
            // alone, whether it still rests or a graceful close cancelled it (decision 0132
            // items 4 and 7).
            let x = x_allowed
                .as_ref()
                .expect("X, placed when drop session one ran");
            self.read_before_two(x).await
        };
        if let Err(why) = ended {
            if let Some(hand) = self.drop_run().hand_cancel {
                self.line(format_args!("RESTING {hand}"));
            }
            return Err(why);
        }
        let (x, x_allowed) = x
            .zip(x_allowed)
            .expect("X, placed when drop session one ran");
        let mine = format!("{label} (drop)");
        let mut reads = self.ev.reads.iter().rev();
        let before_two = reads.find(|r| r.before == places::DROP_TWO && r.label == mine);
        let x_before_two = before_two.is_some_and(|r| r.page_allowed > 0);
        let run = self.drop_run();
        run.x_before_two = Some(x_before_two);
        run.two = Some(TwoRun::default());
        // Drop session two, on the same registry and Link, seeded nothing.
        let (cfg, creds) = two_config;
        let ctx = (cfg, creds, Rc::clone(&link), span_end);
        let ran = self
            .drop_session(places, market, on, ctx, &mut allowance, Some(x))
            .await;
        // A test's flatten budget spent by the drop registry, as a flatten there would have, on
        // the allowance the account's later registries are handed.
        #[cfg(test)]
        if places.test.flatten_spent {
            let _ = allowance.take(Step::Drop, ledger::FLATTEN);
        }
        // What the account's later registries spend from (decision 0148 item 7).
        self.allowances.push((on, allowance));
        // The drop registry serves no more sessions: its market disarmed, so its market lease is
        // given up for the account's later registries, which take it under the same names.
        link.borrow_mut().reg().disarm(market.found.inst);
        let now = self.mono();
        let ended = self.ev.place_registries.last_mut();
        ended
            .expect("the drop registry")
            .session_ended(&link.borrow(), places::DROP_TWO, now);
        let (stop, y, iocs_two) = match ran {
            Ok(ran) => ran,
            Err(why) => {
                self.two_run().error = Some(why.clone());
                (None, None, Vec::new())
            }
        };
        // X and Y, which count ended for a later registry only as 0151 item 5 rules.
        self.built[at].xy = std::iter::once(x).chain(y).map(|p| p.cid).collect();
        // The read before the drop not made is said as counted resting, never as an order seen
        // resting, before any read after the session (FBC-gisx's review).
        let unmade = self.two_run().before_drop_unread;
        let said = stop.map(|stop| {
            let says = (stop == Stop::LeftResting && unmade).then_some(places::UNREAD_BEFORE_DROP);
            self.halt_as(stop, "in drop session two", says)
        });
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        let (mut verdict, mut pressed) = (None, false);
        let allowed = |p: Placed| AllowedOrder {
            account: on,
            cid: p.cid,
            venue: vid_of(p.cid),
        };
        let x_allowed = AllowedOrder {
            venue: vid_of(x.cid),
            ..x_allowed
        };
        let y_allowed = y.map(allowed);
        if self.two_run().ended_by == Some(DROPPED) {
            let y_allowed =
                (y_allowed.as_ref()).expect("Y, placed when drop session two reached its drop");
            let symbol = market.found.spec.venue_symbol.as_wire().to_owned();
            let (judged, held) = self
                .judge_drop(on, &symbol, x, (&x_allowed, y_allowed))
                .await;
            verdict = Some(judged);
            pressed = held;
        }
        // X first, so the read says whether X still rests (FBC-77r8's review), then Y, then each
        // flatten IOC either session sent, so one left resting is ours (the restack's review).
        let ours: Vec<&AllowedOrder> = std::iter::once(&x_allowed)
            .chain(y_allowed.as_ref())
            .collect();
        let iocs: Vec<fbc_core::ClientOrderId> = [iocs_one, iocs_two].concat();
        let iocs_open = (iocs.iter()).any(|&cid| !ended_on(&link.borrow(), cid));
        let flattened = ioc_allowed(&iocs);
        let read_ours: Vec<&AllowedOrder> = ours.iter().copied().chain(&flattened).collect();
        let mut after = self.read_after(Which::Two, on, &read_ours).await;
        // A failure that raised no stop: X's trades, and Y's once it was submitted, read after
        // the account reads, as after drop session one, so a fill of either seen with no session
        // open stops the run not flat whatever the position read before it showed (0151 item 9).
        // A provider rate refusal or a press of Ctrl-C meanwhile is the reads' too. A read not
        // whole adds no stop: the failure is still the run's end, with the reads' stop after it.
        if said.is_none() && self.two_run().error.is_some() {
            let (traded, refused, held) =
                self.read_orders(Which::Two, on, x.wall_secs, &ours).await;
            after.not_flat |= traded.naming.iter().any(|&n| n > 0);
            after.refused = after.refused.or(refused);
            pressed |= held;
            self.two_run().after = Some(traded);
        }
        self.patient = None;
        // X's account not read with no provider rate refusal and no verdict judging X and Y: X,
        // unless the streams showed it end, and Y, once submitted, count as resting (decision
        // 0005, the owner's ruling under 0151 item 6 on FBC-gisx). A verdict, passed or failed,
        // rests on the reads after the drop, which showed their ends or stopped the run. Y's end
        // is not looked for: a session that failed hands back no Y to look up.
        let x_open = !ended_on(&link.borrow(), x.cid);
        let counted = places::Counted::of(x_open, self.two_run().y.submitted, iocs_open);
        let unread = (after.on_unread.filter(|_| verdict.is_none())).zip(counted);
        let two = self.two_run();
        two.resting = after.resting;
        two.read = after.read;
        two.x_resting = after.x_listed;
        two.x_read = after.x_read;
        two.unread = unread;
        two.x_ended = !x_open;
        match unread {
            _ if after.resting => self.line(format_args!("RESTING {}", places::HAND_CANCEL_ORDERS)),
            Some((_, counted)) => self.line(format_args!("RESTING {}", counted.hand_cancel())),
            None => {}
        }
        // The run's end, the first that applies: the session's stop; a failure that was no stop,
        // with the reads' stop after it when they show one (a provider rate refusal, Ctrl-C, a
        // position not flat or a trade naming X or Y, an order still resting), or, with none, X's
        // account not read, the orders sent whose end the streams did not show counted resting,
        // so an order still resting, or counted resting, is marked for the hand cancel the report
        // asks for (0151 items 6 and 9); the verdict's stop (decision 0132 item 7, 0151 items 9
        // and 13); then the reads' stop, a named account not read among them (FBC-8mwz). After a
        // failure, an account not read counts only the orders whose end nothing showed, as above
        // (FBC-gisx), so the reads' stop there takes no account not read.
        let read_stop = after_stop(&after, pressed, false);
        if let Some(said) = said {
            return Err(said);
        }
        if let Some(why) = self.two_run().error.clone() {
            let stop = match (read_stop, unread) {
                (Some((stop, when)), _) => Some((stop, when.to_owned(), None)),
                (None, Some((failed, counted))) => Some((
                    Stop::LeftResting,
                    format!(
                        "after drop session two, X's account not read after it ({}), so {} as \
                         resting (decision 0005)",
                        failed.name(),
                        counted.count()
                    ),
                    Some(places::COUNTED_RESTING),
                )),
                (None, None) => None,
            };
            return Err(match stop {
                Some((stop, when, says)) => {
                    format!("{why}; {}", self.halt_as(stop, &when, says))
                }
                None => why,
            });
        }
        let judged = verdict.as_ref().and_then(crate::verdict::DropVerdict::stop);
        self.drop_run().verdict = verdict;
        // A named account, X's or another, that could not be read after the session is a stop
        // too, after every other the reads show (FBC-8mwz; decision 0132 item 7).
        let read_stop = after_stop(&after, pressed, after.unread);
        // What the reads after the session show of the account, a position not flat or an order
        // or a TWAP the streams did not show, outranks a verdict's stop that comes after it in
        // the verdict's own order ([`outranks`]): the position is then the owner's (0151 item
        // 9), or the account holds what the streams did not show (decision 0148 item 8).
        if let Some(stop) = judged.filter(|j| !outranks(read_stop.map(|(s, _)| s), *j)) {
            return Err(self.halt(stop, "after the drop"));
        }
        // A LeftResting no read listing an order made is a named account not read: said in the
        // owner's words, counted resting, never seen resting (the ruling under 0151 item 6 on
        // FBC-gisx); one a read listed is said as seen resting ([`drop_two_says`]).
        match read_stop {
            Some((stop, when)) => Err(self.halt_as(stop, when, drop_two_says(stop, after.resting))),
            None => Ok(()),
        }
    }

    /// The account's drop test, as the evidence holds it: the last begun.
    fn drop_run(&mut self) -> &mut DropRun {
        self.ev.drops.last_mut().expect("a drop test begun")
    }

    /// Its drop session two, as the evidence holds it.
    fn two_run(&mut self) -> &mut TwoRun {
        self.drop_run().two.get_or_insert_with(TwoRun::default)
    }

    /// What `d`'s order (X in drop session one, Y in two) became, as the evidence holds it.
    fn placed_run(&mut self, which: Which) -> &mut places::XRun {
        match which {
            Which::One => &mut self.drop_run().x,
            Which::Two => &mut self.two_run().y,
            Which::Cross | Which::Take => &mut self.stage_run(which).placed,
            // The stage-three order being placed: its record is pushed before its place.
            Which::Pages => {
                &mut (self.three_run().orders.last_mut())
                    .expect("a stage-three order begun")
                    .1
            }
        }
    }

    /// The flatten of session `which`, as the evidence holds it (FBC-ou9g), begun on first use.
    fn flatten_run(&mut self, which: Which) -> &mut FlattenRun {
        let flatten = match which {
            Which::One => &mut self.drop_run().flatten,
            Which::Two => &mut self.two_run().flatten,
            Which::Cross | Which::Take | Which::Pages => &mut self.stage_run(which).flatten,
        };
        flatten.get_or_insert_with(|| FlattenRun {
            after: crate::flatten::AFTER_FILL,
            ..FlattenRun::default()
        })
    }

    /// The crossing session begun last, as the evidence holds it (FBC-su9i).
    fn cross_run(&mut self) -> &mut CrossRun {
        self.stage_run(Which::Cross)
    }

    /// Stage two's session `which` begun last, as the evidence holds it: the crossing session
    /// (FBC-su9i), or, for [`Which::Take`], the taker IOC session (FBC-ou9g); for
    /// [`Which::Pages`], stage three's session (FBC-4plj).
    fn stage_run(&mut self, which: Which) -> &mut CrossRun {
        if which == Which::Pages {
            return &mut self.three_run().session;
        }
        let two = self.ev.stage_two.get_or_insert_with(StageTwo::default);
        match which {
            Which::Take => two.takes.last_mut().expect("a taker IOC session begun"),
            _ => two.crosses.last_mut().expect("a crossing session begun"),
        }
    }

    /// Stage three's run begun last, as the evidence holds it (FBC-4plj).
    fn three_run(&mut self) -> &mut PagesRun {
        (self.ev.stage_three.last_mut()).expect("a stage-three session begun")
    }

    /// The drop test's verdict on `on` once drop session two dropped (decision 0132 item 4, 0151
    /// items 5 and 13): the reads after the drop until X and Y are absent ([`Run::after_drop`]),
    /// then every `get_user_trades` page since X's send and every `GET /user/historic-orders`
    /// page ([`Run::read_orders`]), judged with what drop session two and the read before it
    /// showed. Whether the owner pressed Ctrl-C meanwhile is the second given: the presses are
    /// taken, none ending the reads.
    async fn judge_drop(
        &mut self,
        on: Armed,
        symbol: &str,
        x: Placed,
        (x_allowed, y_allowed): (&AllowedOrder, &AllowedOrder),
    ) -> (crate::verdict::DropVerdict, bool) {
        let order = |o: &AllowedOrder| crate::verdict::DropOrder::new(o.cid, o.venue.clone());
        let orders = order(x_allowed).and_then(|x| order(y_allowed).map(|y| (x, y)));
        let (after_drop, pressed) = match &orders {
            Ok((x, y)) => self.after_drop(on, symbol, (x, y)).await,
            Err(_) => (crate::verdict::AfterDrop::default(), false),
        };
        let both = [x_allowed, y_allowed];
        let read = self.read_orders(Which::Two, on, x.wall_secs, &both);
        let (traded, refused, read_pressed) = read.await;
        let refused = after_drop
            .refused
            .map(|on| Stop::RateRefused { on })
            .or(refused);
        let refused = refused.and_then(|stop| match stop {
            Stop::RateRefused { on } => Some(on),
            _ => None,
        });
        let run = self.drop_run();
        let two = run.two.clone().unwrap_or_default();
        let naming = [0, 1].map(|n| traded.naming.get(n).copied().unwrap_or(0));
        let terminal = [0, 1].map(|n| traded.terminal.get(n).cloned().flatten());
        let facts = crate::verdict::Facts {
            x_before_two: run.x_before_two.unwrap_or(false),
            arm_reported_x: two.x_cancel_reported,
            x_cancelled_before_drop: two.x_cancelled_before_drop,
            x_before_drop: two.before_drop.map(|[x, _]| x),
            y_before_drop: two.before_drop.map(|[_, y]| y),
            y_rested: two.y.acked,
            y_open_at_drop: two.y_open_at_drop,
            after: after_drop.last,
            // Whole when every page was read, whatever a later read met: a refusal is its own
            // failure (FBC-77r8's review), and one met reading the trades left them not whole.
            traded: traded.trades_whole.then_some(naming),
            // What the pages read showed, whole or not: a fill or a status a page showed is not
            // erased by a later page that failed (0151 items 5 and 9; FBC-77r8's review).
            traded_seen: naming,
            // The rows naming neither X nor Y, on the pages read: another order's, or none's.
            other_trades: traded.trades.saturating_sub(naming.iter().sum()),
            historic: traded.historic_whole.then(|| terminal.clone()),
            terminal_seen: terminal,
            others_after: after_drop.others,
            refused,
        };
        let verdict = crate::verdict::DropVerdict::judge(&facts);
        let two = self.two_run();
        two.after_drop = Some(after_drop);
        two.after = Some(traded);
        self.line(format_args!(
            "VERDICT drop test on {}: {}",
            name(on),
            match verdict.passed {
                true => "passed",
                false => "failed",
            }
        ));
        (verdict, pressed || read_pressed)
    }

    /// The reads after the drop on `on` ([`crate::verdict::until_absent`]): never-arming reads
    /// of `get_user_orders` and REST positions on `symbol`, every frame and request charged to
    /// the shared limiter first, at the comparisons' cadence, until one lists neither `orders`
    /// nor the wait ends (0151 item 4), its socket's refusals reported under
    /// [`AFTER_DROP_TWO`]. The owner's presses meanwhile are taken, none ending them: whether
    /// one was is the second given.
    async fn after_drop(
        &mut self,
        on: Armed,
        symbol: &str,
        orders: (&crate::verdict::DropOrder, &crate::verdict::DropOrder),
    ) -> (crate::verdict::AfterDrop, bool) {
        let mut presses = std::mem::take(&mut self.presses);
        let mut pressed = false;
        let made = match self.socket_parts(on) {
            Err(_) => None,
            Ok(mut parts) => {
                let places = self.places.expect("a place-stage run");
                let cadence = crate::compare::Cadence::of(places);
                let ctx = &mut *parts.ctx;
                let mut socket =
                    crate::compare::Socket::new(self.io(symbol), &parts.cfg, &parts.reader, ctx);
                let after = {
                    let reads = crate::verdict::until_absent(&mut socket, orders, cadence);
                    tokio::pin!(reads);
                    loop {
                        tokio::select! {
                            biased;
                            after = &mut reads => break after,
                            () = presses.pressed() => pressed = true,
                        }
                    }
                };
                Some((after, socket.close().await))
            }
        };
        pressed |= presses.try_pressed();
        self.presses = presses;
        let Some((after, heard)) = made else {
            return (crate::verdict::AfterDrop::default(), pressed);
        };
        self.ev.read_sockets.push(ReadSocket {
            before: AFTER_DROP_TWO,
            label: format!("{} (the reads after the drop)", name(on)),
            refusals: heard,
        });
        self.line(format_args!(
            "READ after the drop: {}: {} reads, {}",
            name(on),
            after.reads.len(),
            match after.absent_after {
                Some(_) => "the last listing neither X nor Y",
                None => "none listing neither X nor Y",
            }
        ));
        (after, pressed)
    }

    /// What a never-arming read socket of `on`'s is made of: `on`'s configuration, which the
    /// guard admitted, its reader and the context of each signed frame or request.
    fn socket_parts(&self, on: Armed) -> Result<SocketParts, String> {
        let (cfg, _) = configured(&self.base, self.lookup, on)?;
        let selection = account_selection(&cfg).map_err(|e| e.to_string())?;
        let reader = auth_qfex::reader(self.lookup, selection)?;
        let mut nonces = self.nonces.clone();
        let origin = self.origin;
        let ctx = Box::new(move || EncodeCtx {
            wall: wall_ns(),
            mono: ledger::mono_since(origin),
            nonces: nonces.reserve(ReadAuth::NONCES),
        });
        Ok(SocketParts { cfg, reader, ctx })
    }

    /// What a never-arming read socket reads with: the run's connector, shared limiter, caps,
    /// namespace, page size and waits, and the traded `symbol` as QFEX writes it.
    fn io<'s>(&'s self, symbol: &'s str) -> crate::compare::Io<'s> {
        crate::compare::Io {
            connector: &self.connector,
            limiter: &self.limiter,
            caps: &self.caps,
            ns: self.ns(),
            page_size: self.opts.page_size,
            connect_wait: self.opts.connect_wait,
            read_wait: self.opts.read_wait,
            symbol,
        }
    }

    /// A place stage's session configuration on `cfg` (decision 0148 item 4; [`place_config`]):
    /// the discovered spec table, the run's `--namespace`, its one `RpcIds`, the run's next
    /// session number, an `AccountWideCodAuthority` for its own account `acct` (decisions 0121
    /// and 0122), and one connection attempt.
    fn place_session_config(
        &self,
        acct: AccountKey,
        cfg: VenueConfig,
        creds: fbc_core::Secrets,
        specs: SpecTable,
    ) -> ExecSessionConfig {
        let pacing = place_pacing(self.opts.connect_wait);
        let bound = ReservedBound::new(4, 4).expect("a bound with Normal room");
        let conn = u16::try_from(self.ev.sessions.len()).expect("a few sessions");
        ExecSessionConfig {
            venue: &QFEX,
            cfg,
            creds,
            acct,
            account_wide_cod_authority: Some(AccountWideCodAuthority::new(acct)),
            rpc_ids: self.rpc_ids.clone(),
            ns: self.ns(),
            specs,
            connector: self.connector.clone(),
            pacing,
            clock: self.clock.clone(),
            nonces: Box::new(self.nonces.clone()),
            nonce_source: NonceSourceId(0),
            conn,
            limiter: self.limiter.clone(),
            write_stall: WriteStall::new(Duration::from_secs(10)).expect("a ten-second window"),
            http_max_body: 1 << 20,
            outbound: bound,
            in_flight: InFlight::new(bound),
        }
    }

    /// A drop session on `on` (decision 0132 item 4): drop session one, or, given X (`x`, which
    /// session one placed), drop session two. An unchanged `ExecSession` whose handler is a
    /// [`PlaceHandler`] over the drop registry's `link`, its steps ([`Run::drop_steps`] or
    /// [`Run::two_steps`]) run beside it, the MDS touch driven on by [`Run::stage`] meanwhile
    /// (FBC-kllr). Whatever ends the steps, a stop that cancels in a drop session (the owner's
    /// Ctrl-C, or any stop once a fill of ours came: 0151 item 2) is followed by its cancels
    /// ([`Run::stop_cancels`]) and a comparison after them; every other stop sends no cancel. Then
    /// the session ends by a graceful close (0151 item 11: no drop test passed yet), unless its
    /// connection ended first, after waiting for the shared budget's room when a provider refusal
    /// holds it; a Ctrl-C while it waits, with no cancels run yet, is taken as one made during the
    /// steps, and so is a stop the tee or the handler raised meanwhile, a fill of ours among them:
    /// its cancels run once there is room, or, with none within the patience, are held as 0151 item
    /// 11 says. But drop session two that ran its course, Y placed and the read before the drop
    /// made, ends by the abrupt drop: its run is dropped, unpolled, and its connection with it,
    /// with no WebSocket close, no wait for room and no cancel (decision 0132 item 4). Gives the
    /// run's stop, if any came, the order it placed (X, or Y), once it was submitted, and each
    /// flatten IOC it sent, so the reads after it count one left resting as ours; an error is a
    /// failure before the session opened.
    async fn drop_session(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        (cfg, creds, link, span_end): (
            VenueConfig,
            fbc_core::Secrets,
            Rc<RefCell<Link>>,
            tokio::time::Instant,
        ),
        allowance: &mut Allowance,
        x: Option<Placed>,
    ) -> Result<(Option<Stop>, Option<Placed>, Vec<fbc_core::ClientOrderId>), String> {
        let which = match x {
            None => Which::One,
            Some(_) => Which::Two,
        };
        let acct = account_key(on);
        let label = name(on);
        let before = self.rejected();
        let sockets = self.ev.read_sockets.len();
        let wake = Rc::new(tokio::sync::Notify::new());
        // A later session of the registry's watches only what follows it.
        let watched = Rc::new(RefCell::new(Watched::after(&link.borrow())));
        let control = Rc::new(RefCell::new(None));
        let handler = PlaceHandler {
            inner: LinkHandler {
                link: Rc::clone(&link),
                wake: Rc::clone(&wake),
            },
            watched: Rc::clone(&watched),
            control: Rc::clone(&control),
        };
        let config = self.place_session_config(acct, cfg, creds, market.found.specs.clone());
        let built = ExecSession::new(config, handler);
        let (mut session, ctl) = built.map_err(|e| format!("the {label} session: {e}"))?;
        *control.borrow_mut() = Some(ctl);
        session.set_journal(self.journal.clone());
        let orders = session.orders();
        link.borrow_mut().attach(orders.clone(), acct);
        match which {
            Which::One => self.line(format_args!(
                "SESSION {label} (drop session one): it arms, places X, compares, and closes \
                 gracefully"
            )),
            _ => self.line(format_args!(
                "SESSION {label} (drop session two): it arms, places Y, reads before the drop, \
                 and drops abruptly"
            )),
        }
        let (heard_from, sent_from) = {
            let kept = self.tee.kept();
            (kept.heard.len(), kept.sent().len())
        };
        let d = DropCtx {
            link,
            wake,
            watched: Rc::clone(&watched),
            orders,
            acct,
            inst: market.found.inst,
            caps: market.caps,
            span_end,
            on,
            which,
            heard_from,
            sent_from,
        };
        let mut presses = std::mem::take(&mut self.presses);
        let mut placed = None;
        let (mut stop, mut error) = (None, None);
        let view = market.touch.clone();
        let found = market.found.clone();
        // Whether `running` completed: an `async fn` polled again once it completed panics, so
        // every select on it after the first one that took it is skipped.
        let mut over = false;
        let (ended_by, rested_at_close);
        {
            let running = session.run();
            tokio::pin!(running);
            let steps: Pin<Box<dyn Future<Output = Result<(), Halt>> + '_>> = match x {
                None => Box::pin(self.drop_steps(
                    places,
                    &found,
                    &view,
                    &d,
                    allowance,
                    &mut presses,
                    &mut placed,
                )),
                Some(x) => Box::pin(self.two_steps(
                    places,
                    (&found, &view),
                    &d,
                    allowance,
                    &mut presses,
                    (x, &mut placed),
                )),
            };
            let stepped = tokio::select! {
                biased;
                ended = &mut running => Err(ended),
                stepped = steps => Ok(stepped),
            };
            match stepped {
                // A stop the tee or the handler raised as the steps ended is the session's too.
                Ok(Ok(())) => stop = self.stop_now(&d),
                Ok(Err(Halt::Stop(s))) => stop = Some(s),
                Ok(Err(Halt::Error(e))) => error = Some(e),
                // The session ended first: its handler or the tee says why, or it ended before
                // any epoch opened.
                Err(ended) => {
                    over = true;
                    (stop, error) = ended_first(self.stop_now(&d), ended);
                }
            }
            // Every order of the drop test placed so far: X, and Y in drop session two.
            let ours = (x.iter().chain(placed.iter())).map(|p| p.cid);
            let ours: Vec<fbc_core::ClientOrderId> = ours.collect();
            // A fill of ours, the session still open: the flatten, before its Stop (FBC-ou9g;
            // 0151 item 9).
            let flattened = d.watched.borrow().fill_of_ours && !over;
            if flattened {
                (stop, over) = self
                    .flatten_open(
                        places,
                        (&found, &view),
                        &d,
                        &mut running,
                        (allowance, &mut presses),
                        (&ours, which.name()),
                        stop,
                    )
                    .await;
            }
            let policy = Policy::Drop {
                abrupt: which == Which::Two,
            };
            let closed = self
                .end_session(
                    places,
                    (&found, &view),
                    &d,
                    (&mut running, &control),
                    (allowance, &mut presses),
                    policy,
                    (stop, error.is_some(), over, flattened, false),
                    (&ours, placed.map(|p| p.cid), before),
                )
                .await;
            if let Some(cancels) = closed.cancels {
                self.stop_cancelled(which, cancels);
            }
            if let Some(compared) = closed.compared {
                self.compared(which, compared);
            }
            // A press, or a stop the tee or the handler raised, taken as the close waited.
            stop = closed.stop;
            ended_by = closed.ended_by;
            rested_at_close = closed.rested_at_close;
        }
        // A stop raised after the steps, as the session closed or while it waited for room to
        // close, is the session's when none came before it.
        stop = stop.or_else(|| self.stop_now(&d));
        self.presses = presses;
        let counters = session.counters();
        drop(session);
        // What the comparisons' sockets put on the shared limiter themselves, each counted under
        // its socket ([`Run::compare_step`]), so not counted again as the session's.
        let compared: u64 = (self.ev.read_sockets[sockets..].iter())
            .map(|r| r.refusals.limiter)
            .sum();
        let refused = self.rejected().saturating_sub(before + compared);
        let seen = (counters, refused, error.clone(), ended_by, rested_at_close);
        self.session_seen(&d, label, seen);
        // Each flatten IOC the session sent, its fills expected ([`Watched::expected`]).
        let iocs = watched.borrow().expected.clone();
        match (stop, error) {
            (None, Some(why)) => {
                match which {
                    Which::One => self.drop_run().error = Some(why),
                    _ => self.two_run().error = Some(why),
                }
                Ok((None, placed, iocs))
            }
            (stop, _) => Ok((stop, placed, iocs)),
        }
    }

    /// Records what session `d` on `label` showed as it ended, in its record and as a session of
    /// the run, and prints its `SESSION` line: its counters, the provider rate refusals its
    /// runtime made while it ran (its comparisons' own taken off), its error, how it ended and
    /// whether its own order rested as its close was sent, then what the tee heard of it and
    /// what its handler saw.
    fn session_seen(
        &mut self,
        d: &DropCtx,
        label: &'static str,
        (counters, refused, error, ended_by, rested_at_close): (
            fbc_runtime::ExecCounters,
            u64,
            Option<String>,
            &'static str,
            bool,
        ),
    ) {
        let w = d.watched.borrow();
        let arm = w.arm.as_ref().map(places::outcome_name);
        let (echoes, as_encoded) = {
            let kept = self.tee.kept();
            let echoes = places::echoes(&kept, d.heard_from);
            (echoes, places::as_encoded(&kept, d.sent_from))
        };
        let (authenticated, resynced) = (w.authenticated, w.resynced);
        drop(w);
        // Stage three's session has no order of its own until P1 is placed.
        if d.which != Which::Pages || !self.three_run().orders.is_empty() {
            self.placed_run(d.which).rested_at_close = rested_at_close;
        }
        match d.which {
            Which::One => {
                let run = self.drop_run();
                run.echoes = echoes;
                run.as_encoded = as_encoded;
                run.authenticated = authenticated;
                run.arm.clone_from(&arm);
                run.resynced = resynced;
                run.ended_by = Some(ended_by);
            }
            Which::Two => {
                let two = self.two_run();
                two.echoes = echoes;
                two.as_encoded = as_encoded;
                two.authenticated = authenticated;
                two.arm.clone_from(&arm);
                two.resynced = resynced;
                two.ended_by = Some(ended_by);
            }
            Which::Cross | Which::Take | Which::Pages => {
                let run = self.stage_run(d.which);
                run.echoes = echoes;
                run.as_encoded = as_encoded;
                run.authenticated = authenticated;
                run.arm.clone_from(&arm);
                run.resynced = resynced;
                run.ended_by = Some(ended_by);
            }
        }
        self.line(format_args!(
            "SESSION {label} ({}): arm outcome {}, ended by {ended_by}",
            d.which.name(),
            arm.as_deref().unwrap_or("none")
        ));
        self.ev.sessions.push(SessionRun {
            account: label,
            name: d.which.name(),
            authenticated,
            arm,
            ended_by,
            attempts: counters.attempts,
            failed_attempts: counters.failed_attempts,
            arm_failures: counters.arm_failures,
            decode_errors: counters.decode_errors,
            error,
            refusals: Refusals {
                limiter: refused,
                ..Refusals::default()
            },
            ..SessionRun::default()
        });
    }

    /// Ends a place stage's session `d`, its steps ended with `stop` (none when they ran their
    /// course), `failed` when they ended on a failure that is no stop, `over` when its run,
    /// `running`, completed first (its connection ended), and `flattened` when a fill of ours
    /// already had its flatten while the steps' session was open ([`Run::flatten_open`]), and
    /// `late_taker` when the taker IOC session's IOC left nothing to follow it as its steps ended,
    /// as `policy` says: the stop's cancels when it cancels ([`Policy::cancels`];
    /// [`Run::stop_cancels`]: Kill, every order of ours by venue id as fbc-oms authorizations
    /// through the `Link`, the wait for each to end), then, once each was seen to end, the streams
    /// of `ours` on `view`'s market compared with the reads, the comparison's stop the session's
    /// when none came before it; then the flatten of a fill of ours that reached the session only
    /// during those cancels or after them, the taker IOC's own under `late_taker` once it left the
    /// position off flat ([`Run::late_fill`]), the session still open ([`Run::flatten_late`], from
    /// the account's `allowance`), the stop that came first, the comparison's included, kept
    /// before the flatten's own; then the market disarmed once those cancels ran to their end
    /// ([`Run::stop_disarm`]); then the ending [`Policy::ending`] gives: a graceful close, which
    /// waits first for the shared budget's room when a provider rate refusal holds it (0151 item
    /// 11), or the abrupt drop, its end planned and its run left for the caller to drop unpolled.
    /// While the close waits for room with no cancels run, a press of Ctrl-C, or a stop the tee or
    /// the handler raised that cancels under `policy` (a fill of ours in a drop session), is the
    /// session's stop, and its cancels run first, then that flatten, then the disarm (0151 items
    /// 2, 9 and 11); [`Closed::stop`] gives it, the flatten's own in place of the fill's. A press
    /// the stop's cancels or that flatten took is the session's stop when none came before it; a
    /// stop the tee or the handler raised before that press came first, and is the session's,
    /// whether or not the connection's end cuts the cancels short (decision 0148 item 8). A
    /// fill of ours that reaches the session while the close waits for room after those cancels
    /// ran is flattened once there is room, the market disarmed again after it, and the close
    /// waits again. The touch is driven throughout by [`Run::stage`] (FBC-kllr), so the touch a
    /// late flatten prices from is current and its age counts the waits. A connection that ended
    /// first ends it, and nothing waits on its run again. `own` is the session's own order, whose
    /// state the close is sent with; `before`, the shared limiter's refusal count as the session
    /// began.
    #[allow(clippy::too_many_arguments)]
    async fn end_session<F>(
        &mut self,
        places: &Places,
        view: (&Discovered, &market::TouchView),
        d: &DropCtx,
        (running, control): (&mut Pin<&mut F>, &RefCell<Option<ExecControl>>),
        (allowance, presses): (&mut Allowance, &mut Presses),
        policy: Policy,
        (stop, failed, mut over, mut flattened, late_taker): (Option<Stop>, bool, bool, bool, bool),
        (ours, own, before): (
            &[fbc_core::ClientOrderId],
            Option<fbc_core::ClientOrderId>,
            u64,
        ),
    ) -> Closed
    where
        F: Future<Output = Result<(), ExecSessionError>>,
    {
        let found = view.0;
        let mut closed = Closed {
            stop,
            ended_by: GRACEFUL,
            cancels: None,
            compared: None,
            rested_at_close: false,
        };
        // The stop the session's ending is judged by: the steps' own, or the late flatten's in
        // place of the fill it answered. A press taken as the close waits is not one of them, so
        // drop session two that ran its course still waits for no room ([`Policy::ending`]).
        let mut ending_stop = stop;
        // The stop's cancels, then the late flatten, the disarm and the wait for room to close. A
        // press of Ctrl-C taken while the close waits, before any cancels ran, is a stop that
        // cancels too (0151 item 2), and so is a stop the tee or the handler raised meanwhile that
        // cancels under `policy`, a fill of ours among them in a drop session: they run once there
        // is room, or, with none within the patience, are held (0151 item 11), and the close
        // waits again after them.
        let (mut cancelled, mut pressed, mut roomless) = (false, false, false);
        // Whether the market was disarmed after the stop's cancels, so that a late flatten after
        // it, which arms the market again into Exit, is followed by a disarm of its own.
        let mut disarmed = false;
        let ending = loop {
            let fill = d.watched.borrow().fill_of_ours || self.tee.kept().fill_of_ours;
            let mut disarm = false;
            if !cancelled && !over && (pressed || policy.cancels(closed.stop, fill)) {
                cancelled = true;
                // The cancels record into `cancels` as they go, so what they sent and the presses
                // they took survive the connection's end cutting them short.
                let mut cancels = Cancels::default();
                let cut_short = tokio::select! {
                    biased;
                    _ = &mut *running => true,
                    () = self.stop_cancels(d, presses, &mut cancels) => false,
                };
                over |= cut_short;
                // Cancels that ran to their end leave the market Killed and still armed, for the
                // disarm below.
                disarm = !cut_short;
                if cut_short {
                    cancels.held = true;
                    cancels.cut_short = true;
                }
                let compare = !cancels.held && cancels.ended && !over;
                // A press of Ctrl-C those cancels took, which ended none of them, is the session's
                // stop when none came before it (decision 0148 item 8): under `Policy::Stage` they
                // run on a session that ran its course too, and the press is not lost there. A
                // stop the tee or the handler raised before that press came first, and is the
                // session's ([`Run::halt`]: the first stop is the run's).
                if cancels.presses > 0 {
                    let first = cancels.stop_at_press.unwrap_or(Stop::Interrupted);
                    closed.stop = closed.stop.or(Some(first));
                }
                closed.cancels = Some(cancels);
                if compare {
                    let step = "the stop's cancels";
                    let compare = self.compare_step(d, (found, ours), d.on, step, None);
                    let compared = tokio::select! {
                        biased;
                        _ = &mut *running => None,
                        c = compare => Some(c),
                    };
                    over |= compared.is_none();
                    // The comparison's stop, a provider rate refusal or a read that disagrees, is
                    // the session's when none came before it. Under `Policy::Stage` the cancels
                    // run on a session that ran its course too, and the reads after the session
                    // begin past this refusal, so no later read takes it again.
                    if let Some((c, _)) = &compared {
                        closed.stop = closed.stop.or(c.stop);
                    }
                    closed.compared = compared;
                }
            }
            // A fill of ours that reached the session only during its Stop's cancels or after
            // them, the taker IOC's own included, the session still open: the flatten now, before
            // its close (0151 item 9).
            let late = (!flattened && !over)
                .then(|| self.late_fill(d, late_taker, own))
                .flatten();
            if let Some(after) = late {
                flattened = true;
                let first = closed.stop.or_else(|| self.stop_now(d));
                let killed = closed.cancels.is_some();
                let late;
                (late, over) = self
                    .flatten_late(
                        places,
                        view,
                        d,
                        &mut *running,
                        (&mut *allowance, &mut *presses),
                        (ours, d.which.name()),
                        (first, killed, after),
                    )
                    .await;
                (closed.stop, ending_stop) = (late, late);
                // A late flatten after the disarm armed the market again, into Exit under its
                // market lease: disarmed again, its lease released, as the Stop left it.
                if disarmed {
                    d.link.borrow_mut().reg().disarm(d.inst);
                    self.line(format_args!(
                        "FLATTEN on {}: the market disarmed again, as the Stop left it",
                        name(d.on)
                    ));
                }
            }
            if disarm && let Some(c) = closed.cancels.as_ref() {
                self.stop_disarm(d, c);
                disarmed = true;
            }
            let ending = policy.ending(ending_stop, failed, closed.cancels.as_ref());
            // A provider rate refusal holds the shared budget (decision 0126), the session's
            // close frame included, which the runtime would then drop unsent, the connection with
            // it: the close waits out the hold, the session read on meanwhile, so it is graceful
            // (0151 item 11). A hold that outlasts the patience leaves no room.
            if !over && ending == Ending::Close && !roomless && self.rejected() > before {
                let room = self.room_for_close();
                tokio::pin!(room);
                let room = loop {
                    tokio::select! {
                        biased;
                        _ = &mut *running => break None,
                        () = presses.pressed(), if !cancelled && !pressed => pressed = true,
                        room = &mut room => break Some(room),
                    }
                };
                over |= room.is_none();
                roomless = room == Some(false);
                if roomless {
                    closed.ended_by = NO_ROOM_TO_CLOSE;
                }
            }
            // The press is the session's stop when none came before it, even when the connection
            // ended as it waited, so that it is not lost: no later read takes it again.
            if pressed && !cancelled {
                closed.stop = closed.stop.or(Some(Stop::Interrupted));
            }
            // A stop the tee or the handler raised as the close waited is the session's too when
            // none came before it, and cancels as one raised during the steps would: back to the
            // cancels, with room.
            closed.stop = closed.stop.or_else(|| self.stop_now(d));
            let fill = d.watched.borrow().fill_of_ours || self.tee.kept().fill_of_ours;
            if !cancelled && !over && (pressed || policy.cancels(closed.stop, fill)) {
                continue;
            }
            // A fill of ours that reached the session as the close waited, its stop's cancels
            // already run: back to its flatten, with room (0151 item 9).
            if !flattened && !over && self.late_fill(d, late_taker, own).is_some() {
                continue;
            }
            break ending;
        };
        if over {
            closed.ended_by = CONNECTION_ENDED;
        } else if ending == Ending::Drop {
            // The abrupt drop (decision 0132 item 4): its end planned, the session's run is
            // dropped by the caller unpolled, its connection with it, with no WebSocket close.
            d.watched.borrow_mut().closing = true;
            closed.ended_by = DROPPED;
        } else {
            // What a graceful close could cancel: the session's own order as the registry holds
            // it now.
            closed.rested_at_close = own.is_some_and(|cid| {
                let state = d.link.borrow().registry().get(cid).map(|r| r.state());
                matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled))
            });
            // The close: the control's drop sends the close frame (decision 0132 item 4), the
            // end of its connection planned.
            d.watched.borrow_mut().closing = true;
            drop(control.borrow_mut().take());
            let _ = tokio::time::timeout(CLOSE_WAIT, &mut *running).await;
        }
        closed
    }

    /// Records `cancels`, a stop's cancels in session `which`.
    fn stop_cancelled(&mut self, which: Which, cancels: Cancels) {
        match which {
            Which::One => self.drop_run().cancels = Some(cancels),
            Which::Two => self.two_run().cancels = Some(cancels),
            Which::Cross | Which::Take | Which::Pages => {
                self.stage_run(which).cancels = Some(cancels)
            }
        }
    }

    /// Waits, for [`AFTER_PATIENCE`] at most, until the shared limiter has room for a control
    /// frame, a session's close: each time it has none, until the time it gives. The room is
    /// found by charging one control frame, so the close is charged twice, which over-counts
    /// what is sent and never under-counts it (decision 0030). Whether room came: none when the
    /// hold ends past the patience or the limiter gives no time ([`NO_ROOM_TO_CLOSE`]).
    async fn room_for_close(&self) -> bool {
        let until = tokio::time::Instant::now() + AFTER_PATIENCE;
        loop {
            let now = tokio::time::Instant::now();
            match self.limiter.charge(now, NO_CONN, &[CONTROL]) {
                Ok(_) => return true,
                Err(fbc_runtime::Refused {
                    ready_at: Some(ready),
                }) if ready <= until => tokio::time::sleep_until(ready).await,
                Err(_) => return false,
            }
        }
    }

    /// The first stop the tee or the session's handler raised, if any: the tee's first.
    fn stop_now(&self, d: &DropCtx) -> Option<Stop> {
        self.tee.stop().or_else(|| d.watched.borrow().stop())
    }

    /// Waits until `found` finds what it looks for in the session's `Link` and handler, or a
    /// stop comes first: one the tee or the handler raised ([`Run::stop_now`]), the owner's
    /// Ctrl-C ([`Stop::Interrupted`]) or the registry's span bound ([`Stop::SpanBound`]).
    async fn waiting<T>(
        &self,
        d: &DropCtx,
        presses: &mut Presses,
        mut found: impl FnMut(&Link, &Watched) -> Option<T>,
    ) -> Result<T, Stop> {
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if let Some(stop) = self.stop_now(d) {
                return Err(stop);
            }
            if let Some(t) = found(&d.link.borrow(), &d.watched.borrow()) {
                return Ok(t);
            }
            tokio::select! {
                biased;
                () = presses.pressed() => return Err(Stop::Interrupted),
                () = tokio::time::sleep_until(d.span_end) => return Err(Stop::SpanBound),
                _ = self.tee.stopped() => {}
                () = woken => {}
            }
        }
    }

    /// The stop already raised, or a press already made, if either: as [`Run::waiting`] would
    /// take it at once, without waiting.
    fn not_stopped(&self, d: &DropCtx, presses: &mut Presses) -> Result<(), Stop> {
        match self.stop_now(d) {
            Some(stop) => Err(stop),
            None if presses.try_pressed() => Err(Stop::Interrupted),
            None => Ok(()),
        }
    }

    /// `work`, or the stop that comes first, as [`Run::waiting`] watches for one.
    async fn unless_stopped<T>(
        &self,
        d: &DropCtx,
        presses: &mut Presses,
        work: impl Future<Output = T>,
    ) -> Result<T, Stop> {
        tokio::pin!(work);
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if let Some(stop) = self.stop_now(d) {
                return Err(stop);
            }
            tokio::select! {
                biased;
                () = presses.pressed() => return Err(Stop::Interrupted),
                () = tokio::time::sleep_until(d.span_end) => return Err(Stop::SpanBound),
                done = &mut work => return Ok(done),
                _ = self.tee.stopped() => {}
                () = woken => {}
            }
        }
    }

    /// Waits for the session's stream authenticated, its arm accepted and its resync applied,
    /// the place gate open: an arm that came back otherwise is a failure that is no stop, its
    /// order not placed (decision 0132 item 4).
    async fn armed(&self, d: &DropCtx, presses: &mut Presses) -> Result<(), Halt> {
        let orders = d.orders.clone();
        let arm = self
            .waiting(d, presses, |_, w| {
                let ready = w.authenticated && w.resynced && orders.may_place();
                let refused =
                    (w.arm.as_ref()).filter(|a| !matches!(a, SubmitOutcome::Accepted { .. }));
                match (ready, refused) {
                    (_, Some(arm)) => Some(Err(places::outcome_name(arm))),
                    (true, None) => Some(Ok(())),
                    (false, None) => None,
                }
            })
            .await?;
        arm.map_err(|name| {
            Halt::Error(format!(
                "{}'s arm came back {name}, not Accepted: {} is not placed (decision 0132 item 4)",
                d.which.name(),
                d.which.order()
            ))
        })
    }

    /// Drop session one's steps (decision 0132 item 4): authentication, an `Accepted` arm and
    /// the resync, the place gate open ([`Run::armed`]); Start under the market lease; X
    /// ([`Run::place_resting`]); then the streams compared with the reads. Each step ends at the
    /// first stop ([`Run::waiting`]).
    #[allow(clippy::too_many_arguments)]
    async fn drop_steps(
        &mut self,
        places: &Places,
        found: &Discovered,
        touch: &market::TouchView,
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        x: &mut Option<Placed>,
    ) -> Result<(), Halt> {
        self.armed(d, presses).await?;
        let dir = self.leases.as_ref().map(|l| l.dir.clone());
        let dir = dir.ok_or_else(|| Halt::Error("no lease directory".to_owned()))?;
        places::start(d.link.borrow_mut().reg(), &dir, d.inst).map_err(Halt::Error)?;
        self.drop_run().started = true;
        self.line(format_args!(
            "START {}: the market armed under its market lease, with no account lease (QFEX's \
             nonce scope is none)",
            name(d.on)
        ));
        let placed = self
            .place_resting(places, (found, touch), d, allowance, presses, x)
            .await?;
        let ours = [placed.cid];
        let compared = self.compare_step(d, (found, &ours), d.on, "X's place", Some(placed.at));
        let compared = self.unless_stopped(d, presses, compared).await?;
        let stop = compared.0.stop;
        self.compared(Which::One, compared);
        match stop {
            Some(stop) => Err(Halt::Stop(stop)),
            None => Ok(()),
        }
    }

    /// Drop session two's steps (decision 0132 item 4): authentication, an `Accepted` arm and
    /// the resync ([`Run::armed`]); no Start, since the market stays armed from drop session one
    /// on the same registry; Y ([`Run::place_resting`]); then the read before the drop, one
    /// never-arming read of `get_user_orders` and REST positions beside the session, which lists
    /// X, `x`, and Y or not. Each step ends at the first stop ([`Run::waiting`]), and a provider
    /// rate refusal in the read is one (decision 0148 item 8); a stop raised, or a press made,
    /// as the read's socket closed is taken before the drop, so Ctrl-C then cancels. Then a read
    /// showing what the streams did not, an order or a TWAP neither X nor Y or a position the
    /// registry does not hold, is a stop before the drop ([`Stop::ReadDisagrees`], decision 0148
    /// item 8; FBC-77r8's review), and so is a read not made ([`Stop::LeftResting`]), neither
    /// cancelling anything (0151 item 2). Then, as
    /// the session drops (nothing is awaited between the steps' end and the drop), what its
    /// frames and its registry showed of X and Y ([`Run::at_drop`]).
    async fn two_steps(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        (x, y): (Placed, &mut Option<Placed>),
    ) -> Result<(), Halt> {
        self.armed(d, presses).await?;
        let placed = self
            .place_resting(places, (found, touch), d, allowance, presses, y)
            .await?;
        let vid = |cid| (d.link.borrow().registry().get(cid)).and_then(|r| r.vid().cloned());
        let order = |cid| crate::verdict::DropOrder::new(cid, vid(cid)).map_err(Halt::Error);
        let (x, y) = (order(x.cid)?, order(placed.cid)?);
        let parts = self.socket_parts(d.on).map_err(Halt::Error)?;
        let symbol = found.spec.venue_symbol.as_wire();
        let SocketParts {
            cfg,
            reader,
            mut ctx,
        } = parts;
        let mut socket = crate::compare::Socket::new(self.io(symbol), &cfg, &reader, &mut *ctx);
        let read = self.unless_stopped(d, presses, socket.read()).await?;
        // The arm's window ends with the read before the drop: a CANCELLED of X heard by then
        // is the arm's, reported (decision 0132 item 4: no report settles nothing until that
        // read).
        let reported = self
            .tee
            .kept()
            .cancelled_of_from(d.heard_from, x.cid_text());
        let heard = socket.close().await;
        self.two_run().x_cancel_reported = reported;
        self.ev.read_sockets.push(ReadSocket {
            before: places::DROP_TWO,
            label: format!("{} (the read before the drop)", name(d.on)),
            refusals: heard,
        });
        // What the streams did not show (decision 0148 item 8): an order or a TWAP that is
        // neither X nor Y, which the account-wide drop could cancel out of sight, or a position
        // the registry does not hold. X absent is allowed, since the arm may have cancelled it.
        let mut disagrees = false;
        let before_drop = match read {
            crate::compare::Attempt::Read(read) => {
                let listing = crate::verdict::Listing::of(&read, &x, &y);
                let streams = streams_of(&d.link.borrow(), d.inst, found, &[x.cid, y.cid]);
                let others = crate::verdict::others(&read, &x, &y);
                disagrees = others > 0 || read.position != streams.position;
                Some([listing.x, listing.y])
            }
            crate::compare::Attempt::Refused(on) => {
                self.line(format_args!(
                    "READ before the drop: {}: refused by a provider rate refusal",
                    name(d.on)
                ));
                return Err(Halt::Stop(Stop::RateRefused { on }));
            }
            _ => None,
        };
        self.two_run().before_drop = before_drop;
        self.two_run().before_drop_disagrees = disagrees;
        self.line(format_args!(
            "READ before the drop: {}: {}",
            name(d.on),
            match before_drop {
                Some([x, y]) => format!("X listed {x}, Y listed {y}"),
                None => "not made".to_owned(),
            }
        ));
        // A stop raised, or a press made, meanwhile comes first: Ctrl-C or a fill of ours then
        // cancels (0151 item 2).
        self.not_stopped(d, presses)?;
        // Then the read's own stop, before the drop: it cancels nothing, and the session ends by
        // a graceful close (0151 item 2), its orders left for the owner's hand cancel. A read not
        // made (it failed, or found no room) looked at nothing the drop could cancel out of
        // sight: X and Y may rest unjudged, as an account the read before drop session two could
        // not read leaves X (FBC-77r8's review).
        if before_drop.is_none() {
            self.line(format_args!(
                "READ before the drop: {}: not made, so no drop",
                name(d.on)
            ));
            self.two_run().before_drop_unread = true;
            return Err(Halt::Stop(Stop::LeftResting));
        }
        if disagrees {
            self.line(format_args!(
                "READ before the drop: {}: an order, a TWAP or a position the streams did not \
                 show (decision 0148 item 8): no drop",
                name(d.on)
            ));
            return Err(Halt::Stop(Stop::ReadDisagrees));
        }
        self.at_drop(d, (&x, &y));
        Ok(())
    }

    /// What drop session two showed of X and Y, `x` and `y`, as it drops: whether a `CANCELLED`
    /// of X was heard in the session, so the drop did not end it (decision 0132 item 4), and
    /// whether Y was still open in the registry, with no `CANCELLED` of it heard, so the drop had
    /// Y of its own to cancel (0151 item 13).
    fn at_drop(
        &mut self,
        d: &DropCtx,
        (x, y): (&crate::verdict::DropOrder, &crate::verdict::DropOrder),
    ) {
        let (x_cancelled, y_cancelled) = {
            let kept = self.tee.kept();
            let of =
                |o: &crate::verdict::DropOrder| kept.cancelled_of_from(d.heard_from, o.cid_text());
            (of(x), of(y))
        };
        let state = d.link.borrow().registry().get(y.cid).map(|r| r.state());
        let open = matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled));
        let two = self.two_run();
        two.x_cancelled_before_drop = x_cancelled;
        two.y_open_at_drop = open && !y_cancelled;
    }

    /// Places `d`'s order, X in drop session one, Y in two, or in stage three the one whose
    /// record was pushed last, P1 to P3: a post-only GTC at the instrument's minimum size
    /// `--away-bps` behind the touch, spent from its registry's list first ([`Allowance`]: the
    /// drop registry's, or the pages registry's own), its client id minted under the run's
    /// namespace lease, as an fbc-oms authorization; in a drop session the one place before an
    /// ACK echoes our client id in its session (decision 0132 item 7). Before it is built into a
    /// command, both USD caps are asked again at the touch it is priced at
    /// ([`market::caps_admit`]); an order either would not admit is not sent, and never resized,
    /// the run's stop [`Stop::BeyondCaps`] (decision 0113).
    /// Then its ACK, or its deadline. Gives it, as placed, once handed to the session (`placed`
    /// holds it from then); a stop or a press first places nothing.
    async fn place_resting(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        placed: &mut Option<Placed>,
    ) -> Result<Placed, Halt> {
        let what = match d.which {
            Which::One => ledger::X,
            Which::Pages => ledger::PAGED,
            _ => ledger::Y,
        };
        // Stage three's orders by name, P1 to P3, its record pushed before its place.
        let order = match d.which {
            Which::Pages => crate::pager::NAMES[self.three_run().orders.len() - 1],
            _ => d.which.order(),
        };
        allowance.take(d.which.step(), what)?;
        let leases = self.leases.as_mut().expect("a place-stage run's leases");
        let cid = leases.mint.mint(1).await.map_err(Halt::Error)?[0];
        let quote = touch.now(places.touch_max_age).map_err(Halt::Error)?;
        let px = market::rest_price(&found.spec, places.side, quote, places.away_bps);
        let px = px.map_err(Halt::Error)?;
        // A test's X, or Y, not priced: a failure of its drop session that is no stop.
        #[cfg(test)]
        if places.test.x_unpriced && d.which == Which::One {
            return Err(Halt::Error("a test's X could not be priced".to_owned()));
        }
        #[cfg(test)]
        if places.test.y_unpriced && d.which == Which::Two {
            return Err(Halt::Error("a test's Y could not be priced".to_owned()));
        }
        let new = fbc_core::NewOrder {
            cid,
            inst: d.inst,
            side: places.side,
            qty: found.min_size,
            kind: fbc_core::OrderKind::Limit { px },
            tif: fbc_core::Tif::Gtc,
            channel: fbc_core::Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        // A stop raised, or a press made, while the id was minted and the touch read, before the
        // order is built: nothing more is placed (decision 0132 item 7, 0151 item 2).
        self.not_stopped(d, presses)?;
        // The caps asked again at the touch the order (X, or Y) is priced at: fbc-oms holds them
        // in lots fixed at the preflight's touch, which a touch risen since leaves above what the
        // USD caps admit (decision 0113). It is then not sent, and never resized.
        if !market::caps_admit(places, found, quote, d.caps, found.min_size) {
            return Err(Halt::Stop(Stop::BeyondCaps));
        }
        let submitted = {
            let mut link = d.link.borrow_mut();
            let cmd = link
                .reg()
                .place(new)
                .map_err(|e| format!("{order} was refused: {}", places::variant_name(&e)));
            let auth = cmd.and_then(|cmd| {
                let auth = link.reg().authorize(d.acct, cmd);
                auth.map_err(|e| {
                    format!("{order} was not authorized: {}", places::variant_name(&e))
                })
            });
            auth.and_then(|auth| link.submit(&d.orders, auth))
        };
        let rpc = submitted.map_err(Halt::Error)?;
        let now = Placed {
            cid,
            rpc,
            at: tokio::time::Instant::now(),
            wall_secs: u64::try_from(wall_secs()).unwrap_or(0),
        };
        *placed = Some(now);
        self.placed_run(d.which).submitted = true;
        // Stage three's session sent an order of ours, which the reads after it are held to.
        if d.which == Which::Pages {
            self.stage_run(Which::Pages).placed.submitted = true;
        }
        self.line(format_args!(
            "PLACE {order} on {}: one post-only GTC at the instrument's minimum size, --away-bps \
             behind the touch (no price, size or id is shown)",
            name(d.on)
        ));
        // Its ACK, or its deadline.
        let acked = self
            .waiting(d, presses, |link, _| {
                let state = link.registry().get(cid).map(|r| r.state());
                let rested =
                    state.is_some_and(|s| !matches!(s, OrdState::PendingNew | OrdState::Unknown));
                match (rested, link.outcome_of(rpc)) {
                    (true, outcome) => Some((true, outcome)),
                    (false, Some(outcome)) => Some((false, Some(outcome))),
                    (false, None) => None,
                }
            })
            .await?;
        // Its own ACK alone, by its client id, never another order's in the same session (stage
        // three places three; PR #284's review).
        let text = two::cid_text(cid).unwrap_or_default();
        let exact = self.tee.kept().exact_ack_of(d.heard_from, &text);
        // It is ours whatever client id its later frames carry (decision 0132 item 7).
        let vid = d
            .link
            .borrow()
            .registry()
            .get(cid)
            .and_then(|r| r.vid().cloned());
        if let Some(vid) = vid {
            self.tee.ours(vid.as_str());
        }
        // Acked: resting now, or ended after an `ACK` of ours (a fill at once, say); an order
        // that ended with no `ACK`, refused or cancelled as it came, never rested.
        let open = {
            let link = d.link.borrow();
            let state = link.registry().get(cid).map(|r| r.state());
            matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled))
        };
        let acked_ours = self.tee.kept().acked_of(d.heard_from, &text);
        let run = self.placed_run(d.which);
        run.acked = acked.0 && (open || acked_ours);
        run.outcome = acked.1.as_ref().map(places::outcome_name);
        run.exact_echo = exact;
        run.ack_after = run.acked.then(|| now.at.elapsed());
        // A test's drop session failing once its order was answered, that order resting.
        #[cfg(test)]
        if matches!(d.which, Which::One | Which::Two)
            && places.test.fail_after_place == Some(usize::from(d.which == Which::Two))
        {
            return Err(Halt::Error(format!(
                "a test's session failed after {order}'s answer"
            )));
        }
        Ok(now)
    }
    /// The streams of `d`'s session, its orders `ours` on `found`'s market, compared with
    /// never-arming reads of `on` after the step named `step`, made at `since` (now when `None`),
    /// at the run's cadence (0151 item 4, [`crate::compare`]): its socket's refusals reported
    /// under `d`'s session.
    async fn compare_step(
        &self,
        d: &DropCtx,
        (found, ours): (&Discovered, &[fbc_core::ClientOrderId]),
        on: Armed,
        step: &'static str,
        since: Option<tokio::time::Instant>,
    ) -> (crate::compare::Compared, Option<ReadSocket>) {
        let since = since.unwrap_or_else(tokio::time::Instant::now);
        let places = self.places.expect("a place-stage run");
        let label = name(on);
        let (cfg, _) = match configured(&self.base, self.lookup, on) {
            Ok(configured) => configured,
            Err(why) => return (unread(step, why), None),
        };
        let reader = account_selection(&cfg)
            .map_err(|e| e.to_string())
            .and_then(|selection| auth_qfex::reader(self.lookup, selection));
        let reader = match reader {
            Ok(reader) => reader,
            Err(why) => return (unread(step, why), None),
        };
        let io = crate::compare::Io {
            connector: &self.connector,
            limiter: &self.limiter,
            caps: &self.caps,
            ns: self.ns(),
            page_size: self.opts.page_size,
            connect_wait: self.opts.connect_wait,
            read_wait: self.opts.read_wait,
            symbol: found.spec.venue_symbol.as_wire(),
        };
        let mut nonces = self.nonces.clone();
        let origin = self.origin;
        let mut ctx = move || EncodeCtx {
            wall: wall_ns(),
            mono: ledger::mono_since(origin),
            nonces: nonces.reserve(ReadAuth::NONCES),
        };
        let mut socket = crate::compare::Socket::new(io, &cfg, &reader, &mut ctx);
        let streams = || streams_of(&d.link.borrow(), d.inst, found, ours);
        let cadence = crate::compare::Cadence::of(places);
        let compared = crate::compare::compare(step, &mut socket, streams, cadence, since).await;
        // Its limiter count is the refusals it held the shared budget for itself, so a session's
        // refusal met meanwhile is counted once, as the session's.
        let heard = socket.close().await;
        let socket = ReadSocket {
            before: d.which.name(),
            label: format!("{label} (the comparison after {step})"),
            refusals: heard,
        };
        self.line(format_args!(
            "COMPARE after {step}: {} reads, {}",
            compared.reads.len(),
            match compared.first_agreed {
                Some(after) =>
                    format!("the first agreeing {} ms after the step", after.as_millis()),
                None => "none agreed".to_owned(),
            }
        ));
        (compared, Some(socket))
    }

    /// Records `compared`, a comparison of session `which`, and its read socket's refusals.
    fn compared(
        &mut self,
        which: Which,
        (compared, socket): (crate::compare::Compared, Option<ReadSocket>),
    ) {
        self.ev.read_sockets.extend(socket);
        match which {
            Which::One => self.drop_run().compared.push(compared),
            Which::Two => self.two_run().compared.push(compared),
            Which::Cross | Which::Take | Which::Pages => {
                self.stage_run(which).compared.push(compared)
            }
        }
    }

    /// A stop's cancels in a drop session (0151 items 2 and 11; as `paradex_testnet_acceptance`
    /// stops): Kill, cancel everything of ours on the market by venue id, each an fbc-oms
    /// authorization through the `Link`, those awaiting their order's acknowledgement sent as
    /// each lands; then a wait, up to `--rpc-timeout-ms`, for every order they name to end,
    /// each of the owner's presses meanwhile counted, a second Ctrl-C included, none ending it
    /// early. The market is left armed and Killed: [`Run::end_session`] disarms it once a fill
    /// that reached the session meanwhile had its flatten ([`Run::flatten_late`]), which so
    /// needs no new market lease. A cancel refused locally, answered other than accepted,
    /// or met by a provider rate refusal holds them: the wait ends there ([`Cancels::held`]);
    /// refused locally or met by a rate refusal, it is one of 0151 item 11's causes too
    /// ([`Cancels::blocked`]). They are recorded in `c`, the caller's, as they go: what was sent
    /// and each press taken survive a connection's end that cuts them short and drops this
    /// future, and so does the stop the tee or the handler had raised as the first press came
    /// ([`Cancels::stop_at_press`]).
    async fn stop_cancels(&self, d: &DropCtx, presses: &mut Presses, c: &mut Cancels) {
        self.cancels_of_ours(d, presses, true, (c, None)).await;
    }

    /// The cancels of every order of ours on `d`'s market by venue id, as [`Run::stop_cancels`]
    /// sends them and recorded in `c` as they go: with `stop`, a Stop's, the market Killed first
    /// (its disarm and its line are [`Run::stop_disarm`]'s); without, a flatten's
    /// ([`Run::flatten`]: 0151 item 9), the market left armed in its state, so the owner's
    /// Flatten can put it in Exit after them, and its line saying so. A flatten's also record
    /// the stop at the owner's first press in its `at_press`, as [`Run::note_press`] does, a
    /// record that outlives the flatten's drop.
    async fn cancels_of_ours(
        &self,
        d: &DropCtx,
        presses: &mut Presses,
        stop: bool,
        (c, mut at_press): (&mut Cancels, Option<&mut Option<Option<Stop>>>),
    ) {
        let refusals_before = self.tee.kept().stops.len();
        let (cmds, mut waiting, refused) = {
            let mut link = d.link.borrow_mut();
            let caps = link.caps().clone();
            let reg = link.reg();
            if stop {
                reg.kill(d.inst);
            }
            match reg.cancel_everything(d.inst, &caps) {
                fbc_oms::CancelEverything::CancelAll {
                    command,
                    unanswered,
                } => {
                    let mut cmds = vec![command];
                    cmds.extend(unanswered.commands);
                    (cmds, unanswered.awaiting_ack, 0)
                }
                fbc_oms::CancelEverything::Explicit { plan, .. } => {
                    (plan.commands, plan.awaiting_ack, plan.refused.len())
                }
            }
        };
        if refused > 0 {
            c.refused_locally();
        }
        let mut rpcs = Vec::new();
        // A test's Stop cancels of a session outside the drop test refused locally (FBC-3xzn).
        #[cfg(test)]
        let refuse = stop
            && matches!(d.which, Which::Cross | Which::Take | Which::Pages)
            && self.places.is_some_and(|p| p.test.refuse_stop_cancels);
        let submit = |cmd, rpcs: &mut Vec<fbc_core::RpcId>| {
            #[cfg(test)]
            if refuse {
                return Err("a test's local refusal of a Stop cancel".to_owned());
            }
            let mut link = d.link.borrow_mut();
            let auth = link
                .reg()
                .authorize(d.acct, cmd)
                .map_err(|e| format!("{e:?}"));
            auth.and_then(|auth| link.submit(&d.orders, auth))
                .map(|rpc| rpcs.push(rpc))
        };
        for cmd in cmds {
            if submit(cmd, &mut rpcs).is_err() {
                c.refused_locally();
            }
        }
        let until = tokio::time::Instant::now() + Duration::from_millis(self.opts.rpc_timeout_ms);
        // A provider rate refusal the tee raised since the cancels began: one met on the
        // session's connection, which wakes no wait here, so it is read again as the wait ends.
        let rate = || {
            let kept = self.tee.kept();
            let since = kept.stops.get(refusals_before..).unwrap_or_default();
            since.iter().any(|s| matches!(s, Stop::RateRefused { .. }))
        };
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let due: Vec<fbc_core::ClientOrderId> = {
                let mut link = d.link.borrow_mut();
                let caps = link.caps().clone();
                let due = link.reg().cancels_due(&caps);
                due.into_iter()
                    .filter(|cid| waiting.contains(cid))
                    .collect()
            };
            for cid in due {
                waiting.retain(|w| *w != cid);
                let built = {
                    let mut link = d.link.borrow_mut();
                    let caps = link.caps().clone();
                    link.reg()
                        .cancellable(cid)
                        .map(|permit| permit.cancel(&caps))
                };
                match built {
                    Ok(fbc_oms::CancelChoice::Send(cmd)) => {
                        if submit(cmd, &mut rpcs).is_err() {
                            c.refused_locally();
                        }
                    }
                    Ok(fbc_oms::CancelChoice::AwaitAck) => waiting.push(cid),
                    Err(_) => c.refused_locally(),
                }
            }
            // Every cancel handed to the session so far, before any wait.
            c.sent = rpcs.len();
            let (named, answered) = {
                let link = d.link.borrow();
                let named: Vec<_> = rpcs.iter().flat_map(|r| link.cids_of(*r)).collect();
                let answered: Vec<_> = rpcs.iter().filter_map(|r| link.outcome_of(*r)).collect();
                let ended = named
                    .iter()
                    .all(|cid| (link.registry().get(*cid)).is_none_or(|r| r.state().is_terminal()));
                (ended && waiting.is_empty(), answered)
            };
            c.answered(&answered, rate());
            if named || c.held {
                c.ended = named;
                break;
            }
            tokio::select! {
                biased;
                () = presses.pressed() => {
                    // The stop that came before the owner's first press, if one did: the
                    // session's stop then, not the press (decision 0148 item 8).
                    if c.presses == 0 {
                        c.stop_at_press = self.stop_now(d);
                    }
                    if let Some(at_press) = at_press.as_deref_mut() {
                        self.note_press(d, at_press);
                    }
                    c.presses += 1;
                }
                () = woken => {}
                () = tokio::time::sleep_until(until) => {
                    // The wait's end: a rate refusal met by then holds them too (0151 item 11).
                    c.answered(&[], rate());
                    break;
                }
            }
        }
        if !stop {
            self.line(format_args!(
                "FLATTEN cancels: {} cancels by venue id of every other order of ours (0151 item \
                 9), {}",
                c.sent,
                cancels_said(c)
            ));
        }
    }

    /// The end of a Stop's cancels, `c`, in session `d`: the market disarmed, its market lease
    /// released, and their line. Made once a fill of ours that reached the session during them
    /// had its flatten ([`Run::flatten_late`]), which so armed nothing anew.
    fn stop_disarm(&self, d: &DropCtx, c: &Cancels) {
        d.link.borrow_mut().reg().disarm(d.inst);
        let why = match d.which {
            Which::Cross | Which::Take | Which::Pages => "decision 0132 item 7, 0148 item 8",
            _ => "0151 item 2",
        };
        self.line(format_args!(
            "STOP cancels: Kill, then {} cancels by venue id ({why}), {}; the market disarmed",
            c.sent,
            cancels_said(c)
        ));
        if c.presses > 0 {
            self.line(format_args!(
                "STOP cancels: {} press{} of Ctrl-C while they ran, recorded",
                c.presses,
                if c.presses == 1 { "" } else { "es" }
            ));
        }
    }

    /// The latest read of each named account made before a session named `before` (its
    /// [`Which::name`]), in the order the configuration names them, each with the one order it
    /// admitted, if any, taken off its counts: that order is ours and a registry of the run holds
    /// it, which [`crate::stage_two::no_other_order`] judges by (FBC-77r8's X, before drop
    /// session two). An account with no such read is left out, so it keeps a flatten IOC back.
    fn latest_reads(&self, before: &'static str) -> Vec<Option<AccountRead>> {
        let of = |account: Armed| {
            let label = name(account);
            let theirs = |r: &&AccountRead| {
                r.before == before
                    && (r.label == label || r.label.starts_with(&format!("{label} (")))
            };
            let read = self.ev.reads.iter().rev().find(theirs).cloned();
            read.map(|r| AccountRead {
                page_orders: r.page_orders.saturating_sub(r.page_allowed),
                rest_open_orders: (r.rest_open_orders)
                    .saturating_sub(u64::try_from(r.page_allowed).unwrap_or(u64::MAX)),
                page_allowed: 0,
                ..r
            })
        };
        self.named.iter().map(|a| of(*a)).collect()
    }

    /// The flatten on session `d` while it is open ([`Run::flatten`]), on the latest read of each
    /// named account made before the session named `before` ([`Run::latest_reads`]), its run,
    /// `running`, polled meanwhile, the session's first stop `stop`. Gives the session's stop, the
    /// flatten's own in place of the fill it answered ([`crate::flatten::session_stop`]), and
    /// whether the run completed first (its connection ended), which drops the flatten where it
    /// was and says so in its record. A press of the owner's Ctrl-C the flatten took, which ended
    /// nothing of it, is the session's stop when neither gives one ([`Stop::Interrupted`],
    /// decision 0148 item 8), so that a session whose flatten ended flat, the taker IOC's, still
    /// stops the run and no later session starts; a stop the tee or the handler raised during the
    /// flatten before its first press came first, and is the session's in its place
    /// ([`Run::note_press`]). Both hold when the connection's end drops the flatten, whose record
    /// then counts every press it took.
    #[allow(clippy::too_many_arguments)]
    async fn flatten_open<F>(
        &mut self,
        places: &Places,
        view: (&Discovered, &market::TouchView),
        d: &DropCtx,
        running: &mut Pin<&mut F>,
        (allowance, presses): (&mut Allowance, &mut Presses),
        (ours, before): (&[fbc_core::ClientOrderId], &'static str),
        stop: Option<Stop>,
    ) -> (Option<Stop>, bool)
    where
        F: Future<Output = Result<(), ExecSessionError>>,
    {
        let reads = self.latest_reads(before);
        let taken = presses.taken();
        // The stop at the flatten's first press, kept here, where the flatten's drop leaves it.
        let mut at_press = None;
        let budget = (&mut *presses, &mut at_press);
        let flatten = self.flatten(places, view, d, allowance, budget, ours, &reads);
        let flattened = tokio::select! {
            biased;
            _ = &mut *running => None,
            own = flatten => Some(own),
        };
        let took = presses.taken().saturating_sub(taken);
        let pressed = (took > 0).then(|| at_press.flatten().unwrap_or(Stop::Interrupted));
        match flattened {
            Some(own) => (crate::flatten::session_stop(stop, own).or(pressed), false),
            None => {
                let ended = crate::flatten::CONNECTION_ENDED.to_owned();
                let run = self.flatten_run(d.which);
                run.error = Some(ended);
                // The presses its wait took as the connection's end dropped it, with the rest.
                run.presses = took;
                (stop.or(pressed), true)
            }
        }
    }

    /// The flatten of a fill of ours that reached session `d` only after its steps ended, during
    /// its Stop's cancels or after them, the session still open (0151 item 9), as
    /// [`Run::flatten_open`] runs it, given the session's stop, whether its Stop's cancels ran
    /// (`killed`) and what it answers (`after`, [`Run::late_fill`]): those Killed the market, so
    /// the kill switch is lifted first, the market Cancel-only (decision 0012), for the owner's
    /// Flatten to put it in Exit, under the market lease the registry still holds while the market
    /// is armed, or under the lease taken again once [`Run::end_session`] disarmed it, and once
    /// the flatten ended the market is Killed again, as the Stop left it, for
    /// [`Run::end_session`] to disarm.
    #[allow(clippy::too_many_arguments)]
    async fn flatten_late<F>(
        &mut self,
        places: &Places,
        view: (&Discovered, &market::TouchView),
        d: &DropCtx,
        session: &mut Pin<&mut F>,
        budget: (&mut Allowance, &mut Presses),
        ours: (&[fbc_core::ClientOrderId], &'static str),
        (stop, killed, after): (Option<Stop>, bool, &'static str),
    ) -> (Option<Stop>, bool)
    where
        F: Future<Output = Result<(), ExecSessionError>>,
    {
        self.flatten_run(d.which).after = after;
        if killed {
            d.link.borrow_mut().reg().lift_kill(d.inst);
            self.line(format_args!(
                "FLATTEN on {}: the Stop's Kill lifted, the market Cancel-only (decision 0012), \
                 for a fill of ours that reached the session during its Stop's cancels or after \
                 them (0151 item 9)",
                name(d.on)
            ));
        }
        let flattened = self
            .flatten_open(places, view, d, session, budget, ours, stop)
            .await;
        if killed {
            d.link.borrow_mut().reg().kill(d.inst);
            self.line(format_args!(
                "FLATTEN on {}: the market Killed again, as the Stop left it",
                name(d.on)
            ));
        }
        flattened
    }

    /// The flatten (FBC-ou9g; decisions 0012, 0063, 0148 items 5, 6 and 9, 0151 items 5 and 9;
    /// [`crate::flatten`]) on session `d`, once a fill of ours other than an expected one reached
    /// it, while it is open: every other order of ours on the market, `ours` among them, cancelled
    /// by venue id and seen to end ([`Run::cancels_of_ours`]), else `Flatten(NotEnded)`; when the
    /// registry's position is not flat, the owner's Flatten into Exit
    /// ([`places::flatten`]); then, while the account's flatten budget in `allowance` lasts
    /// ([`crate::flatten::next`]): its client id minted, the one await before it; no other order
    /// of ours on any registry of the run or in any named account's read of `reads`
    /// ([`Run::no_other_order`]); a touch read then; one reducing limit IOC at the instrument's
    /// minimum size priced within `--flatten-cap-bps` of it ([`crate::flatten::ioc_price`]), spent
    /// from the budget and placed, an fbc-oms authorization through the registry's one pre-trade
    /// path, with nothing awaited from the check to the place; its fills expected; its end with
    /// its fills settled or its deadline, an `Unknown` one never resent; the streams compared
    /// with a never-arming read after it; the position read again from the registry. Then, once
    /// flat, a comparison after its last step, which shows REST positions flat by agreeing with
    /// the registry. The owner's presses meanwhile are counted, none ending it; its session's
    /// span bound does not end it. `reads` are the named accounts' reads from before the session
    /// ([`Run::latest_reads`]), each clean for the session to start, so on them the check
    /// restates those reads: no named account is read again before a flatten IOC. What is
    /// fresher is the registries, every one of the run, and the session's own account, whose
    /// streams each comparison holds to a never-arming read made after the step, a disagreement
    /// stopping the flatten before any IOC follows (decision 0148 items 6 and 8).
    /// Gives its own stop, if it met one: a [`Stop::Flatten`], the stop
    /// [`crate::stage_two::no_other_order`] keeps an IOC back with, or a comparison's.
    #[allow(clippy::too_many_arguments)]
    async fn flatten(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        (presses, at_press): (&mut Presses, &mut Option<Option<Stop>>),
        ours: &[fbc_core::ClientOrderId],
        reads: &[Option<AccountRead>],
    ) -> Option<Stop> {
        let which = d.which;
        self.flatten_run(which);
        self.line(format_args!(
            "FLATTEN on {} ({}): a fill of ours other than an expected one, while the session is \
             open (0151 item 9)",
            name(d.on),
            which.name()
        ));
        let mut ours = ours.to_vec();
        // Every other order of ours on the market, each seen to end (decision 0148 item 6).
        let mut cancels = Cancels::default();
        let flattens = (&mut cancels, Some(&mut *at_press));
        self.cancels_of_ours(d, presses, false, flattens).await;
        let ended = cancels.ended && !cancels.held;
        let (sent, pressed) = (cancels.sent, cancels.presses);
        let run = self.flatten_run(which);
        run.cancels = Some(cancels);
        run.presses += pressed;
        if !ended {
            run.error = Some(crate::flatten::NOT_ENDED.to_owned());
            return Some(self.flatten_stopped(which, Stop::Flatten(Flattening::NotEnded)));
        }
        // The last comparison since the last cancel or IOC, when one was made.
        let mut last = None;
        if sent > 0 {
            let compared = self
                .flatten_compare(d, found, &ours, "the flatten's cancels")
                .await;
            if let Some(stop) = compared.stop {
                return Some(self.flatten_stopped(which, stop));
            }
            last = Some(compared.ended);
        }
        let position = |link: &Link| link.registry().inventory(d.inst);
        #[cfg(test)]
        self.test_kill(places, d, crate::probe_args::KillAt::Flatten);
        if position(&d.link.borrow()).0 != 0 {
            let dir = self.leases.as_ref().map(|l| l.dir.clone());
            let dir = dir.expect("a place-stage run's lease directory");
            let pressed = places::flatten(d.link.borrow_mut().reg(), &dir, d.inst);
            self.flatten_run(which).exit = Some(pressed.is_ok());
            if pressed.is_err() {
                self.line(format_args!("FLATTEN refused on {}", name(d.on)));
                return Some(self.flatten_stopped(which, Stop::Flatten(Flattening::Refused)));
            }
            self.line(format_args!(
                "FLATTEN on {}: the owner's Flatten, the market in Exit (decision 0148 item 5)",
                name(d.on)
            ));
            // A fill heard before the session's place gate opened (its arm accepted and its
            // resync's end pushed, decision 0058), as one QFEX sends just after the arm's ack
            // can be once the session asked for its resync's pages: a place before then is
            // NotSent(Disconnected), so the flatten's first IOC waits for the gate.
            let due = tokio::time::Instant::now() + Duration::from_millis(self.opts.rpc_timeout_ms);
            let pressed = self.place_gate(d, (presses, at_press), due).await;
            self.flatten_run(which).presses += pressed;
        }
        let step = which.step();
        loop {
            let at = position(&d.link.borrow());
            let left = allowance.left(step, ledger::FLATTEN);
            let side = match crate::flatten::next(at, found.min_size, left) {
                Next::Flat => break,
                Next::Stop(why) => return Some(self.flatten_stopped(which, Stop::Flatten(why))),
                Next::Ioc(side) => side,
            };
            // The client id first, the one await before the IOC: its high-water mark's write may
            // wait, so the reads, the touch, the price and the budget are judged after it, with
            // nothing awaited between them and the place (decision 0148 items 5 and 6). A mint
            // refused spends nothing of the budget.
            let leases = self.leases.as_mut().expect("a place-stage run's leases");
            let minted = leases.mint.mint(1).await;
            #[cfg(test)]
            let minted = match places.test.refuse_mint_in_flatten {
                true => Err("a test's refusal of the flatten's client id".to_owned()),
                false => minted,
            };
            let cid = match minted {
                Ok(cids) => cids[0],
                Err(why) => {
                    self.flatten_run(which).error = Some(why);
                    let refused = Stop::Flatten(Flattening::Refused);
                    return Some(self.flatten_stopped(which, refused));
                }
            };
            let reads: Vec<AccountRead> = reads.iter().flatten().cloned().collect();
            let reads = match reads.len() == self.named.len() {
                true => reads,
                false => Vec::new(),
            };
            #[cfg(test)]
            let reads = test_listed(places, reads);
            if let Err(blocked) = self.no_other_order(d.inst, &reads) {
                let said = blocked.says();
                self.line(format_args!(
                    "BLOCKED a flatten IOC on {}: {said} (decision 0148 item 6)",
                    name(d.on)
                ));
                self.flatten_run(which).blocked = Some(said);
                return Some(self.flatten_stopped(which, blocked.stop()));
            }
            let quote = match touch.now(places.touch_max_age) {
                Ok(quote) => quote,
                Err(why) => {
                    self.flatten_run(which).error = Some(why);
                    let no_touch = Stop::Flatten(Flattening::NoTouch);
                    return Some(self.flatten_stopped(which, no_touch));
                }
            };
            let side_name = match side {
                fbc_core::Side::Buy => "buy",
                fbc_core::Side::Sell => "sell",
            };
            let cap = places.flatten_cap_bps;
            let px = match crate::flatten::ioc_price(&found.spec, side, quote, cap) {
                Ok(px) => px,
                Err(why) => {
                    let unsent = IocRun {
                        side: side_name,
                        ..IocRun::default()
                    };
                    self.flatten_run(which).iocs.push(unsent);
                    self.line(format_args!(
                        "FLATTEN capped on {}: no price on the grid within --flatten-cap-bps \
                         through the touch it crosses, so no IOC is sent",
                        name(d.on)
                    ));
                    return Some(self.flatten_stopped(which, Stop::Flatten(why)));
                }
            };
            // The caps asked again at the touch the IOC is priced from: fbc-oms holds them in
            // lots fixed at the preflight's touch and applies them to reducing orders too (0013
            // rule 2), so an IOC the USD caps would not admit there is not sent, and never
            // resized (decision 0113). Nothing of the budget is spent on it.
            if !market::caps_admit(places, found, quote, d.caps, found.min_size) {
                let unsent = IocRun {
                    side: side_name,
                    ..IocRun::default()
                };
                self.flatten_run(which).iocs.push(unsent);
                self.line(format_args!(
                    "FLATTEN beyond the caps on {}: --resting-cap-usd or --inventory-cap-usd \
                     would not admit the IOC at the touch it is priced from, so it is not sent \
                     (decision 0113)",
                    name(d.on)
                ));
                let beyond = Stop::Flatten(Flattening::BeyondCaps);
                return Some(self.flatten_stopped(which, beyond));
            }
            allowance
                .take(step, ledger::FLATTEN)
                .expect("a flatten IOC the budget has left");
            d.watched.borrow_mut().expected.push(cid);
            ours.push(cid);
            #[cfg(test)]
            self.test_kill(places, d, crate::probe_args::KillAt::Ioc);
            let new = fbc_core::NewOrder {
                cid,
                inst: d.inst,
                side,
                qty: found.min_size,
                kind: fbc_core::OrderKind::Limit { px },
                tif: fbc_core::Tif::Ioc,
                channel: fbc_core::Channel::Public,
                post_only: false,
                reduce_only: false,
                reducing: true,
            };
            let (heard_from, at_send) = (self.tee.kept().heard.len(), tokio::time::Instant::now());
            let submitted = {
                let mut link = d.link.borrow_mut();
                let cmd = link.reg().place(new).map_err(|e| places::variant_name(&e));
                let auth = cmd.and_then(|cmd| {
                    let auth = link.reg().authorize(d.acct, cmd);
                    auth.map_err(|e| places::variant_name(&e))
                });
                auth.and_then(|auth| {
                    let submitted = link.submit(&d.orders, auth);
                    submitted.map_err(|e| places::refusal_named(&e))
                })
            };
            let rpc = match submitted {
                Ok(rpc) => rpc,
                Err(why) => {
                    self.flatten_run(which).iocs.push(IocRun {
                        side: side_name,
                        refused: Some(why),
                        ..IocRun::default()
                    });
                    self.line(format_args!("FLATTEN IOC refused on {}", name(d.on)));
                    return Some(self.flatten_stopped(which, Stop::Flatten(Flattening::Refused)));
                }
            };
            // Recorded as sent before its wait, so a connection that ends during the wait, which
            // drops the flatten there, leaves it recorded as sent ([`crate::flatten::IN_FLIGHT`]).
            self.flatten_run(which).iocs.push(IocRun {
                side: side_name,
                sent: true,
                came_back: None,
                refused: None,
            });
            self.line(format_args!(
                "FLATTEN IOC on {}: one reducing limit IOC at the instrument's minimum size, at \
                 most --flatten-cap-bps through the touch it crosses (no price, size or id is \
                 shown)",
                name(d.on)
            ));
            let due = at_send + Duration::from_millis(self.opts.rpc_timeout_ms);
            let pressed = self.ioc_settled(d, (presses, at_press), cid, due).await;
            let came_back = {
                let link = d.link.borrow();
                let record = link.registry().get(cid);
                let state = record.map(|r| r.state());
                let counts =
                    record.map_or((Lots::ZERO, Lots::ZERO), |r| (r.cum_fills(), r.cum_venue()));
                let not_sent = matches!(link.outcome_of(rpc), Some(SubmitOutcome::NotSent(_)));
                // An err echoing this IOC's own add_order alone, never an earlier IOC's that
                // came late (PR #284's review).
                let text = two::cid_text(cid).unwrap_or_default();
                let echoed = self.tee.kept().err_echo_of(heard_from, &text);
                let vid = record.and_then(|r| r.vid().cloned());
                if let Some(vid) = vid {
                    self.tee.ours(vid.as_str());
                }
                Ioc::of(
                    state,
                    (counts.0, counts.1, found.min_size),
                    echoed,
                    not_sent,
                )
            };
            let run = self.flatten_run(which);
            run.presses += pressed;
            if let Some(ioc) = run.iocs.last_mut() {
                ioc.came_back = Some(came_back);
            }
            self.line(format_args!(
                "FLATTEN IOC on {}: {}",
                name(d.on),
                came_back.says()
            ));
            if matches!(came_back, Ioc::Refused | Ioc::NotSent) {
                return Some(self.flatten_stopped(which, Stop::Flatten(Flattening::Refused)));
            }
            // Fills its order events state that had not all reached the registry: its position
            // is not the venue's, so no further flatten IOC is judged by it.
            if came_back == Ioc::Unsettled {
                return Some(self.flatten_stopped(which, Stop::Flatten(Flattening::Unsettled)));
            }
            let compared = self.flatten_compare(d, found, &ours, "a flatten IOC").await;
            if let Some(stop) = compared.stop {
                return Some(self.flatten_stopped(which, stop));
            }
            last = Some(compared.ended);
        }
        // Flat in the registry: REST positions flat when the read after its last step agreed.
        let run = self.flatten_run(which);
        run.flat = true;
        run.rest_flat = last == Some(crate::compare::Ended::Agreed);
        let said = run.ended();
        self.line(format_args!("FLATTENED on {}: {said}", name(d.on)));
        None
    }

    /// A test's Kill of `d`'s market at `at` in the flatten ([`crate::probe_args::KillAt`]), so
    /// fbc-oms refuses what the flatten builds there: a path no answer of the QFEX model reaches.
    #[cfg(test)]
    fn test_kill(&self, places: &Places, d: &DropCtx, at: crate::probe_args::KillAt) {
        if places.test.kill_in_flatten == Some(at) {
            d.link.borrow_mut().reg().kill(d.inst);
        }
    }

    /// Records `stop` as the flatten of session `which`'s own, and gives it back.
    fn flatten_stopped(&mut self, which: Which, stop: Stop) -> Stop {
        let run = self.flatten_run(which);
        run.stop = Some(stop.name());
        run.flat = false;
        stop
    }

    /// The streams of `d`'s session, its orders `ours` on `found`'s market, compared with
    /// never-arming reads after the flatten's step named `step` ([`Run::compare_step`]): kept in
    /// the flatten's record, its read socket's refusals under the session's.
    async fn flatten_compare(
        &mut self,
        d: &DropCtx,
        found: &Discovered,
        ours: &[fbc_core::ClientOrderId],
        step: &'static str,
    ) -> crate::compare::Compared {
        let (compared, socket) = self.compare_step(d, (found, ours), d.on, step, None).await;
        self.ev.read_sockets.extend(socket);
        self.flatten_run(d.which).compared.push(compared.clone());
        compared
    }

    /// Records in `at_press`, at a flatten's first press of the owner's Ctrl-C, the stop the tee
    /// or the handler had raised by then on `d`'s session, if any ([`Run::stop_now`]): that stop
    /// came before the press, so it is the session's in place of [`Stop::Interrupted`] (decision
    /// 0148 item 8), as [`Cancels::stop_at_press`] is for a Stop's cancels. A later press changes
    /// nothing.
    fn note_press(&self, d: &DropCtx, at_press: &mut Option<Option<Stop>>) {
        at_press.get_or_insert_with(|| self.stop_now(d));
    }

    /// Waits until `d`'s session takes places on its market (`ExecOrders::may_place_on`: its
    /// stream authenticated, its arm accepted and its resync's end pushed, decision 0058), or
    /// `due`: the owner's presses meanwhile counted, none ending it, and no stop raised meanwhile
    /// ending it, since the flatten answers a stop already raised. Its line, [`PLACE_GATE`], is
    /// printed when the gate is shut as the wait begins. How many presses it took; the stop at
    /// the flatten's first press is recorded in `at_press` ([`Run::note_press`]).
    async fn place_gate(
        &self,
        d: &DropCtx,
        (presses, at_press): (&mut Presses, &mut Option<Option<Stop>>),
        due: tokio::time::Instant,
    ) -> u32 {
        let mut pressed = 0;
        if !d.orders.may_place_on(d.inst) {
            self.line(format_args!("FLATTEN on {}: {PLACE_GATE}", name(d.on)));
        }
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if d.orders.may_place_on(d.inst) {
                return pressed;
            }
            tokio::select! {
                biased;
                () = presses.pressed() => {
                    self.note_press(d, at_press);
                    pressed += 1;
                }
                () = tokio::time::sleep_until(due) => return pressed,
                () = woken => {}
            }
        }
    }

    /// Waits until the flatten IOC `cid` on `d`'s session ended with its fills settled
    /// ([`crate::flatten::Ioc::settled`]), or `due`, its deadline: the owner's presses meanwhile
    /// counted, none ending it, and no stop raised meanwhile ending it, since the flatten answers
    /// a stop already raised. How many presses it took; the stop at the flatten's first press is
    /// recorded in `at_press` ([`Run::note_press`]).
    async fn ioc_settled(
        &self,
        d: &DropCtx,
        (presses, at_press): (&mut Presses, &mut Option<Option<Stop>>),
        cid: fbc_core::ClientOrderId,
        due: tokio::time::Instant,
    ) -> u32 {
        let mut pressed = 0;
        loop {
            let woken = d.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let settled = {
                let link = d.link.borrow();
                let record = link.registry().get(cid);
                let state = record.map(|r| r.state());
                let (fills, venue) =
                    record.map_or((Lots::ZERO, Lots::ZERO), |r| (r.cum_fills(), r.cum_venue()));
                Ioc::settled(state, fills, venue)
            };
            if settled {
                return pressed;
            }
            tokio::select! {
                biased;
                () = presses.pressed() => {
                    self.note_press(d, at_press);
                    pressed += 1;
                }
                () = tokio::time::sleep_until(due) => return pressed,
                () = woken => {}
            }
        }
    }

    /// The read of every named account after drop session `which` on `on` (decision 0132 item
    /// 7), each whatever another showed. `ours` are the drop test's orders placed so far, X
    /// first: X is admitted on its account, so the read says whether it rests, and an order on
    /// any account's page that is none of `ours`, or a TWAP, is what the streams did not show
    /// ([`AfterReads::unexpected`], decision 0148 item 8). The owner's presses meanwhile are
    /// taken, none ending the reads: the run stops after them. A provider rate refusal they
    /// meet, a `GET /user/positions` answered 429 or 418, any rise in the shared limiter's
    /// `rejected.user` count, or one their read sockets' frames show, in clear or kept whole
    /// ([`Refusals::shown`]), is a stop too, once they all ran. A read of `on`'s account that
    /// failed with no such refusal is named by the step that failed ([`AfterReads::on_unread`],
    /// FBC-gisx).
    async fn read_after(&mut self, which: Which, on: Armed, ours: &[&AllowedOrder]) -> AfterReads {
        let mut after = AfterReads::default();
        let mark = self.read_mark();
        let mut presses = std::mem::take(&mut self.presses);
        let mut pressed = false;
        let x = ours.first().copied();
        for account in self.named.clone() {
            let at = self.read_mark();
            let mut failed = places::AccountUnread::Positions;
            let read = {
                let admits = (x, ours);
                let read = self.read_one(
                    which.after(),
                    on,
                    which.mark(),
                    account,
                    admits,
                    &mut failed,
                );
                tokio::pin!(read);
                loop {
                    tokio::select! {
                        biased;
                        read = &mut read => break read,
                        () = presses.pressed() => pressed = true,
                    }
                }
            };
            match read {
                Ok(read) => {
                    // X's account is read when its page listed X or was the last: a full page
                    // not listing X leaves X unread, not absent. With no X to find, after drop
                    // session two or a crossing session, the account is read when its read
                    // ended, and every order its page lists counts as resting.
                    let found = x.is_none() || read.page_lists_it || !read.page_full;
                    after.read |= account == on;
                    after.x_read |= account == on && found;
                    after.x_flat |= account == on && read.flat;
                    after.not_flat |= !read.flat;
                    after.resting |= read.page_orders > 0 || read.rest_open_orders > 0;
                    // An order of ours: one the page names among `ours`, or one REST counts and
                    // the page does not list, which is not told from ours.
                    let rest = usize::try_from(read.rest_open_orders).unwrap_or(usize::MAX);
                    after.ours_resting |=
                        read.page_orders > read.page_unknown || rest > read.page_orders;
                    // An order none of ours, or a TWAP: what the streams did not show (decision
                    // 0148 item 8), as compare::judge counts one. An open order REST alone counts
                    // (a stop order) or one on a later page of a full page is not told from ours
                    // ([`AccountRead::page_unknown`]): it shows an order resting, marked.
                    after.unexpected |= read.page_unknown > 0 || read.page_twaps > 0;
                    // X by its venue id once known, whatever client id its row carries.
                    after.x_listed |= read.page_lists_it;
                }
                Err(why) => {
                    after.unread = true;
                    self.ev.errors.push(why);
                    if account == on && self.refused_since(at).is_none() {
                        after.on_unread = Some(failed);
                    }
                }
            }
        }
        // A press made as the last read ended, which the read's own end outran.
        pressed |= presses.try_pressed();
        self.presses = presses;
        after.pressed = pressed;
        after.refused = self.refused_since(mark);
        after
    }

    /// The reads after stage two's session `which` on `on`, a crossing or a taker IOC session,
    /// that ended by the abrupt drop (decision 0151 item 11): [`Run::read_after`] of every named
    /// account, again at the comparisons' cadence
    /// (one at once, then each one interval after the one before began, at most
    /// `cadence.reads()`, none due at or past the end of `--agree-wait-ms`; 0151 item 4), until
    /// one lists no order of ours, or the round they judge ends them ([`AfterReads::read_again`]:
    /// it lists an order or a TWAP not ours, shows a position not flat, could not read a named
    /// account, met a provider rate refusal or took the owner's Ctrl-C), or the wait ends. QFEX
    /// cancels on a drop as it learns of it, which a read made at once can precede, as the reads
    /// after drop session two allow ([`crate::verdict::until_absent`]). A round finished past the
    /// wait, or one that could not read a named account, is not judged and ends them: the round
    /// before it is, with what it showed folded in ([`AfterReads::fold`]), so no later round
    /// erases what an earlier one showed (FBC-77r8's review). `ours` are the session's order, once
    /// placed, as [`Run::read_after`] takes them.
    async fn read_after_stage_drop(
        &mut self,
        which: Which,
        on: Armed,
        ours: &[&AllowedOrder],
    ) -> AfterReads {
        let places = self.places.expect("a place-stage run");
        let cadence = crate::compare::Cadence::of(places);
        let start = tokio::time::Instant::now();
        let end = start + cadence.wait;
        let mut after = self.read_after(which, on, ours).await;
        let (mut began, mut rounds) = (start, 1);
        while rounds < cadence.reads() && after.read_again() {
            let due = began + cadence.interval;
            if due >= end {
                break;
            }
            let mut presses = std::mem::take(&mut self.presses);
            let slept = unless_pressed(&mut presses, tokio::time::sleep_until(due)).await;
            self.presses = presses;
            if slept.is_none() {
                after.pressed = true;
                break;
            }
            began = tokio::time::Instant::now();
            let next = self.read_after(which, on, ours).await;
            rounds += 1;
            // A round finished past the wait, or one that could not read a named account, is not
            // judged: the round before it is, with what it showed folded in.
            let judged = tokio::time::Instant::now() <= end && !next.unread;
            after.fold(next, judged);
            if !judged {
                break;
            }
        }
        self.line(format_args!(
            "READ after {}'s drop: {}: {rounds} of at most {} rounds of reads of every named \
             account, the last judged listing {}",
            which.name(),
            name(on),
            cadence.reads(),
            match (after.ours_resting, after.unexpected) {
                (_, true) => "an order or a TWAP not ours",
                (true, false) => "an order of ours",
                (false, false) => "no order of ours",
            }
        ));
        after
    }

    /// Where the account reads stand, for [`Run::refused_since`].
    fn read_mark(&self) -> ReadMark {
        ReadMark {
            rejected: self.rejected(),
            rest: self.rest_refused,
            sockets: self.ev.read_sockets.len(),
        }
    }

    /// The provider rate refusal the account reads met since `mark`, if any, a stop (decision
    /// 0148 item 8): a `GET /user/positions` answered 429 or 418, then any rise in the shared
    /// limiter's `rejected.user` count, or one their read sockets' frames show, in clear or kept
    /// whole ([`Refusals::shown`]).
    fn refused_since(&self, mark: ReadMark) -> Option<Stop> {
        let sockets = &self.ev.read_sockets[mark.sockets..];
        let shown = sockets.iter().any(|r| r.refusals.shown() > 0);
        let socket = self.rejected() > mark.rejected || shown;
        match (self.rest_refused > mark.rest, socket) {
            (true, _) => Some(Stop::RateRefused {
                on: places::RefusedOn::Rest,
            }),
            (false, true) => Some(Stop::RateRefused {
                on: places::RefusedOn::ReadSocket,
            }),
            (false, false) => None,
        }
    }

    /// `reads`, the account reads made since `mark`, before `when`: a provider rate refusal they
    /// met is the run's stop, marked (decision 0148 item 8, 0151 item 6), whether or not they
    /// failed on it, the failure kept among the errors; otherwise their own result.
    fn unless_refused(
        &mut self,
        mark: ReadMark,
        reads: Result<(), String>,
        when: &str,
    ) -> Result<(), String> {
        let Some(stop) = self.refused_since(mark) else {
            return reads;
        };
        if let Err(why) = reads {
            self.ev.errors.push(why);
        }
        Err(self.halt(stop, when))
    }

    /// The read before drop session two (FBC-77r8; decision 0132 item 7, 0144 item 4): every
    /// named account read as before a session ([`Run::check_accounts_allowing`]) under
    /// [`places::DROP_TWO`], admitting `x`, X resting from drop session one, alone on its account
    /// ([`AccountRead::refusal`]: X listed, and REST counting one open order on its symbol and
    /// none on another). The reads wait out a hold the shared budget is under, as the reads after
    /// a session do, and the owner's presses meanwhile are taken, none ending them. X rests
    /// whatever they show, so each way they end the run is a stop, marked: a provider rate
    /// refusal they met ([`Stop::RateRefused`]; one their sockets' frames show, kept whole
    /// included, [`Refusals::shown`]), then the owner's Ctrl-C ([`Stop::Interrupted`]),
    /// then a read that is not clean, an open order but X, a TWAP or a position the streams never
    /// showed ([`Stop::ReadDisagrees`]), then an account that could not be read, X left resting
    /// unjudged ([`Stop::LeftResting`]).
    async fn read_before_two(&mut self, x: &AllowedOrder) -> Result<(), String> {
        let (mark, made) = (self.read_mark(), self.ev.reads.len());
        let mut presses = std::mem::take(&mut self.presses);
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        let mut pressed = false;
        let read = {
            let some = Some(x);
            let read = self.check_accounts_allowing(places::DROP_TWO, x.account, " (drop)", some);
            tokio::pin!(read);
            loop {
                tokio::select! {
                    biased;
                    read = &mut read => break read,
                    () = presses.pressed() => pressed = true,
                }
            }
        };
        pressed |= presses.try_pressed();
        self.patient = None;
        self.presses = presses;
        // A refusal their read sockets' frames show, in clear or kept whole, is one too, though
        // the adapter reads no hold from a frame kept whole ([`Refusals::shown`]).
        let refused = self.refused_since(mark);
        let unclean = (self.ev.reads[made..].last()).is_some_and(|r| r.refusal().is_some());
        let stop = match (refused, pressed, &read) {
            (Some(stop), ..) => stop,
            (None, true, _) => Stop::Interrupted,
            (None, false, Ok(())) => return Ok(()),
            (None, false, Err(_)) if unclean => Stop::ReadDisagrees,
            (None, false, Err(_)) => Stop::LeftResting,
        };
        let why = read.err().map_or(String::new(), |why| format!("{why}; "));
        // An account not read, X not listed by the reads after drop session one: counted
        // resting, never seen resting (the owner's ruling under 0151 item 6 on FBC-gisx; its
        // review). X listed by them was seen resting after a session.
        let seen = self.drop_run().x_resting;
        let says = (stop == Stop::LeftResting && !seen).then_some(places::COUNTED_UNREAD);
        let said = self.halt_as(stop, "in the read before drop session two", says);
        Err(format!("{why}{said}"))
    }

    /// The drop test's orders and their trades read after drop session `which` (FBC-be57,
    /// FBC-77r8) on a socket of `on`'s that never arms: every `get_user_trades` page from X's
    /// send, `since` in whole seconds, and every `GET /user/historic-orders` page, each charged to
    /// the shared limiter first; the trades naming each of `orders` (X, then Y after the drop)
    /// counted, each one's `terminal_status` kept as a status name, and the member names the
    /// rows carry. A full page the same as the one before it, row for row, as a venue ignoring
    /// the offset answers, fails the read rather than paging on, and the whole read ends within
    /// [`AFTER_PATIENCE`] of its start ([`HISTORY_CUT`], FBC-8o7a), so a venue answering new
    /// full pages for ever, on historic orders, which QFEX declares no REST bucket for, too,
    /// leaves X's history not read whole rather than holding the run open. A provider rate
    /// refusal is a stop. The owner's presses meanwhile, and one made before they end, are taken,
    /// none ending them: whether one was is the last given.
    async fn read_orders(
        &mut self,
        which: Which,
        on: Armed,
        since: u64,
        orders: &[&AllowedOrder],
    ) -> (Traded, Option<Stop>, bool) {
        let mut after = Traded {
            naming: vec![0; orders.len()],
            terminal: vec![None; orders.len()],
            ..Traded::default()
        };
        let before = self.rejected();
        let mut heard = Refusals::default();
        let mut presses = std::mem::take(&mut self.presses);
        let mut pressed = false;
        let cut = tokio::time::Instant::now() + AFTER_PATIENCE;
        let read = {
            let read = self.reading_orders(on, since, orders, &mut after, &mut heard);
            let read = tokio::time::timeout_at(cut, read);
            tokio::pin!(read);
            loop {
                tokio::select! {
                    biased;
                    read = &mut read => break read,
                    () = presses.pressed() => pressed = true,
                }
            }
        };
        let read = read.unwrap_or_else(|_| Err((HISTORY_CUT.to_owned(), None)));
        pressed |= presses.try_pressed();
        self.presses = presses;
        heard.limiter = self.rejected().saturating_sub(before);
        let what = match which {
            Which::One => "X and its trades",
            Which::Take => "the taker IOC and its trades",
            _ => "X, Y and their trades",
        };
        self.ev.read_sockets.push(ReadSocket {
            before: which.after(),
            label: format!("{} ({what})", name(on)),
            refusals: heard,
        });
        let refused = match read {
            Ok(()) => None,
            Err((why, refused)) => {
                after.error = Some(why);
                refused
            }
        };
        let rose = self.rejected() > before;
        let refused = refused.or(rose.then_some(Stop::RateRefused {
            on: places::RefusedOn::ReadSocket,
        }));
        (after, refused, pressed)
    }

    /// [`Run::read_orders`]'s reads, into `after`, each frame counted in `heard`.
    async fn reading_orders(
        &mut self,
        on: Armed,
        since: u64,
        orders: &[&AllowedOrder],
        after: &mut Traded,
        heard: &mut Refusals,
    ) -> Result<(), (String, Option<Stop>)> {
        let plain = |e: String| (e, None);
        let (cfg, _) = configured(&self.base, self.lookup, on).map_err(plain)?;
        let selection = account_selection(&cfg).map_err(|e| plain(e.to_string()))?;
        let reader = auth_qfex::reader(self.lookup, selection).map_err(plain)?;
        let mut drop_orders = Vec::new();
        for order in orders {
            let made = crate::verdict::DropOrder::new(order.cid, order.venue.clone());
            drop_orders.push(made.map_err(|e| plain(format!("a client id: {e}")))?);
        }
        let url = account_reads::trade_url(&cfg, &reader).map_err(|e| plain(e.to_string()))?;
        let opened = auth_qfex::open(&self.connector, &url, self.opts.connect_wait).await;
        let mut opened = opened.map_err(plain)?;
        let ws = &mut opened;
        let ctx = self.ctx();
        let auth = account_reads::authentication(&cfg, &reader, &ctx);
        self.write(ws, &auth.map_err(|e| plain(e.to_string()))?)
            .await
            .map_err(plain)?;
        let reply = next(ws, self.opts.read_wait, &self.limiter)
            .await
            .map_err(plain)?;
        self.heard_x(heard, &reader, &reply)?;
        if !account_reads::authenticated(&reader, reply.raw()) {
            return Err(plain(
                "QFEX did not authenticate the read socket".to_owned(),
            ));
        }
        let mut offset = 0;
        let mut before: Option<Vec<account_reads::TradeRow>> = None;
        loop {
            let ask = account_reads::TradesAsk::new(since, self.opts.page_size, offset);
            let ask = ask.map_err(|e| plain(e.to_string()))?;
            let write = account_reads::user_trades(&cfg, ask).map_err(|e| plain(e.to_string()))?;
            self.write(ws, &write).await.map_err(plain)?;
            let reply = next(ws, self.opts.read_wait, &self.limiter)
                .await
                .map_err(plain)?;
            self.heard_x(heard, &reader, &reply)?;
            let page = account_reads::decode_trades_page(&reader, reply.raw());
            let page =
                page.map_err(|e| plain(format!("get_user_trades: {}", places::variant_name(&e))))?;
            after.trades += page.rows().len();
            for (n, order) in drop_orders.iter().enumerate() {
                after.naming[n] += page.rows().iter().filter(|r| order.named_by(r)).count();
            }
            let side = self.places.map(|p| p.side);
            self.rows_read(page.rows(), &drop_orders, side, after);
            for row in page.rows() {
                for member in &row.members {
                    let member = places::member_name(member);
                    if !after.fields.contains(&member) {
                        after.fields.push(member);
                    }
                }
            }
            if page.rows().len() < usize::from(ask.limit()) {
                break;
            }
            // A full page the same as the one before it, row for row: a venue ignoring the
            // offset, which would otherwise be paged for as long as the limiter had room. Rows
            // are compared whole, since a row may carry no trade id.
            if before.as_deref() == Some(page.rows()) {
                return Err(plain(
                    "a full get_user_trades page repeated the page before it".to_owned(),
                ));
            }
            before = Some(page.rows().to_vec());
            offset += u32::from(ask.limit());
        }
        after.trades_whole = true;
        let _ = self.close_read(opened).await;
        let mut offset = 0;
        let mut before: Option<Vec<account_reads::HistoricOrder>> = None;
        loop {
            let ask = account_reads::HistoricAsk::new(self.opts.page_size, offset);
            let ask = ask.map_err(|e| plain(e.to_string()))?;
            let ctx = self.ctx();
            let read = account_reads::historic_orders(&cfg, &reader, &ctx, ask);
            let (status, body) = self
                .rest(&read.map_err(|e| plain(e.to_string()))?)
                .await
                .map_err(plain)?;
            if matches!(status, 429 | 418) {
                let rest = places::RefusedOn::Rest;
                return Err((
                    format!("GET /user/historic-orders: HTTP {status}"),
                    Some(Stop::RateRefused { on: rest }),
                ));
            }
            let resp = HttpResponse {
                status,
                headers: &[],
                body: &body,
            };
            let page = account_reads::decode_historic(&resp);
            let page = page.map_err(|e| plain(format!("GET /user/historic-orders: {e}")))?;
            for (n, order) in drop_orders.iter().enumerate() {
                if let Some(status) = order.terminal_in(page.rows()) {
                    after.terminal[n] = Some(status);
                }
            }
            if page.rows().len() < usize::from(ask.limit()) {
                break;
            }
            // As for the trades, a full page the same as the one before it, which QFEX, declaring
            // no REST bucket, would otherwise let the read page without end. A row carries no
            // order id, and two orders' rows may be alike, so a page is compared whole.
            if before.as_deref() == Some(page.rows()) {
                return Err(plain(
                    "a full GET /user/historic-orders page repeated the page before it".to_owned(),
                ));
            }
            before = Some(page.rows().to_vec());
            offset += u32::from(ask.limit());
        }
        after.historic_whole = true;
        Ok(())
    }

    /// What `rows`, one `get_user_trades` page's, show beyond the counts naming each of `orders`
    /// (FBC-4plj; decision 0132's Consequences), into `after`: the kind of each `timestamp` of a
    /// row naming one of them ([`places::time_kind`]), whether its `trade_id` is one a
    /// `fill_response` of ours carried (the tee's, in memory), how a row naming the first states
    /// its side against `side`, ours, and the rows naming none of them, orders the codec did not
    /// place. No value of a row is kept.
    fn rows_read(
        &self,
        rows: &[account_reads::TradeRow],
        orders: &[crate::verdict::DropOrder],
        side: Option<fbc_core::Side>,
        after: &mut Traded,
    ) {
        let kept = self.tee.kept();
        for row in rows {
            if !orders.iter().any(|o| o.named_by(row)) {
                after.foreign += 1;
                continue;
            }
            let kind = places::time_kind(row.timestamp.as_ref());
            if !after.time_kinds.contains(&kind) {
                after.time_kinds.push(kind);
            }
            (after.trade_ids).count(row.trade_id.as_deref(), |id| kept.heard_trade(id));
            let first = orders.first().is_some_and(|o| o.named_by(row));
            if let (true, Some(ours)) = (first, side) {
                after.sides.count(ours, row.side, row.aggressor_side);
            }
        }
    }

    /// Counts `frame`, off the read socket of X and its trades, as a read socket's
    /// ([`Refusals::hear`]), holding the shared budget for a `RateLimited` err, which is a stop;
    /// so is every other refusal its count shows, one in a frame the journal keeps whole
    /// included ([`Refusals::shown`]), from which the adapter reads no hold (decision 0134 items
    /// 2 and 6, 0135 item 2).
    fn heard_x(
        &self,
        heard: &mut Refusals,
        reader: &ReadAuth,
        frame: &Data,
    ) -> Result<(), (String, Option<Stop>)> {
        let raw = frame.raw();
        let spans = account_reads::redact_inbound(reader, Inbound::Frame(raw));
        let shown = heard.shown();
        heard.hear(
            &refusals::kept(raw, &spans),
            &String::from_utf8_lossy(raw.bytes()),
        );
        let on = places::RefusedOn::ReadSocket;
        match account_reads::socket_err(reader, raw) {
            Some(SocketErr::RateLimited(hold)) => {
                let now = tokio::time::Instant::now();
                (self.limiter).provider_rejected(now, NO_CONN, hold.charge, hold.retry_after);
                Err((
                    "a RateLimited err on the read socket of X and its trades".to_owned(),
                    Some(Stop::RateRefused { on }),
                ))
            }
            _ if heard.shown() > shown => Err((
                "a provider rate refusal on X's read socket".to_owned(),
                Some(Stop::RateRefused { on }),
            )),
            _ => Ok(()),
        }
    }
}

/// Where the account reads stood ([`Run::read_mark`]): the shared limiter's `rejected.user`
/// count, the REST refusals and the read sockets recorded.
#[derive(Copy, Clone, Debug)]
struct ReadMark {
    rejected: u64,
    rest: u64,
    sockets: usize,
}

// ----- Stage two: the crossing post-only (FBC-su9i; decisions 0005, 0132 item 7, 0148, 0151) --

impl Run<'_> {
    /// Stage two (FBC-su9i), once every named account's drop test ran with no stop: the run's
    /// exact-echo gate (decision 0148 item 2), judged on what its tee heard, in memory, never
    /// carried to another run. Shut, the run ends [`Stop::NoEcho`], which is not a fault and
    /// writes no stop marker. Open, each named account whose drop test passed in this run
    /// ([`crate::stage_two::crossers`]) in turn places its crossing post-only, then, unless that
    /// stopped the run, its taker IOC, then stage three's pages ([`crate::pager`]), each in a
    /// session of its own ([`Run::cross_test`] under [`Which::Cross`], [`Which::Take`] and
    /// [`Which::Pages`]), before the next account's crossing session (decision 0148 item 1: the
    /// stages on each named account in turn); the first stop ends the run, and with none it ends
    /// `Ok` after the last account's stage three, saying [`FINISHED`].
    async fn cross_tests(&mut self, places: &Places, mut market: Market) -> Result<(), String> {
        let gate = two::EchoGate::of(&self.tee.kept());
        self.ev.stage_two.get_or_insert_with(StageTwo::default).gate = Some(gate.open);
        self.line(format_args!("GATE {}", gate.says()));
        if !gate.open {
            return Err(self.halt(Stop::NoEcho, "once every named account's drop test ran"));
        }
        let named: Vec<&'static str> = self.named.iter().map(|a| name(*a)).collect();
        let crossers = two::crossers(&self.ev.drops, &named);
        let crossing = self.named.clone().into_iter();
        for on in crossing.filter(|a| crossers.contains(&name(*a))) {
            self.cross_test(places, &mut market, on, Which::Cross)
                .await?;
            self.cross_test(places, &mut market, on, Which::Take)
                .await?;
            self.cross_test(places, &mut market, on, Which::Pages)
                .await?;
        }
        self.ev
            .stage_two
            .get_or_insert_with(StageTwo::default)
            .finished = true;
        self.line(format_args!("FINISHED {FINISHED}"));
        Ok(())
    }

    /// Stage two's session `which` on `on`, the crossing post-only ([`Which::Cross`]; decision
    /// 0132 item 7, 0148 items 2, 6 and 8) or the taker IOC ([`Which::Take`]; FBC-ou9g, 0148 item
    /// 5): every named account read clean (a provider rate refusal in those reads a stop, marked,
    /// [`Run::unless_refused`]), the account's registry for it (cross or take) built, its trip
    /// given to the journal before any market arms (decision 0139) and seeded flat (decision 0148
    /// item 3); then its session ([`Run::cross_session`]); then the read of every named account
    /// after it, the session's orders known as ours on its own account. Each way it ends the run
    /// is a stop, marked: the session's stop, or with none its Stop's cancels held as 0151 item
    /// 11 names ([`stage_session_stop`]), a failure that was no stop (with the stop the reads
    /// after it show), then the reads' own ([`stage_after_stop`]): a provider rate refusal, the
    /// owner's Ctrl-C, a position not flat, an order or a TWAP not ours, or an order of ours still
    /// resting, listed by a read or counted resting by the registry, an `Unknown` order included,
    /// never resent (decision 0005). With none it ends `Ok`, the account's taker IOC or the next
    /// account's crossing next.
    async fn cross_test(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        which: Which,
    ) -> Result<(), String> {
        let label = name(on);
        let (mark, made) = (self.read_mark(), self.ev.reads.len());
        let mut presses = std::mem::take(&mut self.presses);
        let reads = self.check_accounts(which.name(), on, which.mark());
        let reads = unless_pressed(&mut presses, reads).await;
        self.presses = presses;
        match reads {
            None => {
                let when = format!("during the reads before {}", which.name());
                return Err(self.halt(Stop::Interrupted, &when));
            }
            // A provider rate refusal they met is the run's stop, marked, whether or not they
            // failed on it (0151 items 4 and 6), as before drop session one: before the crossing
            // session and before the taker IOC session alike.
            Some(reads) => {
                let when = format!("in the reads before {}", which.name());
                self.unless_refused(mark, reads, &when)?;
            }
        }
        let reads = self.ev.reads[made..].to_vec();
        let (cfg, creds) = place_config(&self.base, self.lookup, on)?;
        let acct = account_key(on);
        let order = (self.caps.exec.as_ref()).map(|e| e.order.clone());
        let order = order.ok_or("the probe's caps declare no order entry")?;
        let inst = market.found.inst;
        let symbol = market.found.spec.venue_symbol.clone();
        let keys = places::lease_keys(label, &order, inst, symbol);
        let opened = Opened::now(self.origin, wall_ns());
        let caps = market::pre_trade_caps(inst, market.caps);
        let built = PlaceRegistry::new(
            which.step(),
            label,
            acct,
            &market.bounds,
            opened,
            inst,
            caps,
            keys,
        );
        built.trip_on(&self.journal);
        let rpc = Duration::from_millis(self.opts.rpc_timeout_ms);
        let ladder = fbc_oms::LadderConfig::new(rpc, Duration::ZERO, rpc * 6, 2);
        let ladder = ladder.map_err(|e| format!("the ladder: {}", places::variant_name(&e)))?;
        let (link, mut placed) = built.into_link(order, ladder);
        let link = Rc::new(RefCell::new(link));
        self.built.push(BuiltReg {
            on,
            step: which.step(),
            link: Rc::clone(&link),
            xy: Vec::new(),
        });
        let seeded = link
            .borrow_mut()
            .reg()
            .seed_position(inst, fbc_core::SignedLots(0));
        seeded.map_err(|e| format!("the flat seed was refused: {}", places::variant_name(&e)))?;
        self.line(format_args!(
            "REGISTRY {} on {label}: {} on its one market, its CancelOnlyTrip given to the \
             journal before any market arms (decision 0139), then seeded flat after clean reads \
             of every named account (decision 0148 item 3)",
            which.step().name(),
            ledger::PLACES_DECLARATION
        ));
        let two = self.ev.stage_two.get_or_insert_with(StageTwo::default);
        let begun = CrossRun {
            account: label,
            ..CrossRun::default()
        };
        match which {
            Which::Take => two.takes.push(begun),
            Which::Pages => self.ev.stage_three.push(PagesRun {
                session: begun,
                ..PagesRun::default()
            }),
            _ => two.crosses.push(begun),
        }
        let at = self.allowances.iter().position(|(a, _)| *a == on);
        let (_, mut allowance) = self
            .allowances
            .remove(at.expect("its drop test's allowance"));
        let span_end = placed.deadline();
        let ctx = (cfg, creds, Rc::clone(&link), span_end);
        let (ran, session_orders) = self
            .cross_session(places, market, on, ctx, (&mut allowance, &reads), which)
            .await;
        self.allowances.push((on, allowance));
        // The registry serves no more sessions: its market disarmed and its lease given up, as its
        // Stop's cancels already did unless its connection ended first.
        link.borrow_mut().reg().disarm(inst);
        placed.session_ended(&link.borrow(), which.name(), self.mono());
        self.ev.place_registries.push(placed);
        // The session's stop: its own, or, with none, its Stop's cancels held as 0151 item 11
        // names, which stop the run in their own right (FBC-3xzn). A registry's order that kept
        // the session's order back is counted resting, never seen resting (the owner's ruling
        // under 0151 item 6 on FBC-gisx; its review).
        let kept_back = self.stage_run(which).blocked.is_some();
        let cancels = self.stage_run(which).cancels.clone();
        let session = stage_session_stop(ran, kept_back, cancels.as_ref());
        let said =
            session.map(|(stop, says)| self.halt_as(stop, &format!("in {}", which.name()), says));
        // The session's orders, once placed (the crossing post-only or the taker IOC, then its
        // flatten IOCs), are ours on its own account, by their venue ids once the streams named
        // them: an order or a TWAP any read lists that is none of them is what the streams did
        // not show (decision 0148 item 8; FBC-77r8's review).
        let session_orders: Vec<AllowedOrder> = (session_orders.into_iter())
            .map(|cid| AllowedOrder {
                account: on,
                cid,
                venue: (link.borrow().registry().get(cid)).and_then(|r| r.vid().cloned()),
            })
            .collect();
        let ours: Vec<&AllowedOrder> = session_orders.iter().collect();
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        let after = match self.stage_run(which).ended_by == Some(DROPPED) {
            true => self.read_after_stage_drop(which, on, &ours).await,
            false => self.read_after(which, on, &ours).await,
        };
        self.patient = None;
        let counted = two::counted_resting(link.borrow().registry(), inst);
        let run = self.stage_run(which);
        run.resting = after.ours_resting;
        run.read = after.x_read;
        run.counted_resting = counted;
        run.others = match (after.unexpected, after.unread) {
            (true, _) => Some(true),
            (false, true) => None,
            (false, false) => Some(false),
        };
        // A position not flat in those reads is said whatever stop came first: it is the
        // owner's (0151 item 9; FBC-su9i's review).
        run.not_flat_after = match (after.not_flat, after.unread) {
            (true, _) => Some(true),
            (false, true) => None,
            (false, false) => Some(false),
        };
        // The owner's hand cancel: an order of ours a read listed, one only the registry counts
        // resting, or the account not read once an order was sent there, each said as it is,
        // never an order seen resting that no read listed (FBC-gisx's review).
        let session = which.stage().expect("a stage-two or stage-three session");
        let hand = two::hand_cancel(self.stage_run(which), session);
        if let Some(hand) = hand {
            self.line(format_args!("RESTING {hand}"));
        }
        if after.unexpected {
            let others = match which {
                Which::Take => crate::taker::OTHERS_TAKE,
                Which::Pages => crate::pager::OTHERS_PAGES,
                _ => two::OTHERS_CROSS,
            };
            self.line(format_args!("READ after {}: {others}", which.name()));
        }
        let first = (after.refused, after.pressed);
        let shown = (after.not_flat, after.unexpected);
        let resting = (after.ours_resting || counted, after.unread);
        let read = stage_after_stop(session, first, shown, resting);
        // An order of ours no read listed, counted resting by the registry or on an account not
        // read, is said so, never as seen resting (the owner's ruling under 0151 item 6 on
        // FBC-gisx).
        let says = read.and_then(|(stop, _)| stage_counted_says(stop, after.ours_resting, counted));
        let ended = match (said, self.stage_run(which).error.clone(), read) {
            (Some(said), ..) => Err(said),
            (None, Some(why), Some((stop, when))) => {
                Err(format!("{why}; {}", self.halt_as(stop, when, says)))
            }
            (None, Some(why), None) => Err(why),
            (None, None, Some((stop, when))) => Err(self.halt_as(stop, when, says)),
            (None, None, None) => Ok(()),
        };
        // The taker IOC's trades and historic orders, once its session and the reads after it
        // ended with no stop (FBC-4plj): the answers decision 0132's Consequences ask of a
        // trade.
        let ended = match (ended, which) {
            (Ok(()), Which::Take) => self.take_trades(on, &ours).await,
            (ended, _) => ended,
        };
        let stop = self.stop.map(Stop::name);
        self.stage_run(which).stop = stop;
        ended
    }

    /// After a taker IOC session on `on` that ended with no stop, its orders `ours` (the taker
    /// IOC, then each flatten IOC) read as the drop test's are after a session
    /// ([`Run::read_orders`], on a socket that never arms): every `get_user_trades` page since
    /// the taker IOC's send and every `GET /user/historic-orders` page, each charged to the
    /// shared limiter first, waiting out a hold for at most [`AFTER_PATIENCE`]. Its rows give
    /// decision 0132's Consequences what kind of time a trade's `timestamp` is, whether its
    /// `trade_id` is the fills channel's, which side field it carries, and any row naming an
    /// order the codec did not place; the historic orders give each order's `terminal_status`.
    /// A read that could not be made is recorded and stops nothing; a provider rate refusal in
    /// it is the run's stop, marked, and so is the owner's Ctrl-C meanwhile, and then a row
    /// naming none of `ours`, or none at all: a trade of an order the streams did not show,
    /// [`Stop::ReadDisagrees`] (decision 0148 item 8; PR #284's review), each in that order, as
    /// [`stage_read_stop`] ranks them. Nothing is read when no taker IOC was handed to the
    /// session.
    async fn take_trades(&mut self, on: Armed, ours: &[&AllowedOrder]) -> Result<(), String> {
        let Some(since) = self.taker_secs.take().filter(|_| !ours.is_empty()) else {
            return Ok(());
        };
        self.patient = Some(tokio::time::Instant::now() + AFTER_PATIENCE);
        let (traded, refused, pressed) = self.read_orders(Which::Take, on, since, ours).await;
        self.patient = None;
        self.line(format_args!(
            "TRADES after {} on {}: {} row{} read since the taker IOC's send, {} naming it; \
             read whole: trades {}, historic orders {}",
            places::TAKE,
            name(on),
            traded.trades,
            if traded.trades == 1 { "" } else { "s" },
            traded.naming.first().copied().unwrap_or(0),
            places::yn(traded.trades_whole),
            places::yn(traded.historic_whole)
        ));
        let foreign = traded.foreign;
        self.stage_run(Which::Take).traded = Some(traded);
        let when = format!("reading the taker IOC's trades after {}", places::TAKE);
        match (refused, pressed, foreign) {
            (Some(stop), ..) => Err(self.halt(stop, &when)),
            (None, true, _) => Err(self.halt(Stop::Interrupted, &when)),
            (None, false, 1..) => Err(self.halt(Stop::ReadDisagrees, TAKE_TRADES_OTHERS)),
            (None, false, 0) => Ok(()),
        }
    }

    /// Stage two's session `which` on `on` (decision 0132 item 7), the crossing session or the
    /// taker IOC session: an unchanged `ExecSession` whose handler is a [`PlaceHandler`] over its
    /// registry's `link`, its steps ([`Run::cross_steps`] or [`Run::take_steps`]) run beside it and
    /// the MDS touch driven on by [`Run::stage`] meanwhile (FBC-kllr), its span bound met one
    /// `--rpc-timeout-ms` early ([`cancels_begin`]; decision 0148 item 8). Once its steps end, how
    /// its order came back is read before any cancel of ours: the crossing post-only's refusal
    /// ([`crate::stage_two::Crossed::of`]), or the taker IOC's end, its fees' signs and its frames'
    /// order ([`Run::taken`]), which may call for the flatten or stop the run left open with a fill
    /// ([`crate::taker::after_ioc`]). A fill of ours that calls for it is flattened while the
    /// session is open ([`Run::flatten_open`]). Then, whatever ended its steps, it ends under
    /// [`Policy::Stage`]: every order of ours cancelled by venue id, then a graceful close, or,
    /// those cancels refused locally or met by a provider rate refusal, the abrupt drop, since this
    /// account's drop test passed in this run (0151 item 11). Gives the run's stop, if any came; a
    /// failure that is no stop is recorded as the session's error, and one before the session
    /// opened is its error too.
    async fn cross_session(
        &mut self,
        places: &Places,
        market: &mut Market,
        on: Armed,
        (cfg, creds, link, span_end): (
            VenueConfig,
            fbc_core::Secrets,
            Rc<RefCell<Link>>,
            tokio::time::Instant,
        ),
        (allowance, reads): (&mut Allowance, &[AccountRead]),
        which: Which,
    ) -> (Option<Stop>, Vec<fbc_core::ClientOrderId>) {
        let acct = account_key(on);
        let label = name(on);
        let before = self.rejected();
        let sockets = self.ev.read_sockets.len();
        let wake = Rc::new(tokio::sync::Notify::new());
        let watched = Rc::new(RefCell::new(Watched::after(&link.borrow())));
        let control = Rc::new(RefCell::new(None));
        let handler = PlaceHandler {
            inner: LinkHandler {
                link: Rc::clone(&link),
                wake: Rc::clone(&wake),
            },
            watched: Rc::clone(&watched),
            control: Rc::clone(&control),
        };
        let config = self.place_session_config(acct, cfg, creds, market.found.specs.clone());
        let built = ExecSession::new(config, handler);
        let (mut session, ctl) = match built {
            Ok(built) => built,
            Err(e) => {
                self.stage_run(which).error = Some(format!("the {label} session: {e}"));
                return (None, Vec::new());
            }
        };
        *control.borrow_mut() = Some(ctl);
        session.set_journal(self.journal.clone());
        let orders = session.orders();
        link.borrow_mut().attach(orders.clone(), acct);
        self.line(format_args!(
            "SESSION {label} ({}): it arms, resyncs and Starts, places {}, compares, and cancels \
             every order of ours by venue id before it stops",
            which.name(),
            which.order()
        ));
        let (heard_from, sent_from) = {
            let kept = self.tee.kept();
            (kept.heard.len(), kept.sent().len())
        };
        let rpc = Duration::from_millis(self.opts.rpc_timeout_ms);
        let d = DropCtx {
            link,
            wake,
            watched: Rc::clone(&watched),
            orders,
            acct,
            inst: market.found.inst,
            caps: market.caps,
            span_end: cancels_begin(span_end, rpc, tokio::time::Instant::now()),
            on,
            which,
            heard_from,
            sent_from,
        };
        let mut presses = std::mem::take(&mut self.presses);
        let mut placed = None;
        // Stage three's orders, P1 to P3, as each is handed to the session (FBC-4plj).
        let mut paged: Vec<Placed> = Vec::new();
        let (mut stop, mut error) = (None, None);
        let view = market.touch.clone();
        let found = market.found.clone();
        let passed = two::crossers(&self.ev.drops, &[label]) == [label];
        let mut over = false;
        let (ended_by, rested_at_close);
        {
            let running = session.run();
            tokio::pin!(running);
            let steps = async {
                let ctx = (&found, &view);
                let state = (&mut placed, reads);
                match which {
                    Which::Take => {
                        (self.take_steps(places, ctx, &d, allowance, &mut presses, state)).await
                    }
                    Which::Pages => {
                        let state = (state.0, &mut paged);
                        (self.pages_steps(places, ctx, &d, allowance, &mut presses, state)).await
                    }
                    _ => (self.cross_steps(places, ctx, &d, allowance, &mut presses, state)).await,
                }
            };
            let stepped = tokio::select! {
                biased;
                ended = &mut running => Err(ended),
                stepped = steps => Ok(stepped),
            };
            match stepped {
                Ok(Ok(())) => stop = self.stop_now(&d),
                Ok(Err(Halt::Stop(s))) => stop = Some(s),
                Ok(Err(Halt::Error(e))) => error = Some(e),
                Err(ended) => {
                    over = true;
                    (stop, error) = ended_first(self.stop_now(&d), ended);
                }
            }
            // How its order came back, before any cancel of ours; the taker IOC's may call for
            // the flatten, or stop the run left open with a fill (0151 item 8).
            let after = match which {
                Which::Take => self.taken(&d, placed),
                Which::Pages => crate::taker::After::Nothing,
                _ => {
                    self.crossed(&d, placed.map(|p: Placed| p.cid));
                    crate::taker::After::Nothing
                }
            };
            if let Some(raised) = after.stop() {
                stop = stop.or(Some(raised));
            }
            if stop == Some(Stop::SpanBound) {
                let lead = span_end.saturating_duration_since(tokio::time::Instant::now());
                self.stage_run(which).cancels_lead = Some(lead);
            }
            // The session's orders: its own, or stage three's P1 to P3 once placed.
            let ours: Vec<fbc_core::ClientOrderId> = match which {
                Which::Pages => paged.iter().map(|p| p.cid).collect(),
                _ => placed.iter().map(|p| p.cid).collect(),
            };
            // A fill of ours, the crossing post-only's or the taker IOC's, the session still
            // open: the flatten, before its Stop (FBC-ou9g; 0151 item 9). The taker IOC's fill
            // is expected, so once flat it stops nothing.
            let taker = after == crate::taker::After::Flatten;
            // A taker IOC that left nothing to follow it as the steps ended may still fill, its
            // fill expected, during the Stop's cancels or after them: then it is flattened before
            // the close ([`Run::late_fill`]). One left open with a fill, or not settled, is not.
            let late_taker = which == Which::Take && after == crate::taker::After::Nothing;
            let flattened = (d.watched.borrow().fill_of_ours || taker) && !over;
            if flattened {
                if taker && !d.watched.borrow().fill_of_ours {
                    self.flatten_run(which).after = crate::taker::AFTER_TAKER;
                }
                (stop, over) = self
                    .flatten_open(
                        places,
                        (&found, &view),
                        &d,
                        &mut running,
                        (allowance, &mut presses),
                        (&ours, which.name()),
                        stop,
                    )
                    .await;
            }
            let policy = Policy::Stage {
                drop_passed: passed,
            };
            let closed = self
                .end_session(
                    places,
                    (&found, &view),
                    &d,
                    (&mut running, &control),
                    (allowance, &mut presses),
                    policy,
                    (stop, error.is_some(), over, flattened, late_taker),
                    (&ours, placed.map(|p| p.cid), before),
                )
                .await;
            if let Some(cancels) = closed.cancels {
                self.stop_cancelled(which, cancels);
            }
            if let Some(compared) = closed.compared {
                self.compared(which, compared);
            }
            // A press, or a stop the tee or the handler raised, taken as the close waited.
            stop = closed.stop;
            ended_by = closed.ended_by;
            rested_at_close = closed.rested_at_close;
        }
        stop = stop.or_else(|| self.stop_now(&d));
        self.presses = presses;
        let counters = session.counters();
        drop(session);
        // Stage three's pager, a read socket of the session's account, with the refusals it met
        // as it went, kept whether or not its steps ran to their end.
        if which == Which::Pages && self.three_run().paged {
            let refusals = self.three_run().refusals;
            self.ev.read_sockets.push(ReadSocket {
                before: which.name(),
                label: format!("{label} (stage three's pager)"),
                refusals,
            });
        }
        let compared: u64 = (self.ev.read_sockets[sockets..].iter())
            .map(|r| r.refusals.limiter)
            .sum();
        let refused = self.rejected().saturating_sub(before + compared);
        let seen = (counters, refused, error.clone(), ended_by, rested_at_close);
        self.session_seen(&d, label, seen);
        self.stage_run(which).error = error;
        // Every order of ours the session placed: its own, or stage three's, then each flatten
        // IOC it sent, each once.
        let mut orders: Vec<fbc_core::ClientOrderId> = match which {
            Which::Pages => paged.iter().map(|p| p.cid).collect(),
            _ => placed.iter().map(|p| p.cid).collect(),
        };
        for cid in watched.borrow().expected.iter() {
            if !orders.contains(cid) {
                orders.push(*cid);
            }
        }
        (stop, orders)
    }

    /// Records how the crossing post-only, `cid` once placed, came back
    /// ([`crate::stage_two::Crossed::of`]): from the frames the tee heard on `d`'s session, its
    /// client id as written, and where `d`'s registry holds it now, before any cancel of ours.
    fn crossed(&mut self, d: &DropCtx, cid: Option<fbc_core::ClientOrderId>) {
        let Some(cid) = cid else {
            return;
        };
        let text = two::cid_text(cid).unwrap_or_default();
        let state = d.link.borrow().registry().get(cid).map(|r| r.state());
        let crossed = two::Crossed::of(&self.tee.kept(), d.heard_from, &text, state);
        self.line(format_args!(
            "CROSSED on {}: {}",
            name(d.on),
            crossed.says()
        ));
        self.cross_run().crossed = Some(crossed);
    }

    /// Records how the taker IOC, `placed` once handed to the session, came back on `d`'s session
    /// ([`crate::taker::Taken`]; FBC-ou9g), before any cancel of ours: its end as `d`'s registry
    /// holds it now ([`Ioc::of`]: an err QFEX echoed its `add_order` with among the session's
    /// frames, or the session's refusal to send it, included), the sign of each of its fills'
    /// fees ([`crate::taker::FeeSign`]) and the order of its final `order_response` and its
    /// `fill_response`s ([`crate::taker::FrameOrder`]). Gives what follows it
    /// ([`crate::taker::after_ioc`]): nothing when it was not placed.
    fn taken(&mut self, d: &DropCtx, placed: Option<Placed>) -> crate::taker::After {
        let Some(placed) = placed else {
            return crate::taker::After::Nothing;
        };
        let cid = placed.cid;
        let not_sent = {
            let link = d.link.borrow();
            matches!(link.outcome_of(placed.rpc), Some(SubmitOutcome::NotSent(_)))
        };
        let (ioc, position, unsettled) = self.ioc_now(d, cid, not_sent);
        let fees: Vec<crate::taker::FeeSign> = (d.watched.borrow().fee_signs.iter())
            .filter(|(of, _)| *of == cid)
            .map(|(_, sign)| *sign)
            .collect();
        let order = self.frame_order(d, cid);
        let taken = crate::taker::Taken {
            ioc: Some(ioc),
            fees,
            frame_order: order,
        };
        self.line(format_args!(
            "TAKEN on {}: {}; the taker fee's sign: {}",
            name(d.on),
            crate::taker::came_back(Some(&taken)),
            crate::taker::fee_said(&taken.fees)
        ));
        self.stage_run(Which::Take).taken = Some(taken);
        crate::taker::after_ioc(ioc, position, unsettled)
    }

    /// The order of the taker IOC `cid`'s final `order_response` and its `fill_response`s among
    /// the frames the tee heard on `d`'s session so far ([`crate::taker::FrameOrder`]), once its
    /// `add_order` was sent.
    fn frame_order(
        &self,
        d: &DropCtx,
        cid: fbc_core::ClientOrderId,
    ) -> Option<crate::taker::FrameOrder> {
        let text = two::cid_text(cid).unwrap_or_default();
        let kept = self.tee.kept();
        let heard = kept.heard.get(d.heard_from..).unwrap_or_default();
        kept.index_of(&text)
            .map(|sent| crate::taker::FrameOrder::of(heard, sent))
    }

    /// Whether a `fill_response` naming the taker IOC `cid` was heard on `d`'s session and no
    /// `order_response` naming it with a status other than `ACK`, its final one, was yet.
    fn final_order_unheard(&self, d: &DropCtx, cid: fbc_core::ClientOrderId) -> bool {
        use crate::tee::Heard;
        let text = two::cid_text(cid).unwrap_or_default();
        let kept = self.tee.kept();
        let Some(sent) = kept.index_of(&text) else {
            return false;
        };
        let heard = kept.heard.get(d.heard_from..).unwrap_or_default();
        let names = |cid: &crate::tee::Cid| cid.sent() == Some(sent);
        let filled = (heard.iter()).any(|h| matches!(h, Heard::Fill { cid, .. } if names(cid)));
        let ended = heard.iter().any(|h| match h {
            Heard::Order { cid, status, .. } => names(cid) && status != "ACK",
            _ => false,
        });
        filled && !ended
    }

    /// How the taker IOC `cid` stands now on `d`'s session: its end as `d`'s registry holds it
    /// ([`Ioc::of`], with `not_sent` whether the session refused to send it and an err QFEX echoed
    /// its `add_order` with among the session's frames), the registry's position on the market,
    /// and whether the venue's count of its fills is above the registry's.
    fn ioc_now(
        &self,
        d: &DropCtx,
        cid: fbc_core::ClientOrderId,
        not_sent: bool,
    ) -> (Ioc, fbc_core::SignedLots, bool) {
        let link = d.link.borrow();
        let record = link.registry().get(cid);
        let state = record.map(|r| r.state());
        let counts = record.map_or((Lots::ZERO, Lots::ZERO), |r| (r.cum_fills(), r.cum_venue()));
        let qty = record.map_or(Lots::ZERO, |r| r.qty());
        // An err echoing its own add_order alone, never a flatten IOC's (PR #284's review).
        let text = two::cid_text(cid).unwrap_or_default();
        let echoed = self.tee.kept().err_echo_of(d.heard_from, &text);
        let ioc = Ioc::of(state, (counts.0, counts.1, qty), echoed, not_sent);
        (ioc, link.registry().inventory(d.inst), counts.1 > counts.0)
    }

    /// What a flatten that [`Run::end_session`] runs on `d`'s session after its steps ended
    /// answers, if one is due: a fill of ours other than an expected one
    /// ([`crate::flatten::AFTER_LATE_FILL`]), or, when `late_taker` (the taker IOC session, whose
    /// IOC `own` left nothing to follow it as its steps ended), the taker IOC's own fill that
    /// reached the session since and left the registry's position off flat, judged again by
    /// [`crate::taker::after_ioc`] ([`crate::taker::AFTER_LATE_TAKER`]). The session sent the
    /// IOC, since a refused one fills nothing.
    fn late_fill(
        &self,
        d: &DropCtx,
        late_taker: bool,
        own: Option<fbc_core::ClientOrderId>,
    ) -> Option<&'static str> {
        if d.watched.borrow().fill_of_ours {
            return Some(crate::flatten::AFTER_LATE_FILL);
        }
        let cid = own.filter(|_| late_taker)?;
        let (ioc, position, unsettled) = self.ioc_now(d, cid, false);
        let after = crate::taker::after_ioc(ioc, position, unsettled);
        (after == crate::taker::After::Flatten).then_some(crate::taker::AFTER_LATE_TAKER)
    }

    /// The taker IOC session's steps (FBC-ou9g; decision 0132 item 7): authentication, an
    /// `Accepted` arm and the resync ([`Run::armed`]); Start under the take registry's market
    /// lease; no other order of ours on any registry of the run or any named account's read
    /// before the session ([`Run::no_other_order`]; decision 0148 item 6); the taker IOC
    /// ([`Run::place_taker`]), waited on until it ends with its fills settled or its deadline,
    /// never resent (decision 0005); then the streams compared with the reads. Each step ends at
    /// the first stop ([`Run::waiting`]).
    async fn take_steps(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        (placed, reads): (&mut Option<Placed>, &[AccountRead]),
    ) -> Result<(), Halt> {
        self.armed(d, presses).await?;
        let dir = self.leases.as_ref().map(|l| l.dir.clone());
        let dir = dir.ok_or_else(|| Halt::Error("no lease directory".to_owned()))?;
        places::start(d.link.borrow_mut().reg(), &dir, d.inst).map_err(Halt::Error)?;
        self.stage_run(Which::Take).started = true;
        self.line(format_args!(
            "START the take registry on {}: the market armed under its market lease, with no \
             account lease (QFEX's nonce scope is none)",
            name(d.on)
        ));
        #[cfg(test)]
        self.test_other_order(places, found, Which::Take).await;
        let checked = self.no_other_order(d.inst, reads);
        // What the report says of another order of ours comes from this check alone (FBC-77r8's
        // review, carried).
        self.stage_run(Which::Take).checked = true;
        if let Err(blocked) = checked {
            let said = blocked.says();
            self.line(format_args!(
                "BLOCKED the taker IOC on {}: {said} (decision 0148 item 6)",
                name(d.on)
            ));
            self.stage_run(Which::Take).blocked = Some(said);
            return Err(Halt::Stop(blocked.stop()));
        }
        let sent = self
            .place_taker(places, (found, touch), d, allowance, presses, placed)
            .await?;
        let ours = [sent.cid];
        let compared = self.compare_step(d, (found, &ours), d.on, "the taker IOC", Some(sent.at));
        let compared = self.unless_stopped(d, presses, compared).await?;
        let stop = compared.0.stop;
        self.compared(Which::Take, compared);
        match stop {
            Some(stop) => Err(Halt::Stop(stop)),
            None => Ok(()),
        }
    }

    /// Places the taker IOC on `d`'s session (FBC-ou9g; decision 0132 item 7): a limit IOC at the
    /// instrument's minimum size at the far touch (the ask for a buy, the bid for a sell),
    /// through it, spent from the take registry's list first ([`Allowance`]), its client id
    /// minted under the run's namespace lease, as an fbc-oms authorization, its fills expected
    /// ([`Watched::expected`]: no [`Stop::UnexpectedFill`], 0151 item 9). Then its end with its
    /// fills settled ([`Ioc::settled`]), or, left open with every fill its order events state
    /// (0151 item 8), its fills settled alone, each once a `fill_response` naming it is followed
    /// by its final `order_response` ([`Run::final_order_unheard`]), or its deadline
    /// (`--rpc-timeout-ms` from its send):
    /// one never answered ends `Unknown` there unless an echoed `ACK` already routed it, never
    /// resent (decision 0005). Before it is built into a command, both USD caps are asked again
    /// at the touch it is priced at ([`market::caps_admit`]); an order either would not admit is
    /// not sent, and never resized, the run's stop [`Stop::BeyondCaps`] (decision 0113). Gives
    /// it, as placed, once handed to the session (`placed` holds it from then); a stop or a press
    /// first places nothing.
    async fn place_taker(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        placed: &mut Option<Placed>,
    ) -> Result<Placed, Halt> {
        allowance.take(Step::Take, ledger::TAKER)?;
        let leases = self.leases.as_mut().expect("a place-stage run's leases");
        let cid = leases.mint.mint(1).await.map_err(Halt::Error)?[0];
        let quote = touch.now(places.touch_max_age).map_err(Halt::Error)?;
        let px = market::far_touch(&found.spec, places.side, quote).map_err(Halt::Error)?;
        let new = fbc_core::NewOrder {
            cid,
            inst: d.inst,
            side: places.side,
            qty: found.min_size,
            kind: fbc_core::OrderKind::Limit { px },
            tif: fbc_core::Tif::Ioc,
            channel: fbc_core::Channel::Public,
            post_only: false,
            reduce_only: false,
            reducing: false,
        };
        self.not_stopped(d, presses)?;
        // The caps asked again at the touch the taker IOC is priced at, as for X, Y and the
        // crossing post-only: fbc-oms holds them in lots fixed at the preflight's touch
        // (decision 0113).
        if !market::caps_admit(places, found, quote, d.caps, found.min_size) {
            return Err(Halt::Stop(Stop::BeyondCaps));
        }
        // Its fills are expected (0151 item 9), before it is handed to the session.
        d.watched.borrow_mut().expected.push(cid);
        let submitted = {
            let mut link = d.link.borrow_mut();
            let cmd = link
                .reg()
                .place(new)
                .map_err(|e| format!("the taker IOC was refused: {}", places::variant_name(&e)));
            let auth = cmd.and_then(|cmd| {
                let auth = link.reg().authorize(d.acct, cmd);
                auth.map_err(|e| {
                    format!(
                        "the taker IOC was not authorized: {}",
                        places::variant_name(&e)
                    )
                })
            });
            auth.and_then(|auth| {
                let submitted = link.submit(&d.orders, auth);
                submitted.map_err(|e| {
                    let named = places::refusal_named(&e);
                    format!("the taker IOC was refused by the session: {named}")
                })
            })
        };
        let rpc = submitted.map_err(Halt::Error)?;
        let now = Placed {
            cid,
            rpc,
            at: tokio::time::Instant::now(),
            wall_secs: u64::try_from(wall_secs()).unwrap_or(0),
        };
        *placed = Some(now);
        self.taker_secs = Some(now.wall_secs);
        self.stage_run(Which::Take).placed.submitted = true;
        self.line(format_args!(
            "PLACE the taker IOC on {}: one limit IOC at the instrument's minimum size at the far \
             touch, through it (no price, size or id is shown)",
            name(d.on)
        ));
        // Its end with its fills settled, or, left open with a fill, its fills settled, or its
        // deadline. A fill that ends it whole in the registry can come before its final
        // order_response, which QFEX may send a moment later: once a fill_response named it, the
        // wait is for an order_response naming it too, so the frame order (FBC-tfxk) is read on
        // both frames, or the deadline.
        let due = now.at + Duration::from_millis(self.opts.rpc_timeout_ms);
        let ended = self.waiting(d, presses, |link, _| {
            let record = link.registry().get(cid);
            let state = record.map(|r| r.state());
            let (fills, venue) =
                record.map_or((Lots::ZERO, Lots::ZERO), |r| (r.cum_fills(), r.cum_venue()));
            let open = matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled));
            let left_open = open && venue > Lots::ZERO && fills >= venue;
            let over = tokio::time::Instant::now() >= due;
            let unordered = self.final_order_unheard(d, cid);
            ((Ioc::settled(state, fills, venue) || left_open) && !unordered || over).then_some(())
        });
        let woken = async {
            tokio::time::sleep_until(due).await;
            d.wake.notify_waiters();
            std::future::pending::<()>().await
        };
        tokio::select! {
            biased;
            ended = ended => ended?,
            () = woken => unreachable!("it wakes the wait, then waits for ever"),
        }
        // It is ours whatever client id its later frames carry (decision 0132 item 7).
        let vid = (d.link.borrow().registry().get(cid)).and_then(|r| r.vid().cloned());
        if let Some(vid) = vid {
            self.tee.ours(vid.as_str());
        }
        // Its own ACK alone, by its client id (PR #284's review).
        let text = two::cid_text(cid).unwrap_or_default();
        let exact = self.tee.kept().exact_ack_of(d.heard_from, &text);
        let acked_ours = self.tee.kept().acked_of(d.heard_from, &text);
        let outcome = d.link.borrow().outcome_of(rpc);
        let run = &mut self.stage_run(Which::Take).placed;
        run.acked = acked_ours;
        run.outcome = outcome.as_ref().map(places::outcome_name);
        run.exact_echo = exact;
        Ok(now)
    }

    /// The crossing session's steps (decision 0132 item 7): authentication, an `Accepted` arm
    /// and the resync ([`Run::armed`]); Start under the cross registry's market lease; no other
    /// order of ours on any registry of the run or any named account's read before the session
    /// ([`Run::no_other_order`]); the crossing post-only ([`Run::place_crossing`]); its first
    /// answer, then its end or its deadline, an `Unknown` outcome included, never resent
    /// (decision 0005); then the streams compared with the reads. Each step ends at the first
    /// stop ([`Run::waiting`]).
    async fn cross_steps(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        (placed, reads): (&mut Option<Placed>, &[AccountRead]),
    ) -> Result<(), Halt> {
        self.armed(d, presses).await?;
        let dir = self.leases.as_ref().map(|l| l.dir.clone());
        let dir = dir.ok_or_else(|| Halt::Error("no lease directory".to_owned()))?;
        places::start(d.link.borrow_mut().reg(), &dir, d.inst).map_err(Halt::Error)?;
        self.cross_run().started = true;
        self.line(format_args!(
            "START the cross registry on {}: the market armed under its market lease, with no \
             account lease (QFEX's nonce scope is none)",
            name(d.on)
        ));
        #[cfg(test)]
        self.test_other_order(places, found, Which::Cross).await;
        let checked = self.no_other_order(d.inst, reads);
        self.cross_run().checked = true;
        if let Err(blocked) = checked {
            let said = blocked.says();
            self.line(format_args!(
                "BLOCKED the crossing post-only on {}: {said} (decision 0148 item 6)",
                name(d.on)
            ));
            self.cross_run().blocked = Some(said);
            return Err(Halt::Stop(blocked.stop()));
        }
        let sent = self
            .place_crossing(places, (found, touch), d, allowance, presses, placed)
            .await?;
        let ours = [sent.cid];
        let compared = self.compare_step(
            d,
            (found, &ours),
            d.on,
            "the crossing post-only",
            Some(sent.at),
        );
        let compared = self.unless_stopped(d, presses, compared).await?;
        let stop = compared.0.stop;
        self.compared(Which::Cross, compared);
        match stop {
            Some(stop) => Err(Halt::Stop(stop)),
            None => Ok(()),
        }
    }

    /// Stage three's session's steps (FBC-4plj; decision 0132 item 7): authentication, an
    /// `Accepted` arm and the resync ([`Run::armed`]); Start under the pages registry's market
    /// lease; then the pager ([`crate::pager`]), a second socket that never arms, on the
    /// session's account and the configuration the guard admitted, at a page size of 1, every
    /// frame charged to the shared limiter first; then [`Run::paging`]; then the pager closed,
    /// its close charged first as a read socket's is. Each step ends at the first stop
    /// ([`Run::waiting`]).
    async fn pages_steps(
        &mut self,
        places: &Places,
        view: (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        placed: (&mut Option<Placed>, &mut Vec<Placed>),
    ) -> Result<(), Halt> {
        self.armed(d, presses).await?;
        let dir = self.leases.as_ref().map(|l| l.dir.clone());
        let dir = dir.ok_or_else(|| Halt::Error("no lease directory".to_owned()))?;
        places::start(d.link.borrow_mut().reg(), &dir, d.inst).map_err(Halt::Error)?;
        self.stage_run(Which::Pages).started = true;
        self.line(format_args!(
            "START the pages registry on {}: the market armed under its market lease, with no \
             account lease (QFEX's nonce scope is none)",
            name(d.on)
        ));
        let (cfg, _) = configured(&self.base, self.lookup, d.on).map_err(Halt::Error)?;
        let reader = account_selection(&cfg)
            .map_err(|e| e.to_string())
            .and_then(|selection| auth_qfex::reader(self.lookup, selection))
            .map_err(Halt::Error)?;
        // The pager's own handles on what the run shares: the limiter's clone shares its buckets.
        let (connector, limiter, caps) = (
            self.connector.clone(),
            self.limiter.clone(),
            self.caps.clone(),
        );
        let io = crate::compare::Io {
            connector: &connector,
            limiter: &limiter,
            caps: &caps,
            ns: self.ns(),
            page_size: crate::pager::PAGE_SIZE,
            connect_wait: self.opts.connect_wait,
            read_wait: self.opts.read_wait,
            symbol: view.0.spec.venue_symbol.as_wire(),
        };
        let mut nonces = self.nonces.clone();
        let origin = self.origin;
        let mut ctx = move || EncodeCtx {
            wall: wall_ns(),
            mono: ledger::mono_since(origin),
            nonces: nonces.reserve(ReadAuth::NONCES),
        };
        let mut pager = crate::compare::Socket::new(io, &cfg, &reader, &mut ctx);
        let paged = self
            .paging(places, view, d, (allowance, presses), placed, &mut pager)
            .await;
        let heard = pager.close().await;
        self.three_run().refusals = heard;
        paged
    }

    /// Stage three's interleaving, [`crate::pager::PLAN`] (FBC-4plj): each place of P1 to P3
    /// ([`Run::place_resting`], from the pages registry's own list), each cancel by venue id
    /// ([`Run::cancel_paged`]), each followed by the streams compared with never-arming reads
    /// (0151 item 4), and each page `pager` is asked for ([`Run::page_now`]), then the walk of
    /// every page from offset 0 ([`Run::walk`]). `placed` holds the order placed last and `paged`
    /// every one, as each is handed to the session. When the run saw each order ended, and the
    /// comparisons since, are kept for the pages ([`crate::pager::Ends`]).
    async fn paging(
        &mut self,
        places: &Places,
        view: (&Discovered, &market::TouchView),
        d: &DropCtx,
        (allowance, presses): (&mut Allowance, &mut Presses),
        (placed, paged): (&mut Option<Placed>, &mut Vec<Placed>),
        pager: &mut crate::compare::Socket<'_>,
    ) -> Result<(), Halt> {
        use crate::pager::{Action, CANCELLED, Ends, NAMES, PLACED, PLAN};
        let mut ends = Ends::new(places.agree_wait);
        for action in PLAN {
            match action {
                Action::Place(n) => {
                    let begun = (NAMES[n], places::XRun::default());
                    self.three_run().orders.push(begun);
                    let sent =
                        (self.place_resting(places, view, d, allowance, presses, placed)).await?;
                    paged.push(sent);
                    let step = (PLACED[n], sent.at);
                    (self.compare_paged(d, view.0, (paged, &mut ends), step, presses)).await?;
                }
                Action::Cancel(n) => {
                    let at = self.cancel_paged(d, presses, n, paged[n].cid).await?;
                    let step = (CANCELLED[n], at);
                    (self.compare_paged(d, view.0, (paged, &mut ends), step, presses)).await?;
                }
                Action::Page(offset) => {
                    let paged = (paged.as_slice(), &mut ends);
                    self.page_now(d, presses, pager, paged, (offset, false))
                        .await?;
                }
                Action::Walk => self.walk(d, presses, pager, (paged, &mut ends)).await?,
            }
        }
        Ok(())
    }

    /// The streams of stage three's orders placed so far, `paged`, compared with never-arming
    /// reads after `step`, made at its instant ([`Run::compare_step`]): the comparison's stop the
    /// session's. Each order the run saw ended before it began is past its lag once it has ended
    /// (`ends`, [`crate::pager::Ends::compared`]).
    async fn compare_paged(
        &mut self,
        d: &DropCtx,
        found: &Discovered,
        (paged, ends): (&[Placed], &mut crate::pager::Ends),
        (step, since): (&'static str, tokio::time::Instant),
        presses: &mut Presses,
    ) -> Result<(), Halt> {
        ends.note(&self.paged_ended(d, paged), tokio::time::Instant::now());
        let covered = ends.ended();
        let ours: Vec<fbc_core::ClientOrderId> = paged.iter().map(|p| p.cid).collect();
        let compared = self.compare_step(d, (found, &ours), d.on, step, Some(since));
        let compared = self.unless_stopped(d, presses, compared).await?;
        ends.compared(&covered);
        ends.note(&self.paged_ended(d, paged), tokio::time::Instant::now());
        let stop = compared.0.stop;
        self.compared(Which::Pages, compared);
        match stop {
            Some(stop) => Err(Halt::Stop(stop)),
            None => Ok(()),
        }
    }

    /// Cancels stage three's order `n` of [`crate::pager::NAMES`], `cid`, by venue id, an fbc-oms
    /// authorization through the `Link`, then waits until it ends or `--rpc-timeout-ms` from the
    /// cancel's send, recording the cancel as it goes ([`crate::pager::CancelRun`]). An order
    /// that already ended is not cancelled, and one whose acknowledgement, which a cancel by venue
    /// id needs, has not come is left to the session's Stop, which cancels it once it lands; each
    /// is recorded with why. A cancel fbc-oms or the session refuses is a failure that is no
    /// stop. Gives when the cancel began, which its comparison is timed from.
    async fn cancel_paged(
        &mut self,
        d: &DropCtx,
        presses: &mut Presses,
        n: usize,
        cid: fbc_core::ClientOrderId,
    ) -> Result<tokio::time::Instant, Halt> {
        use crate::pager::{AWAITING_ACK, CancelRun, ENDED_FIRST, NAMES};
        let who = NAMES[n];
        let ended =
            |link: &Link| (link.registry().get(cid)).is_none_or(|r| r.state().is_terminal());
        let built = {
            let mut link = d.link.borrow_mut();
            match ended(&link) {
                true => None,
                false => {
                    let caps = link.caps().clone();
                    Some((link.reg().cancellable(cid)).map(|permit| permit.cancel(&caps)))
                }
            }
        };
        let at = tokio::time::Instant::now();
        let mut record = CancelRun {
            name: who,
            ..CancelRun::default()
        };
        let cmd = match built {
            None => {
                record.not_sent = Some(ENDED_FIRST);
                record.ended = true;
                self.three_run().cancels.push(record);
                return Ok(at);
            }
            Some(Ok(fbc_oms::CancelChoice::AwaitAck)) => {
                record.not_sent = Some(AWAITING_ACK);
                self.three_run().cancels.push(record);
                return Ok(at);
            }
            Some(Err(e)) => {
                let named = places::variant_name(&e);
                return Err(Halt::Error(format!("{who}'s cancel was refused: {named}")));
            }
            Some(Ok(fbc_oms::CancelChoice::Send(cmd))) => cmd,
        };
        let submitted = {
            let mut link = d.link.borrow_mut();
            let auth = link.reg().authorize(d.acct, cmd).map_err(|e| {
                let named = places::variant_name(&e);
                format!("{who}'s cancel was not authorized: {named}")
            });
            auth.and_then(|auth| {
                let submitted = link.submit(&d.orders, auth);
                submitted.map_err(|e| {
                    let named = places::refusal_named(&e);
                    format!("{who}'s cancel was refused by the session: {named}")
                })
            })
        };
        let rpc = submitted.map_err(Halt::Error)?;
        record.sent = true;
        self.three_run().cancels.push(record);
        self.line(format_args!(
            "CANCEL {who} on {}: by venue id, an fbc-oms authorization (no id is shown)",
            name(d.on)
        ));
        let due = at + Duration::from_millis(self.opts.rpc_timeout_ms);
        let waited = self.waiting(d, presses, |link, _| {
            (ended(link) || tokio::time::Instant::now() >= due).then_some(())
        });
        let woken = async {
            tokio::time::sleep_until(due).await;
            d.wake.notify_waiters();
            std::future::pending::<()>().await
        };
        let waited = tokio::select! {
            biased;
            waited = waited => waited,
            () = woken => unreachable!("it wakes the wait, then waits for ever"),
        };
        let (outcome, gone) = {
            let link = d.link.borrow();
            (link.outcome_of(rpc), ended(&link))
        };
        let record = (self.three_run().cancels.last_mut()).expect("the cancel's record");
        record.outcome = outcome.as_ref().map(places::outcome_name);
        record.ended = gone;
        waited?;
        Ok(at)
    }

    /// Stage three's orders placed so far, `paged`, by name, with their venue ids once the
    /// streams named them: what a page is matched to ([`crate::pager::named`]).
    fn paged_ours(&self, d: &DropCtx, paged: &[Placed]) -> Vec<crate::pager::Ours> {
        let link = d.link.borrow();
        (paged.iter().zip(crate::pager::NAMES))
            .map(|(p, name)| crate::pager::Ours {
                name,
                cid: p.cid,
                venue: (link.registry().get(p.cid)).and_then(|r| r.vid().cloned()),
            })
            .collect()
    }

    /// The names of stage three's orders placed so far, `paged`, that the registry holds ended.
    fn paged_ended(&self, d: &DropCtx, paged: &[Placed]) -> Vec<&'static str> {
        let link = d.link.borrow();
        let ended =
            |p: &&Placed| (link.registry().get(p.cid)).is_some_and(|r| r.state().is_terminal());
        let named = paged.iter().zip(crate::pager::NAMES);
        named.filter(|(p, _)| ended(p)).map(|(_, n)| n).collect()
    }

    /// The names of stage three's orders placed so far, `paged`, that the registry holds open.
    fn paged_open(&self, d: &DropCtx, paged: &[Placed]) -> Vec<&'static str> {
        let link = d.link.borrow();
        let open = |p: &&Placed| {
            let state = link.registry().get(p.cid).map(|r| r.state());
            matches!(state, Some(OrdState::Open | OrdState::PartiallyFilled))
        };
        let named = paged.iter().zip(crate::pager::NAMES);
        named.filter(|(p, _)| open(p)).map(|(_, n)| n).collect()
    }

    /// Asks `pager` for the page at `offset` (one of the walk's when `walk`) and records it
    /// ([`crate::pager::PageRead`]): its size and offset, which of stage three's orders, `paged`,
    /// the streams showed open as it was asked, what it listed by name, and its frame for the
    /// slice; the pager's refusals kept as they go. A provider rate refusal on the pager is the
    /// run's stop, [`Stop::RateRefused`] on [`places::RefusedOn::Pager`], and so is a page listing
    /// an order that is none of `paged`, or a TWAP, [`Stop::ReadDisagrees`], each recorded first
    /// (decision 0148 item 8), and then a page listing one of `paged` that the streams showed
    /// ended as it was asked, past its lag (`ends`, [`crate::pager::Ends::past`]), as
    /// `compare::compare` stops on such a read at the end of its wait; one within its lag is
    /// recorded and stops nothing. Any other page that could not be made is recorded and stops
    /// nothing. Gives what it came to.
    async fn page_now(
        &mut self,
        d: &DropCtx,
        presses: &mut Presses,
        pager: &mut crate::compare::Socket<'_>,
        (paged, ends): (&[Placed], &mut crate::pager::Ends),
        (offset, walk): (u32, bool),
    ) -> Result<crate::compare::PageAttempt, Halt> {
        let open = self.paged_open(d, paged);
        let asked = tokio::time::Instant::now();
        let ended = self.paged_ended(d, paged);
        ends.note(&ended, asked);
        let past = ends.past(asked);
        self.three_run().paged = true;
        let attempt = self.unless_stopped(d, presses, pager.page(offset)).await;
        self.three_run().refusals = pager.heard;
        let attempt = attempt?;
        ends.note(&self.paged_ended(d, paged), tokio::time::Instant::now());
        let ours = self.paged_ours(d, paged);
        let (came, stop) = crate::pager::came(&attempt, &ours);
        let at = self.three_run().pages.len();
        let (listed, stale) = crate::pager::ended_listed(&came, at, (&ended, &past));
        let stop = stop.or(stale);
        self.three_run().ended_listed.extend(listed);
        if let crate::compare::PageAttempt::Read { kept, .. } = &attempt {
            self.three_run().frames.push(kept.clone());
        }
        let said = {
            let run = self.three_run();
            let ended: Vec<&crate::pager::EndedListed> =
                run.ended_listed.iter().filter(|l| l.page == at).collect();
            crate::pager::ended_said(&ended)
        };
        self.line(format_args!(
            "PAGE {}at size {}, offset {offset}, on {}: {}{said}",
            if walk { "of the walk " } else { "" },
            crate::pager::PAGE_SIZE,
            name(d.on),
            came.says()
        ));
        self.three_run().pages.push(crate::pager::PageRead {
            walk,
            size: crate::pager::PAGE_SIZE,
            offset,
            open,
            came,
        });
        match stop {
            Some(stop) => Err(Halt::Stop(stop)),
            None => Ok(attempt),
        }
    }

    /// The walk (FBC-4plj): `pager` asked for every page from offset 0 until a short one, a full
    /// page listing an order an earlier one of the walk listed, or a page that could not be made
    /// ([`crate::pager::walk_end`]), each recorded by [`Run::page_now`], all within
    /// [`crate::pager::WALK_PATIENCE`] of its first page, so a venue answering new full pages for
    /// ever cannot hold the run open.
    async fn walk(
        &mut self,
        d: &DropCtx,
        presses: &mut Presses,
        pager: &mut crate::compare::Socket<'_>,
        (paged, ends): (&[Placed], &mut crate::pager::Ends),
    ) -> Result<(), Halt> {
        use crate::pager::{PAGE_SIZE, WALK_PATIENCE, WalkEnd, walk_end};
        let until = tokio::time::Instant::now() + WALK_PATIENCE;
        let mut seen: Vec<fbc_core::VenueOrderId> = Vec::new();
        let mut offset = 0;
        let end = loop {
            let page = self.page_now(d, presses, pager, (paged, &mut *ends), (offset, true));
            let Ok(page) = tokio::time::timeout_at(until, page).await else {
                break WalkEnd::Cut;
            };
            let page = match page {
                Ok(page) => page,
                Err(halt) => {
                    // The page itself, recorded, listed an order or a TWAP not ours, or one of
                    // ours the streams showed ended past its lag: its stop.
                    let disagreed = crate::pager::last_disagreed(self.three_run());
                    let end = match disagreed {
                        true => WalkEnd::Disagreed,
                        false => WalkEnd::NotRead,
                    };
                    self.three_run().walk = Some(end);
                    return Err(halt);
                }
            };
            if let Some(end) = walk_end(&page, &seen) {
                break end;
            }
            if let crate::compare::PageAttempt::Read { listed, .. } = &page {
                seen.extend(listed.iter().map(|l| l.order.clone()));
            }
            offset += u32::from(PAGE_SIZE);
        };
        self.three_run().walk = Some(end);
        let walked = self.three_run().pages.iter().filter(|p| p.walk).count();
        self.line(format_args!(
            "WALK on {}: {walked} pages from offset 0, ended by {}",
            name(d.on),
            end.says()
        ));
        Ok(())
    }

    /// [`crate::stage_two::no_other_order`] for `inst` now: every registry the run built on every
    /// named account, X and Y on a drop registry counted ended only as 0151 item 5 rules
    /// ([`crate::stage_two::ended_after_drop`]), and each named account's read of `reads`, the
    /// reads before the session, in the order the configuration names them.
    fn no_other_order(
        &self,
        inst: fbc_core::InstrumentId,
        reads: &[AccountRead],
    ) -> Result<(), two::Blocked> {
        let links: Vec<std::cell::Ref<'_, Link>> =
            self.built.iter().map(|b| b.link.borrow()).collect();
        let ended: Vec<Vec<fbc_core::ClientOrderId>> = (self.built.iter())
            .map(|b| {
                let drop = self.ev.drops.iter().find(|d| d.account == name(b.on));
                let shown = drop.and_then(|d| d.two.as_ref());
                let shown = shown.map_or([false; 2], two::ended_after_drop);
                let ended = b.xy.iter().zip(shown).filter(|(_, e)| *e);
                ended.map(|(cid, _)| *cid).collect()
            })
            .collect();
        let built: Vec<two::Built<'_>> = (self.built.iter().zip(&links).zip(&ended))
            .map(|((b, link), ended)| two::Built {
                account: name(b.on),
                step: b.step,
                registry: link.registry(),
                ended,
            })
            .collect();
        let reads: Vec<(&'static str, Option<&AccountRead>)> = (self.named.iter().enumerate())
            .map(|(n, a)| (name(*a), reads.get(n)))
            .collect();
        two::no_other_order(inst, &built, &reads)
    }

    /// A test's order of ours, never sent, held in the first named account's cross registry
    /// before the `n`th session `which` of stage two's [`Run::no_other_order`], `n` its
    /// `TestHooks::other_order` for a crossing session or `TestHooks::take_other_order` for a
    /// taker IOC session (0 the first named account's): a path no answer of the QFEX model
    /// reaches, since the reads before each session and the stop an order left resting after one
    /// is keep every such order away first.
    #[cfg(test)]
    async fn test_other_order(&mut self, places: &Places, found: &Discovered, which: Which) {
        let two = self.ev.stage_two.as_ref();
        let (n, hook) = match which {
            Which::Take => (
                two.map_or(0, |t| t.takes.len() - 1),
                places.test.take_other_order,
            ),
            _ => (
                two.map_or(0, |t| t.crosses.len() - 1),
                places.test.other_order,
            ),
        };
        if hook != Some(n) {
            return;
        }
        let leases = self.leases.as_mut().expect("a place-stage run's leases");
        let cid = leases.mint.mint(1).await.expect("a client id")[0];
        let first = self.built.iter().find(|b| b.step == Step::Cross);
        let new = fbc_core::NewOrder {
            cid,
            inst: found.inst,
            side: places.side,
            qty: found.min_size,
            kind: fbc_core::OrderKind::Limit {
                px: fbc_core::Ticks(1),
            },
            tif: fbc_core::Tif::Gtc,
            channel: fbc_core::Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        let mut link = first.expect("a cross registry").link.borrow_mut();
        link.reg().insert(new).expect("a fresh client id registers");
    }

    /// Places the crossing post-only on `d`'s session: a post-only GTC at the instrument's
    /// minimum size at the far touch (the ask for a buy, the bid for a sell), through it, spent
    /// from the cross registry's list first ([`Allowance`]), its client id minted under the
    /// run's namespace lease, as an fbc-oms authorization (decision 0132 item 7). Then its first
    /// answer, its time to an `ACK` timed, and then its end or its deadline (`--rpc-timeout-ms`
    /// from its send): an order that rests is waited on to its deadline, and one never answered
    /// ends `Unknown` there unless an echoed `ACK` already routed it, never resent (decision
    /// 0005). Before it is built into a command, both USD caps are asked again at the touch it
    /// is priced at ([`market::caps_admit`]); an order either would not admit is not sent, and
    /// never resized, the run's stop [`Stop::BeyondCaps`] (decision 0113). Gives it, as placed,
    /// once handed to the session (`placed` holds it from then); a stop or a press first places
    /// nothing.
    async fn place_crossing(
        &mut self,
        places: &Places,
        (found, touch): (&Discovered, &market::TouchView),
        d: &DropCtx,
        allowance: &mut Allowance,
        presses: &mut Presses,
        placed: &mut Option<Placed>,
    ) -> Result<Placed, Halt> {
        allowance.take(Step::Cross, ledger::THROUGH)?;
        let leases = self.leases.as_mut().expect("a place-stage run's leases");
        let cid = leases.mint.mint(1).await.map_err(Halt::Error)?[0];
        let quote = touch.now(places.touch_max_age).map_err(Halt::Error)?;
        let px = market::far_touch(&found.spec, places.side, quote).map_err(Halt::Error)?;
        let new = fbc_core::NewOrder {
            cid,
            inst: d.inst,
            side: places.side,
            qty: found.min_size,
            kind: fbc_core::OrderKind::Limit { px },
            tif: fbc_core::Tif::Gtc,
            channel: fbc_core::Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        };
        self.not_stopped(d, presses)?;
        // The caps asked again at the touch the crossing post-only is priced at, as for X and Y:
        // fbc-oms holds them in lots fixed at the preflight's touch (decision 0113).
        if !market::caps_admit(places, found, quote, d.caps, found.min_size) {
            return Err(Halt::Stop(Stop::BeyondCaps));
        }
        let submitted = {
            let mut link = d.link.borrow_mut();
            let cmd = link.reg().place(new).map_err(|e| {
                format!(
                    "the crossing post-only was refused: {}",
                    places::variant_name(&e)
                )
            });
            let auth = cmd.and_then(|cmd| {
                let auth = link.reg().authorize(d.acct, cmd);
                auth.map_err(|e| {
                    format!(
                        "the crossing post-only was not authorized: {}",
                        places::variant_name(&e)
                    )
                })
            });
            auth.and_then(|auth| link.submit(&d.orders, auth))
        };
        let rpc = submitted.map_err(Halt::Error)?;
        let now = Placed {
            cid,
            rpc,
            at: tokio::time::Instant::now(),
            wall_secs: u64::try_from(wall_secs()).unwrap_or(0),
        };
        *placed = Some(now);
        self.cross_run().placed.submitted = true;
        self.line(format_args!(
            "PLACE the crossing post-only on {}: one post-only GTC at the instrument's minimum \
             size at the far touch, through it (no price, size or id is shown)",
            name(d.on)
        ));
        // Its first answer: no longer in flight.
        let state = |link: &Link| link.registry().get(cid).map(|r| r.state());
        self.waiting(d, presses, |link, _| {
            (state(link) != Some(OrdState::PendingNew)).then_some(())
        })
        .await?;
        let rested = matches!(
            state(&d.link.borrow()),
            Some(OrdState::Open | OrdState::PartiallyFilled)
        );
        let ack_after = now.at.elapsed();
        self.line(format_args!(
            "ANSWERED the crossing post-only on {}: {}",
            name(d.on),
            match rested {
                true => "it rests, so it is waited on until it ends or its deadline",
                false => "it does not rest",
            }
        ));
        // It is ours whatever client id its later frames carry (decision 0132 item 7).
        let vid = (d.link.borrow().registry().get(cid)).and_then(|r| r.vid().cloned());
        if let Some(vid) = vid {
            self.tee.ours(vid.as_str());
        }
        // Its end, or its deadline: one that rests is waited on until then.
        let due = now.at + Duration::from_millis(self.opts.rpc_timeout_ms);
        let ended = self.waiting(d, presses, |link, _| match state(link) {
            Some(OrdState::Open | OrdState::PartiallyFilled)
                if tokio::time::Instant::now() < due =>
            {
                None
            }
            _ => Some(()),
        });
        let woken = async {
            tokio::time::sleep_until(due).await;
            d.wake.notify_waiters();
            std::future::pending::<()>().await
        };
        tokio::select! {
            biased;
            ended = ended => ended?,
            () = woken => unreachable!("it wakes the wait, then waits for ever"),
        }
        // Its own ACK alone, by its client id (PR #284's review).
        let text = two::cid_text(cid).unwrap_or_default();
        let exact = self.tee.kept().exact_ack_of(d.heard_from, &text);
        let acked_ours = self.tee.kept().acked_of(d.heard_from, &text);
        let outcome = d.link.borrow().outcome_of(rpc);
        let run = &mut self.cross_run().placed;
        run.acked = rested || acked_ours;
        run.outcome = outcome.as_ref().map(places::outcome_name);
        run.exact_echo = exact;
        run.ack_after = run.acked.then_some(ack_after);
        Ok(now)
    }
}

/// What a stop's or a flatten's cancels, `c`, came to, as their line says it.
fn cancels_said(c: &Cancels) -> &'static str {
    match (c.held, c.ended) {
        (true, _) => "held",
        (false, true) => "every order they named seen to end",
        (false, false) => "not every order they named seen to end in time",
    }
}

/// The reads `reads` a flatten judges a flatten IOC by, the first listing one order more when a
/// test asks it to ([`crate::probe_args::TestHooks::listed_in_flatten`]).
#[cfg(test)]
fn test_listed(places: &Places, mut reads: Vec<AccountRead>) -> Vec<AccountRead> {
    if let Some(first) = reads.first_mut().filter(|_| places.test.listed_in_flatten) {
        first.page_orders += 1;
    }
    reads
}

/// The stop the reads after a stage-two or stage-three session of the kind `session` names make
/// (the crossing session, FBC-su9i; the taker IOC session, FBC-ou9g; stage three's, FBC-4plj),
/// and when it was met, said as met after that session, the first that applies: the provider
/// rate refusal they met, `refused`; the owner's Ctrl-C during them, `pressed`; a position not
/// flat; an order of ours still resting, `resting`, whether a read listed it or the session's
/// registry counts it resting, an `Unknown` one included, never resent (decision 0005): an
/// order of ours still resting after a session stops the probe (decision 0132 item 7); then a
/// named account that could not be read, `unread`, after an order was placed on one: nobody
/// looked at it, so an order of ours may still rest there unjudged, as the read before drop
/// session two takes an account it could not read (`LeftResting`; before every session an
/// account that cannot be read stops the probe, decision 0132 item 7). Each is marked (0151 item
/// 6), so the next run must reconcile. None when they show none of them.
pub fn stage_read_stop(
    session: two::Session,
    (refused, pressed): (Option<Stop>, bool),
    (not_flat, resting, unread): (bool, bool, bool),
) -> Option<(Stop, &'static str)> {
    let (after, in_reads, during, not_read) = match session {
        two::Session::Cross => (
            AFTER_CROSS,
            "in the reads after the crossing session",
            "during the reads after the crossing session",
            "after the crossing session, a named account not read after it",
        ),
        two::Session::Take => (
            AFTER_TAKE,
            "in the reads after the taker IOC session",
            "during the reads after the taker IOC session",
            "after the taker IOC session, a named account not read after it",
        ),
        two::Session::Pages => (
            AFTER_PAGES,
            "in the reads after the stage-three session",
            "during the reads after the stage-three session",
            "after the stage-three session, a named account not read after it",
        ),
    };
    if let Some(stop) = refused {
        return Some((stop, in_reads));
    }
    match (pressed, not_flat, resting, unread) {
        (true, ..) => Some((Stop::Interrupted, during)),
        (false, true, ..) => Some((Stop::NotFlat, after)),
        (false, false, true, _) => Some((Stop::LeftResting, after)),
        (false, false, false, true) => Some((Stop::LeftResting, not_read)),
        (false, false, false, false) => None,
    }
}

/// The stop the reads after a stage-two or stage-three session make ([`Run::cross_test`]), of
/// the kind `session` names: [`stage_read_stop`]'s,
/// with [`Stop::ReadDisagrees`] for an order or a TWAP not ours, `unexpected`, after a provider
/// rate refusal, the owner's Ctrl-C and a position not flat, and before an order of ours resting
/// (`resting`: listed by a read, counted by REST and listed by no page, or counted resting by the
/// session's registry) or a named account not read (`unread`), as [`after_stop`] orders them
/// after drop session two (decision 0148 item 8; FBC-77r8's review).
pub fn stage_after_stop(
    session: two::Session,
    first: (Option<Stop>, bool),
    (not_flat, unexpected): (bool, bool),
    (resting, unread): (bool, bool),
) -> Option<(Stop, &'static str)> {
    let shown = stage_read_stop(session, first, (not_flat, false, false));
    let others = match session {
        two::Session::Cross => CROSS_OTHERS,
        two::Session::Take => TAKE_OTHERS,
        two::Session::Pages => crate::pager::PAGES_OTHERS,
    };
    let unexpected = unexpected.then_some((Stop::ReadDisagrees, others));
    let rest = || stage_read_stop(session, (None, false), (false, resting, unread));
    (shown.or(unexpected)).or_else(rest)
}

/// What a stage-two session's `stop` says in place of [`Stop::says`] ([`Run::halt_as`]): for a
/// [`Stop::LeftResting`] that no read listing an order of ours made (`listed`), an order the
/// session's registry counts resting (`counted`, [`places::COUNTED_BY_REGISTRY`]), or else a
/// named account not read ([`places::COUNTED_UNREAD`]): counted resting, never seen resting (the
/// owner's ruling under 0151 item 6 on FBC-gisx). `None`, [`Stop::says`], otherwise.
pub fn stage_counted_says(stop: Stop, listed: bool, counted: bool) -> Option<&'static str> {
    match (stop, listed, counted) {
        (Stop::LeftResting, false, true) => Some(places::COUNTED_BY_REGISTRY),
        (Stop::LeftResting, false, false) => Some(places::COUNTED_UNREAD),
        _ => None,
    }
}

/// The stop a session outside the drop test ends the run with before the reads after it
/// ([`Run::cross_test`]: the crossing session's, the taker IOC session's and stage three's
/// alike), and what it says in place of [`Stop::says`], given `ran`, the stop its steps, its
/// flatten, its Stop's cancels or its close raised, `kept_back`, whether the check before its
/// order kept that order back, and `cancels`, its Stop's cancels when they ran. The first stop
/// is the run's: `ran`, its [`Stop::LeftResting`] said as [`places::COUNTED_BLOCKED`] when the
/// check kept its order back. With none, cancels refused locally or met by a provider rate
/// refusal ([`Cancels::blocked`], 0151 item 11's two causes) stop the run in their own right,
/// [`Stop::CancelsBlocked`] (0151 item 11; FBC-3xzn), which says nothing of an order resting
/// (the owner's ruling under 0151 item 6). A provider rate refusal also raises
/// [`Stop::RateRefused`] through the tee, which comes first as `ran`. `None` when neither came:
/// the reads after the session decide ([`stage_after_stop`]).
pub fn stage_session_stop(
    ran: Option<Stop>,
    kept_back: bool,
    cancels: Option<&Cancels>,
) -> Option<(Stop, Option<&'static str>)> {
    match (ran, cancels.is_some_and(|c| c.blocked)) {
        (Some(stop), _) => {
            let says = (stop == Stop::LeftResting && kept_back).then_some(places::COUNTED_BLOCKED);
            Some((stop, says))
        }
        (None, true) => Some((Stop::CancelsBlocked, None)),
        (None, false) => None,
    }
}

/// When the taker IOC's trades read after its session held a row naming none of its orders, or
/// none at all (decision 0148 item 8; PR #284's review).
pub const TAKE_TRADES_OTHERS: &str = "reading the taker IOC's trades after the taker IOC session, \
     a get_user_trades row naming no order of ours";

/// When the reads after a taker IOC session listed an order or a TWAP not ours.
pub const TAKE_OTHERS: &str =
    "after the taker IOC session, a read listing an order or a TWAP not ours";

/// When the reads after a crossing session listed an order or a TWAP not ours.
pub const CROSS_OTHERS: &str =
    "after the crossing session, a read listing an order or a TWAP not ours";

/// What the after-session read of every named account showed ([`Run::read_after`]).
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct AfterReads {
    /// Whether an account held an open order (on the page, or counted by REST).
    pub resting: bool,
    /// Whether an account held an open order of ours: one its page names among the orders the
    /// reads know, or one REST counts and no page lists, which is not told from ours.
    pub ours_resting: bool,
    /// Whether an account's page listed an order that is none of the drop test's, or a TWAP:
    /// what the streams did not show (decision 0148 item 8, FBC-77r8's review).
    pub unexpected: bool,
    /// Whether an account held a position that is not flat, or, after drop session two failed
    /// with no stop, a trade named X or Y.
    pub not_flat: bool,
    /// Whether X was listed on its account's page.
    pub x_listed: bool,
    /// Whether the drop test's account was read.
    pub read: bool,
    /// Whether the drop test's account was read, on a page that listed X or was its last.
    pub x_read: bool,
    /// Whether X's account was read and its position was flat.
    pub x_flat: bool,
    /// Whether a named account could not be read.
    pub unread: bool,
    /// Why the session's own account could not be read, when its read failed with no provider
    /// rate refusal (FBC-gisx): the step that failed.
    pub on_unread: Option<places::AccountUnread>,
    /// Whether the owner pressed Ctrl-C while they ran.
    pub pressed: bool,
    /// The provider rate refusal they met, if any: a stop ([`Run::read_after`]).
    pub refused: Option<Stop>,
}

impl AfterReads {
    /// Whether the reads after a crossing session's drop read every named account again
    /// ([`Run::read_after_stage_drop`]): only while the round they judge lists an order of ours
    /// and shows nothing that ends them, no order or TWAP not ours, no position not flat, no
    /// account it could not read, no provider rate refusal and no press of Ctrl-C. A position
    /// not flat ends them too: it stops the run, and the reads after it learn nothing more of it
    /// (FBC-77r8's review).
    pub fn read_again(&self) -> bool {
        let ends = self.unexpected || self.not_flat || self.unread || self.pressed;
        self.ours_resting && !ends && self.refused.is_none()
    }

    /// Folds `next`, a later round of [`Run::read_after_stage_drop`], into these: a round that
    /// is `judged` (it finished within the wait and read every named account) replaces what an
    /// earlier one showed of our orders, since QFEX cancels on a drop only once it learns of it;
    /// one that is not judged replaces nothing, and adds what it showed. Whatever either showed
    /// of a provider rate refusal, a press of Ctrl-C, an order or a TWAP not ours or a position
    /// not flat is kept, as the reads after drop session two keep what a read showed whatever
    /// a later one shows (FBC-77r8's review).
    pub fn fold(&mut self, next: AfterReads, judged: bool) {
        let earlier = *self;
        if judged {
            *self = next;
        } else {
            self.resting |= next.resting;
            self.ours_resting |= next.ours_resting;
            self.x_listed |= next.x_listed;
            self.unread |= next.unread;
        }
        self.refused = earlier.refused.or(next.refused);
        self.pressed = earlier.pressed || next.pressed;
        self.unexpected = earlier.unexpected || next.unexpected;
        self.not_flat = earlier.not_flat || next.not_flat;
    }
}

/// The line a stop ends the run with ([`Run::halt_as`]): the run's stop, `first`, by name, met
/// `when`, then what it is, `says` when `stop` is the run's and [`Stop::says`] otherwise.
pub fn stop_said(first: Stop, stop: Stop, when: &str, says: Option<&str>) -> String {
    let says = says.filter(|_| first == stop).unwrap_or(first.says());
    format!("{} {when}: {says}; no further session runs", first.name())
}

/// Whether `link`'s registry holds the order `cid` as ended, so the streams showed its end
/// (decision 0005): one it does not hold, or holds in any other state, an `Unknown` one included,
/// did not (FBC-gisx).
fn ended_on(link: &Link, cid: fbc_core::ClientOrderId) -> bool {
    (link.registry().get(cid)).is_some_and(|r| r.state().is_terminal())
}

/// What the reads' stop after a drop session two that reached its end says in place of
/// [`Stop::says`]'s (FBC-8mwz): a [`Stop::LeftResting`] when no read listed an order resting,
/// `resting` false, is a named account not read, said as [`places::COUNTED_UNREAD`], counted
/// resting, not seen resting (the ruling under 0151 item 6 on FBC-gisx). One a read listed, and
/// every other stop, is said as itself: `None`.
pub fn drop_two_says(stop: Stop, resting: bool) -> Option<&'static str> {
    (stop == Stop::LeftResting && !resting).then_some(places::COUNTED_UNREAD)
}

/// The stop the reads after drop session two make, given `after` and whether the owner pressed
/// Ctrl-C during the reads after the drop, or during X's and Y's trades read after a failure
/// (`pressed`): [`read_stop`]'s, with [`Stop::ReadDisagrees`] for an order none of ours or a
/// TWAP after a position not flat and before an order of ours resting, and a named account not
/// read, `unread`, last (FBC-8mwz). The caller passes `unread` false after a failed drop session
/// two, where an account not read counts only the orders whose end nothing showed (FBC-gisx).
pub fn after_stop(after: &AfterReads, pressed: bool, unread: bool) -> Option<(Stop, &'static str)> {
    let pressed = pressed || after.pressed;
    // After a position not flat and before an order of ours still resting: an order or a TWAP
    // the streams did not show (decision 0148 item 8).
    let shown = read_stop(after.refused, pressed, after.not_flat, false, false);
    let unexpected = (after.unexpected).then_some((Stop::ReadDisagrees, "after drop session two"));
    let rest = || read_stop(None, false, false, after.resting, unread);
    (shown.or(unexpected)).or_else(rest)
}

/// Whether `read`, the stop the reads after drop session two make, outranks `judged`, the
/// verdict's, in the verdict's own order ([`crate::verdict::DropVerdict::stop`]): a position
/// not flat outranks an order or a TWAP not ours, an arm-time cancel QFEX did not report and a
/// test that only did not pass; an order or a TWAP not ours outranks the last two (FBC-77r8's
/// review). Nothing else outranks the verdict's stop.
fn outranks(read: Option<Stop>, judged: Stop) -> bool {
    let after = |s: Stop| matches!(s, Stop::ArmCancelUnreported | Stop::DropFailed);
    match read {
        Some(Stop::NotFlat) => judged == Stop::ReadDisagrees || after(judged),
        Some(Stop::ReadDisagrees) => after(judged),
        _ => false,
    }
}

/// The stop the reads after drop session two make, and when it was met, the first that applies:
/// the provider rate refusal they met, `refused`; the owner's Ctrl-C during them, `pressed`; a
/// position not flat; an order of ours still resting, for the owner's hand cancel (decision
/// 0132 item 7); then a named account that could not be read, `unread` (FBC-8mwz): nobody
/// looked at it after the session, so an order of ours may still rest there unjudged, as
/// [`stage_read_stop`] takes one after a stage-two session (`LeftResting`; an account that
/// cannot be read stops the probe, decision 0132 item 7). Each is marked (0151 item 6), so the
/// next run must reconcile. None when they show none of them.
pub fn read_stop(
    refused: Option<Stop>,
    pressed: bool,
    not_flat: bool,
    resting: bool,
    unread: bool,
) -> Option<(Stop, &'static str)> {
    const AFTER: &str = "after drop session two";
    if let Some(stop) = refused {
        return Some((stop, "in the reads after drop session two"));
    }
    match (pressed, not_flat, resting, unread) {
        (true, ..) => Some((Stop::Interrupted, "during the reads after drop session two")),
        (false, true, ..) => Some((Stop::NotFlat, AFTER)),
        (false, false, true, _) => Some((Stop::LeftResting, AFTER)),
        (false, false, false, true) => Some((Stop::LeftResting, NOT_READ_AFTER_TWO)),
        (false, false, false, false) => None,
    }
}

/// When the reads after drop session two could not read a named account ([`read_stop`],
/// FBC-8mwz).
pub const NOT_READ_AFTER_TWO: &str = "after drop session two, a named account not read after it";

/// A comparison that made no read, since its socket could not be set up: `why` is not shown.
fn unread(step: &'static str, _why: String) -> crate::compare::Compared {
    crate::compare::Compared {
        step,
        reads: Vec::new(),
        first_agreed: None,
        ended: crate::compare::Ended::Unread,
        stop: None,
    }
}

/// What the streams of a drop session show of the account on `inst` ([`crate::compare`]): each
/// of `ours`, the orders its registry placed, by client id and venue id, and where it stands,
/// and the registry's position in the venue's units.
fn streams_of(
    link: &Link,
    inst: fbc_core::InstrumentId,
    found: &Discovered,
    ours: &[fbc_core::ClientOrderId],
) -> crate::compare::Streams {
    use crate::compare::{Ours, Seen, Streams};
    let reg = link.registry();
    let orders = (ours.iter().filter_map(|cid| reg.get(*cid)))
        .map(|r| Ours {
            cid: r.cid(),
            venue: r.vid().cloned(),
            seen: match r.state() {
                OrdState::PendingNew | OrdState::Unknown => Seen::InFlight,
                OrdState::Open | OrdState::PartiallyFilled => Seen::Open,
                OrdState::Terminal(_) => Seen::Ended,
            },
        })
        .collect();
    let step = found.spec.size_step.get();
    Streams {
        orders,
        position: rust_decimal::Decimal::from(reg.inventory(inst).0) * step,
    }
}
