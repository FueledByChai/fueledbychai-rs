# 0144 — Stage one of the QFEX probe reads the primary account and the subaccounts its configuration names and sends no GET /user/subaccounts, the owner confirming before each run that no other subaccount exists, amending 0132 item 7

Status: accepted
Date: 2026-11-12

## Context

0132 item 7's "Its credentials" clause has stage one of the QFEX probe read every account of the
provider user: the primary account, which `GET /user/subaccounts` leaves out, and every
subaccount that endpoint lists. The account it trades must be the primary account or a listed
subaccount. FBC-l931 built the probe that way, and 0143 kept every check of item 7 for a
production run.

**What happened.** FBC-291a's first production run of stage one (0143) stopped at its first
request. `GET /user/subaccounts` on `https://api.qfex.com` answered HTTP 403, "insufficient API
key permissions", to the primary account's key. No trade socket had opened, and nothing armed.

**The keys.** QFEX's [API keys page](https://docs.qfex.com/api-reference/api-keys) limits each
key by its account access and by its permissions, and a request must satisfy both.

- Account access is either "A specific account", the selected primary account or subaccount
  only, or "All accounts", any account the user owns.
- The permissions are Execute orders, View orders, View positions, View balance, and Deposit and
  withdraw, which only an All-accounts key can hold.
- The owner keeps one key per account, each scoped to that one account, so that each can be
  revoked alone. The primary account's key already held every permission a single-account key
  can (FBC-5ph8).

**Who may list.** No page names a permission that lists subaccounts.

- The [managed-account page](https://docs.qfex.com/qfex/managed-account) says All accounts
  access "grants the required `subaccount_ops` permission" for its own calls, which create a
  managed account and move funds with `POST /user/transfer`.
- The [subaccounts API page](https://docs.qfex.com/api-reference/subaccounts-api) groups the
  listing with `POST /user/subaccounts`, which creates a subaccount, and `POST /user/transfer`,
  which moves a balance between two of the user's accounts.
- Read together, listing appears to need an All-accounts key, which carries `subaccount_ops`.
  Such a key may then also be able to create subaccounts and move funds between the user's own
  accounts. This is read from the docs. Neither QFEX nor a run has confirmed it, and the key
  page offers `subaccount_ops` as no setting of its own.
- Two more reads list the user's accounts: `GET /user/subaccounts/balance`
  ([list account balances](https://docs.qfex.com/api-reference/rest/user/list-account-balances))
  and `GET /user/subaccounts/equity`
  ([list account equities](https://docs.qfex.com/api-reference/rest/user/list-account-equities)),
  each returning "the authenticated user's primary account and each secondary account". No page
  ties either to a permission, and no run has tried either with a single-account key. They are
  untested routes to a listing, not ones this record rules out.

**What other code does.** The FueledByChaiTrading Java library's QFEX adapter takes one
configured account id (`qfex.account.id`, in its `QfexConfiguration`) and an optional
cancel-on-disconnect flag. It lists no subaccounts.

**The owner's choice.** On 2026-11-12 the owner chose to drop the listing. The probe reads the
accounts its configuration names, and the owner confirms in QFEX's app that the user has no
other subaccount (FBC-5ph8).

## Decision

1. **The accounts the probe reads.** Stage one reads the primary account and each subaccount its
   configuration names: today the one `QFEX_SUBACCOUNT_ID` names, and later each numbered slot
   FBC-e937 would add.
   - Each is read with credentials that reach it, as now: its own pair where one is set, and the
     primary account's where it has none (`crates/fbc-examples/src/auth_qfex.rs`). Its REST
     reads select a subaccount as 0132 item 5's do.
   - The probe never sends `GET /user/subaccounts`.
   - The account it arms is the primary account or a named subaccount. `--arm subaccount` arms
     the one `QFEX_SUBACCOUNT_ID` names, so this holds by construction, with no listing to check
     it against.
   - Each named account's configuration passes the probe mode's guard and 0122 item 1's check
     before anything connects, whether or not a session arms on it.
   - `QFEX_SUBACCOUNT_ID` names a subaccount. 0122 item 1 admits `primary` as an account id, but
     a subaccount slot selecting the primary account is refused before anything connects, naming
     the variable. Otherwise the primary account would be read twice, counted as two accounts,
     and armed under the subaccount's account key, and no subaccount would be read.
   - This replaces the sentences of 0132 item 7's "Its credentials" that name the listing, for
     stage one (item 6). The rest of that clause stands: if any account cannot be read, the
     probe does not start.
   - 0132 item 7's "Its origins" names the account listing among the probe's own requests. That
     no longer applies to stage one, which makes none. The rest of that clause stands.
2. **The owner's confirmation.** Before each run, the owner confirms in QFEX's app that the user
   has no subaccount the configuration does not name. The probe can neither check nor record
   this, and its report and evidence say so: they state it as a condition of the run, not as
   something that happened.
3. **What the report and the evidence say.**
   - The accounts came from the configuration, not from QFEX: how many, and their names
     (`primary`, `subaccount`), never an id.
   - The probe sent no listing, and checked no unnamed subaccount.
   - 0132's Consequences ask whether every other account of the user still held no open order
     and a flat position at item 7's checks. The answer covers the named accounts only.
4. **Unchanged.**
   - Every other part of 0132 item 7. Before every session, `get_user_orders` is empty and the
     REST positions are flat on every account the probe reads. One arm is in flight at a time.
     The refusals before anything connects, and the seed, are as they were.
   - 0122, and 0143 but for the statements item 5 reads anew.
   - The adapter's `GET /user/subaccounts` reader in `src/account_reads.rs` (`subaccounts`,
     `decode_subaccounts`, `Subaccounts`) stays as it is, with its tests. The probe does not
     use it. It is kept for a later tool whose key may list.
5. **Statements of 0132 and 0143 that rested on the listing**, as they now read for stage one.
   Each "every account of the user" becomes every account the configuration names, which the
   probe reads, with the user's other accounts covered by the owner's confirmation (item 2).
   - 0143's Context: "The probe starts a session only when every account of the user shows no
     open order and flat positions (item 7)."
   - 0143's Alternative that takes no acknowledgement flag beside `--production`, because
     "before every session it reads every account of the user". Its reason holds for the named
     accounts. For any other it rests on the owner's confirmation, and this record takes no
     flag for that either (Alternatives).
   - 0143 item 3's "Every check 0132 item 7 makes runs unchanged" means those checks as this
     record amends them.
   - 0132 item 4: the drop test does not measure whether an arm or a drop cancels another
     account's orders, "since item 7 lets no order rest on any other account while a session
     runs". For a named account that holds by the reads. For any other it holds by the owner's
     confirmation.
6. **Not decided here.**
   - Stages two and three (FBC-9sk2). 0132 item 7's "Its credentials" covers the whole probe, and
     this record replaces its listing sentences for stage one only. Whether the place stages
     list the user's accounts, with a key that may, or read the named accounts as stage one
     does, is left to FBC-9sk2's record. So is what stage two's places "against no order of any
     account of the user" then rest on.
   - A flag recording the owner's confirmation. The owner may still ask for one.

## Alternatives

- **A separate All-accounts key used only for the listing, created before each run and revoked
  after it:** the owner declined. It is an extra key on every run, with wider powers than the
  one read needs.
- **Widening the primary trading key to All accounts:** rejected. One key could then trade every
  account of the user. A subaccount with no pair of its own would be read, and armed, with it
  silently, where a scoped key's refusal now stops the run.
- **Asking QFEX to let a single-account key list subaccounts:** not taken now, since it waits on
  QFEX. It may still be asked, and a record can bring the listing back if QFEX allows it.
- **Keeping the listing, and falling back to the configured accounts when it is refused:**
  rejected. That is two paths, and a listing that may be absent proves nothing: a run where it
  is refused rests on the owner's confirmation anyway.
- **A required flag recording the owner's confirmation** (say `--no-other-subaccounts`, written
  to `evidence.json`): not taken. A flag shows only that it was typed, not that the app was
  checked, and the report and evidence already say the probe records no confirmation. 0143 left
  an acknowledgement flag to the owner's asking, and this record does the same.

## Consequences

- **What the probe no longer proves.** That the user has no subaccount the configuration does
  not name. That rests on the owner's confirmation (item 2), which the report records as not
  checked. An unnamed subaccount is not read before a session, so an open order or a position
  on it does not stop the run.
- **Cancel-on-disconnect's reach.** The probe arms only named accounts, each read flat with no
  open order. Whether an arm or a drop reaches the user's other accounts is not measured (0132
  item 4). If it does, an unnamed subaccount's orders could be cancelled. The owner's
  confirmation keeps that case out.
- **The report and the evidence.** The report's accounts section says how many accounts the
  run's configuration names, by name, to be read before every session, that they came from the
  configuration and not from QFEX, and that no `GET /user/subaccounts` was sent. Its table lists
  the reads made, so a run that stopped shows what it read. It says the owner must confirm that
  no other subaccount exists, and that the probe neither records nor checks it.
  `evidence.json` replaces `subaccounts_listed` with `accounts`: `source` `configuration`, the
  names, the count, `others_checked` false, and a note saying the same. The program prints the
  count and names on its `ACCOUNTS` line, where it printed `LISTED`.
- **The keys.** A run needs no All-accounts key. Each account has a key of its own, scoped to
  that account, with Execute orders, View orders, View positions and View balance, and never
  Deposit and withdraw (`crates/fbc-examples/README.md`). `GET /user/positions` answers with the
  account's `balance` object, so a key without View balance is untested.
- **More subaccounts.** FBC-e937's numbered slots would add named subaccounts, each read before
  every session as item 1 says, and each slot selecting a subaccount, never the primary account,
  nor one another slot selects. Its done line's refusal of a slot naming an unlisted subaccount
  has no listing to judge against, and its ticket text follows this record.
- **The adapter.** Its listing reader stays, with its tests, and nothing in the probe calls it.
- **The proof.** `crates/fbc-examples/tests/qfex_preprod_probe.rs`, against a QFEX-shaped stub
  that answers `GET /user/subaccounts` 403 as FBC-291a's run met it:
  - `each_named_account_arms_once_in_turn_with_no_subaccount_listing_and_its_capture_is_written_scrubbed`
    and
    `with_production_stage_one_completes_with_no_subaccount_listing_names_production_and_its_registries_carry_no_testnet_declaration`:
    a complete run on each environment that sends no listing, reads the primary account and
    the configured subaccount before every session, and says in its report, evidence and
    output that the accounts came from the configuration.
  - `the_probe_reads_only_the_accounts_its_configuration_names`: with no subaccount named, or
    `QFEX_SUBACCOUNT_ID` set but empty, only the primary account is read; a named subaccount
    with no pair of its own is read with the primary's.
  - `an_account_that_cannot_be_read_stops_the_probe_before_anything_arms`: a configured
    subaccount whose positions read is refused, or whose read socket is not authenticated, stops
    the run before any session.
  - `a_production_host_an_invalid_account_a_proxied_loopback_or_a_missing_credential_is_refused_before_connecting`:
    a named subaccount's invalid id, and a `QFEX_SUBACCOUNT_ID` of `primary`, are refused before
    anything connects, even when no session arms on the subaccount.

## What would show this was wrong

- A subaccount the configuration did not name is found to have had an order cancelled or a
  position changed during a run.
- QFEX documents, or a run shows, that a single-account key may list subaccounts, through
  `GET /user/subaccounts` or through `GET /user/subaccounts/balance` or `/equity`. Then a record
  may bring a listing back on that route.
- Cancel-on-disconnect is shown to reach other accounts of the user. Then the owner's
  confirmation carries more than this record assumes.
