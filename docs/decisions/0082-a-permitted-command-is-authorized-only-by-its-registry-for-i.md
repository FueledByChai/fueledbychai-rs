# 0082 — A permitted command is authorized only by its registry, for its account, judged against the caps again, and a place not sent is withdrawn only with its command, augmenting 0045

Status: accepted
Date: 2026-10-08

## Context

0045 has every order-affecting command reach a gateway only through an `Authorization` that
`fbc-oms` issues from a `PermittedCommand` it built, and 0013 rule 2 puts the pre-trade caps
(0005's I6, the inventory cap, and 0052's resting cap) in the one path every order command
takes. The caps were judged once, when a command was built. Codex's four P1s on PR #74
(FBC-2e4), filed as FBC-0d9k, showed a `PermittedCommand` outliving the state it was judged on:

- r4189335960: a cap-sized place is built and retained; a venue-initiated fill of an
  own-namespace client id the registry does not hold moves the position to the cap; the
  retained place is still authorized, for a worst case of twice the cap.
- r4189335969: a `PermittedCommand` carries no registry or account, and `Registry::authorize`
  takes any `AccountKey`, so a place judged against account A's position can be authorized for
  account B.
- r4189335956: a place or batch item reported `NotSent` through `Registry::on_outcome` ends its
  order by client id and frees its exposure while the caller still holds the command, which can
  then be authorized alongside a new place that used the freed headroom.
- r4189335949: an amend's build token was (client id, build number); two accounts' registries
  may hold the same client id at the same build (accounts may lease the same namespace, 0068),
  so one registry's command could release the other's reservation.

## Decision

1. **A command records its registry.** Every `PermittedCommand` records the registry that
   built it (the registry identity of 0068): places, batches, amends, cancels, cancel-manys,
   tombstone cancels and the instrument cancel-all. `Registry::authorize` refuses a command
   another registry built (`IssueRefusal::OtherRegistry`) and changes nothing; what the command
   reserved in its own registry stays reserved there (fail closed). An amend's build token is
   (registry, client id, build number): `Registry::amend_not_submitted` releases nothing for
   another registry's amend.
2. **A registry is one account's.** A registry is for the account given at construction
   (`Registry::for_account`), or else the account of its first authorization, which binds it.
   An authorization for any other account is refused (`IssueRefusal::OtherAccount`), cancels
   included. The execution planner refuses a pass for an account through a registry bound to
   another (`PlanError::OtherAccount`, as in 0068), before anything is built.
3. **Judged again at authorization.** `Registry::authorize` judges a place, a batch or an
   amend again: each of its orders must still hold what its build reserved (a place's or batch
   item's command still held, an amend still the build its order holds;
   `IssueRefusal::Released`), and on each side the command's orders take, the position and our
   other orders as they are now, with what the command's orders may add and have resting as
   their records count them now, must stay within both caps (`IssueRefusal::Capped`). Cancels
   and the instrument cancel-all are not judged, as at build (0012).
4. **Refused at authorization is not sent.** A command of the registry refused for its
   account, its caps or a released order is never sent, so what it reserved is released: each
   place or batch item still held ends `NotSent(StaleAuthorization)`, and an amend's build is
   withdrawn.
5. **A place not sent is withdrawn with its command.** From its build until an authorization
   is issued for it, a place's or batch item's command is held: `Registry::on_outcome` refuses
   a placement outcome that would end the order (not sent or refused) with
   `OmsError::NotIssued` and changes nothing. A place never handed to a gateway is withdrawn by
   `Registry::place_not_submitted(cmd, reason)`, which consumes its command, as
   `Registry::amend_not_submitted` does for an amend. Once an authorization is issued the
   command is spent, and the gateway's outcome, not sent included, applies by client id as
   before.
6. `Registry::authorize` takes the registry mutably: issuing an authorization spends the
   commands it holds and binds the account.

## Alternatives

- Re-judge the caps at submit, in the gateway, as 0060 re-checks the market's state:
  rejected for now. The gateway holds no registry (0060 reads shared counters only); a cap
  judgement needs the registry's orders and position. Authorization is the last point with the
  registry in hand. 0066 covers Exit's position at submit; a fill between authorization and
  submit in any other state is not judged again here: follow-up ticket FBC-zux3.
- Invalidate every outstanding permit when the inventory moves: rejected. It refuses commands
  a fill made safer (a buy after a sell fill), and judging again refuses exactly the ones the
  caps refuse.
- Release a refused command's reservation by client id: rejected; that is r4189335956.
- Make the account mandatory at construction: deferred. `fbc-runtime`'s and Paradex's tests
  and the owner's `testnet_trade` sample (PR #114) build registries without one; binding at
  the first authorization keeps one account per registry from then on, and a consumer that
  builds with `for_account` has no first-authorization window. Follow-up ticket FBC-xz9m.
- Hand a command refused for another registry back to the caller: rejected. Another
  registry's command reaching this one is a wiring error; dropping it keeps its reservation
  held in its own registry, which overcounts, which fails closed.

## Consequences

- A command retained across a fill, a resync or another order's build is authorized only if
  the caps admit it as things are then; a planner pass, which builds and authorizes each
  command with nothing between, is unaffected.
- A consumer reports a place it built and did not hand to a gateway with
  `place_not_submitted`, not with a `NotSent` outcome.
- A registry used for a second account fails every authorization for it, which fails closed.

## What would show this was wrong

- A command authorized whose admission, judged with the position and orders at authorization,
  breaches a cap; or one authorized for another account than its registry's.
- A place's or amend's reservation released while its command can still be authorized.
- A legitimate flow that must authorize one registry's commands for two accounts.
