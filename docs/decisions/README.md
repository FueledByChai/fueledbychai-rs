# Decisions

One file per decision, never edited in place: a change is a new record that supersedes
the old one. Cite a decision by its number. `scripts/decisions.sh new "<title>"` adds one.

- [0001](0001-two-repositories-a-public-mit-library-and-a-private-consumer.md) Two repositories: a public MIT library and a private consumer, split at the line between connectivity and quoting — accepted
- [0002](0002-venue-adapters-are-sans-io-codecs-driven-by-one-generic-runt.md) Venue adapters are sans-IO codecs driven by one generic runtime that proxies through SOCKS5 from day one — accepted
- [0003](0003-venue-capabilities-are-data-and-every-field-is-mandatory.md) Venue capabilities are data and every field is mandatory — accepted
- [0004](0004-ids-fees-time-prices-and-quantities-are-sealed-newtypes-and-.md) Ids, fees, time, prices and quantities are sealed newtypes, and a fee is positive when we paid it — accepted
- [0005](0005-the-library-owns-order-truth-a-monotone-order-lattice-permit.md) The library owns order truth: a monotone order lattice, permits, and one ExecutionPlanner — accepted
- [0006](0006-the-journal-records-everything-that-crosses-the-shard-bounda.md) The journal records everything that crosses the shard boundary and keeps a reserve so safety traffic never blocks — accepted
- [0007](0007-venue-order-paradex-first-then-hibachi-with-binance-usd-m-fu.md) Venue order: Paradex first, then Hibachi, with Binance USD-M futures as reference market data — superseded by 0016
- [0008](0008-pinned-toolchain-loop-kit-beads-queue-and-consumption-by-git.md) Pinned toolchain, loop kit, Beads queue, and consumption by git tag with no crates.io release yet — accepted
- [0009](0009-secrets-and-private-data-never-enter-this-repository-and-sig.md) Secrets and private data never enter this repository, and signing and auth are the only review paths — accepted
- [0010](0010-three-safety-rules-every-change-keeps-no-trading-after-a-res.md) Three safety rules every change keeps: no trading after a restart until Start, pre-trade caps on every order, one quoter per market — superseded by 0013
- [0011](0011-the-kill-switch-blocks-every-order-reducing-orders-included-.md) The kill switch blocks every order, reducing orders included; cancels always go through — superseded by 0012
- [0012](0012-the-kill-switch-blocks-every-place-and-amend-cancels-go-thro.md) The kill switch blocks every place and amend; cancels go through within 0005's guards; lifting it leaves the market cancel-only — accepted, supersedes 0011
- [0013](0013-three-safety-rules-every-change-keeps-nothing-sent-after-a-r.md) Three safety rules every change keeps: nothing sent after a restart until Start, Flatten or Wind-down, pre-trade caps on every order, one quoter per market — accepted, supersedes 0010
- [0014](0014-the-venue-boundary-as-built-refines-design-4-6-to-4-8-where-.md) The venue boundary as built refines design 4.6 to 4.8 where a sans-IO codec needs more than the design text gives it — accepted
- [0015](0015-order-and-fill-capabilities-fold-into-one-optional-exec-bloc.md) Order and fill capabilities fold into one optional exec block, so a venue cannot claim fills without orders or orders without fills — accepted
- [0016](0016-venue-order-paradex-first-its-signer-offline-before-market-d.md) Venue order: Paradex first, its signer offline before market data, then order entry; then Hibachi; Binance USD-M as reference — accepted, supersedes 0007
- [0017](0017-a-licence-gate-cargo-deny-pinned-at-0-20-2-checks-every-depe.md) A licence gate: cargo-deny pinned at 0.20.2 checks every dependency against a permissive allowlist on every machine that runs the check — accepted
- [0018](0018-every-frame-and-http-request-carries-its-rate-charge-and-lim.md) Every frame and HTTP request carries its rate charge, and limits gain a per-connection scope and a connect operation — accepted
- [0019](0019-the-runtime-s-network-stack-tokio-tokio-tungstenite-and-hype.md) The runtime's network stack: tokio, tokio-tungstenite and hyper over one connector with a hand-written SOCKS5 CONNECT, each pinned — accepted
- [0020](0020-tls-runs-on-the-connector-s-stream-with-rustls-and-the-ring-.md) TLS runs on the connector's stream with rustls and the ring provider, trusting webpki-roots plus consumer anchors — accepted
