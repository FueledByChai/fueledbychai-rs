# Licence gate fixture

A standalone Cargo workspace (its own `[workspace]`, outside the main one, which excludes
`fixtures/`) for `scripts/licence-check.sh --self-test`. The root crate, `licence-gate-app`,
is MIT and depends on the path crate `gpl-dep`, which is GPL-3.0-only. The self-test runs the
repository's own `deny.toml` against it and expects the gate to fail naming `gpl-dep` and its
licence, then to pass with `gpl-dep` excluded from the graph, so the failure is the licence and
nothing else. Neither crate is built; only `cargo metadata` reads them.
