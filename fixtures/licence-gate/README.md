# Licence gate fixture

A standalone Cargo workspace (its own `[workspace]`, outside the main one, which excludes
`fixtures/`) for `scripts/licence-check.sh --self-test`. The root crate, `licence-gate-app`,
is MIT; it depends on the path crate `gpl-dep` and, as a dev-dependency only, on the path crate
`gpl-dev-dep`, both GPL-3.0-only and neither a workspace member, so each is reached only as a
dependency. The self-test runs the repository's own `deny.toml` against it and expects the gate
to fail naming both crates and their licence, then to pass with both excluded from the graph,
so the failure is the licence and nothing else. Nothing here is built; only `cargo metadata`
reads it.
