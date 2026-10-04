# Dependency-direction fixture

A standalone Cargo workspace (its own `[workspace]`, outside the main one, which excludes
`fixtures/`) for `scripts/check-deps.sh --self-test`. It copies the main workspace's layout,
since the check tells a venue crate by its manifest's path under `crates/venues/`. Every crate
is empty; nothing here is built, only `cargo metadata --no-deps` reads it. The root package,
`dep-direction-root`, has no dependencies; it exists because every manifest in the repository
is a package that says `publish = false` (`crates/fbc-core/tests/manifests.rs`).

The self-test expects the check to fail (exit 1) naming exactly these three edges:

- `fbc-core -> fbc-venue-a`, a normal dependency under the alias `venue`: a crate outside
  `crates/venues/` never depends on a venue crate;
- `fbc-book -> fbc-venues`, a build-dependency: the same rule, and the registry is a venue crate;
- `fbc-venue-b -> fbc-venue-a`: a venue crate depends on `fbc-core` and protocol crates only.

and to name none of the allowed edges: `fbc-journal`'s dev-dependency on `fbc-venue-a`,
`fbc-venue-a`'s dev-dependency on `fbc-journal`, `fbc-venue-b -> fbc-core`, and the registry
`fbc-venues` on both venue crates.
