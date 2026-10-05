# Conformance toy fixtures

The fixture directory the named conformance suite (`fbc_conformance::suite!`) is given for the
conformance toy venue (`crates/fbc-conformance/src/toy/`, decision 0044), in
`crates/fbc-conformance/tests/suite_toy.rs`. The toy describes no real venue, so nothing here
is recorded from one.

A check that reads recorded data reads it from a subdirectory named after the check
(`<check>/`), described in that check's documentation in `crates/fbc-conformance/src/suite/`.
The checks so far, `caps_truthful` and `commands_selfcontained`, read none: they need only the
toy's factory and the setup the suite's test gives (the toy's spec table, an empty
configuration and no credentials), so the directory holds only this file.
