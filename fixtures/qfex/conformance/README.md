# QFEX conformance fixtures

The fixture directory the named conformance suite (`fbc_conformance::suite!`) is to be given for
fbc-venue-qfex once its order entry declares `ExecCaps` (FBC-jdj7 runs the suite on it). Until
then `crates/venues/fbc-venue-qfex/tests/ids.rs` reads it. Nothing here is recorded from QFEX
or from an account (decision 0009).

- `ids_roundtrip/java_era.txt`: client ids in the shapes other systems send QFEX (a random
  UUID in either case, short hyphenated labels, the Java library's decimal millisecond
  counter), each of which must decode Unparseable under the crate's client-id format; every
  value is made up.
