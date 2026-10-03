//! Decision 0004: a `VenueOrderId` or a `FillId` is built only through the `DecodeScope` the
//! core's dispatch lends, and there is no back door for tests either. The crate-private
//! `from_wire` constructors are reachable from anywhere inside `fbc-core`, including its unit
//! tests, so the compiler alone cannot hold the line; this test reads the crate's sources and
//! fails if anything but `scope.rs` calls them, or if anything builds the tuple structs other
//! than the two `from_wire` bodies.

use std::fs;
use std::path::{Path, PathBuf};

fn sources(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, found);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            found.push(path);
        }
    }
}

fn crate_sources() -> Vec<(String, String)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&src, &mut files);
    files.sort();
    assert!(!files.is_empty(), "no sources under {}", src.display());
    files
        .into_iter()
        .map(|path| {
            let name = path.strip_prefix(&src).unwrap().display().to_string();
            (name, fs::read_to_string(&path).unwrap())
        })
        .collect()
}

#[test]
fn only_the_decode_scope_calls_the_venue_id_constructors() {
    let mut calls = Vec::new();
    for (name, text) in crate_sources() {
        for (n, line) in text.lines().enumerate() {
            let is_call = ["VenueOrderId::from_wire(", "FillId::from_wire("]
                .iter()
                .any(|pat| line.contains(pat));
            if is_call && name != "scope.rs" {
                calls.push(format!("{name}:{}: {}", n + 1, line.trim()));
            }
        }
    }
    assert!(
        calls.is_empty(),
        "venue ids built outside DecodeScope (0004):\n{}",
        calls.join("\n")
    );
}

#[test]
fn only_from_wire_builds_the_venue_id_structs() {
    let mut builds = Vec::new();
    for (name, text) in crate_sources() {
        // The `pub struct` lines declare the types; every other occurrence builds one.
        for line in text.lines().filter(|line| !line.contains("pub struct")) {
            for pat in ["VenueOrderId(CompactString", "FillId(CompactString"] {
                builds.extend(line.matches(pat).map(|_| format!("{name}: {pat}")));
            }
        }
    }
    builds.sort();
    assert_eq!(
        builds,
        [
            "ids.rs: FillId(CompactString",
            "ids.rs: VenueOrderId(CompactString"
        ],
        "a venue id struct is built somewhere other than its from_wire body (0004)"
    );
}
