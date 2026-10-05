//! Decision 0004: a `VenueOrderId`, a `FillId`, a `VenueSymbol` or a `Fee` is built only
//! through the `DecodeScope` the core's dispatch lends, and there is no back door for tests
//! either. The crate-private constructors (`from_wire`, `Fee::from_declared`) are reachable
//! from anywhere inside `fbc-core`, including its unit tests, so the compiler alone cannot
//! hold the line; these tests read the crate's sources and fail if anything but `scope.rs`
//! calls them, or if anything builds the tuple structs other than those constructors' bodies.
//!
//! Decisions 0002 and 0033: a codec gets time only from `EncodeCtx` or callback arguments, and
//! marks its latency stages through `PathStamps`, which gives back none. Nothing in `fbc-core`
//! reads a clock, so nothing in it can hand a codec a time the runtime did not journal.

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
            let is_call = [
                "VenueOrderId::from_wire(",
                "FillId::from_wire(",
                "VenueSymbol::from_wire(",
            ]
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
            for pat in [
                "VenueOrderId(CompactString",
                "FillId(CompactString",
                "VenueSymbol(CompactString",
            ] {
                builds.extend(line.matches(pat).map(|_| format!("{name}: {pat}")));
            }
        }
    }
    builds.sort();
    assert_eq!(
        builds,
        [
            "ids.rs: FillId(CompactString",
            "ids.rs: VenueOrderId(CompactString",
            "ids.rs: VenueSymbol(CompactString"
        ],
        "a venue id struct is built somewhere other than its from_wire body (0004)"
    );
}

#[test]
fn only_the_decode_scope_calls_the_fee_constructor() {
    let mut calls = Vec::new();
    for (name, text) in crate_sources() {
        for (n, line) in text.lines().enumerate() {
            if line.contains("Fee::from_declared(") && name != "scope.rs" {
                calls.push(format!("{name}:{}: {}", n + 1, line.trim()));
            }
        }
    }
    assert!(
        calls.is_empty(),
        "fees built outside DecodeScope (0004):\n{}",
        calls.join("\n")
    );
}

#[test]
fn only_from_declared_builds_the_fee_struct() {
    let mut builds = Vec::new();
    for (name, text) in crate_sources() {
        for line in text.lines().filter(|line| !line.contains("pub struct")) {
            builds.extend(line.matches("Fee(Money").map(|_| name.clone()));
        }
    }
    assert_eq!(
        builds,
        ["fee.rs"],
        "a Fee is built somewhere other than Fee::from_declared (0004)"
    );
}

#[test]
fn nothing_in_the_core_reads_a_clock() {
    let mut reads = Vec::new();
    for (name, text) in crate_sources() {
        for (n, line) in text.lines().enumerate() {
            let is_read = [
                "Instant::now",
                ".elapsed()",
                "SystemTime",
                "UNIX_EPOCH",
                "quanta",
                "clock_gettime",
                "Utc::now",
                "Local::now",
            ]
            .iter()
            .any(|pat| line.contains(pat));
            if is_read {
                reads.push(format!("{name}:{}: {}", n + 1, line.trim()));
            }
        }
    }
    assert!(
        reads.is_empty(),
        "fbc-core reads a clock (0002, 0033):\n{}",
        reads.join("\n")
    );
}
