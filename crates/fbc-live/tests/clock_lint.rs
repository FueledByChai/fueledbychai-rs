//! fbc-live reads no clock but its session's shard clock (FBC-y7lq, decision 0155 item 3): its
//! `clippy.toml` disallows the std clock reads and the runtime's system wall clock, each with a
//! reason naming 0155, so `cargo clippy -D warnings` fails a direct read in the crate, and its
//! source names none of them.

use std::path::Path;

/// The paths `clippy.toml` must disallow, each under its table.
const DISALLOWED: [(&str, &str); 5] = [
    ("disallowed-methods", "std::time::Instant::now"),
    ("disallowed-methods", "std::time::Instant::elapsed"),
    ("disallowed-methods", "std::time::SystemTime::now"),
    ("disallowed-methods", "std::time::SystemTime::elapsed"),
    ("disallowed-types", "fbc_runtime::SystemWallClock"),
];

/// Each `[[table]]` entry of `config`: its table, and its `path` and `reason` values.
fn entries(config: &str) -> Vec<(String, String, String)> {
    let mut entries: Vec<(String, String, String)> = Vec::new();
    for line in config.lines().map(str::trim) {
        if let Some(table) = line.strip_prefix("[[").and_then(|t| t.strip_suffix("]]")) {
            entries.push((table.to_owned(), String::new(), String::new()));
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let value = value.trim().trim_matches('"').to_owned();
        match (key.trim(), entries.last_mut()) {
            ("path", Some(entry)) => entry.1 = value,
            ("reason", Some(entry)) => entry.2 = value,
            _ => {}
        }
    }
    entries
}

#[test]
fn clippy_disallows_each_std_clock_read_and_the_system_wall_clock_citing_0155() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/clippy.toml");
    let entries = entries(&std::fs::read_to_string(path).unwrap());
    let named: Vec<(&str, &str)> = entries
        .iter()
        .map(|(table, path, _)| (table.as_str(), path.as_str()))
        .collect();
    assert_eq!(named, DISALLOWED);
    for (_, path, reason) in &entries {
        assert!(reason.contains("decision 0155"), "{path}: {reason}");
    }
}

#[test]
fn the_crates_source_names_no_clock_read() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut read = 0;
    for file in std::fs::read_dir(src).unwrap() {
        let file = file.unwrap().path();
        let text = std::fs::read_to_string(&file).unwrap();
        for word in ["Instant", "SystemTime", "SystemWallClock", "elapsed("] {
            assert!(!text.contains(word), "{} names {word}", file.display());
        }
        read += 1;
    }
    assert!(read >= 2, "read {read} source files");
}

#[test]
fn the_reader_takes_each_entrys_table_path_and_reason() {
    let read = entries(
        "# a comment = no key\n\n[[disallowed-methods]]\npath = \"a::b\"\nreason = \"why\"\n\
         other = 1\n[[disallowed-types]]\npath = \"c::D\"\n",
    );
    let want = [
        ("disallowed-methods", "a::b", "why"),
        ("disallowed-types", "c::D", ""),
    ];
    let want: Vec<(String, String, String)> = want
        .iter()
        .map(|(t, p, r)| ((*t).to_owned(), (*p).to_owned(), (*r).to_owned()))
        .collect();
    assert_eq!(read, want);
}
