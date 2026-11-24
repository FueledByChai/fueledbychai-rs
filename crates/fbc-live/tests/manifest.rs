//! fbc-live's normal dependencies are fbc-core, fbc-oms and fbc-runtime, exactly (decision 0155
//! item 1): no fourth crate, whether a workspace crate, tokio or `rust_decimal`, and no build or
//! target-specific dependency. `scripts/check-deps.sh` refuses only an edge to a venue crate, so
//! this test reads the manifest itself.

use std::collections::BTreeSet;

/// Each table of the manifest, by its header, with the keys it holds, a dotted key
/// (`fbc-core.workspace = true`) by its first part.
fn tables(manifest: &str) -> Vec<(String, BTreeSet<String>)> {
    let mut tables: Vec<(String, BTreeSet<String>)> = Vec::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(header) = line.strip_prefix('[') {
            let header = header.trim_start_matches('[');
            let header = header.split(']').next().unwrap_or_default().trim();
            tables.push((header.to_owned(), BTreeSet::new()));
            continue;
        }
        if let (Some((key, _)), Some((_, keys))) = (line.split_once('='), tables.last_mut()) {
            let key = key.trim();
            keys.insert(key.split('.').next().unwrap_or(key).to_owned());
        }
    }
    tables
}

#[test]
fn the_normal_dependencies_are_fbc_core_fbc_oms_and_fbc_runtime_only() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let manifest = std::fs::read_to_string(path).unwrap();
    let tables = tables(&manifest);

    let normal: Vec<&BTreeSet<String>> = tables
        .iter()
        .filter(|(header, _)| header == "dependencies")
        .map(|(_, keys)| keys)
        .collect();
    let want: BTreeSet<String> = ["fbc-core", "fbc-oms", "fbc-runtime"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    assert_eq!(normal, [&want], "fbc-live's [dependencies]");

    // Any other dependency table but a dev-dependency one is a build, a target-specific or a
    // dotted one (`[dependencies.tokio]`), each a normal or build dependency past 0155's three.
    let others: Vec<&str> = tables
        .iter()
        .map(|(header, _)| header.as_str())
        .filter(|header| *header != "dependencies" && header.contains("dependencies"))
        .filter(|header| !header.split('.').any(|part| part == "dev-dependencies"))
        .collect();
    assert!(others.is_empty(), "fbc-live declares {others:?}");
}

#[test]
fn the_reader_sees_every_kind_of_dependency_table() {
    let tables = tables(
        "[package]\nname = \"x\"\n\n[dependencies]\na = 1 # one\nb.workspace = true\n\
         [build-dependencies]\nc = 1\n[target.'cfg(unix)'.dependencies]\nd = 1\n\
         [dependencies.e]\npath = \"../e\"\n[dev-dependencies]\nf = 1\n",
    );
    let headers: Vec<&str> = tables.iter().map(|(h, _)| h.as_str()).collect();
    assert_eq!(
        headers,
        [
            "package",
            "dependencies",
            "build-dependencies",
            "target.'cfg(unix)'.dependencies",
            "dependencies.e",
            "dev-dependencies",
        ]
    );
    assert_eq!(
        tables[1].1,
        ["a", "b"].into_iter().map(str::to_owned).collect()
    );
}
