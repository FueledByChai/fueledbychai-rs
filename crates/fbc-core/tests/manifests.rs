//! Nothing in this repository is published to crates.io; releases are git tags the consumer
//! pins (0008). Every package manifest says `publish = false`, and so does the workspace's
//! package defaults, so `cargo publish` refuses rather than uploading by accident.

use std::fs;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The `key = value` lines of `[section]` in a manifest, whitespace-trimmed.
fn section_lines(manifest: &str, section: &str) -> Vec<String> {
    let header = format!("[{section}]");
    let mut inside = false;
    let mut lines = Vec::new();
    for line in manifest.lines().map(str::trim) {
        if line.starts_with('[') {
            inside = line == header;
        } else if inside {
            lines.push(line.to_owned());
        }
    }
    lines
}

fn manifests_under(dir: &Path, found: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name != "target") {
                manifests_under(&path, found);
            }
        } else if path.file_name().is_some_and(|name| name == "Cargo.toml") {
            found.push(path);
        }
    }
}

#[test]
fn no_package_is_publishable() {
    let root = repo_root();
    let workspace = fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(
        section_lines(&workspace, "workspace.package").contains(&"publish = false".to_owned()),
        "Cargo.toml [workspace.package] lacks publish = false"
    );
    let mut manifests = Vec::new();
    manifests_under(&root.join("crates"), &mut manifests);
    manifests_under(&root.join("fixtures"), &mut manifests);
    assert!(
        manifests
            .iter()
            .any(|m| m.ends_with("crates/fbc-core/Cargo.toml"))
    );
    for manifest in manifests {
        let text = fs::read_to_string(&manifest).unwrap();
        assert!(
            section_lines(&text, "package").contains(&"publish = false".to_owned()),
            "{} [package] lacks publish = false",
            manifest.display()
        );
    }
}
