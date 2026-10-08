//! `signing_golden` (design §6, decision 0009): a venue's encoding of each golden command,
//! signature included, is exactly the bytes committed for it.
//!
//! Each [`Golden`](super::Golden) the [`Setup`](super::Setup) lists is encoded as its request
//! id under its own [`EncodeCtx`](fbc_core::EncodeCtx) (the time and nonces signed) by a codec
//! built fresh from the factory, against the setup's spec table. It must be sent as one frame
//! ([`Effect::Send`]) and nothing else that writes to the venue, and that frame's bytes must
//! equal `<fixtures>/signing_golden/<name>.golden`. A command that writes more than one frame
//! or an HTTP request is not modelled here and is a breach, not a pass: no venue in this
//! workspace signs that way yet.
//!
//! The fixture directory `signing_golden/` must hold a `SYNTHETIC` file, as decision 0009
//! requires of golden signing vectors, and no `.golden` file that no command names, so a
//! golden that is never checked cannot sit there looking checked. A breach reports where the
//! bytes first differ and their lengths, never the bytes: a frame can carry a credential.
//!
//! Skipped for a venue whose caps declare no order entry: it signs nothing. A venue that
//! declares order entry and lists no golden command fails.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use fbc_core::{Effect, Effects, PathStamps};

use super::harness::Harness;
use super::{Breach, Failure, Golden, Subject, Verdict};

const CHECK: &str = "signing_golden";

/// The fixture subdirectory the golden files are read from.
const DIR: &str = "signing_golden";
/// The golden files' extension.
const EXT: &str = "golden";

/// Runs `signing_golden` against `subject`.
pub fn signing_golden(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    if h.caps.exec.is_none() {
        return Ok(Verdict::Skipped {
            check: CHECK,
            why: "the caps declare no order entry (VenueCaps.exec is None): nothing is signed",
        });
    }
    let setup = subject.setup();
    if setup.goldens.is_empty() {
        return Err(h.fail(
            "Setup.goldens",
            "lists no command, though the caps declare order entry",
        ));
    }
    let dir = subject.fixtures().join(DIR);
    let on_disk = golden_files(&dir).map_err(|what| h.fail(DIR, what))?;
    let mut breaches = Vec::new();
    if !dir.join("SYNTHETIC").is_file() {
        breaches.push(Breach {
            capability: format!("{DIR}/SYNTHETIC"),
            what: "missing: golden signing vectors live in a directory marked synthetic \
                   (decision 0009)"
                .to_owned(),
        });
    }
    let mut named = BTreeSet::new();
    let mut probed = Vec::new();
    for golden in &setup.goldens {
        let label = format!("golden {}", golden.name);
        let what = if !valid_name(golden.name) {
            Some("its name is not a file name of letters, digits, '-' and '_'".to_owned())
        } else if !named.insert(golden.name) {
            Some("named twice in Setup.goldens".to_owned())
        } else {
            match fs::read(dir.join(format!("{}.{EXT}", golden.name))) {
                Err(e) => Some(format!("cannot read {DIR}/{}.{EXT}: {e}", golden.name)),
                Ok(want) => compare(&h, &setup.specs, golden, &want)?,
            }
        };
        match what {
            Some(what) => breaches.push(Breach {
                capability: label,
                what,
            }),
            None => probed.push(label),
        }
    }
    for stem in on_disk.iter().filter(|stem| !named.contains(stem.as_str())) {
        breaches.push(Breach {
            capability: format!("{DIR}/{stem}.{EXT}"),
            what: "no command in Setup.goldens names it, so it is never checked".to_owned(),
        });
    }
    if breaches.is_empty() {
        Ok(Verdict::Passed {
            check: CHECK,
            probed,
        })
    } else {
        Err(Failure {
            check: CHECK,
            breaches,
        })
    }
}

/// How `golden`, encoded by a fresh codec, departs from `want`; `None` when it is `want`.
fn compare(
    h: &Harness<'_>,
    specs: &fbc_core::SpecTable,
    golden: &Golden,
    want: &[u8],
) -> Result<Option<String>, Failure> {
    let mut codec = h.exec_codec()?;
    let mut fx = Effects::new();
    let stamps = &mut PathStamps::off();
    let sent = codec.encode(&golden.cmd, golden.rpc, specs, &golden.ctx, stamps, &mut fx);
    if let Err(why) = sent {
        return Ok(Some(format!(
            "refused as NotSent({why:?}) by a freshly built codec"
        )));
    }
    let Some(got) = one_frame(&fx) else {
        return Ok(Some(
            "not sent as exactly one frame and nothing else written: a command written as \
             several frames or an HTTP request is not modelled by signing_golden"
                .to_owned(),
        ));
    };
    Ok(differs(got, want))
}

/// The bytes of the one frame `fx` writes, when it writes exactly one frame and no HTTP
/// request.
fn one_frame(fx: &Effects) -> Option<&[u8]> {
    let mut writes = fx.as_slice().iter().filter_map(|effect| match effect {
        Effect::Send { frame, .. } => Some(Some(frame.bytes())),
        Effect::Http { .. } => Some(None),
        Effect::Timer { .. } | Effect::Reconnect { .. } => None,
    });
    match (writes.next(), writes.next()) {
        (Some(frame), None) => frame,
        _ => None,
    }
}

/// Where `got` first departs from `want`, by offset and lengths only; `None` when equal.
fn differs(got: &[u8], want: &[u8]) -> Option<String> {
    if got == want {
        return None;
    }
    let at = got
        .iter()
        .zip(want)
        .position(|(a, b)| a != b)
        .unwrap_or(got.len().min(want.len()));
    Some(format!(
        "encoded {} bytes that differ from the golden's {} at byte {at}",
        got.len(),
        want.len()
    ))
}

/// Whether `name` is a plain file stem: non-empty, of ASCII letters, digits, `-` and `_`.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// The stems of the `.golden` files in `dir`.
fn golden_files(dir: &Path) -> Result<BTreeSet<String>, String> {
    let entries = fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    let mut stems = BTreeSet::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == EXT) {
            let stem = path.file_stem().unwrap_or_default();
            stems.insert(stem.to_string_lossy().into_owned());
        }
    }
    Ok(stems)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_difference_is_reported_by_offset_and_lengths_only() {
        assert_eq!(differs(b"abc", b"abc"), None);
        let at_one = differs(b"abc", b"axc").unwrap();
        assert_eq!(
            at_one,
            "encoded 3 bytes that differ from the golden's 3 at byte 1"
        );
        let shorter = differs(b"ab", b"abc").unwrap();
        assert_eq!(
            shorter,
            "encoded 2 bytes that differ from the golden's 3 at byte 2"
        );
    }

    #[test]
    fn a_golden_name_is_a_plain_file_stem() {
        for ok in ["place", "batch-cancel_2", "A1"] {
            assert!(valid_name(ok), "{ok}");
        }
        for bad in ["", "../place", "a.b", "a/b", "sp ace"] {
            assert!(!valid_name(bad), "{bad}");
        }
    }
}
