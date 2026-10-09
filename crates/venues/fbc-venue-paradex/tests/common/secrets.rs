//! The rehearsal's secret search (FBC-8mv), shared so that every test looking for a leaked
//! credential looks the same way (FBC-3f8z): the synthetic account and key of
//! `fixtures/paradex/signing`, a session token and the login signatures a test saw.

use fbc_venue_paradex::sign::Felt;

use super::Vectors;

/// The synthetic account and key of `fixtures/paradex/signing`, from the vectors' header.
pub fn synthetic() -> (String, String) {
    let vectors = Vectors::read();
    (
        vectors.header["account"].clone(),
        vectors.header["key"].clone(),
    )
}

/// No credential, session token or login signature in `text`: the synthetic account and key
/// (as hex, with or without `0x`, any case), `token`, and each part of each login signature in
/// `signatures`.
pub fn secrets_absent(text: &str, token: &str, signatures: &[String], what: &str) {
    let lower = text.to_ascii_lowercase();
    for needle in credential_needles(token, signatures) {
        assert!(
            !lower.contains(&needle),
            "{what} shows a credential, the token or a login signature"
        );
    }
}

/// What [`secrets_absent`] looks for, lowercase: the account, the key, `token` and the login
/// signatures' numbers, each as text and as the hex of its bytes, as tungstenite dumps a
/// payload. A number is also looked for as starknet's `Felt` prints it: in hex without leading
/// zeros (which `{:x}`, `{:#x}` and any zero-padded form contain) and in decimal.
pub fn credential_needles(token: &str, signatures: &[String]) -> Vec<String> {
    let (account, key) = synthetic();
    let mut numbers = vec![account, key];
    for sig in signatures {
        assert!(!sig.is_empty(), "no login signature was read");
        numbers.extend(signature_numbers(sig));
    }
    let mut secrets = vec![token.to_owned()];
    for number in numbers {
        let digits = number.trim_start_matches("0x");
        let felt = if number.starts_with("0x") || !digits.bytes().all(|b| b.is_ascii_digit()) {
            Felt::from_hex(&format!("0x{digits}"))
        } else {
            Felt::from_dec_str(digits)
        };
        let felt = felt.unwrap_or_else(|_| panic!("{number} is not a field element"));
        secrets.push(digits.to_owned());
        secrets.push(
            format!("{felt:x}")
                .trim_start_matches("0x")
                .trim_start_matches('0')
                .to_owned(),
        );
        secrets.push(format!("{felt}"));
    }
    let mut needles = Vec::new();
    for secret in secrets {
        let hex: String = secret.bytes().map(|b| format!("{b:02x}")).collect();
        needles.push(secret.to_ascii_lowercase());
        needles.push(hex);
    }
    needles
}

/// A login signature's numbers, not its punctuation or a short fragment.
pub fn signature_numbers(sig: &str) -> Vec<String> {
    sig.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|p| p.trim_start_matches("0x").len() >= 16)
        .map(str::to_owned)
        .collect()
}
