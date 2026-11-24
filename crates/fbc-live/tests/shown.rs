//! A venue's words as the link shows them (`fbc_live::link::shown` and `described`): every word
//! that could hold an echoed credential withheld (decision 0009), the limits fixed (decision 0155
//! item 8). Moved from `testnet_trade`'s test with the link (decision 0155 item 9).

use fbc_live::link;

/// A session token's shape (dotted parts, synthetic): what an auth frame's refusal can echo.
const TOKEN: &str = "SYNTHETIC.session-token.testnet-trade";

#[test]
fn a_venue_error_is_shown_with_every_word_that_could_hold_a_secret_withheld() {
    // Kept: the codec's sentence, Paradex's code, an HTTP status and a short JSON-RPC code.
    let sentence = "the Paradex login was refused: HTTP 401, \
                    STARKNET_SIGNATURE_VERIFICATION_FAILED: verification failed on Curve";
    assert_eq!(link::shown(sentence), sentence);
    assert_eq!(link::shown("code -32600"), "code -32600");
    // Withheld: a token, a long word, hex, digits mixed with letters; control characters
    // become spaces.
    // A token's shape (dotted parts), built at run time: no token-shaped literal sits here.
    let token = format!("{0}.{0}.{0}", "SyntheticPart");
    assert_eq!(
        link::shown(&format!("invalid bearer {token}")),
        "invalid bearer <withheld>"
    );
    assert_eq!(
        link::shown("key 0x1a2b3c4d and abcdef and a1b2c3 and 12345678"),
        "key <withheld> and <withheld> and <withheld> and <withheld>"
    );
    assert_eq!(link::shown("bad\nline\tbreak"), "bad line break");
    let long = "word ".repeat(100);
    assert!(link::shown(&long).ends_with("..."));
    assert!(link::shown(&long).chars().count() <= 203);
    // A venue error: its kind, its code (when one is plain) and its words.
    let reject = |code: Option<&str>, raw: &str| fbc_core::Reject {
        kind: fbc_core::RejectKind::Other,
        venue_code: code.map(Into::into),
        raw: raw.into(),
    };
    assert_eq!(link::described(&reject(None, sentence)), sentence);
    assert_eq!(
        link::described(&reject(
            Some("STARKNET_SIGNATURE_VERIFICATION_FAILED"),
            sentence
        )),
        sentence
    );
    assert_eq!(
        link::described(&reject(Some("40110"), &format!("invalid bearer {TOKEN}"))),
        "40110: invalid bearer <withheld>"
    );
    assert_eq!(
        link::described(&reject(Some(&"A".repeat(80)), "refused")),
        "<withheld>: refused"
    );
}
