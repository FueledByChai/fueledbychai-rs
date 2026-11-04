#!/usr/bin/env python3
"""Writes hmac-vectors.json, the QFEX authentication vectors tests/signing.rs checks.

The signatures are computed here, apart from the Rust signer, with Python's standard hmac
and hashlib, by the rule QFEX's authentication page states
(https://docs.qfex.com/websocket/channels/trade/authenticate): the message is the nonce, a
colon and the Unix time in whole seconds; the signature is HMAC-SHA256 of the message keyed
with the secret's UTF-8 bytes, written as lowercase hex. The nonce is the two journaled u64
values the Rust signer is given, each written as 16 lowercase hex digits, zero padded.

Every secret here is made up for these vectors and belongs to no account. Run it from the
repository root with no arguments:

    python3 -I fixtures/qfex/signing/hmac_oracle.py > fixtures/qfex/signing/hmac-vectors.json
"""

import hashlib
import hmac
import json

U64_MAX = 2**64 - 1
I64_MAX = 2**63 - 1

# Made-up secrets: one ASCII, one with characters outside ASCII, whose UTF-8 bytes are the key.
SECRETS = {
    "ascii": "SYNTHETIC-qfex-secret-belonging-to-no-account",
    "utf8": "SYNTHETIC-sécret-ключ",
}

# (name, secret, nonce values, wall time in nanoseconds since the Unix epoch)
CASES = [
    ("docs_shaped", "ascii", [0xC0FE, 0xBABE], 1_760_545_414_000_000_000),
    ("nonce_zero", "ascii", [0, 0], 1_760_545_414_000_000_000),
    ("nonce_max", "ascii", [U64_MAX, U64_MAX], 1_760_545_414_000_000_000),
    ("padding_low_half", "ascii", [0, U64_MAX], 1_760_545_414_000_000_000),
    ("padding_high_half", "ascii", [U64_MAX, 0], 1_760_545_414_000_000_000),
    ("padding_ones", "ascii", [1, 1], 1_760_545_414_000_000_000),
    ("floor_last_nanosecond", "ascii", [7, 8], 1_760_545_414_999_999_999),
    ("floor_next_second", "ascii", [7, 8], 1_760_545_415_000_000_000),
    ("floor_epoch", "ascii", [7, 8], 0),
    ("floor_epoch_last_nanosecond", "ascii", [7, 8], 999_999_999),
    ("floor_latest_wall", "ascii", [7, 8], I64_MAX),
    ("utf8_secret", "utf8", [0xC0FE, 0xBABE], 1_760_545_414_000_000_000),
]


def vector(name, secret, nonce, wall_ns):
    nonce_hex = "".join(f"{value:016x}" for value in nonce)
    unix_ts = wall_ns // 1_000_000_000
    message = f"{nonce_hex}:{unix_ts}"
    key = SECRETS[secret].encode("utf-8")
    signature = hmac.new(key, message.encode("ascii"), hashlib.sha256).hexdigest()
    return {
        "name": name,
        "secret": SECRETS[secret],
        "nonce": nonce,
        "wall_ns": wall_ns,
        "nonce_hex": nonce_hex,
        "unix_ts": unix_ts,
        "message": message,
        "signature": signature,
    }


def main():
    out = {
        "source": "fixtures/qfex/signing/hmac_oracle.py (Python hmac and hashlib)",
        "vectors": [vector(*case) for case in CASES],
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
