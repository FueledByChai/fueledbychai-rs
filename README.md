# fueledbychai-rs

Venue-agnostic trading connectivity for Rust: market data and order entry behind common traits,
with one crate per venue. The Rust counterpart of
[FueledByChaiTrading](https://github.com/FueledByChai/FueledByChaiTrading).

First venues: Paradex, then Hibachi; Binance USD-M futures for reference market data.

Status: scaffolding. The Cargo workspace exists with two crates:

| Crate | Status |
| --- | --- |
| `fbc-core` | Time kinds, units, exact prices (`PxExact`), money and price grids (fixed, significant-figure, banded); sealed order and fill ids, the canonical client-id codec, restart-safe minting under a namespace lease, `Fee` (positive means we paid) and the per-account `FeeBook`, `DecodeScope`, `InstrumentSpec` with maker-safe quantization, `VenueCaps` with every field mandatory, and the venue boundary: market-data and execution events, venue commands and their outcomes, and the sans-IO `MdCodec`, `ExecCodec`, `OrderSigner`, `NonceSource` and `VenueFactory` traits with `Effects` and `EncodeCtx` |
| `fbc-venue-paradex` | The Paradex signer only (`src/sign`, owner-reviewed): the SNIP-12 revision 0 typed-data hash of `Order`, `ModifyOrder` and the auth `Request` and its Stark-curve ECDSA signature with RFC 6979 nonces, behind `OrderSigner`, held to the Java library's vectors in `fixtures/paradex/signing/` |

No venue is supported yet: Paradex has its signer, not yet market data or order entry.
