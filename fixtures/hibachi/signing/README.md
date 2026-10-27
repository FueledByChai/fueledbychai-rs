# Hibachi signing vectors

The golden file `crates/venues/fbc-venue-hibachi` holds its signer to (FBC-8mzv): what the Java
FueledByChaiTrading library packs and signs for each command, on a synthetic key (`SYNTHETIC`
says where every value comes from).

- `HibachiSignOracle.java` packs every command with the Java library's `HibachiPayloadPacker`
  and signs it with `EcdsaHibachiSigner`, and writes `hibachi-vectors.tsv`.
- `run-oracle.sh` compiles and runs it against a FueledByChaiTrading checkout and records the
  checkout's commit and its count of local changes to the four signer files in the header.
- `hibachi-vectors.tsv` is what it wrote; `tests/java_vectors.rs`, `tests/order_signer.rs`,
  `tests/auth.rs` and `tests/no_leak.rs` in the venue crate read it.

## How it was run

On 2026-10-27, against the public library at
[`b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27`](https://github.com/FueledByChai/FueledByChaiTrading/tree/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/commons/hibachi-common-api),
with no local change to the signer files, OpenJDK 26.0.1 and Maven 3.9.16 resolving offline
(web3j 4.10.3, bcprov-jdk18on 1.73):

```
FBC_JAVA_ROOT=/path/to/FueledByChaiTrading fixtures/hibachi/signing/run-oracle.sh
```

It needs `commons/hibachi-common-api` compiled (`mvn -pl commons/hibachi-common-api -am
compile`) and writes nothing outside this directory and a temporary build directory. Run again,
it writes the same file: the signatures are deterministic.

## What Java signs

Sources, at the commit above (`P` is
`commons/hibachi-common-api/src/main/java/com/fueledbychai/hibachi/common/api`):

- [`P/signer/HibachiPayloadPacker.java#L59-L110`](https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/commons/hibachi-common-api/src/main/java/com/fueledbychai/hibachi/common/api/signer/HibachiPayloadPacker.java#L59-L110):
  a place or modify packs nonce u64, contract id u32, quantity u64 (× 10^underlyingDecimals),
  side u32 (ASK 0, BID 1), price u64 (× 2^32 × 10^(settlementDecimals − underlyingDecimals),
  left out for a market order) and max fees u64 (× 10^8), big-endian: 40 bytes, or 32 without a
  price. A cancel packs the order id or the order's nonce as 8 bytes; a cancel-all its nonce.
  Every scaled value is truncated toward zero and refused past 63 bits (L146-L152).
- [`P/signer/EcdsaHibachiSigner.java#L36-L53`](https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/commons/hibachi-common-api/src/main/java/com/fueledbychai/hibachi/common/api/signer/EcdsaHibachiSigner.java#L36-L53):
  SHA-256 of the payload, signed by web3j's `ECKeyPair.sign` (BouncyCastle's `ECDSASigner` with
  `HMacDSAKCalculator` over SHA-256, which is RFC 6979, then a low s), written r || s || v with
  v the recovery id 0 or 1, as 130 lowercase hex characters. k256 0.13.4 gives the same 65
  bytes for every row.

The Rust signer packs from the same decimal inputs, then refuses what Java would let through:
a price off `tick_size` or a quantity off `step_size` (Java checks neither; these two columns
are the Rust signer's inputs only, and every row lies on them), a quantity or max fees that do
not scale exactly (Java truncates them), and a negative value (Java writes it as two's
complement).

## Columns

`name`, `kind` (`place`, `modify`, `cancel_order_id`, `cancel_nonce`, `cancel_all`, `arm`),
`nonce`, `contract_id`, `underlying_decimals`, `settlement_decimals`, `tick_size`, `step_size`,
`side` (`BID`, `ASK`), `quantity`, `price` (empty for a market order or a modify naming no
price), `max_fees`, `order_id`, `outcome` (`signed`, or `refused` where Java's packer refuses a
value past 63 bits), `payload`, `sha256`, `signature` and `documented` (the buffer a Hibachi
page prints for that row, which the oracle checks its payload against).

- A `modify` row is packed as a place under the modify's own fresh nonce, as Java's
  `HibachiTranslator` packs it; the order it changes is named only in the request's JSON.
- An `arm` row is the cancel-on-disconnect arm's 8-byte nonce. Java has no function for the arm
  (it sends `orders.enableCancelOnDisconnect` unsigned, as the SDK at 14a76b9 does), so its
  vector is `packCancelAll(nonce)`, the same 8 bytes. Which form Hibachi takes is FBC-xvtr's
  probe to find.
- A cancel by client id (its ASCII bytes, which only Hibachi's Postman collection describes) has
  no row: neither Java nor the SDK has the path, so the signer refuses it.

## The docs' worked examples

From [Hibachi's signing page](https://hibachi-docs.redocly.app/api/signing) and the Signing
page and Account API / Trade folder of [Hibachi's API collection](https://api-doc.hibachi.xyz/),
as read on 2026-10-27:

- `docs_example`: BTC/USDT-P, contract 2, underlying 10, settlement 6, ASK, nonce
  1714701600000000, quantity 1, price 100000. Both pages print the 40-byte buffer in
  `documented` for max fees "0.0005", packing the fee as 5000 (`0x1388`). That is 0.0005 ×
  10^7: the pages say rate fees scale by 10^8, as the SDK and Java do, so the buffer is
  reproduced from max fees 0.00005. `docs_inputs_fee_0_0005` is the same order with max fees
  0.0005, its fee packed as 50000 (`0x000000000000c350`).
- `docs_example_fee_0`: the Trade folder's version of the example, max fees 0.
- `docs_cancel`: both pages' cancel of order 579183763093760000, 8 bytes `0x0809ac905ae0a800`.
- The pages state no tick or step for the example; 0.1 and 10^-10 are made up so its price and
  quantity lie on them.

Other errors in those pages, kept out of the vectors: the fixed-fee example writes 1230000 as
`0x00000000012C4B00` (that is 19,680,000; 1230000 is `0x12C4B0`); the redocly page writes the
price exponent as underlyingDecimals − settlementDecimals ("10^(10 - 6 = -4)"), while the API
collection, the example's own value, Java and the SDK use settlementDecimals −
underlyingDecimals; the collection lists `creationDeadline` and `triggerPrice` among the signed
fields of `POST /trade/order` and documents an 8-byte price limit for market orders, which
neither Java, the SDK nor the docs' JavaScript example packs; and the fee's unit is given as
basis points while the examples pass a fraction. The signer packs what Java and the SDK pack.

## Other rows

- `truncation_2500_01`: ETH's decimals (9 and 6) on a 0.01 tick: 2500.01 × 2^32 × 10^-3 =
  10737461189.67296, packed truncated as 10737461189.
- `java_test_*`: the inputs of the library's `HibachiPayloadPackerTest`, whose expected hex the
  oracle checks the packer against before writing anything.
- `edge_*`: settlement above underlying, zero and eight-place fees, an on-grid price that scales
  to zero, scaled values at and past 2^63 − 1 (the refused rows), contract id 2^32 − 1 (Java's
  int −1), and the largest nonce and order id.
- `random_*`: 1000 commands from `java.util.Random` with the header's seed, every price and
  quantity on its row's grid and every fee a whole multiple of 10^-8.
