import com.fueledbychai.hibachi.common.api.HibachiContract;
import com.fueledbychai.hibachi.common.api.order.HibachiSide;
import com.fueledbychai.hibachi.common.api.signer.EcdsaHibachiSigner;
import com.fueledbychai.hibachi.common.api.signer.HibachiPayloadPacker;

import org.web3j.crypto.ECKeyPair;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Random;

/**
 * The Java oracle for the Rust Hibachi signer (FBC-8mzv). It packs every command with the Java
 * FueledByChaiTrading HibachiPayloadPacker, exactly as HibachiTranslator and HibachiBroker call
 * it, and signs the packed bytes with EcdsaHibachiSigner (SHA-256, then secp256k1 through web3j
 * 4.10.3's ECKeyPair.sign, an RFC 6979 nonce and a low s, as r || s || v with v the recovery
 * id 0 or 1, in lowercase hex).
 *
 * Kinds: place (limit or market: a market order has no price), modify (Java packs a modify
 * with packPlaceOrder, as HibachiTranslator does, under the modify's own nonce), cancel_order_id
 * and cancel_nonce (packCancelOrder by order id, or by the order's placement nonce),
 * cancel_all (packCancelAll) and arm, the cancel-on-disconnect arm's 8-byte nonce: Java has no
 * function for the arm (it sends orders.enableCancelOnDisconnect unsigned), so the arm's
 * vector is packCancelAll(nonce), the same 8-byte big-endian nonce.
 *
 * Writes hibachi-vectors.tsv (or the path given as the first argument): fixed vectors, the
 * worked examples of Hibachi's signing docs (each payload checked here against the buffer the
 * docs print, in the documented column), the Java library's own packer-test vectors (checked
 * against the literals of HibachiPayloadPackerTest), edges, and RANDOM_INTENTS seeded random
 * commands. A command the packer refuses (a scaled value past 63 bits) is written with outcome
 * refused and no payload. tick_size and step_size are inputs of the Rust signer only, which
 * refuses a price or quantity off them; Java checks neither, and this oracle only writes values
 * on them. The key is SHA-256 of KEY_PHRASE, made up for these vectors (see SYNTHETIC).
 * run-oracle.sh compiles and runs it against a FueledByChaiTrading checkout and passes the
 * checkout's commit and its count of local changes to the signer files as the second and third
 * arguments.
 */
public class HibachiSignOracle {
    static final String KEY_PHRASE = "fueledbychai-rs FBC-8mzv synthetic Hibachi signing key, belonging to no account";
    static final long SEED = 20261027L;
    static final int RANDOM_INTENTS = 1000;
    static final String[] COLUMNS = { "name", "kind", "nonce", "contract_id", "underlying_decimals",
            "settlement_decimals", "tick_size", "step_size", "side", "quantity", "price", "max_fees", "order_id",
            "outcome", "payload", "sha256", "signature", "documented" };
    static final HexFormat HEX = HexFormat.of();
    /** The order of secp256k1's generator: a key lies in [1, N). */
    static final BigInteger N = new BigInteger("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141", 16);

    final EcdsaHibachiSigner signer;
    final PrintWriter out;
    int rows = 0;

    HibachiSignOracle(PrintWriter out, String keyHex) {
        this.out = out;
        this.signer = new EcdsaHibachiSigner(keyHex);
    }

    /** A contract as the Java library builds it from exchange-info, with the Rust-only grid. */
    record Contract(int id, int underlying, int settlement, String tick, String step) {
        HibachiContract java() {
            return HibachiContract.builder().id(id).symbol("SYNTH/USDT-P").underlyingDecimals(underlying)
                    .settlementDecimals(settlement).build();
        }
    }

    // ETH/USDT-P's public id, decimals, tick and step (api-doc.hibachi.xyz, the SDK's test data).
    static final Contract ETH = new Contract(1, 9, 6, "0.000001", "0.000000001");
    // The signing docs' BTC/USDT-P: contract 2, underlying 10, settlement 6. The docs state no
    // tick or step; these are made up so the example's price and quantity lie on them.
    static final Contract BTC = new Contract(2, 10, 6, "0.1", "0.0000000001");
    // EUR/USDT-P's decimals (the Java library's test values): settlement above underlying.
    static final Contract EUR = new Contract(2, 2, 6, "0.00001", "0.01");
    // ETH's decimals on a 0.01 tick: the truncation vector of BT-602 and FBC-8mzv.
    static final Contract ETH_CENT = new Contract(1, 9, 6, "0.01", "0.000000001");

    /** Packs and signs a place or modify; a null price is a market order. */
    void order(String name, String kind, long nonce, Contract c, HibachiSide side, String qty, String price,
            String fees, String documented) {
        check(onGrid(qty, c.step) && (price == null || onGrid(price, c.tick)), name + ": off the grid");
        byte[] payload;
        try {
            payload = HibachiPayloadPacker.packPlaceOrder(nonce, c.java(), new BigDecimal(qty), side,
                    price == null ? null : new BigDecimal(price), new BigDecimal(fees));
        } catch (IllegalArgumentException refused) {
            check(documented.isEmpty(), name + ": a documented vector was refused");
            row(name, kind, Long.toString(nonce), c, side.name(), qty, price, fees, "", null, documented);
            return;
        }
        check(payload.length == (price == null ? 32 : 40), name + ": payload length");
        row(name, kind, Long.toString(nonce), c, side.name(), qty, price, fees, "", payload, documented);
    }

    void cancelById(String name, long orderId, String documented) {
        row(name, "cancel_order_id", "", null, "", "", null, "", Long.toString(orderId),
                HibachiPayloadPacker.packCancelOrder(orderId, null), documented);
    }

    void cancelByNonce(String name, long placementNonce) {
        row(name, "cancel_nonce", Long.toString(placementNonce), null, "", "", null, "", "",
                HibachiPayloadPacker.packCancelOrder(null, placementNonce), "");
    }

    void nonceOnly(String name, String kind, long nonce) {
        row(name, kind, Long.toString(nonce), null, "", "", null, "", "", HibachiPayloadPacker.packCancelAll(nonce),
                "");
    }

    /** Writes a row; a null payload is a command the packer refused. */
    void row(String name, String kind, String nonce, Contract c, String side, String qty, String price, String fees,
            String orderId, byte[] payload, String documented) {
        String outcome = payload == null ? "refused" : "signed";
        String hex = payload == null ? "" : HEX.formatHex(payload);
        String digest = payload == null ? "" : HEX.formatHex(sha256(payload));
        String signature = payload == null ? "" : signer.sign(payload);
        if (payload != null) {
            check(signature.length() == 130 && signature.equals(signature.toLowerCase()), name + ": signature form");
            check(signature.endsWith("00") || signature.endsWith("01"), name + ": v is not 0 or 1");
            check(signature.equals(signer.sign(payload)), name + ": signature is not deterministic");
        }
        if (!documented.isEmpty()) {
            check(documented.equals(hex), name + ": the payload differs from the documented buffer");
        }
        String[] cells = { name, kind, nonce, c == null ? "" : Integer.toUnsignedString(c.id),
                c == null ? "" : Integer.toString(c.underlying), c == null ? "" : Integer.toString(c.settlement),
                c == null ? "" : c.tick, c == null ? "" : c.step, side, qty, price == null ? "" : price, fees, orderId,
                outcome, hex, digest, signature, documented };
        for (String cell : cells) {
            check(cell.indexOf('\t') < 0 && cell.indexOf('\n') < 0, name + ": a cell holds a tab or newline");
        }
        out.println(String.join("\t", cells));
        rows++;
    }

    static boolean onGrid(String value, String step) {
        return new BigDecimal(value).remainder(new BigDecimal(step)).signum() == 0;
    }

    static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static void check(boolean ok, String what) {
        if (!ok) {
            throw new IllegalStateException(what);
        }
    }

    /** The Java library's own literals (HibachiPayloadPackerTest), checked against its packer. */
    static void javaTestLiterals() {
        HibachiContract eth = ETH.java();
        check(HEX.formatHex(HibachiPayloadPacker.packPlaceOrder(1L, eth, new BigDecimal("0.01"), HibachiSide.BID,
                new BigDecimal("2500.0"), new BigDecimal("0.5")))
                .equals("00000000000000010000000100000000009896800000000100000002800000000000000002faf080"),
                "HibachiPayloadPackerTest limit bid");
        check(HEX.formatHex(HibachiPayloadPacker.packPlaceOrder(1L, eth, new BigDecimal("0.01"), HibachiSide.ASK,
                null, new BigDecimal("0.5"))).equals("0000000000000001000000010000000000989680000000000000000002faf080"),
                "HibachiPayloadPackerTest market ask");
        check(HEX.formatHex(HibachiPayloadPacker.packCancelOrder(12345L, null)).equals("0000000000003039"),
                "HibachiPayloadPackerTest cancel by order id");
        check(HEX.formatHex(HibachiPayloadPacker.packCancelOrder(null, 1234567890L)).equals("00000000499602d2"),
                "HibachiPayloadPackerTest cancel by nonce");
    }

    void fixed() {
        long n = 1761500000000000L;
        // One of each command the signer signs.
        order("limit_buy", "place", n, ETH, HibachiSide.BID, "0.25", "2501.123456", "0.00045", "");
        order("limit_sell", "place", n + 1, BTC, HibachiSide.ASK, "0.015", "101234.5", "0.0005", "");
        order("market_buy", "place", n + 2, ETH, HibachiSide.BID, "1.5", null, "0.00045", "");
        order("market_sell", "place", n + 3, BTC, HibachiSide.ASK, "0.002", null, "0.0005", "");
        // A modify of limit_buy: its own fresh nonce, the full post-modify order.
        order("modify", "modify", n + 4, ETH, HibachiSide.BID, "0.3", "2502.5", "0.00045", "");
        // A modify that names no price (an untriggered trigger order's): 32 bytes, as a market.
        order("modify_no_price", "modify", n + 5, ETH, HibachiSide.ASK, "0.3", null, "0.00045", "");
        cancelById("cancel_order_id", 579183763093760001L, "");
        cancelByNonce("cancel_nonce", n);
        nonceOnly("cancel_all", "cancel_all", n + 6);
        nonceOnly("arm", "arm", n + 7);

        // Hibachi's signing docs. Their worked example
        // (https://hibachi-docs.redocly.app/api/signing, and the Signing page of
        // https://api-doc.hibachi.xyz/) prints this 40-byte buffer for max fees "0.0005", packing
        // the fee as 5000 (0x1388): that is 0.00005 x 10^8, so the buffer is reproduced from max
        // fees 0.00005 (FBC-8mzv). With max fees 0.0005, the fee is 50000 (0xc350).
        String docsBuffer = "0006178313c388000000000200000002540be400000000000000000a000000000000000000001388";
        order("docs_example", "place", 1714701600000000L, BTC, HibachiSide.ASK, "1", "100000", "0.00005", docsBuffer);
        order("docs_inputs_fee_0_0005", "place", 1714701600000000L, BTC, HibachiSide.ASK, "1", "100000", "0.0005",
                "");
        // The Trade folder of https://api-doc.hibachi.xyz/ prints the same order with max fees 0.
        String tradeBuffer = "0006178313c388000000000200000002540be400000000000000000a000000000000000000000000";
        order("docs_example_fee_0", "place", 1714701600000000L, BTC, HibachiSide.ASK, "1", "100000", "0",
                tradeBuffer);
        // Both pages: cancelling order 579183763093760000 signs it as 8 bytes, 0x0809ac905ae0a800.
        cancelById("docs_cancel", 579183763093760000L, "0809ac905ae0a800");

        // The truncation vector: 2500.01 x 2^32 x 10^-3 = 10737461189.67296, packed 10737461189.
        order("truncation_2500_01", "place", n + 8, ETH_CENT, HibachiSide.BID, "0.01", "2500.01", "0.00045", "");

        // The Java library's packer-test inputs (HibachiPayloadPackerTest, checked in
        // javaTestLiterals): nonce 1 on ETH's decimals.
        Contract ethTest = new Contract(1, 9, 6, "0.1", "0.000000001");
        order("java_test_limit_bid", "place", 1L, ethTest, HibachiSide.BID, "0.01", "2500.0", "0.5", "");
        order("java_test_market_ask", "place", 1L, ethTest, HibachiSide.ASK, "0.01", null, "0.5", "");
        cancelById("java_test_cancel_order_id", 12345L, "");
        cancelByNonce("java_test_cancel_nonce", 1234567890L);

        // Edges.
        order("edge_settlement_above_underlying", "place", n + 9, EUR, HibachiSide.ASK, "1250.5", "1.08765",
                "0.0002", "");
        order("edge_fee_zero", "place", n + 10, ETH, HibachiSide.BID, "0.1", "2400", "0", "");
        order("edge_fee_eight_places", "place", n + 11, ETH, HibachiSide.ASK, "0.1", "2400", "0.12345678", "");
        // A price on its tick whose scaled value truncates to zero: packed as 0, as Java and the
        // SDK pack it.
        Contract fineUnderlying = new Contract(7, 12, 0, "0.000001", "0.000000000001");
        order("edge_price_scales_to_zero", "place", n + 12, fineUnderlying, HibachiSide.BID, "1", "0.000001", "0.0001",
                "");
        // Scaled values at 63 bits and just past them: Java refuses past 2^63 - 1, and so must Rust.
        Contract flat = new Contract(4, 0, 0, "1", "1");
        order("edge_price_below_2_63", "place", n + 13, flat, HibachiSide.BID, "1", "2147483647", "0.0001", "");
        order("edge_price_at_2_63", "place", n + 14, flat, HibachiSide.BID, "1", "2147483648", "0.0001", "");
        Contract fineQty = new Contract(5, 18, 6, "1", "0.000000000000000001");
        order("edge_quantity_max", "place", n + 15, fineQty, HibachiSide.ASK, "9.223372036854775807", "3000000",
                "0.0001", "");
        order("edge_quantity_past_max", "place", n + 16, fineQty, HibachiSide.ASK, "9.223372036854775808", "3000000",
                "0.0001", "");
        Contract highSettlement = new Contract(6, 0, 18, "1", "1");
        order("edge_price_overflow", "place", n + 17, highSettlement, HibachiSide.BID, "1", "1", "0.0001", "");
        // Contract id 2^32 - 1: Java's int -1, which putInt writes as ffffffff, the u32 the SDK packs.
        order("edge_contract_id_max", "place", n + 18, new Contract(-1, 9, 6, "0.01", "0.001"), HibachiSide.BID,
                "0.5", "99.99", "0.0005", "");
        nonceOnly("edge_nonce_max", "cancel_all", Long.MAX_VALUE);
        cancelById("edge_order_id_max", Long.MAX_VALUE, "");
        cancelByNonce("edge_cancel_nonce_zero", 0L);
    }

    void random() {
        Random rnd = new Random(SEED);
        String[] kinds = { "place", "place", "place", "place", "place", "place", "place", "place", "place", "place",
                "market", "market", "market", "modify", "modify", "modify", "modify_no_price", "cancel_order_id",
                "cancel_nonce", "cancel_all", "arm" };
        for (int i = 0; i < RANDOM_INTENTS; i++) {
            String name = String.format("random_%04d", i);
            String kind = kinds[rnd.nextInt(kinds.length)];
            long nonce = 1700000000000000L + (long) (rnd.nextDouble() * 100000000000000L);
            switch (kind) {
                case "cancel_order_id" -> cancelById(name, 500000000000000000L + (long) (rnd.nextDouble() * 1e17),
                        "");
                case "cancel_nonce" -> cancelByNonce(name, nonce);
                case "cancel_all", "arm" -> nonceOnly(name, kind, nonce);
                default -> {
                    int underlying = rnd.nextInt(13);
                    int settlement = rnd.nextInt(9);
                    BigDecimal tick = new BigDecimal(new int[] { 1, 2, 5 }[rnd.nextInt(3)])
                            .scaleByPowerOfTen(-rnd.nextInt(10));
                    BigDecimal step = BigDecimal.ONE.scaleByPowerOfTen(-rnd.nextInt(underlying + 1));
                    Contract c = new Contract(1 + rnd.nextInt(Integer.MAX_VALUE), underlying, settlement,
                            tick.toPlainString(), step.toPlainString());
                    String qty = step.multiply(BigDecimal.valueOf(1 + rnd.nextInt(10_000_000))).toPlainString();
                    BigDecimal ceiling = BigDecimal.ONE.scaleByPowerOfTen(rnd.nextInt(8));
                    long ticks = 1 + (long) (rnd.nextDouble() * ceiling.divide(tick).doubleValue());
                    String price = kind.equals("market") || kind.equals("modify_no_price") ? null
                            : tick.multiply(BigDecimal.valueOf(ticks)).toPlainString();
                    // Whole multiples of 1e-8 of every magnitude: the Rust signer refuses a fee that
                    // does not scale exactly, which Java would truncate.
                    String fees = BigDecimal.valueOf(rnd.nextInt(BigInteger.TEN.pow(1 + rnd.nextInt(8)).intValue()))
                            .scaleByPowerOfTen(-8).stripTrailingZeros().toPlainString();
                    HibachiSide side = rnd.nextBoolean() ? HibachiSide.BID : HibachiSide.ASK;
                    String rowKind = kind.equals("market") ? "place" : kind.equals("modify_no_price") ? "modify" : kind;
                    order(name, rowKind, nonce, c, side, qty, price, fees, "");
                }
            }
        }
    }

    public static void main(String[] a) throws Exception {
        Path path = Path.of(a.length > 0 ? a[0] : "hibachi-vectors.tsv");
        String commit = a.length > 1 ? a[1] : "unknown";
        String localChanges = a.length > 2 ? a[2] : "unknown";
        String keyHex = "0x" + HEX.formatHex(sha256(KEY_PHRASE.getBytes(StandardCharsets.UTF_8)));
        BigInteger secret = new BigInteger(keyHex.substring(2), 16);
        check(secret.signum() > 0 && secret.compareTo(N) < 0, "the key is not in [1, N)");
        BigInteger publicKey = ECKeyPair.create(secret).getPublicKey();
        String uncompressed = "04" + String.format("%0128x", publicKey);
        javaTestLiterals();
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            out.println("# Hibachi signing vectors written by HibachiSignOracle.java (FBC-8mzv); synthetic, see SYNTHETIC.");
            out.println("# Java packer and signer: FueledByChaiTrading HibachiPayloadPacker and EcdsaHibachiSigner.");
            out.println("# java_commit=" + commit);
            out.println("# java_local_changes=" + localChanges);
            out.println("# key_phrase=" + KEY_PHRASE);
            out.println("# key=" + keyHex);
            out.println("# public_key=" + uncompressed);
            out.println("# seed=" + SEED);
            out.println("# random_intents=" + RANDOM_INTENTS);
            out.println(String.join("\t", COLUMNS));
            HibachiSignOracle o = new HibachiSignOracle(out, keyHex);
            o.fixed();
            o.random();
            System.out.println("wrote " + o.rows + " vectors to " + path);
        }
    }
}
