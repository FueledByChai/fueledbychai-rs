import com.fueledbychai.paradex.common.api.ParadexMessageSigner;
import com.fueledbychai.paradex.common.api.ParadexTypedDataSigner;
import com.fueledbychai.paradex.common.api.order.OrderType;
import com.fueledbychai.paradex.common.api.order.ParadexOrder;
import com.fueledbychai.paradex.common.api.order.Side;
import com.swmansion.starknet.crypto.StarknetCurve;
import com.swmansion.starknet.crypto.StarknetCurveSignature;
import com.swmansion.starknet.data.TypedData;
import com.swmansion.starknet.data.types.Felt;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The Java oracle for the Rust Paradex signer (FBC-6). It drives the Java FueledByChaiTrading
 * signer exactly as ParadexRestApi does: a ParadexOrder carries the decimal size and price, its
 * getChainSize()/getChainLimitPrice() do the x1e8 scaling (BigDecimal.scaleByPowerOfTen(8)
 * .toBigInteger(), which truncates), and ParadexTypedDataSigner signs the result. The message
 * hash is the one ParadexTypedDataSigner itself builds (its private message builder and static
 * type maps, read by reflection), so the file records Java's hash and Java's signature for the
 * same message.
 *
 * Writes paradex-vectors.tsv (or the path given as the first argument): fixed vectors (buy and
 * sell limit orders, market orders, a ModifyOrder, the auth Request, decimal edge cases) and
 * RANDOM_INTENTS seeded random decimal intents with 9 to 18 decimal places. Every input is
 * synthetic (see SYNTHETIC). run-oracle.sh compiles and runs it against a FueledByChaiTrading
 * checkout. "--bench" as the second argument also prints the signer's warm timing.
 */
public class ParadexHashOracle {
    static final String ACCOUNT = "0x129f3dc1b8962d8a87abc692424c78fda963ade0e1cd17bf3d1c26f8d41ee7a";
    static final String KEY = "0x0139fe4d6f02e666e86a6f58e65060f115cd3c185bd9e98bd829636931458f79";
    static final BigInteger CHAIN = new BigInteger("8458834024819506728615521019831122032732688838300957472069977523540");
    static final long SEED = 20261003L;
    static final int RANDOM_INTENTS = 2000;
    static final String[] MARKETS = { "BTC-USD-PERP", "ETH-USD-PERP", "SOL-USD-PERP", "HYPE-USD-PERP", "kBONK-USD-PERP",
            "XRP-USD-PERP", "DOGE-USD-PERP", "PAXG-USD-PERP" };
    static final String[] COLUMNS = { "name", "kind", "timestamp", "market", "side", "order_type", "size", "price",
            "order_id", "method", "path", "body", "expiration", "chain_size", "chain_price", "hash", "r", "s" };

    final ParadexTypedDataSigner signer;
    final ParadexMessageSigner legacy;
    final Felt account = Felt.fromHex(ACCOUNT);
    final Felt publicKey = StarknetCurve.getPublicKey(Felt.fromHex(KEY));
    final PrintWriter out;
    final Method orderJson, modifyJson, requestJson;
    final Map<String, List<TypedData.Type>> typesOrder, typesModify, typesRequest;
    int rows = 0;

    ParadexHashOracle(PrintWriter out, String chainHex) throws Exception {
        this.out = out;
        this.signer = new ParadexTypedDataSigner(ACCOUNT, KEY, chainHex);
        this.legacy = new ParadexMessageSigner(ACCOUNT, KEY, CHAIN);
        Class<?> c = ParadexTypedDataSigner.class;
        orderJson = c.getDeclaredMethod("buildOrderMessageJson", long.class, String.class, String.class, String.class,
                String.class, String.class);
        modifyJson = c.getDeclaredMethod("buildModifyMessageJson", long.class, String.class, String.class,
                String.class, String.class, String.class, String.class);
        requestJson = c.getDeclaredMethod("buildRequestMessageJson", String.class, String.class, String.class,
                long.class, long.class);
        for (Method m : new Method[] { orderJson, modifyJson, requestJson }) {
            m.setAccessible(true);
        }
        typesOrder = types(c, "TYPES_ORDER");
        typesModify = types(c, "TYPES_MODIFY");
        typesRequest = types(c, "TYPES_REQUEST");
    }

    @SuppressWarnings("unchecked")
    static Map<String, List<TypedData.Type>> types(Class<?> c, String name) throws Exception {
        Field f = c.getDeclaredField(name);
        f.setAccessible(true);
        return (Map<String, List<TypedData.Type>>) f.get(null);
    }

    static ParadexOrder order(String market, Side side, OrderType type, String size, String price, String id) {
        ParadexOrder o = new ParadexOrder();
        o.setTicker(market);
        o.setSide(side);
        o.setOrderType(type);
        o.setSize(new BigDecimal(size));
        o.setLimitPrice(price == null ? null : new BigDecimal(price));
        o.setOrderId(id);
        return o;
    }

    /** Signs a new order the way ParadexRestApi.buildSignedPlaceOrderJson does. */
    void place(String name, long ts, String market, Side side, OrderType type, String size, String price)
            throws Exception {
        ParadexOrder o = order(market, side, type, size, price, null);
        String chainSize = o.getChainSize().toString();
        String chainPrice = o.getChainLimitPrice().toString();
        StarknetCurveSignature sig = signer.signOrder(ts, o.getTicker(), o.getSide().getChainSide(),
                o.getOrderType().toString(), chainSize, chainPrice);
        String json = (String) orderJson.invoke(signer, ts, o.getTicker(), o.getSide().getChainSide(),
                o.getOrderType().toString(), chainSize, chainPrice);
        Felt hash = new TypedData(typesOrder, "Order", signer.getDomainJson(), json).getMessageHash(account);
        row(name, "order", Long.toString(ts), market, side.name(), type.name(), size, price == null ? "" : price, "",
                "", "", "", "", chainSize, chainPrice, hash, sig);
    }

    /** Signs a modify the way ParadexRestApi.buildSignedModifyOrderJson does: the order id is signed. */
    void modify(String name, long ts, String market, Side side, OrderType type, String size, String price, String id)
            throws Exception {
        ParadexOrder o = order(market, side, type, size, price, id);
        String chainSize = o.getChainSize().toString();
        String chainPrice = o.getChainLimitPrice().toString();
        StarknetCurveSignature sig = signer.signModifyOrder(ts, o.getTicker(), o.getSide().getChainSide(),
                o.getOrderType().toString(), chainSize, chainPrice, o.getOrderId());
        String json = (String) modifyJson.invoke(signer, ts, o.getTicker(), o.getSide().getChainSide(),
                o.getOrderType().toString(), chainSize, chainPrice, o.getOrderId());
        Felt hash = new TypedData(typesModify, "ModifyOrder", signer.getDomainJson(), json).getMessageHash(account);
        row(name, "modify", Long.toString(ts), market, side.name(), type.name(), size, price == null ? "" : price, id,
                "", "", "", "", chainSize, chainPrice, hash, sig);
    }

    /** Signs a Request; the auth one through signAuthRequestAsParadexArray, as getJwtTokenSingleTry does. */
    void request(String name, String method, String path, String body, long ts, long exp) throws Exception {
        StarknetCurveSignature sig = signer.signRequest(method, path, body, ts, exp);
        if (method.equals("POST") && path.equals("/v1/auth") && body.isEmpty()) {
            String auth = signer.signAuthRequestAsParadexArray(ts, exp);
            check(auth.equals(array(sig)), name + ": signAuthRequestAsParadexArray disagrees with signRequest");
        }
        String json = (String) requestJson.invoke(signer, method, path, body, ts, exp);
        Felt hash = new TypedData(typesRequest, "Request", signer.getDomainJson(), json).getMessageHash(account);
        row(name, "request", Long.toString(ts), "", "", "", "", "", "", method, path, body, Long.toString(exp), "", "",
                hash, sig);
    }

    static String array(StarknetCurveSignature sig) {
        return "[\"" + sig.getR().getValue() + "\",\"" + sig.getS().getValue() + "\"]";
    }

    void row(String name, String kind, String ts, String market, String side, String type, String size, String price,
            String id, String method, String path, String body, String exp, String chainSize, String chainPrice,
            Felt hash, StarknetCurveSignature sig) {
        check(StarknetCurve.verify(publicKey, hash, sig.getR(), sig.getS()), name + ": signature does not verify");
        String[] cells = { name, kind, ts, market, side, type, size, price, id, method, path, body, exp, chainSize,
                chainPrice, hash.hexString(), sig.getR().hexString(), sig.getS().hexString() };
        for (String cell : cells) {
            check(cell.indexOf('\t') < 0 && cell.indexOf('\n') < 0, name + ": a cell holds a tab or newline");
        }
        out.println(String.join("\t", cells));
        rows++;
    }

    static void check(boolean ok, String what) {
        if (!ok) {
            throw new IllegalStateException(what);
        }
    }

    /** A plain decimal with intDigits integer digits (at most) and fracDigits decimal places. */
    static String decimal(Random rnd, int intDigits, int fracDigits) {
        StringBuilder sb = new StringBuilder();
        long whole = intDigits == 0 ? 0 : (long) (rnd.nextDouble() * Math.pow(10, intDigits));
        sb.append(whole).append('.');
        for (int i = 0; i < fracDigits; i++) {
            sb.append((char) ('0' + rnd.nextInt(10)));
        }
        return sb.toString();
    }

    static String digits(Random rnd, int n) {
        StringBuilder sb = new StringBuilder();
        sb.append((char) ('1' + rnd.nextInt(9)));
        for (int i = 1; i < n; i++) {
            sb.append((char) ('0' + rnd.nextInt(10)));
        }
        return sb.toString();
    }

    void fixed() throws Exception {
        long ts = 1759400000123L;
        place("buy_limit", ts, "BTC-USD-PERP", Side.BUY, OrderType.LIMIT, "0.01", "65123.45");
        place("sell_limit", ts + 1, "ETH-USD-PERP", Side.SELL, OrderType.LIMIT, "1.25", "2450.7");
        place("market_buy", ts + 2, "SOL-USD-PERP", Side.BUY, OrderType.MARKET, "3", null);
        place("market_sell", ts + 3, "BTC-USD-PERP", Side.SELL, OrderType.MARKET, "0.5", null);
        modify("modify", ts + 4, "BTC-USD-PERP", Side.SELL, OrderType.LIMIT, "0.02", "65200.1",
                "1759400000123201703010000");
        request("auth_request", "POST", "/v1/auth", "", 1759400000L, 1759403600L);
        // Decimal edges: below one unit of 1e-8 (truncates to zero), exactly 8 places, trailing
        // zeros past 8 places, an integer, a scaled value past i64, and 18 places.
        place("edge_sub_unit", ts + 5, "kBONK-USD-PERP", Side.BUY, OrderType.LIMIT, "0.000000009", "0.000000019");
        place("edge_eight_places", ts + 6, "kBONK-USD-PERP", Side.SELL, OrderType.LIMIT, "123.12345678",
                "0.00001234");
        place("edge_trailing_zeros", ts + 7, "ETH-USD-PERP", Side.BUY, OrderType.LIMIT, "2.500000000000",
                "2450.700000000000");
        place("edge_integer", ts + 8, "SOL-USD-PERP", Side.SELL, OrderType.LIMIT, "7", "150");
        place("edge_past_i64", ts + 9, "BTC-USD-PERP", Side.BUY, OrderType.LIMIT, "99999999999.999999999",
                "99999999999.123456789");
        place("edge_eighteen_places", ts + 10, "HYPE-USD-PERP", Side.SELL, OrderType.LIMIT, "0.123456789123456789",
                "45.987654321987654321");
        modify("edge_modify_truncates", ts + 11, "ETH-USD-PERP", Side.BUY, OrderType.LIMIT, "1.000000019",
                "2450.123456789", "1759400000123201703010001");
    }

    void random() throws Exception {
        Random rnd = new Random(SEED);
        for (int i = 0; i < RANDOM_INTENTS; i++) {
            long ts = 1700000000000L + (long) (rnd.nextDouble() * 200000000000L);
            String market = MARKETS[rnd.nextInt(MARKETS.length)];
            Side side = rnd.nextBoolean() ? Side.BUY : Side.SELL;
            String size = decimal(rnd, rnd.nextInt(7), 9 + rnd.nextInt(10));
            String price = decimal(rnd, rnd.nextInt(8), 9 + rnd.nextInt(10));
            int pick = rnd.nextInt(10);
            String name = String.format("random_%04d", i);
            if (pick < 6) {
                place(name, ts, market, side, OrderType.LIMIT, size, price);
            } else if (pick < 8) {
                place(name, ts, market, side, OrderType.MARKET, size, null);
            } else {
                modify(name, ts, market, side, OrderType.LIMIT, size, price, digits(rnd, 20 + rnd.nextInt(10)));
            }
        }
    }

    /**
     * ParadexMessageSigner, the older Java signer, draws its nonce differently (starknet-jvm's
     * StarknetCurve.sign, not RFC 6979 over BouncyCastle), so its signatures differ; it must
     * still sign the same message hash, so its signature verifies against that hash.
     */
    void crossCheckLegacy() throws Exception {
        long ts = 1759400000123L;
        String json = (String) orderJson.invoke(signer, ts, "BTC-USD-PERP", "1", "LIMIT", "1000000", "6512345000000");
        Felt h = new TypedData(typesOrder, "Order", signer.getDomainJson(), json).getMessageHash(account);
        verifyArray(legacy.signOrderMessageDirect(ts, "BTC-USD-PERP", "1", "LIMIT", "1000000", "6512345000000"), h,
                "ParadexMessageSigner order");
        String id = "1759400000123201703010000";
        json = (String) modifyJson.invoke(signer, ts, "BTC-USD-PERP", "2", "LIMIT", "2000000", "6520010000000", id);
        h = new TypedData(typesModify, "ModifyOrder", signer.getDomainJson(), json).getMessageHash(account);
        verifyArray(legacy.signModifyOrderMessageDirect(ts, "BTC-USD-PERP", "2", "LIMIT", "2000000", "6520010000000",
                id), h, "ParadexMessageSigner modify");
    }

    void verifyArray(String array, Felt hash, String what) {
        String[] rs = array.replaceAll("[\\[\\]\"]", "").split(",");
        Felt r = new Felt(new BigInteger(rs[0]));
        Felt s = new Felt(new BigInteger(rs[1]));
        check(StarknetCurve.verify(publicKey, hash, r, s), what + ": signature does not verify against the hash");
    }

    void bench() {
        for (int i = 0; i < 2000; i++) {
            signer.signOrder(1759400000123L + i, "BTC-USD-PERP", "1", "LIMIT", "1000000", "6512345000000");
        }
        int n = 2000;
        long[] t = new long[n];
        for (int i = 0; i < n; i++) {
            long t0 = System.nanoTime();
            signer.signOrder(1759400100123L + i, "BTC-USD-PERP", "1", "LIMIT", "1000000", "6512345000000");
            t[i] = System.nanoTime() - t0;
        }
        java.util.Arrays.sort(t);
        System.out.printf("java ParadexTypedDataSigner.signOrder warm: p50 %.1f us  p99 %.1f us  max %.1f us%n",
                t[n / 2] / 1e3, t[(int) (n * 0.99)] / 1e3, t[n - 1] / 1e3);
    }

    public static void main(String[] a) throws Exception {
        Path path = Path.of(a.length > 0 ? a[0] : "paradex-vectors.tsv");
        String chainHex = "0x" + CHAIN.toString(16).toUpperCase(); // as ParadexRestApi passes it
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8))) {
            out.println("# Paradex signing vectors written by ParadexHashOracle.java (FBC-6); synthetic, see SYNTHETIC.");
            out.println("# Java signer: FueledByChaiTrading ParadexTypedDataSigner, scaling by ParadexOrder.getChainSize/getChainLimitPrice.");
            out.println("# account=" + ACCOUNT);
            out.println("# key=" + KEY);
            out.println("# chain_id=" + chainHex);
            out.println("# seed=" + SEED + " random_intents=" + RANDOM_INTENTS);
            out.println(String.join("\t", COLUMNS));
            ParadexHashOracle o = new ParadexHashOracle(out, chainHex);
            o.crossCheckLegacy();
            o.fixed();
            o.random();
            System.out.println("wrote " + o.rows + " vectors to " + path);
            if (a.length > 1 && a[1].equals("--bench")) {
                o.bench();
            }
        }
    }
}
