import com.swmansion.starknet.data.TypedData;
import com.swmansion.starknet.data.types.Felt;
import com.swmansion.starknet.crypto.StarknetCurveSignature;
import com.fueledbychai.paradex.common.api.ParadexTypedDataSigner;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

public class ParadexHashOracle {
    public static void main(String[] a) {
        BigInteger chain = new BigInteger("8458834024819506728615521019831122032732688838300957472069977523540");
        String chainHex = "0x" + chain.toString(16).toUpperCase();
        String account = "0x129f3dc1b8962d8a87abc692424c78fda963ade0e1cd17bf3d1c26f8d41ee7a";
        String priv = "0x0139fe4d6f02e666e86a6f58e65060f115cd3c185bd9e98bd829636931458f79";
        List<TypedData.Type> dom = List.of(new TypedData.StandardType("name", "felt"),
            new TypedData.StandardType("chainId", "felt"), new TypedData.StandardType("version", "felt"));
        Map<String, List<TypedData.Type>> types = Map.of("StarkNetDomain", dom, "Order",
            List.of(new TypedData.StandardType("timestamp", "felt"), new TypedData.StandardType("market", "felt"),
                    new TypedData.StandardType("side", "felt"), new TypedData.StandardType("orderType", "felt"),
                    new TypedData.StandardType("size", "felt"), new TypedData.StandardType("price", "felt")));
        String domainJson = "{\"name\":\"Paradex\",\"chainId\":\"" + chainHex + "\",\"version\":\"1\"}";
        String msg = "{\"timestamp\":1759400000123,\"market\":\"BTC-USD-PERP\",\"side\":\"1\",\"orderType\":\"LIMIT\",\"size\":\"1000000\",\"price\":\"6512345000000\"}";
        TypedData td = new TypedData(types, "Order", domainJson, msg);
        Felt h = td.getMessageHash(Felt.fromHex(account));
        System.out.println("java_hash " + h.hexString());
        ParadexTypedDataSigner s = new ParadexTypedDataSigner(account, priv, chainHex);
        StarknetCurveSignature sig = s.signOrder(1759400000123L, "BTC-USD-PERP", "1", "LIMIT", "1000000", "6512345000000");
        System.out.println("java_sig_r " + sig.getR().hexString());
        System.out.println("java_sig_s " + sig.getS().hexString());
        // timing (JIT-warm) for reference
        for (int i = 0; i < 2000; i++) s.signOrder(1759400000123L + i, "BTC-USD-PERP", "1", "LIMIT", "1000000", "6512345000000");
        int n = 2000; long[] t = new long[n];
        for (int i = 0; i < n; i++) { long t0 = System.nanoTime(); s.signOrder(1759400100123L + i, "BTC-USD-PERP", "1", "LIMIT", "1000000", "6512345000000"); t[i] = System.nanoTime() - t0; }
        java.util.Arrays.sort(t);
        System.out.printf("java ParadexTypedDataSigner.signOrder warm: p50 %.1f us  p99 %.1f us  max %.1f us%n", t[n/2]/1e3, t[(int)(n*0.99)]/1e3, t[n-1]/1e3);
    }
}
