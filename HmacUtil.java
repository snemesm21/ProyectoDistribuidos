import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class HmacUtil {
    private static final String HMAC_ALGO = "HmacSHA256";

    public static String hmacSha256Hex(String secret, String data) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGO);
            SecretKeySpec keySpec = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HMAC_ALGO);
            mac.init(keySpec);
            byte[] raw = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(raw);
        } catch (Exception e) {
            return null;
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    public static String addSignatureToJson(String json, String signature) {
        if (json == null || signature == null) return json;
        json = json.trim();
        if (json.endsWith("}")) {
            return json.substring(0, json.length() - 1) + ",\"signature\":\"" + signature + "\"}";
        }
        return json;
    }

    public static String extractSignatureFromJson(String jsonWithSig) {
        if (jsonWithSig == null) return null;
        try {
            Pattern p = Pattern.compile("\\\"signature\\\"\\s*:\\s*\\\"([^\\\"]*)\\\"");
            Matcher m = p.matcher(jsonWithSig);
            if (m.find()) return m.group(1);
        } catch (Exception e) {
        }
        return null;
    }

    public static String stripSignatureFromJson(String jsonWithSig) {
        if (jsonWithSig == null) return null;
        try {
            // remove the signature field (assumes it's at the end or somewhere with a preceding comma)
            String result = jsonWithSig.replaceAll(",?\\s*\\\"signature\\\"\\s*:\\s*\\\"[^\\\"]*\\\"\\s*}\\s*$", "}");
            result = result.replaceAll(",\\s*\\\"signature\\\"\\s*:\\s*\\\"[^\\\"]*\\\"", "");
            return result;
        } catch (Exception e) {
            return jsonWithSig;
        }
    }

    public static boolean verifyJson(String jsonWithSig, String secret) {
        if (jsonWithSig == null || secret == null) return false;
        String sig = extractSignatureFromJson(jsonWithSig);
        if (sig == null) return false;
        String stripped = stripSignatureFromJson(jsonWithSig);
        String expected = hmacSha256Hex(secret, stripped);
        if (expected == null) return false;
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), sig.getBytes(StandardCharsets.UTF_8));
    }

    public static String signPlainWithSuffix(String text, String secret) {
        if (text == null || secret == null) return text;
        String sig = hmacSha256Hex(secret, text);
        return text + "||SIG:" + sig;
    }

    public static boolean verifyPlainWithSuffix(String textWithSig, String secret) {
        if (textWithSig == null || secret == null) return false;
        int idx = textWithSig.lastIndexOf("||SIG:");
        if (idx == -1) return false;
        String text = textWithSig.substring(0, idx);
        String sig = textWithSig.substring(idx + 6);
        String expected = hmacSha256Hex(secret, text);
        if (expected == null) return false;
        return MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), sig.getBytes(StandardCharsets.UTF_8));
    }
}
