import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ZmqSub {
    public static void main(String[] args) {
        String addr = args.length > 0 ? args[0] : "tcp://127.0.0.1:6666";
        String topic = args.length > 1 ? args[1] : "";
        System.out.println("[ZmqSub] Subscribing to " + addr + " topic='" + topic + "' for 20s");
        try (ZContext ctx = new ZContext()) {
            ZMQ.Socket sub = ctx.createSocket(ZMQ.SUB);
            sub.connect(addr);
            sub.subscribe(topic.getBytes(ZMQ.CHARSET));

            long end = System.currentTimeMillis() + 20000;
            while (System.currentTimeMillis() < end) {
                String msg = sub.recvStr(ZMQ.DONTWAIT);
                if (msg != null) {
                    try {
                        Configuracion conf = null;
                        boolean hmac = false;
                        try {
                            conf = Configuracion.getInstance();
                            hmac = conf.isHmacEnabled();
                        } catch (Throwable ignored) {}

                        if (hmac) {
                            String[] partes = msg.split(" ", 2);
                            String json = partes.length > 1 ? partes[1] : null;
                            if (json != null && HmacUtil.verifyJson(json, conf.getSharedSecret())) {
                                System.out.println(msg);
                            } else {
                                System.err.println("[ZmqSub][DROP] Firma inválida o faltante: " + msg);
                            }
                        } else {
                            System.out.println(msg);
                        }
                    } catch (Exception e) {
                        System.out.println(msg);
                    }
                } else {
                    Thread.sleep(100);
                }
            }
            sub.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
