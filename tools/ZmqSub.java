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
                    System.out.println(msg);
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
