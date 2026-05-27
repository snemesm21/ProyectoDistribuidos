import org.zeromq.ZMQ;
import org.zeromq.ZContext;

/**
 * SensorLoadGenerator - simple PUB generator to simulate sensors
 * Usage: java SensorLoadGenerator <brokerIP> <brokerPort> <topic> <ratePerSec> <payloadBytes> <durationSec>
 */
public class SensorLoadGenerator {
    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.out.println("Usage: java SensorLoadGenerator <brokerIP> <brokerPort> <topic> <ratePerSec> <payloadBytes> <durationSec>");
            return;
        }
        String brokerIP = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        String topic = args[2];
        int ratePerSec = Integer.parseInt(args[3]);
        int payloadBytes = Integer.parseInt(args[4]);
        int durationSec = Integer.parseInt(args[5]);

        String payload = new String(new byte[payloadBytes]);
        String endpoint = "tcp://" + brokerIP + ":" + brokerPort;

        try (ZContext context = new ZContext()) {
            ZMQ.Socket publisher = context.createSocket(ZMQ.PUB);
            publisher.connect(endpoint);
            System.out.println("[GEN] Connected to " + endpoint + " topic=" + topic + " rate=" + ratePerSec + "/s payload=" + payloadBytes);

            long intervalNs = 1_000_000_000L / Math.max(1, ratePerSec);
            long endTime = System.nanoTime() + durationSec * 1_000_000_000L;
            long count = 0;
            while (System.nanoTime() < endTime) {
                long t0 = System.nanoTime();
                String msg = topic + " sensor-1 " + System.currentTimeMillis() + " " + payload;
                publisher.send(msg.getBytes(ZMQ.CHARSET), 0);
                count++;
                long t1 = System.nanoTime();
                long sleepNs = intervalNs - (t1 - t0);
                if (sleepNs > 0) {
                    Thread.sleep(sleepNs / 1_000_000L, (int)(sleepNs % 1_000_000L));
                } else {
                    // busy loop fallback
                    Thread.yield();
                }
            }
            System.out.println("[GEN] Sent " + count + " messages");
            publisher.close();
        }
    }
}
