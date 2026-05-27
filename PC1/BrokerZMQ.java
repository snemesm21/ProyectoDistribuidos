import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.net.InetSocketAddress;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.charset.StandardCharsets;
public class BrokerZMQ {
    private int puertoSensores;
    private int puertoAnalitica;
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesForwarded = new AtomicLong(0);
    private final AtomicLong totalProcessingLatencyNs = new AtomicLong(0);
    private HttpServer metricsServer = null;

    public BrokerZMQ(int puertoSensores, int puertoAnalitica) {
        this.puertoSensores = puertoSensores;
        this.puertoAnalitica = puertoAnalitica;
    }

    public void iniciar() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);
            ZMQ.Socket publisher = context.createSocket(ZMQ.PUB);

            subscriber.bind("tcp://*:" + puertoSensores);
            publisher.bind("tcp://*:" + puertoAnalitica);

            String[] topicosSuscripcion = {"ESPIRA", "CAMARA", "GPS"};
            for (String topico : topicosSuscripcion) {
                subscriber.subscribe(topico.getBytes(ZMQ.CHARSET));
            }

            System.out.println("=================================");
            System.out.println("[BROKER] Normal iniciado");
            System.out.println("[BROKER] Recibiendo de sensores en puerto " + puertoSensores);
            System.out.println("[BROKER] Publicando a analítica en puerto " + puertoAnalitica);
            System.out.println("=================================\n");

            startMetricsServer(9001);

            try {
                Configuracion conf = Configuracion.getInstance();
                while (!Thread.currentThread().isInterrupted()) {
                    String mensaje = subscriber.recvStr(0);
                    if (mensaje != null) {
                        long recvNs = System.nanoTime();
                        messagesReceived.incrementAndGet();

                        String[] partes = mensaje.split(" ", 2);
                        String tipo = partes.length > 0 ? partes[0] : "?";
                        String jsonPart = partes.length > 1 ? partes[1] : null;

                        boolean ok = true;
                        if (conf.isHmacEnabled() && jsonPart != null) {
                            ok = HmacUtil.verifyJson(jsonPart, conf.getSharedSecret());
                        }

                        if (!ok) {
                            System.err.println("[BROKER][DROP] Firma inválida o faltante en mensaje: " + tipo);
                            continue;
                        }

                        String out = mensaje + " BROKER_RECV_NS=" + recvNs;
                        long publishTime = System.nanoTime();
                        publisher.send(out.getBytes(ZMQ.CHARSET), 0);
                        long delta = publishTime - recvNs;
                        totalProcessingLatencyNs.addAndGet(delta);
                        messagesForwarded.incrementAndGet();
                        System.out.println("[BROKER][FORWARD] Evento reenviado: " + tipo + " delta_ns=" + delta);
                    }
                }
            } finally {
                subscriber.close();
                publisher.close();
                stopMetricsServer();
            }

        } catch (Exception e) {
            System.err.println("[ERROR BROKER]: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Configuracion config = Configuracion.getInstance();
        int puertoSensores = config.getSensoresPub();
        int puertoAnalitica = config.getBrokerPub();

        BrokerZMQ broker = new BrokerZMQ(puertoSensores, puertoAnalitica);
        broker.iniciar();
    }

    private void startMetricsServer(int port) {
        try {
            metricsServer = HttpServer.create(new InetSocketAddress(port), 0);
            metricsServer.createContext("/metrics", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) {
                    try {
                        StringBuilder sb = new StringBuilder();
                        sb.append("messages_received_total " + messagesReceived.get() + "\n");
                        sb.append("messages_forwarded_total " + messagesForwarded.get() + "\n");
                        long forwarded = Math.max(1, messagesForwarded.get());
                        long avgNs = totalProcessingLatencyNs.get() / forwarded;
                        sb.append("average_processing_latency_ns " + avgNs + "\n");
                        byte[] resp = sb.toString().getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, resp.length);
                        OutputStream os = exchange.getResponseBody();
                        os.write(resp);
                        os.close();
                    } catch (Exception e) {
                        try { exchange.sendResponseHeaders(500,0); exchange.close(); } catch (Exception ex) {}
                    }
                }
            });
            metricsServer.setExecutor(null);
            metricsServer.start();
        } catch (Exception e) {
            System.err.println("[METRICS] Failed to start metrics server: " + e.getMessage());
        }
    }

    private void stopMetricsServer() {
        if (metricsServer != null) {
            metricsServer.stop(0);
            metricsServer = null;
        }
    }
}
