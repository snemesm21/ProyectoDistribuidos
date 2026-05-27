import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.net.InetSocketAddress;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.charset.StandardCharsets;
public class BrokerZMQMultihilos {
    private int puertoSensores;
    private int puertoAnalitica;
    private List<String> topicosSuscripcion;
    private final int workerCount;
    private final AtomicLong messagesReceived = new AtomicLong(0);
    private final AtomicLong messagesProcessed = new AtomicLong(0);
    private final AtomicLong messagesPublished = new AtomicLong(0);
    private final AtomicLong totalProcessingLatencyNs = new AtomicLong(0);
    private HttpServer metricsServer = null;
    private LinkedBlockingQueue<String> metricsToProcess = null;
    private LinkedBlockingQueue<String> metricsToPublish = null;

    public BrokerZMQMultihilos(int puertoSensores, int puertoAnalitica, int workerCount) {
        this.puertoSensores = puertoSensores;
        this.puertoAnalitica = puertoAnalitica;
        this.topicosSuscripcion = new ArrayList<>();
        this.topicosSuscripcion.add("ESPIRA");
        this.topicosSuscripcion.add("CAMARA");
        this.topicosSuscripcion.add("GPS");
        this.workerCount = Math.max(1, workerCount);
    }

    public BrokerZMQMultihilos(int puertoSensores, int puertoAnalitica) {
        this(puertoSensores, puertoAnalitica, 4);
    }

    public void iniciar() {
        metricsToProcess = new LinkedBlockingQueue<>(10000);
        metricsToPublish = new LinkedBlockingQueue<>(10000);
        final LinkedBlockingQueue<String> toProcess = metricsToProcess;
        final LinkedBlockingQueue<String> toPublish = metricsToPublish;

        startMetricsServer(9000);

        try (ZContext context = new ZContext()) {
            Thread publisherThread = new Thread(() -> {
                ZMQ.Socket publisher = context.createSocket(ZMQ.PUB);
                publisher.bind("tcp://*:" + puertoAnalitica);
                System.out.println("[BROKER-MT][PUBLISHER] Bound to tcp://*:" + puertoAnalitica);
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        String msg = toPublish.take();
                            long publishTime = System.nanoTime();
                            try {
                                int markerIdx = msg.indexOf("BROKER_RECV_NS=");
                                if (markerIdx != -1) {
                                    int end = msg.indexOf(' ', markerIdx);
                                    String marker = (end == -1) ? msg.substring(markerIdx) : msg.substring(markerIdx, end);
                                    long recvNs = Long.parseLong(marker.split("=")[1]);
                                    long delta = publishTime - recvNs;
                                    totalProcessingLatencyNs.addAndGet(delta);
                                }
                            } catch (Exception ignore) {}
                            publisher.send(msg.getBytes(ZMQ.CHARSET), 0);
                            messagesPublished.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("[BROKER-MT][PUBLISHER] Error: " + e.getMessage());
                } finally {
                    publisher.close();
                }
            }, "broker-publisher");

            ExecutorService workers = Executors.newFixedThreadPool(workerCount);
            for (int i = 0; i < workerCount; i++) {
                final int id = i;
                workers.submit(() -> {
                    Thread.currentThread().setName("broker-worker-" + id);
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            String mensaje = toProcess.take();
                            long start = System.nanoTime();
                            System.out.println("[BROKER-MT][WORKER-" + id + "] Procesando evento: " + mensaje.split(" ")[0]);
                            if (!mensaje.contains("BROKER_RECV_NS=")) {
                                mensaje = mensaje + " BROKER_RECV_NS=" + start;
                            }
                            toPublish.put(mensaje);
                            long end = System.nanoTime();
                            messagesProcessed.incrementAndGet();
                            totalProcessingLatencyNs.addAndGet(end - start);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("[BROKER-MT][WORKER-" + id + "] Error: " + e.getMessage());
                    }
                });
            }

            publisherThread.start();

            ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);
            subscriber.bind("tcp://*:" + puertoSensores);

            for (String topico : topicosSuscripcion) {
                subscriber.subscribe(topico.getBytes(ZMQ.CHARSET));
            }

            System.out.println("=================================");
            System.out.println("[BROKER-MT] Multihilo iniciado");
            System.out.println("[BROKER-MT] Recibiendo de sensores en puerto " + puertoSensores);
            System.out.println("[BROKER-MT] Publicando a analítica en puerto " + puertoAnalitica);
            System.out.println("[BROKER-MT] Workers: " + workerCount);
            System.out.println("=================================\n");

            Configuracion conf = Configuracion.getInstance();
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String mensaje = subscriber.recvStr(0);
                    if (mensaje != null) {
                        long recvNs = System.nanoTime();
                        String[] partes = mensaje.split(" ", 2);
                        String tipo = partes.length > 0 ? partes[0] : "?";
                        String jsonPart = partes.length > 1 ? partes[1] : null;

                        boolean ok = true;
                        if (conf.isHmacEnabled() && jsonPart != null) {
                            ok = HmacUtil.verifyJson(jsonPart, conf.getSharedSecret());
                        }

                        if (!ok) {
                            System.err.println("[BROKER-MT][DROP] Firma inválida o faltante en mensaje: " + tipo);
                            continue;
                        }

                        String mark = " BROKER_RECV_NS=" + recvNs;
                        toProcess.put(mensaje + mark);
                        messagesReceived.incrementAndGet();
                        System.out.println("[BROKER-MT][RECV] Encolado evento: " + tipo + " queuesize=" + toProcess.size());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                subscriber.close();
                workers.shutdownNow();
                try {
                    workers.awaitTermination(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                publisherThread.interrupt();
                try {
                    publisherThread.join(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

        } catch (Exception e) {
            System.err.println("[ERROR BROKER-MT]: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Configuracion config = Configuracion.getInstance();
        int puertoSensores = config.getSensoresPub();
        int puertoAnalitica = config.getBrokerPub();
        int workers = 4;
        if (args.length > 0) {
            try {
                workers = Integer.parseInt(args[0]);
            } catch (Exception e) {
            }
        }

        BrokerZMQMultihilos broker = new BrokerZMQMultihilos(puertoSensores, puertoAnalitica, workers);
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
                        sb.append("messages_processed_total " + messagesProcessed.get() + "\n");
                        sb.append("messages_published_total " + messagesPublished.get() + "\n");
                        long processed = Math.max(1, messagesProcessed.get());
                        long avgNs = totalProcessingLatencyNs.get() / processed;
                        sb.append("average_processing_latency_ns " + avgNs + "\n");
                        sb.append("queue_toProcess_size " + (metricsToProcess != null ? metricsToProcess.size() : 0) + "\n");
                        sb.append("queue_toPublish_size " + (metricsToPublish != null ? metricsToPublish.size() : 0) + "\n");
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
            metricsServer.setExecutor(Executors.newSingleThreadExecutor());
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