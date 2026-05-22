import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Broker ZMQ multihilos - PC1
 * Arquitectura simple:
 *  - hilo principal: recibe (SUB) mensajes de sensores y los encola
 *  - pool de workers: procesan mensajes en paralelo (sin usar sockets)
 *  - hilo publicador: toma mensajes procesados y los publica (PUB)
 *
 * Esta aproximación mantiene la única socket PUB en el hilo que la crea,
 * evita uso concurrente de sockets y permite medir rendimiento con varios workers.
 */
public class BrokerZMQ {
    private int puertoSensores;
    private int puertoAnalitica;
    private List<String> topicosSuscripcion;
    private final int workerCount;

    public BrokerZMQ(int puertoSensores, int puertoAnalitica, int workerCount) {
        this.puertoSensores = puertoSensores;
        this.puertoAnalitica = puertoAnalitica;
        this.topicosSuscripcion = new ArrayList<>();
        this.topicosSuscripcion.add("ESPIRA");
        this.topicosSuscripcion.add("CAMARA");
        this.topicosSuscripcion.add("GPS");
        this.workerCount = Math.max(1, workerCount);
    }

    public BrokerZMQ(int puertoSensores, int puertoAnalitica) {
        this(puertoSensores, puertoAnalitica, 4);
    }

    public void iniciar() {
        final LinkedBlockingQueue<String> toProcess = new LinkedBlockingQueue<>(10000);
        final LinkedBlockingQueue<String> toPublish = new LinkedBlockingQueue<>(10000);

        try (ZContext context = new ZContext()) {
            // Publisher thread: posee el socket PUB
            Thread publisherThread = new Thread(() -> {
                ZMQ.Socket publisher = context.createSocket(ZMQ.PUB);
                publisher.bind("tcp://*:" + puertoAnalitica);
                System.out.println("[BROKER][PUBLISHER] Bound to tcp://*:" + puertoAnalitica);
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        String msg = toPublish.take();
                        publisher.send(msg.getBytes(ZMQ.CHARSET), 0);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println("[BROKER][PUBLISHER] Error: " + e.getMessage());
                } finally {
                    publisher.close();
                }
            }, "broker-publisher");

            // Worker pool: procesan mensajes y los colocan para publicación
            ExecutorService workers = Executors.newFixedThreadPool(workerCount);
            for (int i = 0; i < workerCount; i++) {
                final int id = i;
                workers.submit(() -> {
                    Thread.currentThread().setName("broker-worker-" + id);
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            String mensaje = toProcess.take();
                            // Aquí podría ir procesamiento complejo / filtrado / transformaciones
                            System.out.println("[BROKER][WORKER-" + id + "] Procesando evento: " + mensaje.split(" ")[0]);
                            toPublish.put(mensaje);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        System.err.println("[BROKER][WORKER-" + id + "] Error: " + e.getMessage());
                    }
                });
            }

            // Start publisher
            publisherThread.start();

            // Subscriber: recibe mensajes directamente y los encola
            ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);
            subscriber.bind("tcp://*:" + puertoSensores);

            for (String topico : topicosSuscripcion) {
                subscriber.subscribe(topico.getBytes(ZMQ.CHARSET));
            }

            System.out.println("=================================");
            System.out.println("[BROKER] Multithreaded iniciado");
            System.out.println("[BROKER] Recibiendo de sensores en puerto " + puertoSensores);
            System.out.println("[BROKER] Publicando a analítica en puerto " + puertoAnalitica);
            System.out.println("[BROKER] Workers: " + workerCount);
            System.out.println("=================================\n");

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String mensaje = subscriber.recvStr(0);
                    if (mensaje != null) {
                        toProcess.put(mensaje);
                        System.out.println("[BROKER][RECV] Encolado evento: " + mensaje.split(" ")[0]);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                // Shutdown sequence
                subscriber.close();
                workers.shutdownNow();
                try { workers.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                publisherThread.interrupt();
                try { publisherThread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
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
        int workers = 4;
        if (args.length > 0) {
            try { workers = Integer.parseInt(args[0]); } catch (Exception e) { /* ignore */ }
        }

        BrokerZMQ broker = new BrokerZMQ(puertoSensores, puertoAnalitica, workers);
        broker.iniciar();
    }
}
