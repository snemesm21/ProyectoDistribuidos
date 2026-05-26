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
 * Variante para la segunda medida de desempeno del proyecto.
 *
 * Arquitectura:
 *  - hilo principal: recibe mensajes SUB desde sensores y los encola
 *  - pool de workers: procesa mensajes en paralelo sin tocar sockets
 *  - hilo publicador: consume la cola final y publica por PUB
 */
public class BrokerZMQMultihilos {
    private int puertoSensores;
    private int puertoAnalitica;
    private List<String> topicosSuscripcion;
    private final int workerCount;

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
        final LinkedBlockingQueue<String> toProcess = new LinkedBlockingQueue<>(10000);
        final LinkedBlockingQueue<String> toPublish = new LinkedBlockingQueue<>(10000);

        try (ZContext context = new ZContext()) {
            Thread publisherThread = new Thread(() -> {
                ZMQ.Socket publisher = context.createSocket(ZMQ.PUB);
                publisher.bind("tcp://*:" + puertoAnalitica);
                System.out.println("[BROKER-MT][PUBLISHER] Bound to tcp://*:" + puertoAnalitica);
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        String msg = toPublish.take();
                        publisher.send(msg.getBytes(ZMQ.CHARSET), 0);
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
                            System.out.println("[BROKER-MT][WORKER-" + id + "] Procesando evento: " + mensaje.split(" ")[0]);
                            toPublish.put(mensaje);
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

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String mensaje = subscriber.recvStr(0);
                    if (mensaje != null) {
                        toProcess.put(mensaje);
                        System.out.println("[BROKER-MT][RECV] Encolado evento: " + mensaje.split(" ")[0]);
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
                // ignore
            }
        }

        BrokerZMQMultihilos broker = new BrokerZMQMultihilos(puertoSensores, puertoAnalitica, workers);
        broker.iniciar();
    }
}