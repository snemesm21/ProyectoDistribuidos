import org.zeromq.ZMQ;
import org.zeromq.ZContext;

/**
 * Broker ZMQ normal - PC1
 * Arquitectura base del proyecto:
 *  - un hilo recibe eventos desde los sensores por SUB
 *  - el mismo hilo reenvia los eventos al servicio de analitica por PUB
 *
 * Esta version conserva el desacoplamiento asincrono de ZMQ, pero sin
 * paralelismo adicional dentro del broker.
 */
public class BrokerZMQ {
    private int puertoSensores;
    private int puertoAnalitica;

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

            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String mensaje = subscriber.recvStr(0);
                    if (mensaje != null) {
                        publisher.send(mensaje.getBytes(ZMQ.CHARSET), 0);
                        System.out.println("[BROKER][FORWARD] Evento reenviado: " + mensaje.split(" ")[0]);
                    }
                }
            } finally {
                subscriber.close();
                publisher.close();
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
}
