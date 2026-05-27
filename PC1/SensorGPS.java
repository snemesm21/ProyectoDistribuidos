import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.Random;


public class SensorGPS implements Runnable {
    private String sensorId;
    private String interseccion;
    private int intervaloSegundos;
    private String brokerAddress;
    private Random random;
    private boolean activo;
    
    public SensorGPS(String sensorId, String interseccion, int intervaloSegundos, String brokerAddress) {
        this.sensorId = sensorId;
        this.interseccion = interseccion;
        this.intervaloSegundos = intervaloSegundos;
        this.brokerAddress = brokerAddress;
        this.random = new Random();
        this.activo = true;
    }
    
    @Override
    public void run() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket publisher = context.createSocket(ZMQ.PUB);
            publisher.connect(brokerAddress);
            
            System.out.println("[GPS] " + sensorId + " iniciado en " + interseccion);
            
            Thread.sleep(1000);
            
            while (activo) {

                double probabilidad = random.nextDouble();
                double velocidadPromedio;
                double densidad;

                if (probabilidad < 0.80) {
                    velocidadPromedio = 40 + random.nextDouble() * 10;
                } else if (probabilidad < 0.95) {
                    velocidadPromedio = 25 + random.nextDouble() * 14;
                } else {
                    velocidadPromedio = 5 + random.nextDouble() * 14;
                }

                if (velocidadPromedio > 40) {
                    densidad = 8 + random.nextDouble() * 10;
                } else if (velocidadPromedio >= 20) {
                    densidad = 18 + random.nextDouble() * 18;
                } else {
                    densidad = 40 + random.nextDouble() * 20;
                }
                
                EventoGPS evento = new EventoGPS(
                    sensorId,
                    interseccion,
                    velocidadPromedio,
                    densidad
                );

                String json = evento.toJson();
                Configuracion conf = Configuracion.getInstance();
                if (conf.isHmacEnabled()) {
                    String sig = HmacUtil.hmacSha256Hex(conf.getSharedSecret(), json);
                    json = HmacUtil.addSignatureToJson(json, sig);
                }

                String mensaje = "GPS " + json;
                publisher.send(mensaje.getBytes(ZMQ.CHARSET), 0);
                
                System.out.println(String.format("[GPS] %s -> Velocidad: %.2f km/h, Densidad: %.2f veh/km, Congestión: %s", 
                    sensorId, velocidadPromedio, densidad, evento.getNivelCongestion()));
                
                Thread.sleep(intervaloSegundos * 1000);
            }
            
            publisher.close();
        } catch (Exception e) {
            System.err.println("[ERROR GPS] " + sensorId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void detener() {
        activo = false;
    }

    public static void main(String[] args) {
        Configuracion config = Configuracion.getInstance();
        String brokerAddress = "tcp://" + config.getPC1() + ":" + config.getSensoresPub();
    }
}
