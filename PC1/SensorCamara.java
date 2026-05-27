import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.Random;


public class SensorCamara implements Runnable {
    private String sensorId;
    private String interseccion;
    private int intervaloSegundos;
    private String brokerAddress;
    private Random random;
    private boolean activo;
    
    public SensorCamara(String sensorId, String interseccion, int intervaloSegundos, String brokerAddress) {
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
            
            System.out.println("[CAMARA] " + sensorId + " iniciado en " + interseccion);
            
            Thread.sleep(1000);
            
            while (activo) {
                double probabilidad = random.nextDouble();
                int volumen;
                double velocidadPromedio;

                if (probabilidad < 0.80) {
                    volumen = random.nextInt(5);
                    velocidadPromedio = 36 + random.nextDouble() * 14;
                } else if (probabilidad < 0.95) {
                    volumen = 5 + random.nextInt(5);
                    velocidadPromedio = 25 + random.nextDouble() * 10;
                } else {
                    volumen = 10 + random.nextInt(6);
                    velocidadPromedio = 10 + random.nextDouble() * 9;
                }
                
                EventoCamara evento = new EventoCamara(
                    sensorId,
                    interseccion,
                    volumen,
                    velocidadPromedio
                );

                String json = evento.toJson();
                Configuracion conf = Configuracion.getInstance();
                if (conf.isHmacEnabled()) {
                    String sig = HmacUtil.hmacSha256Hex(conf.getSharedSecret(), json);
                    json = HmacUtil.addSignatureToJson(json, sig);
                }

                String mensaje = "CAMARA " + json;
                publisher.send(mensaje.getBytes(ZMQ.CHARSET), 0);
                
                System.out.println(String.format("[CAMARA] %s -> Cola: %d veh, Velocidad: %.2f km/h", 
                    sensorId, volumen, velocidadPromedio));
                
                Thread.sleep(intervaloSegundos * 1000);
            }
            
            publisher.close();
        } catch (Exception e) {
            System.err.println("[ERROR CAMARA] " + sensorId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void detener() {
        activo = false;
    }
}
