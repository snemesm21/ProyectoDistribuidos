import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.Random;


public class SensorEspiraInductiva implements Runnable {
    private String sensorId;
    private String interseccion;
    private int intervaloSegundos;
    private String brokerAddress;
    private Random random;
    private boolean activo;
    
    public SensorEspiraInductiva(String sensorId, String interseccion, int intervaloSegundos, String brokerAddress) {
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
            
            System.out.println("[ESPIRA] " + sensorId + " iniciado en " + interseccion);
            

            Thread.sleep(1000);
            
            while (activo) {
                double probabilidad = random.nextDouble();
                int vehiculosContados;

                if (probabilidad < 0.80) {
                    vehiculosContados = 6 + random.nextInt(7); // 6-12
                } else if (probabilidad < 0.95) {
                    vehiculosContados = random.nextInt(6); // 0-5
                } else {
                    vehiculosContados = 13 + random.nextInt(8); // 13-20
                }
                
                EventoEspira evento = new EventoEspira(
                    sensorId, 
                    interseccion, 
                    vehiculosContados, 
                    intervaloSegundos
                );
                
                String mensaje = "ESPIRA " + evento.toJson();
                publisher.send(mensaje.getBytes(ZMQ.CHARSET), 0);
                
                System.out.println("[ESPIRA] " + sensorId + " -> Vehículos contados: " + vehiculosContados);
                
                Thread.sleep(intervaloSegundos * 1000);
            }
            
            publisher.close();
        } catch (Exception e) {
            System.err.println("[ERROR ESPIRA] " + sensorId + ": " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void detener() {
        activo = false;
    }
}
