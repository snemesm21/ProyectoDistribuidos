import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.HashMap;
import java.util.Map;


public class ServicioAnalitica {
    private String brokerAddress;
    private String bdPrincipalAddress;
    private String bdReplicaAddress;
    private String semaforosAddress;
    private ZContext context;

    private Map<String, DatosInterseccion> datosIntersecciones;
    
    private class DatosInterseccion {
        int cola = 0;
        double velocidadPromedio = 30.0;
        int vehiculosContados = 0;
        double densidad = 0;
        String nivelCongestion = "NORMAL";
        EstadoTrafico estadoActual = EstadoTrafico.NORMAL;
    }
    
    public ServicioAnalitica(String brokerAddress, String bdPrincipalAddress, 
                             String bdReplicaAddress, String semaforosAddress) {
        this.brokerAddress = brokerAddress;
        this.bdPrincipalAddress = bdPrincipalAddress;
        this.bdReplicaAddress = bdReplicaAddress;
        this.semaforosAddress = semaforosAddress;
        this.datosIntersecciones = new HashMap<>();
    }
    
    public void iniciar() {
        try {
            context = new ZContext();
        
        ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);
        subscriber.connect(brokerAddress);
        subscriber.subscribe("ESPIRA".getBytes(ZMQ.CHARSET));
        subscriber.subscribe("CAMARA".getBytes(ZMQ.CHARSET));
        subscriber.subscribe("GPS".getBytes(ZMQ.CHARSET));
        

        ZMQ.Socket pusherBDPrincipal = context.createSocket(ZMQ.PUSH);
        pusherBDPrincipal.connect(bdPrincipalAddress);
        

        ZMQ.Socket pusherBDReplica = context.createSocket(ZMQ.PUSH);
        pusherBDReplica.connect(bdReplicaAddress);
        

        ZMQ.Socket publisherSemaforos = context.createSocket(ZMQ.PUB);
        publisherSemaforos.bind(semaforosAddress);
        
        System.out.println("╔════════════════════════════════════════════╗");
        System.out.println("║      PC2 - SERVICIO DE ANALÍTICA          ║");
        System.out.println("╠════════════════════════════════════════════╣");
        System.out.println("║ BD Principal: " + String.format("%-28s", bdPrincipalAddress) + "║");
        System.out.println("║ BD Réplica:   " + String.format("%-28s", bdReplicaAddress) + "║");
        System.out.println("║ Estado:       " + String.format("%-28s", "ACTIVO") + "║");
        System.out.println("╚════════════════════════════════════════════╝");
            System.out.println("[ANALÍTICA] Servicio iniciado");
            System.out.println("[ANALÍTICA] Escuchando eventos del broker");
            System.out.println("[ANALÍTICA] Reglas activas:");
            System.out.println("  - NORMAL: Q < 5 AND Cv <= 12 AND Vp > 35 AND D < 20");
            System.out.println("  - CONGESTION: Q >= 10 OR Cv >= 15 OR Vp < 20 OR D >= 40 OR GPS=ALTA");
            System.out.println("  - PRIORIZACION: comando manual (duración extendida)");
            System.out.println("=================================\n");
            
            Thread.sleep(1000);
            
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    String mensaje = subscriber.recvStr(0);
                    
                    if (mensaje != null) {
                        procesarEvento(mensaje, pusherBDPrincipal, pusherBDReplica, publisherSemaforos);
                    }
                }
            } finally {
                subscriber.close();
                pusherBDPrincipal.close();
                pusherBDReplica.close();
                publisherSemaforos.close();
                context.close();
            }
            
        } catch (Exception e) {
            System.err.println("[ERROR ANALÍTICA]: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private void procesarEvento(String mensaje, ZMQ.Socket bdPrincipal, 
                                ZMQ.Socket bdReplica, ZMQ.Socket semaforos) {
        try {
            String[] partes = mensaje.split(" ", 2);
            String tipo = partes[0];
            String jsonEvento = partes[1];

            almacenarEvento(jsonEvento, bdPrincipal, bdReplica);
            
            String interseccion = extraerCampo(jsonEvento, "interseccion");
            if (interseccion.isEmpty()) {
                interseccion = extraerInterseccionDesdeSensorId(extraerCampo(jsonEvento, "sensor_id"));
            }
            

            DatosInterseccion datos = datosIntersecciones.computeIfAbsent(
                interseccion, k -> new DatosInterseccion()
            );
            

            String sensorId = extraerCampo(jsonEvento, "sensor_id");
            switch (tipo) {
                case "CAMARA":
                    datos.cola = Integer.parseInt(extraerCampo(jsonEvento, "volumen"));
                    System.out.println("  [CÁMARA] " + sensorId + " → INT-" + interseccion + " | Q=" + datos.cola + " veh");
                    break;
                case "ESPIRA":
                    datos.vehiculosContados = Integer.parseInt(extraerCampo(jsonEvento, "vehiculos_contados"));
                    System.out.println("  [ESPIRA] " + sensorId + " → INT-" + interseccion + " | Cv=" + datos.vehiculosContados + " veh/intervalo");
                    break;
                case "GPS":
                    datos.velocidadPromedio = Double.parseDouble(extraerCampo(jsonEvento, "velocidad_promedio"));
                    String densidadCampo = extraerCampo(jsonEvento, "densidad");
                    if (!densidadCampo.isEmpty()) {
                        datos.densidad = Double.parseDouble(densidadCampo);
                    }
                    String nivelCampo = extraerCampo(jsonEvento, "nivel_congestion");
                    if (!nivelCampo.isEmpty()) {
                        datos.nivelCongestion = nivelCampo;
                    }
                    System.out.println("  [GPS]    " + sensorId + " → INT-" + interseccion + " | D=" + String.format("%.1f", datos.densidad) + " veh/km | Vp=" + String.format("%.1f", datos.velocidadPromedio) + " km/h | Nivel=" + datos.nivelCongestion);
                    break;
            }

            EstadoTrafico estadoAnterior = datos.estadoActual;
            datos.estadoActual = ReglasTrafico.evaluarEstado(
                datos.cola, datos.vehiculosContados, datos.velocidadPromedio, datos.densidad, datos.nivelCongestion
            );


            if (estadoAnterior != datos.estadoActual) {
                int tiempoVerde = ReglasTrafico.obtenerTiempoSemaforo(datos.estadoActual);

                // Calcular orientación a priorizar según sensores (heurística simple):
                // - Si la cámara reporta cola alta, priorizamos HORIZONTAL
                // - Si la espira reporta conteo alto, priorizamos VERTICAL
                // - Si ambos o ninguno, priorizamos HORIZONTAL por defecto
                boolean camaraFlag = datos.cola >= 10;
                boolean espiraFlag = datos.vehiculosContados >= 15;
                boolean gpsFlag = datos.velocidadPromedio < 20 || datos.densidad >= 40 || "ALTA".equalsIgnoreCase(datos.nivelCongestion);

                String orientacion;
                if (camaraFlag && !espiraFlag) orientacion = "HORIZONTAL";
                else if (espiraFlag && !camaraFlag) orientacion = "VERTICAL";
                else if (camaraFlag && espiraFlag) orientacion = "HORIZONTAL";
                else if (gpsFlag) orientacion = "HORIZONTAL";
                else orientacion = "HORIZONTAL";

                String comandoSemaforo = String.format(
                    "{\"interseccion\":\"%s\",\"estado\":\"VERDE\",\"tiempo\":%d,\"razon\":\"%s\",\"orientacion\":\"%s\"}",
                    interseccion, tiempoVerde, datos.estadoActual, orientacion
                );

                semaforos.send(("COMANDO " + comandoSemaforo).getBytes(ZMQ.CHARSET), 0);

                System.out.println("\n╔════════════════════════════════════════════════════════════╗");
                System.out.println("║           CAMBIO DE ESTADO → CONTROL SEMÁFOROS          ║");
                System.out.println("╠════════════════════════════════════════════════════════════╣");
                System.out.println("║ Intersección:        " + String.format("%-38s", interseccion) + "║");
                System.out.println("║ Estado ANTERIOR:     " + String.format("%-38s", estadoAnterior.toString()) + "║");
                System.out.println("║ Estado NUEVO:        " + String.format("%-38s", datos.estadoActual.toString()) + "║");
                System.out.println("╠════════════════════════════════════════════════════════════╣");
                System.out.println("║ Q (Cola):            " + String.format("%-38s", datos.cola + " veh") + "║");
                System.out.println("║ Cv (Conteo):         " + String.format("%-38s", datos.vehiculosContados + " veh/intervalo") + "║");
                System.out.println("║ Vp (Velocidad):      " + String.format("%-38s", String.format("%.2f km/h", datos.velocidadPromedio)) + "║");
                System.out.println("║ D (Densidad):        " + String.format("%-38s", String.format("%.2f veh/km", datos.densidad)) + "║");
                System.out.println("║ Nivel GPS:           " + String.format("%-38s", datos.nivelCongestion) + "║");
                System.out.println("╠════════════════════════════════════════════════════════════╣");
                System.out.println("║  COMANDO ENVIADO:  VERDE por " + String.format("%-33s", tiempoVerde + "s") + "║");
                System.out.println("╚════════════════════════════════════════════════════════════╝\n");
            }
            
        } catch (Exception e) {
            System.err.println("[ERROR ANALÍTICA] Procesando evento: " + e.getMessage());
        }
    }

    private String extraerInterseccionDesdeSensorId(String sensorId) {
        try {
            if (sensorId == null || sensorId.isEmpty()) return "DESCONOCIDA";
            if (sensorId.startsWith("GPS-INT-") || sensorId.startsWith("CAM-INT-") || sensorId.startsWith("ESP-INT-")) {
                String[] partes = sensorId.split("-");
                if (partes.length >= 3) {
                    return "INT-" + partes[2];
                }
            }
            return "DESCONOCIDA";
        } catch (Exception e) {
            return "DESCONOCIDA";
        }
    }

    private String extraerCampo(String json, String campo) {
        try {
            String buscar = "\"" + campo + "\":";
            int inicio = json.indexOf(buscar);
            if (inicio == -1) return "";
            
            inicio += buscar.length();
            while (inicio < json.length() && (json.charAt(inicio) == ' ' || json.charAt(inicio) == '"')) {
                inicio++;
            }
            
            int fin = inicio;
            boolean enComillas = json.charAt(inicio - 1) == '"';
            
            if (enComillas) {
                fin = json.indexOf('"', inicio);
            } else {
                while (fin < json.length() && json.charAt(fin) != ',' && json.charAt(fin) != '}') {
                    fin++;
                }
            }
            
            return json.substring(inicio, fin).trim();
        } catch (Exception e) {
            return "";
        }
    }
    
    private void almacenarEvento(String jsonEvento, ZMQ.Socket bdPrincipal, ZMQ.Socket bdReplica) {
        byte[] data = jsonEvento.getBytes(ZMQ.CHARSET);

        boolean exitoPrincipal = bdPrincipal.send(data, 0);
        boolean exitoReplica = bdReplica.send(data, 0);

        if (!exitoPrincipal || !exitoReplica) {
            System.err.println("[  ANALÍTICA] Error enviando evento a BD: Principal=" + exitoPrincipal + ", Réplica=" + exitoReplica);
        }
    }
    
    public static void main(String[] args) {
        try {
            Configuracion config = Configuracion.getInstance();
            
            String brokerAddress = "tcp://" + config.getPC1() + ":" + config.getBrokerPub();
            String bdPrincipalAddress = "tcp://" + config.getPC3() + ":" + config.getAnaliticaPushBdPrincipal();
            String bdReplicaAddress = "tcp://" + config.getPC2() + ":" + config.getAnaliticaPushBdReplica();
            String semaforosAddress = "tcp://*:" + config.getAnaliticaPubSemaforos();
            
            System.out.println("╔════════════════════════════════════════════╗");
            System.out.println("║          CONFIGURACIÓN SERVICIO           ║");
            System.out.println("╠════════════════════════════════════════════╣");
            System.out.println("║ Broker:       " + String.format("%-28s", brokerAddress) + "║");
            System.out.println("║ BD Principal: " + String.format("%-28s", bdPrincipalAddress) + "║");
            System.out.println("║ BD Réplica:   " + String.format("%-28s", bdReplicaAddress) + "║");
            System.out.println("║ Semáforos:    " + String.format("%-28s", semaforosAddress) + "║");
            System.out.println("╚════════════════════════════════════════════╝\n");
            
            ServicioAnalitica servicio = new ServicioAnalitica(
                brokerAddress,
                bdPrincipalAddress,
                bdReplicaAddress,
                semaforosAddress
            );
            servicio.iniciar();
        } catch (Exception e) {
            System.err.println("Error iniciando servicio: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
