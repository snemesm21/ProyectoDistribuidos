import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.HashMap;
import java.util.Map;


public class ServicioAnalitica {
    private String brokerAddress;
    private String bdPrincipalAddress;
    private String bdReplicaAddress;
    private String semaforosAddress;
    private String controlAddress;
    private ZContext context;
    private volatile boolean persistirEnReplica;
    private volatile boolean failoverActiva;
    private volatile boolean principalDisponible;

    private Map<String, DatosInterseccion> datosIntersecciones;
    private final LinkedBlockingQueue<String> colaEventosPendientesPrimario;
    
    private class DatosInterseccion {
        int cola = 0;
        double velocidadPromedio = 30.0;
        int vehiculosContados = 0;
        double densidad = 0;
        String nivelCongestion = "NORMAL";
        EstadoTrafico estadoActual = EstadoTrafico.NORMAL;
        long priorizacionHasta = 0L;
        String orientacionPrioridad = "HORIZONTAL";
        String motivoPrioridad = "";

        boolean priorizacionActiva() {
            return System.currentTimeMillis() < priorizacionHasta;
        }
    }
    
    public ServicioAnalitica(String brokerAddress, String bdPrincipalAddress, 
                             String bdReplicaAddress, String semaforosAddress,
                             String controlAddress) {
        this.brokerAddress = brokerAddress;
        this.bdPrincipalAddress = bdPrincipalAddress;
        this.bdReplicaAddress = bdReplicaAddress;
        this.semaforosAddress = semaforosAddress;
        this.controlAddress = controlAddress;
        this.datosIntersecciones = new HashMap<>();
        this.colaEventosPendientesPrimario = new LinkedBlockingQueue<>();
        this.persistirEnReplica = false;
        this.failoverActiva = false;
        this.principalDisponible = true;
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

        ZMQ.Socket controlRep = context.createSocket(ZMQ.REP);
        controlRep.bind(controlAddress);

        iniciarSupervisorFailover();

        ZMQ.Poller poller = context.createPoller(2);
        poller.register(subscriber, ZMQ.Poller.POLLIN);
        poller.register(controlRep, ZMQ.Poller.POLLIN);
        
        System.out.println("╔════════════════════════════════════════════╗");
        System.out.println("║      PC2 - SERVICIO DE ANALÍTICA          ║");
        System.out.println("╠════════════════════════════════════════════╣");
        System.out.println("║ BD Principal: " + String.format("%-28s", bdPrincipalAddress) + "║");
        System.out.println("║ BD Réplica:   " + String.format("%-28s", bdReplicaAddress) + "║");
        System.out.println("║ Control REP:  " + String.format("%-28s", controlAddress) + "║");
        System.out.println("║ Estado:       " + String.format("%-28s", "ACTIVO") + "║");
        System.out.println("╚════════════════════════════════════════════╝");
            System.out.println("[ANALÍTICA] Servicio iniciado");
            System.out.println("[ANALÍTICA] Escuchando eventos del broker");
            System.out.println("[ANALÍTICA] Reglas activas:");
            System.out.println("  - NORMAL: Q < 5 AND Cv <= 12 AND Vp > 35 AND D < 20");
            System.out.println("  - CONGESTION: 2 o más sensores críticos (cámara + espira + GPS)");
            System.out.println("  - PRIORIZACION: comando manual (duración extendida)");
            System.out.println("=================================\n");
            
            Thread.sleep(1000);
            
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    poller.poll(1000);

                    if (poller.pollin(0)) {
                        String mensaje = subscriber.recvStr(0);
                        if (mensaje != null) {
                            procesarEvento(mensaje, pusherBDPrincipal, pusherBDReplica, publisherSemaforos);
                        }
                    }

                    if (poller.pollin(1)) {
                        String solicitud = controlRep.recvStr(0);
                        String respuesta = procesarComandoManual(solicitud, publisherSemaforos);
                        controlRep.send(respuesta.getBytes(ZMQ.CHARSET), 0);
                    }

                    if (principalDisponible && !colaEventosPendientesPrimario.isEmpty()) {
                        sincronizarPendientesConPrincipal(pusherBDPrincipal);
                    }
                }
            } finally {
                subscriber.close();
                pusherBDPrincipal.close();
                pusherBDReplica.close();
                publisherSemaforos.close();
                controlRep.close();
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

            if (datos.priorizacionActiva()) {
                datos.estadoActual = EstadoTrafico.PRIORIZACION;
                long restantes = Math.max(0L, datos.priorizacionHasta - System.currentTimeMillis());
                System.out.println("[ANALÍTICA] Priorización activa en " + interseccion
                    + " | motivo=" + datos.motivoPrioridad
                    + " | orientación=" + datos.orientacionPrioridad
                    + " | restante=" + (restantes / 1000L) + "s");
                return;
            }

            EstadoTrafico estadoAnterior = datos.estadoActual;
            datos.estadoActual = ReglasTrafico.evaluarEstado(
                datos.cola, datos.vehiculosContados, datos.velocidadPromedio, datos.densidad, datos.nivelCongestion
            );


            if (estadoAnterior != datos.estadoActual) {
                int tiempoVerde = ReglasTrafico.obtenerTiempoSemaforo(datos.estadoActual);

                // La orientación se resuelve por heurística, pero la congestión ya fue
                // correlacionada antes: se requieren al menos 2 sensores críticos.
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

    private String procesarComandoManual(String solicitud, ZMQ.Socket semaforos) {
        try {
            if (solicitud == null || solicitud.trim().isEmpty()) {
                return "ERROR|Solicitud vacia";
            }

            String[] partes = solicitud.trim().split("\\s+");
            String comando = partes[0].toUpperCase();

            if ("PRIORIZAR".equals(comando)) {
                return aplicarPriorizacionManual(partes, semaforos);
            }

            if ("LIBERAR".equals(comando)) {
                return liberarPriorizacionManual(partes);
            }

            return "ERROR|Comando manual desconocido";
        } catch (Exception e) {
            return "ERROR|" + e.getMessage();
        }
    }

    private String aplicarPriorizacionManual(String[] partes, ZMQ.Socket semaforos) {
        if (partes.length < 2) {
            return "ERROR|Debe indicar la interseccion";
        }

        String interseccion = partes[1];
        String orientacion = "HORIZONTAL";
        int indice = 2;

        if (partes.length > indice && ("HORIZONTAL".equalsIgnoreCase(partes[indice]) || "VERTICAL".equalsIgnoreCase(partes[indice]))) {
            orientacion = partes[indice].toUpperCase();
            indice++;
        }

        int duracion = 60;
        if (partes.length > indice) {
            try {
                duracion = Integer.parseInt(partes[indice]);
                indice++;
            } catch (NumberFormatException ignored) {
            }
        }

        String motivo = "PRIORIDAD_MANUAL";
        if (partes.length > indice) {
            StringBuilder sb = new StringBuilder();
            for (int i = indice; i < partes.length; i++) {
                if (sb.length() > 0) {
                    sb.append(' ');
                }
                sb.append(partes[i]);
            }
            motivo = sb.toString();
        }

        DatosInterseccion datos = datosIntersecciones.computeIfAbsent(interseccion, k -> new DatosInterseccion());
        datos.estadoActual = EstadoTrafico.PRIORIZACION;
        datos.orientacionPrioridad = orientacion;
        datos.motivoPrioridad = motivo;
        datos.priorizacionHasta = System.currentTimeMillis() + (duracion * 1000L);

        String comandoSemaforo = String.format(
            "{\"interseccion\":\"%s\",\"estado\":\"VERDE\",\"tiempo\":%d,\"razon\":\"PRIORIZACION:%s\",\"orientacion\":\"%s\"}",
            interseccion, duracion, motivo, orientacion
        );
        semaforos.send(("COMANDO " + comandoSemaforo).getBytes(ZMQ.CHARSET), 0);

        System.out.println("\n╔════════════════════════════════════════════════════════════╗");
        System.out.println("║            PRIORIZACIÓN MANUAL APLICADA                   ║");
        System.out.println("╠════════════════════════════════════════════════════════════╣");
        System.out.println("║ Intersección:        " + String.format("%-38s", interseccion) + "║");
        System.out.println("║ Orientación:         " + String.format("%-38s", orientacion) + "║");
        System.out.println("║ Duración:            " + String.format("%-38s", duracion + "s") + "║");
        System.out.println("║ Motivo:              " + String.format("%-38s", motivo) + "║");
        System.out.println("╚════════════════════════════════════════════════════════════╝\n");

        return "OK|Priorizacion aplicada a " + interseccion + " por " + duracion + "s";
    }

    private String liberarPriorizacionManual(String[] partes) {
        if (partes.length < 2) {
            return "ERROR|Debe indicar la interseccion";
        }

        String interseccion = partes[1];
        DatosInterseccion datos = datosIntersecciones.computeIfAbsent(interseccion, k -> new DatosInterseccion());
        datos.priorizacionHasta = 0L;
        datos.estadoActual = EstadoTrafico.NORMAL;
        datos.motivoPrioridad = "";

        return "OK|Priorizacion liberada en " + interseccion;
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
    
    private void iniciarSupervisorFailover() {
        Thread supervisor = new Thread(() -> {
            boolean ultimoEstadoDisponible = true;
            while (!Thread.currentThread().isInterrupted()) {
                boolean principalViva = verificarDestino(bdPrincipalAddress, "BD_PRINCIPAL");
                principalDisponible = principalViva;

                if (principalViva && !ultimoEstadoDisponible) {
                    System.out.println("[ANALÍTICA] BD principal recuperada; pendiente sincronización de eventos acumulados");
                }

                if (!principalViva) {
                    if (!persistirEnReplica) {
                        persistirEnReplica = true;
                        failoverActiva = true;
                        System.out.println("[ANALÍTICA] Failover activado: persistencia redirigida a la réplica");
                    }
                } else if (failoverActiva && colaEventosPendientesPrimario.isEmpty()) {
                    persistirEnReplica = false;
                    failoverActiva = false;
                    System.out.println("[ANALÍTICA] Failover desactivado: el primario ya está disponible y sincronizado");
                }

                ultimoEstadoDisponible = principalViva;
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "analitica-failover-supervisor");

        supervisor.setDaemon(true);
        supervisor.start();
    }

    private boolean verificarDestino(String direccion, String etiqueta) {
        try (ZContext contextoPing = new ZContext()) {
            ZMQ.Socket req = contextoPing.createSocket(ZMQ.REQ);
            req.setReceiveTimeOut(1000);
            req.setSendTimeOut(1000);
            req.connect(direccion);
            req.send("PING".getBytes(ZMQ.CHARSET), 0);
            String respuesta = req.recvStr(0);
            req.close();
            return respuesta != null && respuesta.startsWith("PONG");
        } catch (Exception e) {
            return false;
        }
    }

    private void almacenarEvento(String jsonEvento, ZMQ.Socket bdPrincipal, ZMQ.Socket bdReplica) {
        byte[] data = jsonEvento.getBytes(ZMQ.CHARSET);

        boolean exito;
        if (persistirEnReplica) {
            exito = bdReplica.send(data, 0);
            if (!exito) {
                System.err.println("[ANALÍTICA] Error enviando a réplica, se conserva failover activo");
            }
            colaEventosPendientesPrimario.offer(jsonEvento);
        } else {
            exito = bdPrincipal.send(data, 0);
            if (!exito) {
                System.err.println("[ANALÍTICA] Error enviando a BD principal, activando failover a réplica");
                persistirEnReplica = true;
                failoverActiva = true;
                principalDisponible = false;
                colaEventosPendientesPrimario.offer(jsonEvento);
                boolean replicaOk = bdReplica.send(data, 0);
                if (!replicaOk) {
                    System.err.println("[ANALÍTICA] Error enviando también a la réplica");
                }
            }
        }
    }

    private void sincronizarPendientesConPrincipal(ZMQ.Socket bdPrincipal) {
        if (!principalDisponible) {
            return;
        }

        String evento;
        while ((evento = colaEventosPendientesPrimario.poll()) != null) {
            boolean enviado = bdPrincipal.send(evento.getBytes(ZMQ.CHARSET), 0);
            if (!enviado) {
                colaEventosPendientesPrimario.offer(evento);
                principalDisponible = false;
                persistirEnReplica = true;
                failoverActiva = true;
                System.out.println("[ANALÍTICA] Falló la sincronización con la BD principal; se mantiene failover activo");
                return;
            }
        }

        if (failoverActiva) {
            persistirEnReplica = false;
            failoverActiva = false;
            System.out.println("[ANALÍTICA] Sincronización completada: la BD principal recuperó los eventos pendientes");
        }
    }
    
    public static void main(String[] args) {
        try {
            Configuracion config = Configuracion.getInstance();
            
            String brokerAddress = "tcp://" + config.getPC1() + ":" + config.getBrokerPub();
            String bdPrincipalAddress = "tcp://" + config.getPC3() + ":" + config.getAnaliticaPushBdPrincipal();
            String bdReplicaAddress = "tcp://" + config.getPC2() + ":" + config.getAnaliticaPushBdReplica();
            String semaforosAddress = "tcp://*:" + config.getAnaliticaPubSemaforos();
            String controlAddress = "tcp://*:" + config.getAnaliticaRepControl();
            
            System.out.println("╔════════════════════════════════════════════╗");
            System.out.println("║          CONFIGURACIÓN SERVICIO           ║");
            System.out.println("╠════════════════════════════════════════════╣");
            System.out.println("║ Broker:       " + String.format("%-28s", brokerAddress) + "║");
            System.out.println("║ BD Principal: " + String.format("%-28s", bdPrincipalAddress) + "║");
            System.out.println("║ BD Réplica:   " + String.format("%-28s", bdReplicaAddress) + "║");
            System.out.println("║ Semáforos:    " + String.format("%-28s", semaforosAddress) + "║");
            System.out.println("║ Control REP:  " + String.format("%-28s", controlAddress) + "║");
            System.out.println("╚════════════════════════════════════════════╝\n");
            
            ServicioAnalitica servicio = new ServicioAnalitica(
                brokerAddress,
                bdPrincipalAddress,
                bdReplicaAddress,
                semaforosAddress,
                controlAddress
            );
            servicio.iniciar();
        } catch (Exception e) {
            System.err.println("Error iniciando servicio: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
