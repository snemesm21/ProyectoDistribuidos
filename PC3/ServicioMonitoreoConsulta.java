import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import java.util.Scanner;


public class ServicioMonitoreoConsulta {

    private final String bdPrincipalAddress;
    private final String bdReplicaAddress;
    private volatile boolean ultimoEstadoPrincipalDisponible;
    private volatile boolean sincronizandoBackup;

    public ServicioMonitoreoConsulta(String bdPrincipalAddress, String bdReplicaAddress) {
        this.bdPrincipalAddress = bdPrincipalAddress;
        this.bdReplicaAddress = bdReplicaAddress;
        this.ultimoEstadoPrincipalDisponible = true;
        this.sincronizandoBackup = false;
    }

    public void iniciar() {
        System.out.println("╔════════════════════════════════════════════╗");
        System.out.println("║   PC3 - MONITOREO Y CONSULTA DE TRÁFICO   ║");
        System.out.println("╠════════════════════════════════════════════╣");
        System.out.println("║ BD Principal: " + formatearCampo(bdPrincipalAddress) + "║");
        System.out.println("║ BD Réplica:   " + formatearCampo(bdReplicaAddress) + "║");
        System.out.println("╚════════════════════════════════════════════╝");

        iniciarSupervisorRecuperacion();

        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                mostrarMenu();
                String opcion = scanner.nextLine().trim();

                switch (opcion) {
                    case "1":
                        imprimirRespuesta("ESTADO");
                        break;
                    case "2":
                        System.out.print("Intersección (ej. INT-C5): ");
                        String interseccion = scanner.nextLine().trim();
                        imprimirRespuesta("INTERSECCION " + interseccion);
                        break;
                    case "3":
                        System.out.print("Fecha inicio (ISO, ej. 2026-02-09T15:00:00Z): ");
                        String inicio = scanner.nextLine().trim();
                        System.out.print("Fecha fin (ISO, ej. 2026-02-09T16:00:00Z): ");
                        String fin = scanner.nextLine().trim();
                        imprimirRespuesta("HISTORICO " + inicio + " " + fin);
                        break;
                    case "4":
                        System.out.print("Cantidad de eventos: ");
                        String limite = scanner.nextLine().trim();
                        if (limite.isEmpty()) {
                            limite = "10";
                        }
                        imprimirRespuesta("ULTIMOS " + limite);
                        break;
                    case "5":
                        manejarPriorizacionManual(scanner);
                        break;
                    case "6":
                        System.out.println("[MONITOREO] Saliendo del servicio.");
                        return;
                    default:
                        System.out.println("[MONITOREO] Opcion invalida. Intente otra vez.");
                }
            }
        }
    }

    private void mostrarMenu() {
        System.out.println();
        System.out.println("1) Estado general del sistema");
        System.out.println("2) Consulta por interseccion");
        System.out.println("3) Consulta historica por rango");
        System.out.println("4) Ultimos eventos");
        System.out.println("5) Priorizacion manual");
        System.out.println("6) Salir");
        System.out.print("> ");
    }

    private void manejarPriorizacionManual(Scanner scanner) {
        System.out.print("Interseccion (ej. INT-C5): ");
        String interseccion = scanner.nextLine().trim();
        System.out.print("Orientacion [HORIZONTAL/VERTICAL] (Enter = HORIZONTAL): ");
        String orientacion = scanner.nextLine().trim();
        if (orientacion.isEmpty()) {
            orientacion = "HORIZONTAL";
        }
        System.out.print("Duracion en segundos (Enter = 60): ");
        String duracion = scanner.nextLine().trim();
        if (duracion.isEmpty()) {
            duracion = "60";
        }
        System.out.print("Motivo (ej. AMBULANCIA): ");
        String motivo = scanner.nextLine().trim();
        if (motivo.isEmpty()) {
            motivo = "AMBULANCIA";
        }

        imprimirRespuestaControl("PRIORIZAR " + interseccion + " " + orientacion + " " + duracion + " " + motivo);
    }

    private void imprimirRespuesta(String solicitud) {
        String respuesta = consultarConFailover(solicitud);
        System.out.println();
        System.out.println("[MONITOREO] Solicitud: " + solicitud);
        System.out.println("[MONITOREO] Respuesta:\n" + respuesta);
    }

    private void imprimirRespuestaControl(String solicitud) {
        String respuesta = enviarControlManual(solicitud);
        System.out.println();
        System.out.println("[MONITOREO] Solicitud de control: " + solicitud);
        System.out.println("[MONITOREO] Respuesta:\n" + respuesta);
    }

    private String consultarConFailover(String solicitud) {
        String respuesta = consultarEnServidor(bdPrincipalAddress, solicitud);
        if (respuesta != null && !respuesta.startsWith("ERROR")) {
            System.out.println("[MONITOREO] Respuesta obtenida desde BD principal");
            return respuesta;
        }

        if (respuesta == null) {
            System.out.println("[MONITOREO] BD principal sin respuesta, usando réplica...");
        } else {
            System.out.println("[MONITOREO] BD principal respondió con error, usando réplica...");
        }

        String respuestaReplica = consultarEnServidor(bdReplicaAddress, solicitud);
        if (respuestaReplica != null) {
            System.out.println("[MONITOREO] Respuesta obtenida desde BD réplica");
            return respuestaReplica;
        }

        return "ERROR|No fue posible consultar la base principal ni la réplica";
    }

    private String consultarEnServidor(String direccion, String solicitud) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket req = context.createSocket(ZMQ.REQ);
            req.setReceiveTimeOut(3000);
            req.setSendTimeOut(3000);
            req.connect(direccion);

            req.send(solicitud.getBytes(ZMQ.CHARSET), 0);
            String respuesta = req.recvStr(0);
            req.close();
            return respuesta;
        } catch (Exception e) {
            return null;
        }
    }

    private void iniciarSupervisorRecuperacion() {
        Thread supervisor = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                boolean disponible = verificarPrincipal();
                if (disponible && !ultimoEstadoPrincipalDisponible && !sincronizandoBackup) {
                    sincronizandoBackup = true;
                    try {
                        sincronizarBackupCompleto();
                    } finally {
                        sincronizandoBackup = false;
                    }
                }
                ultimoEstadoPrincipalDisponible = disponible;

                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, "monitoreo-recuperacion-backup");

        supervisor.setDaemon(true);
        supervisor.start();
    }

    private boolean verificarPrincipal() {
        String respuesta = consultarEnServidor(bdPrincipalAddress, "PING");
        return respuesta != null && respuesta.startsWith("PONG");
    }

    private void sincronizarBackupCompleto() {
        System.out.println("[MONITOREO] PC3 volvió. Solicitando snapshot completo a la réplica...");

        String respuestaBackup = enviarSolicitudDirecta(bdReplicaAddress, "BACKUP_EXPORT");
        if (respuestaBackup == null || !respuestaBackup.startsWith("OK|BACKUP_EXPORT")) {
            System.out.println("[MONITOREO] No fue posible exportar el backup desde la réplica");
            return;
        }

        int salto = respuestaBackup.indexOf('\n');
        String payload = salto == -1 ? "" : respuestaBackup.substring(salto + 1);
        String respuestaRestore = enviarSolicitudDirecta(bdPrincipalAddress, "BACKUP_RESTORE\n" + payload);

        if (respuestaRestore != null && respuestaRestore.startsWith("OK|BACKUP_RESTORE")) {
            System.out.println("[MONITOREO] Snapshot aplicado en la BD principal: " + respuestaRestore);
        } else {
            System.out.println("[MONITOREO] Falló la restauración del snapshot en la BD principal");
        }
    }

    private String enviarSolicitudDirecta(String direccion, String solicitud) {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket req = context.createSocket(ZMQ.REQ);
            req.setReceiveTimeOut(4000);
            req.setSendTimeOut(4000);
            req.connect(direccion);
            req.send(solicitud.getBytes(ZMQ.CHARSET), 0);
            String respuesta = req.recvStr(0);
            req.close();
            return respuesta;
        } catch (Exception e) {
            return null;
        }
    }

    private String enviarControlManual(String solicitud) {
        Configuracion config = Configuracion.getInstance();
        String direccion = "tcp://" + config.getPC2() + ":" + config.getAnaliticaRepControl();

        try (ZContext context = new ZContext()) {
            ZMQ.Socket req = context.createSocket(ZMQ.REQ);
            req.setReceiveTimeOut(3000);
            req.setSendTimeOut(3000);
            req.connect(direccion);

            req.send(solicitud.getBytes(ZMQ.CHARSET), 0);
            String respuesta = req.recvStr(0);
            req.close();
            return respuesta != null ? respuesta : "ERROR|Sin respuesta del servicio de analítica";
        } catch (Exception e) {
            return "ERROR|No fue posible enviar el comando manual: " + e.getMessage();
        }
    }

    private String formatearCampo(String valor) {
        String resultado = valor == null ? "N/A" : valor;
        if (resultado.length() >= 31) {
            return resultado.substring(0, 31);
        }
        return String.format("%-31s", resultado);
    }

    public static void main(String[] args) {
        Configuracion config = Configuracion.getInstance();
        String bdPrincipalAddress = "tcp://" + config.getPC3() + ":" + config.getBdPrincipalRep();
        String bdReplicaAddress = "tcp://" + config.getPC2() + ":" + config.getBdReplicaRep();

        ServicioMonitoreoConsulta servicio = new ServicioMonitoreoConsulta(
            bdPrincipalAddress,
            bdReplicaAddress
        );
        servicio.iniciar();
    }
}