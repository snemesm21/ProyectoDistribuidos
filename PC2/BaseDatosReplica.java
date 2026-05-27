import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BaseDatosReplica {
    private String puertoRecepcion;
    private String puertoConsulta;
    private String archivoDB;
    private int contadorEventos;

    private Connection conexion;
    private PreparedStatement stmtInsertarEvento;
    private PreparedStatement stmtTotalEventos;
    private PreparedStatement stmtEventosPorInterseccion;
    private PreparedStatement stmtVelocidadPromedioPorInterseccion;
    private PreparedStatement stmtUltimosEventos;
    private PreparedStatement stmtEventosPorRango;

    private static final String SQL_CREAR_TABLA_EVENTOS =
        "CREATE TABLE IF NOT EXISTS eventos (" +
        "id INTEGER PRIMARY KEY AUTOINCREMENT," +
        "recibido_en TEXT NOT NULL," +
        "tipo_evento TEXT," +
        "sensor_id TEXT," +
        "tipo_sensor TEXT," +
        "interseccion TEXT," +
        "vehiculos_contados INTEGER," +
        "intervalo_segundos INTEGER," +
        "timestamp_inicio TEXT," +
        "timestamp_fin TEXT," +
        "volumen INTEGER," +
        "velocidad_promedio REAL," +
        "nivel_congestion TEXT," +
        "timestamp_evento TEXT," +
        "json_raw TEXT NOT NULL" +
        ")";
    private static final String SQL_INSERTAR_EVENTO =
        "INSERT INTO eventos (recibido_en, tipo_evento, sensor_id, tipo_sensor, interseccion, " +
        "vehiculos_contados, intervalo_segundos, timestamp_inicio, timestamp_fin, volumen, " +
        "velocidad_promedio, nivel_congestion, timestamp_evento, json_raw) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    
    public BaseDatosReplica(String puertoRecepcion, String puertoConsulta, String archivoDB) {
        this.puertoRecepcion = puertoRecepcion;
        this.puertoConsulta = puertoConsulta;
        this.archivoDB = archivoDB;
        this.contadorEventos = 0;
    }

    public BaseDatosReplica(String puertoRecepcion, String archivoDB) {
        this(puertoRecepcion, null, archivoDB);
    }
    
    public void iniciar() {
        try {
            inicializarBaseDatos();
            prepararConsultasMonitoreo();
            probarConexionMonitoreo();
            iniciarServidorConsultas();
        } catch (Exception e) {
            System.err.println("[ERROR BD RÉPLICA] Inicializando SQLite: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        try (ZContext context = new ZContext()) {
            ZMQ.Socket puller = context.createSocket(ZMQ.PULL);
            puller.bind("tcp://*:" + puertoRecepcion);
            
            System.out.println("=================================");
            System.out.println("[BD RÉPLICA] Servicio iniciado");
            System.out.println("[BD RÉPLICA] Puerto: " + puertoRecepcion);
            System.out.println("[BD RÉPLICA] SQLite: " + archivoDB);
            System.out.println("=================================\n");
            
            while (!Thread.currentThread().isInterrupted()) {
                String evento = puller.recvStr(0);
                
                if (evento != null) {
                    almacenarEventoEnSQLite(evento);
                }
            }
            
            puller.close();
            
        } catch (Exception e) {
            System.err.println("[ERROR BD RÉPLICA]: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cerrarRecursosBD();
        }
    }

    private void iniciarServidorConsultas() {
        if (puertoConsulta == null || puertoConsulta.isBlank()) {
            return;
        }

        Thread servidorConsultas = new Thread(() -> {
            try (ZContext contextConsultas = new ZContext()) {
                ZMQ.Socket rep = contextConsultas.createSocket(ZMQ.REP);
                rep.bind("tcp://*:" + puertoConsulta);
                System.out.println("[BD RÉPLICA] Consulta REP activa en puerto " + puertoConsulta);

                while (!Thread.currentThread().isInterrupted()) {
                    String solicitud = rep.recvStr(0);
                    if (solicitud == null) {
                        continue;
                    }

                    try {
                        Configuracion conf = Configuracion.getInstance();
                        if (conf.isHmacEnabled()) {
                            boolean ok = HmacUtil.verifyPlainWithSuffix(solicitud, conf.getSharedSecret());
                            if (!ok) {
                                rep.send("ERROR|Firma inválida".getBytes(ZMQ.CHARSET), 0);
                                continue;
                            }
                            int idx = solicitud.lastIndexOf("||SIG:");
                            if (idx != -1) solicitud = solicitud.substring(0, idx);
                        }
                    } catch (Throwable ignored) {}

                    String respuesta = manejarSolicitudConsulta(solicitud);
                    // sign reply if enabled
                    try {
                        Configuracion conf2 = Configuracion.getInstance();
                        if (conf2.isHmacEnabled()) {
                            respuesta = HmacUtil.signPlainWithSuffix(respuesta, conf2.getSharedSecret());
                        }
                    } catch (Throwable ignored) {}

                    rep.send(respuesta.getBytes(ZMQ.CHARSET), 0);
                }

                rep.close();
            } catch (Exception e) {
                System.err.println("[ERROR BD RÉPLICA] Servidor de consultas: " + e.getMessage());
            }
        }, "bd-replica-consultas");

        servidorConsultas.setDaemon(true);
        servidorConsultas.start();
    }

    private void inicializarBaseDatos() throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        conexion = DriverManager.getConnection("jdbc:sqlite:" + archivoDB);
        
        try (Statement stmt = conexion.createStatement()) {
            try {
                stmt.execute("PRAGMA journal_mode = WAL");
            } catch (SQLException ignore) {
            }
            try {
                stmt.execute("PRAGMA busy_timeout = 5000");
            } catch (SQLException ignore) {
            }

            stmt.execute(SQL_CREAR_TABLA_EVENTOS);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_eventos_interseccion ON eventos(interseccion)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_eventos_tipo ON eventos(tipo_evento)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_eventos_timestamp ON eventos(timestamp_evento)");
            stmt.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_eventos_json_raw ON eventos(json_raw)");
        }

        stmtInsertarEvento = conexion.prepareStatement(SQL_INSERTAR_EVENTO);
    }

    private void prepararConsultasMonitoreo() throws SQLException {
        stmtTotalEventos = conexion.prepareStatement("SELECT COUNT(*) FROM eventos");
        stmtEventosPorInterseccion = conexion.prepareStatement(
            "SELECT COUNT(*) FROM eventos WHERE interseccion = ?"
        );
        stmtVelocidadPromedioPorInterseccion = conexion.prepareStatement(
            "SELECT AVG(velocidad_promedio) FROM eventos WHERE interseccion = ? AND velocidad_promedio IS NOT NULL"
        );
        stmtUltimosEventos = conexion.prepareStatement(
            "SELECT id, tipo_evento, interseccion, timestamp_evento FROM eventos ORDER BY id DESC LIMIT ?"
        );
        stmtEventosPorRango = conexion.prepareStatement(
            "SELECT id, tipo_evento, interseccion, timestamp_evento FROM eventos WHERE timestamp_evento BETWEEN ? AND ? ORDER BY timestamp_evento DESC"
        );

        System.out.println("[BD RÉPLICA] Consultas de monitoreo preparadas");
    }

    private void probarConexionMonitoreo() throws SQLException {
        try (Statement stmt = conexion.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            if (rs.next() && rs.getInt(1) == 1) {
                System.out.println("[BD RÉPLICA] Conexión SQLite verificada para monitoreo");
            }
        }
    }

    private synchronized void almacenarEventoEnSQLite(String eventoJson) {
        try {
            Configuracion conf = Configuracion.getInstance();
            if (conf.isHmacEnabled()) {
                boolean ok = HmacUtil.verifyJson(eventoJson, conf.getSharedSecret());
                if (!ok) {
                    System.err.println("[BD RÉPLICA][DROP] Evento con firma inválida descartado");
                    return;
                }
            }
        } catch (Throwable ignored) {}

        contadorEventos++;
        String recibidoEn = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        String tipoEvento = extraerCampoTexto(eventoJson, "tipo_evento");
        String sensorId = extraerCampoTexto(eventoJson, "sensor_id");
        String tipoSensor = extraerCampoTexto(eventoJson, "tipo_sensor");
        String interseccion = extraerCampoTexto(eventoJson, "interseccion");


        if (sensorId == null) sensorId = buscarCampoFlexible(eventoJson, "sensor_id");
        if (tipoSensor == null) tipoSensor = buscarCampoFlexible(eventoJson, "tipo_sensor");
        if (interseccion == null) interseccion = buscarCampoFlexible(eventoJson, "interseccion");
        Integer vehiculosContados = extraerCampoEntero(eventoJson, "vehiculos_contados");
        Integer intervaloSegundos = extraerCampoEntero(eventoJson, "intervalo_segundos");
        String timestampInicio = extraerCampoTexto(eventoJson, "timestamp_inicio");
        String timestampFin = extraerCampoTexto(eventoJson, "timestamp_fin");
        Integer volumen = extraerCampoEntero(eventoJson, "volumen");
        Double velocidadPromedio = extraerCampoDecimal(eventoJson, "velocidad_promedio");
        String nivelCongestion = extraerCampoTexto(eventoJson, "nivel_congestion");
        String timestampEvento = extraerCampoTexto(eventoJson, "timestamp");

        if (timestampEvento == null) {
            timestampEvento = timestampFin != null ? timestampFin : timestampInicio;
        }

        try {
            stmtInsertarEvento.setString(1, recibidoEn);
            stmtInsertarEvento.setString(2, tipoEvento);
            stmtInsertarEvento.setString(3, sensorId);
            stmtInsertarEvento.setString(4, tipoSensor);
            stmtInsertarEvento.setString(5, interseccion);
            if (vehiculosContados != null) {
                stmtInsertarEvento.setInt(6, vehiculosContados);
            } else {
                stmtInsertarEvento.setNull(6, java.sql.Types.INTEGER);
            }
            if (intervaloSegundos != null) {
                stmtInsertarEvento.setInt(7, intervaloSegundos);
            } else {
                stmtInsertarEvento.setNull(7, java.sql.Types.INTEGER);
            }
            stmtInsertarEvento.setString(8, timestampInicio);
            stmtInsertarEvento.setString(9, timestampFin);
            if (volumen != null) {
                stmtInsertarEvento.setInt(10, volumen);
            } else {
                stmtInsertarEvento.setNull(10, java.sql.Types.INTEGER);
            }
            if (velocidadPromedio != null) {
                stmtInsertarEvento.setDouble(11, velocidadPromedio);
            } else {
                stmtInsertarEvento.setNull(11, java.sql.Types.REAL);
            }
            stmtInsertarEvento.setString(12, nivelCongestion);
            stmtInsertarEvento.setString(13, timestampEvento);
            stmtInsertarEvento.setString(14, eventoJson);
            stmtInsertarEvento.executeUpdate();
            String detalles = "";
            if ("camara".equalsIgnoreCase(tipoSensor)) {
                detalles = "Q=" + (volumen != null ? volumen : "N/A") + " veh";
            } else if ("espira_inductiva".equalsIgnoreCase(tipoSensor)) {
                detalles = "Cv=" + (vehiculosContados != null ? vehiculosContados : "N/A") + " veh/" + 
                          (intervaloSegundos != null ? intervaloSegundos : "?") + "s";
            } else if ("gps".equalsIgnoreCase(tipoSensor)) {
                detalles = "Vp=" + (velocidadPromedio != null ? String.format("%.1f", velocidadPromedio) : "N/A") + 
                          " km/h | Nivel=" + (nivelCongestion != null ? nivelCongestion : "N/A");
            }
            
            String tipoSensorPrint = tipoSensor != null ? tipoSensor.toUpperCase() : "???";
            String interPrint = interseccion != null ? interseccion : "?";
            System.out.println("[BD RÉPLICA] Evento #" + contadorEventos + " | " + tipoSensorPrint + " | INT-" + interPrint + " | " + detalles);
        } catch (SQLException e) {
            System.err.println("[ERROR BD RÉPLICA] Insertando evento en SQLite: " + e.getMessage());
        }

        if (contadorEventos % 10 == 0) {
            System.out.println("[BD RÉPLICA] " + contadorEventos + " eventos persistidos\n");
        }
    }

    private synchronized String manejarSolicitudConsulta(String solicitud) {
        try {
            String[] partes = solicitud.trim().split("\\s+");
            if (partes.length == 0) {
                return "ERROR|Solicitud vacia";
            }

            String comando = partes[0].toUpperCase();
            if ("PING".equals(comando) || "HEALTH".equals(comando)) {
                return "PONG|BD_REPLICA|" + archivoDB + "|" + contadorEventos;
            }
            if ("BACKUP_EXPORT".equals(comando)) {
                return exportarBackupCompleto();
            }
            switch (comando) {
                case "ESTADO":
                case "RESUMEN":
                    return consultarEstadoSistema();
                case "INTERSECCION":
                    if (partes.length < 2) {
                        return "ERROR|Debe indicar la interseccion";
                    }
                    return consultarInterseccion(partes[1]);
                case "HISTORICO":
                    if (partes.length >= 4) {
                        return consultarHistorico(partes[1], partes[2], partes[3]);
                    } else if (partes.length == 3) {
                        return consultarHistorico(null, partes[1], partes[2]);
                    } else if (partes.length == 2) {
                        return consultarHistorico(partes[1], null, null);
                    }
                    return consultarHistorico(null, null, null);
                case "CAMBIOS_ESTADO":
                    if (partes.length >= 4) {
                        return consultarCambiosEstado(partes[1], partes[2], partes[3]);
                    } else if (partes.length == 2) {
                        return consultarCambiosEstado(partes[1], null, null);
                    }
                    return consultarCambiosEstado(null, null, null);
                case "PRIORIZACION":
                    if (partes.length >= 4) {
                        return consultarPriorizacion(partes[1], partes[2], partes[3]);
                    } else if (partes.length == 2) {
                        return consultarPriorizacion(partes[1], null, null);
                    }
                    return consultarPriorizacion(null, null, null);
                case "CONGESTION":
                    if (partes.length >= 3) {
                        return consultarEstadisticasCongestion(partes[1], partes[2]);
                    }
                    return consultarEstadisticasCongestion(null, null);
                case "VELOCIDAD_PROMEDIO":
                    if (partes.length < 2) {
                        return "ERROR|Debe indicar la interseccion";
                    }
                    return consultarVelocidadPromedioHistorica(partes[1]);
                case "EVENTOS_RANGO":
                    if (partes.length >= 3) {
                        return consultarEventosRango(partes[1], partes[2]);
                    }
                    return consultarEventosRango(null, null);
                case "TASA_ALMACENAMIENTO":
                    if (partes.length >= 3) {
                        return consultarTasaAlmacenamiento(partes[1], partes[2]);
                    }
                    return consultarTasaAlmacenamiento(null, null);
                case "LATENCIA_PROMEDIO":
                    if (partes.length >= 3) {
                        return consultarLatenciaPromedio(partes[1], partes[2]);
                    }
                    return consultarLatenciaPromedio(null, null);
                case "LATENCIA_ESTADISTICAS":
                    if (partes.length >= 3) {
                        return consultarLatenciaEstadisticas(partes[1], partes[2]);
                    }
                    return consultarLatenciaEstadisticas(null, null);
                case "ULTIMOS":
                    int limite = 10;
                    if (partes.length >= 2) {
                        limite = Math.max(1, Integer.parseInt(partes[1]));
                    }
                    return consultarUltimosEventos(limite);
                case "AYUDA":
                    return "OK|Comandos: ESTADO, INTERSECCION <INT-X>, HISTORICO <INT-X?> <inicio> <fin>, CAMBIOS_ESTADO <INT-X> <inicio> <fin>, PRIORIZACION <INT-X> <inicio> <fin>, CONGESTION <inicio> <fin>, VELOCIDAD_PROMEDIO <INT-X>, EVENTOS_RANGO <inicio> <fin>, TASA_ALMACENAMIENTO <inicio> <fin>, LATENCIA_PROMEDIO <inicio> <fin>, LATENCIA_ESTADISTICAS <inicio> <fin>, ULTIMOS <n>";
                default:
                    return "ERROR|Comando desconocido: " + comando;
            }
        } catch (Exception e) {
            return "ERROR|" + e.getMessage();
        }
    }

    private String consultarEstadoSistema() throws SQLException {
        int totalEventos = 0;
        try (ResultSet rs = stmtTotalEventos.executeQuery()) {
            if (rs.next()) {
                totalEventos = rs.getInt(1);
            }
        }

        StringBuilder tipos = new StringBuilder();
        try (Statement stmt = conexion.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT COALESCE(tipo_sensor, 'desconocido') AS tipo_sensor, COUNT(*) AS total " +
                 "FROM eventos GROUP BY COALESCE(tipo_sensor, 'desconocido') ORDER BY total DESC, tipo_sensor ASC"
             )) {
            while (rs.next()) {
                if (tipos.length() > 0) {
                    tipos.append(", ");
                }
                tipos.append(rs.getString("tipo_sensor")).append("=").append(rs.getInt("total"));
            }
        }

        String ultimoTimestamp = "N/A";
        try (Statement stmt = conexion.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT timestamp_evento FROM eventos WHERE timestamp_evento IS NOT NULL ORDER BY id DESC LIMIT 1"
             )) {
            if (rs.next()) {
                String valor = rs.getString(1);
                if (valor != null && !valor.isBlank()) {
                    ultimoTimestamp = valor;
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=ESTADO\n")
            .append("ARCHIVO_DB=").append(archivoDB).append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos).append('\n')
            .append("EVENTOS_POR_TIPO=").append(tipos.length() == 0 ? "sin_datos" : tipos).append('\n')
            .append("ULTIMO_EVENTO=").append(ultimoTimestamp)
            .toString();
    }

    private String consultarInterseccion(String interseccion) throws SQLException {
        Integer q = null;
        Integer cv = null;
        Double vp = null;
        Double d = null;
        String congestion = "N/A";
        String estadoTrafico = "N/A";
        String orientacion = "N/A";
        String timestampEstado = "N/A";

        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT volumen, velocidad_promedio, timestamp_evento FROM eventos WHERE interseccion = ? AND tipo_sensor = 'camara' ORDER BY id DESC LIMIT 1"
             )) {
            stmt.setString(1, interseccion);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    q = rs.getObject("volumen") != null ? rs.getInt("volumen") : null;
                    vp = rs.getObject("velocidad_promedio") != null ? rs.getDouble("velocidad_promedio") : null;
                }
            }
        }

        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT vehiculos_contados, timestamp_evento FROM eventos WHERE interseccion = ? AND tipo_sensor = 'espira_inductiva' ORDER BY id DESC LIMIT 1"
             )) {
            stmt.setString(1, interseccion);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    cv = rs.getObject("vehiculos_contados") != null ? rs.getInt("vehiculos_contados") : null;
                }
            }
        }

        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT velocidad_promedio, nivel_congestion, json_raw, timestamp_evento FROM eventos WHERE interseccion = ? AND tipo_sensor = 'gps' ORDER BY id DESC LIMIT 1"
             )) {
            stmt.setString(1, interseccion);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    vp = rs.getObject("velocidad_promedio") != null ? rs.getDouble("velocidad_promedio") : vp;
                    d = extraerCampoDecimal(rs.getString("json_raw"), "densidad");
                    congestion = valorTexto(rs.getString("nivel_congestion"));
                }
            }
        }

        String jsonEstado = null;
        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT json_raw, timestamp_evento FROM eventos WHERE interseccion = ? AND tipo_evento IN ('CAMBIO_ESTADO', 'PRIORIZACION') ORDER BY id DESC LIMIT 1"
             )) {
            stmt.setString(1, interseccion);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    jsonEstado = rs.getString("json_raw");
                    String estadoNuevo = jsonEstado != null ? extraerCampoTexto(jsonEstado, "estado_nuevo") : null;
                    estadoTrafico = valorTexto(estadoNuevo);
                    if ("N/A".equals(estadoTrafico)) {
                        String tipoEvento = jsonEstado != null ? extraerCampoTexto(jsonEstado, "tipo_evento") : null;
                        estadoTrafico = valorTexto(tipoEvento);
                    }
                    orientacion = valorTexto(jsonEstado != null ? extraerCampoTexto(jsonEstado, "orientacion") : null);
                    timestampEstado = valorTexto(rs.getString("timestamp_evento"));
                }
            }
        }

        String ejeVerde = "N/A";
        if (!"N/A".equalsIgnoreCase(orientacion)) {
            ejeVerde = orientacion;
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=INTERSECCION\n")
            .append("INTERSECCION=").append(interseccion).append('\n')
            .append("Q=").append(valorNumerico(q)).append('\n')
            .append("Cv=").append(valorNumerico(cv)).append('\n')
            .append("Vp=").append(valorDecimal(vp)).append('\n')
            .append("D=").append(valorDecimal(d)).append('\n')
            .append("CONGESTION=").append(congestion).append('\n')
            .append("ESTADO_TRAFICO=").append(estadoTrafico).append('\n')
            .append("SEMAFORO_VERDE=").append(ejeVerde).append('\n')
            .append("ULTIMO_CAMBIO=").append(timestampEstado)
            .toString();
    }

    private String consultarHistorico(String fechaInicio, String fechaFin) throws SQLException {
        return consultarHistorico(null, fechaInicio, fechaFin);
    }

    private String consultarHistorico(String interseccion, String fechaInicio, String fechaFin) throws SQLException {
        StringBuilder eventos = new StringBuilder();
        int totalEventos = 0;
        String sql = "SELECT id, tipo_evento, tipo_sensor, json_raw FROM eventos WHERE tipo_evento IN ('CAMARA', 'ESPIRA', 'GPS') ORDER BY id DESC LIMIT 500";

        try (PreparedStatement stmt = conexion.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next() && totalEventos < 50) {
                    String json = rs.getString("json_raw");
                    String inter = extraerCampoTexto(json, "interseccion");
                    if (inter == null || inter.isBlank()) inter = buscarCampoFlexible(json, "interseccion");

                    if (interseccion != null && !interseccion.isBlank() && (inter == null || !inter.equalsIgnoreCase(interseccion))) {
                        continue;
                    }

                    totalEventos++;
                    if (eventos.length() > 0) eventos.append('\n');

                    String tipoSensor = extraerCampoTexto(json, "tipo_sensor");
                    if (tipoSensor == null) tipoSensor = buscarCampoFlexible(json, "tipo_sensor");
                    String tipoEvento = extraerCampoTexto(json, "tipo_evento");
                    if (tipoEvento == null) tipoEvento = buscarCampoFlexible(json, "tipo_evento");

                    String q = extraerCampoNumerico(json, "volumen");
                    String cv = extraerCampoNumerico(json, "vehiculos_contados");
                    String vp = extraerCampoNumerico(json, "velocidad_promedio");
                    String nivel = extraerCampoTexto(json, "nivel_congestion");
                    eventos.append("- #").append(rs.getInt("id"))
                        .append(" | ").append(valorTexto(tipoSensor))
                        .append(" | ").append(valorTexto(tipoEvento))
                        .append(" | ").append(valorTexto(inter))
                        .append(" | Q=").append(q != null ? q : "N/A")
                        .append(" | Cv=").append(cv != null ? cv : "N/A")
                        .append(" | Vp=").append(vp != null ? vp : "N/A")
                        .append(" | Nivel=").append(valorTexto(nivel));
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=HISTORICO\n")
            .append("INTERSECCION=").append(interseccion != null && !interseccion.isBlank() ? interseccion : "TODAS").append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos).append('\n')
            .append("EVENTOS=\n")
            .append(eventos.length() == 0 ? "- sin eventos" : eventos)
            .toString();
    }

    private String consultarCambiosEstado(String interseccion, String fechaInicio, String fechaFin) throws SQLException {
        StringBuilder eventos = new StringBuilder();
        int totalEventos = 0;
        String sql = "SELECT json_raw, timestamp_evento FROM eventos WHERE tipo_evento = 'CAMBIO_ESTADO' ORDER BY id DESC LIMIT 500";

        try (PreparedStatement stmt = conexion.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String json = rs.getString("json_raw");
                    String inter = extraerCampoTexto(json, "interseccion");
                    if (inter == null || inter.isBlank()) inter = buscarCampoFlexible(json, "interseccion");

                    if (interseccion != null && !interseccion.isBlank() && (inter == null || !inter.equalsIgnoreCase(interseccion))) {
                        continue;
                    }

                    totalEventos++;
                    if (eventos.length() > 0) eventos.append('\n');
                    String ts = extraerCampoTexto(json, "timestamp");
                    if (ts == null) ts = valorTexto(rs.getString("timestamp_evento"));

                    eventos.append("- ts=").append(valorTexto(ts))
                        .append(" | INT=").append(valorTexto(extraerCampoTexto(json, "interseccion")))
                        .append(" | ANTERIOR=").append(valorTexto(extraerCampoTexto(json, "estado_anterior")))
                        .append(" | NUEVO=").append(valorTexto(extraerCampoTexto(json, "estado_nuevo")))
                        .append(" | Q=").append(valorTexto(extraerCampoTexto(json, "q")))
                        .append(" | Cv=").append(valorTexto(extraerCampoTexto(json, "cv")))
                        .append(" | Vp=").append(valorTexto(extraerCampoTexto(json, "vp")))
                        .append(" | D=").append(valorTexto(extraerCampoTexto(json, "d")))
                        .append(" | Nivel=").append(valorTexto(extraerCampoTexto(json, "nivel_congestion")))
                        .append(" | Orientacion=").append(valorTexto(extraerCampoTexto(json, "orientacion")));
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=CAMBIOS_ESTADO\n")
            .append("INTERSECCION=").append(interseccion != null && !interseccion.isBlank() ? interseccion : "TODAS").append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos).append('\n')
            .append("EVENTOS=\n")
            .append(eventos.length() == 0 ? "- sin cambios de estado" : eventos)
            .toString();
    }

    private String consultarPriorizacion(String interseccion, String fechaInicio, String fechaFin) throws SQLException {
        StringBuilder eventos = new StringBuilder();
        int totalEventos = 0;
        String sql = "SELECT json_raw, timestamp_evento FROM eventos WHERE tipo_evento = 'PRIORIZACION' ORDER BY id DESC LIMIT 500";

        try (PreparedStatement stmt = conexion.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String json = rs.getString("json_raw");
                    String inter = extraerCampoTexto(json, "interseccion");
                    if (inter == null || inter.isBlank()) inter = buscarCampoFlexible(json, "interseccion");

                    if (interseccion != null && !interseccion.isBlank() && (inter == null || !inter.equalsIgnoreCase(interseccion))) {
                        continue;
                    }

                    totalEventos++;
                    if (eventos.length() > 0) eventos.append('\n');
                    String ts = extraerCampoTexto(json, "timestamp");
                    if (ts == null) ts = valorTexto(rs.getString("timestamp_evento"));

                    eventos.append("- ts=").append(valorTexto(ts))
                        .append(" | INT=").append(valorTexto(extraerCampoTexto(json, "interseccion")))
                        .append(" | DURACION=").append(valorTexto(extraerCampoTexto(json, "duracion_verde"))).append('s')
                        .append(" | CAUSA=").append(valorTexto(extraerCampoTexto(json, "causa")))
                        .append(" | MOTIVO=").append(valorTexto(extraerCampoTexto(json, "motivo")))
                        .append(" | ORIENTACION=").append(valorTexto(extraerCampoTexto(json, "orientacion")));
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=PRIORIZACION\n")
            .append("INTERSECCION=").append(interseccion != null && !interseccion.isBlank() ? interseccion : "TODAS").append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos).append('\n')
            .append("EVENTOS=\n")
            .append(eventos.length() == 0 ? "- sin priorizaciones" : eventos)
            .toString();
    }

    private String consultarEstadisticasCongestion(String fechaInicio, String fechaFin) throws SQLException {
        StringBuilder resumen = new StringBuilder();
        java.util.Map<String, Integer> counts = new java.util.HashMap<>();
        String sql = "SELECT json_raw FROM eventos WHERE tipo_evento = 'CAMBIO_ESTADO' AND json_raw LIKE '%\"estado_nuevo\":\"CONGESTION\"%' ORDER BY id DESC LIMIT 1000";
        try (PreparedStatement stmt = conexion.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                String json = rs.getString(1);
                String inter = extraerCampoTexto(json, "interseccion");
                if (inter == null || inter.isBlank()) inter = buscarCampoFlexible(json, "interseccion");
                if (inter == null || inter.isBlank()) inter = "DESCONOCIDA";
                counts.put(inter, counts.getOrDefault(inter, 0) + 1);
            }
        }

        java.util.List<java.util.Map.Entry<String,Integer>> lista = new java.util.ArrayList<>(counts.entrySet());
        lista.sort((a,b) -> b.getValue().compareTo(a.getValue()));
        for (java.util.Map.Entry<String,Integer> e : lista) {
            if (resumen.length() > 0) resumen.append('\n');
            resumen.append("- ").append(valorTexto(e.getKey())).append(" | TRANSICIONES_CONGESTION=").append(e.getValue());
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=CONGESTION\n")
            .append("RESULTADOS=\n")
            .append(resumen.length() == 0 ? "- sin transiciones a congestión" : resumen)
            .toString();
    }

    private String consultarVelocidadPromedioHistorica(String interseccion) throws SQLException {
        Double velocidadPromedio = null;
        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT AVG(velocidad_promedio) FROM eventos WHERE interseccion = ? AND tipo_sensor = 'gps' AND velocidad_promedio IS NOT NULL"
             )) {
            stmt.setString(1, interseccion);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    double valor = rs.getDouble(1);
                    if (!rs.wasNull()) {
                        velocidadPromedio = valor;
                    }
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=VELOCIDAD_PROMEDIO\n")
            .append("INTERSECCION=").append(interseccion).append('\n')
            .append("VELOCIDAD_PROMEDIO=").append(velocidadPromedio != null ? String.format("%.2f", velocidadPromedio) : "N/A")
            .toString();
    }

    private String consultarEventosRango(String fechaInicio, String fechaFin) throws SQLException {
        if (fechaInicio == null || fechaInicio.isBlank() || fechaFin == null || fechaFin.isBlank()) {
            try (Statement s = conexion.createStatement();
                 ResultSet rs = s.executeQuery("SELECT MIN(timestamp_evento), MAX(timestamp_evento) FROM eventos WHERE timestamp_evento IS NOT NULL")) {
                if (rs.next()) {
                    fechaInicio = rs.getString(1);
                    fechaFin = rs.getString(2);
                }
            }
            if (fechaInicio == null || fechaFin == null) {
                return "OK\nCONSULTA=EVENTOS_RANGO\nDESDE=N/A\nHASTA=N/A\nTOTAL_EVENTOS=0";
            }
        }

        int totalEventos = 0;
        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT COUNT(*) FROM eventos WHERE timestamp_evento BETWEEN ? AND ?"
             )) {
            stmt.setString(1, fechaInicio);
            stmt.setString(2, fechaFin);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    totalEventos = rs.getInt(1);
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=EVENTOS_RANGO\n")
            .append("DESDE=").append(fechaInicio).append('\n')
            .append("HASTA=").append(fechaFin).append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos)
            .toString();
    }

    private String consultarTasaAlmacenamiento(String fechaInicio, String fechaFin) throws SQLException {
        if (fechaInicio == null || fechaInicio.isBlank() || fechaFin == null || fechaFin.isBlank()) {
            try (Statement s = conexion.createStatement();
                 ResultSet rs = s.executeQuery("SELECT MIN(timestamp_evento), MAX(timestamp_evento) FROM eventos WHERE timestamp_evento IS NOT NULL")) {
                if (rs.next()) {
                    fechaInicio = rs.getString(1);
                    fechaFin = rs.getString(2);
                }
            }
            if (fechaInicio == null || fechaFin == null) {
                return "OK\nCONSULTA=TASA_ALMACENAMIENTO\nDESDE=N/A\nHASTA=N/A\nTOTAL_EVENTOS=0\nSEGUNDOS=0\nTASA_POR_SEGUNDO=0.0000";
            }
        }

        int totalEventos = 0;
        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT COUNT(*) FROM eventos WHERE timestamp_evento BETWEEN ? AND ?"
             )) {
            stmt.setString(1, fechaInicio);
            stmt.setString(2, fechaFin);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    totalEventos = rs.getInt(1);
                }
            }
        }

        long segundos = calcularDuracionSegundos(fechaInicio, fechaFin);
        double tasa = segundos > 0 ? (double) totalEventos / (double) segundos : 0.0;

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=TASA_ALMACENAMIENTO\n")
            .append("DESDE=").append(fechaInicio).append('\n')
            .append("HASTA=").append(fechaFin).append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos).append('\n')
            .append("SEGUNDOS=").append(segundos).append('\n')
            .append("TASA_POR_SEGUNDO=").append(String.format("%.4f", tasa))
            .toString();
    }

    private String consultarLatenciaPromedio(String fechaInicio, String fechaFin) throws SQLException {
        long total = 0L;
        long suma = 0L;
        String sql;
        boolean useRange = fechaInicio != null && !fechaInicio.isBlank() && fechaFin != null && !fechaFin.isBlank();
        if (useRange) {
            sql = "SELECT json_raw FROM eventos WHERE timestamp_evento BETWEEN ? AND ? AND tipo_evento IN ('CAMBIO_ESTADO', 'PRIORIZACION')";
        } else {
            sql = "SELECT json_raw FROM eventos WHERE tipo_evento IN ('CAMBIO_ESTADO', 'PRIORIZACION')";
        }

        try (PreparedStatement stmt = conexion.prepareStatement(sql)) {
            if (useRange) {
                stmt.setString(1, fechaInicio);
                stmt.setString(2, fechaFin);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Integer latencia = extraerCampoEntero(rs.getString("json_raw"), "latencia_ms");
                    if (latencia != null) {
                        suma += latencia;
                        total++;
                    }
                }
            }
        }

        double promedio = total > 0 ? (double) suma / (double) total : 0.0;
        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=LATENCIA_PROMEDIO\n")
            .append("DESDE=").append(useRange ? fechaInicio : "N/A").append('\n')
            .append("HASTA=").append(useRange ? fechaFin : "N/A").append('\n')
            .append("MUESTRAS=").append(total).append('\n')
            .append("LATENCIA_PROMEDIO_MS=").append(String.format("%.2f", promedio))
            .toString();
    }

    private String consultarLatenciaEstadisticas(String fechaInicio, String fechaFin) throws SQLException {
        long total = 0L;
        long suma = 0L;
        Long minimo = null;
        Long maximo = null;
        String sql;
        boolean useRange = fechaInicio != null && !fechaInicio.isBlank() && fechaFin != null && !fechaFin.isBlank();
        if (useRange) {
            sql = "SELECT json_raw FROM eventos WHERE timestamp_evento BETWEEN ? AND ? AND tipo_evento IN ('CAMBIO_ESTADO', 'PRIORIZACION')";
        } else {
            sql = "SELECT json_raw FROM eventos WHERE tipo_evento IN ('CAMBIO_ESTADO', 'PRIORIZACION')";
        }

        try (PreparedStatement stmt = conexion.prepareStatement(sql)) {
            if (useRange) {
                stmt.setString(1, fechaInicio);
                stmt.setString(2, fechaFin);
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Integer latencia = extraerCampoEntero(rs.getString("json_raw"), "latencia_ms");
                    if (latencia != null) {
                        long valor = latencia.longValue();
                        suma += valor;
                        total++;
                        if (minimo == null || valor < minimo) {
                            minimo = valor;
                        }
                        if (maximo == null || valor > maximo) {
                            maximo = valor;
                        }
                    }
                }
            }
        }

        double promedio = total > 0 ? (double) suma / (double) total : 0.0;
        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=LATENCIA_ESTADISTICAS\n")
            .append("DESDE=").append(useRange ? fechaInicio : "N/A").append('\n')
            .append("HASTA=").append(useRange ? fechaFin : "N/A").append('\n')
            .append("MUESTRAS=").append(total).append('\n')
            .append("LATENCIA_MIN_MS=").append(minimo != null ? minimo : 0).append('\n')
            .append("LATENCIA_MAX_MS=").append(maximo != null ? maximo : 0).append('\n')
            .append("LATENCIA_PROMEDIO_MS=").append(String.format("%.2f", promedio))
            .toString();
    }

    private long calcularDuracionSegundos(String fechaInicio, String fechaFin) {
        try {
            java.time.LocalDateTime inicio = java.time.LocalDateTime.parse(fechaInicio, java.time.format.DateTimeFormatter.ISO_DATE_TIME);
            java.time.LocalDateTime fin = java.time.LocalDateTime.parse(fechaFin, java.time.format.DateTimeFormatter.ISO_DATE_TIME);
            long segundos = java.time.Duration.between(inicio, fin).getSeconds();
            return Math.max(0L, segundos);
        } catch (Exception e) {
            return 0L;
        }
    }

    private String consultarUltimosEventos(int limite) throws SQLException {
        StringBuilder eventos = new StringBuilder();

        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT id, tipo_evento, tipo_sensor, interseccion, timestamp_evento FROM eventos ORDER BY id DESC LIMIT ?"
             )) {
            stmt.setInt(1, limite);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    if (eventos.length() > 0) {
                        eventos.append('\n');
                    }
                    eventos.append("- #").append(rs.getInt("id"))
                        .append(" | ").append(valorTexto(rs.getString("tipo_sensor")))
                        .append(" | ").append(valorTexto(rs.getString("tipo_evento")))
                        .append(" | ").append(valorTexto(rs.getString("interseccion")))
                        .append(" | ts=").append(valorTexto(rs.getString("timestamp_evento")));
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=ULTIMOS\n")
            .append("LIMITE=").append(limite).append('\n')
            .append("EVENTOS=\n")
            .append(eventos.length() == 0 ? "- sin eventos" : eventos)
            .toString();
    }

    private synchronized String exportarBackupCompleto() throws SQLException {
        StringBuilder snapshot = new StringBuilder();
        int total = 0;

        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT json_raw FROM eventos ORDER BY id ASC"
             );
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                if (snapshot.length() > 0) {
                    snapshot.append('\n');
                }
                snapshot.append(rs.getString(1));
                total++;
            }
        }

        return new StringBuilder()
            .append("OK|BACKUP_EXPORT|count=").append(total).append('\n')
            .append(snapshot)
            .toString();
    }

    private String valorTexto(String valor) {
        return valor == null || valor.isBlank() ? "N/A" : valor;
    }

    private String valorNumerico(Object valor) {
        return valor == null ? "N/A" : String.valueOf(valor);
    }

    private String valorDecimal(Object valor) {
        if (valor == null) {
            return "N/A";
        }
        if (valor instanceof Number) {
            return String.format("%.2f", ((Number) valor).doubleValue());
        }
        return valor.toString();
    }

    private String extraerCampoTexto(String json, String campo) {
        String patron = "\"" + campo + "\":\"";
        int inicio = json.indexOf(patron);
        if (inicio == -1) {
            return null;
        }
        inicio += patron.length();
        int fin = json.indexOf('"', inicio);
        if (fin == -1) {
            return null;
        }
        return json.substring(inicio, fin);
    }

    private Integer extraerCampoEntero(String json, String campo) {
        String valor = extraerCampoNumerico(json, campo);
        if (valor == null) {
            return null;
        }
        try {
            return Integer.parseInt(valor);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Double extraerCampoDecimal(String json, String campo) {
        String valor = extraerCampoNumerico(json, campo);
        if (valor == null) {
            return null;
        }
        try {
            return Double.parseDouble(valor);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String extraerCampoNumerico(String json, String campo) {
        String patron = "\"" + campo + "\":";
        int inicio = json.indexOf(patron);
        if (inicio == -1) {
            return null;
        }
        inicio += patron.length();
        int fin = inicio;

        while (fin < json.length()) {
            char c = json.charAt(fin);
            if (c == ',' || c == '}') {
                break;
            }
            fin++;
        }

        String valor = json.substring(inicio, fin).trim();
        return valor.isEmpty() ? null : valor;
    }

    private String buscarCampoFlexible(String json, String campo) {
        if (json == null || campo == null) return null;
        try {
            Pattern p1 = Pattern.compile("\"" + Pattern.quote(campo) + "\"\\s*:\\s*\"([^\"]*)\"");
            Matcher m1 = p1.matcher(json);
            if (m1.find()) return m1.group(1);

            Pattern p2 = Pattern.compile("\\\\\"" + Pattern.quote(campo) + "\\\\\"\\s*:\\s*\\\\\"([^\\\\\"]*)\\\\\"");
            Matcher m2 = p2.matcher(json);
            if (m2.find()) return m2.group(1).replaceAll("\\\\\\\\", "\\\\");

            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private void cerrarRecursosBD() {
        cerrarSilencioso(stmtEventosPorRango);
        cerrarSilencioso(stmtUltimosEventos);
        cerrarSilencioso(stmtVelocidadPromedioPorInterseccion);
        cerrarSilencioso(stmtEventosPorInterseccion);
        cerrarSilencioso(stmtTotalEventos);
        cerrarSilencioso(stmtInsertarEvento);
        cerrarSilencioso(conexion);
    }

    private void cerrarSilencioso(AutoCloseable recurso) {
        if (recurso == null) {
            return;
        }
        try {
            recurso.close();
        } catch (Exception e) {
            System.err.println("[BD RÉPLICA] Error cerrando recurso: " + e.getMessage());
        }
    }
    
    public int getContadorEventos() {
        return contadorEventos;
    }
    
    public static void main(String[] args) {
        Configuracion config = Configuracion.getInstance();
        BaseDatosReplica bd = new BaseDatosReplica(
            String.valueOf(config.getAnaliticaPushBdReplica()),
            String.valueOf(config.getBdReplicaRep()),
            "bd_replica.db"
        );

        bd.iniciar();
    }
}
