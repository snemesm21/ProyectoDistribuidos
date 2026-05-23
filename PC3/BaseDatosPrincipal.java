import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class BaseDatosPrincipal {
    private String puertoRecepcion;
    private String puertoConsulta;
    private String archivoDB;
    private int contadorEventos;
    private boolean activa;

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
    
    public BaseDatosPrincipal(String puertoRecepcion, String puertoConsulta, String archivoDB) {
        this.puertoRecepcion = puertoRecepcion;
        this.puertoConsulta = puertoConsulta;
        this.archivoDB = archivoDB;
        this.contadorEventos = 0;
        this.activa = true;
    }

    public BaseDatosPrincipal(String puertoRecepcion, String archivoDB) {
        this(puertoRecepcion, null, archivoDB);
    }
    
    public void iniciar() {
        try {
            inicializarBaseDatos();
            prepararConsultasMonitoreo();
            probarConexionMonitoreo();
            iniciarServidorConsultas();
        } catch (Exception e) {
            System.err.println("[ERROR BD PRINCIPAL] Inicializando SQLite: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        try (ZContext context = new ZContext()) {
            ZMQ.Socket puller = context.createSocket(ZMQ.PULL);
            puller.bind("tcp://*:" + puertoRecepcion);
            
            System.out.println("╔════════════════════════════════════════════╗");
            System.out.println("║   PC3 - BASE DE DATOS PRINCIPAL           ║");
            System.out.println("╚════════════════════════════════════════════╝");
            System.out.println("[BD PRINCIPAL] Puerto: " + puertoRecepcion);
            System.out.println("[BD PRINCIPAL] SQLite: " + archivoDB);
            System.out.println("[BD PRINCIPAL] Estado: ACTIVA\n");
            
            while (activa && !Thread.currentThread().isInterrupted()) {
                String evento = puller.recvStr(0);
                
                if (evento != null) {
                    almacenarEventoEnSQLite(evento);
                }
            }
            
            puller.close();
            
        } catch (Exception e) {
            System.err.println("[ERROR BD PRINCIPAL]: " + e.getMessage());
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
                System.out.println("[BD PRINCIPAL] Consulta REP activa en puerto " + puertoConsulta);

                while (activa && !Thread.currentThread().isInterrupted()) {
                    String solicitud = rep.recvStr(0);
                    if (solicitud == null) {
                        continue;
                    }

                    String respuesta = manejarSolicitudConsulta(solicitud);
                    rep.send(respuesta.getBytes(ZMQ.CHARSET), 0);
                }

                rep.close();
            } catch (Exception e) {
                System.err.println("[ERROR BD PRINCIPAL] Servidor de consultas: " + e.getMessage());
            }
        }, "bd-principal-consultas");

        servidorConsultas.setDaemon(true);
        servidorConsultas.start();
    }
    
    private void inicializarBaseDatos() throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        conexion = DriverManager.getConnection("jdbc:sqlite:" + archivoDB);

        try (Statement stmt = conexion.createStatement()) {
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

        System.out.println("[BD PRINCIPAL] Consultas de monitoreo preparadas");
    }

    private void probarConexionMonitoreo() throws SQLException {
        try (Statement stmt = conexion.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            if (rs.next() && rs.getInt(1) == 1) {
                System.out.println("[BD PRINCIPAL] Conexión SQLite verificada para monitoreo");
            }
        }
    }

    private synchronized void almacenarEventoEnSQLite(String eventoJson) {
        contadorEventos++;
        String recibidoEn = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        String tipoEvento = extraerCampoTexto(eventoJson, "tipo_evento");
        String sensorId = extraerCampoTexto(eventoJson, "sensor_id");
        String tipoSensor = extraerCampoTexto(eventoJson, "tipo_sensor");
        String interseccion = extraerCampoTexto(eventoJson, "interseccion");
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
        } catch (SQLException e) {
            System.err.println("[ERROR BD PRINCIPAL] Insertando evento en SQLite: " + e.getMessage());
            return;
        }
        
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
        
        System.out.println("[BD PRINCIPAL] Evento #" + contadorEventos + " | " + 
            (tipoSensor != null ? tipoSensor.toUpperCase() : "???") + 
            " | INT-" + (interseccion != null ? interseccion : "?") + 
            " | " + detalles);
        
        if (contadorEventos % 10 == 0) {
            System.out.println("\n╔════════════════════════════════════════════╗");
            System.out.println("║        ESTADÍSTICAS BD PRINCIPAL           ║");
            System.out.println("╠════════════════════════════════════════════╣");
            System.out.println(String.format("║ Total eventos: %-28d║", contadorEventos));
            System.out.println(String.format("║ Estado:        %-28s║", "ACTIVA"));
            System.out.println("╚════════════════════════════════════════════╝\n");
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
                return "PONG|BD_PRINCIPAL|" + archivoDB + "|" + contadorEventos;
            }
            if ("BACKUP_EXPORT".equals(comando)) {
                return exportarBackupCompleto();
            }
            if ("BACKUP_RESTORE".equals(comando)) {
                String payload = extraerCargaBackup(solicitud);
                return restaurarBackupCompleto(payload);
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
                    if (partes.length < 3) {
                        return "ERROR|Debe indicar fecha_inicio y fecha_fin";
                    }
                    return consultarHistorico(partes[1], partes[2]);
                case "ULTIMOS":
                    int limite = 10;
                    if (partes.length >= 2) {
                        limite = Math.max(1, Integer.parseInt(partes[1]));
                    }
                    return consultarUltimosEventos(limite);
                case "AYUDA":
                    return "OK|Comandos: ESTADO, INTERSECCION <INT-X>, HISTORICO <inicio> <fin>, ULTIMOS <n>";
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
        int totalEventos = 0;
        Double velocidadPromedio = null;

        stmtEventosPorInterseccion.setString(1, interseccion);
        try (ResultSet rs = stmtEventosPorInterseccion.executeQuery()) {
            if (rs.next()) {
                totalEventos = rs.getInt(1);
            }
        }

        stmtVelocidadPromedioPorInterseccion.setString(1, interseccion);
        try (ResultSet rs = stmtVelocidadPromedioPorInterseccion.executeQuery()) {
            if (rs.next()) {
                double valor = rs.getDouble(1);
                if (!rs.wasNull()) {
                    velocidadPromedio = valor;
                }
            }
        }

        StringBuilder ultimos = new StringBuilder();
        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT id, tipo_evento, tipo_sensor, timestamp_evento, volumen, vehiculos_contados, velocidad_promedio, nivel_congestion " +
                 "FROM eventos WHERE interseccion = ? ORDER BY id DESC LIMIT 5"
             )) {
            stmt.setString(1, interseccion);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    if (ultimos.length() > 0) {
                        ultimos.append('\n');
                    }
                    ultimos.append("- #").append(rs.getInt("id"))
                        .append(" | ").append(valorTexto(rs.getString("tipo_sensor")))
                        .append(" | ").append(valorTexto(rs.getString("tipo_evento")))
                        .append(" | ts=").append(valorTexto(rs.getString("timestamp_evento")))
                        .append(" | Q=").append(valorNumerico(rs.getObject("volumen")))
                        .append(" | Cv=").append(valorNumerico(rs.getObject("vehiculos_contados")))
                        .append(" | Vp=").append(valorDecimal(rs.getObject("velocidad_promedio")))
                        .append(" | Nivel=").append(valorTexto(rs.getString("nivel_congestion")));
                }
            }
        }

        return new StringBuilder()
            .append("OK\n")
            .append("CONSULTA=INTERSECCION\n")
            .append("INTERSECCION=").append(interseccion).append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos).append('\n')
            .append("VELOCIDAD_PROMEDIO=").append(velocidadPromedio != null ? String.format("%.2f", velocidadPromedio) : "N/A").append('\n')
            .append("ULTIMOS_EVENTOS=\n")
            .append(ultimos.length() == 0 ? "- sin eventos" : ultimos)
            .toString();
    }

    private String consultarHistorico(String fechaInicio, String fechaFin) throws SQLException {
        int totalEventos = 0;
        StringBuilder eventos = new StringBuilder();

        try (PreparedStatement stmt = conexion.prepareStatement(
                 "SELECT id, tipo_evento, tipo_sensor, interseccion, timestamp_evento FROM eventos " +
                 "WHERE timestamp_evento BETWEEN ? AND ? ORDER BY timestamp_evento DESC LIMIT 20"
             )) {
            stmt.setString(1, fechaInicio);
            stmt.setString(2, fechaFin);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    totalEventos++;
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
            .append("CONSULTA=HISTORICO\n")
            .append("DESDE=").append(fechaInicio).append('\n')
            .append("HASTA=").append(fechaFin).append('\n')
            .append("TOTAL_EVENTOS=").append(totalEventos).append('\n')
            .append("EVENTOS=\n")
            .append(eventos.length() == 0 ? "- sin eventos en el rango" : eventos)
            .toString();
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

    private synchronized String restaurarBackupCompleto(String payload) throws SQLException {
        if (payload == null) {
            return "ERROR|Payload de backup vacio";
        }

        try (Statement stmt = conexion.createStatement()) {
            stmt.executeUpdate("DELETE FROM eventos");
        }

        int importados = 0;
        String[] lineas = payload.split("\\r?\\n");
        for (String linea : lineas) {
            String json = linea.trim();
            if (json.isEmpty()) {
                continue;
            }
            if (insertarEventoDesdeJson(json)) {
                importados++;
            }
        }

        contadorEventos = importados;
        return "OK|BACKUP_RESTORE|importados=" + importados;
    }

    private String extraerCargaBackup(String solicitud) {
        int salto = solicitud.indexOf('\n');
        if (salto == -1 || salto + 1 >= solicitud.length()) {
            return "";
        }
        return solicitud.substring(salto + 1);
    }

    private boolean insertarEventoDesdeJson(String eventoJson) {
        String recibidoEn = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        String tipoEvento = extraerCampoTexto(eventoJson, "tipo_evento");
        String sensorId = extraerCampoTexto(eventoJson, "sensor_id");
        String tipoSensor = extraerCampoTexto(eventoJson, "tipo_sensor");
        String interseccion = extraerCampoTexto(eventoJson, "interseccion");
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
            if (vehiculosContados != null) stmtInsertarEvento.setInt(6, vehiculosContados); else stmtInsertarEvento.setNull(6, java.sql.Types.INTEGER);
            if (intervaloSegundos != null) stmtInsertarEvento.setInt(7, intervaloSegundos); else stmtInsertarEvento.setNull(7, java.sql.Types.INTEGER);
            stmtInsertarEvento.setString(8, timestampInicio);
            stmtInsertarEvento.setString(9, timestampFin);
            if (volumen != null) stmtInsertarEvento.setInt(10, volumen); else stmtInsertarEvento.setNull(10, java.sql.Types.INTEGER);
            if (velocidadPromedio != null) stmtInsertarEvento.setDouble(11, velocidadPromedio); else stmtInsertarEvento.setNull(11, java.sql.Types.REAL);
            stmtInsertarEvento.setString(12, nivelCongestion);
            stmtInsertarEvento.setString(13, timestampEvento);
            stmtInsertarEvento.setString(14, eventoJson);
            stmtInsertarEvento.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
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
            System.err.println("[BD PRINCIPAL] Error cerrando recurso: " + e.getMessage());
        }
    }
    
    public int getContadorEventos() {
        return contadorEventos;
    }
    
    public void detener() {
        activa = false;
    }
    
    public static void main(String[] args) {
        Configuracion config = Configuracion.getInstance();
        BaseDatosPrincipal bd = new BaseDatosPrincipal(
            String.valueOf(config.getAnaliticaPushBdPrincipal()),
            String.valueOf(config.getBdPrincipalRep()),
            "bd_principal.db"
        );

        bd.iniciar();
    }
}
