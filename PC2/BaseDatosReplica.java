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

public class BaseDatosReplica {
    private String puertoRecepcion;
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
    
    public BaseDatosReplica(String puertoRecepcion, String archivoDB) {
        this.puertoRecepcion = puertoRecepcion;
        this.archivoDB = archivoDB;
        this.contadorEventos = 0;
    }
    
    public void iniciar() {
        try {
            inicializarBaseDatos();
            prepararConsultasMonitoreo();
            probarConexionMonitoreo();
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

    private void inicializarBaseDatos() throws SQLException, ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        conexion = DriverManager.getConnection("jdbc:sqlite:" + archivoDB);

        try (Statement stmt = conexion.createStatement()) {
            stmt.execute(SQL_CREAR_TABLA_EVENTOS);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_eventos_interseccion ON eventos(interseccion)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_eventos_tipo ON eventos(tipo_evento)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_eventos_timestamp ON eventos(timestamp_evento)");
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

    private void almacenarEventoEnSQLite(String eventoJson) {
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

            // Imprimir detalles del registro guardado según tipo de sensor
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
            
            System.out.println("[BD RÉPLICA] Evento #" + contadorEventos + " | " + 
                (tipoSensor != null ? tipoSensor.toUpperCase() : "???") + 
                " | INT-" + (interseccion != null ? interseccion : "?") + 
                " | " + detalles);
        } catch (SQLException e) {
            System.err.println("[ERROR BD RÉPLICA] Insertando evento en SQLite: " + e.getMessage());
        }

        if (contadorEventos % 10 == 0) {
            System.out.println("[BD RÉPLICA] " + contadorEventos + " eventos persistidos\n");
        }
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
            "bd_replica.db"
        );

        bd.iniciar();
    }
}
