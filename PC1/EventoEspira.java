import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class EventoEspira extends Evento {
    private int vehiculosContados;
    private int intervaloSegundos;
    private String timestampInicio;
    private String timestampFin;
    
    public EventoEspira(String sensorId, String interseccion, int vehiculosContados, int intervaloSegundos) {
        super(sensorId, "espira_inductiva", interseccion);
        this.vehiculosContados = vehiculosContados;
        this.intervaloSegundos = intervaloSegundos;
        LocalDateTime now = LocalDateTime.now();
        this.timestampFin = now.format(DateTimeFormatter.ISO_DATE_TIME);
        this.timestampInicio = now.minusSeconds(intervaloSegundos).format(DateTimeFormatter.ISO_DATE_TIME);
    }
    
    public int getVehiculosContados() { return vehiculosContados; }
    public int getIntervaloSegundos() { return intervaloSegundos; }
    
    @Override
    public String toJson() {
        return String.format(
            "{\"sensor_id\":\"%s\"," +
            "\"tipo_sensor\":\"%s\"," +
            "\"interseccion\":\"%s\"," +
            "\"vehiculos_contados\":%d," +
            "\"intervalo_segundos\":%d," +
            "\"timestamp_inicio\":\"%s\"," +
            "\"timestamp_fin\":\"%s\"}",
            sensorId, tipoSensor, interseccion, vehiculosContados, 
            intervaloSegundos, timestampInicio, timestampFin
        );
    }
}
