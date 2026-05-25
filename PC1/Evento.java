import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public abstract class Evento {
    protected String sensorId;
    protected String tipoSensor;
    protected String interseccion;
    protected String timestamp;
    
    public Evento(String sensorId, String tipoSensor, String interseccion) {
        this.sensorId = sensorId;
        this.tipoSensor = tipoSensor;
        this.interseccion = interseccion;
        this.timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
    }
    
    public String getSensorId() { return sensorId; }
    public String getTipoSensor() { return tipoSensor; }
    public String getInterseccion() { return interseccion; }
    public String getTimestamp() { return timestamp; }
    
    public abstract String toJson();
}
