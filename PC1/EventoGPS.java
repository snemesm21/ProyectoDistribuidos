
public class EventoGPS extends Evento {
    private String nivelCongestion;
    private double densidad;
    private double velocidadPromedio;
    
    public EventoGPS(String sensorId, String interseccion, double velocidadPromedio, double densidad) {
        super(sensorId, "gps", interseccion);
        this.velocidadPromedio = velocidadPromedio;
        this.densidad = densidad;
        this.nivelCongestion = calcularNivelCongestion(velocidadPromedio);
    }
    
    private String calcularNivelCongestion(double velocidad) {
        if (velocidad < 10) return "ALTA";
        else if (velocidad <= 39) return "NORMAL";
        else return "BAJA";
    }
    
    public String getNivelCongestion() { return nivelCongestion; }
    public double getDensidad() { return densidad; }
    public double getVelocidadPromedio() { return velocidadPromedio; }
    
    @Override
    public String toJson() {
        return String.format(
            "{\"sensor_id\":\"%s\"," +
            "\"tipo_sensor\":\"%s\"," +
            "\"interseccion\":\"%s\"," +
            "\"nivel_congestion\":\"%s\"," +
            "\"densidad\":%.2f," +
            "\"velocidad_promedio\":%.2f," +
            "\"timestamp\":\"%s\"}",
            sensorId, tipoSensor, interseccion, nivelCongestion, densidad, velocidadPromedio, timestamp
        );
    }
}
