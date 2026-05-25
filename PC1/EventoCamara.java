
public class EventoCamara extends Evento {
    private int volumen; 
    private double velocidadPromedio; 
    
    public EventoCamara(String sensorId, String interseccion, int volumen, double velocidadPromedio) {
        super(sensorId, "camara", interseccion);
        this.volumen = volumen;
        this.velocidadPromedio = Math.min(velocidadPromedio, 50.0); 
    }
    
    public int getVolumen() { return volumen; }
    public double getVelocidadPromedio() { return velocidadPromedio; }
    
    @Override
    public String toJson() {
        return String.format(
            "{\"sensor_id\":\"%s\"," +
            "\"tipo_sensor\":\"%s\"," +
            "\"interseccion\":\"%s\"," +
            "\"volumen\":%d," +
            "\"velocidad_promedio\":%.2f," +
            "\"timestamp\":\"%s\"}",
            sensorId, tipoSensor, interseccion, volumen, velocidadPromedio, timestamp
        );
    }
}
