import java.time.LocalDateTime;

/**
 * Representa los semáforos en una intersección

 */
public class Semaforo {
    private String interseccion;
    private EstadoSemaforo estadoHorizontal;  // Tráfico en filas (Occidente-Oriente)
    private EstadoSemaforo estadoVertical;    // Tráfico en columnas (Norte-Sur)
    private LocalDateTime ultimoCambio;
    private int tiempoActual;
    
    public Semaforo(String interseccion) {
        this.interseccion = interseccion;
        // Inicialmente: horizontal en VERDE, vertical en ROJO
        this.estadoHorizontal = EstadoSemaforo.VERDE;
        this.estadoVertical = EstadoSemaforo.ROJO;
        this.ultimoCambio = LocalDateTime.now();
        this.tiempoActual = 15;
    }
    
    /**
     * Cambia el estado del eje horizontal
     * Automáticamente invierte el vertical para coordinación
     */
    public void cambiarEstadoHorizontal(EstadoSemaforo nuevoEstado, int tiempoSegundos) {
        this.estadoHorizontal = nuevoEstado;
        this.estadoVertical = (nuevoEstado == EstadoSemaforo.VERDE) ? 
            EstadoSemaforo.ROJO : EstadoSemaforo.VERDE;
        this.tiempoActual = tiempoSegundos;
        this.ultimoCambio = LocalDateTime.now();
    }
    
    /**
     * Cambia el estado del eje vertical
     * Automáticamente invierte el horizontal para coordinación
     */
    public void cambiarEstadoVertical(EstadoSemaforo nuevoEstado, int tiempoSegundos) {
        this.estadoVertical = nuevoEstado;
        this.estadoHorizontal = (nuevoEstado == EstadoSemaforo.VERDE) ? 
            EstadoSemaforo.ROJO : EstadoSemaforo.VERDE;
        this.tiempoActual = tiempoSegundos;
        this.ultimoCambio = LocalDateTime.now();
    }
    
    /**
     * Alterna entre horizontal verde y vertical verde
     */
    public void alternar(int tiempoSegundos) {
        if (estadoHorizontal == EstadoSemaforo.VERDE) {
            estadoHorizontal = EstadoSemaforo.ROJO;
            estadoVertical = EstadoSemaforo.VERDE;
        } else {
            estadoHorizontal = EstadoSemaforo.VERDE;
            estadoVertical = EstadoSemaforo.ROJO;
        }
        this.tiempoActual = tiempoSegundos;
        this.ultimoCambio = LocalDateTime.now();
    }
    
    public String getInterseccion() { return interseccion; }
    public EstadoSemaforo getEstadoHorizontal() { return estadoHorizontal; }
    public EstadoSemaforo getEstadoVertical() { return estadoVertical; }
    public int getTiempoActual() { return tiempoActual; }
    
    @Override
    public String toString() {
        return String.format("Semaforo[%s: H=%s V=%s por %ds]", 
            interseccion, estadoHorizontal, estadoVertical, tiempoActual);
    }
}
