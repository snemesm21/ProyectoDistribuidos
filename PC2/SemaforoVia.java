import java.time.LocalDateTime;

/**
 * Representa un semáforo unidireccional (una vía) dentro de una intersección.
 */
public class SemaforoVia {
    public enum Orientacion { HORIZONTAL, VERTICAL }

    private final String interseccion;
    private final Orientacion orientacion;
    private EstadoSemaforo estado;
    private LocalDateTime ultimoCambio;
    private int tiempoActual;

    public SemaforoVia(String interseccion, Orientacion orientacion, EstadoSemaforo estadoInicial, int tiempoInicial) {
        this.interseccion = interseccion;
        this.orientacion = orientacion;
        this.estado = estadoInicial;
        this.ultimoCambio = LocalDateTime.now();
        this.tiempoActual = tiempoInicial;
    }

    public String getInterseccion() { return interseccion; }
    public Orientacion getOrientacion() { return orientacion; }
    public EstadoSemaforo getEstado() { return estado; }
    public int getTiempoActual() { return tiempoActual; }

    public void setEstado(EstadoSemaforo nuevoEstado, int tiempoSegundos) {
        this.estado = nuevoEstado;
        this.tiempoActual = tiempoSegundos;
        this.ultimoCambio = LocalDateTime.now();
    }

    public void alternar(int tiempoSegundos) {
        this.estado = (this.estado == EstadoSemaforo.VERDE) ? EstadoSemaforo.ROJO : EstadoSemaforo.VERDE;
        this.tiempoActual = tiempoSegundos;
        this.ultimoCambio = LocalDateTime.now();
    }

    @Override
    public String toString() {
        return String.format("SemaforoVia[%s-%s=%s for %ds]", interseccion, orientacion, estado, tiempoActual);
    }
}
