
public class InterseccionSemaforos {
    private final String interseccion;
    private final SemaforoVia horizontal;
    private final SemaforoVia vertical;

    public InterseccionSemaforos(String interseccion) {
        this.interseccion = interseccion;
        this.horizontal = new SemaforoVia(interseccion, SemaforoVia.Orientacion.HORIZONTAL, EstadoSemaforo.VERDE, 15);
        this.vertical   = new SemaforoVia(interseccion, SemaforoVia.Orientacion.VERTICAL,   EstadoSemaforo.ROJO,  15);
    }

    public String getInterseccion() { return interseccion; }
    public EstadoSemaforo getEstadoHorizontal() { return horizontal.getEstado(); }
    public EstadoSemaforo getEstadoVertical()   { return vertical.getEstado(); }
    public int getTiempoActual() { return Math.max(horizontal.getTiempoActual(), vertical.getTiempoActual()); }

    public synchronized void cambiarEstadoHorizontal(EstadoSemaforo nuevoEstado, int tiempoSegundos) {
        horizontal.setEstado(nuevoEstado, tiempoSegundos);
        // Coordina la otra vía
        vertical.setEstado((nuevoEstado == EstadoSemaforo.VERDE) ? EstadoSemaforo.ROJO : EstadoSemaforo.VERDE, tiempoSegundos);
    }

    public synchronized void cambiarEstadoVertical(EstadoSemaforo nuevoEstado, int tiempoSegundos) {
        vertical.setEstado(nuevoEstado, tiempoSegundos);
        horizontal.setEstado((nuevoEstado == EstadoSemaforo.VERDE) ? EstadoSemaforo.ROJO : EstadoSemaforo.VERDE, tiempoSegundos);
    }

    public synchronized void alternar(int tiempoSegundos) {
        if (horizontal.getEstado() == EstadoSemaforo.VERDE) {
            horizontal.setEstado(EstadoSemaforo.ROJO, tiempoSegundos);
            vertical.setEstado(EstadoSemaforo.VERDE, tiempoSegundos);
        } else {
            horizontal.setEstado(EstadoSemaforo.VERDE, tiempoSegundos);
            vertical.setEstado(EstadoSemaforo.ROJO, tiempoSegundos);
        }
    }

    @Override
    public String toString() {
        return String.format("Interseccion[%s H=%s V=%s]", interseccion, getEstadoHorizontal(), getEstadoVertical());
    }
}
