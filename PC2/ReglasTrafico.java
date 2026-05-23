public class ReglasTrafico {
    

    public static EstadoTrafico evaluarEstado(int cola, int vehiculosContados, double velocidadPromedio,
                                              double densidad, String nivelCongestion) {
        // Estado 1: TRÁFICO NORMAL
        // Condición por sensores: Q < 5 (cámara), Cv <= 12 (espira), D < 20 y Vp > 35 (GPS)
        if (cola < 5 && vehiculosContados <= 12 && velocidadPromedio > 35 && densidad < 20) {
            return EstadoTrafico.NORMAL;
        }

        // Estado 2: CONGESTIÓN
        // Correlación de eventos: se requiere que al menos 2 de 3 sensores
        // (cámara, espira y GPS) reporten una condición crítica para declarar congestión.
        // La lógica interna de cada sensor puede usar OR, pero la decisión final no.
        int senalesCriticas = contarSenalesCriticas(cola, vehiculosContados, velocidadPromedio, densidad, nivelCongestion);

        if (senalesCriticas >= 2) {
            return EstadoTrafico.CONGESTION;
        }

        // Si no alcanza correlación, consideramos tráfico aún NORMAL en este umbral intermedio
        return EstadoTrafico.NORMAL;
    }

    private static int contarSenalesCriticas(int cola, int vehiculosContados, double velocidadPromedio,
                                             double densidad, String nivelCongestion) {
        int senales = 0;

        if (cola >= 10) {
            senales++;
        }

        if (vehiculosContados >= 15) {
            senales++;
        }

        boolean gpsCritico = velocidadPromedio < 20 || densidad >= 40 || "ALTA".equalsIgnoreCase(nivelCongestion);
        if (gpsCritico) {
            senales++;
        }

        return senales;
    }
    

    public static int obtenerTiempoSemaforo(EstadoTrafico estado) {
        switch (estado) {
            case NORMAL:
                return 15; 
            case CONGESTION:
                return 30; 
            case PRIORIZACION:
                return 60; 
            default:
                return 15;
        }
    }
}
