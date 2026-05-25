public class ReglasTrafico {
    

    public static EstadoTrafico evaluarEstado(int cola, int vehiculosContados, double velocidadPromedio,
                                              double densidad, String nivelCongestion) {

        if (cola < 5 && vehiculosContados <= 12 && velocidadPromedio > 35 && densidad < 20) {
            return EstadoTrafico.NORMAL;
        }
        int senalesCriticas = contarSenalesCriticas(cola, vehiculosContados, velocidadPromedio, densidad, nivelCongestion);

        if (senalesCriticas >= 2) {
            return EstadoTrafico.CONGESTION;
        }

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
