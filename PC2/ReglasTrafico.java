public class ReglasTrafico {
    

    public static EstadoTrafico evaluarEstado(int cola, int vehiculosContados, double velocidadPromedio,
                                              double densidad, String nivelCongestion) {
        // Estado 1: TRÁFICO NORMAL
        // Condición por sensores: Q < 5 (cámara), Cv <= 12 (espira), D < 20 y Vp > 35 (GPS)
        if (cola < 5 && vehiculosContados <= 12 && velocidadPromedio > 35 && densidad < 20) {
            return EstadoTrafico.NORMAL;
        }

        // Estado 2: CONGESTIÓN
        // Implementación con correlación: cada tipo de sensor emite su señal de alarma
        // y sólo si al menos DOS tipos de sensores reportan condición crítica se considera
        // CONGESTIÓN. Evitamos usar OR entre sensores, en vez contamos las senales positivas.
        boolean camaraAlarma = cola >= 10; // indicador por cámara
        boolean espiraAlarma = vehiculosContados >= 15; // indicador por espira
        boolean gpsAlarma = velocidadPromedio < 20 || densidad >= 40 || "ALTA".equalsIgnoreCase(nivelCongestion);

        int senales = 0;
        if (camaraAlarma) senales++;
        if (espiraAlarma) senales++;
        if (gpsAlarma) senales++;

        if (senales >= 2) {
            return EstadoTrafico.CONGESTION;
        }

        // Si no alcanza correlación, consideramos tráfico aún NORMAL en este umbral intermedio
        return EstadoTrafico.NORMAL;
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
