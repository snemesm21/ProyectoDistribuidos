import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Aplicación principal PC1
 * Genera automáticamente 3 sensores por cada intersección
 */
public class PC1_SensoresMain {
    
    // Patrón para extraer intervalo de JSON
    private static final Pattern PATRON_INTERVALO = Pattern.compile("\"intervalo_segundos\"\\s*:\\s*(\\d+)");


    private static String leerArchivo(String nombreArchivo) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(nombreArchivo))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                sb.append(linea).append('\n');
            }
        }
        return sb.toString();
    }

    private static int extraerEntero(String json, String campo, int defecto) {
        Pattern p = Pattern.compile("\"" + campo + "\"\\s*:\\s*(\\d+)");
        Matcher m = p.matcher(json);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return defecto;
    }


    private static List<String> extraerArrayStrings(String json, String campo) {
        List<String> lista = new ArrayList<>();
        Pattern p = Pattern.compile("\"" + campo + "\"\\s*:\\s*\\[([^\\]]+)\\]");
        Matcher m = p.matcher(json);
        if (m.find()) {
            String contenido = m.group(1);
            Matcher valores = Pattern.compile("\"([^\"]+)\"|([\\d]+)").matcher(contenido);
            while (valores.find()) {
                if (valores.group(1) != null) {
                    lista.add(valores.group(1));
                } else {
                    lista.add(valores.group(2));
                }
            }
        }
        return lista;
    }


    private static String obtenerPrefijo(String tipo) {
        switch (tipo) {
            case "espira_inductiva": return "ESP";
            case "camara":           return "CAM";
            case "gps":              return "GPS";
            default:                 return "SEN";
        }
    }

    private static void agregarSensor(List<Thread> hilos, String tipo,
                                       String sensorId, String interseccion,
                                       int intervalo, String brokerAddress) {
        switch (tipo) {
            case "espira_inductiva":
                hilos.add(new Thread(new SensorEspiraInductiva(sensorId, interseccion, intervalo, brokerAddress)));
                break;
            case "camara":
                hilos.add(new Thread(new SensorCamara(sensorId, interseccion, intervalo, brokerAddress)));
                break;
            case "gps":
                hilos.add(new Thread(new SensorGPS(sensorId, interseccion, intervalo, brokerAddress)));
                break;
            default:
                System.out.println("[PC1] Tipo de sensor desconocido: " + tipo);
        }
    }


    private static void agregarSensoresPorDefecto(List<Thread> hilos, String brokerAddress) {
        String[] intersecciones = {"INT-A1", "INT-B2", "INT-C3"};
        String[] tipos = {"espira_inductiva", "camara", "gps"};
        for (String inter : intersecciones) {
            for (String tipo : tipos) {
                String id = obtenerPrefijo(tipo) + "-" + inter + "-1";
                agregarSensor(hilos, tipo, id, inter, 10, brokerAddress);
            }
        }
    }



    public static void main(String[] args) {
        System.out.println("╔════════════════════════════════════════════╗");
        System.out.println("║    PC1 - SENSORES DE TRÁFICO URBANO       ║");
        System.out.println("╚════════════════════════════════════════════╝\n");

        Configuracion configuracion = Configuracion.getInstance();
        String brokerAddress = "tcp://" + configuracion.getPC1() + ":" + configuracion.getSensoresPub();

        List<Thread> hilos = new ArrayList<>();

        try {
            String json = leerArchivo("ConfiguracionSensores.json");

            // Leer dimensiones
            int filas    = extraerEntero(json, "filas",    5);
            int columnas = extraerEntero(json, "columnas", 5);

            List<String> etiquetasFilas    = extraerArrayStrings(json, "etiquetas_filas");
            List<String> etiquetasColumnas = extraerArrayStrings(json, "etiquetas_columnas");

            if (etiquetasFilas.isEmpty()) {
                for (int i = 0; i < filas; i++) {
                    etiquetasFilas.add(String.valueOf((char)('A' + i)));
                }
            }
            if (etiquetasColumnas.isEmpty()) {
                for (int i = 1; i <= columnas; i++) {
                    etiquetasColumnas.add(String.valueOf(i));
                }
            }

            // Leer intervalo
            Matcher mIntervalo = PATRON_INTERVALO.matcher(json);
            int intervalo = mIntervalo.find() ? Integer.parseInt(mIntervalo.group(1)) : 10;

            // Tipos de sensor (fijos, los 3 siempre)
            String[] tipos = {"espira_inductiva", "camara", "gps"};

            System.out.println("Cuadrícula: " + filas + "×" + columnas
                + " = " + (filas * columnas) + " intersecciones");
            System.out.println("Sensores por intersección: " + tipos.length);
            System.out.println("Total sensores: " + (filas * columnas * tipos.length));
            System.out.println("Intervalo: " + intervalo + "s");
            System.out.println("Broker: " + brokerAddress + "\n");

            // Generar un hilo por cada sensor de cada intersección
            for (String etFila : etiquetasFilas) {
                for (String etCol : etiquetasColumnas) {
                    String interseccion = "INT-" + etFila + etCol;
                    for (String tipo : tipos) {
                        String sensorId = obtenerPrefijo(tipo) + "-" + interseccion + "-1";
                        agregarSensor(hilos, tipo, sensorId, interseccion, intervalo, brokerAddress);
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("[PC1] Error leyendo JSON, usando configuración por defecto: " + e.getMessage());
            agregarSensoresPorDefecto(hilos, brokerAddress);
        }

        System.out.println("Iniciando " + hilos.size() + " sensores...\n");

        for (Thread hilo : hilos) {
            hilo.start();
        }

        System.out.println("Todos los sensores activos. Presione Ctrl+C para detener.\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
            System.out.println("\n[PC1] Deteniendo sensores...")
        ));

        for (Thread hilo : hilos) {
            try {
                hilo.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}