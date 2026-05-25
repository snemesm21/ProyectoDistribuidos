import org.zeromq.ZMQ;
import org.zeromq.ZContext;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServicioControlSemaforos {

    private String analiticaAddress;

    private Map<String, InterseccionSemaforos> semaforos;

    public ServicioControlSemaforos(String analiticaAddress) {
        this.analiticaAddress = analiticaAddress;
        this.semaforos = new HashMap<>();
        inicializarSemaforosDesdeJson();
    }



    private String leerArchivo(String nombreArchivo) throws IOException {
        StringBuilder contenido = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(nombreArchivo))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                contenido.append(linea).append('\n');
            }
        }
        return contenido.toString();
    }

    private int extraerEntero(String json, String campo, int defecto) {
        Pattern p = Pattern.compile("\"" + campo + "\"\\s*:\\s*(\\d+)");
        Matcher m = p.matcher(json);
        return m.find() ? Integer.parseInt(m.group(1)) : defecto;
    }

    private List<String> extraerArrayStrings(String json, String campo) {
        List<String> lista = new ArrayList<>();
        Pattern p = Pattern.compile("\"" + campo + "\"\\s*:\\s*\\[([^\\]]+)\\]");
        Matcher m = p.matcher(json);
        if (m.find()) {
            Matcher valores = Pattern.compile("\"([^\"]+)\"|([\\d]+)").matcher(m.group(1));
            while (valores.find()) {
                lista.add(valores.group(1) != null ? valores.group(1) : valores.group(2));
            }
        }
        return lista;
    }


    private Set<String> cargarInterseccionesDesdeJson() {
        Set<String> intersecciones = new LinkedHashSet<>();

        try {
            String contenido = leerArchivo("ConfiguracionSensores.json");

            int filas    = extraerEntero(contenido, "filas",    5);
            int columnas = extraerEntero(contenido, "columnas", 5);

            List<String> etiquetasFilas = extraerArrayStrings(contenido, "etiquetas_filas");
            if (etiquetasFilas.isEmpty()) {
                for (int i = 0; i < filas; i++) {
                    etiquetasFilas.add(String.valueOf((char) ('A' + i)));
                }
            }

            List<String> etiquetasColumnas = extraerArrayStrings(contenido, "etiquetas_columnas");
            if (etiquetasColumnas.isEmpty()) {
                for (int i = 1; i <= columnas; i++) {
                    etiquetasColumnas.add(String.valueOf(i));
                }
            }

            for (String fila : etiquetasFilas) {
                for (String col : etiquetasColumnas) {
                    intersecciones.add("INT-" + fila + col);
                }
            }

            System.out.println("[SEMAFOROS] Cuadricula: " + filas + "x" + columnas
                + " = " + intersecciones.size() + " intersecciones");

        } catch (Exception e) {
            System.err.println("[SEMAFOROS] Error leyendo JSON: " + e.getMessage());
        }

        return intersecciones;
    }

    private void inicializarSemaforosDesdeJson() {
        Set<String> intersecciones = cargarInterseccionesDesdeJson();

        if (intersecciones.isEmpty()) {
            semaforos.put("INT-A1", new InterseccionSemaforos("INT-A1"));
            semaforos.put("INT-B2", new InterseccionSemaforos("INT-B2"));
            semaforos.put("INT-C3", new InterseccionSemaforos("INT-C3"));
            System.out.println("[SEMAFOROS] JSON no disponible, usando fallback (3 intersecciones)");
            return;
        }

        for (String interseccion : intersecciones) {
            semaforos.put(interseccion, new InterseccionSemaforos(interseccion));
        }

        System.out.println("[SEMAFOROS] " + semaforos.size() + " intersecciones inicializadas — cada una con 2 semaforos (H y V)");
    }


    public void iniciar() {
        try (ZContext context = new ZContext()) {
            ZMQ.Socket subscriber = context.createSocket(ZMQ.SUB);
            subscriber.connect(analiticaAddress);
            subscriber.subscribe("COMANDO".getBytes(ZMQ.CHARSET));

            System.out.println("╔════════════════════════════════════════════╗");
            System.out.println("║     PC2 - CONTROL DE SEMAFOROS            ║");
            System.out.println("╠════════════════════════════════════════════╣");
            System.out.println("║ Analitica: " + String.format("%-31s", analiticaAddress) + "║");
            System.out.println("║ Semaforos: " + String.format("%-31s", semaforos.size() + " intersecciones") + "║");
            System.out.println("║ BD Estado: " + String.format("%-31s", "HashMap en memoria") + "║");
            System.out.println("╚════════════════════════════════════════════╝\n");

            Thread.sleep(1000);

            while (!Thread.currentThread().isInterrupted()) {
                String mensaje = subscriber.recvStr(0);
                if (mensaje != null) {
                    procesarComando(mensaje);
                }
            }

            subscriber.close();

        } catch (Exception e) {
            System.err.println("[ERROR SEMAFOROS]: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private void procesarComando(String mensaje) {
        try {
            String[] partes = mensaje.split(" ", 2);
            if (partes.length < 2) return;

            String jsonComando = partes[1];

            String interseccion = extraerCampo(jsonComando, "interseccion");
            String estadoStr    = extraerCampo(jsonComando, "estado");
            int    tiempo       = Integer.parseInt(extraerCampo(jsonComando, "tiempo"));
            String razon        = extraerCampo(jsonComando, "razon");
            String orientacion  = extraerCampo(jsonComando, "orientacion");

            InterseccionSemaforos semaforo = semaforos.get(interseccion);
            if (semaforo == null) {
                semaforo = new InterseccionSemaforos(interseccion);
                semaforos.put(interseccion, semaforo);
                System.out.println("[SEMAFOROS] Intersección creada dinámicamente: " + interseccion);
            }

            EstadoSemaforo nuevoEstado = EstadoSemaforo.valueOf(estadoStr);

            if (orientacion != null && !orientacion.isEmpty()) {
                if ("HORIZONTAL".equalsIgnoreCase(orientacion)) {
                    if (nuevoEstado == EstadoSemaforo.VERDE) {
                        semaforo.cambiarEstadoHorizontal(EstadoSemaforo.VERDE, tiempo);
                    } else {
                        semaforo.cambiarEstadoHorizontal(nuevoEstado, tiempo);
                    }
                } else if ("VERTICAL".equalsIgnoreCase(orientacion)) {
                    if (nuevoEstado == EstadoSemaforo.VERDE) {
                        semaforo.cambiarEstadoVertical(EstadoSemaforo.VERDE, tiempo);
                    } else {
                        semaforo.cambiarEstadoVertical(nuevoEstado, tiempo);
                    }
                } else {

                    if (nuevoEstado == EstadoSemaforo.VERDE) {
                        if (semaforo.getEstadoHorizontal() == EstadoSemaforo.VERDE) {
                            semaforo.cambiarEstadoVertical(EstadoSemaforo.VERDE, tiempo);
                        } else {
                            semaforo.cambiarEstadoHorizontal(EstadoSemaforo.VERDE, tiempo);
                        }
                    } else {
                        semaforo.alternar(tiempo);
                    }
                }
            } else {
                if (nuevoEstado == EstadoSemaforo.VERDE) {
                    if (semaforo.getEstadoHorizontal() == EstadoSemaforo.VERDE) {
                        semaforo.cambiarEstadoVertical(EstadoSemaforo.VERDE, tiempo);
                    } else {
                        semaforo.cambiarEstadoHorizontal(EstadoSemaforo.VERDE, tiempo);
                    }
                } else {
                    semaforo.alternar(tiempo);
                }
            }

            System.out.println("╔═══════════════════════════════════════════╗");
            System.out.println("║    CAMBIO DE SEMAFORO EJECUTADO           ║");
            System.out.println("╠═══════════════════════════════════════════╣");
            System.out.println("║ Interseccion: " + String.format("%-28s", interseccion)              + "║");
            System.out.println("║ Estado H:     " + String.format("%-28s", semaforo.getEstadoHorizontal()) + "║");
            System.out.println("║ Estado V:     " + String.format("%-28s", semaforo.getEstadoVertical())   + "║");
            System.out.println("║ Duracion:     " + String.format("%-28s", tiempo + " segundos")       + "║");
            System.out.println("║ Razon:        " + String.format("%-28s", razon)                     + "║");
            System.out.println("╚═══════════════════════════════════════════╝\n");

            final InterseccionSemaforos semaforoFinal     = semaforo;
            final String   interseccionFinal = interseccion;
            new Thread(() -> {
                try {
                    Thread.sleep(tiempo * 1000L);
                    semaforoFinal.alternar(15);
                    System.out.println("[SEMAFOROS] " + interseccionFinal
                        + " alterno automaticamente: H=" + semaforoFinal.getEstadoHorizontal()
                        + " V=" + semaforoFinal.getEstadoVertical() + "\n");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();

        } catch (Exception e) {
            System.err.println("[ERROR SEMAFOROS] Procesando comando: " + e.getMessage());
        }
    }


    private String extraerCampo(String json, String campo) {
        try {
            String buscar = "\"" + campo + "\":";
            int inicio = json.indexOf(buscar);
            if (inicio == -1) return "";

            inicio += buscar.length();
            while (inicio < json.length() &&
                   (json.charAt(inicio) == ' ' || json.charAt(inicio) == '"')) {
                inicio++;
            }

            int fin = inicio;
            boolean enComillas = json.charAt(inicio - 1) == '"';

            if (enComillas) {
                fin = json.indexOf('"', inicio);
            } else {
                while (fin < json.length() &&
                       json.charAt(fin) != ',' && json.charAt(fin) != '}') {
                    fin++;
                }
            }

            return json.substring(inicio, fin).trim();
        } catch (Exception e) {
            return "";
        }
    }

    public Map<String, InterseccionSemaforos> getSemaforos() {
        return semaforos;
    }


    public static void main(String[] args) {
        Configuracion config = Configuracion.getInstance();
        String analiticaAddress = "tcp://" + config.getPC2() + ":" + config.getAnaliticaPubSemaforos();

        ServicioControlSemaforos servicio = new ServicioControlSemaforos(analiticaAddress);
        servicio.iniciar();
    }
}