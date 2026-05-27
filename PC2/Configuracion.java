import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
public class Configuracion {

    private static Configuracion instance;

    private String pc1;
    private String pc2;
    private String pc3;
    private int sensoresPub;
    private int brokerPub;
    private int analiticaPushBdPrincipal;
    private int analiticaPushBdReplica;
    private int analiticaPubSemaforos;
    private int analiticaRepControl;
    private int bdPrincipalRep;
    private int bdReplicaRep;
    private int filas;
    private int columnas;
    private String sharedSecret;
    private boolean hmacEnabled;

    private Configuracion() {
        cargarConfiguracion();
    }

    public static Configuracion getInstance() {
        if (instance == null) {
            instance = new Configuracion();
        }
        return instance;
    }

    private void cargarConfiguracion() {
        try {
            String contenido = leerArchivo("ConfiguracionSensores.json");
            pc1 = extraerValor(contenido, "pc1");
            pc2 = extraerValor(contenido, "pc2");
            pc3 = extraerValor(contenido, "pc3");
            sensoresPub              = Integer.parseInt(extraerValor(contenido, "sensores_pub"));
            brokerPub                = Integer.parseInt(extraerValor(contenido, "broker_pub"));
            analiticaPushBdPrincipal = Integer.parseInt(extraerValor(contenido, "analitica_push_bd_principal"));
            analiticaPushBdReplica   = Integer.parseInt(extraerValor(contenido, "analitica_push_bd_replica"));
            analiticaPubSemaforos    = Integer.parseInt(extraerValor(contenido, "analitica_pub_semaforos"));
            analiticaRepControl      = Integer.parseInt(extraerValor(contenido, "analitica_rep_control"));

            String repStr = extraerValor(contenido, "bd_principal_rep");
            bdPrincipalRep = repStr.isEmpty() ? 11000 : Integer.parseInt(repStr);

            String repReplicaStr = extraerValor(contenido, "bd_replica_rep");
            bdReplicaRep = repReplicaStr.isEmpty() ? 11001 : Integer.parseInt(repReplicaStr);
            filas    = Integer.parseInt(extraerValor(contenido, "filas"));
            columnas = Integer.parseInt(extraerValor(contenido, "columnas"));
            String secret = extraerValor(contenido, "shared_secret");
            sharedSecret = secret.isEmpty() ? "changeme" : secret;
            String hmacStr = extraerValor(contenido, "hmac_enabled");
            hmacEnabled = hmacStr.isEmpty() ? false : Boolean.parseBoolean(hmacStr);

            System.out.println("[CONFIGURACION] JSON cargado correctamente");
            System.out.println("[CONFIGURACION] Cuadricula: " + filas + "x" + columnas);
            System.out.println("[CONFIGURACION] Broker: " + pc1 + ":" + brokerPub);

        } catch (Exception e) {
            System.err.println("[CONFIGURACION] Error leyendo JSON, usando valores por defecto: " + e.getMessage());
            pc1                      = "10.43.100.83";
            pc2                      = "10.43.99.238";
            pc3                      = "10.43.99.225";
            sensoresPub              = 5555;
            brokerPub                = 6666;
            analiticaPushBdPrincipal = 7777;
            analiticaPushBdReplica   = 8888;
            analiticaPubSemaforos    = 9999;
            analiticaRepControl      = 12000;
            bdPrincipalRep           = 11000;
            bdReplicaRep             = 11001;
            filas                    = 5;
            columnas                 = 5;
            sharedSecret             = "changeme";
            hmacEnabled              = false;
        }
    }

    private String leerArchivo(String nombreArchivo) throws IOException {
        StringBuilder contenido = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(nombreArchivo))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                contenido.append(linea);
            }
        }
        return contenido.toString();
    }

    private String extraerValor(String json, String clave) {
        try {
            String buscar = "\"" + clave + "\":";
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
    public String getPC1()                      { return pc1; }
    public String getPC2()                      { return pc2; }
    public String getPC3()                      { return pc3; }
    public int getSensoresPub()                 { return sensoresPub; }
    public int getBrokerPub()                   { return brokerPub; }
    public int getAnaliticaPushBdPrincipal()    { return analiticaPushBdPrincipal; }
    public int getAnaliticaPushBdReplica()      { return analiticaPushBdReplica; }
    public int getAnaliticaPubSemaforos()       { return analiticaPubSemaforos; }
    public int getAnaliticaRepControl()         { return analiticaRepControl; }
    public int getBdPrincipalRep()              { return bdPrincipalRep; }
    public int getBdReplicaRep()                { return bdReplicaRep; }
    public int getFilas()                       { return filas; }
    public int getColumnas()                    { return columnas; }
    public String getSharedSecret()             { return sharedSecret; }
    public boolean isHmacEnabled()              { return hmacEnabled; }
}