# Pruebas de Rendimiento — ProyectoDistribuidos

Este README explica cómo reproducir las pruebas de rendimiento para el proyecto distribuido (broker ZMQ simple vs multihilo), cómo generar carga desde 3 máquinas y cómo recoger métricas.

Resumen:
- Implementaciones instrumentadas: `PC1/BrokerZMQ.java` (single-thread) y `PC1/BrokerZMQMultihilos.java` (multithread) exponen métricas HTTP en `/metrics`.
- Generador de carga Java: `tools/SensorLoadGenerator.java` (publica mensajes ZMQ hacia el broker).
- Scripts: `tools/run_iperf.sh` / `tools/run_iperf.ps1` y `tools/collect_metrics.sh` para medir red y recolectar métricas.

Requisitos
- Java 11+ con `javac` y `java` en PATH
- librería ZeroMQ para Java (jeromq or org.zeromq). Este repo usa `org.zeromq` (asegúrate de tenerla en classpath).
- `iperf3` instalado en las 3 VMs
- `curl`, `vmstat`, `iostat` (opcional) para recolección

Compilar

Desde la raíz del proyecto (where README.md is):

```bash
javac -cp ".;path/to/jeromq.jar" PC1/*.java PC2/*.java PC3/*.java tools/*.java
```

Ejecutar brokers

- Broker single-thread (puerto por defecto viene desde `Configuracion`):

```bash
java -cp ".;path/to/jeromq.jar" PC1.BrokerZMQ
```

- Broker multi-thread (pasa número de workers opcional):

```bash
java -cp ".;path/to/jeromq.jar" PC1.BrokerZMQMultihilos 8
```

Al arrancar, cada broker expone un endpoint HTTP para métricas:
- `BrokerZMQMultihilos`: http://<broker_ip>:9000/metrics
- `BrokerZMQ` (single): http://<broker_ip>:9001/metrics

Simulación de sensores (desde 3 VMs distintas)

Compila `tools/SensorLoadGenerator.java` y ejecútalo desde cada VM apuntando al puerto de `puertoSensores` del broker (por ejemplo 5556):

```bash
java -cp ".;path/to/jeromq.jar" tools.SensorLoadGenerator <broker_ip> <broker_port> <TOPIC> <ratePerSec> <payloadBytes> <durationSec>
# ejemplo:
java -cp ".;path/to/jeromq.jar" tools.SensorLoadGenerator 10.0.0.5 5556 ESPIRA 100 200 60
```

Estrategia de pruebas (recomendado)
1. Baseline: lanzar 3 generadores pequeños (por ejemplo 10 msg/s cada uno) y medir métricas durante 1-2 minutos.
2. Ramp-up: aumentar la tasa gradualmente (p.e. 10 -> 50 -> 100 -> 500) observando `average_processing_latency_ns`, `messages_*` y tamaños de cola.
3. Stress: seguir incrementando hasta degradación (latencia P99 explode o errores). Repetir para broker single y multihilo.
4. Endurance: ejecutar una carga de producción esperada de 1-2 horas para detectar fugas de memoria.
5. Spike: lanzar ráfagas cortas y muy intensas desde 1 o más VMs.
6. Volume: enviar payloads grandes para comprobar I/O y uso de red.

Tests de red con iperf3

- En una VM (server):

```bash
# Linux
./tools/run_iperf.sh server
# Windows (PowerShell)
./tools/run_iperf.ps1 -mode server
```

- En una VM cliente:

```bash
./tools/run_iperf.sh client <server_ip> 30
# o PowerShell
./tools/run_iperf.ps1 -mode client -server <server_ip> -duration 30
```

Recolección de métricas

Ejecuta `tools/collect_metrics.sh` apuntando al endpoint `/metrics` de cada broker para grabar métricas periódicamente:

```bash
chmod +x tools/collect_metrics.sh
./tools/collect_metrics.sh http://<broker_ip>:9000/metrics 5 outdir
```

Análisis y puntos de inflexión
- Grafica latencia promedio y percentiles (p50/p90/p99) frente a la carga (req/s). La "rodilla" o punto donde la latencia crece abruptamente indica el punto de inflexión.
- Observa saturación de CPU, memoria, uso de red o crecimiento de la cola `queue_toProcess_size`.

Entregables que están en este repositorio
- Instrumentación en: `PC1/BrokerZMQ.java` y `PC1/BrokerZMQMultihilos.java` (exponen `/metrics`).
- Generador de carga: `tools/SensorLoadGenerator.java`.
- Scripts: `tools/run_iperf.sh`, `tools/run_iperf.ps1`, `tools/collect_metrics.sh`.

Siguientes pasos sugeridos (puedo hacerlo por ti):
- Añadir soporte Prometheus con `simpleclient` para métricas más ricas (histogramas p50/p90/p99).
- Crear un `benchmark-runner.sh` que ejecute ramp-ups automáticamente y capture resultados.
- Ayudarte a ejecutar las pruebas en las 3 VMs (subir scripts, arrancar iperf y generadores) si me das las IPs y acceso.

Contacto
Si quieres que implemente histogramas o suba los scripts a las 3 VMs, pásame las IPs y el método de acceso (SSH/WinRM) y lo hago.
