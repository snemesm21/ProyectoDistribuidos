# Proyecto Distribuidos

Sistema distribuido de monitoreo y control de tráfico con tres máquinas:

- PC1: sensores y broker ZMQ.
- PC2: analítica, réplica de base de datos y control de semáforos.
- PC3: base de datos principal y módulo de consulta/monitoreo.

## Integrantes

- Maria Alejandra Garcia
- Luna Rengifo Moreno
- Samuel Nemes Moreno
- Juan Camilo Moreno

## Antes de arrancar

1. Tener instalado Java y `make`.
2. Copiar el proyecto completo en las tres máquinas.
3. Verificar que en cada carpeta existan los .jar que pide el `Makefile`.
4. Revisar las IPs en `ConfiguracionSensores.json` para que coincidan con la red real de cada equipo.

Las direcciones que trae la configuración por defecto son:

- PC1: `10.43.100.83`
- PC2: `10.43.99.238`
- PC3: `10.43.99.225`

## Orden recomendado de ejecución

Lo más práctico es levantar primero PC3, luego PC2 y al final PC1. Así los servicios que reciben conexiones ya están arriba cuando los demás empiecen a publicar o consultar.

## Cómo ejecutar en cada máquina

### PC3

Entrar a la carpeta `PC3` y ejecutar:

```bash
make compile
make run-db
make run-monitoreo
```

Si se quiere abrir todo en terminales separadas y el sistema tiene un emulador compatible, también sirve:

```bash
make run-all
```

### PC2

Entrar a la carpeta `PC2` y ejecutar:

```bash
make compile
make run-replica
make run-analitica
make run-semaforos
```

También se puede usar:

```bash
make run-all
```

### PC1

Entrar a la carpeta `PC1` y ejecutar:

```bash
make compile
make run-broker
make run-sensores
```

Si se quiere la versión multihilos del broker:

```bash
make run-broker-multi
```

Y si el entorno tiene un emulador de terminal compatible:

```bash
make run-all
make run-all-multi
```

## Puertos usados por defecto

Si no cambian la configuración, el proyecto trabaja con estos puertos:

- `5555`: sensores
- `6666`: broker
- `7777`: envío de analítica a la base principal
- `8888`: envío de analítica a la réplica
- `9999`: publicación hacia semáforos
- `11000`: réplica de consulta de la base principal
- `11001`: réplica de consulta de la base réplica
- `12000`: control de respaldo para monitoreo

## Consultas de monitoreo

El módulo de PC3 acepta estas consultas:

- `ESTADO`
- `INTERSECCION <INT-X>`
- `HISTORICO <inicio> <fin>`
- `ULTIMOS <n>`

## Limpieza

Para borrar clases compiladas en cada carpeta:

```bash
make clean
```

## Nota final

Si algo no conecta, casi siempre es por una IP mal puesta en la configuración, un puerto ocupado o porque un proceso se arrancó antes de que su servicio dependiente estuviera listo.