# Pulso del transporte

Retrasos de Renfe (Cercanías y AV/LD/MD) en tiempo real: ingesta continua, procesamiento en
streaming, calidad de datos, predicción de retrasos y un panel en vivo.

> 🚧 En construcción. Hechas: fases 0 (recolector), 1 (horarios + modelo de datos) y 2 (bus + calidad).

## Datos

| Fuente | Licencia |
|---|---|
| [Renfe GTFS-Realtime](https://data.renfe.com) — `gtfsrt.renfe.com` (trip updates, posiciones, avisos) | CC-BY 4.0 |
| [Renfe GTFS estático Cercanías](https://data.renfe.com/dataset/horarios-cercanias) y AV/LD | CC-BY 4.0 |

Fuente de los datos: Renfe Viajeros (data.renfe.com), licencia CC-BY 4.0.

## Modelo de datos

```
data/
├── bronze/feed=<feed>/date=<día>/part-*.parquet      protobuf crudo + hash (inmutable)
├── static/<red>/raw/<versión>.zip                    GTFS estático tal cual se descargó
├── static/<red>/v=<versión>/*.parquet                GTFS limpio (routes, stops, trips,
│                                                     stop_times, trip_summary, service_dates)
└── silver/<tabla>/network=<red>/date=<día>/*.parquet trip_updates, vehicle_positions, alerts
```

Vistas DuckDB (`pulso.warehouse`): `static_*` (para cada clave, la versión más reciente
que la contiene), `rt_*` (silver) y `trip_delays` (cada observación de retraso con su línea,
núcleo y estación).

### Problemas reales encontrados en los datos (y cómo se tratan)

- **Retrasos con ±24 h cerca de medianoche.** Renfe publica, por ejemplo, −86 340 s cuando
  el tren va 1 min tarde (calcula contra el día equivocado). Se normaliza a (−12 h, +12 h]
  y la fila queda marcada (`delay_wrapped`) para medirlo.
- **El GTFS de AV/LD se regenera cada día y borra los viajes pasados**, pero el tiempo real
  sigue publicando trenes de ayer. Por eso se guarda cada versión del estático y, si no hay
  cruce exacto, se cruza por número de tren + fecha.
- **Renfe recomprime el mismo GTFS con otra fecha**: el zip cambia byte a byte aunque el
  contenido sea idéntico. La versión se identifica por el contenido (CRC de cada fichero).
- **Cabeceras y valores rellenos de espacios** (Cercanías) y **horas sin cero inicial**
  (`8:30:00`, AV/LD): se limpian al construir las tablas.

## Bus de eventos y calidad de datos (fase 2)

```
Renfe GTFS-RT ──► productor ──► bronze (Parquet crudo)
                     │
                     ├─ contratos (Pydantic) ──► renfe.trip_updates.v1      (clave: trip_id)
                     │                       ──► renfe.vehicle_positions.v1 (clave: vehículo)
                     │                       ──► renfe.alerts.v1            (solo cambios)
                     ├─ rechazados + motivo  ──► renfe.dlq.v1
                     └─ informe por snapshot ──► pulso.quality.v1
```

Redpanda (compatible con Kafka) corre en Docker. El productor publica con idempotencia
(sin duplicados por reintentos) y `pulso replay` puede volver a publicar todo el histórico
desde bronze.

| Regla | Gravedad | Qué detecta |
|---|---|---|
| `esquema` | error | tipos o campos que no cumplen el contrato |
| `timestamp_nulo` / `timestamp_futuro` | error | cabecera sin hora o más de 2 min en el futuro |
| `duplicado` | error | mismo viaje+parada (o vehículo+viaje) dos veces en un snapshot |
| `parada_nula` / `retraso_nulo` | error | tren programado sin parada o sin retraso |
| `posicion_nula` / `fuera_de_espana` | error | coordenadas vacías o fuera de España |
| `retraso_corregido_24h` | aviso | retraso con un día de desfase (se corrige) |
| `cabecera_desfase_horario` | aviso | cabecera desfasada 1 o 2 h exactas (se corrige) |
| `feed_desactualizado` | aviso | cabecera con más de 5 min de antigüedad |
| `retraso_extremo` / `adelanto_extremo` | aviso | más de 3 h tarde o más de 15 min antes |

Métricas reales de las primeras ~9 h recogidas (`pulso quality`), 1 120 snapshots:

| Feed | Eventos | % válidos | Hallazgo |
|---|---|---|---|
| Cercanías, retrasos | 3 082 | 100 % | 1 193 retrasos con desfase de ±24 h corregidos (todos cerca de medianoche) |
| Cercanías, posiciones | 3 012 | 99,87 % | 4 trenes sin coordenadas |
| AV/LD, retrasos | 6 475 | 99,77 % | 15 viajes cancelados repetidos en el mismo mensaje |
| AV/LD, posiciones | 1 449 | 100 % | la cabecera llega siempre 1 h atrasada, aunque las posiciones son frescas |

## Arranque rápido

Requisitos: Python 3.12 y [uv](https://docs.astral.sh/uv/).

```powershell
uv sync
uv run pulso collect        # recoge los feeds cada 30 s en data/bronze (Ctrl+C para parar)
.\scripts\collector.ps1 start   # ...o en segundo plano en Windows (stop / status)
uv run pulso status         # resumen de lo recogido
uv run pulso static         # descarga y limpia el GTFS estático si cambió
uv run pulso silver         # decodifica lo pendiente de bronze a silver
uv run pulso report         # % de trenes en tiempo real que cruzan con su horario
uv run pytest
```

Con Docker (recomendado, se reinicia solo):

```powershell
docker compose up -d        # Redpanda + consola (http://localhost:8080) + productor
uv run pulso quality        # métricas de calidad de la última hora
uv run pulso replay         # re-publicar el histórico de bronze en Redpanda
```
