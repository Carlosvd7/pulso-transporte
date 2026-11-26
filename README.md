# Pulso del transporte

Retrasos de Renfe (Cercanías y AV/LD/MD) en tiempo real: ingesta continua, procesamiento en
streaming, calidad de datos, predicción de retrasos y un panel en vivo.

> 🚧 En construcción. Hechas: fase 0 (recolector) y fase 1 (horarios + modelo de datos).

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
