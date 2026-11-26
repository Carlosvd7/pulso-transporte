# Pulso del transporte

Retrasos de Renfe (Cercanías y AV/LD/MD) en tiempo real: ingesta continua, procesamiento en
streaming, calidad de datos, predicción de retrasos y un panel en vivo.

> 🚧 En construcción. Hechas: fases 0 (recolector), 1 (horarios + modelo de datos),
> 2 (bus + calidad), 3 (procesamiento en streaming + anomalías) y 4 (panel web).

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

## Procesamiento en streaming (fase 3)

`pulso process` consume los temas de Redpanda y calcula, en ventanas fijas de 5 minutos
por **hora del evento** (la cabecera del feed, no la hora de llegada):

- **KPIs por línea, núcleo, red y estación**: trenes, retraso medio, p50, p90, máximo,
  % de puntualidad (criterio propio: < 3 min en Cercanías, < 5 min en AV/LD) y cancelaciones.
- **Anomalías**: pico de retraso respecto a la historia reciente de la línea (z-score robusto
  con mediana y MAD), retraso medio alto (≥ 15 min), oleada de cancelaciones (≥ 3 en un
  núcleo en 5 min) y **tren detenido** (su retraso crece al ritmo del reloj durante 15 min).
- **Estado en vivo** (`data/live/state.json`, cada 10 s): trenes con retraso y posición,
  líneas con su evolución, anomalías, avisos de Renfe y salud de los datos.

Salidas: `data/gold/{kpi_line_5min, kpi_stop_5min, quality_5min, anomalies}` (Parquet) y los
temas `pulso.kpis.v1` y `pulso.anomalies.v1`.

Decisiones de diseño:

- **Python en vez de Spark Structured Streaming.** El volumen real es de decenas de eventos
  por segundo como mucho, y el despliegue gratuito es un solo contenedor. El núcleo
  (`stream.py`) es lógica pura sin Kafka, así que el mismo código corre con Redpanda, con un
  bus en memoria o sobre el histórico (`pulso process --offline`: 9 h de datos en ~5 s).
- **Marca de agua por partición** (el mínimo de las fuentes activas) con 90 s de margen, como
  en Flink o Spark. Al arrancar, el backlog se ordena por hora del evento antes de procesarlo.
- **Arranque sin estado guardado.** El procesador relee los últimos 30 min de Redpanda y la
  historia de KPIs de gold. Reescribir una ventana es idempotente (un fichero por ventana).
- **Cada tren cuenta una vez por ventana.** Se excluyen de las medias los trenes de días
  pasados que Renfe sigue publicando (4 818 eventos en la primera noche), los retrasos
  marcados como extremos y los trenes detenidos (que se muestran como anomalía).

## Panel en vivo (fase 4)

`pulso serve` levanta una API FastAPI y el panel (HTML + JS sin framework, Leaflet para el mapa):

- Resumen: trenes en circulación, retraso medio, % de puntuales, anomalías y cancelados.
- Mapa con cada tren coloreado por su retraso, y lista de líneas con su evolución en la
  última hora (al pulsar una línea, el mapa se queda solo con sus trenes).
- Anomalías (activas y resueltas en 24 h) y avisos oficiales de Renfe.
- Evolución del retraso medio y de la puntualidad por red (2, 6 o 24 h), con tabla accesible.
- Estaciones con más retraso en la última hora y salud de los datos por fuente.
- Modo claro/oscuro, adaptado a móvil.

API: `/api/state`, `/api/kpis`, `/api/stops`, `/api/anomalies`, `/api/quality`,
`/api/predictions`, `/api/health` (documentación automática en `/docs`).

## Despliegue (0 €)

Servidor: una VM **Oracle Cloud Always Free** (Ubuntu 24.04, ARM64, 2 OCPU y 12 GB) con la
misma pila de Docker Compose que en local, más [Caddy](https://caddyserver.com) para HTTPS
automático. El dominio es gratuito y no requiere registro: `<ip-con-guiones>.sslip.io`.

```bash
# en la VM (abre antes los puertos 80 y 443 en la "security list" de la VCN)
curl -fsSLO https://raw.githubusercontent.com/Carlosvd7/pulso-transporte/main/deploy/setup-vm.sh
bash setup-vm.sh <IP_PUBLICA>
```

Si Oracle no tiene capacidad ARM (pasa a menudo en Madrid), hay un **modo ligero** para la
VM AMD gratuita de 1 GB: `bash setup-vm.sh <IP> lite`. Ejecuta toda la tubería en un solo
proceso (`pulso lite`), con el mismo código y un bus en memoria en lugar de Redpanda. Medido con
un límite de 950 MB: pico de ~216 MB con los horarios completos cargados y 700 trenes en
circulación.

`docker-compose.prod.yml` deja Redpanda y su consola accesibles solo desde la propia máquina
(consola por túnel SSH: `ssh -L 8080:localhost:8080 ubuntu@<IP>`) y publica solo el panel.

¿Por qué no Hugging Face Spaces? Desde julio de 2026 los Spaces con Docker exigen el plan PRO,
y el resto de planes gratuitos (Render, Koyeb…) duermen el servicio cuando no hay visitas, lo
que cortaría la ingesta.

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
docker compose up -d        # Redpanda + consola (:8080) + productor + procesador + panel (:8088)
uv run pulso quality        # métricas de calidad de la última hora
uv run pulso replay         # re-publicar el histórico de bronze en Redpanda
```
