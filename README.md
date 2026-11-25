# Pulso del transporte

Retrasos de Renfe (Cercanías y AV/LD/MD) en tiempo real: ingesta continua, procesamiento en
streaming, calidad de datos, predicción de retrasos y un panel en vivo.

> 🚧 En construcción. Fase 0: recolector de datos crudos funcionando.

## Datos

| Fuente | Licencia |
|---|---|
| [Renfe GTFS-Realtime](https://data.renfe.com) — `gtfsrt.renfe.com` (trip updates, posiciones, avisos) | CC-BY 4.0 |
| [Renfe GTFS estático Cercanías](https://data.renfe.com/dataset/horarios-cercanias) y AV/LD | CC-BY 4.0 |

Fuente de los datos: Renfe Viajeros (data.renfe.com), licencia CC-BY 4.0.

## Arranque rápido (fase 0)

Requisitos: Python 3.12 y [uv](https://docs.astral.sh/uv/).

```powershell
uv sync
uv run pulso collect        # recoge los feeds cada 30 s en data/bronze (Ctrl+C para parar)
uv run pulso status         # resumen de lo recogido
uv run pytest
```
