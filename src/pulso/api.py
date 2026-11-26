"""API del panel: sirve el estado en vivo y el histórico de gold, y la web estática.

Solo lee ficheros (live/state.json y gold/*.parquet), así que funciona igual con la pila
completa (Redpanda + procesador) que en el despliegue ligero de un solo contenedor.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from pulso.config import Settings, load_settings

WEB_DIR = Path(__file__).parent / "web"
STALE_AFTER_S = 90
CACHE_S = 10


class _Cache:
    def __init__(self) -> None:
        self._items: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, fn: Callable[[], Any]) -> Any:
        hit = self._items.get(key)
        if hit and time.monotonic() - hit[0] < CACHE_S:
            return hit[1]
        value = fn()
        self._items[key] = (time.monotonic(), value)
        return value


def _iso(v: Any) -> Any:
    return v.isoformat() if isinstance(v, datetime) else v


class GoldReader:
    def __init__(self, data_dir: Path) -> None:
        self.root = data_dir / "gold"

    def _query(self, table: str, sql: str, params: list[Any]) -> list[dict]:
        if not any((self.root / table).glob("date=*/*.parquet")):
            return []
        pattern = (self.root / table / "date=*" / "*.parquet").as_posix()
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        cur = con.execute(
            sql.replace(
                "{src}",
                f"read_parquet('{pattern}', hive_partitioning = true, union_by_name = true)",
            ),
            params,
        )
        cols = [c[0] for c in cur.description]
        rows = [{k: _iso(v) for k, v in zip(cols, r, strict=True)} for r in cur.fetchall()]
        con.close()
        return rows

    def kpis(self, since: datetime, network: str | None, area: str, line: str) -> list[dict]:
        where = "window_start >= ? and area = ? and line = ?"
        params: list[Any] = [since, area, line]
        if network:
            where += " and network = ?"
            params.append(network)
        return self._query(
            "kpi_line_5min",
            f"""
            select window_start, network, area, line, n_trains, n_canceled, mean_delay_s,
                   p50_delay_s, p90_delay_s, max_delay_s, pct_on_time
            from {{src}} where {where}
            qualify row_number() over (partition by window_start, network, area, line
                                       order by window_start) = 1
            order by window_start, network""",
            params,
        )

    def stops(self, since: datetime, network: str | None, limit: int) -> list[dict]:
        where = "window_start >= ? and n_trains > 0"
        params: list[Any] = [since]
        if network:
            where += " and network = ?"
            params.append(network)
        params.append(limit)
        return self._query(
            "kpi_stop_5min",
            f"""
            select network, stop_id, any_value(stop_name) as stop_name,
                   any_value(lat) as lat, any_value(lon) as lon,
                   sum(n_trains) as n_obs,
                   round(sum(mean_delay_s * n_trains) / sum(n_trains), 1) as mean_delay_s,
                   max(max_delay_s) as max_delay_s
            from {{src}} where {where}
            group by network, stop_id having sum(n_trains) >= 3
            order by mean_delay_s desc limit ?""",
            params,
        )

    def anomalies(self, since: datetime) -> list[dict]:
        return self._query(
            "anomalies",
            """
            select anomaly_id, any_value(type) as type, any_value(network) as network,
                   any_value(area) as area, any_value(line) as line,
                   any_value(trip_id) as trip_id, min(started_at) as started_at,
                   max(resolved_at) as resolved_at, max(peak) as peak,
                   arg_max(message, coalesce(resolved_at, last_seen)) as message
            from {src} where started_at >= ?
            group by anomaly_id order by started_at desc limit 200""",
            [since.isoformat()],
        )

    def quality(self, since: datetime) -> list[dict]:
        return self._query(
            "quality_5min",
            """
            select window_start, feed, sum(n_in) as n_in, sum(n_valid) as n_valid,
                   sum(n_rejected) as n_rejected, max(max_lag_s) as max_lag_s
            from {src} where window_start >= ?
            group by window_start, feed order by window_start, feed""",
            [since],
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    gold = GoldReader(settings.data_dir)
    cache = _Cache()
    app = FastAPI(
        title="Pulso del transporte",
        version="0.1.0",
        description="Retrasos de Renfe en tiempo real (datos CC-BY 4.0 de Renfe).",
    )

    def since(hours: float) -> datetime:
        return datetime.now(UTC) - timedelta(hours=hours)

    @app.get("/api/health")
    def health() -> dict:
        path = settings.data_dir / "live" / "state.json"
        age = time.time() - path.stat().st_mtime if path.exists() else None
        return {"ok": age is not None and age < STALE_AFTER_S, "state_age_s": age}

    @app.get("/api/state")
    def state() -> JSONResponse:
        path = settings.data_dir / "live" / "state.json"
        if not path.exists():
            raise HTTPException(503, "Todavía no hay estado: el procesador no ha arrancado.")
        data = json.loads(path.read_text(encoding="utf-8"))
        data["state_age_s"] = round(time.time() - path.stat().st_mtime, 1)
        data["stale"] = data["state_age_s"] > STALE_AFTER_S
        return JSONResponse(data, headers={"Cache-Control": "no-store"})

    @app.get("/api/kpis")
    def kpis(
        hours: float = Query(6, gt=0, le=24 * 14),
        network: str | None = None,
        area: str = "*",
        line: str = "*",
    ) -> list[dict]:
        key = f"kpis|{hours}|{network}|{area}|{line}"
        return cache.get(key, lambda: gold.kpis(since(hours), network, area, line))

    @app.get("/api/stops")
    def stops(
        hours: float = Query(1, gt=0, le=24 * 7),
        network: str | None = None,
        limit: int = Query(15, gt=0, le=200),
    ) -> list[dict]:
        return cache.get(
            f"stops|{hours}|{network}|{limit}", lambda: gold.stops(since(hours), network, limit)
        )

    @app.get("/api/anomalies")
    def anomalies(hours: float = Query(24, gt=0, le=24 * 30)) -> list[dict]:
        return cache.get(f"anom|{hours}", lambda: gold.anomalies(since(hours)))

    @app.get("/api/quality")
    def quality(hours: float = Query(6, gt=0, le=24 * 14)) -> list[dict]:
        return cache.get(f"quality|{hours}", lambda: gold.quality(since(hours)))

    @app.get("/api/predictions")
    def predictions() -> JSONResponse:
        path = settings.data_dir / "live" / "predictions.json"
        if not path.exists():
            return JSONResponse(
                {
                    "available": False,
                    "reason": "El modelo aún no está entrenado: hacen falta "
                    "varios días de datos reales.",
                }
            )
        return JSONResponse(json.loads(path.read_text(encoding="utf-8")))

    @app.get("/", include_in_schema=False)
    def index() -> FileResponse:
        return FileResponse(WEB_DIR / "index.html")

    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
    return app


def serve(host: str = "0.0.0.0", port: int = 8000) -> None:
    import uvicorn

    uvicorn.run(create_app(), host=host, port=port, log_level="info")
