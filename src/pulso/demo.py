"""Demo estática: «repetición» de un tramo real de datos, sin servidor.

Reprocesa el histórico de bronze con el mismo código que en directo (ingesta, contratos y
procesador en streaming) y guarda una foto del estado cada N minutos de tiempo de evento.
El panel web, en modo demo, reproduce esas fotos como si fueran en vivo, con un aviso visible
de que es una repetición. Se publica en GitHub Pages y no necesita ningún servidor.

Salida (en `out/`):
  index.html, static/*      el mismo panel que en directo
  data/manifest.json        rango, fotogramas y fuente
  data/frames/NNNN.json     estado en vivo en cada instante
  data/kpis.json            KPIs por red (ventanas de 5 min) para los gráficos
  data/anomalies.json       todas las anomalías detectadas en el tramo
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from collections import deque
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pulso.bus import MemoryBus
from pulso.config import Settings
from pulso.ingest import Ingestor
from pulso.lookup import StaticLookup
from pulso.processor import ProcessorApp
from pulso.producer import iter_bronze

log = logging.getLogger(__name__)

WEB_DIR = Path(__file__).parent / "web"


def _iso(v: Any) -> Any:
    return v.isoformat() if isinstance(v, datetime) else v


def _compact_train(t: dict) -> dict:
    """Solo lo que usa el panel (los fotogramas se descargan uno a uno)."""
    out = {
        k: t[k]
        for k in (
            "network",
            "trip_id",
            "line",
            "area",
            "color",
            "status",
            "delay_s",
            "stop_name",
            "updated_at",
        )
    }
    out["flags"] = [f for f in t["flags"] if f in ("retraso_extremo", "adelanto_extremo")]
    out["stalled"] = t["stalled"]
    out["stale"] = t["stale"]
    out["lat"] = round(t["lat"], 5) if t["lat"] is not None else None
    out["lon"] = round(t["lon"], 5) if t["lon"] is not None else None
    return out


def _top_stops(rows: deque, now: datetime, limit: int = 12) -> list[dict]:
    agg: dict[tuple, dict] = {}
    for r in rows:
        if r["window_start"] < now - timedelta(hours=1) or not r["n_trains"]:
            continue
        a = agg.setdefault(
            (r["network"], r["stop_id"]),
            {
                "network": r["network"],
                "stop_id": r["stop_id"],
                "stop_name": r["stop_name"],
                "n_obs": 0,
                "_sum": 0.0,
                "max_delay_s": None,
            },
        )
        a["n_obs"] += r["n_trains"]
        a["_sum"] += (r["mean_delay_s"] or 0) * r["n_trains"]
        if r["max_delay_s"] is not None:
            a["max_delay_s"] = max(a["max_delay_s"] or r["max_delay_s"], r["max_delay_s"])
    out = []
    for a in agg.values():
        if a["n_obs"] >= 3:
            a["mean_delay_s"] = round(a.pop("_sum") / a["n_obs"], 1)
            out.append(a)
    return sorted(out, key=lambda a: -a["mean_delay_s"])[:limit]


def build_demo(
    settings: Settings,
    out: Path,
    since: datetime,
    until: datetime,
    every_min: float = 5,
    warmup_min: float = 30,
) -> dict:
    """Genera la demo estática en `out`. Devuelve el manifiesto."""
    work = out / "_work"
    shutil.rmtree(out, ignore_errors=True)
    (out / "data" / "frames").mkdir(parents=True)
    app = ProcessorApp(
        replace(settings, data_dir=work), publisher=None, lookup=StaticLookup(settings)
    )
    bus = MemoryBus()
    ingestor = Ingestor(bus)

    every = timedelta(minutes=every_min)
    next_frame = since
    frames: list[dict] = []
    stops: deque = deque()
    kpis: list[dict] = []  # solo red (area='*'), para los gráficos
    line_kpis: deque = deque()  # todas las líneas, últimas 2 h, para las tendencias
    anomalies: dict[str, dict] = {}

    def collect(o) -> None:
        stops.extend(o.stops)
        while stops and stops[0]["window_start"] < (app.proc.watermark or since) - timedelta(
            hours=1
        ):
            stops.popleft()
        kpis.extend(r for r in o.kpis if r["area"] == "*")
        line_kpis.extend(o.kpis)
        while line_kpis and line_kpis[0]["window_start"] < (
            app.proc.watermark or since
        ) - timedelta(hours=2):
            line_kpis.popleft()
        for a in o.anomalies:
            keep = {
                k: a.get(k)
                for k in (
                    "anomaly_id",
                    "type",
                    "network",
                    "area",
                    "line",
                    "trip_id",
                    "message",
                    "peak",
                    "started_at",
                    "resolved_at",
                )
            }
            prev = anomalies.get(a["anomaly_id"])
            if prev:  # la resolución actualiza mensaje y cierre, no el inicio
                keep["started_at"] = prev["started_at"]
            anomalies[a["anomaly_id"]] = keep

    for feed, snap in iter_bronze(settings, since - timedelta(minutes=warmup_min), until):
        ingestor.handle(feed, snap)
        for topic, msgs in bus.messages.items():
            for _key, value in msgs:
                app.handle(topic, value, f"{topic}:0")
        bus.messages.clear()
        collect(app.proc.drain())
        wm = app.proc.watermark
        if wm is None or wm < next_frame:
            continue
        state = app.proc.live_state(list(line_kpis), now=wm)
        frame = {
            "t": wm.isoformat(),
            "trains": [_compact_train(t) for t in state["trains"]],
            "lines": state["lines"],
            "anomalies": state["anomalies"],
            "alerts": state["alerts"],
            "quality": state["quality"],
            "counters": state["counters"],
            "stops": _top_stops(stops, wm),
        }
        name = f"{len(frames):04d}.json"
        (out / "data" / "frames" / name).write_text(
            json.dumps(frame, ensure_ascii=False, separators=(",", ":"), default=_iso),
            encoding="utf-8",
        )
        frames.append(
            {
                "t": wm.isoformat(),
                "file": f"frames/{name}",
                "trains": sum(1 for t in state["trains"] if t["status"] != "CANCELED"),
            }
        )
        next_frame = wm + every

    (out / "data" / "kpis.json").write_text(
        json.dumps(
            [
                {
                    k: _iso(r[k])
                    for k in (
                        "window_start",
                        "network",
                        "n_trains",
                        "mean_delay_s",
                        "pct_on_time",
                        "n_canceled",
                    )
                }
                for r in kpis
            ],
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    (out / "data" / "anomalies.json").write_text(
        json.dumps(
            sorted(anomalies.values(), key=lambda a: a["started_at"]),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    manifest = {
        "kind": "replay",
        "since": since.isoformat(),
        "until": until.isoformat(),
        "every_min": every_min,
        "frames": frames,
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "Renfe Data, GTFS-Realtime (CC-BY 4.0)",
    }
    (out / "data" / "manifest.json").write_text(
        json.dumps(manifest, separators=(",", ":")), encoding="utf-8"
    )

    shutil.copytree(WEB_DIR, out / "static", ignore=shutil.ignore_patterns("index.html"))
    html = (WEB_DIR / "index.html").read_text(encoding="utf-8")
    # Versión por contenido en las URLs: tras republicar, nadie se queda con el JS viejo en caché.
    version = hashlib.sha256(
        b"".join((WEB_DIR / f).read_bytes() for f in ("app.js", "styles.css"))
    ).hexdigest()[:10]
    for asset in ("styles.css", "app.js"):
        html = html.replace(f"/static/{asset}", f"static/{asset}?v={version}")
    html = html.replace(
        "<head>", '<head>\n  <script>window.PULSO_DEMO = "data/manifest.json";</script>', 1
    )
    (out / "index.html").write_text(html, encoding="utf-8")
    (out / ".nojekyll").write_text("")
    shutil.rmtree(work, ignore_errors=True)
    log.info("demo: %d fotogramas en %s", len(frames), out)
    return manifest
