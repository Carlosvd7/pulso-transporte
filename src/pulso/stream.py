"""Núcleo del procesamiento en streaming (lógica pura, sin Kafka).

Recibe eventos ya validados y mantiene:
- Estado en vivo de cada tren (último retraso, parada, posición), con caducidad.
- Ventanas fijas de 5 min por *tiempo del evento* (cabecera del feed), cerradas con una
  marca de agua (watermark) y un margen para eventos tardíos.
- KPIs por línea, núcleo, red y estación al cerrar cada ventana.
- Anomalías (ver anomaly.py).

Decisiones:
- En cada ventana cada tren cuenta una vez (su última observación), para no pesar más a
  los trenes que aparecen en más snapshots.
- Watermark por partición: el mínimo de los máximos de cada fuente activa. Así, al
  rehidratar desde Kafka, una partición leída antes que otra no hace que los eventos de la
  otra lleguen "tarde".
- Idempotencia: los eventos repetidos (mismo event_id) se ignoran.
- Los retrasos marcados como extremos por los contratos se excluyen de las medias y se
  cuentan aparte.
"""

from __future__ import annotations

import math
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from pulso.anomaly import AnomalyTracker, detect_line_anomalies, detect_stalled
from pulso.lookup import UNKNOWN_AREA, UNKNOWN_LINE, StaticLookup, TripInfo, ld_key_date
from pulso.quality import summarize

MADRID = ZoneInfo("Europe/Madrid")
WINDOW = timedelta(minutes=5)
LATENESS = timedelta(seconds=90)
TRAIN_TTL = timedelta(minutes=10)
IDLE_SOURCE = timedelta(minutes=10)
ON_TIME_S = {"cercanias": 180, "ld": 300}
EXCLUDED_FLAGS = frozenset({"adelanto_extremo", "retraso_extremo"})
ALL = "*"


def parse_ts(v: Any) -> datetime | None:
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(v)


def window_start(ts: datetime) -> datetime:
    epoch = int(ts.timestamp())
    return datetime.fromtimestamp(epoch - epoch % int(WINDOW.total_seconds()), tz=UTC)


def quantile(sorted_vals: list[float], q: float) -> float | None:
    """Cuantil con interpolación lineal (como numpy por defecto)."""
    if not sorted_vals:
        return None
    pos = (len(sorted_vals) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (pos - lo)


@dataclass
class Train:
    network: str
    trip_id: str
    line: str = UNKNOWN_LINE
    area: str = UNKNOWN_AREA
    color: str | None = None
    service_date: date | None = None
    delay_s: int | None = None
    status: str = "SCHEDULED"
    stop_id: str | None = None
    stop_name: str | None = None
    flags: list[str] = field(default_factory=list)
    updated_at: datetime | None = None
    lat: float | None = None
    lon: float | None = None
    position_at: datetime | None = None
    history: deque = field(default_factory=lambda: deque(maxlen=120))


@dataclass
class Obs:
    delay_s: int | None
    canceled: bool
    excluded: bool
    new_cancel: bool
    stop_id: str | None


@dataclass
class Output:
    kpis: list[dict] = field(default_factory=list)
    stops: list[dict] = field(default_factory=list)
    quality: list[dict] = field(default_factory=list)
    anomalies: list[dict] = field(default_factory=list)
    closed_windows: list[datetime] = field(default_factory=list)


class StreamProcessor:
    def __init__(self, lookup: StaticLookup | None = None, max_ids: int = 300_000) -> None:
        self.lookup = lookup or StaticLookup()
        self.trains: dict[tuple[str, str], Train] = {}
        # ventana -> (red, núcleo, línea) -> trip_id -> última observación
        self.windows: dict[datetime, dict[tuple, dict[str, Obs]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        self.quality_buf: dict[datetime, list[dict]] = defaultdict(list)
        self.quality_recent: deque = deque(maxlen=5000)
        self.alerts: dict[str, dict] = {}
        self.source_ts: dict[str, datetime] = {}
        self.watermark: datetime | None = None
        self.closed_until: datetime | None = None
        self.history: dict[tuple, deque] = defaultdict(lambda: deque(maxlen=48))
        self.anomalies = AnomalyTracker()
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._max_ids = max_ids
        self._canceled: set[tuple[str, str]] = set()
        self.counters: dict[str, int] = defaultdict(int)
        self.out = Output()

    # ---------------------------------------------------------------- entrada
    def _is_dup(self, event_id: str) -> bool:
        if event_id in self._seen:
            self.counters["duplicados_ignorados"] += 1
            return True
        self._seen[event_id] = None
        if len(self._seen) > self._max_ids:
            self._seen.popitem(last=False)
        return False

    def _train(self, network: str, trip_id: str) -> Train:
        key = (network, trip_id)
        tr = self.trains.get(key)
        if tr is None:
            tr = Train(network, trip_id)
            info: TripInfo | None = self.lookup.trip(network, trip_id)
            if info is not None:
                tr.line, tr.area, tr.color = info.line, info.area, info.color
                tr.service_date = info.service_date
            elif network == "ld":
                # Sin cruce, la fecha de servicio sigue estando en el propio trip_id.
                tr.service_date = ld_key_date(trip_id)[1]
            self.trains[key] = tr
        return tr

    def on_trip_update(self, ev: dict, source: str = "trip_updates") -> None:
        if self._is_dup(ev["event_id"]):
            return
        ts = parse_ts(ev["feed_ts"])
        self.counters["eventos_retraso"] += 1
        net, trip_id = ev["network"], ev["trip_id"]
        canceled = ev["schedule_relationship"] == "CANCELED"
        tr = self._train(net, trip_id)
        flags = ev.get("flags") or []
        if tr.updated_at is None or ts >= tr.updated_at:
            tr.delay_s, tr.flags, tr.updated_at = ev.get("delay_s"), flags, ts
            tr.status = ev["schedule_relationship"]
            if ev.get("stop_id"):
                tr.stop_id = ev["stop_id"]
                stop = self.lookup.stop(net, tr.stop_id)
                tr.stop_name = stop.name if stop else None
            if tr.delay_s is not None and not canceled:
                tr.history.append((ts, tr.delay_s))

        new_cancel = False
        if canceled and (net, trip_id) not in self._canceled:
            self._canceled.add((net, trip_id))
            new_cancel = not self._is_stale(tr, ts)

        if self._is_stale(tr, ts):
            # Renfe sigue publicando trenes de días pasados: no cuentan para los KPIs.
            self.counters["viajes_de_dias_pasados"] += 1
        elif self.closed_until is not None and ts < self.closed_until:
            self.counters["eventos_tardios"] += 1
        else:
            w = window_start(ts)
            self.windows[w][(net, tr.area, tr.line)][trip_id] = Obs(
                ev.get("delay_s"),
                canceled,
                bool(EXCLUDED_FLAGS & set(flags))
                or ("tren_detenido", net, trip_id) in self.anomalies.active,
                new_cancel,
                ev.get("stop_id"),
            )
        self._advance(source, ts)

    def on_vehicle(self, ev: dict, source: str = "vehicle_positions") -> None:
        if self._is_dup(ev["event_id"]) or not ev.get("trip_id"):
            return
        ts = parse_ts(ev["feed_ts"])
        tr = self._train(ev["network"], ev["trip_id"])
        if tr.position_at is None or ts >= tr.position_at:
            tr.lat, tr.lon, tr.position_at = ev["lat"], ev["lon"], ts
            if tr.updated_at is None:
                tr.updated_at = ts
        self._advance(source, ts)

    def on_alert(self, ev: dict) -> None:
        if ev.get("status") == "removed":
            self.alerts.pop(ev["alert_id"], None)
        else:
            self.alerts[ev["alert_id"]] = ev

    def on_quality(self, report: dict) -> None:
        self.quality_recent.append(report)
        ts = parse_ts(report["fetched_at"])
        if self.closed_until is None or ts >= self.closed_until:
            self.quality_buf[window_start(ts)].append(report)

    # --------------------------------------------------------------- tiempo
    def _is_stale(self, tr: Train, ts: datetime) -> bool:
        """Viaje de un día que ya pasó (Renfe sigue publicando cancelaciones antiguas)."""
        if tr.service_date is None:
            return False
        return tr.service_date < ts.astimezone(MADRID).date() - timedelta(days=1)

    def _advance(self, source: str, ts: datetime) -> None:
        if ts > self.source_ts.get(source, datetime.min.replace(tzinfo=UTC)):
            self.source_ts[source] = ts
        newest = max(self.source_ts.values())
        active = [t for t in self.source_ts.values() if t >= newest - IDLE_SOURCE]
        wm = min(active)
        if self.watermark is None or wm > self.watermark:
            self.watermark = wm
            self._close_ready()

    def _close_ready(self) -> None:
        assert self.watermark is not None
        limit = self.watermark - LATENESS
        for w in sorted(set(self.windows) | set(self.quality_buf)):
            if w + WINDOW <= limit:
                self._close(w)

    def flush_all(self) -> None:
        """Cierra todas las ventanas abiertas (fin de un replay o de un test)."""
        for w in sorted(set(self.windows) | set(self.quality_buf)):
            self._close(w)

    # ------------------------------------------------------- cierre de ventana
    def _close(self, w: datetime) -> None:
        groups = self.windows.pop(w, {})
        end = w + WINDOW
        self.closed_until = max(self.closed_until or end, end)
        self.out.closed_windows.append(w)
        rows = self._kpi_rows(w, groups)
        self.out.kpis.extend(rows)
        self.out.stops.extend(self._stop_rows(w, groups))
        self.out.quality.extend(self._quality_rows(w, self.quality_buf.pop(w, [])))
        for r in rows:
            if r["n_trains"] > 0 and r["mean_delay_s"] is not None:
                self.history[(r["network"], r["area"], r["line"])].append(r["mean_delay_s"])
        self.out.anomalies.extend(detect_line_anomalies(self.anomalies, rows, self.history, w, end))
        self._expire(end)
        self.out.anomalies.extend(detect_stalled(self.anomalies, self.trains.values(), end))
        self.out.anomalies.extend(self.anomalies.resolve_missing(end))

    def _expire(self, now: datetime) -> None:
        for key, tr in list(self.trains.items()):
            last = max(t for t in (tr.updated_at, tr.position_at) if t is not None)
            if now - last > TRAIN_TTL:
                del self.trains[key]

    def _kpi_rows(self, w: datetime, groups: dict[tuple, dict[str, Obs]]) -> list[dict]:
        merged: dict[tuple, dict[str, Obs]] = defaultdict(dict)
        for (net, area, line), trips in groups.items():
            for key in ((net, area, line), (net, area, ALL), (net, ALL, ALL)):
                merged[key].update(trips)
        rows = []
        for (net, area, line), trips in sorted(merged.items()):
            rows.append(
                {
                    "window_start": w,
                    "window_end": w + WINDOW,
                    "network": net,
                    "area": area,
                    "line": line,
                    **self._stats(net, trips.values()),
                }
            )
        return rows

    @staticmethod
    def _stats(network: str, obs) -> dict[str, Any]:
        obs = list(obs)
        canceled = [o for o in obs if o.canceled]
        usable = sorted(
            o.delay_s for o in obs if not o.canceled and not o.excluded and o.delay_s is not None
        )
        thr = ON_TIME_S.get(network, 180)
        n = len(usable)
        return {
            "n_trains": n,
            "n_canceled": len(canceled),
            "n_new_canceled": sum(o.new_cancel for o in canceled),
            "n_excluded": sum(o.excluded and not o.canceled for o in obs),
            "mean_delay_s": round(sum(usable) / n, 1) if n else None,
            "p50_delay_s": quantile(usable, 0.5),
            "p90_delay_s": quantile(usable, 0.9),
            "max_delay_s": usable[-1] if n else None,
            "pct_on_time": round(100.0 * sum(d < thr for d in usable) / n, 1) if n else None,
        }

    def _stop_rows(self, w: datetime, groups: dict[tuple, dict[str, Obs]]) -> list[dict]:
        by_stop: dict[tuple, list[Obs]] = defaultdict(list)
        for (net, _, _), trips in groups.items():
            for o in trips.values():
                if o.stop_id and not o.canceled:
                    by_stop[(net, o.stop_id)].append(o)
        rows = []
        for (net, stop_id), obs in sorted(by_stop.items()):
            stop = self.lookup.stop(net, stop_id)
            st = self._stats(net, obs)
            rows.append(
                {
                    "window_start": w,
                    "network": net,
                    "stop_id": stop_id,
                    "stop_name": stop.name if stop else None,
                    "lat": stop.lat if stop else None,
                    "lon": stop.lon if stop else None,
                    "n_trains": st["n_trains"],
                    "mean_delay_s": st["mean_delay_s"],
                    "max_delay_s": st["max_delay_s"],
                    "pct_on_time": st["pct_on_time"],
                }
            )
        return rows

    @staticmethod
    def _quality_rows(w: datetime, reports: list[dict]) -> list[dict]:
        rows = []
        for feed, q in sorted(summarize(reports).items()):
            rows.append(
                {
                    "window_start": w,
                    "feed": feed,
                    "snapshots": q.snapshots,
                    "n_in": q.n_in,
                    "n_valid": q.n_valid,
                    "n_rejected": q.n_rejected,
                    "errors": dict(q.errors),
                    "warnings": dict(q.warnings),
                    "max_lag_s": q.max_lag_s,
                }
            )
        return rows

    # ------------------------------------------------------------- salidas
    def drain(self) -> Output:
        out, self.out = self.out, Output()
        return out

    def live_state(self, recent_kpis: list[dict], now: datetime | None = None) -> dict:
        """Foto del estado actual para el panel (JSON serializable)."""
        trains = []
        for tr in self.trains.values():
            trains.append(
                {
                    "network": tr.network,
                    "trip_id": tr.trip_id,
                    "line": tr.line,
                    "area": tr.area,
                    "color": tr.color,
                    "status": tr.status,
                    "delay_s": tr.delay_s,
                    "stop_id": tr.stop_id,
                    "stop_name": tr.stop_name,
                    "flags": tr.flags,
                    "lat": tr.lat,
                    "lon": tr.lon,
                    "updated_at": tr.updated_at.isoformat() if tr.updated_at else None,
                    "stalled": ("tren_detenido", tr.network, tr.trip_id) in self.anomalies.active,
                }
            )
        lines: dict[tuple, dict] = {}
        for t in trains:
            key = (t["network"], t["area"], t["line"])
            d = lines.setdefault(
                key,
                {
                    "network": key[0],
                    "area": key[1],
                    "line": key[2],
                    "color": t["color"],
                    "delays": [],
                    "n_canceled": 0,
                },
            )
            if t["status"] == "CANCELED":
                d["n_canceled"] += 1
            elif (
                t["delay_s"] is not None
                and not EXCLUDED_FLAGS & set(t["flags"])
                and not t["stalled"]
            ):
                d["delays"].append(t["delay_s"])
        # Una ventana puede venir dos veces (historia de gold + recalculada al rehidratar):
        # se queda la última.
        spark: dict[tuple, dict] = defaultdict(dict)
        for r in recent_kpis:
            spark[(r["network"], r["area"], r["line"])][r["window_start"]] = r["mean_delay_s"]
        line_list = []
        for key, d in lines.items():
            ds = sorted(d.pop("delays"))
            thr = ON_TIME_S.get(key[0], 180)
            d.update(
                {
                    "n_trains": len(ds),
                    "mean_delay_s": round(sum(ds) / len(ds), 1) if ds else None,
                    "max_delay_s": ds[-1] if ds else None,
                    "pct_on_time": round(100 * sum(x < thr for x in ds) / len(ds), 1)
                    if ds
                    else None,
                    "history": [
                        {"t": t.isoformat() if isinstance(t, datetime) else t, "v": v}
                        for t, v in sorted(spark.get(key, {}).items(), key=lambda kv: kv[0])[-24:]
                    ],
                }
            )
            line_list.append(d)
        line_list.sort(key=lambda d: (-(d["mean_delay_s"] or -1e9), d["network"], d["line"]))
        alerts = []
        for a in self.alerts.values():
            lines_affected = sorted(
                {
                    r.line
                    for rid in a.get("route_ids", [])
                    if (r := self.lookup.route(a["network"], rid)) is not None
                }
            )
            alerts.append(
                {
                    "alert_id": a["alert_id"],
                    "network": a["network"],
                    "lines": lines_affected,
                    "header": a.get("header"),
                    "description": a.get("description"),
                    "active_start": a.get("active_start"),
                }
            )
        q = summarize(list(self.quality_recent)[-600:])
        return {
            "generated_at": (now or datetime.now(UTC)).isoformat(),
            "watermark": self.watermark.isoformat() if self.watermark else None,
            "trains": trains,
            "lines": line_list,
            "anomalies": self.anomalies.recent(self.watermark),
            "alerts": alerts,
            "quality": {
                feed: {
                    "snapshots": s.snapshots,
                    "n_in": s.n_in,
                    "pct_valid": round(s.pct_valid, 2),
                    "n_rejected": s.n_rejected,
                    "errors": dict(s.errors),
                    "warnings": dict(s.warnings),
                    "max_lag_s": s.max_lag_s,
                    "last_fetched_at": s.last_fetched_at,
                }
                for feed, s in q.items()
            },
            "counters": dict(self.counters),
        }
