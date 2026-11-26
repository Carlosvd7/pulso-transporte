"""Detección de anomalías sobre las ventanas cerradas.

Tipos:
- pico_retraso: el retraso medio de una línea/núcleo se dispara respecto a su propia
  historia reciente (z-score robusto con mediana y MAD >= 3,5 y al menos 5 min de media).
- retraso_alto: retraso medio >= 15 min con al menos 3 trenes (umbral absoluto, útil
  cuando todavía no hay historia).
- cancelaciones: 3 o más cancelaciones nuevas en un núcleo en una ventana de 5 min.
- tren_detenido: el retraso de un tren crece al mismo ritmo que el reloj durante 15 min
  o más (el tren no avanza, o el dato está congelado).

Cada anomalía se emite al empezar y al resolverse (tras 2 ventanas sin cumplirse), no en
cada ventana, para no inundar el panel.
"""

from __future__ import annotations

import statistics
from collections import deque
from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from pulso.lookup import UNKNOWN_AREA, UNKNOWN_LINE

MIN_TRAINS = 3
MIN_HISTORY = 6
Z_THRESHOLD = 3.5
MIN_SPIKE_S = 300
HIGH_DELAY_S = 900
MIN_SCALE_S = 60
CANCEL_WAVE = 3
STALL_SPAN = timedelta(minutes=15)
STALL_LOOKBACK = timedelta(minutes=30)
STALL_MIN_DELAY_S = 600
STALL_RATIO = 0.8
RESOLVE_AFTER = 2
KEEP_RESOLVED = timedelta(hours=2)


def _fmt_min(seconds: float) -> str:
    return f"{seconds / 60:.0f} min"


class AnomalyTracker:
    def __init__(self) -> None:
        self.active: dict[tuple, dict[str, Any]] = {}
        self.misses: dict[tuple, int] = {}
        self.resolved: deque = deque(maxlen=500)
        self._fired: set[tuple] = set()

    def fire(self, kind: str, key: tuple, at: datetime, **info: Any) -> list[dict]:
        k = (kind, *key)
        self._fired.add(k)
        self.misses[k] = 0
        if k in self.active:
            a = self.active[k]
            a["last_seen"] = at.isoformat()
            a["value"] = info.get("value", a.get("value"))
            a["peak"] = max(a.get("peak") or 0, info.get("value") or 0)
            a["message"] = info.get("message", a["message"])
            return []
        a = {
            "anomaly_id": f"{kind}|{'|'.join(str(x) for x in key)}|{at:%Y%m%dT%H%M}",
            "type": kind,
            "status": "started",
            "started_at": at.isoformat(),
            "last_seen": at.isoformat(),
            "peak": info.get("value"),
            **info,
        }
        self.active[k] = a
        return [dict(a)]

    def resolve_missing(self, at: datetime) -> list[dict]:
        out = []
        for k in list(self.active):
            if k in self._fired:
                continue
            self.misses[k] = self.misses.get(k, 0) + 1
            if self.misses[k] >= RESOLVE_AFTER:
                a = self.active.pop(k)
                self.misses.pop(k, None)
                a.update(status="resolved", resolved_at=at.isoformat())
                self.resolved.append(a)
                out.append(dict(a))
        self._fired.clear()
        return out

    def recent(self, now: datetime | None) -> list[dict]:
        items = [dict(a, status="active") for a in self.active.values()]
        if now is not None:
            items += [
                dict(a)
                for a in self.resolved
                if datetime.fromisoformat(a["resolved_at"]) >= now - KEEP_RESOLVED
            ]
        return sorted(items, key=lambda a: a["started_at"], reverse=True)


def robust_z(value: float, history: list[float]) -> tuple[float, float]:
    """(z, mediana) con escala = 1,4826 * MAD (mínimo MIN_SCALE_S)."""
    med = statistics.median(history)
    mad = statistics.median(abs(x - med) for x in history)
    scale = max(1.4826 * mad, MIN_SCALE_S)
    return (value - med) / scale, med


def _where(row: dict) -> str:
    if row["line"] != "*":
        return f"{row['line']} ({row['area']})"
    if row["area"] != "*":
        return row["area"]
    return "toda la red de " + ("Cercanías" if row["network"] == "cercanias" else "AV/LD")


def detect_line_anomalies(
    tracker: AnomalyTracker,
    rows: list[dict],
    history: dict[tuple, deque],
    w: datetime,
    end: datetime,
) -> list[dict]:
    out: list[dict] = []
    for r in rows:
        if r["line"] == UNKNOWN_LINE or r["area"] == UNKNOWN_AREA:
            continue  # sin cruzar con el horario no sabemos a qué línea culpar
        key = (r["network"], r["area"], r["line"])
        base = {
            "network": r["network"],
            "area": r["area"],
            "line": r["line"],
            "trip_id": None,
            "window_start": w.isoformat(),
        }
        mean, n = r["mean_delay_s"], r["n_trains"]
        if mean is not None and n >= MIN_TRAINS:
            past = list(history.get(key, []))[:-1]  # la ventana actual ya está al final
            if len(past) >= MIN_HISTORY:
                z, med = robust_z(mean, past)
                if z >= Z_THRESHOLD and mean >= MIN_SPIKE_S:
                    out += tracker.fire(
                        "pico_retraso",
                        key,
                        end,
                        **base,
                        value=mean,
                        baseline=med,
                        message=f"Pico de retraso en {_where(r)}: {_fmt_min(mean)} de media "
                        f"(lo normal en las últimas horas: {_fmt_min(med)})",
                    )
            if mean >= HIGH_DELAY_S:
                out += tracker.fire(
                    "retraso_alto",
                    key,
                    end,
                    **base,
                    value=mean,
                    baseline=None,
                    message=f"Retraso alto en {_where(r)}: {_fmt_min(mean)} de media "
                    f"con {n} trenes",
                )
        if r["line"] == "*" and r["area"] != "*" and r["n_new_canceled"] >= CANCEL_WAVE:
            out += tracker.fire(
                "cancelaciones",
                key,
                end,
                **base,
                value=r["n_new_canceled"],
                baseline=None,
                message=f"{r['n_new_canceled']} trenes cancelados en {_where(r)} en 5 min",
            )
    return out


def stall_rate(history: Iterable[tuple[datetime, int]], now: datetime) -> tuple[float, int] | None:
    """(segundos de retraso ganados por segundo de reloj, último retraso) o None."""
    pts = [(t, d) for t, d in history if t >= now - STALL_LOOKBACK]
    if len(pts) < 2:
        return None
    (t0, d0), (t1, d1) = pts[0], pts[-1]
    elapsed = (t1 - t0).total_seconds()
    if elapsed < STALL_SPAN.total_seconds():
        return None
    return (d1 - d0) / elapsed, d1


def detect_stalled(tracker: AnomalyTracker, trains, now: datetime) -> list[dict]:
    out: list[dict] = []
    for tr in trains:
        if tr.status == "CANCELED":
            continue
        res = stall_rate(tr.history, now)
        if res is None:
            continue
        rate, last = res
        if rate >= STALL_RATIO and last >= STALL_MIN_DELAY_S:
            out += tracker.fire(
                "tren_detenido",
                (tr.network, tr.trip_id),
                now,
                network=tr.network,
                area=tr.area,
                line=tr.line,
                trip_id=tr.trip_id,
                value=last,
                baseline=None,
                window_start=None,
                message=f"Tren {tr.trip_id} ({tr.line}, {tr.area}) parece detenido "
                f"(o con el dato congelado): su retraso crece al ritmo del reloj y ya es de "
                f"{_fmt_min(last)}" + (f", cerca de {tr.stop_name}" if tr.stop_name else ""),
            )
    return out
