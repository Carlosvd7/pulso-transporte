"""Contratos de datos: qué debe cumplir cada evento antes de entrar en el bus.

Dos niveles de gravedad:
- ERROR: el evento se rechaza y va a la cola de rechazados (DLQ) con el motivo.
- AVISO: el evento pasa, pero lleva una marca (`flags`) y se cuenta en las métricas.

Cada snapshot genera además un `QualityReport` con los recuentos por regla, que es lo que
muestra el panel de "salud de los datos".
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pulso.parse import Row

SCHEMA_VERSION = 1

# Caja que contiene España peninsular, Baleares y Canarias.
SPAIN_BBOX = {"lat": (27.0, 44.5), "lon": (-18.5, 4.5)}

FUTURE_TOLERANCE = timedelta(minutes=2)
STALE_AFTER = timedelta(minutes=5)
EXTREME_DELAY_S = 3 * 3600
HOUR_SHIFT_TOLERANCE_S = 120
EXTREME_EARLY_S = -15 * 60


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "aviso"


@dataclass(frozen=True)
class Issue:
    check: str
    severity: Severity
    detail: str = ""


def normalize_delay(raw: int | None) -> int | None:
    """Reduce un retraso a (-12 h, +12 h]. Ver warehouse.NORMALIZE_DELAY (misma fórmula)."""
    if raw is None:
        return None
    return ((raw + 43200) % 86400 + 86400) % 86400 - 43200


class _Event(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = SCHEMA_VERSION
    event_id: str = Field(min_length=1)
    network: Literal["cercanias", "ld"]
    feed_ts: datetime
    fetched_at: datetime
    flags: list[str] = Field(default_factory=list)


class TripUpdateEvent(_Event):
    trip_id: str = Field(min_length=1)
    schedule_relationship: Literal[
        "SCHEDULED", "ADDED", "UNSCHEDULED", "CANCELED", "REPLACEMENT", "DUPLICATED", "DELETED"
    ]
    stop_id: str | None = None
    arrival_time: datetime | None = None
    raw_delay_s: int | None = None
    delay_s: int | None = None


class VehiclePositionEvent(_Event):
    trip_id: str | None = None
    vehicle_id: str | None = None
    vehicle_label: str | None = None
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    current_status: str | None = None
    stop_id: str | None = None
    vehicle_ts: datetime | None = None


class AlertEvent(_Event):
    alert_id: str = Field(min_length=1)
    status: Literal["active", "removed"] = "active"
    active_start: datetime | None = None
    active_end: datetime | None = None
    route_ids: list[str] = Field(default_factory=list)
    stop_ids: list[str] = Field(default_factory=list)
    cause: str | None = None
    effect: str | None = None
    header: str | None = None
    description: str | None = None


@dataclass
class Rejected:
    row: Row
    issues: list[Issue]

    def to_dlq(self, feed: str) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "feed": feed,
            "errors": [
                {"check": i.check, "detail": i.detail}
                for i in self.issues
                if i.severity is Severity.ERROR
            ],
            "row": {
                k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in self.row.items()
            },
        }


@dataclass
class QualityReport:
    feed: str
    network: str
    fetched_at: datetime
    feed_ts: datetime | None
    n_in: int = 0
    n_valid: int = 0
    n_rejected: int = 0
    errors: Counter = field(default_factory=Counter)
    warnings: Counter = field(default_factory=Counter)

    @property
    def feed_lag_s(self) -> float | None:
        if self.feed_ts is None:
            return None
        return (self.fetched_at - self.feed_ts).total_seconds()

    def add(self, issues: list[Issue]) -> None:
        for i in issues:
            (self.errors if i.severity is Severity.ERROR else self.warnings)[i.check] += 1

    def to_event(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "feed": self.feed,
            "network": self.network,
            "fetched_at": self.fetched_at.isoformat(),
            "feed_ts": self.feed_ts.isoformat() if self.feed_ts else None,
            "feed_lag_s": self.feed_lag_s,
            "n_in": self.n_in,
            "n_valid": self.n_valid,
            "n_rejected": self.n_rejected,
            "errors": dict(self.errors),
            "warnings": dict(self.warnings),
        }


@dataclass
class ValidationResult:
    valid: list[_Event]
    rejected: list[Rejected]
    report: QualityReport


def check_snapshot(
    feed_ts: datetime | None, fetched_at: datetime
) -> tuple[datetime | None, list[Issue]]:
    """Reglas sobre la cabecera del snapshot. Devuelve el timestamp (corregido si procede).

    El feed de posiciones AV/LD de Renfe publica la cabecera exactamente 1 h atrasada
    (fallo de zona horaria) aunque las posiciones se actualizan cada 30 s. Un desfase de
    1 o 2 h exactas (+/- unos segundos) se corrige y se marca, en vez de dar el feed por
    caducado.
    """
    if feed_ts is None:
        return None, [Issue("timestamp_nulo", Severity.ERROR, "cabecera sin timestamp")]
    if feed_ts - fetched_at > FUTURE_TOLERANCE:
        return feed_ts, [Issue("timestamp_futuro", Severity.ERROR, feed_ts.isoformat())]
    lag = (fetched_at - feed_ts).total_seconds()
    for hours in (1, 2):
        residual = lag - hours * 3600
        if -30 <= residual <= HOUR_SHIFT_TOLERANCE_S:
            fixed = feed_ts + timedelta(hours=hours)
            return fixed, [Issue("cabecera_desfase_horario", Severity.WARNING, f"+{hours} h")]
    if lag > STALE_AFTER.total_seconds():
        return feed_ts, [Issue("feed_desactualizado", Severity.WARNING, f"{int(lag)} s")]
    return feed_ts, []


def _schema_issue(exc: ValidationError) -> Issue:
    first = exc.errors()[0]
    where = ".".join(str(p) for p in first["loc"])
    return Issue("esquema", Severity.ERROR, f"{where}: {first['msg']}")


def _finish(rows, feed, network, feed_ts, fetched_at, snap_issues, build) -> ValidationResult:
    report = QualityReport(feed, network, fetched_at, feed_ts, n_in=len(rows))
    snap_error = any(i.severity is Severity.ERROR for i in snap_issues)
    valid, rejected = [], []
    for row in rows:
        issues = list(snap_issues)
        event = None
        if not snap_error:
            event, row_issues = build(row)
            issues += row_issues
        report.add(issues)
        if event is None or any(i.severity is Severity.ERROR for i in issues):
            rejected.append(Rejected(row, issues))
        else:
            flags = [i.check for i in issues if i.severity is Severity.WARNING]
            valid.append(event.model_copy(update={"flags": flags}) if flags else event)
    report.n_valid, report.n_rejected = len(valid), len(rejected)
    return ValidationResult(valid, rejected, report)


def validate_trip_updates(
    rows: list[Row], feed: str, network: str, feed_ts: datetime | None, fetched_at: datetime
) -> ValidationResult:
    feed_ts, snap_issues = check_snapshot(feed_ts, fetched_at)
    seen: set[tuple] = set()
    epoch = int(feed_ts.timestamp()) if feed_ts else 0

    def build(row: Row):
        issues: list[Issue] = []
        key = (row.get("trip_id"), row.get("stop_id"))
        if key in seen:
            return None, [Issue("duplicado", Severity.ERROR, f"{key}")]
        seen.add(key)
        raw = row.get("arrival_delay_s")
        if raw is None:
            raw = row.get("departure_delay_s")
        if raw is None:
            raw = row.get("trip_delay_s")
        if row.get("schedule_relationship") == "SCHEDULED":
            if not row.get("stop_id"):
                issues.append(Issue("parada_nula", Severity.ERROR))
            if raw is None:
                issues.append(Issue("retraso_nulo", Severity.ERROR))
        delay = normalize_delay(raw)
        if raw is not None and delay != raw:
            issues.append(Issue("retraso_corregido_24h", Severity.WARNING, f"{raw} -> {delay}"))
        if delay is not None and delay > EXTREME_DELAY_S:
            issues.append(Issue("retraso_extremo", Severity.WARNING, f"{delay} s"))
        if delay is not None and delay < EXTREME_EARLY_S:
            issues.append(Issue("adelanto_extremo", Severity.WARNING, f"{delay} s"))
        try:
            event = TripUpdateEvent(
                event_id=f"{network}:{row.get('trip_id')}:{row.get('stop_id')}:{epoch}",
                network=network,
                feed_ts=feed_ts,
                fetched_at=fetched_at,
                trip_id=row.get("trip_id") or "",
                schedule_relationship=row.get("schedule_relationship"),
                stop_id=row.get("stop_id"),
                arrival_time=row.get("arrival_time"),
                raw_delay_s=raw,
                delay_s=delay,
            )
        except ValidationError as exc:
            return None, [*issues, _schema_issue(exc)]
        return event, issues

    return _finish(rows, feed, network, feed_ts, fetched_at, snap_issues, build)


def validate_vehicle_positions(
    rows: list[Row], feed: str, network: str, feed_ts: datetime | None, fetched_at: datetime
) -> ValidationResult:
    feed_ts, snap_issues = check_snapshot(feed_ts, fetched_at)
    seen: set[str] = set()
    epoch = int(feed_ts.timestamp()) if feed_ts else 0

    def build(row: Row):
        issues: list[Issue] = []
        # Un mismo tren físico puede cubrir dos viajes a la vez (composiciones que se dividen
        # en AV/LD): el duplicado es mismo vehículo *y* mismo viaje.
        ident = f"{row.get('vehicle_id') or row.get('entity_id')}|{row.get('trip_id')}"
        if ident in seen:
            return None, [Issue("duplicado", Severity.ERROR, ident)]
        seen.add(ident)
        lat, lon = row.get("lat"), row.get("lon")
        if lat is None or lon is None:
            return None, [Issue("posicion_nula", Severity.ERROR)]
        (la0, la1), (lo0, lo1) = SPAIN_BBOX["lat"], SPAIN_BBOX["lon"]
        if not (la0 <= lat <= la1 and lo0 <= lon <= lo1):
            issues.append(Issue("fuera_de_espana", Severity.ERROR, f"{lat},{lon}"))
        vts = row.get("vehicle_ts")
        if vts is not None and vts - fetched_at > FUTURE_TOLERANCE:
            issues.append(Issue("timestamp_futuro", Severity.ERROR, vts.isoformat()))
        try:
            event = VehiclePositionEvent(
                event_id=f"{network}:{ident}:{epoch}",
                network=network,
                feed_ts=feed_ts,
                fetched_at=fetched_at,
                **{
                    k: row.get(k)
                    for k in (
                        "trip_id",
                        "vehicle_id",
                        "vehicle_label",
                        "lat",
                        "lon",
                        "current_status",
                        "stop_id",
                        "vehicle_ts",
                    )
                },
            )
        except ValidationError as exc:
            return None, [*issues, _schema_issue(exc)]
        return event, issues

    return _finish(rows, feed, network, feed_ts, fetched_at, snap_issues, build)


def validate_alerts(
    rows: list[Row], feed: str, network: str, feed_ts: datetime | None, fetched_at: datetime
) -> ValidationResult:
    feed_ts, snap_issues = check_snapshot(feed_ts, fetched_at)
    seen: set[str] = set()

    def build(row: Row):
        issues: list[Issue] = []
        aid = row.get("alert_id")
        if aid in seen:
            return None, [Issue("duplicado", Severity.ERROR, str(aid))]
        seen.add(aid)
        if not (row.get("header") or row.get("description")):
            issues.append(Issue("aviso_sin_texto", Severity.WARNING))
        if not (row.get("route_ids") or row.get("stop_ids")):
            issues.append(Issue("aviso_sin_entidad", Severity.WARNING))
        try:
            event = AlertEvent(
                event_id=f"{network}:{aid}:{row.get('content_sha', '')}",
                network=network,
                feed_ts=feed_ts,
                fetched_at=fetched_at,
                alert_id=aid or "",
                **{
                    k: row.get(k)
                    for k in (
                        "active_start",
                        "active_end",
                        "cause",
                        "effect",
                        "header",
                        "description",
                    )
                },
                route_ids=row.get("route_ids") or [],
                stop_ids=row.get("stop_ids") or [],
            )
        except ValidationError as exc:
            return None, [*issues, _schema_issue(exc)]
        return event, issues

    return _finish(rows, feed, network, feed_ts, fetched_at, snap_issues, build)
