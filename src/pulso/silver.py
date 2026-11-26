"""Capa silver: decodifica los ficheros bronze en tablas Parquet tipadas.

- Incremental: cada fichero bronze (inmutable) se procesa una sola vez; se anota en
  <data_dir>/silver/_processed.txt.
- trip_updates y vehicle_positions: una fila por observación (snapshot x entidad).
- alerts: los avisos se repiten en cada snapshot; se guarda una fila por aviso distinto
  (id + contenido) con first_seen / last_seen / n_seen dentro del fichero.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pulso.config import FEEDS_BY_KEY, FeedKind, Settings
from pulso.parse import (
    Row,
    alert_content_sha,
    alert_rows,
    decode,
    trip_update_rows,
    vehicle_position_rows,
)

log = logging.getLogger(__name__)

TS = pa.timestamp("us", tz="UTC")

TRIP_UPDATES_SCHEMA = pa.schema(
    [
        ("network", pa.string()),
        ("fetched_at", TS),
        ("feed_ts", TS),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("route_id", pa.string()),
        ("schedule_relationship", pa.string()),
        ("trip_delay_s", pa.int32()),
        ("trip_ts", TS),
        ("stop_id", pa.string()),
        ("stop_sequence", pa.int32()),
        ("arrival_time", TS),
        ("arrival_delay_s", pa.int32()),
        ("departure_time", TS),
        ("departure_delay_s", pa.int32()),
    ]
)

VEHICLE_POSITIONS_SCHEMA = pa.schema(
    [
        ("network", pa.string()),
        ("fetched_at", TS),
        ("feed_ts", TS),
        ("entity_id", pa.string()),
        ("trip_id", pa.string()),
        ("vehicle_id", pa.string()),
        ("vehicle_label", pa.string()),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
        ("current_status", pa.string()),
        ("stop_id", pa.string()),
        ("vehicle_ts", TS),
    ]
)

ALERTS_SCHEMA = pa.schema(
    [
        ("network", pa.string()),
        ("alert_id", pa.string()),
        ("content_sha", pa.string()),
        ("first_seen", TS),
        ("last_seen", TS),
        ("n_seen", pa.int32()),
        ("feed_ts", TS),
        ("active_start", TS),
        ("active_end", TS),
        ("route_ids", pa.list_(pa.string())),
        ("stop_ids", pa.list_(pa.string())),
        ("cause", pa.string()),
        ("effect", pa.string()),
        ("header", pa.string()),
        ("description", pa.string()),
    ]
)

_PARSERS: dict[FeedKind, Callable] = {
    FeedKind.TRIP_UPDATES: trip_update_rows,
    FeedKind.VEHICLE_POSITIONS: vehicle_position_rows,
    FeedKind.ALERTS: alert_rows,
}
_SCHEMAS = {
    FeedKind.TRIP_UPDATES: TRIP_UPDATES_SCHEMA,
    FeedKind.VEHICLE_POSITIONS: VEHICLE_POSITIONS_SCHEMA,
    FeedKind.ALERTS: ALERTS_SCHEMA,
}


def _dedupe_alerts(rows_by_snapshot: list[tuple[object, list[Row]]]) -> list[Row]:
    out: dict[tuple[str, str], Row] = {}
    for fetched_at, rows in rows_by_snapshot:
        for r in rows:
            key = (r["alert_id"], alert_content_sha(r))
            if key in out:
                out[key]["last_seen"] = fetched_at
                out[key]["n_seen"] += 1
            else:
                out[key] = {
                    **r,
                    "content_sha": key[1],
                    "first_seen": fetched_at,
                    "last_seen": fetched_at,
                    "n_seen": 1,
                }
    return list(out.values())


def bronze_to_rows(bronze_file: Path) -> tuple[FeedKind, list[Row]]:
    """Decodifica todos los snapshots de un fichero bronze."""
    table = pq.read_table(bronze_file, columns=["feed", "fetched_at", "payload"])
    feed_key = table.column("feed")[0].as_py()
    feed = FEEDS_BY_KEY[feed_key]
    parser = _PARSERS[feed.kind]
    per_snapshot = []
    for fetched_at, payload in zip(
        table.column("fetched_at").to_pylist(), table.column("payload").to_pylist(), strict=True
    ):
        per_snapshot.append((fetched_at, parser(decode(payload), feed.network)))
    if feed.kind is FeedKind.ALERTS:
        return feed.kind, _dedupe_alerts(per_snapshot)
    rows = [{**r, "fetched_at": ts} for ts, rs in per_snapshot for r in rs]
    return feed.kind, rows


def _processed_log(settings: Settings) -> Path:
    return settings.data_dir / "silver" / "_processed.txt"


def pending_bronze(settings: Settings) -> list[Path]:
    log_path = _processed_log(settings)
    done = set(log_path.read_text().splitlines()) if log_path.exists() else set()
    files = sorted((settings.data_dir / "bronze").glob("feed=*/date=*/*.parquet"))
    return [f for f in files if f.relative_to(settings.data_dir).as_posix() not in done]


def build_silver(settings: Settings, limit: int | None = None) -> dict[str, int]:
    """Procesa los ficheros bronze pendientes. Devuelve filas escritas por tabla."""
    written: dict[str, int] = {}
    files = pending_bronze(settings)[:limit]
    log_path = _processed_log(settings)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    for f in files:
        kind, rows = bronze_to_rows(f)
        network = FEEDS_BY_KEY[f.parent.parent.name.removeprefix("feed=")].network
        date_part = f.parent.name  # date=YYYY-MM-DD
        out_dir = settings.data_dir / "silver" / kind.value / f"network={network}" / date_part
        out_dir.mkdir(parents=True, exist_ok=True)
        if rows:
            table = pa.Table.from_pylist(rows, schema=_SCHEMAS[kind])
            out = out_dir / f.name
            tmp = out.with_suffix(".parquet.tmp")
            pq.write_table(table, tmp, compression="zstd")
            tmp.replace(out)
        written[kind.value] = written.get(kind.value, 0) + len(rows)
        with log_path.open("a") as fh:
            fh.write(f.relative_to(settings.data_dir).as_posix() + "\n")
    if files:
        log.info("silver: %d ficheros bronze procesados -> %s", len(files), written)
    return written
