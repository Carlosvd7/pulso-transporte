"""Capa bronze: guarda los mensajes crudos tal cual llegan (protobuf) en Parquet.

Guardar el crudo permite reprocesar todo el histórico (re-play hacia Redpanda,
nuevas validaciones, nuevas features) sin depender de que Renfe lo conserve.

Estructura: <data_dir>/bronze/feed=<key>/date=<YYYY-MM-DD>/part-<HHMMSS>-<n>.parquet
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

BRONZE_SCHEMA = pa.schema(
    [
        pa.field("feed", pa.string(), nullable=False),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("header_ts", pa.timestamp("us", tz="UTC")),
        pa.field("n_entities", pa.int32(), nullable=False),
        pa.field("sha256", pa.string(), nullable=False),
        pa.field("payload", pa.binary(), nullable=False),
    ]
)


@dataclass
class Snapshot:
    feed: str
    fetched_at: datetime
    header_ts: datetime | None
    n_entities: int
    payload: bytes
    sha256: str = ""

    def __post_init__(self) -> None:
        if not self.sha256:
            self.sha256 = hashlib.sha256(self.payload).hexdigest()


@dataclass
class BronzeWriter:
    """Acumula snapshots en memoria y los vuelca a Parquet por feed y día (UTC)."""

    root: Path
    buffers: dict[str, list[Snapshot]] = field(default_factory=dict)
    _seq: int = 0

    def add(self, snap: Snapshot) -> None:
        buf = self.buffers.setdefault(snap.feed, [])
        # Cambio de día UTC: se vuelca lo anterior para no mezclar particiones.
        if buf and buf[-1].fetched_at.date() != snap.fetched_at.date():
            self._flush_feed(snap.feed)
            buf = self.buffers.setdefault(snap.feed, [])
        buf.append(snap)

    def pending(self) -> int:
        return sum(len(b) for b in self.buffers.values())

    def flush(self) -> list[Path]:
        written = []
        for feed in list(self.buffers):
            path = self._flush_feed(feed)
            if path:
                written.append(path)
        return written

    def _flush_feed(self, feed: str) -> Path | None:
        snaps = self.buffers.pop(feed, [])
        if not snaps:
            return None
        first = snaps[0].fetched_at.astimezone(UTC)
        part_dir = self.root / "bronze" / f"feed={feed}" / f"date={first:%Y-%m-%d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        self._seq += 1
        path = part_dir / f"part-{first:%H%M%S}-{self._seq:04d}.parquet"
        table = pa.Table.from_pylist(
            [
                {
                    "feed": s.feed,
                    "fetched_at": s.fetched_at,
                    "header_ts": s.header_ts,
                    "n_entities": s.n_entities,
                    "sha256": s.sha256,
                    "payload": s.payload,
                }
                for s in snaps
            ],
            schema=BRONZE_SCHEMA,
        )
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression="zstd")
        tmp.replace(path)  # escritura atómica: nunca queda un Parquet a medias
        return path
