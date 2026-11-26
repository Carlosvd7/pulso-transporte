"""Procesador: consume Redpanda, aplica StreamProcessor y escribe las salidas.

Salidas:
- gold/kpi_line_5min, gold/kpi_stop_5min, gold/quality_5min, gold/anomalies (Parquet, un
  fichero por ventana: reescribirlo es idempotente).
- live/state.json: foto del estado actual para el panel (cada 10 s, escritura atómica).
- Temas pulso.kpis.v1 y pulso.anomalies.v1.

Arranque sin estado guardado: al iniciar relee los últimos N minutos de los temas
(rehidratación) y recupera la historia de KPIs de gold, así que un reinicio no pierde
ni el estado en vivo ni la línea base de las anomalías.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from pulso.bus import (
    TOPIC_ALERTS,
    TOPIC_ANOMALIES,
    TOPIC_KPIS,
    TOPIC_QUALITY,
    TOPIC_TRIP_UPDATES,
    TOPIC_VEHICLE_POSITIONS,
    Publisher,
)
from pulso.config import Settings
from pulso.lookup import StaticLookup
from pulso.stream import WINDOW, Output, StreamProcessor, parse_ts

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

log = logging.getLogger(__name__)

INPUT_TOPICS = (TOPIC_TRIP_UPDATES, TOPIC_VEHICLE_POSITIONS, TOPIC_ALERTS, TOPIC_QUALITY)
LIVE_EVERY_S = 10
RELOAD_STATIC_EVERY_S = 600
SPARK_WINDOWS = 24


def _jsonable(v: Any) -> Any:
    return json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v


class GoldWriter:
    def __init__(self, root: Path) -> None:
        self.root = root / "gold"

    def _write(self, table: str, w: datetime, rows: list[dict], suffix: str = "") -> None:
        if not rows:
            return
        d = self.root / table / f"date={w:%Y-%m-%d}"
        d.mkdir(parents=True, exist_ok=True)
        path = d / f"{w:%H%M}{suffix}.parquet"
        tbl = pa.Table.from_pylist([{k: _jsonable(v) for k, v in r.items()} for r in rows])
        tmp = path.with_suffix(".parquet.tmp")
        pq.write_table(tbl, tmp, compression="zstd")
        tmp.replace(path)

    def write(self, out: Output) -> None:
        for w in out.closed_windows:
            self._write("kpi_line_5min", w, [r for r in out.kpis if r["window_start"] == w])
            self._write("kpi_stop_5min", w, [r for r in out.stops if r["window_start"] == w])
            self._write("quality_5min", w, [r for r in out.quality if r["window_start"] == w])
        if out.anomalies and out.closed_windows:
            w = out.closed_windows[-1]
            cols = (
                "anomaly_id",
                "type",
                "status",
                "network",
                "area",
                "line",
                "trip_id",
                "started_at",
                "last_seen",
                "resolved_at",
                "value",
                "baseline",
                "peak",
                "message",
            )
            rows = [{c: a.get(c) for c in cols} for a in out.anomalies]
            self._write("anomalies", w, rows)

    def recent_kpis(self, since: datetime) -> list[dict]:
        import duckdb

        pattern = (self.root / "kpi_line_5min" / "date=*" / "*.parquet").as_posix()
        if not any((self.root / "kpi_line_5min").glob("date=*/*.parquet")):
            return []
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'")
        cur = con.execute(
            f"select * exclude (date) from read_parquet('{pattern}', hive_partitioning = true)"
            " where window_start >= ? order by window_start",
            [since],
        )
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def write_live(data_dir: Path, state: dict) -> Path:
    d = data_dir / "live"
    d.mkdir(parents=True, exist_ok=True)
    path = d / "state.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, default=str), encoding="utf-8")
    tmp.replace(path)
    return path


class ProcessorApp:
    """Une StreamProcessor con sus salidas. Lo usan tanto el modo Kafka como el modo ligero."""

    def __init__(
        self,
        settings: Settings,
        publisher: Publisher | None = None,
        lookup: StaticLookup | None = None,
    ) -> None:
        self.settings = settings
        self.publisher = publisher
        self.lookup = lookup or StaticLookup(settings)
        self.proc = StreamProcessor(self.lookup)
        self.gold = GoldWriter(settings.data_dir)
        self.recent: deque[dict] = deque()
        self._last_live = 0.0
        self._last_reload = time.monotonic()
        self._preload_history()

    def _preload_history(self) -> None:
        since = datetime.now(UTC) - WINDOW * 48
        rows = self.gold.recent_kpis(since)
        for r in rows:
            if r["mean_delay_s"] is not None and r["n_trains"] > 0:
                self.proc.history[(r["network"], r["area"], r["line"])].append(r["mean_delay_s"])
        self.recent.extend(
            r for r in rows if r["window_start"] >= datetime.now(UTC) - WINDOW * SPARK_WINDOWS
        )
        if rows:
            log.info("historia de KPIs recuperada de gold: %d filas", len(rows))

    def handle(self, topic: str, value: dict, source: str) -> None:
        if topic == TOPIC_TRIP_UPDATES:
            self.proc.on_trip_update(value, source)
        elif topic == TOPIC_VEHICLE_POSITIONS:
            self.proc.on_vehicle(value, source)
        elif topic == TOPIC_ALERTS:
            self.proc.on_alert(value)
        elif topic == TOPIC_QUALITY:
            self.proc.on_quality(value)

    def after_batch(self, force_live: bool = False) -> Output:
        out = self.proc.drain()
        if out.closed_windows:
            self.gold.write(out)
            self.recent.extend(out.kpis)
            if self.recent:
                newest = max(r["window_start"] for r in self.recent)
                while (
                    self.recent
                    and self.recent[0]["window_start"] <= newest - WINDOW * SPARK_WINDOWS
                ):
                    self.recent.popleft()
            if self.publisher is not None:
                for r in out.kpis:
                    self.publisher.publish(TOPIC_KPIS, f"{r['network']}|{r['area']}|{r['line']}", r)
                for a in out.anomalies:
                    self.publisher.publish(TOPIC_ANOMALIES, a["anomaly_id"], a)
            for a in out.anomalies:
                log.info("anomalía %s: %s", a["status"], a["message"])
        now = time.monotonic()
        if force_live or now - self._last_live >= LIVE_EVERY_S:
            write_live(self.settings.data_dir, self.proc.live_state(list(self.recent)))
            self._last_live = now
        if now - self._last_reload >= RELOAD_STATIC_EVERY_S:
            self._last_reload = now
            if self.lookup.maybe_reload():
                log.info("GTFS estático recargado")
        return out


def run_processor(settings: Settings, rehydrate_minutes: float = 30) -> None:
    from confluent_kafka import OFFSET_END, Consumer, TopicPartition

    from pulso.bus import KafkaPublisher

    app = ProcessorApp(settings, KafkaPublisher(settings.kafka_bootstrap, "pulso-processor"))
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "pulso-processor",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    since_ms = int((time.time() - rehydrate_minutes * 60) * 1000)
    parts = []
    for topic in INPUT_TOPICS:
        meta = consumer.list_topics(topic, timeout=30).topics[topic]
        parts += [TopicPartition(topic, p, since_ms) for p in meta.partitions]
    offsets = consumer.offsets_for_times(parts, timeout=30)
    for o in offsets:
        if o.offset < 0:
            o.offset = OFFSET_END
    consumer.assign(offsets)
    ends = {}
    for o in offsets:
        _, high = consumer.get_watermark_offsets(o, timeout=30)
        start = high if o.offset == OFFSET_END else o.offset
        if start < high:
            ends[(o.topic, o.partition)] = high
    log.info(
        "procesador iniciado: rehidratando los últimos %.0f min de %s",
        rehydrate_minutes,
        ", ".join(INPUT_TOPICS),
    )

    stop = False

    def _stop(*_: object) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    def _decode(msg) -> dict | None:
        if msg.error():
            log.warning("error de consumo: %s", msg.error())
            return None
        try:
            return json.loads(msg.value())
        except ValueError:
            log.exception("evento inválido en %s@%d", msg.topic(), msg.offset())
            return None

    # 1) Rehidratación: se lee todo lo pendiente hasta el final actual de cada partición y
    #    se procesa ordenado por tiempo del evento. Kafka entrega las particiones a ráfagas;
    #    sin ordenar, una partición adelantada haría "tardíos" los eventos de las demás.
    backlog: list[tuple[datetime, str, str, dict]] = []
    pending = dict(ends)
    while pending and not stop:
        for msg in consumer.consume(1000, timeout=2.0):
            value = _decode(msg)
            if value is not None:
                ts = parse_ts(value.get("feed_ts") or value.get("fetched_at")) or _EPOCH
                backlog.append((ts, msg.topic(), f"{msg.topic()}:{msg.partition()}", value))
            key = (msg.topic(), msg.partition())
            if key in pending and msg.offset() + 1 >= pending[key]:
                del pending[key]
    backlog.sort(key=lambda x: x[0])
    for _, topic, source, value in backlog:
        app.handle(topic, value, source)
    app.after_batch(force_live=True)
    log.info("rehidratación: %d eventos | %s", len(backlog), dict(app.proc.counters))
    n = len(backlog)
    del backlog

    # 2) En vivo.
    while not stop:
        for msg in consumer.consume(500, timeout=1.0):
            value = _decode(msg)
            if value is not None:
                try:
                    app.handle(msg.topic(), value, f"{msg.topic()}:{msg.partition()}")
                    n += 1
                except Exception:
                    log.exception("fallo procesando %s@%d", msg.topic(), msg.offset())
        app.after_batch()
    consumer.close()
    app.publisher.flush(10)
    log.info("procesador detenido tras %d eventos | %s", n, dict(app.proc.counters))


def replay_offline(settings: Settings, since=None, until=None) -> ProcessorApp:
    """Procesa el histórico de bronze sin Kafka (mismo código, bus en memoria)."""
    from pulso.bus import MemoryBus
    from pulso.ingest import Ingestor
    from pulso.producer import iter_bronze

    app = ProcessorApp(settings, None)
    bus = MemoryBus()
    ingestor = Ingestor(bus)
    for i, (feed, snap) in enumerate(iter_bronze(settings, since, until)):
        ingestor.handle(feed, snap)
        for topic, msgs in bus.messages.items():
            for _key, value in msgs:
                app.handle(topic, value, f"{topic}:0")
        bus.messages.clear()
        if i % 50 == 0:
            app.after_batch()
    app.proc.flush_all()
    app.after_batch(force_live=True)
    return app
