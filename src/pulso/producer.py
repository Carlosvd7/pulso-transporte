"""Productor: recolector + ingesta al bus, y re-play del histórico bronze.

`run_producer`: consulta Renfe cada 30 s, archiva el crudo (bronze), valida y publica.
`replay`: vuelve a publicar snapshots ya archivados, en orden temporal. Sirve para
reconstruir el estado tras un fallo, probar el procesador con datos reales de cualquier
hora del día o hacer una demo de noche, cuando apenas circulan trenes.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from datetime import date, datetime

import duckdb

from pulso.bronze import Snapshot
from pulso.bus import Publisher
from pulso.collector import Collector
from pulso.config import FEEDS, FEEDS_BY_KEY, Feed, Settings
from pulso.ingest import Ingestor

log = logging.getLogger(__name__)

STATIC_REFRESH_S = 6 * 3600


def _static_refresher(settings: Settings, stop: threading.Event) -> None:
    from pulso.static_gtfs import refresh

    while not stop.is_set():
        try:
            refresh(settings)
        except Exception:
            log.exception("fallo al refrescar el GTFS estático (se reintentará)")
        stop.wait(STATIC_REFRESH_S)


def run_producer(
    settings: Settings,
    publisher: Publisher,
    feeds: tuple[Feed, ...] = FEEDS,
    max_cycles: int | None = None,
    refresh_static: bool = True,
) -> Ingestor:
    ingestor = Ingestor(publisher)
    stop = threading.Event()
    if refresh_static:
        threading.Thread(target=_static_refresher, args=(settings, stop), daemon=True).start()
    try:
        Collector(settings, feeds, on_snapshot=ingestor.handle).run(max_cycles=max_cycles)
    finally:
        stop.set()
        pending = publisher.flush(30)
        log.info(
            "productor detenido | publicados %s | pendientes %d", dict(ingestor.published), pending
        )
    return ingestor


def iter_bronze(
    settings: Settings,
    since: date | None = None,
    until: date | None = None,
    feeds: tuple[str, ...] | None = None,
) -> Iterator[tuple[Feed, Snapshot]]:
    """Snapshots archivados en orden de llegada (todas las fuentes intercaladas)."""
    pattern = (settings.data_dir / "bronze" / "feed=*" / "date=*" / "*.parquet").as_posix()
    where, params = ["true"], []
    # date -> filtra por partición (rápido); datetime -> filtra por hora exacta de llegada.
    if since:
        where.append("fetched_at >= ?" if isinstance(since, datetime) else "date >= ?")
        params.append(since)
    if until:
        where.append("fetched_at <= ?" if isinstance(until, datetime) else "date <= ?")
        params.append(until)
    if feeds:
        where.append(f"feed in ({', '.join('?' for _ in feeds)})")
        params.extend(feeds)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    cur = con.execute(
        f"""select feed, fetched_at, header_ts, n_entities, sha256, payload
            from read_parquet('{pattern}', hive_partitioning = true,
                              hive_types = {{'feed': varchar, 'date': date}})
            where {" and ".join(where)} order by fetched_at, feed""",
        params,
    )
    while batch := cur.fetchmany(500):
        for feed, fetched_at, header_ts, n, sha, payload in batch:
            yield FEEDS_BY_KEY[feed], Snapshot(feed, fetched_at, header_ts, n, payload, sha)


def replay(
    settings: Settings,
    publisher: Publisher,
    since: date | None = None,
    until: date | None = None,
    speed: float = 0.0,
) -> Ingestor:
    """Re-publica el histórico. speed=0: lo más rápido posible; speed=60: 1 h en 1 min."""
    ingestor = Ingestor(publisher)
    first_ts = started = None
    n = 0
    for feed, snap in iter_bronze(settings, since, until):
        if speed > 0:
            if first_ts is None:
                first_ts, started = snap.fetched_at, time.monotonic()
            target = (snap.fetched_at - first_ts).total_seconds() / speed
            wait = target - (time.monotonic() - started)
            if wait > 0:
                time.sleep(wait)
        ingestor.handle(feed, snap)
        n += 1
        if n % 1000 == 0:
            log.info("replay: %d snapshots", n)
    publisher.flush(60)
    log.info("replay terminado: %d snapshots | publicados %s", n, dict(ingestor.published))
    return ingestor
