from datetime import UTC, datetime, timedelta

import httpx
import pyarrow.parquet as pq

from pulso.bronze import BronzeWriter, Snapshot
from pulso.collector import Collector
from pulso.config import Feed, FeedKind, Settings


def _settings(tmp_path):
    return Settings(
        data_dir=tmp_path,
        poll_interval_s=0,
        flush_interval_s=3600,
        http_timeout_s=5,
        user_agent="test",
    )


def test_bronze_roundtrip_and_day_partition(tmp_path, fixture_bytes):
    payload = fixture_bytes("trip_updates")
    w = BronzeWriter(tmp_path)
    t0 = datetime(2026, 11, 25, 23, 59, 50, tzinfo=UTC)
    w.add(Snapshot("f", t0, t0, 95, payload))
    w.add(Snapshot("f", t0 + timedelta(seconds=30), None, 95, payload + b""))  # cambia de día
    w.flush()

    files = sorted(tmp_path.rglob("*.parquet"))
    assert [f.parent.name for f in files] == ["date=2026-11-25", "date=2026-11-26"]
    table = pq.read_table(files[0])
    assert table.num_rows == 1
    assert table.column("payload")[0].as_py() == payload
    assert not list(tmp_path.rglob("*.tmp"))


def _feed():
    return Feed("test_tu", "cercanias", FeedKind.TRIP_UPDATES, "https://example.test/tu.pb")


def test_collector_dedupes_and_survives_errors(tmp_path, fixture_bytes):
    good = fixture_bytes("trip_updates")
    responses = iter(
        [
            httpx.Response(200, content=good),
            httpx.Response(200, content=good),  # mismo contenido -> no se guarda
            httpx.Response(503),  # error HTTP -> se cuenta y sigue
            httpx.Response(200, content=b"\xff\x00basura"),  # protobuf inválido
            httpx.Response(200, content=fixture_bytes("trip_updates_LD")),  # contenido nuevo
        ]
    )
    client = httpx.Client(transport=httpx.MockTransport(lambda req: next(responses)))
    col = Collector(_settings(tmp_path), (_feed(),))
    for _ in range(5):
        col.poll_once(client)
    col.maybe_flush(force=True)

    c = col.stats.counts
    assert c[("test_tu", "saved")] == 2
    assert c[("test_tu", "unchanged")] == 1
    assert c[("test_tu", "errors")] == 2
    table = pq.read_table(next(tmp_path.rglob("*.parquet")))
    assert table.column("n_entities").to_pylist() == [95, 78]
