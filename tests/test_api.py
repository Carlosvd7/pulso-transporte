from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from pulso.api import create_app
from pulso.config import Settings
from pulso.lookup import StaticLookup, TripInfo
from pulso.processor import GoldWriter, write_live
from pulso.stream import StreamProcessor


def _settings(tmp_path):
    return Settings(
        data_dir=tmp_path,
        poll_interval_s=30,
        flush_interval_s=120,
        http_timeout_s=5,
        user_agent="test",
    )


def _ev(n, trip, delay, ts):
    return {
        "event_id": f"e{n}",
        "network": "cercanias",
        "feed_ts": ts.isoformat(),
        "fetched_at": ts.isoformat(),
        "flags": [],
        "trip_id": trip,
        "schedule_relationship": "SCHEDULED",
        "stop_id": "17000",
        "arrival_time": None,
        "raw_delay_s": delay,
        "delay_s": delay,
    }


def _populate(tmp_path):
    lk = StaticLookup()
    lk.trips[("cercanias", "T1")] = TripInfo("R1", "C1", "Madrid", "75B6E0", None)
    p = StreamProcessor(lk)
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    t0 = now - timedelta(minutes=30)
    n = 0
    for k in range(5):
        for trip, d in (("T1", 120 + k * 60), ("T2", 0), ("T3", 30)):
            n += 1
            p.on_trip_update(_ev(n, trip, d, t0 + timedelta(minutes=5 * k)))
    out = p.drain()
    GoldWriter(tmp_path).write(out)
    write_live(tmp_path, p.live_state(out.kpis))


def test_api_endpoints(tmp_path):
    client = TestClient(create_app(_settings(tmp_path)))
    assert client.get("/api/state").status_code == 503  # aún sin procesador
    assert client.get("/api/kpis").json() == []

    _populate(tmp_path)
    state = client.get("/api/state").json()
    assert state["stale"] is False and len(state["trains"]) == 3
    assert client.get("/api/health").json()["ok"] is True

    kpis = client.get("/api/kpis", params={"hours": 2}).json()
    assert kpis and all(r["area"] == "*" and r["line"] == "*" for r in kpis)
    c1 = client.get("/api/kpis", params={"area": "Madrid", "line": "C1"}).json()
    assert [r["mean_delay_s"] for r in c1][:2] == [120.0, 180.0]

    stops = client.get("/api/stops", params={"hours": 2}).json()
    assert stops[0]["stop_id"] == "17000" and stops[0]["n_obs"] >= 3
    assert client.get("/api/quality").json() == []
    assert client.get("/api/predictions").json()["available"] is False
    assert client.get("/api/kpis", params={"hours": 0}).status_code == 422

    page = client.get("/")
    assert page.status_code == 200 and "Pulso del transporte" in page.text
    assert client.get("/static/app.js").status_code == 200


def test_lite_pipeline_runs_end_to_end_without_kafka(tmp_path, fixture_bytes, monkeypatch):
    """El modo ligero usa el mismo código con un bus en memoria: snapshot -> live/state.json."""
    import httpx

    import pulso.lite as lite
    from pulso.config import Feed, FeedKind

    monkeypatch.setattr("pulso.static_gtfs.refresh", lambda settings: [])
    payload = fixture_bytes("trip_updates")
    feed = Feed("cercanias_trip_updates", "cercanias", FeedKind.TRIP_UPDATES, "https://x/tu.pb")
    real_client = httpx.Client

    def fake_client(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(lambda req: httpx.Response(200, content=payload))
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", fake_client)
    settings = Settings(
        data_dir=tmp_path,
        poll_interval_s=0,
        flush_interval_s=0,
        http_timeout_s=5,
        user_agent="test",
    )
    thread, _ = lite.start_pipeline(settings, (feed,), max_cycles=2)
    thread.join(timeout=30)
    state = TestClient(create_app(settings)).get("/api/state").json()
    assert len(state["trains"]) == 95
    assert state["quality"]["cercanias_trip_updates"]["n_in"] == 95  # 2ª vuelta: sin cambios
    assert list((tmp_path / "bronze").rglob("*.parquet"))
