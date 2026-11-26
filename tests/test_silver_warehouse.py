from datetime import UTC, datetime, timedelta

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from google.transit import gtfs_realtime_pb2 as rt

from pulso.bronze import BronzeWriter, Snapshot
from pulso.config import Settings
from pulso.silver import build_silver, pending_bronze
from pulso.warehouse import NORMALIZE_DELAY, connect, join_report

T0 = datetime(2026, 11, 26, 9, 0, tzinfo=UTC)


def _settings(tmp_path):
    return Settings(
        data_dir=tmp_path,
        poll_interval_s=30,
        flush_interval_s=600,
        http_timeout_s=5,
        user_agent="test",
    )


def _feed(trips: list[tuple[str, int | None, str]]) -> bytes:
    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = int(T0.timestamp())
    for trip_id, delay, rel in trips:
        e = msg.entity.add(id=f"TU_{trip_id}")
        e.trip_update.trip.trip_id = trip_id
        e.trip_update.trip.schedule_relationship = rt.TripDescriptor.ScheduleRelationship.Value(rel)
        if delay is not None:
            stu = e.trip_update.stop_time_update.add(stop_id="17000")
            stu.arrival.delay = delay
            stu.arrival.time = int(T0.timestamp()) + 600
    return msg.SerializeToString()


def _write_static(root, network, version, routes, trips, service_dates, stops):
    d = root / "static" / network / f"v={version}"
    d.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(routes), d / "routes.parquet")
    pq.write_table(pa.Table.from_pylist(trips), d / "trips.parquet")
    pq.write_table(pa.Table.from_pylist(service_dates), d / "service_dates.parquet")
    pq.write_table(pa.Table.from_pylist(stops), d / "stops.parquet")


def test_normalize_delay_fixes_midnight_wrap():
    con = duckdb.connect()
    cases = {-86340: 60, 87254: 854, 0: 0, -300: -300, 30840: 30840, 86400: 0}
    for raw, expected in cases.items():
        got = con.execute(f"select {NORMALIZE_DELAY.format(x=raw)}").fetchone()[0]
        assert got == expected, raw


def test_silver_is_incremental_and_dedupes_alerts(tmp_path, fixture_bytes):
    s = _settings(tmp_path)
    w = BronzeWriter(tmp_path)
    alerts = fixture_bytes("alerts")
    for i in range(3):  # el mismo mensaje de avisos 3 veces
        w.add(Snapshot("cercanias_alerts", T0 + timedelta(seconds=30 * i), T0, 80, alerts))
    w.add(Snapshot("cercanias_trip_updates", T0, T0, 95, fixture_bytes("trip_updates")))
    w.flush()

    assert len(pending_bronze(s)) == 2
    written = build_silver(s)
    assert written == {"alerts": 78, "trip_updates": 95}
    assert pending_bronze(s) == []
    assert build_silver(s) == {}  # segunda pasada: nada que hacer

    con = connect(s)
    n_seen = con.execute("select min(n_seen), max(n_seen) from rt_alerts").fetchone()
    assert n_seen == (3, 3)


def test_trip_delays_join_exact_and_ld_by_train_and_date(tmp_path):
    s = _settings(tmp_path)
    w = BronzeWriter(tmp_path)
    w.add(
        Snapshot(
            "cercanias_trip_updates",
            T0,
            T0,
            2,
            _feed([("1030V1C1", -86340, "SCHEDULED"), ("NOEXISTE", 120, "SCHEDULED")]),
        )
    )
    # El tren 00190 del día 28 cruza con el viaje estático "0019012026-11-26" (que circula
    # del 26 al 28) aunque su trip_id no coincide.
    w.add(Snapshot("ld_trip_updates", T0, T0, 1, _feed([("0019012026-11-28", 300, "SCHEDULED")])))
    w.flush()
    build_silver(s)

    _write_static(
        tmp_path,
        "cercanias",
        "20261124T000000",
        routes=[
            {
                "route_id": "10T0001C1",
                "line": "C1",
                "route_long_name": "x",
                "route_color": "75B6E0",
                "area": "Madrid",
            }
        ],
        trips=[
            {
                "trip_id": "1030V1C1",
                "route_id": "10T0001C1",
                "service_id": "1030V",
                "headsign": None,
            }
        ],
        service_dates=[{"service_id": "1030V", "service_date": datetime(2026, 11, 26).date()}],
        stops=[{"stop_id": "17000", "stop_name": "Chamartín", "lat": 40.47, "lon": -3.68}],
    )
    _write_static(
        tmp_path,
        "ld",
        "20261126T000000",
        routes=[
            {
                "route_id": "R_AVE",
                "line": "AVE",
                "route_long_name": None,
                "route_color": None,
                "area": "Alta velocidad / Larga y media distancia",
            }
        ],
        trips=[
            {
                "trip_id": "0019012026-11-26",
                "route_id": "R_AVE",
                "service_id": "S1",
                "headsign": None,
            }
        ],
        service_dates=[
            {"service_id": "S1", "service_date": datetime(2026, 11, d).date()} for d in (26, 27, 28)
        ],
        stops=[{"stop_id": "17000", "stop_name": "Chamartín", "lat": 40.47, "lon": -3.68}],
    )

    con = connect(s)
    rows = {
        r[0]: r[1:]
        for r in con.execute(
            "select trip_id, line, delay_s, delay_wrapped, trip_match from trip_delays"
        ).fetchall()
    }
    assert rows["1030V1C1"] == ("C1", 60, True, "exacto")
    assert rows["NOEXISTE"] == (None, 120, False, None)
    assert rows["0019012026-11-28"] == ("AVE", 300, False, "tren_y_fecha")

    report = {r[0]: r for r in join_report(con)}
    assert report["cercanias"][1:4] == (2, 1, 50.0)
    assert report["ld"][1:4] == (1, 1, 100.0)
    assert report["cercanias"][6] == 1  # un retraso corregido de ±24 h
