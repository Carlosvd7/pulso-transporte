from datetime import UTC, datetime, timedelta

from google.transit import gtfs_realtime_pb2 as rt

from pulso.bronze import Snapshot
from pulso.bus import TOPIC_ALERTS, TOPIC_DLQ, TOPIC_QUALITY, TOPIC_TRIP_UPDATES, MemoryBus
from pulso.config import FEEDS_BY_KEY
from pulso.contracts import normalize_delay, validate_trip_updates, validate_vehicle_positions
from pulso.ingest import Ingestor
from pulso.parse import decode, header_timestamp, trip_update_rows

NOW = datetime(2026, 11, 26, 9, 0, tzinfo=UTC)


def _tu(trip_id="T1", stop_id="17000", delay=60, rel="SCHEDULED"):
    return {
        "trip_id": trip_id,
        "stop_id": stop_id,
        "arrival_delay_s": delay,
        "departure_delay_s": None,
        "trip_delay_s": delay,
        "schedule_relationship": rel,
        "arrival_time": NOW,
    }


def _checks(result):
    return dict(result.report.errors), dict(result.report.warnings)


def test_normalize_delay_matches_sql_formula():
    assert normalize_delay(-86340) == 60
    assert normalize_delay(87254) == 854
    assert normalize_delay(None) is None
    assert normalize_delay(-300) == -300


def test_trip_update_rules():
    rows = [
        _tu("A", delay=-86340),  # corregido +60 s (aviso)
        _tu("B", delay=4 * 3600),  # retraso extremo (aviso)
        _tu("C", delay=-20 * 60),  # adelanto extremo (aviso)
        _tu("A", delay=60),  # duplicado de A (error)
        _tu("D", stop_id=None),  # programado sin parada (error)
        _tu("E", stop_id=None, delay=None, rel="CANCELED"),  # cancelado sin parada: válido
    ]
    res = validate_trip_updates(rows, "f", "cercanias", NOW, NOW)
    errors, warnings = _checks(res)
    assert errors == {"duplicado": 1, "parada_nula": 1}
    assert warnings == {"retraso_corregido_24h": 1, "retraso_extremo": 1, "adelanto_extremo": 1}
    by_trip = {e.trip_id: e for e in res.valid}
    assert set(by_trip) == {"A", "B", "C", "E"}
    assert by_trip["A"].delay_s == 60 and by_trip["A"].raw_delay_s == -86340
    assert by_trip["A"].flags == ["retraso_corregido_24h"]
    assert res.report.n_in == 6 and res.report.n_valid == 4 and res.report.n_rejected == 2


def test_snapshot_level_timestamp_rules():
    future = validate_trip_updates([_tu()], "f", "cercanias", NOW + timedelta(minutes=10), NOW)
    assert future.valid == [] and _checks(future)[0] == {"timestamp_futuro": 1}

    stale = validate_trip_updates([_tu()], "f", "cercanias", NOW - timedelta(minutes=20), NOW)
    assert stale.valid[0].flags == ["feed_desactualizado"]
    assert stale.report.feed_lag_s == 1200

    missing = validate_trip_updates([_tu()], "f", "cercanias", None, NOW)
    assert _checks(missing)[0] == {"timestamp_nulo": 1}


def test_vehicle_outside_spain_is_rejected():
    base = {
        "entity_id": "e",
        "trip_id": "T",
        "vehicle_label": "C1",
        "current_status": None,
        "stop_id": None,
        "vehicle_ts": None,
    }
    rows = [
        {**base, "vehicle_id": "1", "lat": 40.4, "lon": -3.7},
        {**base, "vehicle_id": "2", "lat": 0.0, "lon": 0.0},
        {**base, "vehicle_id": "3", "lat": None, "lon": None},
    ]
    res = validate_vehicle_positions(rows, "f", "cercanias", NOW, NOW)
    assert [e.vehicle_id for e in res.valid] == ["1"]
    assert _checks(res)[0] == {"fuera_de_espana": 1, "posicion_nula": 1}


def test_real_renfe_snapshot_passes_contracts(fixture_bytes):
    msg = decode(fixture_bytes("trip_updates"))
    feed_ts = header_timestamp(msg)
    res = validate_trip_updates(
        trip_update_rows(msg, "cercanias"),
        "f",
        "cercanias",
        feed_ts,
        feed_ts + timedelta(seconds=5),
    )
    assert res.report.n_in == 95
    assert res.report.n_rejected == 0
    # En la captura real hay trenes con más de 3 h de retraso: se marcan, no se descartan.
    assert res.report.warnings["retraso_extremo"] >= 1


def _snap(feed, payload, at):
    msg = decode(payload)
    return Snapshot(feed, at, header_timestamp(msg), len(msg.entity), payload)


def test_ingestor_publishes_valid_dlq_and_quality(fixture_bytes):
    bus = MemoryBus()
    ing = Ingestor(bus)
    payload = fixture_bytes("trip_updates")
    at = header_timestamp(decode(payload)) + timedelta(seconds=5)
    report = ing.handle(
        FEEDS_BY_KEY["cercanias_trip_updates"], _snap("cercanias_trip_updates", payload, at)
    )
    assert len(bus.messages[TOPIC_TRIP_UPDATES]) == report.n_valid == 95
    assert bus.messages[TOPIC_DLQ] == []
    key, q = bus.messages[TOPIC_QUALITY][0]
    assert key == "cercanias_trip_updates" and q["n_in"] == 95 and q["feed_lag_s"] == 5.0
    k, ev = bus.messages[TOPIC_TRIP_UPDATES][0]
    assert k == ev["trip_id"] and ev["schema_version"] == 1 and "delay_s" in ev


def _alerts_payload(fixture_bytes, drop_first=False):
    msg = decode(fixture_bytes("alerts"))
    if drop_first:
        del msg.entity[0]
    return msg.SerializeToString()


def test_alerts_only_publish_changes(fixture_bytes):
    bus = MemoryBus()
    ing = Ingestor(bus)
    feed = FEEDS_BY_KEY["cercanias_alerts"]
    full = _alerts_payload(fixture_bytes)
    at = header_timestamp(decode(full)) + timedelta(seconds=5)

    ing.handle(feed, _snap(feed.key, full, at))
    assert len(bus.messages[TOPIC_ALERTS]) == 78
    ing.handle(feed, _snap(feed.key, full, at + timedelta(seconds=30)))
    assert len(bus.messages[TOPIC_ALERTS]) == 78  # nada nuevo

    ing.handle(feed, _snap(feed.key, _alerts_payload(fixture_bytes, True), at))
    last_key, last = bus.messages[TOPIC_ALERTS][-1]
    assert len(bus.messages[TOPIC_ALERTS]) == 79
    assert last["status"] == "removed" and last_key == "AVISO_528568"


def test_rejected_rows_go_to_dlq_with_reason():
    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = int(NOW.timestamp())
    for _ in range(2):  # mismo viaje dos veces -> el segundo es duplicado
        e = msg.entity.add(id="x")
        e.trip_update.trip.trip_id = "T1"
        stu = e.trip_update.stop_time_update.add(stop_id="1")
        stu.arrival.delay = 0
    bus = MemoryBus()
    Ingestor(bus).handle(
        FEEDS_BY_KEY["cercanias_trip_updates"],
        _snap("cercanias_trip_updates", msg.SerializeToString(), NOW),
    )
    _, dlq = bus.messages[TOPIC_DLQ][0]
    assert dlq["errors"][0]["check"] == "duplicado"
    assert dlq["row"]["trip_id"] == "T1"


def test_same_vehicle_two_trips_is_not_a_duplicate():
    base = {
        "entity_id": "e",
        "vehicle_label": "AVE",
        "current_status": None,
        "stop_id": None,
        "vehicle_ts": None,
        "vehicle_id": "00194",
        "lat": 40.4,
        "lon": -3.7,
    }
    rows = [
        {**base, "trip_id": "0019412026-11-25"},
        {**base, "trip_id": "0019422026-11-25"},
        {**base, "trip_id": "0019422026-11-25"},
    ]
    res = validate_vehicle_positions(rows, "f", "ld", NOW, NOW)
    assert len(res.valid) == 2
    assert _checks(res)[0] == {"duplicado": 1}


def test_quality_summary_aggregates_reports(fixture_bytes):
    from pulso.quality import format_summary, summarize

    bus = MemoryBus()
    ing = Ingestor(bus)
    payload = fixture_bytes("trip_updates")
    at = header_timestamp(decode(payload)) + timedelta(seconds=5)
    for i in range(3):
        ing.handle(
            FEEDS_BY_KEY["cercanias_trip_updates"],
            _snap("cercanias_trip_updates", payload, at + timedelta(seconds=i)),
        )
    summary = summarize([v for _, v in bus.messages[TOPIC_QUALITY]])
    q = summary["cercanias_trip_updates"]
    assert q.snapshots == 3 and q.n_in == 285 and q.pct_valid == 100.0
    assert q.max_lag_s == 7.0
    assert "cercanias_trip_updates" in format_summary(summary)


def test_header_shifted_exactly_one_hour_is_corrected():
    # Caso real: cabecera del feed de posiciones AV/LD 1 h atrasada con datos frescos.
    res = validate_trip_updates([_tu()], "f", "ld", NOW - timedelta(seconds=3608), NOW)
    ev = res.valid[0]
    assert ev.flags == ["cabecera_desfase_horario"]
    assert ev.feed_ts == NOW - timedelta(seconds=8)
    assert res.report.feed_lag_s == 8
    # Un retraso de 40 min no es un desfase horario: se marca como desactualizado.
    stale = validate_trip_updates([_tu()], "f", "ld", NOW - timedelta(minutes=40), NOW)
    assert stale.valid[0].flags == ["feed_desactualizado"]
