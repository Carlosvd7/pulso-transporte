from datetime import UTC, date, datetime, timedelta

import pytest

from pulso.lookup import RouteInfo, StaticLookup, TripInfo
from pulso.processor import GoldWriter, write_live
from pulso.stream import Output, StreamProcessor, quantile

T0 = datetime(2026, 11, 26, 9, 0, tzinfo=UTC)
TODAY = date(2026, 11, 26)


def lookup() -> StaticLookup:
    lk = StaticLookup()
    for i in range(1, 30):
        lk.trips[("cercanias", f"T{i}")] = TripInfo("R1", "C1", "Madrid", "75B6E0", TODAY)
        lk.trips[("cercanias", f"S{i}")] = TripInfo("R5", "C5", "Madrid", "FFD700", TODAY)
    lk.routes[("cercanias", "R1")] = RouteInfo("C1", "Madrid", "75B6E0")
    return lk


_n = 0


def tu(trip, delay, ts, net="cercanias", rel="SCHEDULED", flags=None, stop="17000"):
    global _n
    _n += 1
    return {
        "event_id": f"e{_n}",
        "network": net,
        "feed_ts": ts.isoformat(),
        "fetched_at": ts.isoformat(),
        "flags": flags or [],
        "trip_id": trip,
        "schedule_relationship": rel,
        "stop_id": stop if rel != "CANCELED" else None,
        "arrival_time": None,
        "raw_delay_s": delay,
        "delay_s": delay,
    }


def rows(out: Output, line="C1", area="Madrid"):
    return [r for r in out.kpis if r["line"] == line and r["area"] == area]


def test_quantile_matches_linear_interpolation():
    assert quantile([0, 60, 240, 600], 0.5) == 150
    assert quantile([0, 60, 240, 600], 0.9) == pytest.approx(492)
    assert quantile([], 0.5) is None


def test_window_kpis_count_each_train_once():
    p = StreamProcessor(lookup())
    for trip, d in [("T1", 0), ("T2", 60), ("T3", 240), ("T4", 600)]:
        p.on_trip_update(tu(trip, 30, T0))  # primera observación, se sobrescribe
        p.on_trip_update(tu(trip, d, T0 + timedelta(minutes=2)))
    p.on_trip_update(tu("T5", 0, T0 + timedelta(minutes=7)))  # avanza la marca de agua
    out = p.drain()
    assert out.closed_windows == [T0]
    (r,) = rows(out)
    assert r["n_trains"] == 4 and r["mean_delay_s"] == 225.0
    assert r["pct_on_time"] == 50.0  # Cercanías: puntual si < 3 min
    assert r["p50_delay_s"] == 150 and r["max_delay_s"] == 600
    net = [x for x in out.kpis if x["area"] == "*"][0]
    assert net["n_trains"] == 4
    assert {s["stop_id"] for s in out.stops} == {"17000"}


def test_watermark_waits_for_slowest_partition():
    p = StreamProcessor(lookup())
    p.on_trip_update(tu("T1", 60, T0 + timedelta(minutes=1)), source="p0")
    p.on_trip_update(tu("T2", 60, T0 + timedelta(minutes=1)), source="p1")
    p.on_trip_update(tu("T3", 60, T0 + timedelta(minutes=8)), source="p0")  # p0 se adelanta
    assert p.drain().closed_windows == []  # p1 sigue en 09:01
    p.on_trip_update(tu("T4", 60, T0 + timedelta(minutes=3)), source="p1")  # no es tardío
    assert p.counters["eventos_tardios"] == 0
    p.on_trip_update(tu("T5", 60, T0 + timedelta(minutes=8)), source="p1")
    out = p.drain()
    assert T0 in out.closed_windows
    assert rows(out)[0]["n_trains"] == 3  # T1, T2, T4


def test_duplicates_ignored_and_late_events_counted():
    p = StreamProcessor(lookup())
    ev = tu("T1", 60, T0)
    p.on_trip_update(ev)
    p.on_trip_update(ev)
    assert p.counters["duplicados_ignorados"] == 1
    p.on_trip_update(tu("T2", 0, T0 + timedelta(minutes=7)))
    p.drain()
    p.on_trip_update(tu("T3", 0, T0 + timedelta(minutes=1)))  # su ventana ya cerró
    assert p.counters["eventos_tardios"] == 1


def _window(p, k, delay, n=3, line_prefix="T"):
    ts = T0 + timedelta(minutes=5 * k + 1)
    for i in range(1, n + 1):
        p.on_trip_update(tu(f"{line_prefix}{i}", delay, ts))


def test_spike_anomaly_starts_and_resolves():
    p = StreamProcessor(lookup())
    for k in range(7):
        _window(p, k, 60 + 10 * (k % 2))  # historia normal: ~1 min
    _window(p, 7, 1200)  # 20 min de media
    _window(p, 8, 60)
    _window(p, 9, 60)  # cierra la ventana del pico (marca de agua + margen)
    started = [a for a in p.drain().anomalies if a["status"] == "started"]
    kinds = {(a["type"], a["line"]) for a in started}
    assert ("pico_retraso", "C1") in kinds and ("retraso_alto", "C1") in kinds
    spike = next(a for a in started if a["type"] == "pico_retraso" and a["line"] == "C1")
    assert spike["baseline"] == pytest.approx(60, abs=10)
    assert "C1 (Madrid)" in spike["message"]
    for k in range(10, 14):
        _window(p, k, 60)
    resolved = [a for a in p.drain().anomalies if a["status"] == "resolved"]
    assert ("pico_retraso", "C1") in {(a["type"], a["line"]) for a in resolved}


def test_stalled_train_is_detected_and_excluded_from_kpis():
    p = StreamProcessor(lookup())
    for m in range(0, 26):  # el retraso crece 60 s por minuto: el tren no avanza
        ts = T0 + timedelta(minutes=m)
        p.on_trip_update(tu("S1", 300 + 60 * m, ts))
        p.on_trip_update(tu("T1", 60, ts))
    out = p.drain()
    stalled = [a for a in out.anomalies if a["type"] == "tren_detenido"]
    assert stalled and stalled[0]["trip_id"] == "S1"
    late_rows = [
        r for r in out.kpis if r["line"] == "C5" and r["window_start"] >= T0 + timedelta(minutes=20)
    ]
    assert all(r["n_trains"] == 0 and r["n_excluded"] == 1 for r in late_rows)
    state = p.live_state([])
    assert next(t for t in state["trains"] if t["trip_id"] == "S1")["stalled"] is True


def test_stale_ld_trips_and_cancellation_waves():
    p = StreamProcessor(lookup())
    # Tren AV/LD de hace 3 días (sin cruce): fecha sacada del trip_id, no cuenta.
    p.on_trip_update(tu("0019412026-11-23", 9000, T0, net="ld"))
    for i in range(1, 4):
        p.on_trip_update(tu(f"T{i}", None, T0 + timedelta(minutes=1), rel="CANCELED"))
    p.on_trip_update(tu("T9", 0, T0 + timedelta(minutes=8)))
    out = p.drain()
    assert p.counters["viajes_de_dias_pasados"] == 1
    assert not [r for r in out.kpis if r["network"] == "ld"]
    area = next(r for r in out.kpis if r["area"] == "Madrid" and r["line"] == "*")
    assert area["n_canceled"] == 3 and area["n_new_canceled"] == 3
    assert any(a["type"] == "cancelaciones" for a in out.anomalies)


def test_live_state_and_gold_roundtrip(tmp_path):
    p = StreamProcessor(lookup())
    p.on_alert(
        {
            "alert_id": "A1",
            "network": "cercanias",
            "route_ids": ["R1"],
            "header": None,
            "description": "Obras",
            "status": "active",
        }
    )
    p.on_trip_update(tu("T1", 120, T0))
    p.on_vehicle(
        {
            "event_id": "v1",
            "network": "cercanias",
            "feed_ts": T0.isoformat(),
            "trip_id": "T1",
            "lat": 40.4,
            "lon": -3.7,
        }
    )
    p.on_trip_update(tu("T2", 0, T0 + timedelta(minutes=7)))
    # Las posiciones son otra fuente: la marca de agua espera también por ellas.
    p.on_vehicle(
        {
            "event_id": "v2",
            "network": "cercanias",
            "feed_ts": (T0 + timedelta(minutes=7)).isoformat(),
            "trip_id": "T2",
            "lat": 40.5,
            "lon": -3.6,
        }
    )
    out = p.drain()
    state = p.live_state(out.kpis, now=T0)
    t1 = next(t for t in state["trains"] if t["trip_id"] == "T1")
    assert (t1["lat"], t1["line"], t1["delay_s"]) == (40.4, "C1", 120)
    assert state["alerts"][0]["lines"] == ["C1"]
    c1 = next(line for line in state["lines"] if line["line"] == "C1")
    assert c1["history"][0]["v"] == 120.0
    write_live(tmp_path, state)
    assert (tmp_path / "live" / "state.json").exists()

    gold = GoldWriter(tmp_path)
    gold.write(out)
    gold.write(out)  # reescribir la misma ventana es idempotente
    back = gold.recent_kpis(T0 - timedelta(hours=1))
    assert len(back) == len(out.kpis)


def test_live_state_tolerates_repeated_windows_with_missing_values():
    p = StreamProcessor(lookup())
    p.on_trip_update(tu("T1", 60, T0))
    w = T0 - timedelta(minutes=5)
    recent = [
        {
            "window_start": w,
            "network": "cercanias",
            "area": "Madrid",
            "line": "C1",
            "mean_delay_s": None,
        },
        {
            "window_start": w,
            "network": "cercanias",
            "area": "Madrid",
            "line": "C1",
            "mean_delay_s": 30.0,
        },
    ]
    c1 = next(x for x in p.live_state(recent)["lines"] if x["line"] == "C1")
    assert c1["history"] == [{"t": w.isoformat(), "v": 30.0}]
