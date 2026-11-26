from datetime import UTC, datetime

from pulso.parse import alert_rows, decode, trip_update_rows, vehicle_position_rows


def test_trip_updates_cercanias(fixture_bytes):
    rows = trip_update_rows(decode(fixture_bytes("trip_updates")), "cercanias")
    assert len(rows) == 95  # 93 con próxima parada + 2 cancelados sin paradas
    assert {r["network"] for r in rows} == {"cercanias"}
    assert rows[0]["feed_ts"] == datetime.fromtimestamp(1795646670, tz=UTC)

    canceled = [r for r in rows if r["schedule_relationship"] == "CANCELED"]
    assert len(canceled) == 2
    assert all(r["stop_id"] is None and r["arrival_delay_s"] is None for r in canceled)

    scheduled = [r for r in rows if r["schedule_relationship"] == "SCHEDULED"]
    assert all(isinstance(r["arrival_delay_s"], int) for r in scheduled)
    assert all(r["stop_id"] for r in scheduled)
    # En este feed el retraso del viaje y el de la próxima parada coinciden.
    assert all(r["trip_delay_s"] == r["arrival_delay_s"] for r in scheduled)


def test_trip_updates_ld_has_cancellations(fixture_bytes):
    rows = trip_update_rows(decode(fixture_bytes("trip_updates_LD")), "ld")
    rels = [r["schedule_relationship"] for r in rows]
    assert rels.count("CANCELED") == 33
    assert rels.count("SCHEDULED") == 45


def test_vehicle_positions_inside_spain(fixture_bytes):
    rows = vehicle_position_rows(decode(fixture_bytes("vehicle_positions")), "cercanias")
    assert len(rows) == 94
    for r in rows:
        # Caja amplia: península, Baleares y Canarias no tienen Cercanías, pero sí margen.
        assert 27 < r["lat"] < 44.5 and -10 < r["lon"] < 4.5
        assert r["trip_id"] and r["vehicle_label"]
    assert {r["current_status"] for r in rows} <= {"INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"}


def test_alerts_keep_spanish_text(fixture_bytes):
    rows = alert_rows(decode(fixture_bytes("alerts")), "cercanias")
    assert len(rows) == 78  # 80 entidades, 2 avisos repetidos fusionados
    first = rows[0]
    assert first["alert_id"] == "AVISO_528568"
    assert "recuperarán" in first["description"]
    assert "10T0017C5" in first["route_ids"]
    assert first["active_start"] is not None


def test_alerts_with_same_id_are_merged(fixture_bytes):
    rows = {r["alert_id"]: r for r in alert_rows(decode(fixture_bytes("alerts")), "cercanias")}
    merged = rows["AVISO_526429"]  # Renfe lo publica en 2 entidades con líneas distintas
    assert merged["n_entities"] == 2
    assert {"51T0029R7", "51T0001R1"} <= set(merged["route_ids"])
