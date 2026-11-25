"""Convierte mensajes GTFS-Realtime (protobuf) en filas planas.

Cada fila es un dict con tipos simples; los contratos de calidad (fase 2) se aplican después.
No se descarta nada aquí: si un campo falta, queda a None para poder medirlo.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from google.transit import gtfs_realtime_pb2 as rt

Row = dict[str, Any]

_SCHED_REL = rt.TripDescriptor.ScheduleRelationship.Name
_VEH_STATUS = rt.VehiclePosition.VehicleStopStatus.Name
_CAUSE = rt.Alert.Cause.Name
_EFFECT = rt.Alert.Effect.Name


def decode(payload: bytes) -> rt.FeedMessage:
    msg = rt.FeedMessage()
    msg.ParseFromString(payload)
    return msg


def _ts(seconds: int | None) -> datetime | None:
    if not seconds:
        return None
    return datetime.fromtimestamp(seconds, tz=UTC)


def _opt(msg: Any, field: str) -> Any:
    return getattr(msg, field) if msg.HasField(field) else None


def header_timestamp(msg: rt.FeedMessage) -> datetime | None:
    return _ts(msg.header.timestamp) if msg.header.HasField("timestamp") else None


def trip_update_rows(msg: rt.FeedMessage, network: str) -> list[Row]:
    feed_ts = header_timestamp(msg)
    rows: list[Row] = []
    for ent in msg.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        trip = tu.trip
        base = {
            "network": network,
            "feed_ts": feed_ts,
            "entity_id": ent.id,
            "trip_id": _opt(trip, "trip_id"),
            "route_id": _opt(trip, "route_id"),
            "schedule_relationship": _SCHED_REL(trip.schedule_relationship),
            "trip_delay_s": _opt(tu, "delay"),
            "trip_ts": _ts(_opt(tu, "timestamp")),
        }
        if not tu.stop_time_update:
            rows.append(
                {
                    **base,
                    "stop_id": None,
                    "stop_sequence": None,
                    "arrival_time": None,
                    "arrival_delay_s": None,
                    "departure_time": None,
                    "departure_delay_s": None,
                }
            )
            continue
        for stu in tu.stop_time_update:
            arr = stu.arrival if stu.HasField("arrival") else None
            dep = stu.departure if stu.HasField("departure") else None
            rows.append(
                {
                    **base,
                    "stop_id": _opt(stu, "stop_id"),
                    "stop_sequence": _opt(stu, "stop_sequence"),
                    "arrival_time": _ts(_opt(arr, "time")) if arr else None,
                    "arrival_delay_s": _opt(arr, "delay") if arr else None,
                    "departure_time": _ts(_opt(dep, "time")) if dep else None,
                    "departure_delay_s": _opt(dep, "delay") if dep else None,
                }
            )
    return rows


def vehicle_position_rows(msg: rt.FeedMessage, network: str) -> list[Row]:
    feed_ts = header_timestamp(msg)
    rows: list[Row] = []
    for ent in msg.entity:
        if not ent.HasField("vehicle"):
            continue
        vp = ent.vehicle
        pos = vp.position if vp.HasField("position") else None
        desc = vp.vehicle if vp.HasField("vehicle") else None
        rows.append(
            {
                "network": network,
                "feed_ts": feed_ts,
                "entity_id": ent.id,
                "trip_id": _opt(vp.trip, "trip_id") if vp.HasField("trip") else None,
                "vehicle_id": _opt(desc, "id") if desc else None,
                "vehicle_label": _opt(desc, "label") if desc else None,
                "lat": _opt(pos, "latitude") if pos else None,
                "lon": _opt(pos, "longitude") if pos else None,
                "current_status": _VEH_STATUS(vp.current_status)
                if vp.HasField("current_status")
                else None,
                "stop_id": _opt(vp, "stop_id"),
                "vehicle_ts": _ts(_opt(vp, "timestamp")),
            }
        )
    return rows


def _text(ts: rt.TranslatedString, lang: str = "es") -> str | None:
    if not ts.translation:
        return None
    for t in ts.translation:
        if t.language == lang:
            return t.text
    return ts.translation[0].text


def alert_rows(msg: rt.FeedMessage, network: str) -> list[Row]:
    feed_ts = header_timestamp(msg)
    rows: list[Row] = []
    for ent in msg.entity:
        if not ent.HasField("alert"):
            continue
        al = ent.alert
        period = al.active_period[0] if al.active_period else None
        rows.append(
            {
                "network": network,
                "feed_ts": feed_ts,
                "alert_id": ent.id,
                "active_start": _ts(_opt(period, "start")) if period else None,
                "active_end": _ts(_opt(period, "end")) if period else None,
                "route_ids": sorted({ie.route_id for ie in al.informed_entity if ie.route_id}),
                "stop_ids": sorted({ie.stop_id for ie in al.informed_entity if ie.stop_id}),
                "cause": _CAUSE(al.cause) if al.HasField("cause") else None,
                "effect": _EFFECT(al.effect) if al.HasField("effect") else None,
                "header": _text(al.header_text),
                "description": _text(al.description_text),
            }
        )
    return rows
