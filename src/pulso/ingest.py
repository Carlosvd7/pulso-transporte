"""Ingesta: snapshot GTFS-RT -> validación (contratos) -> eventos en el bus.

- Válidos: a su tema (clave = trip_id / vehículo / aviso, para mantener el orden por clave).
- Rechazados: a la DLQ con los motivos.
- Un informe de calidad por snapshot al tema de calidad.
- Avisos: solo se publican los nuevos o modificados, y un evento "removed" cuando
  desaparecen (Renfe repite los mismos ~80 avisos cada 30 s).
"""

from __future__ import annotations

import logging
from collections import Counter

from pulso.bronze import Snapshot
from pulso.bus import (
    TOPIC_ALERTS,
    TOPIC_DLQ,
    TOPIC_QUALITY,
    TOPIC_TRIP_UPDATES,
    TOPIC_VEHICLE_POSITIONS,
    Publisher,
)
from pulso.config import Feed, FeedKind
from pulso.contracts import (
    QualityReport,
    validate_alerts,
    validate_trip_updates,
    validate_vehicle_positions,
)
from pulso.parse import (
    alert_content_sha,
    alert_rows,
    decode,
    header_timestamp,
    trip_update_rows,
    vehicle_position_rows,
)

log = logging.getLogger(__name__)


class Ingestor:
    def __init__(self, publisher: Publisher) -> None:
        self.publisher = publisher
        self.published: Counter = Counter()
        # Último contenido publicado de cada aviso, por feed.
        self._alerts: dict[str, dict[str, str]] = {}

    def handle(self, feed: Feed, snap: Snapshot) -> QualityReport:
        msg = decode(snap.payload)
        feed_ts = header_timestamp(msg)
        net = feed.network
        if feed.kind is FeedKind.TRIP_UPDATES:
            result = validate_trip_updates(
                trip_update_rows(msg, net), feed.key, net, feed_ts, snap.fetched_at
            )
            for ev in result.valid:
                self._pub(TOPIC_TRIP_UPDATES, ev.trip_id, ev.model_dump(mode="json"))
        elif feed.kind is FeedKind.VEHICLE_POSITIONS:
            result = validate_vehicle_positions(
                vehicle_position_rows(msg, net), feed.key, net, feed_ts, snap.fetched_at
            )
            for ev in result.valid:
                key = ev.vehicle_id or ev.trip_id
                self._pub(TOPIC_VEHICLE_POSITIONS, key, ev.model_dump(mode="json"))
        else:
            rows = alert_rows(msg, net)
            for r in rows:
                r["content_sha"] = alert_content_sha(r)
            result = validate_alerts(rows, feed.key, net, feed_ts, snap.fetched_at)
            self._publish_alert_changes(feed, result.valid, snap)

        for rej in result.rejected:
            self._pub(TOPIC_DLQ, feed.key, rej.to_dlq(feed.key))
        self._pub(TOPIC_QUALITY, feed.key, result.report.to_event())
        return result.report

    def _publish_alert_changes(self, feed: Feed, events: list, snap: Snapshot) -> None:
        previous = self._alerts.get(feed.key, {})
        current: dict[str, str] = {}
        for ev in events:
            sha = ev.event_id.rsplit(":", 1)[-1]
            current[ev.alert_id] = sha
            if previous.get(ev.alert_id) != sha:
                self._pub(TOPIC_ALERTS, ev.alert_id, ev.model_dump(mode="json"))
        for alert_id in previous.keys() - current.keys():
            self._pub(
                TOPIC_ALERTS,
                alert_id,
                {
                    "schema_version": 1,
                    "event_id": f"{feed.network}:{alert_id}:removed",
                    "network": feed.network,
                    "feed_ts": snap.header_ts,
                    "fetched_at": snap.fetched_at,
                    "flags": [],
                    "alert_id": alert_id,
                    "status": "removed",
                },
            )
        self._alerts[feed.key] = current

    def _pub(self, topic: str, key: str | None, value: dict) -> None:
        self.publisher.publish(topic, key, value)
        self.published[topic] += 1
