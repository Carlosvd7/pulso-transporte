"""Bus de eventos: Redpanda/Kafka en local, o un bus en memoria (tests y despliegue ligero).

El resto del código solo conoce `Publisher.publish(topic, key, value)`, así que el mismo
pipeline corre con Redpanda en Docker o en un único proceso sin broker.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Protocol

log = logging.getLogger(__name__)

TOPIC_TRIP_UPDATES = "renfe.trip_updates.v1"
TOPIC_VEHICLE_POSITIONS = "renfe.vehicle_positions.v1"
TOPIC_ALERTS = "renfe.alerts.v1"
TOPIC_DLQ = "renfe.dlq.v1"
TOPIC_QUALITY = "pulso.quality.v1"

DAY_MS = 24 * 3600 * 1000
# nombre -> (particiones, retención en ms)
TOPICS: dict[str, tuple[int, int]] = {
    TOPIC_TRIP_UPDATES: (3, 7 * DAY_MS),
    TOPIC_VEHICLE_POSITIONS: (3, 2 * DAY_MS),
    TOPIC_ALERTS: (1, 30 * DAY_MS),
    TOPIC_DLQ: (1, 30 * DAY_MS),
    TOPIC_QUALITY: (1, 30 * DAY_MS),
}


def encode(value: dict[str, Any]) -> bytes:
    def default(o: object) -> str:
        if isinstance(o, datetime):
            return o.isoformat()
        raise TypeError(type(o))

    return json.dumps(value, ensure_ascii=False, default=default, separators=(",", ":")).encode()


class Publisher(Protocol):
    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None: ...

    def flush(self, timeout: float = 10) -> int: ...


class MemoryBus:
    """Bus en memoria: guarda los mensajes por tema, en orden."""

    def __init__(self) -> None:
        self.messages: dict[str, list[tuple[str | None, dict[str, Any]]]] = defaultdict(list)

    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        # Se serializa y deserializa para comportarse igual que Kafka (fechas -> texto).
        self.messages[topic].append((key, json.loads(encode(value))))

    def flush(self, timeout: float = 10) -> int:
        return 0


class KafkaPublisher:
    def __init__(self, bootstrap: str, client_id: str = "pulso-producer") -> None:
        from confluent_kafka import Producer

        self.delivered = 0
        self.failed = 0
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": client_id,
                "enable.idempotence": True,  # sin duplicados por reintentos
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.failed += 1
            log.warning("entrega fallida: %s", err)
        else:
            self.delivered += 1

    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        payload = encode(value)
        k = key.encode() if key is not None else None
        while True:
            try:
                self._producer.produce(topic, payload, k, on_delivery=self._on_delivery)
                break
            except BufferError:  # cola local llena: esperar a que salgan mensajes
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self, timeout: float = 10) -> int:
        return self._producer.flush(timeout)


def ensure_topics(bootstrap: str, topics: dict[str, tuple[int, int]] = TOPICS) -> list[str]:
    """Crea los temas que falten. Devuelve los creados."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=15).topics)
    new = [
        NewTopic(
            name,
            num_partitions=parts,
            replication_factor=1,
            config={"retention.ms": str(retention)},
        )
        for name, (parts, retention) in topics.items()
        if name not in existing
    ]
    if not new:
        return []
    for name, fut in admin.create_topics(new).items():
        fut.result(timeout=30)
        log.info("tema creado: %s", name)
    return [t.topic for t in new]
