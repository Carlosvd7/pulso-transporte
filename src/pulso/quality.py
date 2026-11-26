"""Métricas de calidad: agrega los informes por snapshot del tema pulso.quality.v1."""

from __future__ import annotations

import json
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

from pulso.bus import TOPIC_QUALITY


@dataclass
class FeedQuality:
    snapshots: int = 0
    n_in: int = 0
    n_valid: int = 0
    n_rejected: int = 0
    errors: Counter = field(default_factory=Counter)
    warnings: Counter = field(default_factory=Counter)
    max_lag_s: float = 0.0
    last_fetched_at: str | None = None

    @property
    def pct_valid(self) -> float:
        return 100.0 * self.n_valid / self.n_in if self.n_in else 100.0


def summarize(reports: list[dict[str, Any]]) -> dict[str, FeedQuality]:
    out: dict[str, FeedQuality] = defaultdict(FeedQuality)
    for r in reports:
        q = out[r["feed"]]
        q.snapshots += 1
        q.n_in += r["n_in"]
        q.n_valid += r["n_valid"]
        q.n_rejected += r["n_rejected"]
        q.errors.update(r["errors"])
        q.warnings.update(r["warnings"])
        if r.get("feed_lag_s") is not None:
            q.max_lag_s = max(q.max_lag_s, r["feed_lag_s"])
        q.last_fetched_at = max(q.last_fetched_at or "", r["fetched_at"])
    return dict(out)


def read_recent_reports(bootstrap: str, minutes: float) -> list[dict[str, Any]]:
    """Lee del tema de calidad los informes de los últimos `minutes` minutos."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"pulso-quality-cli-{time.time_ns()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    since_ms = int((time.time() - minutes * 60) * 1000)
    meta = consumer.list_topics(TOPIC_QUALITY, timeout=15)
    parts = [
        TopicPartition(TOPIC_QUALITY, p, since_ms) for p in meta.topics[TOPIC_QUALITY].partitions
    ]
    offsets = consumer.offsets_for_times(parts, timeout=15)
    ends = {p.partition: consumer.get_watermark_offsets(p, timeout=15)[1] for p in parts}
    todo = {o.partition for o in offsets if o.offset >= 0 and o.offset < ends[o.partition]}
    consumer.assign([o for o in offsets if o.partition in todo])
    reports = []
    while todo:
        msg = consumer.poll(5)
        if msg is None:
            break
        if msg.error():
            continue
        reports.append(json.loads(msg.value()))
        if msg.offset() + 1 >= ends[msg.partition()]:
            todo.discard(msg.partition())
    consumer.close()
    return reports


def format_summary(summary: dict[str, FeedQuality]) -> str:
    lines = [
        f"{'feed':28s} {'snaps':>6s} {'eventos':>8s} {'%válidos':>9s} {'rechaz.':>8s} "
        f"{'lag máx':>8s}  errores / avisos"
    ]
    for feed, q in sorted(summary.items()):
        issues = ", ".join(f"{k}={v}" for k, v in q.errors.most_common())
        warns = ", ".join(f"{k}={v}" for k, v in q.warnings.most_common())
        lines.append(
            f"{feed:28s} {q.snapshots:6d} {q.n_in:8d} {q.pct_valid:8.2f}% "
            f"{q.n_rejected:8d} {q.max_lag_s:7.0f}s  "
            f"{issues or '-'} / {warns or '-'}"
        )
    return "\n".join(lines)
