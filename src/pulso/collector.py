"""Recolector: consulta los feeds GTFS-RT de Renfe en bucle y guarda los cambios en bronze.

- Solo se guarda un snapshot si su timestamp de cabecera o su contenido cambian
  (Renfe refresca cada ~30-40 s; consultar más a menudo daría duplicados).
- Los errores de red no paran el bucle: se cuentan y se reintenta en la siguiente vuelta.
"""

from __future__ import annotations

import logging
import signal
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx
from google.protobuf.message import DecodeError

from pulso.bronze import BronzeWriter, Snapshot
from pulso.config import FEEDS, Feed, Settings
from pulso.parse import decode, header_timestamp

log = logging.getLogger(__name__)


@dataclass
class FeedState:
    last_header_ts: datetime | None = None
    last_sha: str | None = None


@dataclass
class Stats:
    counts: Counter = field(default_factory=Counter)

    def inc(self, feed: str, what: str) -> None:
        self.counts[(feed, what)] += 1

    def summary(self) -> str:
        by_what = Counter()
        for (_, what), n in self.counts.items():
            by_what[what] += n
        return ", ".join(f"{k}={v}" for k, v in sorted(by_what.items()))


def fetch(client: httpx.Client, feed: Feed) -> Snapshot:
    """Descarga y decodifica un feed. Lanza excepción si la respuesta no es válida."""
    resp = client.get(feed.url)
    resp.raise_for_status()
    msg = decode(resp.content)
    return Snapshot(
        feed=feed.key,
        fetched_at=datetime.now(UTC),
        header_ts=header_timestamp(msg),
        n_entities=len(msg.entity),
        payload=resp.content,
    )


def is_new(state: FeedState, snap: Snapshot) -> bool:
    """Un snapshot es nuevo si su contenido (hash) cambia respecto al último guardado."""
    return state.last_sha != snap.sha256


class Collector:
    def __init__(
        self,
        settings: Settings,
        feeds: tuple[Feed, ...] = FEEDS,
        on_snapshot: Callable[[Feed, Snapshot], object] | None = None,
    ) -> None:
        self.settings = settings
        self.on_snapshot = on_snapshot
        self.feeds = feeds
        self.writer = BronzeWriter(settings.data_dir)
        self.state = {f.key: FeedState() for f in feeds}
        self.stats = Stats()
        self._stop = False
        self._last_flush = time.monotonic()

    def stop(self, *_: object) -> None:
        self._stop = True

    def poll_once(self, client: httpx.Client) -> None:
        for feed in self.feeds:
            try:
                snap = fetch(client, feed)
            except (httpx.HTTPError, DecodeError) as exc:
                self.stats.inc(feed.key, "errors")
                log.warning("feed %s: %s: %s", feed.key, type(exc).__name__, exc)
                continue
            st = self.state[feed.key]
            if not is_new(st, snap):
                self.stats.inc(feed.key, "unchanged")
                continue
            st.last_header_ts, st.last_sha = snap.header_ts, snap.sha256
            self.writer.add(snap)
            self.stats.inc(feed.key, "saved")
            if self.on_snapshot is not None:
                try:
                    self.on_snapshot(feed, snap)
                except Exception:  # el archivo crudo ya está a salvo; se cuenta y se sigue
                    self.stats.inc(feed.key, "handler_errors")
                    log.exception("feed %s: fallo al procesar el snapshot", feed.key)

    def maybe_flush(self, force: bool = False) -> None:
        due = time.monotonic() - self._last_flush >= self.settings.flush_interval_s
        if force or due:
            paths = self.writer.flush()
            self._last_flush = time.monotonic()
            if paths:
                log.info("volcados %d ficheros | %s", len(paths), self.stats.summary())

    def run(self, max_cycles: int | None = None) -> None:
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        headers = {"User-Agent": self.settings.user_agent}
        cycles = 0
        with httpx.Client(
            timeout=self.settings.http_timeout_s, headers=headers, follow_redirects=True
        ) as client:
            log.info(
                "recolector iniciado: %d feeds, cada %.0f s -> %s",
                len(self.feeds),
                self.settings.poll_interval_s,
                self.settings.data_dir,
            )
            while not self._stop:
                started = time.monotonic()
                self.poll_once(client)
                self.maybe_flush()
                cycles += 1
                if max_cycles is not None and cycles >= max_cycles:
                    break
                remaining = self.settings.poll_interval_s - (time.monotonic() - started)
                while remaining > 0 and not self._stop:
                    time.sleep(min(remaining, 1.0))
                    remaining -= 1.0
        self.maybe_flush(force=True)
        log.info("recolector detenido | %s", self.stats.summary())
