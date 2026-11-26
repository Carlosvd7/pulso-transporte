"""Modo ligero: toda la tubería en un solo proceso, sin Redpanda.

Pensado para servidores pequeños (p. ej. Oracle Always Free AMD, 1 GB de RAM). Usa exactamente
el mismo código que la pila completa — recolector, contratos, ingesta, procesador en streaming
y API — pero el bus de eventos es una llamada en memoria en vez de Redpanda:

    Renfe -> Collector -> Ingestor -(LocalBus)-> ProcessorApp -> gold/ + live/ -> API

Lo que se pierde frente a Redpanda: si el proceso se reinicia, el estado en vivo se
reconstruye desde cero (la historia de KPIs sí se recupera de gold) y no hay otros
consumidores posibles del flujo.
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any

from pulso.bronze import Snapshot
from pulso.bus import encode
from pulso.collector import Collector
from pulso.config import FEEDS, Feed, Settings
from pulso.ingest import Ingestor
from pulso.processor import ProcessorApp

log = logging.getLogger(__name__)


class LocalBus:
    """Publicador que entrega cada mensaje directamente al procesador (mismo hilo)."""

    def __init__(self, app: ProcessorApp) -> None:
        self.app = app
        self.lock = threading.Lock()

    def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        # Ida y vuelta por JSON: el procesador recibe lo mismo que recibiría de Redpanda.
        self.app.handle(topic, json.loads(encode(value)), f"{topic}:0")

    def flush(self, timeout: float = 10) -> int:
        return 0


def start_pipeline(
    settings: Settings, feeds: tuple[Feed, ...] = FEEDS, max_cycles: int | None = None
) -> tuple[threading.Thread, Collector]:
    """Arranca recolector + procesador en un hilo. Devuelve el hilo y el recolector."""
    from pulso.producer import STATIC_REFRESH_S
    from pulso.static_gtfs import refresh

    # Primera carga del GTFS estático antes de arrancar, para cruzar desde el primer evento.
    try:
        refresh(settings)
    except Exception:
        log.exception("no se pudo cargar el GTFS estático (se reintentará)")

    stop_static = threading.Event()

    def _refresh_loop() -> None:
        while not stop_static.wait(STATIC_REFRESH_S):
            try:
                refresh(settings)
            except Exception:
                log.exception("fallo al refrescar el GTFS estático")

    threading.Thread(target=_refresh_loop, daemon=True, name="static-refresh").start()

    app = ProcessorApp(settings, publisher=None)
    bus = LocalBus(app)
    ingestor = Ingestor(bus)

    def on_snapshot(feed: Feed, snap: Snapshot) -> None:
        with bus.lock:
            ingestor.handle(feed, snap)
            app.after_batch()

    collector = Collector(settings, feeds, on_snapshot=on_snapshot)

    def _run() -> None:
        try:
            collector.run(max_cycles=max_cycles)
        finally:
            stop_static.set()
            app.after_batch(force_live=True)

    thread = threading.Thread(target=_run, daemon=True, name="pipeline")
    thread.start()
    log.info("modo ligero: tubería iniciada sin Redpanda")
    return thread, collector


def serve_lite(settings: Settings, host: str, port: int) -> None:
    import uvicorn

    from pulso.api import create_app

    _, collector = start_pipeline(settings)
    try:
        uvicorn.run(create_app(settings), host=host, port=port, log_level="warning")
    finally:
        collector.stop()
