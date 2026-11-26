"""Configuración central: feeds de Renfe, rutas y parámetros (sobrescribibles por entorno)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

RENFE_RT_BASE = "https://gtfsrt.renfe.com"


class FeedKind(StrEnum):
    TRIP_UPDATES = "trip_updates"
    VEHICLE_POSITIONS = "vehicle_positions"
    ALERTS = "alerts"


@dataclass(frozen=True)
class Feed:
    """Un endpoint GTFS-Realtime concreto."""

    key: str  # identificador estable, se usa en rutas y temas
    network: str  # "cercanias" | "ld" (alta velocidad, larga y media distancia)
    kind: FeedKind
    url: str


FEEDS: tuple[Feed, ...] = (
    Feed(
        "cercanias_trip_updates",
        "cercanias",
        FeedKind.TRIP_UPDATES,
        f"{RENFE_RT_BASE}/trip_updates.pb",
    ),
    Feed(
        "cercanias_vehicle_positions",
        "cercanias",
        FeedKind.VEHICLE_POSITIONS,
        f"{RENFE_RT_BASE}/vehicle_positions.pb",
    ),
    Feed("cercanias_alerts", "cercanias", FeedKind.ALERTS, f"{RENFE_RT_BASE}/alerts.pb"),
    Feed("ld_trip_updates", "ld", FeedKind.TRIP_UPDATES, f"{RENFE_RT_BASE}/trip_updates_LD.pb"),
    Feed(
        "ld_vehicle_positions",
        "ld",
        FeedKind.VEHICLE_POSITIONS,
        f"{RENFE_RT_BASE}/vehicle_positions_LD.pb",
    ),
)

FEEDS_BY_KEY = {f.key: f for f in FEEDS}

# Horarios planificados (GTFS estático), licencia CC-BY 4.0 de Renfe.
STATIC_GTFS = {
    "cercanias": "https://ssl.renfe.com/ftransit/Fichero_CER_FOMENTO/fomento_transit.zip",
    "ld": "https://ssl.renfe.com/gtransit/Fichero_AV_LD/google_transit.zip",
}


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    poll_interval_s: float
    flush_interval_s: float
    http_timeout_s: float
    user_agent: str
    kafka_bootstrap: str = "localhost:19092"


def load_settings() -> Settings:
    return Settings(
        data_dir=Path(os.environ.get("PULSO_DATA_DIR", "data")),
        poll_interval_s=float(os.environ.get("PULSO_POLL_INTERVAL_S", "30")),
        flush_interval_s=float(os.environ.get("PULSO_FLUSH_INTERVAL_S", "120")),
        http_timeout_s=float(os.environ.get("PULSO_HTTP_TIMEOUT_S", "20")),
        user_agent=os.environ.get("PULSO_USER_AGENT", "pulso-transporte/0.1"),
        kafka_bootstrap=os.environ.get("PULSO_KAFKA_BOOTSTRAP", "localhost:19092"),
    )
