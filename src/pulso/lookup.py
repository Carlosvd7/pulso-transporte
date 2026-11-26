"""Tablas de consulta en memoria (stream-table join): viaje -> línea/núcleo, parada -> nombre.

Se cargan desde las vistas `static_*` del warehouse y se recargan cuando aparece una
versión nueva del GTFS estático.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from pulso.config import Settings

log = logging.getLogger(__name__)

UNKNOWN_AREA = "Sin cruzar con horario"
UNKNOWN_LINE = "?"


@dataclass(frozen=True)
class TripInfo:
    route_id: str | None
    line: str
    area: str
    color: str | None
    service_date: date | None


@dataclass(frozen=True)
class StopInfo:
    name: str
    lat: float | None
    lon: float | None


@dataclass(frozen=True)
class RouteInfo:
    line: str
    area: str
    color: str | None


def _versions(settings: Settings) -> tuple[str, ...]:
    root = settings.data_dir / "static"
    if not root.exists():
        return ()
    return tuple(sorted(p.as_posix() for p in root.glob("*/v=*") if p.is_dir()))


def ld_key_date(trip_id: str) -> tuple[str, date | None]:
    """AV/LD: trip_id = <tren+variante><AAAA-MM-DD>."""
    if len(trip_id) <= 10:
        return trip_id, None
    try:
        return trip_id[:-10], date.fromisoformat(trip_id[-10:])
    except ValueError:
        return trip_id, None


class StaticLookup:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings
        self.trips: dict[tuple[str, str], TripInfo] = {}
        self.ld_days: dict[tuple[str, date], TripInfo] = {}
        self.stops: dict[tuple[str, str], StopInfo] = {}
        self.routes: dict[tuple[str, str], RouteInfo] = {}
        self.version: tuple[str, ...] = ()
        if settings is not None:
            self.reload()

    def maybe_reload(self) -> bool:
        if self.settings is None or _versions(self.settings) == self.version:
            return False
        self.reload()
        return True

    def reload(self) -> None:
        from pulso.warehouse import connect

        assert self.settings is not None
        version = _versions(self.settings)
        con = connect(self.settings)
        tables = {r[0] for r in con.execute("show tables").fetchall()}
        needed = {"static_trips", "static_routes", "static_stops", "static_service_dates"}
        if not needed <= tables:
            log.warning("sin GTFS estático todavía: los eventos no se podrán cruzar")
            self.version = version
            return
        routes = {}
        for net, rid, line, area, color in con.execute(
            "select network, route_id, line, area, route_color from static_routes"
        ).fetchall():
            routes[(net, rid)] = RouteInfo(line or UNKNOWN_LINE, area or UNKNOWN_AREA, color)
        trips = {}
        for net, tid, rid, sdate in con.execute("""
            select t.network, t.trip_id, t.route_id, min(sd.service_date)
            from static_trips t
            left join static_service_dates sd
              on sd.network = t.network and sd.service_id = t.service_id
            group by all""").fetchall():
            r = routes.get((net, rid))
            trips[(net, tid)] = TripInfo(
                rid,
                r.line if r else UNKNOWN_LINE,
                r.area if r else UNKNOWN_AREA,
                r.color if r else None,
                sdate,
            )
        ld_days = {}
        for tid, rid, sdate in con.execute("""
            select t.trip_id, t.route_id, sd.service_date
            from static_trips t
            join static_service_dates sd
              on sd.network = t.network and sd.service_id = t.service_id
            where t.network = 'ld'""").fetchall():
            key, _ = ld_key_date(tid)
            r = routes.get(("ld", rid))
            ld_days.setdefault(
                (key, sdate),
                TripInfo(
                    rid,
                    r.line if r else UNKNOWN_LINE,
                    r.area if r else UNKNOWN_AREA,
                    r.color if r else None,
                    sdate,
                ),
            )
        stops = {
            (net, sid): StopInfo(name, lat, lon)
            for net, sid, name, lat, lon in con.execute(
                "select network, stop_id, stop_name, lat, lon from static_stops"
            ).fetchall()
        }
        con.close()
        self.routes, self.trips, self.ld_days, self.stops = routes, trips, ld_days, stops
        self.version = version
        log.info(
            "horarios cargados: %d viajes, %d días AV/LD, %d paradas",
            len(trips),
            len(ld_days),
            len(stops),
        )

    def trip(self, network: str, trip_id: str | None) -> TripInfo | None:
        if not trip_id:
            return None
        info = self.trips.get((network, trip_id))
        if info is None and network == "ld":
            key, d = ld_key_date(trip_id)
            if d is not None:
                info = self.ld_days.get((key, d))
                if info is not None and info.service_date != d:
                    info = TripInfo(info.route_id, info.line, info.area, info.color, d)
        return info

    def stop(self, network: str, stop_id: str | None) -> StopInfo | None:
        return self.stops.get((network, stop_id)) if stop_id else None

    def route(self, network: str, route_id: str) -> RouteInfo | None:
        return self.routes.get((network, route_id))


def data_has_static(data_dir: Path) -> bool:
    return any((data_dir / "static").glob("*/v=*/trips.parquet"))
