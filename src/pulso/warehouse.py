"""Vistas DuckDB sobre los Parquet: modelo de datos consultable con SQL.

Capas:
- static_*: GTFS estático limpio; para cada clave se toma la versión más reciente que la
  contenga (así los trenes de días pasados siguen cruzando aunque Renfe ya los haya quitado).
- rt_*: silver (observaciones en tiempo real).
- trip_delays: observaciones de retraso enriquecidas con línea, núcleo y estación.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from pulso.config import STATIC_GTFS, Settings

STATIC_KEYS = {
    "routes": "route_id",
    "stops": "stop_id",
    "trips": "trip_id",
    "trip_summary": "trip_id",
    "service_dates": "service_id, service_date",
}


def _glob(path: Path) -> str:
    return path.as_posix()


def _has_files(pattern: Path) -> bool:
    root = pattern.parent
    while "*" in root.as_posix():
        root = root.parent
    rel = pattern.relative_to(root).as_posix()
    return root.exists() and any(root.glob(rel))


def connect(settings: Settings, database: str = ":memory:") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database)
    con.execute("SET TimeZone = 'UTC'")
    create_views(con, settings)
    return con


def create_views(con: duckdb.DuckDBPyConnection, settings: Settings) -> list[str]:
    data = settings.data_dir
    created = []
    for table, key in STATIC_KEYS.items():
        parts = []
        for net in STATIC_GTFS:
            pat = data / "static" / net / "v=*" / f"{table}.parquet"
            if _has_files(pat):
                parts.append(
                    f"select '{net}' as network, * from read_parquet('{_glob(pat)}',"
                    " hive_partitioning = true, union_by_name = true)"
                )
        if not parts:
            continue
        con.execute(f"""
            create or replace view static_{table} as
            select * exclude (rn) from (
                select *, row_number() over (partition by network, {key} order by v desc) as rn
                from ({" union all by name ".join(parts)})
            ) where rn = 1""")
        created.append(f"static_{table}")

    for kind in ("trip_updates", "vehicle_positions", "alerts"):
        pat = data / "silver" / kind / "network=*" / "date=*" / "*.parquet"
        if _has_files(pat):
            con.execute(f"""
                create or replace view rt_{kind} as
                select * exclude (date) from read_parquet('{_glob(pat)}',
                    hive_partitioning = true, hive_types = {{'network': varchar, 'date': date}})""")
            created.append(f"rt_{kind}")

    needed = {
        "rt_trip_updates",
        "static_trips",
        "static_routes",
        "static_stops",
        "static_service_dates",
    }
    if needed <= set(created):
        con.execute(TRIP_DELAYS_SQL)
        created.append("trip_delays")
    return created


# Retraso "crudo" de Renfe -> retraso normalizado a (-12 h, +12 h].
# Cerca de medianoche Renfe calcula algunos retrasos contra el día equivocado
# (p. ej. -86 340 s en vez de +60 s). Ningún retraso real se acerca a 12 h, así que
# reducir módulo 24 h es seguro; se marca la fila para medirlo en calidad de datos.
NORMALIZE_DELAY = "(((({x}) + 43200) % 86400 + 86400) % 86400 - 43200)"

TRIP_DELAYS_SQL = f"""
create or replace view trip_delays as
with base as (
    select tu.*,
           coalesce(tu.arrival_delay_s, tu.departure_delay_s, tu.trip_delay_s) as raw_delay_s,
           -- AV/LD: trip_id = <tren+variante><AAAA-MM-DD>; se usa si no hay cruce exacto.
           case when tu.network = 'ld' and length(tu.trip_id) > 10
                then left(tu.trip_id, length(tu.trip_id) - 10) end as ld_key,
           case when tu.network = 'ld'
                then try_cast(right(tu.trip_id, 10) as date) end as ld_date
    from rt_trip_updates tu
),
ld_trip_days as (
    select t.trip_id as static_trip_id, t.route_id,
           left(t.trip_id, length(t.trip_id) - 10) as ld_key, sd.service_date as ld_date
    from static_trips t
    join static_service_dates sd on sd.network = t.network and sd.service_id = t.service_id
    where t.network = 'ld'
    qualify row_number() over (partition by ld_key, ld_date order by t.trip_id) = 1
)
select b.network, b.fetched_at, b.feed_ts, b.trip_id, b.schedule_relationship,
       b.stop_id, b.arrival_time, b.raw_delay_s,
       {NORMALIZE_DELAY.format(x="b.raw_delay_s")} as delay_s,
       b.raw_delay_s is not null
           and b.raw_delay_s <> {NORMALIZE_DELAY.format(x="b.raw_delay_s")} as delay_wrapped,
       coalesce(t.route_id, ld.route_id) as route_id,
       r.line, r.area, r.route_long_name, r.route_color,
       s.stop_name, s.lat, s.lon,
       case when t.trip_id is not null then 'exacto'
            when ld.static_trip_id is not null then 'tren_y_fecha'
            else null end as trip_match,
       coalesce(t.trip_id, ld.static_trip_id) is not null as trip_matched,
       b.stop_id is null or s.stop_id is not null as stop_matched
from base b
left join static_trips t on t.network = b.network and t.trip_id = b.trip_id
left join ld_trip_days ld
       on t.trip_id is null and ld.ld_key = b.ld_key and ld.ld_date = b.ld_date
left join static_routes r
       on r.network = b.network and r.route_id = coalesce(t.route_id, ld.route_id)
left join static_stops s on s.network = b.network and s.stop_id = b.stop_id
"""


def join_report(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Tasa de cruce tiempo real -> horario planificado, por red (sobre viajes distintos)."""
    return con.execute("""
        select network,
               count(distinct trip_id) as viajes,
               count(distinct trip_id) filter (where trip_matched) as viajes_cruzados,
               round(100.0 * count(distinct trip_id) filter (where trip_matched)
                     / nullif(count(distinct trip_id), 0), 1) as pct_viajes,
               round(100.0 * avg(stop_matched::int), 1) as pct_obs_estacion_ok,
               count(*) as observaciones,
               count(*) filter (where delay_wrapped) as retrasos_corregidos_24h
        from trip_delays group by network order by network""").fetchall()
