"""GTFS estático de Renfe: descarga versionada y limpieza a Parquet.

Por qué versionado: el fichero de AV/LD se regenera a diario y elimina los viajes de días
pasados, pero el feed en tiempo real sigue publicando trenes de ayer. Si solo guardásemos
la última versión, esos trenes no se podrían cruzar con su línea. Guardamos cada versión
(identificada por su Last-Modified) y las vistas eligen, para cada clave, la más reciente.

Limpiezas que aplicamos (problemas reales del fichero de Renfe):
- Cercanías rellena todos los campos y cabeceras con espacios: se recortan.
- AV/LD escribe horas sin cero inicial ("8:30:00"): se convierten a segundos desde medianoche.
- Horas > 24 h (viajes que cruzan medianoche, estándar GTFS) se conservan en segundos.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import shutil
import zipfile
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import duckdb
import httpx

from pulso.config import STATIC_GTFS, Settings

log = logging.getLogger(__name__)

# Prefijo de route_id de Cercanías -> núcleo. Deducido de los nombres de las rutas del propio
# fichero (p. ej. 10T0001C1 = "Principe Pio - Aeropuerto T4").
NUCLEOS = {
    "10": "Madrid",
    "20": "Asturias",
    "30": "Sevilla",
    "31": "Cádiz",
    "32": "Málaga",
    "40": "Valencia",
    "41": "Murcia/Alicante",
    "45": "Cartagena (ancho métrico)",
    "46": "Ferrol (ancho métrico)",
    "47": "León (ancho métrico)",
    "51": "Rodalies de Catalunya",
    "60": "Bilbao",
    "61": "San Sebastián",
    "62": "Santander",
    "70": "Zaragoza",
}

TABLES = ("routes", "stops", "trips", "stop_times", "calendar", "calendar_dates")


def static_root(settings: Settings, network: str) -> Path:
    return settings.data_dir / "static" / network


def content_hash(zip_bytes: bytes) -> str:
    """Huella del contenido de un zip (nombre, tamaño y CRC32 de cada fichero), ignorando
    fechas y metadatos del propio zip."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        members = sorted((i.filename, i.file_size, i.CRC) for i in zf.infolist())
    return hashlib.sha256(repr(members).encode()).hexdigest()


def download(settings: Settings, network: str, client: httpx.Client | None = None) -> Path | None:
    """Descarga el zip solo si cambió (ETag / Last-Modified). Devuelve la ruta nueva o None."""
    root = static_root(settings, network)
    (root / "raw").mkdir(parents=True, exist_ok=True)
    meta_path = root / "latest.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    headers = {"User-Agent": settings.user_agent}
    if meta.get("etag"):
        headers["If-None-Match"] = meta["etag"]
    own = client is None
    client = client or httpx.Client(timeout=180, follow_redirects=True)
    try:
        resp = client.get(STATIC_GTFS[network], headers=headers)
    finally:
        if own:
            client.close()
    if resp.status_code == 304:
        log.info("%s: GTFS estático sin cambios (%s)", network, meta.get("version"))
        return None
    resp.raise_for_status()
    sha = content_hash(resp.content)
    if sha == meta.get("sha256"):
        # Renfe vuelve a comprimir el mismo contenido con otra fecha (zip distinto byte a
        # byte, ficheros idénticos): no es una versión nueva.
        log.info("%s: GTFS estático sin cambios (mismo contenido)", network)
        return None
    lm = resp.headers.get("Last-Modified")
    stamp = parsedate_to_datetime(lm) if lm else datetime.now().astimezone()
    version = f"{stamp:%Y%m%dT%H%M%S}"
    zip_path = root / "raw" / f"{version}.zip"
    zip_path.write_bytes(resp.content)
    meta = {
        "etag": resp.headers.get("ETag"),
        "version": version,
        "sha256": sha,
        "url": STATIC_GTFS[network],
    }
    meta_path.write_text(json.dumps(meta, indent=2))
    log.info("%s: nueva versión %s (%.1f MB)", network, version, len(resp.content) / 1e6)
    return zip_path


def _gtfs_time_to_s(col: str) -> str:
    """SQL: 'H:MM:SS' o 'HH:MM:SS' (también > 24 h) -> segundos desde medianoche."""
    return (
        f"(cast(split_part({col}, ':', 1) as integer) * 3600"
        f" + cast(split_part({col}, ':', 2) as integer) * 60"
        f" + cast(split_part({col}, ':', 3) as integer))"
    )


def _load_raw(con: duckdb.DuckDBPyConnection, csv: Path, name: str) -> list[str]:
    """Carga un .txt GTFS como texto, recortando nombres de columna y valores."""
    con.execute(
        f"create or replace temp table raw_{name} as select * from read_csv(?, all_varchar = true,"
        " header = true, quote = '\"')",
        [csv.as_posix()],
    )
    cols = [r[0] for r in con.execute(f"describe raw_{name}").fetchall()]
    select = ", ".join(f'nullif(trim("{c}"), \'\') as "{c.strip()}"' for c in cols)
    con.execute(f"create or replace temp table {name} as select {select} from raw_{name}")
    return [c.strip() for c in cols]


def build(settings: Settings, network: str, zip_path: Path) -> Path:
    """Limpia un zip GTFS y escribe las tablas en <root>/v=<version>/*.parquet."""
    version = zip_path.stem
    out = static_root(settings, network) / f"v={version}"
    tmp = static_root(settings, network) / f"_tmp_{version}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    try:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp)
        con = duckdb.connect()
        present = {t for t in TABLES if (tmp / f"{t}.txt").exists()}
        for t in present:
            _load_raw(con, tmp / f"{t}.txt", t)

        staging = out.with_name(out.name + ".staging")
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(parents=True)

        def write(sql: str, name: str) -> None:
            con.execute(
                f"copy ({sql}) to ? (format parquet, compression zstd)",
                [(staging / f"{name}.parquet").as_posix()],
            )

        nucleo_case = (
            "case "
            + " ".join(f"when left(route_id, 2) = '{k}' then '{v}'" for k, v in NUCLEOS.items())
            + " else null end"
        )
        if network == "cercanias":
            area = nucleo_case
        else:
            area = "'Alta velocidad / Larga y media distancia'"
        write(
            f"""
            select route_id, route_short_name as line, route_long_name,
                   route_color, {area} as area
            from routes""",
            "routes",
        )
        write(
            """
            select stop_id, stop_name, cast(stop_lat as double) as lat,
                   cast(stop_lon as double) as lon
            from stops""",
            "stops",
        )
        write(
            """
            select trip_id, route_id, service_id, trip_headsign as headsign
            from trips""",
            "trips",
        )

        # Fechas en que circula cada servicio: calendar (rango + días) +/- calendar_dates.
        parts = []
        if "calendar" in present:
            parts.append("""
                select service_id, cast(d as date) as service_date
                from calendar,
                     generate_series(strptime(start_date, '%Y%m%d'),
                                     strptime(end_date, '%Y%m%d'), interval 1 day) g(d)
                where (case isodow(d)
                         when 1 then monday when 2 then tuesday when 3 then wednesday
                         when 4 then thursday when 5 then friday when 6 then saturday
                         else sunday end) = '1'""")
        if "calendar_dates" in present:
            parts.append("""
                select service_id, cast(strptime(date, '%Y%m%d') as date)
                from calendar_dates where exception_type = '1'""")
        removed = (
            """ except select service_id, cast(strptime(date, '%Y%m%d') as date)
                from calendar_dates where exception_type = '2'"""
            if "calendar_dates" in present
            else ""
        )
        write(" union ".join(parts) + removed, "service_dates")

        con.execute(f"""
            create temp table st as
            select trip_id, stop_id, cast(stop_sequence as integer) as stop_sequence,
                   {_gtfs_time_to_s("arrival_time")} as arrival_s,
                   {_gtfs_time_to_s("departure_time")} as departure_s
            from stop_times""")
        write("select * from st order by trip_id, stop_sequence", "stop_times")
        write(
            """
            select trip_id, count(*) as n_stops,
                   arg_min(stop_id, stop_sequence) as origin_stop_id,
                   arg_max(stop_id, stop_sequence) as dest_stop_id,
                   min(departure_s) as start_s, max(arrival_s) as end_s
            from st group by trip_id""",
            "trip_summary",
        )
        con.close()

        shutil.rmtree(out, ignore_errors=True)
        staging.replace(out)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    log.info("%s: versión %s limpia en %s", network, version, out)
    return out


def refresh(settings: Settings, networks: tuple[str, ...] = tuple(STATIC_GTFS)) -> list[Path]:
    """Descarga y limpia las versiones nuevas; reconstruye zips sin procesar."""
    built = []
    for net in networks:
        download(settings, net)
        for zip_path in sorted((static_root(settings, net) / "raw").glob("*.zip")):
            if not (static_root(settings, net) / f"v={zip_path.stem}").exists():
                built.append(build(settings, net, zip_path))
    return built
