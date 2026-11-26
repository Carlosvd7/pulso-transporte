import io
import zipfile

import duckdb

from pulso.config import Settings
from pulso.static_gtfs import build, content_hash

# Mini-GTFS que reproduce las rarezas reales de Renfe: espacios de relleno en cabeceras y
# valores (Cercanías), horas sin cero inicial y > 24 h, y excepciones de calendario.
FILES = {
    "routes.txt": "route_id,route_short_name,route_long_name,route_type,route_color  \n"
    "10T0001C1  ,C1  ,Principe Pio - Aeropuerto T4   ,2,75B6E0  \n",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
    "17000 ,Madrid-Chamartín ,40.47,-3.68\n18000,Atocha,40.40,-3.69\n",
    "trips.txt": "route_id,service_id,trip_id,trip_headsign\n"
    "10T0001C1  ,1028X ,1028X19788C1   ,   \n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    "1028X19788C1 ,8:30:00,8:31:00,17000,001\n"
    "1028X19788C1 ,25:10:00,25:10:00,18000,002\n",
    # 2026-11-23 es lunes. Servicio de lunes a miércoles, se quita el martes 24 y se añade
    # el domingo 29.
    "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
    "start_date,end_date\n1028X,1,1,1,0,0,0,0,20261123,20261129\n",
    "calendar_dates.txt": "service_id,date,exception_type\n1028X,20261124,2\n1028X,20261129,1\n",
}


def _zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in files.items():
            zf.writestr(name, text.encode("utf-8"))
    return buf.getvalue()


def _settings(tmp_path):
    return Settings(
        data_dir=tmp_path,
        poll_interval_s=30,
        flush_interval_s=600,
        http_timeout_s=5,
        user_agent="test",
    )


def test_build_cleans_renfe_quirks(tmp_path):
    settings = _settings(tmp_path)
    raw = tmp_path / "static" / "cercanias" / "raw"
    raw.mkdir(parents=True)
    zip_path = raw / "20261124T030904.zip"
    zip_path.write_bytes(_zip(FILES))

    out = build(settings, "cercanias", zip_path)
    con = duckdb.connect()

    def q(cols: str, table: str, tail: str = "") -> list[tuple]:
        path = (out / f"{table}.parquet").as_posix()
        sql = f"select {cols} from read_parquet('{path}', hive_partitioning = false) {tail}"
        return con.execute(sql).fetchall()

    assert q("*", "routes") == [
        ("10T0001C1", "C1", "Principe Pio - Aeropuerto T4", "75B6E0", "Madrid")
    ]
    # headsign vacío -> NULL
    assert q("*", "trips") == [("1028X19788C1", "10T0001C1", "1028X", None)]
    assert q("stop_sequence, arrival_s", "stop_times") == [
        (1, 8 * 3600 + 30 * 60),
        (2, 25 * 3600 + 10 * 60),
    ]
    assert q("*", "trip_summary") == [
        ("1028X19788C1", 2, "17000", "18000", 8 * 3600 + 31 * 60, 90600)
    ]
    dates = q("strftime(service_date, '%Y-%m-%d')", "service_dates", "order by 1")
    assert [d[0] for d in dates] == ["2026-11-23", "2026-11-25", "2026-11-29"]
    assert ("17000", "Madrid-Chamartín") in q("stop_id, stop_name", "stops")
    assert not list((tmp_path / "static" / "cercanias").glob("_tmp_*"))


def test_content_hash_ignores_zip_metadata():
    a = _zip(FILES)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, text in reversed(FILES.items()):
            info = zipfile.ZipInfo(name, date_time=(2030, 1, 1, 0, 0, 0))
            zf.writestr(info, text.encode("utf-8"))
    assert a != buf.getvalue()
    assert content_hash(a) == content_hash(buf.getvalue())
    changed = dict(FILES, **{"stops.txt": FILES["stops.txt"] + "19000,Sol,40.41,-3.70\n"})
    assert content_hash(_zip(changed)) != content_hash(a)
