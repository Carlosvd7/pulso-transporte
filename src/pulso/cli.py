"""Línea de comandos: collect, status, static, silver, report."""

from __future__ import annotations

import argparse
import logging
import sys

from pulso.config import FEEDS, FEEDS_BY_KEY, load_settings


def _cmd_collect(args: argparse.Namespace) -> int:
    from pulso.collector import Collector

    settings = load_settings()
    feeds = tuple(FEEDS_BY_KEY[k] for k in args.feeds) if args.feeds else FEEDS
    Collector(settings, feeds).run(max_cycles=args.cycles)
    return 0


def _cmd_status(_: argparse.Namespace) -> int:
    import duckdb

    settings = load_settings()
    pattern = (settings.data_dir / "bronze" / "**" / "*.parquet").as_posix()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    try:
        rows = con.sql(f"""
            select feed, count(*) as snapshots, sum(n_entities) as entidades,
                   min(fetched_at) as desde, max(fetched_at) as hasta
            from read_parquet('{pattern}', hive_partitioning = false)
            group by feed order by feed
        """).fetchall()
    except duckdb.IOException:
        print("Todavía no hay datos en bronze.")
        return 0
    for feed, n, ents, t0, t1 in rows:
        print(
            f"{feed:30s} {n:7d} snapshots {ents:9d} entidades  {t0:%Y-%m-%d %H:%M} -> "
            f"{t1:%Y-%m-%d %H:%M} UTC"
        )
    return 0


def _cmd_static(_: argparse.Namespace) -> int:
    from pulso.static_gtfs import refresh

    built = refresh(load_settings())
    print(f"{len(built)} versiones nuevas del GTFS estático procesadas.")
    return 0


def _cmd_silver(args: argparse.Namespace) -> int:
    from pulso.silver import build_silver

    written = build_silver(load_settings(), limit=args.limit)
    print(f"Filas nuevas en silver: {written or 'ninguna (nada pendiente)'}")
    return 0


def _cmd_report(_: argparse.Namespace) -> int:
    from pulso.warehouse import connect, join_report

    con = connect(load_settings())
    tables = {r[0] for r in con.execute("show tables").fetchall()}
    if "trip_delays" not in tables:
        print("Faltan datos: ejecuta antes `pulso static` y `pulso silver`.")
        return 1
    print("red        viajes  cruzados  %viajes  %estación  observaciones  corregidos±24h")
    for net, n, m, pct, pct_stop, obs, wrapped in join_report(con):
        print(
            f"{net:10s} {n:6d} {m:9d} {pct or 0:8.1f} {pct_stop or 0:10.1f} {obs:14d} {wrapped:15d}"
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pulso", description="Pulso del transporte")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("collect", help="recoger los feeds de Renfe en bucle (bronze)")
    p.add_argument("--feeds", nargs="*", choices=sorted(FEEDS_BY_KEY))
    p.add_argument("--cycles", type=int, default=None, help="parar tras N vueltas")
    p.set_defaults(func=_cmd_collect)

    p = sub.add_parser("status", help="resumen de lo recogido")
    p.set_defaults(func=_cmd_status)

    p = sub.add_parser("static", help="descargar y limpiar el GTFS estático (si cambió)")
    p.set_defaults(func=_cmd_static)

    p = sub.add_parser("silver", help="decodificar bronze pendiente a tablas silver")
    p.add_argument("--limit", type=int, default=None, help="máximo de ficheros a procesar")
    p.set_defaults(func=_cmd_silver)

    p = sub.add_parser("report", help="tasa de cruce tiempo real <-> horario planificado")
    p.set_defaults(func=_cmd_report)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
