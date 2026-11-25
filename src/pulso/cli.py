"""Línea de comandos: `pulso collect`, `pulso status`."""

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

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
