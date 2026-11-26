"""Línea de comandos: collect, produce, replay, process, quality, status, static, silver, report."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

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


def _kafka_publisher(settings, wait_s: float = 120):
    """Crea los temas (esperando a que Redpanda esté listo) y devuelve el publicador."""
    import time

    from pulso.bus import KafkaPublisher, ensure_topics

    deadline = time.monotonic() + wait_s
    while True:
        try:
            created = ensure_topics(settings.kafka_bootstrap)
            if created:
                logging.getLogger(__name__).info("temas creados: %s", created)
            break
        except Exception as exc:
            if time.monotonic() > deadline:
                raise
            logging.getLogger(__name__).info("esperando a Redpanda (%s)...", exc)
            time.sleep(5)
    return KafkaPublisher(settings.kafka_bootstrap)


def _cmd_produce(args: argparse.Namespace) -> int:
    from pulso.producer import run_producer

    settings = load_settings()
    feeds = tuple(FEEDS_BY_KEY[k] for k in args.feeds) if args.feeds else FEEDS
    run_producer(
        settings,
        _kafka_publisher(settings),
        feeds,
        max_cycles=args.cycles,
        refresh_static=not args.no_static,
    )
    return 0


def _cmd_replay(args: argparse.Namespace) -> int:
    from pulso.producer import replay

    settings = load_settings()
    replay(settings, _kafka_publisher(settings), args.since, args.until, args.speed)
    return 0


def _cmd_process(args: argparse.Namespace) -> int:
    settings = load_settings()
    if args.offline:
        from pulso.processor import replay_offline

        app = replay_offline(settings, args.since, args.until)
        print(f"Procesado sin Kafka: {dict(app.proc.counters)}")
        return 0
    from pulso.processor import run_processor

    _kafka_publisher(settings)  # asegura que existen los temas (espera a Redpanda)
    run_processor(settings, rehydrate_minutes=args.rehydrate)
    return 0


def _cmd_serve(args: argparse.Namespace) -> int:
    from pulso.api import serve

    serve(args.host, args.port)
    return 0


def _cmd_lite(args: argparse.Namespace) -> int:
    from pulso.lite import serve_lite

    serve_lite(load_settings(), args.host, args.port)
    return 0


def _cmd_quality(args: argparse.Namespace) -> int:
    from pulso.quality import format_summary, read_recent_reports, summarize

    reports = read_recent_reports(load_settings().kafka_bootstrap, args.minutes)
    if not reports:
        print(f"No hay informes de calidad en los últimos {args.minutes:g} min.")
        return 0
    print(f"Calidad de datos, últimos {args.minutes:g} min ({len(reports)} snapshots):")
    print(format_summary(summarize(reports)))
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

    p = sub.add_parser("produce", help="recoger, validar y publicar en Redpanda (+ bronze)")
    p.add_argument("--feeds", nargs="*", choices=sorted(FEEDS_BY_KEY))
    p.add_argument("--cycles", type=int, default=None, help="parar tras N vueltas")
    p.add_argument("--no-static", action="store_true", help="no refrescar el GTFS estático")
    p.set_defaults(func=_cmd_produce)

    p = sub.add_parser("replay", help="re-publicar en Redpanda el histórico de bronze")
    p.add_argument("--since", type=date.fromisoformat, help="AAAA-MM-DD (UTC)")
    p.add_argument("--until", type=date.fromisoformat, help="AAAA-MM-DD (UTC)")
    p.add_argument(
        "--speed",
        type=float,
        default=0.0,
        help="0 = lo más rápido posible; 60 = una hora en un minuto",
    )
    p.set_defaults(func=_cmd_replay)

    p = sub.add_parser("process", help="procesar en streaming: KPIs, anomalías, estado en vivo")
    p.add_argument(
        "--rehydrate",
        type=float,
        default=30,
        help="minutos de historia a releer de Redpanda al arrancar",
    )
    p.add_argument(
        "--offline",
        action="store_true",
        help="procesar el histórico de bronze sin Kafka (mismo código)",
    )
    p.add_argument("--since", type=date.fromisoformat, help="(offline) AAAA-MM-DD")
    p.add_argument("--until", type=date.fromisoformat, help="(offline) AAAA-MM-DD")
    p.set_defaults(func=_cmd_process)

    p = sub.add_parser("serve", help="API + panel web (lee data/live y data/gold)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(func=_cmd_serve)

    p = sub.add_parser("lite", help="todo en un proceso, sin Redpanda (servidores pequeños)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.set_defaults(func=_cmd_lite)

    p = sub.add_parser("quality", help="métricas de calidad recientes (desde Redpanda)")
    p.add_argument("--minutes", type=float, default=60)
    p.set_defaults(func=_cmd_quality)

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
