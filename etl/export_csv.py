# -*- coding: utf-8 -*-
"""Выгрузка витрин из ClickHouse в CSV.

Запуск:
    python etl/export_csv.py                 # все витрины в ./marts_csv
    python etl/export_csv.py --out D:/share  # другой каталог
    python etl/export_csv.py --sep ';'       # для Excel с русской локалью

Выгружает server-side через FORMAT CSVWithNames: файл пишет сам ClickHouse,
данные не проходят через Python. Крупнейшая витрина - mart_routes на ~950 тыс.
строк - выгружается за секунды.

Кодировка UTF-8 с BOM: без него Excel открывает кириллицу кракозябрами.
"""
from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

import clickhouse_connect

PROJECT = Path(__file__).resolve().parents[1]
CH = dict(host="localhost", port=8123, username="analyst",
          password="analyst", database="nyc")
CONTAINER = "nyc-clickhouse"

# Витрины и служебные таблицы качества — то, что уезжает в BI и в отчёты.
TABLES = [
    "mart_overview_daily",
    "mart_hourly_pattern",
    "mart_pickup_zones",
    "mart_routes",
    "mart_pricing",
    "mart_economics_segment",
    "mart_shared_wav_daily",
    "mart_features_by_zone",
    "mart_shared_wav_hourly",
    "mart_wait_histogram",
    "mart_fare_distance",
    "mart_airports",
]

EXTRA = ["zones", "ods_checks", "etl_load_log", "etl_anomalies",
         "etl_column_profile"]


def order_by(table: str, columns: list[str]) -> str:
    """Сортировка выгрузки: по первым измерениям, чтобы файл был читаемым."""
    dims = [c for c in ("month", "trip_date", "airport", "direction",
                        "hour_of_day", "day_of_week", "distance_bin",
                        "bucket_order", "segment", "pu_zone_name",
                        "pu_location_id", "operator", "check_group",
                        "column_name", "file_name", "location_id")
            if c in columns]
    return ", ".join(dims[:4]) if dims else columns[0]


def export(table: str, out_dir: Path, sep: str, ch) -> tuple[int, float]:
    """Выгружает одну таблицу. Возвращает (строк, секунд)."""
    cols = [r[0] for r in ch.query(
        f"SELECT name FROM system.columns "
        f"WHERE database='nyc' AND table='{table}' ORDER BY position").result_rows]
    if not cols:
        raise RuntimeError(f"таблица nyc.{table} не найдена")

    target = out_dir / f"{table}.csv"
    t0 = time.time()

    # Пишем через clickhouse-client внутри контейнера: сервер сам форматирует
    # CSV, Python не участвует в передаче строк.
    sql = (f"SELECT * FROM nyc.{table} ORDER BY {order_by(table, cols)} "
           f"FORMAT CSVWithNames "
           f"SETTINGS format_csv_delimiter = '{sep}'")
    proc = subprocess.run(
        ["docker", "exec", CONTAINER, "clickhouse-client",
         "--user", CH["username"], "--password", CH["password"],
         "--query", sql],
        capture_output=True, timeout=1800)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", "replace")[:300])

    # BOM: без него Excel не распознаёт UTF-8 и ломает кириллицу
    target.write_bytes(b"\xef\xbb\xbf" + proc.stdout.replace(b"\r\n", b"\n"))

    rows = ch.command(f"SELECT count() FROM nyc.{table}")
    return int(rows), time.time() - t0


def main() -> None:
    ap = argparse.ArgumentParser(description="Выгрузка витрин в CSV")
    ap.add_argument("--out", default=str(PROJECT / "marts_csv"),
                    help="каталог назначения")
    ap.add_argument("--sep", default=",",
                    help="разделитель; ';' удобнее для Excel с ru-локалью")
    ap.add_argument("--with-quality", action="store_true",
                    help="выгрузить и служебные таблицы качества")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    ch = clickhouse_connect.get_client(**CH)

    tables = TABLES + (EXTRA if args.with_quality else [])
    total_rows = total_bytes = 0

    print(f"каталог: {out_dir}\nразделитель: {args.sep!r}\n")
    print(f"{'витрина':26} {'строк':>10} {'размер':>10} {'время':>7}")
    print("-" * 58)

    for table in tables:
        try:
            rows, secs = export(table, out_dir, args.sep, ch)
        except Exception as exc:                       # noqa: BLE001
            print(f"{table:26} ОШИБКА: {exc}")
            continue
        size = (out_dir / f"{table}.csv").stat().st_size
        total_rows += rows
        total_bytes += size
        print(f"{table:26} {rows:>10,} {size / 1e6:>8.2f} МБ {secs:>6.1f}s")

    print("-" * 58)
    print(f"{'ИТОГО':26} {total_rows:>10,} {total_bytes / 1e6:>8.2f} МБ")


if __name__ == "__main__":
    main()
