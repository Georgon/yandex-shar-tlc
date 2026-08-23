# -*- coding: utf-8 -*-
"""Prefect-пайплайн загрузки NYC FHVHV в ClickHouse.

Схема: discover -> (convert CSV->parquet) -> load partition -> QC -> log.

Ключевые решения:
  * Данные грузятся НА СТОРОНЕ сервера ClickHouse через табличную функцию
    file(): каталог ./data смонтирован в контейнер как user_files/data,
    поэтому 21 млн строк не гоняются через Python.
  * Идемпотентность на уровне партиции (месяца): если в партиции уже ровно
    столько строк, сколько в файле, — skip; если иначе (недолив/обрыв) —
    DROP PARTITION и перезаливка. Пайплайн можно запускать сколько угодно раз.
  * Каждый файл - строка в nyc.etl_load_log, каждый QC-показатель - строка
    в nyc.etl_anomalies. Это и есть витрина mart_data_quality из спецификации.
  * Месяцы грузятся последовательно: у ClickHouse лимит 5 ГБ RAM на этой
    машине, параллельные вставки его выбьют.

Запуск:
    python etl/load_flow.py              # разовый прогон
    python etl/load_flow.py --force      # перезалить все партиции заново

"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path
import clickhouse_connect
import duckdb
import pandas as pd
import pyarrow.parquet as pq
from prefect import flow, get_run_logger, task

sys.path.insert(0, str(Path(__file__).resolve().parent))

from checks import format_report, profile_file          # noqa: E402
from marts import MARTS, ZONES_DDL, build_statement     # noqa: E402
from ods_checks import (COUNT_CHECKS, MAX_CHECKS,       # noqa: E402
                        DDL as ODS_DDL, count_query, max_query)
from schema import (CH_TYPES, EXPECTED_COLUMNS, TRIPS_DDL,  # noqa: E402
                    check_columns, column_list, select_expr)

# ── конфигурация ─────────────────────────────────────────────────────────
PROJECT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT / "data"          # смонтирован в контейнер: user_files/data
CH = dict(host="localhost", port=8123, username="analyst",
          password="analyst", database="nyc")

FILE_RE = re.compile(r"fhvhv_tripdata_(\d{4})-(\d{2})\.(csv|parquet)$")

# Состав колонок и целевые типы берутся из etl/schema.py — там же, откуда
# собирается DDL таблицы, поэтому файл, таблица и валидация не разъедутся.


def ch_client():
    # send_receive_timeout: вставка месяца занимает минуты, дефолтных 300 с мало
    return clickhouse_connect.get_client(**CH, send_receive_timeout=1800)


def drop_page_cache() -> bool:
    """Сбрасывает page cache виртуальной машины Docker. Best-effort.

    Зачем: файлы лежат на смонтированном томе, и каждое чтение parquet —
    профилирование в DuckDB, затем вставка в ClickHouse - оседает в page cache
    VM (здесь всего ~7.5 ГБ). Забитый кэш не оставляет ClickHouse места под
    буферы чтения, и вставка падает с errno 12 Cannot allocate memory.
    Между профилированием и загрузкой кэш нужно сбрасывать.

    Если docker недоступен, молча возвращает False: это оптимизация, а не
    обязательный шаг - на машине с достаточной памятью он не нужен вовсе.
    """
    try:
        r = subprocess.run(
            ["docker", "run", "--rm", "--privileged", "busybox",
             "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"],
            capture_output=True, timeout=60)
        return r.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


# ── задачи ───────────────────────────────────────────────────────────────
@task
def ensure_tables() -> None:
    """Создаёт таблицы лога загрузки и аномалий (идемпотентно)."""
    ch = ch_client()
    ch.command("""
        CREATE TABLE IF NOT EXISTS nyc.etl_load_log (
            file_name          String,
            partition_id       String,
            rows_loaded        UInt64,
            load_duration_sec  Float64,
            status             LowCardinality(String),
            message            String,
            loaded_at          DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY loaded_at
    """)
    ch.command("""
        CREATE TABLE IF NOT EXISTS nyc.etl_anomalies (
            file_name     String,
            partition_id  String,
            metric_name   String,
            rows_affected UInt64,
            share_pct     Float64,
            action_taken  LowCardinality(String),
            checked_at    DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY (partition_id, metric_name)
    """)
    # профиль NULL по каждой колонке каждого файла — считается по источнику,
    # до вставки: в ClickHouse NULL у не-Nullable колонки станет 0 или ''
    ch.command("""
        CREATE TABLE IF NOT EXISTS nyc.etl_column_profile (
            file_name    String,
            partition_id String,
            column_name  String,
            null_count   UInt64,
            null_pct     Float64,
            expected     Bool,
            checked_at   DateTime DEFAULT now()
        ) ENGINE = MergeTree ORDER BY (partition_id, column_name)
    """)


@task
def ensure_trips_table(migrate: bool = False) -> None:
    """Создаёт nyc.trips по контракту схемы и стережёт расхождение типов.

    Если таблица уже есть, но её типы разошлись с etl/schema.py, задача
    падает с перечнем расхождений. Пересоздать (с потерей данных и полной
    перезаливкой) можно только явно - флагом --migrate.
    """
    logger = get_run_logger()
    ch = ch_client()

    exists = ch.command(
        "SELECT count() FROM system.tables "
        "WHERE database = 'nyc' AND name = 'trips'")

    if exists and migrate:
        logger.warning("--migrate: nyc.trips пересоздаётся, данные будут "
                       "перезалиты из файлов")
        ch.command("DROP TABLE nyc.trips")
        exists = 0

    if not exists:
        ch.command(TRIPS_DDL)
        logger.info("nyc.trips создана по контракту схемы (%d колонок)",
                    len(EXPECTED_COLUMNS))
        return

    actual = dict(ch.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = 'nyc' AND table = 'trips'").result_rows)

    drift = [f"{c}: в таблице {actual.get(c, 'НЕТ')}, ожидается {t}"
             for c, t in CH_TYPES.items() if actual.get(c) != t]
    extra = sorted(set(actual) - set(CH_TYPES))
    if extra:
        drift.append(f"лишние колонки в таблице: {', '.join(extra)}")

    if drift:
        raise RuntimeError(
            "схема nyc.trips разошлась с etl/schema.py:\n  "
            + "\n  ".join(drift)
            + "\nЗапустите с --migrate, чтобы пересоздать таблицу и "
              "перезалить данные.")

    logger.info("схема nyc.trips соответствует контракту")


@task
def discover_files() -> list[dict]:
    """Ищет файлы fhvhv_tripdata_YYYY-MM.* в корне проекта и в ./data.

    Если для месяца есть и CSV, и parquet - берётся parquet (CSV считается
    исходником, который уже сконвертирован).
    """
    logger = get_run_logger()
    by_month: dict[str, dict] = {}
    for folder in (PROJECT, DATA_DIR):
        for p in sorted(folder.glob("fhvhv_tripdata_*")):
            m = FILE_RE.search(p.name)
            if not m:
                continue
            yyyymm, fmt = f"{m.group(1)}{m.group(2)}", m.group(3)
            cur = by_month.get(yyyymm)
            if cur is None or (fmt == "parquet" and cur["fmt"] == "csv"):
                by_month[yyyymm] = {"path": p, "yyyymm": yyyymm, "fmt": fmt}
    files = [by_month[k] for k in sorted(by_month)]
    for f in files:
        logger.info("найден %s -> партиция %s (%s)",
                    f["path"].name, f["yyyymm"], f["fmt"])
    return files


@task(retries=1, retry_delay_seconds=30)
def convert_to_parquet(csv_path: Path, yyyymm: str) -> Path:
    """CSV -> parquet потоково через DuckDB (ZSTD, ~10x сжатие).

    Файл кладётся в ./data - только оттуда его видит сервер ClickHouse.
    """
    logger = get_run_logger()
    out = DATA_DIR / f"{csv_path.stem}.parquet"
    if out.exists():
        logger.info("%s уже существует — конвертация пропущена", out.name)
        return out
    t0 = time.time()
    tmp = out.with_suffix(".parquet.tmp")
    duckdb.connect().execute(
        f"COPY (SELECT * FROM read_csv_auto('{csv_path.as_posix()}')) "
        f"TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    tmp.rename(out)   # атомарно: сервер не увидит недописанный файл
    logger.info("конвертация %s: %.0f c, %.0f МБ",
                out.name, time.time() - t0, out.stat().st_size / 1e6)
    return out


@task
def validate_file(parquet_path: Path, yyyymm: str) -> dict:
    """Гейт качества источника: состав колонок, NULL, пустые строки, дубликаты.

    Падает, если файл не соответствует контракту, — в базу такой файл не
    попадёт. Всё, что не является нарушением, записывается в
    nyc.etl_column_profile и nyc.etl_anomalies как наблюдение.
    """
    logger = get_run_logger()

    # 1. Состав колонок — из футера parquet, мгновенно
    actual = list(pq.ParquetFile(parquet_path).schema_arrow.names)
    info = check_columns(actual, parquet_path.name)   # бросит SchemaError
    logger.info("колонки: все 25 на месте, порядок %s",
                "совпадает" if info["order_matches"] else "ОТЛИЧАЕТСЯ")

    # 2. NULL / пустые строки / дубликаты — один проход по файлу
    prof = profile_file(parquet_path)
    for line in format_report(prof):
        logger.info(line)

    ch = ch_client()
    n = prof["n_rows"]

    ch.command(f"ALTER TABLE nyc.etl_column_profile DELETE "
               f"WHERE partition_id = '{yyyymm}'")
    ch.insert(
        "etl_column_profile",
        [[parquet_path.name, yyyymm, col, cnt,
          round(100 * cnt / n, 4) if n else 0.0,
          col not in prof["unexpected_nulls"]]
         for col, cnt in prof["nulls"].items()],
        column_names=["file_name", "partition_id", "column_name",
                      "null_count", "null_pct", "expected"])

    source_checks = [
        ("empty rows (все поля NULL)", prof["n_empty_rows"], "excluded"),
        ("full duplicate rows", prof["n_duplicate_rows"], "flagged"),
    ]
    names = ", ".join(f"'{name}'" for name, _, _ in source_checks)
    ch.command(f"ALTER TABLE nyc.etl_anomalies DELETE "
               f"WHERE partition_id = '{yyyymm}' AND metric_name IN ({names})")
    ch.insert(
        "etl_anomalies",
        [[parquet_path.name, yyyymm, name, cnt,
          round(100 * cnt / n, 4) if n else 0.0, action]
         for name, cnt, action in source_checks],
        column_names=["file_name", "partition_id", "metric_name",
                      "rows_affected", "share_pct", "action_taken"])

    # 3. Нарушения контракта — загрузку не продолжаем
    if prof["unexpected_nulls"]:
        raise RuntimeError(
            f"{parquet_path.name}: NULL в колонках, где их быть не должно: "
            + ", ".join(f"{c} ({v:,})"
                        for c, v in prof["unexpected_nulls"].items()))
    if prof["n_empty_rows"]:
        raise RuntimeError(
            f"{parquet_path.name}: {prof['n_empty_rows']:,} полностью пустых строк")
    if prof["n_duplicate_rows"]:
        logger.warning("%s: %s полных дубликатов строк — загружаем как есть, "
                       "помечено в etl_anomalies",
                       parquet_path.name, f"{prof['n_duplicate_rows']:,}")
    return prof


def needs_load(parquet_path: Path, yyyymm: str, force: bool = False) -> bool:
    """Нужна ли вставка партиции: её нет, она недолита или задан --force.

    Обычная функция, а не задача: используется как дешёвый предикат, чтобы
    не профилировать файл, который всё равно будет пропущен.
    """
    if force:
        return True
    expected = pq.ParquetFile(parquet_path).metadata.num_rows
    existing = ch_client().command(
        f"SELECT count() FROM nyc.trips "
        f"WHERE toYYYYMM(pickup_datetime) = {yyyymm}")
    return existing != expected


@task(retries=2, retry_delay_seconds=30)
def load_partition(parquet_path: Path, yyyymm: str, force: bool = False) -> dict:
    """Идемпотентная загрузка одного месяца в nyc.trips.

    Ретраи безопасны: недолитая партиция определяется по несовпадению числа
    строк и перезаливается целиком через DROP PARTITION.
    """
    logger = get_run_logger()
    ch = ch_client()
    expected = pq.ParquetFile(parquet_path).metadata.num_rows   # из футера, мгновенно
    existing = ch.command(
        f"SELECT count() FROM nyc.trips WHERE toYYYYMM(pickup_datetime) = {yyyymm}")

    if existing == expected and not force:
        logger.info("партиция %s: уже %s строк — skip", yyyymm, f"{existing:,}")
        return {"status": "skipped", "rows": existing, "duration": 0.0,
                "message": "партиция уже загружена полностью"}

    status = "loaded"
    if existing > 0:
        logger.warning("партиция %s: в базе %s строк, в файле %s — перезаливка",
                       yyyymm, f"{existing:,}", f"{expected:,}")
        ch.command(f"ALTER TABLE nyc.trips DROP PARTITION '{yyyymm}'")
        status = "replaced"

    # Фоновые merge после предыдущего месяца — крупный потребитель памяти;
    # вставка поверх активных merge ловит errno 12 на этой машине. Ждём тишины.
    for _ in range(24):                       # максимум 2 минуты
        if ch.command("SELECT count() FROM system.merges") == 0:
            break
        time.sleep(5)

    # Профилирование в DuckDB только что прочитало этот файл целиком и осело
    # в page cache VM. Сбрасываем — иначе ClickHouse не найдёт памяти под
    # собственные буферы чтения того же файла.
    if drop_page_cache():
        logger.info("page cache VM сброшен перед вставкой")

    rel = parquet_path.relative_to(DATA_DIR).as_posix()   # путь внутри контейнера
    t0 = time.time()
    # Однопоточное чтение parquet: параллельный ридер держит буферы всех
    # колонок по нескольким row-group сразу и на этой машине (рядом DataLens)
    # ловит errno 12 Cannot allocate memory. Медленнее, зато влезает всегда.
    ch.command(
        f"INSERT INTO nyc.trips ({column_list()}) "
        f"SELECT {select_expr()} FROM file('data/{rel}', Parquet) "
        f"SETTINGS max_threads = 1, max_insert_threads = 1, "
        f"input_format_parquet_preserve_order = 1, "
        # мелкие блоки чтения: пиковый буфер parquet-ридера ниже в разы
        f"input_format_parquet_max_block_size = 8192, "
        f"max_insert_block_size = 65536, "
        f"min_insert_block_size_rows = 65536")
    duration = time.time() - t0

    loaded = ch.command(
        f"SELECT count() FROM nyc.trips WHERE toYYYYMM(pickup_datetime) = {yyyymm}")
    if loaded != expected:
        raise RuntimeError(
            f"партиция {yyyymm}: загружено {loaded:,}, ожидалось {expected:,}")
    logger.info("партиция %s: %s строк за %.0f c", yyyymm, f"{loaded:,}", duration)
    return {"status": status, "rows": loaded, "duration": duration,
            "message": f"file('{rel}')"}


@task
def qc_partition(file_name: str, yyyymm: str) -> int:
    """Профилирование партиции -> строки в nyc.etl_anomalies.

    Все строки в bronze-слое сохраняются (action = retained/flagged);
    исключение мусора происходит на уровне витрин, а не загрузки.
    """
    ch = ch_client()
    checks = [   # (metric_name, условие, action)
        ("negative fare",        "base_passenger_fare < 0",               "flagged"),
        ("fare <= 0",            "base_passenger_fare <= 0",              "flagged"),
        ("trip_miles = 0",       "trip_miles = 0",                        "flagged"),
        ("trip_miles > 100",     "trip_miles > 100",                      "retained"),
        ("trip_time > 6h",       "trip_time > 21600",                     "flagged"),
        ("dropoff <= pickup",    "dropoff_datetime <= pickup_datetime",   "flagged"),
        ("pickup < request",     "pickup_datetime < request_datetime",    "retained"),
        ("service zones 264/265", "PULocationID >= 264 OR DOLocationID >= 264", "retained"),
        ("on_scene is null",     "on_scene_datetime IS NULL",             "retained"),
    ]
    count_ifs = ", ".join(f"countIf({cond})" for _, cond, _ in checks)
    row = ch.query(
        f"SELECT count(), {count_ifs} FROM nyc.trips "
        f"WHERE toYYYYMM(pickup_datetime) = {yyyymm}").result_rows[0]
    total, counts = row[0], row[1:]

    # Перезапуск не плодит дубликатов. Удаляем ТОЛЬКО свои метрики: строки,
    # записанные validate_file (пустые строки, дубликаты), должны уцелеть.
    names = ", ".join(f"'{name}'" for name, _, _ in checks)
    ch.command(f"ALTER TABLE nyc.etl_anomalies DELETE "
               f"WHERE partition_id = '{yyyymm}' AND metric_name IN ({names})")
    ch.insert(
        "etl_anomalies",
        [[file_name, yyyymm, name, int(cnt),
          round(100 * cnt / total, 4) if total else 0.0, action]
         for (name, _, action), cnt in zip(checks, counts)],
        column_names=["file_name", "partition_id", "metric_name",
                      "rows_affected", "share_pct", "action_taken"])
    get_run_logger().info("QC партиции %s: %d проверок записано", yyyymm, len(checks))
    return len(checks)


@task
def log_load(file_name: str, yyyymm: str, result: dict) -> None:
    """Строка в nyc.etl_load_log — источник вкладки Data Quality."""
    ch_client().insert(
        "etl_load_log",
        [[file_name, yyyymm, int(result["rows"]),
          round(result["duration"], 1), result["status"], result["message"]]],
        column_names=["file_name", "partition_id", "rows_loaded",
                      "load_duration_sec", "status", "message"])


@task
def ensure_zones_dim() -> int:
    """Справочник зон (dimension) в ClickHouse: nyc.zones, 265 строк.

    Пересоздаётся целиком — таблица крошечная, а джойны витрин зависят от неё.
    """
    ch = ch_client()
    ch.command(ZONES_DDL)
    # у служебных зон 264/265 пустые поля -> pandas читает их как NaN (float),
    # а вставка строковых колонок ждёт str
    z = pd.read_csv(PROJECT / "taxi_zone_lookup.csv").fillna("N/A")
    ch.insert("zones",
              z.values.tolist(),
              column_names=["location_id", "borough", "zone", "service_zone"])
    get_run_logger().info("nyc.zones: %d зон", len(z))
    return len(z)


@task
def run_ods_checks() -> dict:
    """ODS-слой: проверки bronze перед сборкой витрин.

    Все countIf считаются одним проходом по nyc.trips. Результат - строки в
    nyc.ods_checks. Если сработала проверка уровня error, задача падает и
    витрины не пересобираются: лучше оставить вчерашние данные, чем
    построить витрины поверх испорченных.
    """
    logger = get_run_logger()
    ch = ch_client()
    ch.command(ODS_DDL)

    row = ch.query(count_query()).result_rows[0]
    total, counts = row[0], row[1:]
    maxima = ch.query(max_query()).result_rows[0]

    rows = [[grp, name, int(cnt), round(100 * cnt / total, 6) if total else 0.0,
             sev, note]
            for (grp, name, _, sev, note), cnt in zip(COUNT_CHECKS, counts)]
    rows += [["maxima", f"max({col})", 0, 0.0, "info", f"{val}"]
             for col, val in zip(MAX_CHECKS, maxima)]

    ch.command("TRUNCATE TABLE nyc.ods_checks")
    ch.insert("ods_checks", rows,
              column_names=["check_group", "check_name", "rows_affected",
                            "share_pct", "severity", "note"])

    errors = [(name, cnt) for (_, name, _, sev, _), cnt
              in zip(COUNT_CHECKS, counts) if sev == "error" and cnt]
    warnings = sum(1 for (_, _, _, sev, _), cnt
                   in zip(COUNT_CHECKS, counts) if sev == "warning" and cnt)

    logger.info("ODS: %d проверок, ошибок %d, предупреждений %d",
                len(COUNT_CHECKS), len(errors), warnings)
    if errors:
        raise RuntimeError(
            "ODS-проверки уровня error сработали, витрины не собираем:\n  "
            + "\n  ".join(f"{name}: {cnt:,} строк" for name, cnt in errors))
    return {"errors": 0, "warnings": warnings}


@task(retries=1, retry_delay_seconds=30)
def build_marts() -> None:
    """Gold-слой: полная пересборка девяти витрин поверх silver-фильтра.

    CREATE OR REPLACE TABLE атомарен: пока витрина пересобирается, дашборд
    видит старую версию, а не пустую таблицу. Витрины строятся последовательно
    по той же причине, что и партиции: лимит памяти ClickHouse.
    """
    logger = get_run_logger()
    ch = ch_client()
    for name in MARTS:
        t0 = time.time()
        ch.command(build_statement(name))
        rows = ch.command(f"SELECT count() FROM nyc.{name}")
        logger.info("витрина %-24s %8s строк за %4.1f c",
                    name, f"{rows:,}", time.time() - t0)


# ── flow ─────────────────────────────────────────────────────────────────
@flow(name="load-fhvhv", log_prints=True)
def load_fhvhv(force: bool = False, migrate: bool = False) -> None:
    """Полный цикл: discover -> convert -> validate -> load -> qc -> marts."""
    logger = get_run_logger()
    ensure_tables()
    ensure_trips_table(migrate=migrate)

    for f in discover_files():
        path, yyyymm = f["path"], f["yyyymm"]
        if f["fmt"] == "csv":
            path = convert_to_parquet(path, yyyymm)

        # Валидация нужна только перед реальной вставкой: уже загруженную
        # партицию перепроверять незачем, а профилирование файла не бесплатно.
        if needs_load(path, yyyymm, force=force):
            validate_file(path, yyyymm)

        result = load_partition(path, yyyymm, force=force)
        if result["status"] in ("loaded", "replaced"):
            qc_partition(path.name, yyyymm)
        log_load(path.name, yyyymm, result)

    # ODS-слой: проверки bronze. Гейт перед витринами - при ошибках уровня
    # error сборка не начнётся.
    run_ods_checks()

    # gold-слой пересобирается всегда: это секунды, зато витрины гарантированно
    # соответствуют текущему bronze (в т.ч. после --force)
    ensure_zones_dim()
    build_marts()

    # итоговая сводка по партициям
    parts = ch_client().query(
        "SELECT partition, sum(rows) FROM system.parts "
        "WHERE database='nyc' AND table='trips' AND active "
        "GROUP BY partition ORDER BY partition").result_rows
    for p, rows in parts:
        logger.info("ИТОГ: партиция %s — %s строк", p, f"{rows:,}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Загрузка FHVHV в ClickHouse")
    ap.add_argument("--force", action="store_true",
                    help="перезалить партиции, даже если строки сходятся")
    ap.add_argument("--migrate", action="store_true",
                    help="пересоздать nyc.trips по etl/schema.py "
                         "(данные будут перезалиты из файлов)")
    args = ap.parse_args()
    load_fhvhv(force=args.force, migrate=args.migrate)