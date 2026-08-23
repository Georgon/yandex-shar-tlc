# -*- coding: utf-8 -*-
"""Проверки исходного файла до загрузки: NULL, пустые строки, дубликаты.

Считаются DuckDB прямо по parquet - именно там, где данные ещё «сырые».
После вставки в ClickHouse проверять NULL поздно: у не-Nullable колонок
он молча превращается в значение по умолчанию (0, '' или нулевую дату),
и признак потери данных исчезает.

Всё считается ОДНИМ проходом по файлу: 25 счётчиков NULL, счётчик полностью
пустых строк и точный счётчик полных дубликатов (~10 c на 21 млн строк).
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from schema import EXPECTED_COLUMNS, NULLABLE_COLS

# Память DuckDB ограничена: рядом живут ClickHouse и контейнеры DataLens.
DUCKDB_MEMORY_LIMIT = "1500MB"


def profile_file(path: Path) -> dict:
    """Профилирует parquet-файл. Возвращает счётчики и список нарушений."""
    cols = EXPECTED_COLUMNS
    quoted = ", ".join(f'"{c}"' for c in cols)

    null_counts = ",\n  ".join(
        f'count(*) FILTER ("{c}" IS NULL) AS "null_{c}"' for c in cols)
    all_null = " AND ".join(f'"{c}" IS NULL' for c in cols)

    sql = f"""
    SELECT
      count(*) AS n_rows,
      {null_counts},
      count(*) FILTER ({all_null})            AS n_empty_rows,
      count(*) - count(DISTINCT ({quoted}))   AS n_duplicate_rows
    FROM read_parquet('{path.as_posix()}')
    """

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    row = con.execute(sql).fetchone()
    con.close()

    keys = ["n_rows"] + [f"null_{c}" for c in cols] + [
        "n_empty_rows", "n_duplicate_rows"]
    res = dict(zip(keys, row))

    nulls = {c: int(res[f"null_{c}"]) for c in cols}
    n_rows = int(res["n_rows"])

    # NULL допустим только там, где это штатное поведение источника
    unexpected_nulls = {c: n for c, n in nulls.items()
                        if n and c not in NULLABLE_COLS}

    return {
        "n_rows": n_rows,
        "nulls": nulls,
        "unexpected_nulls": unexpected_nulls,
        "n_empty_rows": int(res["n_empty_rows"]),
        "n_duplicate_rows": int(res["n_duplicate_rows"]),
    }


def format_report(prof: dict) -> list[str]:
    """Человекочитаемая сводка профиля для лога Prefect."""
    n = prof["n_rows"]
    out = [f"строк: {n:,}"]

    filled = {c: v for c, v in prof["nulls"].items() if v}
    if filled:
        out.append("NULL: " + ", ".join(
            f"{c} {v:,} ({100 * v / n:.2f} %)" for c, v in filled.items()))
    else:
        out.append("NULL: нет ни в одной колонке")

    out.append(f"полностью пустых строк: {prof['n_empty_rows']:,}")
    out.append(f"полных дубликатов: {prof['n_duplicate_rows']:,}")
    return out
