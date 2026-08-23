# -*- coding: utf-8 -*-
"""Контракт схемы FHVHV: состав колонок, целевые типы и DDL таблицы.

Единственный источник правды о том, как выглядят данные. И валидация файлов,
и DDL в ClickHouse собираются отсюда, поэтому разъехаться они не могут.
"""
from __future__ import annotations

# ── группы колонок по целевому типу ──────────────────────────────────────
TEXT_COLS = [
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
]

DATETIME_COLS = [
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
]

INT32_COLS = [
    "PULocationID",
    "DOLocationID",
]

INT64_COLS = [
    "trip_time",          # секунды, поэтому широкий тип
]

FLOAT_COLS = [
    "trip_miles",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "cbd_congestion_fee",
]

FLAG_COLS = [
    "shared_request_flag",
    "shared_match_flag",
    "access_a_ride_flag",
    "wav_request_flag",
    "wav_match_flag",
]

# ── ожидаемый состав файла: порядок как в выгрузке TLC ───────────────────
EXPECTED_COLUMNS = [
    "hvfhs_license_num", "dispatching_base_num", "originating_base_num",
    "request_datetime", "on_scene_datetime", "pickup_datetime",
    "dropoff_datetime", "PULocationID", "DOLocationID", "trip_miles",
    "trip_time", "base_passenger_fare", "tolls", "bcf", "sales_tax",
    "congestion_surcharge", "airport_fee", "tips", "driver_pay",
    "shared_request_flag", "shared_match_flag", "access_a_ride_flag",
    "wav_request_flag", "wav_match_flag", "cbd_congestion_fee",
]

# Колонки, где NULL - штатное состояние источника, а не брак:
#   originating_base_num - Lyft не заполняет (99.6 % его поездок, все 16 мес.);
#   on_scene_datetime    - Lyft не заполнял до 06.03.2025, дальше заполняет.
# Остальные 23 колонки обязаны быть без NULL - это проверяется на загрузке.
NULLABLE_COLS = {"originating_base_num", "on_scene_datetime"}

# Значения флагов в источнике: проверено на всех 327 млн строк - только Y/N.
FLAG_TRUE = "Y"


def _ch_type(col: str) -> str:
    """Целевой тип колонки в ClickHouse."""
    if col in TEXT_COLS:
        base = "String"
    elif col in DATETIME_COLS:
        base = "DateTime"
    elif col in INT32_COLS:
        base = "Int32"
    elif col in INT64_COLS:
        base = "Int64"
    elif col in FLOAT_COLS:
        base = "Float64"
    elif col in FLAG_COLS:
        base = "Bool"
    else:
        raise KeyError(f"колонка {col} не отнесена ни к одной группе типов")
    return f"Nullable({base})" if col in NULLABLE_COLS else base


# Ожидаемые типы в ClickHouse: {колонка: тип}
CH_TYPES = {col: _ch_type(col) for col in EXPECTED_COLUMNS}

TRIPS_DDL = (
    "CREATE TABLE IF NOT EXISTS nyc.trips (\n    "
    + ",\n    ".join(f"{col:22} {CH_TYPES[col]}" for col in EXPECTED_COLUMNS)
    + "\n)\nENGINE = MergeTree\n"
      "PARTITION BY toYYYYMM(pickup_datetime)\n"
      "ORDER BY (PULocationID, pickup_datetime)"
)


def select_expr() -> str:
    """SELECT-список для чтения parquet: приводит флаги Y/N к Bool.

    Остальные колонки ClickHouse приводит сам - Float64 из double,
    Int32/Int64 из целых, DateTime из TIMESTAMP.
    """
    return ",\n       ".join(
        f"{col} = '{FLAG_TRUE}' AS {col}" if col in FLAG_COLS else col
        for col in EXPECTED_COLUMNS
    )


def column_list() -> str:
    """Список колонок для INSERT INTO ... (...)."""
    return ", ".join(EXPECTED_COLUMNS)


class SchemaError(RuntimeError):
    """Файл не соответствует контракту: лишние или отсутствующие колонки."""


def check_columns(actual: list[str], file_name: str = "") -> dict:
    """Сверяет состав колонок файла с контрактом.

    Порядок колонок не важен - INSERT идёт по именам, — но о расхождении
    сообщаем: это признак, что формат выгрузки поменялся.
    """
    expected = set(EXPECTED_COLUMNS)
    got = set(actual)
    missing = sorted(expected - got)
    extra = sorted(got - expected)

    if missing or extra:
        parts = []
        if missing:
            parts.append(f"отсутствуют: {', '.join(missing)}")
        if extra:
            parts.append(f"лишние: {', '.join(extra)}")
        raise SchemaError(f"{file_name}: " + "; ".join(parts))

    return {
        "columns": len(actual),
        "order_matches": list(actual) == EXPECTED_COLUMNS,
    }
