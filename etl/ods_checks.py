# -*- coding: utf-8 -*-
"""ODS-проверки: слой между bronze (nyc.trips) и витринами.

Запускаются после загрузки всех партиций и до сборки витрин. Все проверки —
это countIf по одной таблице, поэтому считаются ОДНИМ проходом по 327 млн
строк вместо тридцати отдельных запросов.

Уровни:
  error   — данные испорчены, витрины строить нельзя (пайплайн падает);
  warning — реальная аномалия, но обрабатывается silver-фильтром;
  info    — наблюдение о природе данных, не дефект.

Пороги и трактовки опираются на замеры по всем 16 месяцам — см. note у каждой
проверки.
"""
from __future__ import annotations

# Фактическая разница между заявленной и фактической длительностью.
DURATION_DIFF = ("toInt64(trip_time) - "
                 "dateDiff('second', pickup_datetime, dropoff_datetime)")

# Порог расхождения trip_time с фактом: 60 секунд.
# Обоснование: распределение бимодально — 99.98 % строк укладываются в ±10 с
# (p99.9 модуля разницы = 1 с), в коридоре 11–60 с всего 129 строк на 327 млн,
# а дальше сразу 39 304 строки с расхождением больше 5 минут. Провал между
# 10 и 60 секундами и есть естественная граница.
DURATION_TOLERANCE_SEC = 60


# ── DST: ожидаемые артефакты наивного локального времени ───────────────
# Spring-forward:
# разность dropoff - pickup оказывается на час больше реального trip_time.
# Поэтому DURATION_DIFF ≈ -3600 секунд.
SPRING_DURATION_CROSS = (
    "((pickup_datetime >= '2025-03-08 00:00:00' "
    "AND pickup_datetime <  '2025-03-09 02:00:00' "
    "AND dropoff_datetime >= '2025-03-09 03:00:00' "
    "AND dropoff_datetime <  '2025-03-10 00:00:00') OR "

    "(pickup_datetime >= '2026-03-07 00:00:00' "
    "AND pickup_datetime <  '2026-03-08 02:00:00' "
    "AND dropoff_datetime >= '2026-03-08 03:00:00' "
    "AND dropoff_datetime <  '2026-03-09 00:00:00'))"
)
SPRING_DURATION_ARTIFACT = (
    f"({SPRING_DURATION_CROSS}) AND "
    f"abs(({DURATION_DIFF}) + 3600) "
    f"<= {DURATION_TOLERANCE_SEC}"
)
# Fall-back:
# разность dropoff - pickup на час КОРОЧЕ реальной длительности.
# Поэтому DURATION_DIFF ≈ +3600 секунд.
FALLBACK_DURATION_ARTIFACT = (
    "toDate(pickup_datetime) = '2025-11-02' AND "
    f"abs(({DURATION_DIFF}) - 3600) "
    f"<= {DURATION_TOLERANCE_SEC}"
)
DST_DURATION_ARTIFACT = (
    f"(({SPRING_DURATION_ARTIFACT}) "
    f"OR ({FALLBACK_DURATION_ARTIFACT}))"
)
# 02:xx физически отсутствует в spring-forward даты.
SPRING_REQUEST_GAP = (
    "((request_datetime >= '2025-03-09 02:00:00' "
    "AND request_datetime <  '2025-03-09 03:00:00') OR "
    "(request_datetime >= '2026-03-08 02:00:00' "
    "AND request_datetime <  '2026-03-08 03:00:00'))"
)
SPRING_PICKUP_GAP = (
    "((pickup_datetime >= '2025-03-09 02:00:00' "
    "AND pickup_datetime <  '2025-03-09 03:00:00') OR "
    "(pickup_datetime >= '2026-03-08 02:00:00' "
    "AND pickup_datetime <  '2026-03-08 03:00:00'))"
)

# Физически невозможная скорость: выше 100 миль/ч по городу не бывает.
IMPOSSIBLE_MPH = 100

# ── проверки: (группа, имя, выражение, уровень, трактовка) ───────────────
COUNT_CHECKS: list[tuple[str, str, str, str, str]] = [

    # ── зоны ─────────────────────────────────────────────────────────────
    ("zones", "PULocationID вне 1-265",
     "PULocationID NOT BETWEEN 1 AND 265", "error",
     "справочник TLC содержит ровно 265 зон"),
    ("zones", "DOLocationID вне 1-265",
     "DOLocationID NOT BETWEEN 1 AND 265", "error",
     "справочник TLC содержит ровно 265 зон"),
    ("zones", "PULocationID = 264 (Unknown)",
     "PULocationID = 264", "info",
     "зона не определена; исключать только в гео-разрезах"),
    ("zones", "DOLocationID = 264 (Unknown)",
     "DOLocationID = 264", "info",
     "зона не определена; исключать только в гео-разрезах"),
    ("zones", "PULocationID = 265 (Outside of NYC)",
     "PULocationID = 265", "info",
     "посадка за городом — редкость, машины лицензированы в NYC. НЕ удалять"),
    ("zones", "DOLocationID = 265 (Outside of NYC)",
     "DOLocationID = 265", "info",
     "высадка за городом — массовое штатное явление. НЕ удалять"),
    ("zones", "посадка и высадка в одной зоне",
     "PULocationID = DOLocationID", "info",
     "короткие внутризональные поездки — норма, вынести в отдельный сегмент"),

    # ── гео ──────────────────────────────────────────────────────────────
    ("geo", "trip_miles < 0", "trip_miles < 0", "error",
     "дистанция не может быть отрицательной"),
    ("geo", "trip_time < 0", "trip_time < 0", "error",
     "длительность не может быть отрицательной"),
    ("geo", "trip_miles = 0", "trip_miles = 0", "warning",
     "подача без выезда; отсекается silver-фильтром"),
    ("geo", "trip_miles > 100", "trip_miles > 100", "info",
     "межгород: до 156 миль на 99.999-м процентиле — правдоподобно"),
    ("geo", f"скорость > {IMPOSSIBLE_MPH} миль/ч",
     f"trip_time > 0 AND trip_miles / (trip_time / 3600) > {IMPOSSIBLE_MPH}",
     "warning",
     "битый одометр: встречаются значения до 5077 миль/ч"),

    # ── время ──────────────────────────────────────────────────────────── 
    ("time", "DST fall-back: dropoff < pickup",
    f"dropoff_datetime < pickup_datetime "
    f"AND ({FALLBACK_DURATION_ARTIFACT})",
    "info",
    "ожидаемый артефакт 02.11.2025: локальный час 01:xx повторён; "
    "trip_time подтверждает сдвиг +3600 с. Поездку не удалять"),
    ("time", "dropoff < pickup, не объяснён DST",
    f"dropoff_datetime < pickup_datetime "
    f"AND NOT ({FALLBACK_DURATION_ARTIFACT})",
    "warning",
    "реальная временная аномалия: DST-сигнатура +3600 с отсутствует"),
    ("time", "dropoff_datetime = pickup_datetime",
     "dropoff_datetime = pickup_datetime", "warning",
     "нулевая длительность по меткам времени"),
    ("time", "pickup_datetime < request_datetime",
     "pickup_datetime < request_datetime", "warning",
     "не ошибка часов, а две разные причины. 99.9 % — предзаказы: у них "
     "request_datetime хранит НАЗНАЧЕННОЕ время (71 % округлены до минуты, "
     "42 % до четверти часа), водитель приезжает раньше. 0.09 % — переход "
     "на зимнее время 02.11.2025, когда час 01:00 прожит дважды"),
    ("time", "час 01 в ночь возврата на зимнее время",
     "toDate(pickup_datetime) = '2025-11-02' AND toHour(pickup_datetime) = 1",
     "info",
     "данные в наивном местном времени: час прожит дважды, объём удвоен "
     "(75 292 против ~34 тыс. в обычное воскресенье) — почасовые срезы "
     "по этой дате завышены"),
    ("time", "DST spring-forward: request в несуществующем 02:xx",
    SPRING_REQUEST_GAP,
    "warning",
    "02:00-02:59 локально не существовало; поездку не удаляем, "
    "но исключаем из wait-метрик"),
    ("time", "DST spring-forward: pickup в несуществующем 02:xx",
    SPRING_PICKUP_GAP,
    "warning",
    "02:00-02:59 локально не существовало; поездку не удаляем, "
    "но исключаем из wait-метрик и строгого почасового профиля"),
    ("time", "DST spring-forward: duration_diff ≈ -3600",
    SPRING_DURATION_ARTIFACT,
    "info",
    "ожидаемый артефакт 09.03.2025 / 08.03.2026: "
    "поездка пересекла скачок 02:00 -> 03:00"),
    ("time", "DST fall-back: duration_diff ≈ +3600",
    FALLBACK_DURATION_ARTIFACT,
    "info",
    "ожидаемый артефакт 02.11.2025: "
    "повторённый час 01:xx делает разность timestamps короче на час"),
    ("time", "trip_time < 10 секунд", "trip_time < 10", "warning",
     "аномально короткие; отсекаются silver-фильтром (порог 30 с)"),
    ("time", "trip_time > 6 часов", "trip_time > 21600", "warning",
     "аномально длинные; отсекаются silver-фильтром"),
    ("time",
    f"|trip_time - факт| > {DURATION_TOLERANCE_SEC} с, не DST",
    f"abs({DURATION_DIFF}) > {DURATION_TOLERANCE_SEC} "
    f"AND NOT ({DST_DURATION_ARTIFACT})",
    "warning",
    "расхождение длительности после исключения "
    "подтверждённых DST-сдвигов"),
    ("time", "pickup вне окна 2025-01..2026-04",
     "pickup_datetime < '2025-01-01' OR pickup_datetime >= '2026-05-01'",
     "error", "партиционирование по pickup_datetime обязано это исключать"),
    ("time", "request раньше окна",
     "request_datetime < '2025-01-01'", "info",
     "заказы 31 декабря с посадкой 1 января — легитимно"),
    ("time", "dropoff позже окна",
     "dropoff_datetime >= '2026-05-01'", "info",
     "поездки, начатые 30 апреля и завершённые 1 мая — легитимно"),

    # ── финансы ──────────────────────────────────────────────────────────
    ("money", "base_passenger_fare < 0", "base_passenger_fare < 0", "warning",
     "сторно и возвраты; отсекаются silver-фильтром"),
    ("money", "driver_pay < 0", "driver_pay < 0", "warning",
     "корректировки выплат; отсекаются silver-фильтром"),
    ("money", "tips < 0", "tips < 0", "error", "чаевые не бывают отрицательными"),
    ("money", "tolls < 0", "tolls < 0", "error", "сбор не бывает отрицательным"),
    ("money", "bcf < 0", "bcf < 0", "error", "сбор не бывает отрицательным"),
    ("money", "sales_tax < 0", "sales_tax < 0", "error",
     "налог не бывает отрицательным"),
    ("money", "congestion_surcharge < 0", "congestion_surcharge < 0", "error",
     "надбавка не бывает отрицательной"),
    ("money", "airport_fee < 0", "airport_fee < 0", "error",
     "сбор не бывает отрицательным"),
    ("money", "cbd_congestion_fee < 0", "cbd_congestion_fee < 0", "error",
     "сбор не бывает отрицательным"),
    ("money", "base_passenger_fare = 0", "base_passenger_fare = 0", "warning",
     "нулевой тариф; отсекается silver-фильтром"),
    ("money", "cbd_congestion_fee до 05.01.2025",
     "cbd_congestion_fee != 0 AND pickup_datetime < '2025-01-05'", "info",
     "все такие поездки завершились ПОСЛЕ полуночи 5 января — сбор законен"),
    ("money", "congestion_surcharge и cbd одновременно",
     "congestion_surcharge > 0 AND cbd_congestion_fee > 0", "info",
     "разные программы (штат и MTA), совмещаются штатно"),

    # ── флаги ────────────────────────────────────────────────────────────
    ("flags", "wav_request без wav_match",
     "wav_request_flag AND NOT wav_match_flag", "warning",
     "запросили доступное авто и не подали — единичные случаи"),
    ("flags", "wav_match без wav_request",
     "NOT wav_request_flag AND wav_match_flag", "info",
     "доступное авто подано без запроса — массовое штатное явление"),
    ("flags", "orphan_matches",
    "NOT shared_request_flag AND shared_match_flag",
    "warning",
    "DQ-метрика: shared_match=Y при shared_request=N. "
    "В match_rate эти строки не входят, но actual_shared_rate "
    "учитывает их как фактически shared"),
    ("flags", "wav_request и access_a_ride вместе",
     "wav_request_flag AND access_a_ride_flag", "info",
     "флаги независимы: встречаются все четыре комбинации"),
]

# Максимумы: записываются как info, чтобы видеть дрейф от месяца к месяцу.
MAX_CHECKS = [
    "base_passenger_fare", "driver_pay", "tips", "tolls",
    "congestion_surcharge", "bcf", "airport_fee", "sales_tax",
    "cbd_congestion_fee", "trip_miles", "trip_time",
]

DDL = """
CREATE TABLE IF NOT EXISTS nyc.ods_checks (
    check_group   LowCardinality(String),
    check_name    String,
    rows_affected UInt64,
    share_pct     Float64,
    severity      LowCardinality(String),
    note          String,
    checked_at    DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (check_group, check_name)
"""


def count_query() -> str:
    """Один SELECT со всеми countIf: один проход по таблице."""
    parts = ",\n  ".join(
        f"countIf({expr}) AS c{i}"
        for i, (_, _, expr, _, _) in enumerate(COUNT_CHECKS))
    return f"SELECT count() AS total,\n  {parts}\nFROM nyc.trips"


def max_query() -> str:
    """Максимумы денежных и геополей."""
    parts = ",\n  ".join(f"max({c}) AS m{i}" for i, c in enumerate(MAX_CHECKS))
    return f"SELECT\n  {parts}\nFROM nyc.trips"
