# -*- coding: utf-8 -*-
"""SQL-определения gold-слоя: 12 витрин дашборда (формулы v3).

Каждая витрина пересобирается целиком через CREATE OR REPLACE TABLE —
идемпотентно по построению.

Ключевые соглашения:
  * shared/wav match rate считаются ТОЛЬКО по строкам с запросом;
  * все доли - отношения сумм, а не средние отношений (см. правило ниже);
  * рядом с каждой долей лежат суммы, из которых она получена;
  * единица длины - МИЛИ, и это отражено в именах колонок;
  * day_of_week - по ClickHouse: 1 = понедельник ... 7 = воскресенье
    (в pandas-ноутбуке 0 = понедельник, при сверке помнить о сдвиге).
"""

# ── общие выражения ──────────────────────────────────────────────────────
OPERATOR = ("multiIf(hvfhs_license_num = 'HV0003', 'Uber', "
            "hvfhs_license_num = 'HV0005', 'Lyft', "
            "toString(hvfhs_license_num))")
WAIT = "dateDiff('second', request_datetime, pickup_datetime)"

# ── silver-фильтр: строка либо годна целиком, либо нет ───────────────────
# Ожидания здесь НЕТ намеренно. Отрицательное ожидание — это предзаказ, где
# request_datetime хранит назначенное время, а не момент бронирования. Сама
# поездка при этом состоялась: дистанция, тариф и зоны корректны. Раньше эти
# 3.9 млн строк отбрасывались целиком, и вместе с ними уходило 4.62 % ВСЕХ
# аэропортовых поездок против 1.19 % в среднем - систематическое занижение
# самого маржинального сегмента.
#
# Потолков по дистанции и длительности тоже НЕТ. Поездка на 571 милю за
# 10.3 часа (55.7 миль/ч, чек $1288, выплата пропорциональна) - нормальный
# междугородний рейс, а не брак. Прежние пороги 200 миль и 6 часов
# выбрасывали 2 232 такие поездки, из них 2 134 (95.6 %) физически возможны.
# Вместо произвольных потолков - физический критерий: скорость. Быстрее
# 100 миль/ч по дорогам не ездят, и такие строки (их 71) - брак одометра,
# вплоть до 5 077 миль/ч.
#
# Нижний порог в 30 секунд оставлен сознательно: это соглашение о минимальной
# осмысленной поездке, а не физический закон. Стоит 6 055 строк (0.002 %).
SILVER = (
    "trip_miles > 0 "
    "AND trip_time >= 30 "
    "AND base_passenger_fare > 0 "
    "AND driver_pay > 0 "
    "AND trip_miles / (trip_time / 3600) <= 100"
)

# Строки, где ожидание осмысленно. Всё остальное исключается ТОЛЬКО из
# метрик ожидания, но остаётся в объёмах, выручке и географии.
WAIT_VALID = f"{WAIT} BETWEEN 0 AND 7200"

# Признак предзаказа — побочная выгода: в исходных данных такого поля нет.
SCHEDULED = f"{WAIT} < 0"

# Метки времени неоднозначны в час возврата на зимнее время: 02.11.2025 час
# 01:00 прожит дважды, и у 12 204 поездок dropoff оказывается «раньше» pickup.
# Длительность при этом верна - trip_time пишется секундомером, а не разностью
# меток (тождество dropoff - pickup = trip_time - 3600 выполняется точно).
# Поэтому строки остаются, а флаг помечает, что арифметике МЕТОК верить нельзя.
DURATION_VALID = "dropoff_datetime > pickup_datetime"
TOTAL_FARE = ("(base_passenger_fare + tolls + bcf + sales_tax "
              "+ congestion_surcharge + airport_fee + cbd_congestion_fee)")

# ── справочник зон (dimension) ───────────────────────────────────────────
ZONES_DDL = """
CREATE OR REPLACE TABLE nyc.zones (
    location_id  UInt16,
    borough      LowCardinality(String),
    zone         String,
    service_zone LowCardinality(String)
) ENGINE = MergeTree ORDER BY location_id
"""

# ── ПРАВИЛО ОТНОШЕНИЙ ────────────────────────────────────────────────────
# Любая доля считается как ОТНОШЕНИЕ СУММ, а не среднее отношений.
# avg(a/b) смещено: поездка на 0.1 мили за $8 даёт $80/милю и весит столько же,
# сколько 20-мильная. На реальных данных avg(fare/miles) давало $7.82 против
# правильных $5.44 - завышение на 44 %.
#
# Кроме того, рядом с готовой долей лежат СУММЫ, из которых она получена.
# Долю в строке можно читать как есть, но при агрегации по времени BI обязан
# пересчитать её как sum(числитель)/sum(знаменатель) — иначе спокойный вторник
# весит столько же, сколько предпраздничная суббота (ошибка до 4.2 % на доле
# чаевых и 1.9 % на среднем чеке).
MARTS: dict[str, tuple[str, str]] = {

    # 1. День x оператор - вкладка Overview
    "mart_overview_daily": ("(trip_date, operator)", f"""
        SELECT toDate(pickup_datetime)              AS trip_date,
               {OPERATOR}                           AS operator,
               count()                              AS trips,
               countIf({SCHEDULED})                 AS scheduled_rides,
               countIf({WAIT_VALID})                AS trips_with_wait,
               round(quantileTDigestIf(0.5)({WAIT}, {WAIT_VALID}), 1) AS wait_p50,
               round(quantileTDigestIf(0.9)({WAIT}, {WAIT_VALID}), 1) AS wait_p90,
               round(avg(trip_miles), 3)            AS avg_distance_miles,
               round(avg(trip_time) / 60, 2)        AS avg_duration_min,
               round(sum(trip_miles) / (sum(trip_time) / 3600), 2) AS avg_speed_mph,
               round(avg({TOTAL_FARE}), 2)          AS avg_fare,
               round(sum(trip_miles), 2)            AS sum_miles,
               sum(trip_time)                       AS sum_time_sec,
               round(sum({TOTAL_FARE}), 2)          AS sum_total_fare
        FROM nyc.trips
        WHERE {SILVER}
        GROUP BY trip_date, operator
    """),

    # 2. Месяц x час x день недели x оператор - вкладка Demand & Wait
    "mart_hourly_pattern": ("(month, hour_of_day, day_of_week, operator)", f"""
        SELECT toStartOfMonth(pickup_datetime)      AS month,
               toHour(pickup_datetime)              AS hour_of_day,
               toDayOfWeek(pickup_datetime)         AS day_of_week,
               {OPERATOR}                           AS operator,
               count()                              AS trips,
               countIf({SCHEDULED})                 AS scheduled_rides,
               countIf({WAIT_VALID})                AS trips_with_wait,
               round(quantileTDigestIf(0.5)({WAIT}, {WAIT_VALID}), 1) AS wait_p50,
               round(quantileTDigestIf(0.9)({WAIT}, {WAIT_VALID}), 1) AS wait_p90,
               countIf({WAIT} > 600 AND {WAIT_VALID}) AS trips_wait_gt10,
               countIf({WAIT} > 900 AND {WAIT_VALID}) AS trips_wait_gt15,
               round(avgIf({WAIT} > 600, {WAIT_VALID}), 4) AS share_wait_gt10,
               round(avgIf({WAIT} > 900, {WAIT_VALID}), 4) AS share_wait_gt15,
               round(avg(trip_time) / 60, 2)        AS avg_duration_min,
               round(sum(trip_miles) / (sum(trip_time) / 3600), 2) AS avg_speed_mph,
               round(avg({TOTAL_FARE}), 2)          AS avg_fare,
               countIf(shared_request_flag)         AS shared_requests,
               countIf(shared_request_flag
                       AND shared_match_flag)       AS shared_matches,
               countIf(wav_request_flag)            AS wav_requests,
               countIf(wav_request_flag
                       AND wav_match_flag)          AS wav_matches,
               round(sum(trip_miles), 2)            AS sum_miles,
               sum(trip_time)                       AS sum_time_sec,
               round(sum({TOTAL_FARE}), 2)          AS sum_total_fare
        FROM nyc.trips
        WHERE {SILVER}
        GROUP BY month, hour_of_day, day_of_week, operator
    """),

    # 3. День x зона посадки x оператор - вкладка GEO
    "mart_pickup_zones": ("(trip_date, pu_location_id, operator)", f"""
        SELECT toDate(pickup_datetime)              AS trip_date,
               PULocationID                         AS pu_location_id,
               z.zone                               AS pu_zone_name,
               z.borough                            AS pu_borough,
               {OPERATOR}                           AS operator,
               count()                              AS trips,
               countIf({SCHEDULED})                 AS scheduled_rides,
               countIf({WAIT_VALID})                AS trips_with_wait,
               round(quantileTDigestIf(0.5)({WAIT}, {WAIT_VALID}), 1) AS wait_p50,
               round(quantileTDigestIf(0.9)({WAIT}, {WAIT_VALID}), 1) AS wait_p90,
               round(avg({TOTAL_FARE}), 2)          AS avg_fare,
               round(avg(trip_miles), 3)            AS avg_distance_miles,
               round(sum(trip_miles), 2)            AS sum_miles,
               round(sum({TOTAL_FARE}), 2)          AS sum_total_fare
        FROM nyc.trips
        LEFT JOIN nyc.zones AS z ON PULocationID = z.location_id
        WHERE {SILVER}
        GROUP BY trip_date, pu_location_id, pu_zone_name, pu_borough, operator
    """),

    # 4. Месяц x маршрут PU->DO - вкладка GEO
    "mart_routes": ("(month, pu_zone_name, do_zone_name)", f"""
        SELECT toStartOfMonth(pickup_datetime)      AS month,
               zp.zone                              AS pu_zone_name,
               zd.zone                              AS do_zone_name,
               count()                              AS trips
        FROM nyc.trips
        LEFT JOIN nyc.zones AS zp ON PULocationID = zp.location_id
        LEFT JOIN nyc.zones AS zd ON DOLocationID = zd.location_id
        WHERE {SILVER}
        GROUP BY month, pu_zone_name, do_zone_name
    """),

    # 5. День x оператор, компоненты цены - вкладка Economics.
    #    Сумма avg_base_fare + avg_tax + avg_congestion + avg_airport_fee
    #    + avg_toll + avg_bcf обязана сходиться с avg_total_fare.
    "mart_pricing": ("(trip_date, operator)", f"""
        SELECT toDate(pickup_datetime)                               AS trip_date,
               {OPERATOR}                                            AS operator,
               count()                                               AS trips,
               round(avg(base_passenger_fare), 2)                    AS avg_base_fare,
               round(avg(sales_tax), 2)                              AS avg_tax,
               round(avg(congestion_surcharge + cbd_congestion_fee), 2) AS avg_congestion,
               round(avg(airport_fee), 2)                            AS avg_airport_fee,
               round(avg(tolls), 2)                                  AS avg_toll,
               round(avg(bcf), 2)                                    AS avg_bcf,
               round(avg(tips), 2)                                   AS avg_tip,
               round(avg(driver_pay), 2)                             AS avg_driver_pay,
               round(avg({TOTAL_FARE}), 2)                           AS avg_total_fare,
               round(avg(base_passenger_fare - driver_pay), 2)       AS avg_platform_gross_take,
               round(avg(bcf + sales_tax + congestion_surcharge
                         + cbd_congestion_fee), 2)                   AS avg_regulatory_load,
               round(avg(tolls + airport_fee), 2)                    AS avg_pass_through,
               round(sum(base_passenger_fare) / sum(trip_miles), 2)  AS fare_per_mile,
               round(sum(tips) / sum(base_passenger_fare), 4)        AS tip_rate_on_fare,
               round(sum(tips) / sum({TOTAL_FARE}), 4)               AS tips_to_charges,
               round(sum(tips) / (sum({TOTAL_FARE}) + sum(tips)), 4) AS tips_share_of_paid,
               round(sum(driver_pay) / sum(base_passenger_fare), 4)  AS driver_pay_ratio,
               round(sum(base_passenger_fare), 2)                    AS sum_base_fare,
               round(sum({TOTAL_FARE}), 2)                           AS sum_total_fare,
               round(sum(tips), 2)                                   AS sum_tips,
               round(sum(driver_pay), 2)                             AS sum_driver_pay,
               round(sum(trip_miles), 2)                             AS sum_miles
        FROM nyc.trips
        WHERE {SILVER}
        GROUP BY trip_date, operator
    """),

    # 6. Месяц x сегмент x оператор — вкладка Economics.
    #    Сегменты взаимоисключающие, поездка попадает в ПЕРВЫЙ подошедший:
    #      Airport   - посадка ИЛИ высадка в аэропорту (зоны 1/132/138, те же,
    #                  что в mart_airports; шире, чем airport_fee > 0 — сбор
    #                  берут только с посадок, высадки он не видит);
    #      Manhattan - поездка касается Манхэттена любым концом;
    #      Regular   - всё остальное.
    #    Прежний сегмент Shared убран (ревизия 23.08): признак «запрошен
    #    шеринг» ортогонален географии и смешивал сегментацию; шеринг живёт
    #    в mart_shared_funnel и mart_shared_by_zone.
    "mart_economics_segment": ("(month, segment, operator)", f"""
        SELECT toStartOfMonth(pickup_datetime)      AS month,
               multiIf(PULocationID IN (1, 132, 138)
                       OR DOLocationID IN (1, 132, 138), 'Airport',
                       zpu.borough = 'Manhattan'
                       OR zdo.borough = 'Manhattan',     'Manhattan',
                                                         'Regular') AS segment,
               {OPERATOR}                           AS operator,
               count()                              AS trips,
               round(avg({TOTAL_FARE}), 2)          AS avg_fare,
               round(avg(driver_pay), 2)            AS avg_driver_pay,
               round(sum(base_passenger_fare) / sum(trip_miles), 2) AS fare_per_mile,
               round(sum(tips) / sum(base_passenger_fare), 4)       AS tip_rate_on_fare,
               round(sum(driver_pay) / sum(base_passenger_fare), 4) AS driver_pay_ratio,
               round(sum(base_passenger_fare), 2)   AS sum_base_fare,
               round(sum({TOTAL_FARE}), 2)          AS sum_total_fare,
               round(sum(tips), 2)                  AS sum_tips,
               round(sum(driver_pay), 2)            AS sum_driver_pay,
               round(sum(trip_miles), 2)            AS sum_miles
        FROM nyc.trips
        LEFT JOIN nyc.zones AS zpu ON PULocationID = zpu.location_id
        LEFT JOIN nyc.zones AS zdo ON DOLocationID = zdo.location_id
        WHERE {SILVER}
        GROUP BY month, segment, operator
    """),

    # 7. День x оператор - вкладка Shared & WAV.
    #
    # Витрина объединяет:
    #   1) adoption / matching Shared и WAV;
    #   2) Access-A-Ride;
    #   3) service quality (ожидание) для отдельных cohort'ов.
    #
    # Grain:
    #   trip_date x operator
    #
    # ВАЖНО:
    #   * WAIT_VALID = ожидание от 0 до 7200 секунд;
    #   * отрицательное ожидание (предзаказ) не входит в wait-метрики,
    #     но сама поездка остаётся в trips и feature-counts;
    #   * среднее ожидание в DataLens считается как
    #       SUM(sum_wait_sec) / SUM(trips_with_wait),
    #     а НЕ AVG дневных средних;
    #   * cohort'ы НЕ являются взаимоисключающими:
    #     например AAR-поездка может одновременно быть WAV requested.
    #
    # regular = поездки без Shared/WAV/AAR feature-флагов.
    #
    # Для каждого service cohort храним:
    #   *_trips_with_wait - число поездок с валидным ожиданием;
    #   *_sum_wait_sec    - суммарное ожидание в секундах;
    #   *_wait_gt10      - число ожиданий > 10 минут;
    #   *_wait_gt15      - число ожиданий > 15 минут.
    #
    # Это позволяет корректно пересчитывать service-метрики
    # для любого выбранного периода и оператора.
    "mart_shared_wav_daily": ("(trip_date, operator)", f"""
        SELECT
               toDate(pickup_datetime)               AS trip_date,
               {OPERATOR}                            AS operator,

               /* =========================================================
                  ОБЩИЙ DENOMINATOR
                  ========================================================= */
               count()                               AS trips,


               /* =========================================================
                  SHARED
                  ========================================================= */

               /* Пользователь явно запросил Shared */
               countIf(shared_request_flag)
                                                     AS shared_requests,

               /* Match только среди поездок, где Shared был запрошен */
               countIf(
                   shared_request_flag
                   AND shared_match_flag
               )                                     AS shared_requested_matches,

               /* shared_match без explicit shared_request:
                  сохраняем отдельно как особенность / DQ источника */
               countIf(
                   NOT shared_request_flag
                   AND shared_match_flag
               )                                     AS shared_orphan_matches,

               /* Все фактически matched Shared поездки,
                  независимо от наличия explicit request */
               countIf(shared_match_flag)
                                                     AS actual_shared_trips,


               /* =========================================================
                  WAV
                  ========================================================= */

               /* Пользователь явно запросил WAV */
               countIf(wav_request_flag)
                                                     AS wav_requests,

               /* WAV match среди explicit WAV requests */
               countIf(
                   wav_request_flag
                   AND wav_match_flag
               )                                     AS wav_requested_matches,

               /* wav_match без explicit wav_request —
                  это отдельное штатно встречающееся состояние */
               countIf(
                   NOT wav_request_flag
                   AND wav_match_flag
               )                                     AS wav_unrequested_matches,

               /* Все поездки с wav_match_flag */
               countIf(wav_match_flag)
                                                     AS actual_wav_trips,


               /* =========================================================
                  ACCESS-A-RIDE
                  ========================================================= */

               /* Access-A-Ride — отдельный признак, не равен WAV */
               countIf(access_a_ride_flag)
                                                     AS access_a_ride_trips,

               /* AAR, где одновременно был explicit WAV request */
               countIf(
                   access_a_ride_flag
                   AND wav_request_flag
               )                                     AS access_a_ride_wav_requests,

               /* AAR + explicit WAV request + successful WAV match */
               countIf(
                   access_a_ride_flag
                   AND wav_request_flag
                   AND wav_match_flag
               )                                     AS access_a_ride_wav_requested_matches,

               /* Все AAR-поездки с wav_match_flag,
                  независимо от explicit WAV request */
               countIf(
                   access_a_ride_flag
                   AND wav_match_flag
               )                                     AS access_a_ride_wav_matches,


               /* =========================================================
                  SERVICE QUALITY — ALL RIDES
                  ---------------------------------------------------------
                  Baseline по всей системе.
                  ========================================================= */

               countIf(
                   {WAIT_VALID}
               )                                     AS all_trips_with_wait,

               sumIf(
                   {WAIT},
                   {WAIT_VALID}
               )                                     AS all_sum_wait_sec,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 600
               )                                     AS all_wait_gt10,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 900
               )                                     AS all_wait_gt15,


               /* =========================================================
                  SERVICE QUALITY — REGULAR RIDES
                  ---------------------------------------------------------
                  Строгий baseline "обычной" поездки:
                  ни Shared request/match,
                  ни WAV request/match,
                  ни Access-A-Ride.
                  ========================================================= */

               countIf(
                   {WAIT_VALID}
                   AND NOT shared_request_flag
                   AND NOT shared_match_flag
                   AND NOT wav_request_flag
                   AND NOT wav_match_flag
                   AND NOT access_a_ride_flag
               )                                     AS regular_trips_with_wait,

               sumIf(
                   {WAIT},
                   {WAIT_VALID}
                   AND NOT shared_request_flag
                   AND NOT shared_match_flag
                   AND NOT wav_request_flag
                   AND NOT wav_match_flag
                   AND NOT access_a_ride_flag
               )                                     AS regular_sum_wait_sec,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 600
                   AND NOT shared_request_flag
                   AND NOT shared_match_flag
                   AND NOT wav_request_flag
                   AND NOT wav_match_flag
                   AND NOT access_a_ride_flag
               )                                     AS regular_wait_gt10,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 900
                   AND NOT shared_request_flag
                   AND NOT shared_match_flag
                   AND NOT wav_request_flag
                   AND NOT wav_match_flag
                   AND NOT access_a_ride_flag
               )                                     AS regular_wait_gt15,


               /* =========================================================
                  SERVICE QUALITY — SHARED REQUESTED + MATCHED
                  ---------------------------------------------------------
                  Пользователь запросил Shared и система действительно
                  нашла match.
                  ========================================================= */

               countIf(
                   {WAIT_VALID}
                   AND shared_request_flag
                   AND shared_match_flag
               )                                     AS shared_matched_trips_with_wait,

               sumIf(
                   {WAIT},
                   {WAIT_VALID}
                   AND shared_request_flag
                   AND shared_match_flag
               )                                     AS shared_matched_sum_wait_sec,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 600
                   AND shared_request_flag
                   AND shared_match_flag
               )                                     AS shared_matched_wait_gt10,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 900
                   AND shared_request_flag
                   AND shared_match_flag
               )                                     AS shared_matched_wait_gt15,


               /* =========================================================
                  SERVICE QUALITY — SHARED REQUESTED + UNMATCHED
                  ---------------------------------------------------------
                  Shared был запрошен, но второго пассажира не нашли.
                  Позволяет проверить, получает ли пользователь двойной
                  негативный эффект: no match + большее ожидание.
                  ========================================================= */

               countIf(
                   {WAIT_VALID}
                   AND shared_request_flag
                   AND NOT shared_match_flag
               )                                     AS shared_unmatched_trips_with_wait,

               sumIf(
                   {WAIT},
                   {WAIT_VALID}
                   AND shared_request_flag
                   AND NOT shared_match_flag
               )                                     AS shared_unmatched_sum_wait_sec,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 600
                   AND shared_request_flag
                   AND NOT shared_match_flag
               )                                     AS shared_unmatched_wait_gt10,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 900
                   AND shared_request_flag
                   AND NOT shared_match_flag
               )                                     AS shared_unmatched_wait_gt15,


               /* =========================================================
                  SERVICE QUALITY — WAV REQUESTED
                  ---------------------------------------------------------
                  Главный accessibility cohort:
                  пассажир явно запросил WAV.
                  ========================================================= */

               countIf(
                   {WAIT_VALID}
                   AND wav_request_flag
               )                                     AS wav_requested_trips_with_wait,

               sumIf(
                   {WAIT},
                   {WAIT_VALID}
                   AND wav_request_flag
               )                                     AS wav_requested_sum_wait_sec,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 600
                   AND wav_request_flag
               )                                     AS wav_requested_wait_gt10,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 900
                   AND wav_request_flag
               )                                     AS wav_requested_wait_gt15,


               /* =========================================================
                  SERVICE QUALITY — ACCESS-A-RIDE
                  ---------------------------------------------------------
                  Отдельный accessibility cohort; AAR не приравниваем к WAV.
                  ========================================================= */

               countIf(
                   {WAIT_VALID}
                   AND access_a_ride_flag
               )                                     AS aar_trips_with_wait,

               sumIf(
                   {WAIT},
                   {WAIT_VALID}
                   AND access_a_ride_flag
               )                                     AS aar_sum_wait_sec,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 600
                   AND access_a_ride_flag
               )                                     AS aar_wait_gt10,

               countIf(
                   {WAIT_VALID}
                   AND {WAIT} > 900
                   AND access_a_ride_flag
               )                                     AS aar_wait_gt15,


               /* =========================================================
                  ГОТОВЫЕ ДНЕВНЫЕ FEATURE-RATES
                  ---------------------------------------------------------
                  Удобны для QA/просмотра отдельной строки.
                  В DataLens при нескольких днях НЕ делать AVG этих полей:
                  пересчитывать доли из SUM соответствующих counts.
                  ========================================================= */

               round(
                   shared_requests
                   / nullIf(trips, 0),
                   4
               )                                     AS shared_request_rate,

               round(
                   shared_requested_matches
                   / nullIf(shared_requests, 0),
                   4
               )                                     AS shared_match_rate,

               round(
                   actual_shared_trips
                   / nullIf(trips, 0),
                   4
               )                                     AS actual_shared_rate,

               round(
                   wav_requests
                   / nullIf(trips, 0),
                   4
               )                                     AS wav_request_rate,

               round(
                   wav_requested_matches
                   / nullIf(wav_requests, 0),
                   4
               )                                     AS wav_fulfillment_rate,

               round(
                   actual_wav_trips
                   / nullIf(trips, 0),
                   4
               )                                     AS actual_wav_rate,

               round(
                   access_a_ride_trips
                   / nullIf(trips, 0),
                   4
               )                                     AS access_a_ride_rate

        FROM nyc.trips
        WHERE {SILVER}

        GROUP BY
            trip_date,
            operator
    """),
    # 8. День x зона посадки x оператор - Shared/WAV/Accessibility geography.
    #
    # В отличие от прежней mart_shared_by_zone:
    #   * НЕ фильтруем только shared_request_flag - нужны ВСЕ поездки,
    #     чтобы считать penetration относительно общего спроса зоны;
    #   * сохраняем operator - глобальный selector оператора должен работать;
    #   * grain дневной - произвольный date interval работает до конкретного дня;
    #   * сохраняем pu_location_id для join с taxi_zones_datalens.
    #
    # Готовые rates здесь не храним: для карты/диаграмм DataLens должен
    # рассчитывать их из SUM(counts) для выбранного периода.
    "mart_features_by_zone": ("(trip_date, pu_location_id, operator)", f"""
        SELECT toDate(pickup_datetime)               AS trip_date,

               PULocationID                          AS pu_location_id,
               z.zone                                AS pu_zone_name,
               z.borough                             AS pu_borough,

               {OPERATOR}                            AS operator,

               /* ── denominator ─────────────────────────────── */
               count()                               AS trips,

               /* ── Shared ──────────────────────────────────── */
               countIf(shared_request_flag)          AS shared_requests,

               countIf(shared_request_flag
                       AND shared_match_flag)         AS shared_requested_matches,

               countIf(NOT shared_request_flag
                       AND shared_match_flag)         AS shared_orphan_matches,

               countIf(shared_match_flag)            AS actual_shared_trips,

               /* ── WAV ─────────────────────────────────────── */
               countIf(wav_request_flag)             AS wav_requests,

               countIf(wav_request_flag
                       AND wav_match_flag)            AS wav_requested_matches,

               countIf(NOT wav_request_flag
                       AND wav_match_flag)            AS wav_unrequested_matches,

               countIf(wav_match_flag)               AS actual_wav_trips,

               /* ── Access-A-Ride ───────────────────────────── */
               countIf(access_a_ride_flag)           AS access_a_ride_trips,

               countIf(access_a_ride_flag
                       AND wav_request_flag)          AS access_a_ride_wav_requests,

               countIf(access_a_ride_flag
                       AND wav_request_flag
                       AND wav_match_flag)            AS access_a_ride_wav_requested_matches,

               countIf(access_a_ride_flag
                       AND wav_match_flag)            AS access_a_ride_wav_matches,

               /* Полезно для дополнительных tooltip/анализа.
                  Среднее потом считаем как SUM(sum_miles)/SUM(trips),
                  а не AVG(avg_distance). */
               round(sum(trip_miles), 2)             AS sum_miles

        FROM nyc.trips
        LEFT JOIN nyc.zones AS z
            ON PULocationID = z.location_id

        WHERE {SILVER}

        GROUP BY
            trip_date,
            pu_location_id,
            pu_zone_name,
            pu_borough,
            operator
    """),
    # 9. День x час x день недели x оператор - временные паттерны
    # Shared / WAV / Access-A-Ride.
    #
    # Отдельная витрина от mart_hourly_pattern:
    # mart_hourly_pattern остаётся для Demand & Wait и агрегирован по месяцу;
    # здесь нужен trip_date, чтобы dashboard selector с двумя произвольными
    # границами периода работал математически точно.
    #
    # day_of_week по ClickHouse:
    #   1 = Monday ... 7 = Sunday.
    "mart_shared_wav_hourly": (
        "(trip_date, hour_of_day, day_of_week, operator)",
        f"""
        SELECT toDate(pickup_datetime)               AS trip_date,
               toHour(pickup_datetime)               AS hour_of_day,
               toDayOfWeek(pickup_datetime)          AS day_of_week,
               {OPERATOR}                            AS operator,

               /* ── denominator ─────────────────────────────── */
               count()                               AS trips,

               /* ── Shared ──────────────────────────────────── */
               countIf(shared_request_flag)          AS shared_requests,

               countIf(shared_request_flag
                       AND shared_match_flag)         AS shared_requested_matches,

               countIf(NOT shared_request_flag
                       AND shared_match_flag)         AS shared_orphan_matches,

               countIf(shared_match_flag)            AS actual_shared_trips,

               /* ── WAV ─────────────────────────────────────── */
               countIf(wav_request_flag)             AS wav_requests,

               countIf(wav_request_flag
                       AND wav_match_flag)            AS wav_requested_matches,

               countIf(NOT wav_request_flag
                       AND wav_match_flag)            AS wav_unrequested_matches,

               countIf(wav_match_flag)               AS actual_wav_trips,

               /* ── Access-A-Ride ───────────────────────────── */
               countIf(access_a_ride_flag)           AS access_a_ride_trips,

               countIf(access_a_ride_flag
                       AND wav_request_flag)          AS access_a_ride_wav_requests,

               countIf(access_a_ride_flag
                       AND wav_request_flag
                       AND wav_match_flag)            AS access_a_ride_wav_requested_matches,

               countIf(access_a_ride_flag
                       AND wav_match_flag)            AS access_a_ride_wav_matches

        FROM nyc.trips
        WHERE {SILVER}

        GROUP BY
            trip_date,
            hour_of_day,
            day_of_week,
            operator
    """
    ),

    # 10. Гистограмма ожидания — вкладка Demand & Wait.
    #     ВНИМАНИЕ: считается только по строкам с валидным ожиданием, поэтому
    #     колонка называется trips_with_wait, а не trips - её сумма (322.9 млн)
    #     МЕНЬШЕ общего числа поездок (326.8 млн) на величину предзаказов.
    "mart_wait_histogram": ("(month, operator, bucket_order)", f"""
        SELECT toStartOfMonth(pickup_datetime)      AS month,
               {OPERATOR}                           AS operator,
               multiIf({WAIT} <   30,  1,
                       {WAIT} <   60,  2,
                       {WAIT} <  120,  3,
                       {WAIT} <  180,  4,
                       {WAIT} <  240,  5,
                       {WAIT} <  300,  6,
                       {WAIT} <  360,  7,
                       {WAIT} <  480,  8,
                       {WAIT} <  600,  9,
                       {WAIT} <  900, 10,
                       {WAIT} < 1200, 11,
                       {WAIT} < 1500, 12,
                       {WAIT} < 1800, 13, 14)       AS bucket_order,
               ['0-30 сек', '30-60 сек', '1-2 мин', '2-3 мин',
                '3-4 мин', '4-5 мин', '5-6 мин', '6-8 мин',
                '8-10 мин', '10-15 мин', '15-20 мин', '20-25 мин',
                '25-30 мин', '30+ мин'][bucket_order] AS wait_bucket,
               count()                              AS trips_with_wait
        FROM nyc.trips
        WHERE {SILVER} AND {WAIT_VALID}
        GROUP BY month, operator, bucket_order, wait_bucket
    """),

    # 11. Тариф против дистанции — вкладка Economics.
    #     Бин 60 - ХВОСТОВОЙ (60 миль и дальше), поэтому у него есть
    #     avg_distance_miles: на scatter точку надо ставить по нему, а не по
    #     номеру бина, иначе 199 тыс. поездок со средним чеком $310 встанут
    #     в точку x=60.
    "mart_fare_distance": ("(month, operator, distance_bin_miles)", f"""
        SELECT toStartOfMonth(pickup_datetime)      AS month,
               {OPERATOR}                           AS operator,
               least(toUInt16(floor(trip_miles)), 60) AS distance_bin_miles,
               count()                              AS trips,
               round(avg(trip_miles), 3)            AS avg_distance_miles,
               round(avg(base_passenger_fare), 2)   AS avg_base_fare,
               round(avg({TOTAL_FARE}), 2)          AS avg_total_fare,
               round(quantileTDigest(0.5)(base_passenger_fare), 2)  AS fare_p50,
               round(quantileTDigest(0.9)(base_passenger_fare), 2)  AS fare_p90,
               round(avg(trip_time) / 60, 2)        AS avg_duration_min,
               round(sum(base_passenger_fare), 2)   AS sum_base_fare,
               round(sum(trip_miles), 2)            AS sum_miles
        FROM nyc.trips
        WHERE {SILVER}
        GROUP BY month, operator, distance_bin_miles
    """),

    # 12. Аэропорты - вкладка GEO.
    #     Обязательно с направлением: Ньюарк почти не отдаёт посадок (18 против
    #     2.35 млн высадок), и в срезе «только по зоне посадки» он невидим.
    #     Поездка JFK -> LGA попадает в обе строки, это осознанно.
    #     Доли считаются от ВСЕХ silver-поездок месяца (не только аэропортовых);
    #     знаменатели лежат рядом (all_trips_month, all_trips_month_operator) -
    #     при агрегации в BI долю пересчитывать как sum/знаменатель, а не
    #     усреднять готовые проценты (правило отношений в шапке файла).
    "mart_airports": ("(month, airport, direction, operator)", f"""
        WITH
        base AS (
            SELECT toStartOfMonth(pickup_datetime)  AS month,
                   {OPERATOR}                       AS operator,
                   PULocationID,
                   DOLocationID,
                   {TOTAL_FARE}                     AS total_fare_v,
                   trip_miles,
                   trip_time,
                   {WAIT}                           AS wait_sec,
                   {WAIT_VALID}                     AS wait_ok
            FROM nyc.trips
            WHERE {SILVER}
        ),
        month_totals AS (
            SELECT month,
                   count()                          AS all_trips_month
            FROM base
            GROUP BY month
        ),
        month_operator_totals AS (
            SELECT month,
                   operator,
                   count()                          AS all_trips_month_operator
            FROM base
            GROUP BY month, operator
        ),
        airport_trips AS (
            SELECT month,
                   multiIf(PULocationID = 132, 'JFK',
                           PULocationID = 138, 'LaGuardia',
                                               'Newark') AS airport,
                   'pickup'                         AS direction,
                   operator,
                   total_fare_v,
                   trip_miles,
                   trip_time,
                   wait_sec,
                   wait_ok
            FROM base
            WHERE PULocationID IN (1, 132, 138)

            UNION ALL

            SELECT month,
                   multiIf(DOLocationID = 132, 'JFK',
                           DOLocationID = 138, 'LaGuardia',
                                               'Newark') AS airport,
                   'dropoff'                        AS direction,
                   operator,
                   total_fare_v,
                   trip_miles,
                   trip_time,
                   wait_sec,
                   wait_ok
            FROM base
            WHERE DOLocationID IN (1, 132, 138)
        )
        SELECT a.month                              AS month,
               a.airport                            AS airport,
               a.direction                          AS direction,
               a.operator                           AS operator,
               count()                              AS trips,
               mt.all_trips_month                   AS all_trips_month,
               mot.all_trips_month_operator         AS all_trips_month_operator,
               round(100.0 * count()
                     / nullIf(mt.all_trips_month, 0), 4)
                                                    AS airport_share_all_trips_pct,
               round(100.0 * count()
                     / nullIf(mot.all_trips_month_operator, 0), 4)
                                                    AS airport_share_operator_trips_pct,
               round(avg(a.total_fare_v), 2)        AS avg_fare,
               round(avg(a.trip_miles), 3)          AS avg_distance_miles,
               round(avg(a.trip_time) / 60, 2)      AS avg_duration_min,
               countIf(a.wait_ok)                   AS trips_with_wait,
               round(quantileTDigestIf(0.5)(a.wait_sec, a.wait_ok), 1) AS wait_p50,
               round(quantileTDigestIf(0.9)(a.wait_sec, a.wait_ok), 1) AS wait_p90,
               round(sum(a.total_fare_v), 2)        AS sum_total_fare,
               round(sum(a.trip_miles), 2)          AS sum_miles
        FROM airport_trips AS a
        INNER JOIN month_totals AS mt
            ON a.month = mt.month
        INNER JOIN month_operator_totals AS mot
            ON a.month = mot.month AND a.operator = mot.operator
        GROUP BY a.month, a.airport, a.direction, a.operator,
                 mt.all_trips_month, mot.all_trips_month_operator
    """),
}


def build_statement(name: str) -> str:
    """CREATE OR REPLACE TABLE для витрины — атомарная полная пересборка."""
    order_by, select = MARTS[name]
    return (f"CREATE OR REPLACE TABLE nyc.{name} "
            f"ENGINE = MergeTree ORDER BY {order_by} AS {select} "
            # страховка по памяти: тяжёлый GROUP BY уходит на диск
            f"SETTINGS max_threads = 2, "
            f"max_bytes_before_external_group_by = 1500000000")
