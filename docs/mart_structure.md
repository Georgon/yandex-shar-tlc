# Спецификация витрин

TLC High Volume FHV (Uber / Lyft) · проект команды ШАР

12 витрин · 226 атрибутов · данные: 2025-01 — 2026-04 (327 460 054 поездки) · сформировано 23.08.2026

Документ описывает все витрины (gold-слой) проекта: каждый атрибут, его тип, формулу расчёта и исходные колонки. Витрины лежат в ClickHouse (база nyc) и выгружаются в CSV; именно они, а не сырые данные, подключаются к DataLens.

## Как устроены данные

**1. Файлы parquet (TLC).** 16 файлов помесячных выгрузок, январь 2025 — апрель 2026. Перед загрузкой каждый файл проходит проверку: состав 25 колонок, NULL, пустые строки, полные дубликаты.

**2. Bronze — таблица nyc.trips.** 327 460 054 поездки как есть, без чистки, партиции по месяцу посадки. После загрузки — 39 ODS-проверок; ошибка блокирует сборку витрин.

**3. Silver — фильтр качества.** Применяется в WHERE каждой витрины, в таблицах не материализован. Оставляет 326 839 495 строк (99,81 %).

**4. Gold — 12 витрин nyc.mart\_\*.** Компактные агрегаты под конкретные вкладки дашборда — предмет этого документа.

### Фильтр качества (silver)

trip_miles \> 0 И trip_time \>= 30 секунд  
И base_passenger_fare \> 0 И driver_pay \> 0  
И скорость (trip_miles / trip_time) не выше 100 миль/ч

Потолков по дистанции и длительности нет намеренно: поездка на 571 милю за 10,3 часа — это нормальный межгород (56 миль/ч), а не брак. Мусор отсекается физическим критерием скорости: быстрее 100 миль/ч по дорогам не ездят. Строки с «испорченным» одним полем (предзаказы, час перевода стрелок) не выбрасываются, а помечаются — и исключаются только из тех метрик, которые это поле использует.

## Общие обозначения

| Обозначение | Что это                                                     | Смысл                                         |
|-------------|-------------------------------------------------------------|-----------------------------------------------|
| wait        | pickup_datetime − request_datetime, в секундах              | ожидание подачи машины                        |
| wait_valid  | wait от 0 до 7200 секунд                                    | ожидание осмысленно: не предзаказ и не выброс |
| total_fare  | base + tolls + bcf + sales_tax + congestion + airport + cbd | полный счёт пассажира БЕЗ чаевых              |
| operator    | HV0003 → Uber, HV0005 → Lyft                                | других лицензий в данных нет                  |

## Пять правил чтения витрин

**1. Доли — это отношения сумм.** При агрегации по времени долю нельзя усреднять по строкам — её нужно пересчитывать как сумму числителя, делённую на сумму знаменателя. Для этого рядом с каждой долей в витринах лежат колонки sum\_\*. Усреднение готовых долей даёт ошибку до 4,2 %.

**2. Квантили не переагрегируются.** wait_p50, wait_p90, fare_p50, fare_p90 верны только на гранулярности строки витрины. Среднее медиан по дням — не медиана месяца.

**3. Единицы измерения зашиты в имена.** \*\_miles — мили, \*\_min — минуты, \*\_sec — секунды, \*\_mph — мили в час. Ожидание (wait_p50 / wait_p90) хранится в секундах.

**4. Время — наивное местное нью-йоркское.** Переводы часов зашиты в данные: 02.11.2025 час 01:00 прожит дважды (объём удвоен), 09.03.2025 и 08.03.2026 час 02:00 отсутствует. Почасовые срезы по этим датам содержат артефакты.

**5. У метрик ожидания свой знаменатель.** P50/P90 считаются только по строкам trips_with_wait — без предзаказов. Показывайте этот знаменатель рядом с перцентилями.

## Витрины

### mart_overview_daily

Вкладка дашборда: Overview · Гранулярность: день × оператор · Строк: 970

Базовые KPI и динамика.

| Атрибут            | Тип               | Как считается                                  | Комментарий                                                                                                                                                            |
|--------------------|-------------------|------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| trip_date          | Date              | toDate(pickup_datetime)                        | день ПОСАДКИ: поездка через полночь целиком относится ко дню начала                                                                                                    |
| operator           | String            | hvfhs_license_num → имя                        | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                                                             |
| trips              | UInt64            | count()                                        | все поездки группы, прошедшие silver-фильтр                                                                                                                            |
| scheduled_rides    | UInt64            | countIf(wait \< 0)                             | предзаказы: request_datetime хранит назначенное время, а не момент заказа                                                                                              |
| trips_with_wait    | UInt64            | countIf(wait BETWEEN 0 AND 7200)               | знаменатель всех метрик ожидания; показывать рядом с P50/P90                                                                                                           |
| wait_p50           | Nullable(Float32) | quantileTDigestOrNullIf(0.5)(wait, wait_valid) | медиана ожидания, СЕКУНДЫ; t-digest (приближённо); НЕ переагрегируется усреднением; NULL, если в группе нет валидных ожиданий (без -OrNull был NaN, ломавший avg в BI) |
| wait_p90           | Nullable(Float32) | quantileTDigestOrNullIf(0.9)(wait, wait_valid) | 90-й перцентиль ожидания, СЕКУНДЫ; НЕ переагрегируется; NULL при пустой группе                                                                                         |
| avg_distance_miles | Float64           | avg(trip_miles)                                | средняя дистанция, МИЛИ                                                                                                                                                |
| avg_duration_min   | Float64           | avg(trip_time) / 60                            | средняя длительность, МИНУТЫ; из trip_time (секундомер поездки), а не из разности меток                                                                                |
| avg_speed_mph      | Float64           | sum(trip_miles) / (sum(trip_time) / 3600)      | мили/ч; отношение сумм — для переагрегации использовать sum_miles и sum_time_sec                                                                                       |
| avg_fare           | Float64           | avg(total_fare)                                | средний полный счёт пассажира БЕЗ чаевых                                                                                                                               |
| sum_miles          | Float64           | sum(trip_miles)                                | сумма миль — для переагрегации долей                                                                                                                                   |
| sum_time_sec       | Int64             | sum(trip_time)                                 | сумма секунд — для пересчёта скорости                                                                                                                                  |
| sum_total_fare     | Float64           | sum(total_fare)                                | сумма счетов — для переагрегации                                                                                                                                       |

### mart_hourly_pattern

Вкладка дашборда: Demand & Wait · Гранулярность: месяц × час × день недели × оператор · Строк: 5 376

Профиль спроса и ожидания; включает счётчики shared/wav для heatmap.

| Атрибут          | Тип               | Как считается                                      | Комментарий                                                                                                                                                            |
|------------------|-------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| month            | Date              | toStartOfMonth(pickup_datetime)                    | первый день месяца посадки, тип Date                                                                                                                                   |
| hour_of_day      | UInt8             | toHour(pickup_datetime)                            | 0–23, наивное местное время Нью-Йорка (см. оговорку про переводы часов)                                                                                                |
| day_of_week      | UInt8             | toDayOfWeek(pickup_datetime)                       | 1 = понедельник … 7 = воскресенье (конвенция ClickHouse; в pandas 0 = понедельник)                                                                                     |
| operator         | String            | hvfhs_license_num → имя                            | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                                                             |
| trips            | UInt64            | count()                                            | все поездки группы, прошедшие silver-фильтр                                                                                                                            |
| scheduled_rides  | UInt64            | countIf(wait \< 0)                                 | предзаказы: request_datetime хранит назначенное время, а не момент заказа                                                                                              |
| trips_with_wait  | UInt64            | countIf(wait BETWEEN 0 AND 7200)                   | знаменатель всех метрик ожидания; показывать рядом с P50/P90                                                                                                           |
| wait_p50         | Nullable(Float32) | quantileTDigestOrNullIf(0.5)(wait, wait_valid)     | медиана ожидания, СЕКУНДЫ; t-digest (приближённо); НЕ переагрегируется усреднением; NULL, если в группе нет валидных ожиданий (без -OrNull был NaN, ломавший avg в BI) |
| wait_p90         | Nullable(Float32) | quantileTDigestOrNullIf(0.9)(wait, wait_valid)     | 90-й перцентиль ожидания, СЕКУНДЫ; НЕ переагрегируется; NULL при пустой группе                                                                                         |
| trips_wait_gt10  | UInt64            | countIf(wait \> 600 AND wait_valid)                | поездок с ожиданием дольше 10 минут                                                                                                                                    |
| trips_wait_gt15  | UInt64            | countIf(wait \> 900 AND wait_valid)                | поездок с ожиданием дольше 15 минут                                                                                                                                    |
| share_wait_gt10  | Nullable(Float64) | avgOrNullIf(wait \> 600, wait_valid)               | доля долгого ожидания среди trips_with_wait; при переагрегации пересчитывать из trips_wait_gt10; NULL при пустой группе                                                |
| share_wait_gt15  | Nullable(Float64) | avgOrNullIf(wait \> 900, wait_valid)               | доля среди trips_with_wait; пересчитывать из trips_wait_gt15; NULL при пустой группе                                                                                   |
| avg_duration_min | Float64           | avg(trip_time) / 60                                | средняя длительность, МИНУТЫ; из trip_time (секундомер поездки), а не из разности меток                                                                                |
| avg_speed_mph    | Float64           | sum(trip_miles) / (sum(trip_time) / 3600)          | мили/ч; отношение сумм — для переагрегации использовать sum_miles и sum_time_sec                                                                                       |
| avg_fare         | Float64           | avg(total_fare)                                    | средний полный счёт пассажира БЕЗ чаевых                                                                                                                               |
| shared_requests  | UInt64            | countIf(shared_request_flag)                       | запросы совместной поездки                                                                                                                                             |
| shared_matches   | UInt64            | countIf(shared_request_flag AND shared_match_flag) | состыкованные ИЗ ЧИСЛА ЗАПРОШЕННЫХ: 17 254 матча без запроса (дефект источника) в числитель не попадают                                                                |
| wav_requests     | UInt64            | countIf(wav_request_flag)                          | запросы доступного авто (WAV)                                                                                                                                          |
| wav_matches      | UInt64            | countIf(wav_request_flag AND wav_match_flag)       | поданные ИЗ ЧИСЛА ЗАПРОШЕННЫХ; 28.3 млн подач без запроса не учитываются                                                                                               |
| sum_miles        | Float64           | sum(trip_miles)                                    | сумма миль — для переагрегации долей                                                                                                                                   |
| sum_time_sec     | Int64             | sum(trip_time)                                     | сумма секунд — для пересчёта скорости                                                                                                                                  |
| sum_total_fare   | Float64           | sum(total_fare)                                    | сумма счетов — для переагрегации                                                                                                                                       |

### mart_pickup_zones

Вкладка дашборда: GEO · Гранулярность: день × зона посадки × оператор · Строк: 250 254

География спроса и ожидания.

| Атрибут            | Тип                    | Как считается                                  | Комментарий                                                                                                                                                            |
|--------------------|------------------------|------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| trip_date          | Date                   | toDate(pickup_datetime)                        | день ПОСАДКИ: поездка через полночь целиком относится ко дню начала                                                                                                    |
| pu_location_id     | Int32                  | PULocationID                                   | код зоны TLC, 1–265                                                                                                                                                    |
| pu_zone_name       | String                 | JOIN nyc.zones по PULocationID                 | название зоны посадки из taxi_zone_lookup.csv                                                                                                                          |
| pu_borough         | LowCardinality(String) | JOIN nyc.zones по PULocationID                 | боро посадки                                                                                                                                                           |
| operator           | String                 | hvfhs_license_num → имя                        | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                                                             |
| trips              | UInt64                 | count()                                        | все поездки группы, прошедшие silver-фильтр                                                                                                                            |
| scheduled_rides    | UInt64                 | countIf(wait \< 0)                             | предзаказы: request_datetime хранит назначенное время, а не момент заказа                                                                                              |
| trips_with_wait    | UInt64                 | countIf(wait BETWEEN 0 AND 7200)               | знаменатель всех метрик ожидания; показывать рядом с P50/P90                                                                                                           |
| wait_p50           | Nullable(Float32)      | quantileTDigestOrNullIf(0.5)(wait, wait_valid) | медиана ожидания, СЕКУНДЫ; t-digest (приближённо); НЕ переагрегируется усреднением; NULL, если в группе нет валидных ожиданий (без -OrNull был NaN, ломавший avg в BI) |
| wait_p90           | Nullable(Float32)      | quantileTDigestOrNullIf(0.9)(wait, wait_valid) | 90-й перцентиль ожидания, СЕКУНДЫ; НЕ переагрегируется; NULL при пустой группе                                                                                         |
| avg_fare           | Float64                | avg(total_fare)                                | средний полный счёт пассажира БЕЗ чаевых                                                                                                                               |
| avg_distance_miles | Float64                | avg(trip_miles)                                | средняя дистанция, МИЛИ                                                                                                                                                |
| sum_miles          | Float64                | sum(trip_miles)                                | сумма миль — для переагрегации долей                                                                                                                                   |
| sum_total_fare     | Float64                | sum(total_fare)                                | сумма счетов — для переагрегации                                                                                                                                       |

### mart_routes

Вкладка дашборда: GEO · Гранулярность: месяц × маршрут PU→DO · Строк: 952 944

Потоки между зонами.

| Атрибут      | Тип    | Как считается                   | Комментарий                                   |
|--------------|--------|---------------------------------|-----------------------------------------------|
| month        | Date   | toStartOfMonth(pickup_datetime) | первый день месяца посадки, тип Date          |
| pu_zone_name | String | JOIN nyc.zones по PULocationID  | название зоны посадки из taxi_zone_lookup.csv |
| do_zone_name | String | JOIN nyc.zones по DOLocationID  | название зоны высадки                         |
| trips        | UInt64 | count()                         | все поездки группы, прошедшие silver-фильтр   |

### mart_pricing

Вкладка дашборда: Economics · Гранулярность: день × оператор · Строк: 970

Компоненты цены и доли.

| Атрибут                 | Тип     | Как считается                                  | Комментарий                                                                                                                     |
|-------------------------|---------|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| trip_date               | Date    | toDate(pickup_datetime)                        | день ПОСАДКИ: поездка через полночь целиком относится ко дню начала                                                             |
| operator                | String  | hvfhs_license_num → имя                        | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                      |
| trips                   | UInt64  | count()                                        | все поездки группы, прошедшие silver-фильтр                                                                                     |
| avg_base_fare           | Float64 | avg(base_passenger_fare)                       | базовый тариф до надбавок                                                                                                       |
| avg_tax                 | Float64 | avg(sales_tax)                                 | налог штата                                                                                                                     |
| avg_congestion          | Float64 | avg(congestion_surcharge + cbd_congestion_fee) | обе программы: надбавка штата (2019) + congestion pricing MTA (с 05.01.2025)                                                    |
| avg_airport_fee         | Float64 | avg(airport_fee)                               | сборы JFK/LGA/EWR                                                                                                               |
| avg_toll                | Float64 | avg(tolls)                                     | платные мосты и туннели                                                                                                         |
| avg_bcf                 | Float64 | avg(bcf)                                       | Black Car Fund — страховой фонд водителей, НЕ государственный сбор                                                              |
| avg_tip                 | Float64 | avg(tips)                                      | средние чаевые по всем поездкам                                                                                                 |
| avg_driver_pay          | Float64 | avg(driver_pay)                                | выплата водителю без чаевых и tolls                                                                                             |
| avg_total_fare          | Float64 | avg(total_fare)                                | контроль: сумма шести компонентов выше обязана сходиться с этим полем (факт: 0.001 %)                                           |
| avg_platform_gross_take | Float64 | avg(base_passenger_fare − driver_pay)          | грязный остаток платформы, НЕ прибыль: отсюда идут разработка, страхование, промо                                               |
| avg_regulatory_load     | Float64 | avg(bcf + sales_tax + congestion + cbd)        | нагрузка, взимаемая с (почти) каждой поездки: sales_tax покрывает 96.6 %                                                        |
| avg_pass_through        | Float64 | avg(tolls + airport_fee)                       | транзитные платежи, зависящие от маршрута: tolls на 11.9 % поездок, airport_fee на 8.3 %                                        |
| fare_per_mile           | Float64 | sum(base_passenger_fare) / sum(trip_miles)     | \$/миля; отношение сумм — через avg(a/b) было бы завышено на 44 %                                                               |
| tip_rate_on_fare        | Float64 | sum(tips) / sum(base_passenger_fare)           | чаевые к базовому тарифу ПО ВСЕМ поездкам (≈ 4.46 %); не путать со ставкой среди дающих (≈ 20.9 % при 18.4 % поездок с чаевыми) |
| tips_to_charges         | Float64 | sum(tips) / sum(total_fare)                    | чаевые к начислениям (знаменатель БЕЗ чаевых), ≈ 3.68 %                                                                         |
| tips_share_of_paid      | Float64 | sum(tips) / (sum(total_fare) + sum(tips))      | доля чаевых в фактически уплаченном, ≈ 3.55 %                                                                                   |
| driver_pay_ratio        | Float64 | sum(driver_pay) / sum(base_passenger_fare)     | доля водителя от базового тарифа (≈ 77 %); НЕ называть «маржой платформы»                                                       |
| sum_base_fare           | Float64 | sum(base_passenger_fare)                       | сумма базовых тарифов                                                                                                           |
| sum_total_fare          | Float64 | sum(total_fare)                                | сумма счетов — для переагрегации                                                                                                |
| sum_tips                | Float64 | sum(tips)                                      | сумма чаевых                                                                                                                    |
| sum_driver_pay          | Float64 | sum(driver_pay)                                | сумма выплат водителям                                                                                                          |
| sum_miles               | Float64 | sum(trip_miles)                                | сумма миль — для переагрегации долей                                                                                            |

### mart_economics_segment

Вкладка дашборда: Economics · Гранулярность: месяц × сегмент × оператор · Строк: 96

Экономика сегментов.

| Атрибут          | Тип     | Как считается                                                                                       | Комментарий                                                                                                                                                                  |
|------------------|---------|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| month            | Date    | toStartOfMonth(pickup_datetime)                                                                     | первый день месяца посадки, тип Date                                                                                                                                         |
| segment          | String  | multiIf(PU или DO ∈ {1,132,138} → Airport; PU или DO в районе Manhattan → Manhattan; иначе Regular) | поездка попадает в ПЕРВЫЙ подошедший сегмент; зоны аэропортов те же, что в mart_airports; сегмент Shared убран 23.08 — шеринг ортогонален географии и живёт в своих витринах |
| operator         | String  | hvfhs_license_num → имя                                                                             | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                                                                   |
| trips            | UInt64  | count()                                                                                             | все поездки группы, прошедшие silver-фильтр                                                                                                                                  |
| avg_fare         | Float64 | avg(total_fare)                                                                                     | средний полный счёт пассажира БЕЗ чаевых                                                                                                                                     |
| avg_driver_pay   | Float64 | avg(driver_pay)                                                                                     | средняя выплата водителю без чаевых и tolls                                                                                                                                  |
| fare_per_mile    | Float64 | sum(base_passenger_fare) / sum(trip_miles)                                                          | \$/миля; отношение сумм — через avg(a/b) было бы завышено на 44 %                                                                                                            |
| tip_rate_on_fare | Float64 | sum(tips) / sum(base_passenger_fare)                                                                | чаевые к базовому тарифу ПО ВСЕМ поездкам (≈ 4.46 %); не путать со ставкой среди дающих (≈ 20.9 % при 18.4 % поездок с чаевыми)                                              |
| driver_pay_ratio | Float64 | sum(driver_pay) / sum(base_passenger_fare)                                                          | доля водителя от базового тарифа (≈ 77 %); НЕ называть «маржой платформы»                                                                                                    |
| sum_base_fare    | Float64 | sum(base_passenger_fare)                                                                            | сумма базовых тарифов                                                                                                                                                        |
| sum_total_fare   | Float64 | sum(total_fare)                                                                                     | сумма счетов — для переагрегации                                                                                                                                             |
| sum_tips         | Float64 | sum(tips)                                                                                           | сумма чаевых                                                                                                                                                                 |
| sum_driver_pay   | Float64 | sum(driver_pay)                                                                                     | сумма выплат водителям                                                                                                                                                       |
| sum_miles        | Float64 | sum(trip_miles)                                                                                     | сумма миль — для переагрегации долей                                                                                                                                         |

### mart_shared_wav_daily

Вкладка дашборда: Shared & WAV · Гранулярность: день × оператор · Строк: 970

Воронки Shared, WAV и Access-A-Ride с готовыми rate для QA.

| Атрибут                             | Тип               | Как считается                                                       | Комментарий                                                                                                                        |
|-------------------------------------|-------------------|---------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| trip_date                           | Date              | toDate(pickup_datetime)                                             | день ПОСАДКИ: поездка через полночь целиком относится ко дню начала                                                                |
| operator                            | String            | hvfhs_license_num → имя                                             | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                         |
| trips                               | UInt64            | count()                                                             | все поездки группы, прошедшие silver-фильтр                                                                                        |
| shared_requests                     | UInt64            | countIf(shared_request_flag)                                        | запросы совместной поездки                                                                                                         |
| shared_requested_matches            | UInt64            | countIf(shared_request_flag AND shared_match_flag)                  | Shared состыкован СРЕДИ запросов — числитель shared_match_rate                                                                     |
| shared_orphan_matches               | UInt64            | countIf(NOT shared_request_flag AND shared_match_flag)              | матчи БЕЗ запроса — дефект источника (в silver 17 170, все у Lyft); в match rate не участвуют                                      |
| actual_shared_trips                 | UInt64            | countIf(shared_match_flag)                                          | все фактически состыкованные, с запросом и без = shared_requested_matches + shared_orphan_matches                                  |
| wav_requests                        | UInt64            | countIf(wav_request_flag)                                           | запросы доступного авто (WAV)                                                                                                      |
| wav_requested_matches               | UInt64            | countIf(wav_request_flag AND wav_match_flag)                        | WAV подан СРЕДИ явных запросов — числитель wav_fulfillment_rate                                                                    |
| wav_unrequested_matches             | UInt64            | countIf(NOT wav_request_flag AND wav_match_flag)                    | подача WAV без запроса — WAV-машина на обычном заказе, НЕ дефект                                                                   |
| actual_wav_trips                    | UInt64            | countIf(wav_match_flag)                                             | все поездки на WAV-машине, с запросом и без                                                                                        |
| access_a_ride_trips                 | UInt64            | countIf(access_a_ride_flag)                                         | поездки программы MTA Access-A-Ride; AAR НЕ тождественен WAV — отдельный флаг                                                      |
| access_a_ride_wav_requests          | UInt64            | countIf(access_a_ride_flag AND wav_request_flag)                    | пересечение: AAR-поездка с явным запросом WAV                                                                                      |
| access_a_ride_wav_requested_matches | UInt64            | countIf(access_a_ride_flag AND wav_request_flag AND wav_match_flag) | из них состыкованные с WAV                                                                                                         |
| access_a_ride_wav_matches           | UInt64            | countIf(access_a_ride_flag AND wav_match_flag)                      | AAR-поездки, выполненные WAV-машиной (с запросом и без)                                                                            |
| all_trips_with_wait                 | UInt64            | countIf(wait_valid)                                                 | baseline по всей системе — знаменатель среднего ожидания                                                                           |
| all_sum_wait_sec                    | Int64             | sumIf(wait, wait_valid)                                             | сумма секунд ожидания всех поездок; среднее за период = SUM(sec)/SUM(count)                                                        |
| all_wait_gt10                       | UInt64            | countIf(wait_valid AND wait \> 600)                                 | ожидания дольше 10 минут; доля = gt10 / trips_with_wait из сумм                                                                    |
| all_wait_gt15                       | UInt64            | countIf(wait_valid AND wait \> 900)                                 | ожидания дольше 15 минут                                                                                                           |
| regular_trips_with_wait             | UInt64            | countIf(wait_valid AND ни одного feature-флага)                     | СТРОГИЙ baseline «обычной» поездки: без Shared request/match, WAV request/match и AAR; единое определение во всех витринах с 23.08 |
| regular_sum_wait_sec                | Int64             | sumIf(wait, wait_valid AND ни одного feature-флага)                 | сумма секунд ожидания строгого baseline; среднее за период = SUM(sec)/SUM(count)                                                   |
| regular_wait_gt10                   | UInt64            | countIf(строгий baseline AND wait \> 600)                           | хвост \>10 мин обычных поездок                                                                                                     |
| regular_wait_gt15                   | UInt64            | countIf(строгий baseline AND wait \> 900)                           | хвост \>15 мин обычных поездок                                                                                                     |
| shared_matched_trips_with_wait      | UInt64            | countIf(wait_valid AND shared_request AND shared_match)             | когорта: Shared запрошен И состыкован                                                                                              |
| shared_matched_sum_wait_sec         | Int64             | sumIf(wait, та же когорта)                                          | сумма секунд ожидания                                                                                                              |
| shared_matched_wait_gt10            | UInt64            | countIf(когорта AND wait \> 600)                                    | хвост \>10 мин                                                                                                                     |
| shared_matched_wait_gt15            | UInt64            | countIf(когорта AND wait \> 900)                                    | хвост \>15 мин                                                                                                                     |
| shared_unmatched_trips_with_wait    | UInt64            | countIf(wait_valid AND shared_request AND NOT shared_match)         | когорта: Shared запрошен, но пара НЕ найдена — проверка двойного негатива no match + долгое ожидание                               |
| shared_unmatched_sum_wait_sec       | Int64             | sumIf(wait, та же когорта)                                          | сумма секунд ожидания                                                                                                              |
| shared_unmatched_wait_gt10          | UInt64            | countIf(когорта AND wait \> 600)                                    | хвост \>10 мин                                                                                                                     |
| shared_unmatched_wait_gt15          | UInt64            | countIf(когорта AND wait \> 900)                                    | хвост \>15 мин                                                                                                                     |
| wav_requested_trips_with_wait       | UInt64            | countIf(wait_valid AND wav_request_flag)                            | главная accessibility-когорта: явный запрос WAV                                                                                    |
| wav_requested_sum_wait_sec          | Int64             | sumIf(wait, wait_valid AND wav_request_flag)                        | сумма секунд ожидания WAV-запросов                                                                                                 |
| wav_requested_wait_gt10             | UInt64            | countIf(WAV-запрос AND wait \> 600)                                 | хвост \>10 мин                                                                                                                     |
| wav_requested_wait_gt15             | UInt64            | countIf(WAV-запрос AND wait \> 900)                                 | хвост \>15 мин                                                                                                                     |
| aar_trips_with_wait                 | UInt64            | countIf(wait_valid AND access_a_ride_flag)                          | когорта Access-A-Ride; НЕ приравнивается к WAV                                                                                     |
| aar_sum_wait_sec                    | Int64             | sumIf(wait, wait_valid AND access_a_ride_flag)                      | сумма секунд ожидания AAR                                                                                                          |
| aar_wait_gt10                       | UInt64            | countIf(AAR AND wait \> 600)                                        | хвост \>10 мин                                                                                                                     |
| aar_wait_gt15                       | UInt64            | countIf(AAR AND wait \> 900)                                        | хвост \>15 мин                                                                                                                     |
| shared_request_rate                 | Nullable(Float64) | shared_requests / trips                                             | доля запросов Shared от всех поездок дня                                                                                           |
| shared_match_rate                   | Nullable(Float64) | shared_requested_matches / shared_requests                          | match rate ТОЛЬКО среди запросов; NULL при нуле запросов (у Lyft запросов нет вовсе)                                               |
| actual_shared_rate                  | Nullable(Float64) | actual_shared_trips / trips                                         | доля фактического шеринга от всех поездок                                                                                          |
| wav_request_rate                    | Nullable(Float64) | wav_requests / trips                                                | доля явных запросов WAV                                                                                                            |
| wav_fulfillment_rate                | Nullable(Float64) | wav_requested_matches / wav_requests                                | фактически 99.9988 %: 11 отказов на 927 403 запроса за 16 месяцев                                                                  |
| actual_wav_rate                     | Nullable(Float64) | actual_wav_trips / trips                                            | доля поездок на WAV-машинах от всех                                                                                                |
| access_a_ride_rate                  | Nullable(Float64) | access_a_ride_trips / trips                                         | доля поездок Access-A-Ride                                                                                                         |

### mart_shared_wav_hourly

Вкладка дашборда: Shared & WAV · Гранулярность: день × час × день недели × оператор · Строк: 23 278

Временные паттерны Shared/WAV/AAR; только счётчики, доли — из сумм.

| Атрибут                             | Тип    | Как считается                                                       | Комментарий                                                                                                                        |
|-------------------------------------|--------|---------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| trip_date                           | Date   | toDate(pickup_datetime)                                             | день ПОСАДКИ: поездка через полночь целиком относится ко дню начала                                                                |
| hour_of_day                         | UInt8  | toHour(pickup_datetime)                                             | 0–23, наивное местное время Нью-Йорка (см. оговорку про переводы часов)                                                            |
| day_of_week                         | UInt8  | toDayOfWeek(pickup_datetime)                                        | 1 = понедельник … 7 = воскресенье (конвенция ClickHouse; в pandas 0 = понедельник)                                                 |
| operator                            | String | hvfhs_license_num → имя                                             | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                         |
| trips                               | UInt64 | count()                                                             | все поездки группы, прошедшие silver-фильтр                                                                                        |
| trips_with_wait                     | UInt64 | countIf(wait BETWEEN 0 AND 7200)                                    | знаменатель всех метрик ожидания; показывать рядом с P50/P90                                                                       |
| sum_wait_sec                        | Int64  | sumIf(wait, wait_valid)                                             | сумма секунд ожидания; средняя за период = SUM(sum_wait_sec) / SUM(trips_with_wait)                                                |
| shared_trips_with_wait              | UInt64 | countIf(shared_request_flag AND wait_valid)                         | валидные ожидания среди поездок с запросом Shared — знаменатель среднего                                                           |
| shared_sum_wait_sec                 | Int64  | sumIf(wait, shared_request_flag AND wait_valid)                     | сумма секунд ожидания Shared-запросов                                                                                              |
| wav_trips_with_wait                 | UInt64 | countIf(wav_request_flag AND wait_valid)                            | валидные ожидания среди поездок с запросом WAV                                                                                     |
| wav_sum_wait_sec                    | Int64  | sumIf(wait, wav_request_flag AND wait_valid)                        | сумма секунд ожидания WAV-запросов                                                                                                 |
| regular_trips_with_wait             | UInt64 | countIf(wait_valid AND ни одного feature-флага)                     | СТРОГИЙ baseline «обычной» поездки: без Shared request/match, WAV request/match и AAR; единое определение во всех витринах с 23.08 |
| regular_sum_wait_sec                | Int64  | sumIf(wait, wait_valid AND ни одного feature-флага)                 | сумма секунд ожидания строгого baseline; среднее за период = SUM(sec)/SUM(count)                                                   |
| shared_requests                     | UInt64 | countIf(shared_request_flag)                                        | запросы совместной поездки                                                                                                         |
| shared_requested_matches            | UInt64 | countIf(shared_request_flag AND shared_match_flag)                  | Shared состыкован СРЕДИ запросов — числитель shared_match_rate                                                                     |
| shared_orphan_matches               | UInt64 | countIf(NOT shared_request_flag AND shared_match_flag)              | матчи БЕЗ запроса — дефект источника (в silver 17 170, все у Lyft); в match rate не участвуют                                      |
| actual_shared_trips                 | UInt64 | countIf(shared_match_flag)                                          | все фактически состыкованные, с запросом и без = shared_requested_matches + shared_orphan_matches                                  |
| wav_requests                        | UInt64 | countIf(wav_request_flag)                                           | запросы доступного авто (WAV)                                                                                                      |
| wav_requested_matches               | UInt64 | countIf(wav_request_flag AND wav_match_flag)                        | WAV подан СРЕДИ явных запросов — числитель wav_fulfillment_rate                                                                    |
| wav_unrequested_matches             | UInt64 | countIf(NOT wav_request_flag AND wav_match_flag)                    | подача WAV без запроса — WAV-машина на обычном заказе, НЕ дефект                                                                   |
| actual_wav_trips                    | UInt64 | countIf(wav_match_flag)                                             | все поездки на WAV-машине, с запросом и без                                                                                        |
| access_a_ride_trips                 | UInt64 | countIf(access_a_ride_flag)                                         | поездки программы MTA Access-A-Ride; AAR НЕ тождественен WAV — отдельный флаг                                                      |
| access_a_ride_wav_requests          | UInt64 | countIf(access_a_ride_flag AND wav_request_flag)                    | пересечение: AAR-поездка с явным запросом WAV                                                                                      |
| access_a_ride_wav_requested_matches | UInt64 | countIf(access_a_ride_flag AND wav_request_flag AND wav_match_flag) | из них состыкованные с WAV                                                                                                         |
| access_a_ride_wav_matches           | UInt64 | countIf(access_a_ride_flag AND wav_match_flag)                      | AAR-поездки, выполненные WAV-машиной (с запросом и без)                                                                            |

### mart_features_by_zone

Вкладка дашборда: Shared & WAV · Гранулярность: день × зона посадки × оператор · Строк: 250 254

География Shared/WAV/AAR; penetration от всего спроса зоны.

| Атрибут                             | Тип                    | Как считается                                                       | Комментарий                                                                                                                        |
|-------------------------------------|------------------------|---------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| trip_date                           | Date                   | toDate(pickup_datetime)                                             | день ПОСАДКИ: поездка через полночь целиком относится ко дню начала                                                                |
| pu_location_id                      | Int32                  | PULocationID                                                        | код зоны TLC, 1–265; для join с геослоем зон                                                                                       |
| pu_zone_name                        | String                 | JOIN nyc.zones по PULocationID                                      | название зоны посадки из taxi_zone_lookup.csv                                                                                      |
| pu_borough                          | LowCardinality(String) | JOIN nyc.zones по PULocationID                                      | боро посадки                                                                                                                       |
| operator                            | String                 | hvfhs_license_num → имя                                             | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                         |
| trips                               | UInt64                 | count()                                                             | все поездки группы, прошедшие silver-фильтр                                                                                        |
| trips_with_wait                     | UInt64                 | countIf(wait BETWEEN 0 AND 7200)                                    | знаменатель всех метрик ожидания; показывать рядом с P50/P90                                                                       |
| sum_wait_sec                        | Int64                  | sumIf(wait, wait_valid)                                             | сумма секунд ожидания; средняя за период = SUM(sum_wait_sec) / SUM(trips_with_wait)                                                |
| shared_trips_with_wait              | UInt64                 | countIf(shared_request_flag AND wait_valid)                         | валидные ожидания среди поездок с запросом Shared — знаменатель среднего                                                           |
| shared_sum_wait_sec                 | Int64                  | sumIf(wait, shared_request_flag AND wait_valid)                     | сумма секунд ожидания Shared-запросов                                                                                              |
| wav_trips_with_wait                 | UInt64                 | countIf(wav_request_flag AND wait_valid)                            | валидные ожидания среди поездок с запросом WAV                                                                                     |
| wav_sum_wait_sec                    | Int64                  | sumIf(wait, wav_request_flag AND wait_valid)                        | сумма секунд ожидания WAV-запросов                                                                                                 |
| regular_trips_with_wait             | UInt64                 | countIf(wait_valid AND ни одного feature-флага)                     | СТРОГИЙ baseline «обычной» поездки: без Shared request/match, WAV request/match и AAR; единое определение во всех витринах с 23.08 |
| regular_sum_wait_sec                | Int64                  | sumIf(wait, wait_valid AND ни одного feature-флага)                 | сумма секунд ожидания строгого baseline; среднее за период = SUM(sec)/SUM(count)                                                   |
| shared_requests                     | UInt64                 | countIf(shared_request_flag)                                        | запросы совместной поездки                                                                                                         |
| shared_requested_matches            | UInt64                 | countIf(shared_request_flag AND shared_match_flag)                  | Shared состыкован СРЕДИ запросов — числитель shared_match_rate                                                                     |
| shared_orphan_matches               | UInt64                 | countIf(NOT shared_request_flag AND shared_match_flag)              | матчи БЕЗ запроса — дефект источника (в silver 17 170, все у Lyft); в match rate не участвуют                                      |
| actual_shared_trips                 | UInt64                 | countIf(shared_match_flag)                                          | все фактически состыкованные, с запросом и без = shared_requested_matches + shared_orphan_matches                                  |
| wav_requests                        | UInt64                 | countIf(wav_request_flag)                                           | запросы доступного авто (WAV)                                                                                                      |
| wav_requested_matches               | UInt64                 | countIf(wav_request_flag AND wav_match_flag)                        | WAV подан СРЕДИ явных запросов — числитель wav_fulfillment_rate                                                                    |
| wav_unrequested_matches             | UInt64                 | countIf(NOT wav_request_flag AND wav_match_flag)                    | подача WAV без запроса — WAV-машина на обычном заказе, НЕ дефект                                                                   |
| actual_wav_trips                    | UInt64                 | countIf(wav_match_flag)                                             | все поездки на WAV-машине, с запросом и без                                                                                        |
| access_a_ride_trips                 | UInt64                 | countIf(access_a_ride_flag)                                         | поездки программы MTA Access-A-Ride; AAR НЕ тождественен WAV — отдельный флаг                                                      |
| access_a_ride_wav_requests          | UInt64                 | countIf(access_a_ride_flag AND wav_request_flag)                    | пересечение: AAR-поездка с явным запросом WAV                                                                                      |
| access_a_ride_wav_requested_matches | UInt64                 | countIf(access_a_ride_flag AND wav_request_flag AND wav_match_flag) | из них состыкованные с WAV                                                                                                         |
| access_a_ride_wav_matches           | UInt64                 | countIf(access_a_ride_flag AND wav_match_flag)                      | AAR-поездки, выполненные WAV-машиной (с запросом и без)                                                                            |
| sum_miles                           | Float64                | sum(trip_miles)                                                     | сумма миль — для переагрегации долей                                                                                               |

### mart_wait_histogram

Вкладка дашборда: Demand & Wait · Гранулярность: месяц × оператор × корзина ожидания · Строк: 512

Распределение ожидания.

| Атрибут         | Тип    | Как считается                   | Комментарий                                                                          |
|-----------------|--------|---------------------------------|--------------------------------------------------------------------------------------|
| month           | Date   | toStartOfMonth(pickup_datetime) | первый день месяца посадки, тип Date                                                 |
| operator        | String | hvfhs_license_num → имя         | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                           |
| bucket_order    | UInt16 | least(intDiv(wait, 60), 15) + 1 | 1–16, поминутные корзины, для сортировки на графике                                  |
| wait_bucket     | String | подпись корзины                 | «0-1 мин» … «14-15 мин», «15+ мин»; границы: левая включена, правая нет              |
| trips_with_wait | UInt64 | count() WHERE wait_valid        | ВНИМАНИЕ: сумма по витрине 322.9 млн — МЕНЬШЕ общих 326.8 млн на 3.9 млн предзаказов |

### mart_fare_distance

Вкладка дашборда: Economics · Гранулярность: месяц × оператор × мильный бин · Строк: 1 952

Зависимость тарифа от дистанции.

| Атрибут            | Тип     | Как считается                             | Комментарий                                                                                                        |
|--------------------|---------|-------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| month              | Date    | toStartOfMonth(pickup_datetime)           | первый день месяца посадки, тип Date                                                                               |
| operator           | String  | hvfhs_license_num → имя                   | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                         |
| distance_bin_miles | UInt16  | least(floor(trip_miles), 60)              | мильный бин 0–59; бин 60 — ХВОСТОВОЙ (60 миль и дальше)                                                            |
| trips              | UInt64  | count()                                   | все поездки группы, прошедшие silver-фильтр                                                                        |
| avg_distance_miles | Float64 | avg(trip_miles)                           | реальная средняя дистанция бина; для хвостового бина = 80.5 мили — точку scatter ставить по ней, не по номеру бина |
| avg_base_fare      | Float64 | avg(base_passenger_fare)                  | средний базовый тариф до надбавок                                                                                  |
| avg_total_fare     | Float64 | avg(total_fare)                           | средний полный счёт без чаевых                                                                                     |
| fare_p50           | Float32 | quantileTDigest(0.5)(base_passenger_fare) | медиана базового тарифа в бине                                                                                     |
| fare_p90           | Float32 | quantileTDigest(0.9)(base_passenger_fare) | 90-й перцентиль базового тарифа                                                                                    |
| avg_duration_min   | Float64 | avg(trip_time) / 60                       | средняя длительность, МИНУТЫ; из trip_time (секундомер поездки), а не из разности меток                            |
| sum_base_fare      | Float64 | sum(base_passenger_fare)                  | сумма базовых тарифов                                                                                              |
| sum_miles          | Float64 | sum(trip_miles)                           | сумма миль — для переагрегации долей                                                                               |

### mart_airports

Вкладка дашборда: GEO · Гранулярность: месяц × аэропорт × направление × оператор · Строк: 171

Аэропортовый сегмент.

| Атрибут                          | Тип               | Как считается                                      | Комментарий                                                                                                                                                            |
|----------------------------------|-------------------|----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| month                            | Date              | toStartOfMonth(pickup_datetime)                    | первый день месяца посадки, тип Date                                                                                                                                   |
| airport                          | String            | PULocationID/DOLocationID → имя                    | 132 → JFK, 138 → LaGuardia, 1 → Newark                                                                                                                                 |
| direction                        | String            | 'pickup' \| 'dropoff'                              | обязательное измерение: у Ньюарка 18 посадок против 2.35 млн высадок — без направления он невидим. Поездка аэропорт→аэропорт даёт ДВЕ строки                           |
| operator                         | String            | hvfhs_license_num → имя                            | HV0003 → Uber, HV0005 → Lyft; других лицензий в данных нет                                                                                                             |
| trips                            | UInt64            | count()                                            | все поездки группы, прошедшие silver-фильтр                                                                                                                            |
| all_trips_month                  | UInt64            | count() по ВСЕМ silver-поездкам месяца             | знаменатель airport_share_all_trips_pct; повторяется в каждой строке месяца                                                                                            |
| all_trips_month_operator         | UInt64            | count() по всем silver-поездкам месяца у оператора | знаменатель airport_share_operator_trips_pct                                                                                                                           |
| airport_share_all_trips_pct      | Nullable(Float64) | 100 × trips / all_trips_month                      | доля строк витрины от всех поездок месяца, %; при агрегации в BI пересчитывать как sum/знаменатель, не усреднять проценты                                              |
| airport_share_operator_trips_pct | Nullable(Float64) | 100 × trips / all_trips_month_operator             | то же, но от поездок оператора, %                                                                                                                                      |
| avg_fare                         | Float64           | avg(total_fare)                                    | средний счёт без чаевых                                                                                                                                                |
| avg_distance_miles               | Float64           | avg(trip_miles)                                    | средняя дистанция, МИЛИ                                                                                                                                                |
| avg_duration_min                 | Float64           | avg(trip_time) / 60                                | средняя длительность, МИНУТЫ; из trip_time (секундомер поездки), а не из разности меток                                                                                |
| trips_with_wait                  | UInt64            | countIf(wait BETWEEN 0 AND 7200)                   | знаменатель всех метрик ожидания; показывать рядом с P50/P90                                                                                                           |
| wait_p50                         | Nullable(Float32) | quantileTDigestOrNullIf(0.5)(wait, wait_valid)     | медиана ожидания, СЕКУНДЫ; t-digest (приближённо); НЕ переагрегируется усреднением; NULL, если в группе нет валидных ожиданий (без -OrNull был NaN, ломавший avg в BI) |
| wait_p90                         | Nullable(Float32) | quantileTDigestOrNullIf(0.9)(wait, wait_valid)     | 90-й перцентиль ожидания, СЕКУНДЫ; НЕ переагрегируется; NULL при пустой группе                                                                                         |
| sum_total_fare                   | Float64           | sum(total_fare)                                    | сумма счетов — для переагрегации                                                                                                                                       |
| sum_miles                        | Float64           | sum(trip_miles)                                    | сумма миль — для переагрегации долей                                                                                                                                   |

Итого: 12 витрин, 226 атрибутов. Пересборка витрин: python etl/load_flow.py · выгрузка в CSV: python etl/export_csv.py
