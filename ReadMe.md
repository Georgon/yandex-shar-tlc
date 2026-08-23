Проект: TLC Trip Record Dat

В нем представлены данные о поездках такси в Нью-Йорке: время и районы посадки и высадки, 
расстояние, стоимость, чаевые, способ оплаты и число пассажиров. 
Ссылка на датасет: https://disk.yandex.ru/d/p1YAHtu15H2tlA/TLC%20Trip%20Record%20Data

Цель проекта - пройти полный цикл работы продуктового аналитика на большом сыром датасете: от первичного разбора данных до аналитикого дашборда и продуктовых рекомендаций.

Авторы проекта: 
- Зарипова Альмира (создание дашборда)
- Никишев Георгий (очистка данных и создание витрин)
- Трощенков Андрей (генерация гипотез)
- Джамиев Вадим (тех запуск проекта и создание витрин)

По результам анализа был составлен дашборд: https://datalens.yandex/c871p6fiqlcqv?tab=75


NYC TLC FHVHV — ЗАПУСК ПРОЕКТА (ДЛЯ РАБОТЫ ПРОЕКТА ДАННЫЕ НУЖНО ЗАГРУЗИТЬ ЛОКАЛЬНО)
=================================================

Проект загружает NYC TLC High Volume FHV trip records в ClickHouse, выполняет
проверки качества (ODS), применяет silver-фильтр и собирает gold-витрины для BI.

Основной pipeline:
  discover source files
    -> CSV -> Parquet (если нужно)
    -> source validation
    -> load nyc.trips by month partition
    -> partition QC
    -> ODS checks
    -> dimension nyc.zones
    -> gold marts

Готовые витрины хранятся в ClickHouse в базе nyc как таблицы nyc.mart_*.
CSV-копии можно получить командой make export; они появятся в ./marts_csv.


2. СТРУКТУРА ПРОЕКТА
-----------------------------
Рекомендуемая структура:

  yandex-shar-tlc/
  |-- Makefile
  |-- ReadMe.md
  |-- docker-compose.yml
  |-- requirements.txt
  |-- taxi_zone_lookup.csv
  |-- data/ 
  |   |-- fhvhv_tripdata_YYYY-MM.parquet
  |   `-- ...
  |-- etl/
  |   |-- load_flow.py
  |   |-- marts.py
  |   |-- ods_checks.py
  |   |-- checks.py
  |   |-- schema.py
  |   `-- export_csv.py
  `-- .venv/

Папка ./data должна быть смонтирована Docker в:
  /var/lib/clickhouse/user_files/data

Makefile ожидает контейнер:
  nyc-clickhouse

и подключение:
  host:     localhost
  HTTP port: 8123
  database: nyc
  user:     analyst
  password: analyst

Эти значения можно переопределить переменными make, если конфигурация отличается.


3. ЧТО НУЖНО УСТАНОВИТЬ
-----------------------
Перед первым запуском нужны:

  - Python (на Windows Makefile по умолчанию использует launcher "py")
  - Docker Desktop с работающим Docker Engine
  - GNU Make
  - docker-compose.yml для ClickHouse

На Windows рекомендуется запускать команды из PowerShell/терминала VS Code,
а не из Git Bash: Git Bash может преобразовывать Linux-пути внутри docker exec.


4. ПЕРВЫЙ ЗАПУСК
----------------
Шаг 1. Создать виртуальное окружение и установить зависимости:

  make venv

Команда создаёт .venv и выполняет:
  python -m pip install --upgrade pip
  python -m pip install -r requirements.txt

Makefile сам использует Python из .venv, поэтому отдельно активировать venv
для make-команд не обязательно.

Если на Windows команда "py" отсутствует, можно запустить:
  make venv PYTHON_BOOTSTRAP=python


Шаг 2. Положить исходные parquet/CSV в ./data.

Пример:
  data/fhvhv_tripdata_2025-03.parquet
  data/fhvhv_tripdata_2026-03.parquet


Шаг 3. Запустить ClickHouse:

  make docker-up

Эта команда:
  1) выполняет docker compose up -d;
  2) показывает контейнер nyc-clickhouse;
  3) проверяет подключение clickhouse-client;
  4) показывает файлы в /var/lib/clickhouse/user_files/data;
  5) показывает уже загруженные partition nyc.trips (при первом запуске список
     может быть пустым — это нормально).

Повторная проверка без перезапуска:
  make docker-check

Остановить сервисы:
  make docker-down


Шаг 4. Запустить полный pipeline:

  make pipeline

Это эквивалентно:
  .venv/.../python etl/load_flow.py

Pipeline сам:
  - создаёт служебные таблицы;
  - проверяет/создаёт nyc.trips;
  - находит месячные source-файлы;
  - при необходимости конвертирует CSV в Parquet;
  - валидирует source;
  - загружает недостающие/неполные месячные partition;
  - запускает partition QC;
  - запускает ODS checks;
  - пересоздаёт nyc.zones;
  - пересобирает все gold-витрины.

Принудительная перезагрузка всех найденных partition:
  make pipeline-force

Использовать её нужно только когда действительно требуется перезалить данные.


5. ПЕРЕСБОРКА ТОЛЬКО ВИТРИН
---------------------------
Если данные nyc.trips уже загружены, а изменился только etl/marts.py, не нужно
повторно читать parquet. Достаточно:

  make marts

Команда подключается к ClickHouse и для каждого элемента MARTS выполняет
CREATE OR REPLACE TABLE nyc.<mart> ...

Перед этим должны существовать:
  nyc.trips
  nyc.zones

Если их нет, сначала выполнить:
  make pipeline

Важно: полный make pipeline в любом случае пересобирает витрины в конце.
make marts нужен именно для быстрого повторного построения gold-слоя.


6. ВЫГРУЗКА В CSV
-----------------
Только витрины:
  make export

Витрины + служебные таблицы качества:
  make export-quality

Файлы создаются в:
  ./marts_csv
