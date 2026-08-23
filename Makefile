# Makefile для NYC TLC FHVHV pipeline (Windows + VS Code / Linux / macOS)
#
# Основные команды:
#   make venv          - создать .venv и установить requirements.txt
#   make docker-up     - запустить ClickHouse и сразу выполнить проверки
#   make docker-check  - проверить контейнер, ClickHouse, mount data и загруженные партиции
#   make pipeline      - запустить полный load_flow.py
#   make marts         - пересобрать только gold-витрины из текущей nyc.trips
#
# При необходимости параметры можно переопределить:
#   make docker-up CH_USER=analyst CH_PASSWORD=analyst CH_DB=nyc

CH_CONTAINER ?= nyc-clickhouse
CH_HOST ?= localhost
CH_PORT ?= 8123
CH_USER ?= analyst
CH_PASSWORD ?= analyst
CH_DB ?= nyc
COMPOSE_FILE ?= docker-compose.yml

# Поддерживаем стандартную структуру с etl/, но оставляем fallback на корень.
ETL_DIR := $(if $(wildcard etl/load_flow.py),etl,.)

ifeq ($(OS),Windows_NT)
PYTHON_BOOTSTRAP ?= py
PYTHON := venv/Scripts/python.exe
else
PYTHON_BOOTSTRAP ?= python3
PYTHON := venv/bin/python
endif

.PHONY: help venv docker-up docker-check docker-down pipeline pipeline-force marts export export-quality

help:
	@echo "Targets:"
	@echo "  make venv           - create .venv and install requirements"
	@echo "  make docker-up      - start ClickHouse via docker compose and check it"
	@echo "  make docker-check   - check container, server, mounted data and loaded partitions"
	@echo "  make docker-down    - stop docker compose services"
	@echo "  make pipeline       - run full ETL/ODS/gold pipeline"
	@echo "  make pipeline-force - reload all source partitions and rebuild marts"
	@echo "  make marts          - rebuild only gold marts from existing nyc.trips"
	@echo "  make export         - export marts to ./marts_csv"
	@echo "  make export-quality - export marts plus quality/service tables"

venv:
	$(PYTHON_BOOTSTRAP) -m venv venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	@echo "venv is ready: $(PYTHON)"

docker-up:
	docker compose -f "$(COMPOSE_FILE)" up -d

docker-check:
	@echo "=== Docker container ==="
	docker ps --filter "name=$(CH_CONTAINER)"
	@echo "=== ClickHouse connection ==="
	docker exec $(CH_CONTAINER) clickhouse-client --user $(CH_USER) --password $(CH_PASSWORD) --database $(CH_DB) --query "SELECT version() AS clickhouse_version, currentDatabase() AS database"
	@echo "=== Source files visible inside ClickHouse container ==="
	docker exec $(CH_CONTAINER) sh -lc "ls -lah /var/lib/clickhouse/user_files/data"
	@echo "=== Loaded nyc.trips partitions (empty before first pipeline run is OK) ==="
	docker exec $(CH_CONTAINER) clickhouse-client --user $(CH_USER) --password $(CH_PASSWORD) --database $(CH_DB) --query "SELECT partition, sum(rows) AS rows FROM system.parts WHERE database='$(CH_DB)' AND table='trips' AND active GROUP BY partition ORDER BY partition"

docker-down:
	docker compose -f "$(COMPOSE_FILE)" down

pipeline:
	$(PYTHON) $(ETL_DIR)/load_flow.py

pipeline-force:
	$(PYTHON) $(ETL_DIR)/load_flow.py --force

# Пересобирает только таблицы MARTS из etl/marts.py.
# Требование: nyc.trips и nyc.zones уже должны существовать (обычно после make pipeline).
marts:
	$(PYTHON) -c "import sys; sys.path.insert(0, r'$(ETL_DIR)'); import clickhouse_connect; from marts import MARTS, build_statement; ch=clickhouse_connect.get_client(host='$(CH_HOST)', port=$(CH_PORT), username='$(CH_USER)', password='$(CH_PASSWORD)', database='$(CH_DB)', send_receive_timeout=1800); missing=[t for t in ('trips','zones') if int(ch.command(\"SELECT count() FROM system.tables WHERE database='$(CH_DB)' AND name='\"+t+\"'\")) == 0]; assert not missing, 'Missing ClickHouse tables: '+', '.join(missing)+'. Run make pipeline first.'; [ch.command(build_statement(name)) for name in MARTS]; print('Built marts:'); [print('  '+name) for name in MARTS]"
	@echo "=== Mart row counts ==="
	docker exec $(CH_CONTAINER) clickhouse-client --user $(CH_USER) --password $(CH_PASSWORD) --database $(CH_DB) --query "SELECT name, total_rows FROM system.tables WHERE database='$(CH_DB)' AND startsWith(name, 'mart_') ORDER BY name"

export:
	$(PYTHON) $(ETL_DIR)/export_csv.py

export-quality:
	$(PYTHON) $(ETL_DIR)/export_csv.py --with-quality
