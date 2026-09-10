#!/usr/bin/env bash
# Прогоняет ingest_csv_to_raw.py -> transform.py -> load_to_clickhouse.py
# для диапазона дат СТРОГО по порядку. Порядок важен: transform.py
# проверяет дубли customer_id относительно уже обработанной истории
# (read_historical_keys) — если дни идут не по порядку, эта проверка
# будет работать некорректно.
#
# Предполагается, что generate_data.py для этого же диапазона уже
# отработал (см. backfill_history.sh) — эта стадия его не запускает.
#
# Использование:
#   ./run_downstream_pipeline.sh          # с начала
#   ./run_downstream_pipeline.sh 5        # с дня 5
#
# Пароль ClickHouse берётся из CLICKHOUSE_PASSWORD (той же переменной,
# что читает docker-compose.yml), а если её нет — спрашивается скрытым
# вводом. В аргументах командной строки он не передаётся: оттуда он
# попадает и в историю оболочки, и в список процессов.
#
# Возобновление после сбоя — с того дня, на котором он произошёл:
# RESUME_FROM_DAY повторяет день целиком, и каждая стадия это выдерживает.
# - ingest_csv_to_raw.py перезапись запрещает, поэтому для дня с манифестом
#   ingest (raw/_manifests/load_date=.../ingest_manifest.json) он
#   пропускается. Манифест пишется последним, после всех трёх сущностей.
# - transform.py перезаписывает свои партиции за день.
# - load_to_clickhouse.py для загруженного дня — no-op, а недогруженный
#   перед загрузкой очищает (prepare_load_date).
# Исключение — ingest, упавший посреди записи: raw-партиции части сущностей
# уже есть, а манифеста нет. Такой день чинится руками: удалить
# data/raw/*/load_date=<день> и запустить скрипт с этого дня.
#
# Принудительно перезаписать день в ClickHouse — отдельным вызовом
# load_to_clickhouse.py с --force-reload.
set -euo pipefail

DATA_DIR="./data"

# start_date/days — единый источник правды в growth_config.json, общий
# с backfill_history.sh и Airflow DAG'ом (daily_marketplace_pipeline):
# этот скрипт должен обработать РОВНО тот же диапазон дат, что backfill
# сгенерировал, иначе ingest упадёт на дне, для которого нет source-
# данных (или наоборот — часть сгенерированной истории останется не
# обработанной).
GROWTH_CONFIG="$(dirname "$0")/growth_config.json"
_growth_cfg() { python -c "import json; print(json.load(open('${GROWTH_CONFIG}'))['$1'])"; }

START_DATE=$(_growth_cfg start_date)
DAYS=$(_growth_cfg days)

# host/port/database/user — единый источник правды в
# clickhouse_config.json, общий с load_to_clickhouse.py (дефолты
# argparse) и Airflow DAG'ом (daily_marketplace_pipeline). Пароль в
# этом файле намеренно не хранится — см. CLICKHOUSE_PASSWORD ниже.
CLICKHOUSE_CONFIG="$(dirname "$0")/clickhouse_config.json"
_ch_cfg() { python -c "import json; print(json.load(open('${CLICKHOUSE_CONFIG}'))['$1'])"; }

CLICKHOUSE_HOST=$(_ch_cfg host)
CLICKHOUSE_PORT=$(_ch_cfg port)
CLICKHOUSE_DATABASE=$(_ch_cfg database)
CLICKHOUSE_USER=$(_ch_cfg user)
RESUME_FROM_DAY="${1:-1}"  # 1-indexed, как в выводе "(день N/DAYS)"

# -t 0 — проверка, что stdin действительно терминал: под nohup/в CI
# спрашивать некого, и скрипт должен сказать об этом, а не повиснуть на
# приглашении ввода.
if [ -z "${CLICKHOUSE_PASSWORD:-}" ]; then
  if [ -t 0 ]; then
    read -r -s -p "Пароль ClickHouse (${CLICKHOUSE_USER}): " CLICKHOUSE_PASSWORD
    echo
  else
    echo "Задай CLICKHOUSE_PASSWORD: ввод не терминал, спросить пароль негде." >&2
    exit 1
  fi
fi

if [ -z "${CLICKHOUSE_PASSWORD}" ]; then
  echo "Пустой пароль ClickHouse." >&2
  exit 1
fi

for i in $(seq 0 $((DAYS - 1))); do
  day_number=$((i + 1))

  if [ "$day_number" -lt "$RESUME_FROM_DAY" ]; then
    continue  # уже обработан в предыдущем прогоне, пропускаем
  fi

  load_date=$(date -d "${START_DATE} + ${i} days" +%F)
  echo "=== ${load_date} (день ${day_number}/${DAYS}) ==="

  ingest_manifest="${DATA_DIR}/raw/_manifests/load_date=${load_date}/ingest_manifest.json"
  if [ -f "${ingest_manifest}" ]; then
    echo "ingest за ${load_date} уже выполнен (${ingest_manifest}), пропускаю"
  else
    python ingest_csv_to_raw.py \
      --load-date "${load_date}" \
      --data-dir "${DATA_DIR}"
  fi

  python transform.py \
    --load-date "${load_date}" \
    --data-dir "${DATA_DIR}"

  # Пароль уходит переменной окружения, а не флагом: аргументы
  # командной строки видны в ps любому пользователю машины.
  CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" python load_to_clickhouse.py \
    --load-date "${load_date}" \
    --data-dir "${DATA_DIR}" \
    --clickhouse-host "${CLICKHOUSE_HOST}" \
    --clickhouse-port "${CLICKHOUSE_PORT}" \
    --clickhouse-database "${CLICKHOUSE_DATABASE}" \
    --clickhouse-user "${CLICKHOUSE_USER}"
done

# Проверки семантического слоя — после всей загрузки, а не на каждый
# день: это инварианты слоя целиком (дедупликация, монотонность воронки,
# согласованность конверсии), и на 92 днях проверка каждого дня
# стоила бы дороже самой загрузки.
#
# Падение здесь — сигнал, что витрины разъехались с данными; лучше узнать
# об этом тут, чем от посетителя дашборда.
echo "=== проверка витрин ==="
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" python marts_checks.py
