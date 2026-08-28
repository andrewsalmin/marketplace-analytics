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
#   ./run_downstream_pipeline.sh <clickhouse_password>          # с начала
#   ./run_downstream_pipeline.sh <clickhouse_password> 5        # с дня 5
#
# ВАЖНО про возобновление: в отличие от backfill_history.sh, здесь три
# стадии на день, и упасть может любая из трёх. RESUME_FROM_DAY
# перезапускает ВСЕ ТРИ стадии для указанного дня заново —
# ingest_csv_to_raw.py упадёт с "партиция уже существует", если для
# этого дня он уже успешно отработал раньше (значит, возобновляй со
# СЛЕДУЮЩЕГО дня, а не с текущего). load_to_clickhouse.py, в отличие
# от ingest, идемпотентен сам по себе (см. _load_commits в
# clickhouse_schema.sql) — повторный вызов для уже загруженного дня
# безопасный no-op, руками ничего "долечивать" не нужно. Если нужно
# принудительно перезаписать день в ClickHouse — вызови
# load_to_clickhouse.py с --force-reload отдельно.
set -euo pipefail

DATA_DIR="./data"
START_DATE="2026-06-01"
DAYS=92

# host/port/database/user — единый источник правды в
# clickhouse_config.json, общий с load_to_clickhouse.py (дефолты
# argparse) и Airflow DAG'ом (daily_marketplace_pipeline). Пароль в
# этом файле намеренно не хранится — только как CLI-аргумент.
CLICKHOUSE_CONFIG="$(dirname "$0")/clickhouse_config.json"
_ch_cfg() { python -c "import json; print(json.load(open('${CLICKHOUSE_CONFIG}'))['$1'])"; }

CLICKHOUSE_HOST=$(_ch_cfg host)
CLICKHOUSE_PORT=$(_ch_cfg port)
CLICKHOUSE_DATABASE=$(_ch_cfg database)
CLICKHOUSE_USER=$(_ch_cfg user)
CLICKHOUSE_PASSWORD="${1:?Usage: ./run_downstream_pipeline.sh <clickhouse_password> [resume_from_day]}"
RESUME_FROM_DAY="${2:-1}"  # 1-indexed, как в выводе "(день N/DAYS)"

for i in $(seq 0 $((DAYS - 1))); do
  day_number=$((i + 1))

  if [ "$day_number" -lt "$RESUME_FROM_DAY" ]; then
    continue  # уже обработан в предыдущем прогоне, пропускаем
  fi

  load_date=$(date -d "${START_DATE} + ${i} days" +%F)
  echo "=== ${load_date} (день ${day_number}/${DAYS}) ==="

  python ingest_csv_to_raw.py \
    --load-date "${load_date}" \
    --data-dir "${DATA_DIR}"

  python transform.py \
    --load-date "${load_date}" \
    --data-dir "${DATA_DIR}"

  python load_to_clickhouse.py \
    --load-date "${load_date}" \
    --data-dir "${DATA_DIR}" \
    --clickhouse-host "${CLICKHOUSE_HOST}" \
    --clickhouse-port "${CLICKHOUSE_PORT}" \
    --clickhouse-database "${CLICKHOUSE_DATABASE}" \
    --clickhouse-user "${CLICKHOUSE_USER}" \
    --clickhouse-password "${CLICKHOUSE_PASSWORD}"
done
