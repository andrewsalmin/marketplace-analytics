#!/usr/bin/env bash
# Бэкафилл истории маркетплейса от START_DATE на DAYS дней вперёд.
# Кривая роста — две фазы: рост первые RAMP_DAYS дней (раскрутка после
# открытия), дальше плато с лёгким органическим ростом (устоявшийся
# маркетплейс). Подставь свою форму кривой при необходимости.
#
# --launch-date включает в generate_data.py реалистичную динамику
# error_rate (resolve_error_rate()): в первые недели после открытия она
# заметно выше ERROR_RATE ниже (интеграции ещё сырые) и экспоненциально
# сглаживается к нему, поверх — день-в-день шум и редкие инциденты
# (сорвался партнёрский API и т.п.) у отдельных сущностей. Поэтому
# ERROR_RATE ниже — это базовый уровень плато, а не буквальный error_rate
# каждого конкретного дня.
#
# Использование:
#   ./backfill_month.sh          # с самого начала
#   ./backfill_month.sh 26       # возобновить с дня 26 (1-indexed) —
#                                 # например, после падения посреди
#                                 # прогона (диск, сеть и т.п.). День,
#                                 # на котором упало, не публикуется
#                                 # (publish_batch не успевает
#                                 # отработать), так что его нужно
#                                 # повторить, а не пропускать.
set -euo pipefail

DATA_DIR="./data"
DAYS=92
RESUME_FROM_DAY="${1:-1}"  # 1-indexed, как в выводе "(день N/DAYS)"

# Все параметры ниже — единый источник правды в growth_config.json,
# общий с Airflow DAG'ом (daily_marketplace_pipeline): без этого ручной
# бэкафилл и ежедневный пайплайн рано или поздно разъедутся по бизнес-
# правилам и объёму. Менять значения — только в growth_config.json,
# не здесь.
GROWTH_CONFIG="$(dirname "$0")/growth_config.json"
_cfg() { python -c "import json; print(json.load(open('${GROWTH_CONFIG}'))['$1'])"; }

# Множитель на объём (customers/orders) по дню недели — 1..7, ISO
# (1=понедельник, 7=воскресенье, см. `date +%u`), ключ строкой — так и
# лежит в growth_config.json. Тот же файл читает и Airflow DAG, поэтому
# сами коэффициенты определены только там, а не захардкожены здесь.
_weekday_multiplier() {
  python -c "import json; print(json.load(open('${GROWTH_CONFIG}'))['weekday_multipliers']['$1'])"
}
_apply_multiplier() { python -c "print(round($1 * $2))"; }

START_DATE=$(_cfg start_date)
RAMP_DAYS=$(_cfg ramp_days)

# --- Политика бизнеса: постоянна весь месяц, НЕ часть кривой роста ---
PAYMENT_DEADLINE_HOURS=$(_cfg payment_deadline_hours)
SHIPPING_DEADLINE_HOURS=$(_cfg shipping_deadline_hours)
DELIVERY_DEADLINE_DAYS=$(_cfg delivery_deadline_days)
PICKUP_DEADLINE_DAYS=$(_cfg pickup_deadline_days)
RETURN_WINDOW_DAYS=$(_cfg return_window_days)
REFUND_PROCESSING_HOURS=$(_cfg refund_processing_hours)
CANCEL_BEFORE_PAYMENT_RATE=$(_cfg cancel_before_payment_rate)
CANCEL_AFTER_PAYMENT_RATE=$(_cfg cancel_after_payment_rate)
RETURN_RATE=$(_cfg return_rate)
ERROR_RATE=$(_cfg error_rate)

# --- Запас попыток оплаты над числом заказов: подобран эмпирически на
# ~1%, чтобы доля не оплаченных в срок заказов (cancelled/
# not_paid_in_time) была реалистичной (~1-2%), а не нулевой — большой
# запас (5%+) практически полностью вымывает этот тип отмены из данных.
PAYMENTS_MARGIN_PCT=$(_cfg payments_margin_pct)

for i in $(seq 0 $((DAYS - 1))); do
  day_number=$((i + 1))

  if [ "$day_number" -lt "$RESUME_FROM_DAY" ]; then
    continue  # уже опубликован в предыдущем прогоне, пропускаем
  fi

  load_date=$(date -d "${START_DATE} + ${i} days" +%F)

  # Фаза 1 (i <= RAMP_DAYS): линейный рост от мягкого запуска.
  # Фаза 2 (после): объём фиксируется на уровне конца рампы + медленный
  # органический рост — устоявшийся маркетплейс, а не бесконечное
  # ускорение. Клиенты — новые регистрации ЗА ЭТОТ день (накопление уже
  # делает сам генератор через load_existing_customers).
  if [ "$i" -le "$RAMP_DAYS" ]; then
    customers_count=$((80 + i * 4))      # 80  -> ~188 к концу рампы
    orders_count=$((300 + i * 44))     # 300 -> ~1488 к концу рампы
  else
    plateau_day=$((i - RAMP_DAYS))
    customers_count=$((80 + RAMP_DAYS * 4 + plateau_day))       # ~188 -> ~242
    orders_count=$((300 + RAMP_DAYS * 44 + plateau_day * 5))  # ~1488 -> ~1758
  fi

  # Недельная сезонность поверх кривой роста — будни/выходные не
  # одинаковы по объёму (см. weekday_multipliers в growth_config.json).
  # Мультипликативный день-в-день шум сверху этого добавляет уже сам
  # generate_data.py (resolve_count(), включается вместе с --launch-date).
  weekday=$(date -d "${load_date}" +%u)
  weekday_multiplier=$(_weekday_multiplier "${weekday}")
  customers_count=$(_apply_multiplier "${customers_count}" "${weekday_multiplier}")
  orders_count=$(_apply_multiplier "${orders_count}" "${weekday_multiplier}")

  payments_count=$((orders_count * PAYMENTS_MARGIN_PCT / 100))

  echo "=== ${load_date} (день ${day_number}/${DAYS}): customers=${customers_count} orders=${orders_count} payments=${payments_count} ==="

  python generate_data.py \
    --load-date "${load_date}" \
    --data-dir "${DATA_DIR}" \
    --customers-count "${customers_count}" \
    --orders-count "${orders_count}" \
    --payments-count "${payments_count}" \
    --error-rate "${ERROR_RATE}" \
    --launch-date "${START_DATE}" \
    --payment-deadline-hours "${PAYMENT_DEADLINE_HOURS}" \
    --shipping-deadline-hours "${SHIPPING_DEADLINE_HOURS}" \
    --delivery-deadline-days "${DELIVERY_DEADLINE_DAYS}" \
    --pickup-deadline-days "${PICKUP_DEADLINE_DAYS}" \
    --return-window-days "${RETURN_WINDOW_DAYS}" \
    --refund-processing-hours "${REFUND_PROCESSING_HOURS}" \
    --cancel-before-payment-rate "${CANCEL_BEFORE_PAYMENT_RATE}" \
    --cancel-after-payment-rate "${CANCEL_AFTER_PAYMENT_RATE}" \
    --return-rate "${RETURN_RATE}"
done
