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
START_DATE="2026-06-01"
DAYS=92
RESUME_FROM_DAY="${1:-1}"  # 1-indexed, как в выводе "(день N/DAYS)"

RAMP_DAYS=27  # первые ~4 недели — фаза роста, дальше — плато

# --- Политика бизнеса: постоянна весь месяц, НЕ часть кривой роста ---
PAYMENT_DEADLINE_HOURS=24
SHIPPING_DEADLINE_HOURS=48
DELIVERY_DEADLINE_DAYS=7
PICKUP_DEADLINE_DAYS=7
RETURN_WINDOW_DAYS=14
REFUND_PROCESSING_HOURS=72
CANCEL_BEFORE_PAYMENT_RATE=0.02
CANCEL_AFTER_PAYMENT_RATE=0.01
RETURN_RATE=0.05
ERROR_RATE=0.01

# --- Запас попыток оплаты над числом заказов: подобран эмпирически на
# ~1%, чтобы доля не оплаченных в срок заказов (cancelled/
# not_paid_in_time) была реалистичной (~1-2%), а не нулевой — большой
# запас (5%+) практически полностью вымывает этот тип отмены из данных.
PAYMENTS_MARGIN_PCT=101

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
  # делает сам генератор через load_existing_clients).
  if [ "$i" -le "$RAMP_DAYS" ]; then
    clients_count=$((80 + i * 4))      # 80  -> ~188 к концу рампы
    orders_count=$((300 + i * 44))     # 300 -> ~1488 к концу рампы
  else
    plateau_day=$((i - RAMP_DAYS))
    clients_count=$((80 + RAMP_DAYS * 4 + plateau_day))       # ~188 -> ~242
    orders_count=$((300 + RAMP_DAYS * 44 + plateau_day * 5))  # ~1488 -> ~1758
  fi
  payments_count=$((orders_count * PAYMENTS_MARGIN_PCT / 100))

  echo "=== ${load_date} (день ${day_number}/${DAYS}): clients=${clients_count} orders=${orders_count} payments=${payments_count} ==="

  python generate_data.py \
    --load-date "${load_date}" \
    --data-dir "${DATA_DIR}" \
    --clients-count "${clients_count}" \
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
