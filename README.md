# Marketplace Analytics

[![CI](https://github.com/andrewsalmin/marketplace-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/andrewsalmin/marketplace-analytics/actions/workflows/ci.yml)

Аналитический пайплайн маркетплейса на синтетических данных: от ежедневной
выгрузки учётной системы до дашборда в Superset.

**Дашборд:** [andrewsalmin.com/marketplace-analytics](https://andrewsalmin.com/marketplace-analytics),
доступ без регистрации, данные обновляются ежедневно.

[![Обзорный экран дашборда](https://marketplace-analytics.andrewsalmin.com/shots/overview.png)](https://andrewsalmin.com/marketplace-analytics)

## Содержание

- [Задача](#задача)
- [Состав](#состав)
- [Метрики](#метрики)
- [Качество данных](#качество-данных)
- [Хранилище](#хранилище)
- [Генератор данных](#генератор-данных)
- [Запуск](#запуск)
- [Структура репозитория](#структура-репозитория)
- [Тесты и CI](#тесты-и-ci)
- [Зависимости](#зависимости)
- [Известные ограничения](#известные-ограничения)

## Задача

Учётная система маркетплейса с доставкой в пункты выдачи раз в сутки
выгружает покупателей, заказы и платежи. Выгрузка содержит ошибки:
дубликаты идентификаторов, пустые ключи, неположительные суммы,
недопустимые статусы, платежи с датой раньше заказа, противоречия между
статусом и датами.

Требования:

1. Строки с ошибками отделяются от корректных и сохраняются с кодом
   причины.
2. Метрики рассчитываются только по корректным строкам. У каждой метрики
   одно определение, и оно хранится в хранилище, а не в BI.
3. Дашборд показывает метрики бизнеса и качество загрузки.

Источник данных — генератор `generate_data.py`. Он вносит ошибки в
заданной доле и записывает число внесённых ошибок каждого типа в манифест
партии. Манифест позволяет сверить результат валидации с тем, что было
внесено.

## Состав

| Слой | Модуль | Инструменты | Назначение |
|---|---|---|---|
| source | `generate_data.py` | pandas, NumPy, Faker | Выгрузка учётной системы в CSV с внесёнными ошибками |
| raw | `ingest_csv_to_raw.py` | pandas, pyarrow | CSV → Parquet без изменения данных |
| clean / quarantine | `transform.py` | PySpark | DQ-проверки: корректные строки — в clean, остальные — в quarantine с кодом причины; счётчики — в `dq_metrics` |
| хранилище | `load_to_clickhouse.py` | PySpark, ClickHouse Spark Connector | Загрузка в ClickHouse, идемпотентная по `load_date` |
| семантический слой | `clickhouse_marts.sql` | ClickHouse | Витрины дашборда в виде `VIEW` |
| контроль витрин | `marts_checks.py` | ClickHouse | Инварианты витрин на загруженных данных |
| BI | `superset/apply_review_fixes.py` | Superset REST API | Датасеты, чарты, раскладка, права анонимного доступа |

Порядок обработки одного дня:

```
generate_data.py → ingest_csv_to_raw.py → transform.py → load_to_clickhouse.py → marts_checks.py
```

Ежедневно эти шаги выполняет Airflow (`dags/daily_marketplace_pipeline.py`).
Историю за период из `growth_config.json` строят `backfill_history.sh`
(генерация) и `run_downstream_pipeline.sh` (остальные шаги).

GitHub Actions запускает линтер и тесты на push и pull request в `master`.
К обработке данных и к стенду CI отношения не имеет, см.
[«Тесты и CI»](#тесты-и-ci).

Стенд — Superset в Docker за nginx. Сборка, выкладка и права доступа
описаны в [`superset/README.md`](superset/README.md).

## Метрики

Метрики определены в витринах `marketplace_marts` (см.
[«Семантический слой»](#семантический-слой)); дашборд их не пересчитывает.

| Метрика | Формула | Отсечка | Обоснование |
|---|---|---|---|
| GMV | `sum(amount_kopecks) / 100` по заказам с `paid_at IS NOT NULL` | расчёт оплаты, 2 дня | Выручку даёт оплата, а не создание заказа |
| GMV чистый | GMV без заказов с `cancelled_at`, `returned_at` или `refunded_at` | зрелость заказа, 34 дня | До истечения всех сроков заказ может быть отменён или возвращён |
| Заказы | `count()` | нет | Создание заказа — окончательный факт |
| Средний чек | GMV / число оплаченных заказов | расчёт оплаты, 2 дня | Неоплаченные заказы в знаменателе занижали бы чек |
| Доля отмен | заказы с `cancelled_at` / все заказы | зрелость заказа, 34 дня | На свежих заказах отмены ещё не наступили |
| Доля успешных оплат | попытки со статусом `success` или `refunded` / все попытки | нет | Считается по попытке: последующий возврат денег не отменяет успешную авторизацию |
| Конверсия в первый заказ | покупатели с заказом / все покупатели | зрелость покупателя, 30 дней | Недавно зарегистрированные покупатели ещё не успели сделать заказ |
| Доля отклонённых строк | `sum(invalid_rows) / sum(total_rows)` из `dq_metrics` | нет | Характеристика загрузки, а не бизнеса |
| Отставание данных | `max(hours_since_load)` из `load_freshness` | нет | Максимум, а не среднее: отставание одного источника означает отставание всех данных |

Горизонты рассчитываются в `growth.py` из `growth_config.json`:

- **Зрелость заказа — 34 дня** (`maturity_days`). Сумма сроков: оплата
  (1 день) + отправка (2) + доставка (7) + забор (7) + окно возврата (14) +
  возврат денег (3). По её истечении заказ находится в конечном статусе.
  Генератор использует ту же формулу (`compute_lookback_days`).
- **Расчёт оплаты — 2 дня** (`payment_settlement_days`). Срок оплаты плюс
  один день для заказов, созданных в конце суток.
- **Зрелость покупателя — 30 дней** (`customer_maturity_days`). Окно
  наблюдения за новым покупателем.

Отсечка отсчитывается от максимальной даты создания заказа в данных, а не
от текущего времени: при остановке пайплайна зрелые заказы не пропадают с
графиков. Отсутствие зашитых в SQL сроков проверяет
`tests/test_marts.py::test_no_deadline_is_hardcoded_in_sql`.

## Качество данных

![Вкладка «Качество данных»](https://marketplace-analytics.andrewsalmin.com/shots/dq.png)

### Внесение ошибок

`--error-rate` задаёт долю строк с ошибками. В каждой группе строк доля
делится поровну между типами ошибок:

| Строки | Типы ошибок |
|---|---|
| новые покупатели | `customers.null_customer_id`, `customers.duplicate_customer_id_rows`, `customers.duplicate_customer_id_in_history`, `customers.invalid_registration_date` |
| новые заказы | `orders.null_customer_id`, `orders.negative_amount`, `orders.invalid_status`, `orders.duplicate_order_id_rows` |
| заказы, продвинутые по статусам | `orders.cancellation_reason_without_cancelled_status`, `orders.refund_before_cancellation` |
| новые попытки оплаты | `payments.missing_order_id`, `payments.negative_amount`, `payments.invalid_status`, `payments.payment_before_order`, `payments.duplicate_payment_id_rows` |

Число внесённых ошибок каждого типа записывается в
`commit.json → expected_quality_issues`. Если партия слишком мала для
ошибки какого-либо типа, генератор пишет предупреждение в лог и
перечисляет такие типы в `commit.json → skipped_quality_issue_types`.

### Правила валидации

`transform.py` проверяет каждую строку набором правил
(`customer_dq_checks`, `order_dq_checks`, `payment_dq_checks`). Строка,
нарушившая хотя бы одно правило, уходит в quarantine. Поле `dq_reason`
содержит коды всех нарушенных правил через ` | `.

Коды `dq_reason` не соответствуют типам внесённых ошибок один к одному:
тип описывает, что испорчено при генерации, код — что обнаружила
валидация. Например, `CUSTOMER_NOT_FOUND` возникает, когда отбракован
покупатель заказа.

| Сущность | Код | Измерение | Условие |
|---|---|---|---|
| customers | `CUSTOMER_ID_EMPTY` | Completeness | `customer_id` пуст или NULL |
| customers | `CUSTOMER_ID_DUPLICATE_IN_LOAD` | Uniqueness | `customer_id` повторяется в партии |
| customers | `CUSTOMER_ID_DUPLICATE_IN_HISTORY` | Uniqueness | `customer_id` есть в clean-слое за предыдущие 365 дней |
| customers | `REGISTRATION_DATE_INVALID` | Validity | `registration_date` не разобрана или NULL |
| orders | `ORDER_ID_EMPTY` | Completeness | `order_id` пуст или NULL |
| orders | `ORDER_ID_DUPLICATE_IN_LOAD` | Uniqueness | `order_id` повторяется в партии; переиздание заказа в разных партиях ошибкой не считается |
| orders | `CUSTOMER_ID_EMPTY` | Completeness | `customer_id` пуст или NULL |
| orders | `CUSTOMER_NOT_FOUND` | Consistency | покупателя нет в clean-слое |
| orders | `CREATED_AT_INVALID` | Validity | `created_at` не разобрана или NULL |
| orders | `ORDER_AMOUNT_INVALID` | Validity | `amount_kopecks` NULL или ≤ 0 |
| orders | `ORDER_STATUS_INVALID` | Validity | `status` пуст или не входит в `VALID_ORDER_STATUSES` без учёта регистра |
| orders | `CANCELLATION_REASON_INCONSISTENT` | Consistency | причина отмены заполнена при статусе, отличном от `cancelled` и `refunded`, или статус `cancelled` без причины |
| orders | `REFUND_TIMELINE_INVALID` | Consistency | `refunded_at` раньше `cancelled_at` или `returned_at` |
| payments | `PAYMENT_ID_EMPTY` | Completeness | `payment_id` пуст или NULL |
| payments | `PAYMENT_ID_DUPLICATE_IN_LOAD` | Uniqueness | `payment_id` повторяется в партии |
| payments | `ORDER_ID_EMPTY` | Completeness | `order_id` пуст или NULL |
| payments | `ORDER_NOT_FOUND` | Consistency | заказа нет в clean-слое |
| payments | `PAYMENT_DATE_INVALID` | Validity | `payment_date` не разобрана или NULL |
| payments | `PAYMENT_AMOUNT_INVALID` | Validity | `amount_kopecks` NULL или ≤ 0 |
| payments | `PAYMENT_STATUS_INVALID` | Validity | `status` пуст или не входит в `VALID_PAYMENT_STATUSES` без учёта регистра |
| payments | `PAYMENT_BEFORE_ORDER` | Consistency | `payment_date` раньше `created_at` заказа |

Измерение Timeliness кодами не размечается: задержка данных — свойство
суточных партий, её показывает метрика «Отставание данных».

Счётчики строк по сущностям за день (`valid_rows`, `invalid_rows`,
`total_rows`) записываются в `dq_metrics`.

## Хранилище

### Таблицы

Схема — `clickhouse_schema.sql`. Её применяет каждый запуск
`load_to_clickhouse.py` (`CREATE ... IF NOT EXISTS`).

| Таблицы | Движок |
|---|---|
| `customers`, `orders`, `payments` | `ReplacingMergeTree(load_date)` |
| `quarantine_customers`, `quarantine_orders`, `quarantine_payments`, `dq_metrics`, `_load_commits` | `MergeTree` |

Все таблицы партиционированы по `load_date`.

Изменение состояния заказа или платежа приходит новой строкой с тем же
ключом. Версия строки — `load_date`, а не время вставки: перезагрузка
старого дня не делает его данные новее данных последующих дней.

### FINAL

Новая версия заказа записывается в партицию своего дня, прежняя остаётся в
партиции прежнего. Фоновое слияние `ReplacingMergeTree` не объединяет куски
из разных партиций, поэтому обе версии хранятся в таблице постоянно, а не
до слияния. Запрос без `FINAL` учитывает заказ столько раз, сколько у него
версий:

```sql
SELECT status, count() FROM orders GROUP BY status          -- неверно
SELECT status, count() FROM orders FINAL GROUP BY status    -- верно
```

`FINAL` разрешает версии по ключу сортировки в момент запроса; других
механизмов дедупликации в этой схеме нет. Признак ошибки: без `FINAL`
`count()` больше `uniqExact(order_id)`. Альтернатива `FINAL` —
`argMax(<колонка>, load_date)` с группировкой по `order_id`.

Ключ сортировки `orders` — `(created_at, customer_id, order_id)`;
`created_at` и `customer_id` у заказа не меняются, поэтому версии
определяются по `order_id`.

Версии за разные дни остаются в таблице из-за схемы партиционирования, а не
как журнал событий: восстанавливать по ним последовательность переходов
нельзя. Даты этапов доступны в последней версии: каждая новая версия заказа
переносит уже заполненные `paid_at`, `shipped_at`, `ready_for_pickup_at`,
`delivered_at`, `cancelled_at`, `returned_at`, `refunded_at`.

### Семантический слой

`clickhouse_marts.sql` создаёт витрины в отдельной базе
`marketplace_marts`. Датасеты Superset ссылаются на витрины и не содержат
SQL.

- **Уровень 1** — `customers`, `orders`, `payments`: `FINAL`, перевод
  кодов в подписи, признаки `is_mature`, `is_payment_settled`,
  `attempt_succeeded`, `is_refunded`. `FINAL` используется только на этом
  уровне.
- **Уровень 2** — витрины под чарты поверх уровня 1: `order_funnel_stages`,
  `orders_with_customer_dim`, `orders_by_hour_dow`, `customer_first_order`,
  `first_order_delay_distribution`, `payment_retries_distribution`,
  `customer_cohort_retention`, `refund_processing_times`,
  `dq_reason_breakdown`, `quarantine_volume`, `order_stage_durations`,
  `orders_with_sequence`, `customer_order_counts`, `load_freshness`.

Решения:

- **Отдельная база, а не префикс имён.** Витрина `orders` не конфликтует с
  таблицей `marketplace_analytics.orders`, и имена витрин совпадают с
  именами датасетов Superset.
- **`VIEW`, а не `MATERIALIZED VIEW`.** При текущем объёме расчёт на лету не
  требует отдельного слоя с обновлением и инвалидацией. Решение
  пересматривается при росте данных.
- **Горизонты подставляются при применении.** `{{MATURITY_DAYS}}`,
  `{{PAYMENT_SETTLEMENT_DAYS}}` и `{{CUSTOMER_MATURITY_DAYS}}` берутся из
  `growth_config.json`; собственной копии бизнес-правил в SQL нет.

Витрины пересоздаются при каждом запуске `load_to_clickhouse.py`
(`ensure_marts`), в том числе когда день уже загружен. Без Spark:
`python clickhouse_ddl.py --marts`.

Пользователю загрузчика нужно `GRANT ALL ON marketplace_marts.*`:
`CREATE OR REPLACE VIEW` выполняется через временную таблицу и требует
прав `CREATE TABLE`, `DROP TABLE` и `INSERT`.

Соответствие витрин датасетам Superset проверяет `tests/test_marts.py`.

### Проверки витрин

`marts_checks.py` проверяет витрины на загруженных данных: одна строка на
заказ после дедупликации, монотонное убывание воронки, отсутствие платежей
раньше заказов, сохранение числа заказов в агрегированных витринах, наличие
строки в карантине у каждого незакрытого зрелого заказа и другие инварианты.
Полный список с обоснованиями — словарь `CHECKS`.

Проверки выполняются в конце `run_downstream_pipeline.sh` и задачей
`verify` в DAG. Неудачная проверка останавливает DAG до обновления
дашборда.

### Идемпотентная загрузка

`load_to_clickhouse.py` для одного `load_date`:

1. Применяет схему и витрины.
2. Проверяет `_load_commits`. Если день уже загружен и `--force-reload`
   не указан, завершает работу без запуска Spark.
3. Удаляет партиции дня: сначала маркер в `_load_commits`, затем все
   таблицы с данными (`ALTER TABLE ... DROP PARTITION`).
4. Записывает clean-таблицы, quarantine-таблицы и `dq_metrics`.
5. Записывает маркер дня в `_load_commits`.

Шаг 3 выполняется при любой загрузке, а не только с `--force-reload`. День
без маркера — это день, загрузка которого не завершилась, и часть его строк
уже может быть записана. Без очистки повтор задвоил бы строки в
`quarantine_*` и `dq_metrics`: это `MergeTree`, и `FINAL` их не схлопывает.
Маркер удаляется первым, чтобы сбой во время очистки не оставил день
отмеченным, но без данных. Поэтому повторный запуск после сбоя на любом
шаге безопасен.

## Генератор данных

### Модель

Генератор моделирует маркетплейс с доставкой в пункты выдачи (схема Ozon и
Wildberries). Розничную торговлю с покупкой в магазине он не описывает: у
неё нет цепочки доставки, и она требует другой модели статусов.

Один запуск — одна партия за дату `--load-date`. Формат — CSV: генератор
имитирует сырую выгрузку источника, преобразование в Parquet выполняет
`ingest_csv_to_raw.py`.

Параметры откалиброваны под маркетплейс со смешанными категориями товаров
(одежда, электроника, товары для дома). Для другой товарной вертикали их
нужно пересчитать.

### Контракт партиции

```
data/source/
  customers/load_date=YYYY-MM-DD/customers.csv
  orders/load_date=YYYY-MM-DD/orders.csv
  payments/load_date=YYYY-MM-DD/payments.csv
  _commits/load_date=YYYY-MM-DD/commit.json
```

Партиция доступна для чтения только при наличии
`_commits/load_date=YYYY-MM-DD/commit.json`. Файлы сущностей без маркера
читать нельзя.

`publish_batch()` переносит три сущности из `_staging/<batch_id>/` в
целевые партиции через `Path.replace()` и последним шагом записывает
маркер. Перенос атомарен для каждой сущности, но не для трёх вместе,
поэтому при сбое уже перенесённые сущности возвращаются в staging. В
результате в `data/source` находятся либо все три сущности и маркер, либо
ничего за эту дату. Поведение покрыто
`tests/test_generate_data.py::TestPublishBatchAtomicity`.

Повторный запуск генератора для существующей даты завершается ошибкой.
`ingest_csv_to_raw.py` тоже не перезаписывает существующие raw-партиции.
`transform.py` перезаписывает партиции clean и quarantine за свою дату.

Остальные слои: `data/raw/` (Parquet и манифест ingest в `_manifests/`),
`data/clean/`, `data/quarantine/`, `data/dq_metrics/`.

### Накопление между партиями

- Новые заказы ссылаются на покупателей из всех предыдущих партий
  (`load_existing_customers`).
- Незакрытые заказы из предыдущих партий (`load_existing_orders`)
  продвигаются по статусам (`advance_open_orders`). Изменённый заказ
  записывается новой строкой с тем же `order_id` в партию дня изменения;
  опубликованные партии не изменяются.
- История заказов читается на глубину зрелости заказа
  (`compute_lookback_days`): более старых открытых заказов нет.
- При возврате денег платёж переиздаётся с тем же `payment_id` и статусом
  `refunded`.

Поэтому партии обрабатываются строго по порядку дат: генератор читает
покупателей и заказы предыдущих дней, `transform.py` ищет дубликаты в уже
обработанной истории.

### Статусы заказа

```
new ──(не оплачен за payment-deadline-hours)───────────► cancelled [not_paid_in_time]
new ──(покупатель отменил до оплаты)───────────────────► cancelled [cancelled_by_customer_before_payment]
new ──(успешная оплата)──► paid ──(окно отправки)──► shipped ──(окно доставки)──► ready_for_pickup ──(окно на забор)──► delivered
paid/shipped/ready_for_pickup ──(отмена после оплаты)──► cancelled [cancelled_by_customer_after_payment] ──► refunded
ready_for_pickup ──(не забран за pickup-deadline-days)─► cancelled [not_picked_up_in_time] ──► refunded
delivered ──(возврат товара)────────────────────────────► returned ──► refunded
```

- Заказ создаётся со статусом `new` и продвигается в последующих запусках.
- `refunded` следует за `cancelled` или `returned` и только после успешной
  оплаты. Без оплаты `cancelled` — конечный статус.
- `cancellation_reason` заполняется при переходе в `cancelled` и
  сохраняется после `refunded`. На пути `delivered → returned → refunded`
  причина остаётся `NULL`.
- Переход, ограниченный сроком (отправка, доставка, забор, возврат денег),
  происходит в случайный день внутри окна с вероятностью 0.6 в день и
  принудительно — на дедлайне (`DAILY_ADVANCE_PROBABILITY`).
- Минимальные интервалы между этапами заданы константами `*_MIN_GAP`.

### Параметры распределений

Динамика, шум, календарь и нагрузка включаются флагом `--launch-date`. Без
него `--error-rate` и `--*-count` применяются как есть. `backfill_history.sh`
и DAG передают `--launch-date`, равный `start_date` из `growth_config.json`.
Фактически применённые значения записываются в `commit.json` (`effective_*`,
`load_ratio`, `calendar_day_type`).

| Механизм | Реализация | Значения |
|---|---|---|
| Кривая роста | `backfill_history.sh`, `_volume_for_day()` в DAG | рост 27 дней, затем плато с медленным ростом; коэффициенты в `growth_config.json` |
| Недельная сезонность | `weekday_multipliers` | пт ×1.05, сб ×0.8, вс ×0.85, остальные ×1.0; применяется к покупателям и заказам до вызова генератора |
| Попытки оплаты | `payments_margin_pct` | 101 % от числа заказов |
| Шум объёма | `resolve_count()` | логнормальный, σ = 0.15, независимо для каждой сущности |
| Календарь | `resolve_calendar_day_type()` | промо (11.11, последняя пятница ноября) ×2.5, праздник ×0.75, дни зарплаты (5 и 20 число) ×1.15; при совпадении применяется один множитель в порядке промо > праздник > зарплата |
| Нагрузка | `resolve_load_stress_multiplier()` | если заказов больше базового объёма, доля отказов оплаты и доли отмен умножаются на 1 + 0.4 · (load_ratio − 1); при меньшем объёме не меняются |
| Брак данных | `resolve_error_rate()` | в день запуска ×4 к `--error-rate`, экспоненциальное снижение с постоянной 21 день, шум σ = 0.15, инциденты отдельной сущности длительностью 2–4 дня с множителем ×3–6 |
| Отказы оплаты | `resolve_payment_success_rate()` | базовая успешность 0.95; доля отказов в день запуска ×3, постоянная снижения 14 дней, шум σ = 0.15, инциденты из календаря `payments` |
| Время суток | `WEEKDAY_HOUR_WEIGHTS`, `WEEKEND_HOUR_WEIGHTS` | отдельные почасовые профили для будней и выходных; влияют на `created_at` и `registration_date` |
| Города | `CITY_WEIGHTS` | веса 16 городов-миллионников: Москва 0.30, Санкт-Петербург 0.14, остальные 0.025–0.055; нормируются |
| Каналы привлечения | `ACQUISITION_CHANNEL_WEIGHTS` | organic 0.35, yandex_direct 0.22, social 0.13, vk_ads 0.10, referral 0.10, telegram_ads 0.07, email 0.03 |
| Сумма заказа | `generate_order_amounts()` | логнормальное распределение (μ = 11.9, σ = 0.8): медиана ≈ 1 470 ₽, среднее ≈ 2 000 ₽, ограничение 150–150 000 ₽; множители города (0.88–1.22) и канала (0.87–1.14) нормированы к среднему 1 |
| Частота заказов | `_customer_order_weights()` | вес покупателя из md5(`customer_id`), экспоненциальное распределение, множитель канала 0.6–1.4 |
| Покупатели без заказов | `_customer_never_orders()` | `--never-order-rate` × множитель канала 0.6–1.6, не выше 0.95; признак берётся из другого фрагмента md5, чем вес частоты |

## Запуск

### Генератор

```bash
pip install -r requirements.txt
python generate_data.py --load-date 2026-08-18 --data-dir ./data
```

Параметры по умолчанию:

| Параметр | Значение |
|---|---|
| `--customers-count` | 100 |
| `--orders-count` | 15000 |
| `--payments-count` | 15150 |
| `--error-rate` | 0.01 |
| `--launch-date` | не задан |
| `--payment-deadline-hours` | 24 |
| `--shipping-deadline-hours` | 48 |
| `--delivery-deadline-days` | 7 |
| `--pickup-deadline-days` | 7 |
| `--return-window-days` | 14 |
| `--refund-processing-hours` | 72 |
| `--cancel-before-payment-rate` | 0.02 |
| `--cancel-after-payment-rate` | 0.01 |
| `--return-rate` | 0.05 |
| `--never-order-rate` | 0.18 |

Продвижение заказов по статусам проявляется при запуске на несколько
последовательных дат с одним `--data-dir`.

### Полный прогон

Требования: Python 3.10+, JVM 17, Docker, Linux (скрипты используют GNU
`date`). При первом запуске Spark загружает JAR коннектора ClickHouse из
Maven Central; без доступа к сети JAR подключаются через `spark.jars` или
флаг `--spark-clickhouse-connector`.

**1. Зависимости**

```bash
pip install -r requirements-spark.txt
```

**2. ClickHouse**

```bash
export CLICKHOUSE_PASSWORD=<пароль>
docker compose up -d clickhouse
```

ClickHouse 24.8, пользователь `analytics_user`, база
`marketplace_analytics`, HTTP — `localhost:8123`, native — `localhost:9000`.
Параметры подключения без пароля хранятся в `clickhouse_config.json`, их
читают `load_to_clickhouse.py`, `run_downstream_pipeline.sh` и DAG. Таблицы
создаёт загрузчик.

**3. История источника**

```bash
./backfill_history.sh
```

Период и параметры — `growth_config.json`; при текущих значениях это 92 дня
с 2026-06-01 по 2026-08-31. Продолжение после сбоя:
`./backfill_history.sh <номер дня>`. День, на котором произошёл сбой, не
опубликован и выполняется заново.

**4. Обработка и загрузка**

```bash
./run_downstream_pipeline.sh
```

Для каждого дня выполняются `ingest_csv_to_raw.py`, `transform.py`,
`load_to_clickhouse.py`; после всех дней — `marts_checks.py`. Пароль
берётся из `CLICKHOUSE_PASSWORD`, при её отсутствии запрашивается скрытым
вводом. В аргументах командной строки пароль не передаётся: они видны в
списке процессов.

Продолжение после сбоя: `./run_downstream_pipeline.sh <номер дня>`. Скрипт
повторяет для этого дня все три шага. Если ingest дня уже выполнен,
повторный ingest завершится ошибкой; в этом случае оставшиеся шаги дня
запускаются вручную, а скрипт — со следующего дня.

Скрипты шагов 3 и 4 могут выполняться несколько часов. В SSH-сессии их
запускают в `tmux` или `screen` либо через `nohup`:

```bash
nohup ./run_downstream_pipeline.sh > pipeline.log 2>&1 &
```

Перезагрузка одного дня:

```bash
python load_to_clickhouse.py --load-date 2026-07-28 --force-reload
```

Список загруженных дней:

```bash
docker exec -it analytics-clickhouse clickhouse-client \
  --user analytics_user --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT load_date, entity, rows, loaded_at FROM marketplace_analytics._load_commits ORDER BY load_date"
```

**5. Superset**

`superset/apply_review_fixes.py` приводит датасеты, чарты и раскладку
дашборда к состоянию, описанному в коде, через REST API. Без `--apply`
скрипт выводит план; перед записью сохраняет резервную копию. Подробности —
[`superset/README.md`](superset/README.md).

### Airflow

DAG `daily_marketplace_pipeline`: расписание `@daily` (00:00 UTC), один
`logical_date` за запуск, `catchup=False`.

| Задача | Действие | Повторы |
|---|---|---|
| `generate` | `generate_data.py` с объёмом по кривой роста | 2 |
| `ingest` | `ingest_csv_to_raw.py` | 0 — шаг не идемпотентен |
| `transform` | `transform.py` | 2 |
| `load` | `load_to_clickhouse.py` | 2 |
| `verify` | `marts_checks.py` | 0 — результат детерминирован |
| `refresh_dashboard` | `apply_review_fixes.py --phases p2 --apply`: пересчёт блока выводов на обзоре | 1 |
| `refresh_screenshots` | `docs/refresh_screenshots.sh` | 1 |

- `max_active_runs=1` и `depends_on_past=True`: дни выполняются
  последовательно, и день не запускается после неудачного предыдущего. У
  `refresh_dashboard` и `refresh_screenshots` `depends_on_past=False`: они
  не зависят от предыдущих дней, и неудача одной ночи не блокирует
  следующие.
- Пароль ClickHouse берётся из Airflow Connection `clickhouse_default`; её
  нужно создать до первого запуска.
- Скрипты запускаются через `subprocess` интерпретатором `venv/bin/python`
  в корне репозитория. В этом окружении нужны `requirements-spark.txt` и
  `requirements-superset.txt`; сам Airflow ставится отдельно
  (`requirements-airflow.txt`).
- `refresh_dashboard` и `refresh_screenshots` рассчитаны на стенд: им
  нужны учётные данные Superset и каталог для скриншотов на сервере.

## Структура репозитория

| Путь | Назначение |
|---|---|
| `generate_data.py` | Генератор source-слоя |
| `ingest_csv_to_raw.py` | source → raw |
| `transform.py` | raw → clean / quarantine, `dq_metrics` |
| `load_to_clickhouse.py` | Загрузка в ClickHouse |
| `clickhouse_ddl.py` | Применение схемы и витрин без Spark |
| `clickhouse_schema.sql` | Схема таблиц |
| `clickhouse_marts.sql` | Витрины `marketplace_marts` |
| `marts_checks.py` | Проверки витрин на данных |
| `overview_findings.py` | Текст блока выводов на обзоре дашборда с числами из ClickHouse |
| `growth.py` | Горизонты зрелости из `growth_config.json` |
| `growth_config.json` | Период, кривая роста, сроки, вероятности |
| `clickhouse_config.json` | Подключение к ClickHouse без пароля |
| `backfill_history.sh` | Генерация истории |
| `run_downstream_pipeline.sh` | Обработка и загрузка истории |
| `dags/daily_marketplace_pipeline.py` | DAG Airflow |
| `docker-compose.yml` | Локальный ClickHouse |
| `superset/` | Скрипт настройки дашборда и документация стенда |
| `deploy/` | Конфигурация стенда: nginx, Docker Compose, Dockerfile и `superset_config.py` |
| `docs/refresh_screenshots.sh` | Скриншоты дашборда для README |
| `tests/` | Тесты |
| `.github/workflows/ci.yml` | CI |

## Тесты и CI

```bash
pip install -r requirements-dev.txt -r requirements-superset.txt
pip install -e . --no-deps
pytest -v
```

| Файл | Проверяет | Требует |
|---|---|---|
| `test_generate_data.py` | Число вносимых ошибок, динамику и календарь, валидацию аргументов, генерацию сущностей, статусы заказа, атомарность публикации, множители суммы заказа | — |
| `test_io_layer.py` | Запрет перезаписи raw-партиций, прерванную запись, манифест ingest, разбор SQL-скрипта; маркер `_load_commits` и очистку партиций перед загрузкой | маркер — `pyspark` |
| `test_transform_dq.py` | Каждое DQ-правило на однострочном DataFrame: валидная строка с одним изменённым полем | `pyspark`, JVM 17 |
| `test_marts.py` | Согласованность витрин, датасетов Superset и `growth_config.json`; `FINAL` при каждом чтении `ReplacingMergeTree`; отсутствие зашитых сроков; импорт модулей без правки `sys.path` | — |
| `test_marts_data.py` | Проверки `marts_checks.py` | ClickHouse с данными |
| `test_overview_findings.py` | Формат текста выводов, запрет причинно-следственных формулировок | — |
| `test_superset_charts.py` | Поиск чартов по прежним именам, раскладку, фильтры, удаление чартов, очистку резервных копий | `requests` |
| `test_superset_permissions.py` | Разбор и выдачу прав роли `Public` | `requests` |

При отсутствии зависимости тесты пропускаются (`pytest.importorskip`,
`skipif`).

CI (`.github/workflows/ci.yml`) запускается на push и pull request в
`master`. Python 3.12, зависимости ставятся из lock-файлов с
`--require-hashes`.

| Job | Шаги |
|---|---|
| `test` | `ruff check .`, `pytest -v` без pyspark; тесты, требующие Spark, пропускаются |
| `spark` | Java 17, pyspark, `pytest -v`; тесты Superset пропускаются, они выполняются в `test` |

`test_marts_data.py` в CI не выполняется: ему нужен ClickHouse с
загруженными данными. Те же проверки выполняет пайплайн (`marts_checks.py`).

## Зависимости

Python 3.10+; для Spark — JVM 17.

| Файл | Состав | Lock-файл |
|---|---|---|
| `requirements.txt` | numpy, pandas, faker, pyarrow | `requirements.lock` |
| `requirements-dev.txt` | `requirements.txt` + pytest, ruff | `requirements-dev.lock` |
| `requirements-spark.txt` | `requirements.txt` + pyspark 3.5 | `requirements-spark.lock` (вместе с dev) |
| `requirements-superset.txt` | requests | `requirements-superset.lock` |
| `requirements-airflow.txt` | `requirements.txt` + apache-airflow | нет |

pyspark ограничен версией ниже 4.0: Spark 4 собран со Scala 2.13, а
коннектор `clickhouse-spark-runtime-3.5_2.12` — со Scala 2.12.
`requirements-superset.txt` не включает `requirements.txt`: скрипту
Superset не нужны pandas и pyspark.

Файлы `.txt` редактируются вручную. Файлы `.lock` содержат точные версии
и хеши всего дерева зависимостей и пересобираются:

```bash
pip install pip-tools
pip-compile --generate-hashes --strip-extras --output-file=requirements.lock requirements.txt
pip-compile --generate-hashes --strip-extras --output-file=requirements-dev.lock requirements-dev.txt
pip-compile --generate-hashes --strip-extras --output-file=requirements-spark.lock requirements-dev.txt requirements-spark.txt
pip-compile --generate-hashes --strip-extras --output-file=requirements-superset.lock requirements-superset.txt
```

У Airflow lock-файла нет: его дерево зависимостей определяется версией
Python и constraints самого Airflow и разрешается в окружении установки.

## Известные ограничения

### Незакрытые заказы старше горизонта зрелости

Часть заказов старше 34 дней хранится в незавершённом статусе. Причина —
внесённые ошибки: закрывающая строка заказа получает дефект
(`CANCELLATION_REASON_INCONSISTENT` или `REFUND_TIMELINE_INVALID`),
`transform.py` отправляет её в карантин, и в ClickHouse остаётся
предыдущая, открытая версия. Проверка
«SLA: незакрытые зрелые заказы объяснены карантином» в `marts_checks.py`
требует строки в `quarantine_orders` для каждого такого заказа.

Следствие: доли статусов на дашборде рассчитаны по строкам, прошедшим
проверки, и смещены в сторону незавершённых статусов.

### Дубликаты идентификаторов ищутся за 365 дней

`read_historical_keys()` читает clean-слой за `DUPLICATE_LOOKBACK_DAYS`
(365) дней: ограничение по дате позволяет Spark отсекать партиции.
Идентификатор, повторно выданный больше чем через год,
`CUSTOMER_ID_DUPLICATE_IN_HISTORY` не обнаружит. При чтении через `FINAL`
такие строки схлопнутся по ключу: задвоения в витринах не будет, но
метрика качества их не отразит.

### Конец периода

Отмена или возврат заказа происходят только в последующих запусках
генератора. Для заказов, созданных в последние 34 дня перед
`max(load_date)`, отмены и возвраты ещё не наступили, и доля отмен по дате
создания к концу периода снижается. Витрины учитывают это отсечкой по
зрелости. В собственных запросах нужно отбрасывать последние 34 дня или
считать долю в фиксированном окне от даты создания заказа.

### Агрегация по дню недели

Если период не кратен 7 дням, дни недели входят в него разное число раз, и
`SUM` или `COUNT` по дню недели искажает сезонность. Для таких агрегаций
используется среднее за день: `COUNT(*) / COUNT(DISTINCT <дата>)`.

### Перенос праздников

`_holiday_dates_for_year()` переносит праздник, выпавший на выходной, на
следующий будний день. Фактические переносы устанавливаются постановлением
Правительства РФ на каждый год и могут отличаться. Таблица фактических дат
потребовала бы ежегодного обновления.

### Упрощения модели

- Нет товарного измерения: позиций заказа, SKU, категорий.
- Покупатели неизменны: нет истории атрибутов (SCD2) и дублей одного
  человека под разными `customer_id`.
- Задержек доставки нет: время доставки и забора всегда меньше срока.
- Партия — календарный день. Переходы по статусам вычисляются раз в сутки,
  поэтому событие попадает в выгрузку с задержкой до суток; метки времени
  при этом точны. Частые партии потребуют другого контракта и схемы
  партиционирования.
- Почасовой профиль один для всех будних дней и один для выходных.
- Seed зависит только от `--load-date`. Запуск с теми же параметрами на той
  же истории воспроизводит партию.
- `load_existing_customers` читает покупателей из всех партий, и время
  запуска генератора растёт линейно с длиной истории.
