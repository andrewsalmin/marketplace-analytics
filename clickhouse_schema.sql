-- Схема ClickHouse для analytics-платформы — единственный источник
-- истины (сверяй с кластером перед --force-reload в load_to_clickhouse.py,
-- т.к. DROP PARTITION требует совпадения PARTITION BY).
--
-- customers/orders/payments переиздаются по мере продвижения state
-- machine (см. README.md) через ReplacingMergeTree(load_date) —
-- поэтому SELECT обязан использовать FINAL или argMax(..., load_date).
-- quarantine_*/dq_metrics — обычный MergeTree, не переиздаются.
--
-- Версия строки — load_date. Раньше эту роль играл ingested_at, время
-- записи строки: он означал «свежее то, что залито позже», и одного
-- перезалитого старого дня хватало, чтобы он оказался свежее всех
-- последующих — FINAL начинал отдавать состояние заказа на тот день
-- вместо нынешнего. Дата загрузки говорит то же самое о данных, а не о
-- процессе, и от порядка перезаливки не зависит вовсе. Самой колонки
-- ingested_at больше нет: без этой роли её никто не читал, а когда
-- какой день приехал, помнит _load_commits.
--
-- На уже поднятом кластере смена движка сама не произойдёт: DDL здесь
-- весь через CREATE TABLE IF NOT EXISTS. Существующие таблицы нужно
-- пересоздать и перелить — INSERT ... SELECT в таблицу с новым движком
-- и RENAME на её место.

CREATE DATABASE IF NOT EXISTS marketplace_analytics;

CREATE TABLE IF NOT EXISTS marketplace_analytics.customers
(
    customer_id         String,
    registration_date   DateTime,
    city                LowCardinality(String),
    acquisition_channel LowCardinality(String),
    email               String,
    load_date           Date
)
ENGINE = ReplacingMergeTree(load_date)
PARTITION BY load_date
ORDER BY (customer_id);

CREATE TABLE IF NOT EXISTS marketplace_analytics.orders
(
    order_id            String,
    customer_id         String,
    created_at          DateTime,
    amount_kopecks      Int64,
    status              LowCardinality(String),
    cancellation_reason Nullable(String),
    payment_method      Nullable(String),
    paid_at             Nullable(DateTime),
    shipped_at          Nullable(DateTime),
    ready_for_pickup_at Nullable(DateTime),
    delivered_at        Nullable(DateTime),
    cancelled_at        Nullable(DateTime),
    returned_at         Nullable(DateTime),
    refunded_at         Nullable(DateTime),
    load_date           Date
)
ENGINE = ReplacingMergeTree(load_date)
PARTITION BY load_date
-- Ключ синхронизирован с README.md ("Запросы к ClickHouse: FINAL
-- обязателен") — created_at/customer_id у заказа не меняются, order_id
-- один и тот же для всех переизданий, так что ключ фактически
-- эквивалентен order_id, но упорядочивает данные по времени создания.
ORDER BY (created_at, customer_id, order_id);

CREATE TABLE IF NOT EXISTS marketplace_analytics.payments
(
    payment_id     String,
    order_id       String,
    payment_date   DateTime,
    amount_kopecks Int64,
    payment_method LowCardinality(String),
    status         LowCardinality(String),
    load_date      Date
)
ENGINE = ReplacingMergeTree(load_date)
PARTITION BY load_date
-- payment_id переиздаётся при рефанде (generate_data.py:
-- _build_refund_payment_row переиспользует pay_{order_id}) — ключ на
-- payment_id разрешает success/refunded версии одного платежа.
ORDER BY (payment_id);

CREATE TABLE IF NOT EXISTS marketplace_analytics.quarantine_customers
(
    customer_id           Nullable(String),
    registration_date    Nullable(DateTime),
    city                 LowCardinality(String),
    acquisition_channel  LowCardinality(String),
    email                String,
    dq_reason            String,
    quarantine_timestamp DateTime64(3),
    load_date            Date
)
ENGINE = MergeTree
PARTITION BY load_date
ORDER BY (load_date, customer_id)
-- customer_id может быть NULL (см. CUSTOMER_ID_EMPTY в transform.py) и при
-- этом остаётся частью ключа сортировки — ClickHouse требует явного
-- разрешения на Nullable-колонки в ORDER BY.
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS marketplace_analytics.quarantine_orders
(
    order_id             String,
    customer_id          Nullable(String),
    created_at           Nullable(DateTime),
    amount_kopecks       Int64,
    status               String,
    cancellation_reason  Nullable(String),
    payment_method       Nullable(String),
    paid_at              Nullable(DateTime),
    shipped_at           Nullable(DateTime),
    ready_for_pickup_at  Nullable(DateTime),
    delivered_at         Nullable(DateTime),
    cancelled_at         Nullable(DateTime),
    returned_at          Nullable(DateTime),
    refunded_at          Nullable(DateTime),
    dq_reason            String,
    quarantine_timestamp DateTime64(3),
    load_date            Date
)
ENGINE = MergeTree
PARTITION BY load_date
ORDER BY (load_date, order_id);

CREATE TABLE IF NOT EXISTS marketplace_analytics.quarantine_payments
(
    payment_id            String,
    order_id              Nullable(String),
    payment_date          Nullable(DateTime),
    amount_kopecks        Int64,
    payment_method        LowCardinality(String),
    status                String,
    dq_reason             String,
    quarantine_timestamp  DateTime64(3),
    load_date             Date
)
ENGINE = MergeTree
PARTITION BY load_date
ORDER BY (load_date, payment_id);

CREATE TABLE IF NOT EXISTS marketplace_analytics.dq_metrics
(
    load_date   Date,
    entity      LowCardinality(String),
    valid_rows  Int64,
    invalid_rows Int64,
    total_rows  Int64
)
ENGINE = MergeTree
PARTITION BY load_date
ORDER BY (load_date, entity);

-- Маркер идемпотентной загрузки. Пишется load_to_clickhouse.py ПОСЛЕ
-- того, как все таблицы за load_date успешно загружены — тот же
-- принцип, что commit.json в generate_data.py (publish_batch): маркер
-- как единственный источник истины о том, что батч точно догружен
-- целиком, а не "файлы вроде на месте".
CREATE TABLE IF NOT EXISTS marketplace_analytics._load_commits
(
    load_date  Date,
    entity     LowCardinality(String),
    rows       UInt64,
    loaded_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY load_date
ORDER BY (load_date, entity);
