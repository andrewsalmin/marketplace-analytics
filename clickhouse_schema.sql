-- Схема ClickHouse для analytics-платформы.
--
-- Раньше эта схема нигде не версионировалась — таблицы существовали
-- только в живом кластере, не в репозитории. Этот файл — единственный
-- источник истины: применяй его целиком на пустой базе, либо сверяй
-- вручную с уже развёрнутыми таблицами перед тем, как полагаться на
-- --force-reload в load_to_clickhouse.py (DROP PARTITION работает
-- только если PARTITION BY у реальной таблицы совпадает с этой схемой).
--
-- Принцип: clients/orders/payments переиздаются построчно по мере
-- продвижения state machine (см. README.md, "State machine заказа").
-- ReplacingMergeTree(ingested_at) разрешает версии по ключу ORDER BY —
-- SELECT обязан использовать FINAL (или argMax(..., ingested_at)),
-- иначе между вставкой и фоновым мерджем в таблице будут физически
-- лежать сразу все версии строки. quarantine_*/dq_metrics не
-- переиздаются — обычный MergeTree.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.clients
(
    client_id           String,
    registration_date   DateTime,
    city                LowCardinality(String),
    acquisition_channel LowCardinality(String),
    email               String,
    load_date           Date,
    ingested_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY load_date
ORDER BY (client_id);

CREATE TABLE IF NOT EXISTS analytics.orders
(
    order_id            String,
    client_id           String,
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
    load_date           Date,
    ingested_at         DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY load_date
-- Ключ синхронизирован с README.md ("Запросы к ClickHouse: FINAL
-- обязателен") — created_at/client_id у заказа не меняются, order_id
-- один и тот же для всех переизданий, так что ключ фактически
-- эквивалентен order_id, но упорядочивает данные по времени создания.
ORDER BY (created_at, client_id, order_id);

CREATE TABLE IF NOT EXISTS analytics.payments
(
    payment_id     String,
    order_id       String,
    payment_date   DateTime,
    amount_kopecks Int64,
    payment_method LowCardinality(String),
    status         LowCardinality(String),
    load_date      Date,
    ingested_at    DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY load_date
-- payment_id переиздаётся при рефанде (generate_data.py:
-- _build_refund_payment_row переиспользует pay_{order_id}) — ключ на
-- payment_id разрешает success/refunded версии одного платежа.
ORDER BY (payment_id);

CREATE TABLE IF NOT EXISTS analytics.quarantine_clients
(
    client_id            Nullable(String),
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
ORDER BY (load_date, client_id)
-- client_id может быть NULL (см. CLIENT_ID_EMPTY в transform.py) и при
-- этом остаётся частью ключа сортировки — ClickHouse требует явного
-- разрешения на Nullable-колонки в ORDER BY.
SETTINGS allow_nullable_key = 1;

CREATE TABLE IF NOT EXISTS analytics.quarantine_orders
(
    order_id             String,
    client_id            Nullable(String),
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

CREATE TABLE IF NOT EXISTS analytics.quarantine_payments
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

CREATE TABLE IF NOT EXISTS analytics.dq_metrics
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
CREATE TABLE IF NOT EXISTS analytics._load_commits
(
    load_date  Date,
    entity     LowCardinality(String),
    rows       UInt64,
    loaded_at  DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY load_date
ORDER BY (load_date, entity);
