-- Семантический слой: витрины дашборда «Аналитика маркетплейса».
--
-- До этого весь SQL витрин жил внутри Superset (virtual datasets в
-- apply_review_fixes.py): определение GMV, «зрелости» заказа, воронки и
-- словари статусов существовали единственным экземпляром внутри BI. Их
-- нельзя было переиспользовать вне дашборда, проверить запросом или
-- посмотреть отдельно от раскладки чартов. Здесь они — объекты
-- хранилища, а Superset становится тонким слоем поверх.
--
-- Отдельная база, а не префикс в marketplace_analytics: сырой и
-- семантический слои разделяются физически, и — практическая деталь —
-- витрина может называться `orders`, не конфликтуя с таблицей
-- `marketplace_analytics.orders`. Благодаря этому имена витрин совпадают
-- с именами датасетов в Superset один в один.
--
-- VIEW, а не MATERIALIZED VIEW: объёмы MVP считаются на лету за
-- миллисекунды, а материализация здесь означала бы ещё один слой,
-- который нужно обновлять и инвалидировать. Пересматривать при росте
-- данных, а не заранее.
--
-- FINAL обязателен везде, где читаются customers/orders/payments: это
-- ReplacingMergeTree, и одна и та же сущность лежит в нескольких
-- версиях (см. README.md, «Запросы к ClickHouse»). Ровно поэтому
-- витрины первого уровня ниже существуют — чтобы FINAL и словари не
-- переписывались заново в каждой витрине и в каждом чарте.
--
-- {{MATURITY_DAYS}} и {{CUSTOMER_MATURITY_DAYS}} подставляются при
-- применении (load_to_clickhouse.py, ensure_marts) из growth_config.json
-- — того же файла, что читают генератор, backfill и Airflow DAG.
-- Захардкодить их здесь значило бы завести хранилищу собственную копию
-- бизнес-правил.

CREATE DATABASE IF NOT EXISTS marketplace_marts;

-- ---------------------------------------------------------------------
-- Уровень 1. Дедуплицированные сущности + словари
--
-- Единственное место, где произносится FINAL и где enum'ы источника
-- переводятся в человеческие подписи.
-- ---------------------------------------------------------------------

CREATE OR REPLACE VIEW marketplace_marts.customers AS
SELECT
    *,
    CASE acquisition_channel
        WHEN 'telegram_ads'  THEN 'Реклама в Telegram'
        WHEN 'yandex_direct' THEN 'Яндекс.Директ'
        WHEN 'vk_ads'        THEN 'Реклама во ВКонтакте'
        WHEN 'seo'           THEN 'Поисковый трафик'
        WHEN 'referral'      THEN 'Рекомендации'
        WHEN 'email'         THEN 'Email-рассылка'
        WHEN 'organic'       THEN 'Прямые заходы'
        ELSE acquisition_channel
    END AS acquisition_channel_ru
FROM marketplace_analytics.customers FINAL;

-- is_mature: заказ уже не может сменить статус. Горизонт — сумма всех
-- дедлайнов подряд (оплата, отправка, доставка, забор, окно возврата,
-- обработка рефанда), ровно как её считает compute_lookback_days() в
-- generate_data.py.
--
-- Отсчёт от максимума в данных, а не от now(): остановленный пайплайн
-- иначе съедал бы график целиком — «зрелых» заказов не осталось бы
-- вовсе, хотя данные на месте.
CREATE OR REPLACE VIEW marketplace_marts.orders AS
SELECT
    *,
    CASE status
        WHEN 'new'              THEN 'Новый'
        WHEN 'paid'             THEN 'Оплачен'
        WHEN 'shipped'          THEN 'Отправлен'
        WHEN 'ready_for_pickup' THEN 'Готов к выдаче'
        WHEN 'delivered'        THEN 'Доставлен'
        WHEN 'cancelled'        THEN 'Отменён'
        WHEN 'returned'         THEN 'Возвращён'
        WHEN 'refunded'         THEN 'Возмещён'
        ELSE status
    END AS status_ru,
    CASE cancellation_reason
        WHEN 'cancelled_by_customer_before_payment'
            THEN 'Отменён покупателем до оплаты'
        WHEN 'cancelled_by_customer_after_payment'
            THEN 'Отменён покупателем после оплаты'
        WHEN 'not_paid_in_time'     THEN 'Не оплачен вовремя'
        WHEN 'not_picked_up_in_time' THEN 'Не забрали вовремя'
        ELSE cancellation_reason
    END AS cancellation_reason_ru,
    created_at < (
        SELECT max(created_at) - INTERVAL {{MATURITY_DAYS}} DAY
        FROM marketplace_analytics.orders FINAL
    ) AS is_mature
FROM marketplace_analytics.orders FINAL;

CREATE OR REPLACE VIEW marketplace_marts.payments AS
SELECT
    *,
    CASE status
        WHEN 'success'  THEN 'Успешно'
        WHEN 'failed'   THEN 'Отказ'
        WHEN 'refunded' THEN 'Возмещён'
        ELSE status
    END AS status_ru,
    CASE payment_method
        WHEN 'card'         THEN 'Банковская карта'
        WHEN 'sbp'          THEN 'СБП'
        WHEN 'wallet'       THEN 'Электронный кошелёк'
        WHEN 'installments' THEN 'Рассрочка'
        ELSE payment_method
    END AS payment_method_ru,
    -- Исход авторизации и последующий возврат — разные события, и
    -- смешивать их нельзя. Возврат переиздаёт платёж под тем же
    -- payment_id со статусом refunded, поэтому после FINAL успешная
    -- попытка исчезает: success rate за июнь меняется в сентябре, хотя
    -- в июне банк её провёл. attempt_succeeded помнит исход попытки,
    -- is_refunded — что было с деньгами дальше.
    -- CAST снимает LowCardinality: status объявлен как
    -- LowCardinality(String), сравнение с ним возвращает
    -- LowCardinality(UInt8), а такую колонку ClickHouse во вьюхе
    -- создавать отказывается (SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY).
    CAST(status IN ('success', 'refunded'), 'UInt8') AS attempt_succeeded,
    CAST(status = 'refunded', 'UInt8')               AS is_refunded
FROM marketplace_analytics.payments FINAL;

-- ---------------------------------------------------------------------
-- Уровень 2. Витрины дашборда
--
-- Строятся поверх уровня 1, поэтому FINAL и словари здесь уже не
-- повторяются.
-- ---------------------------------------------------------------------

-- Воронка считается только по зрелым заказам: незрелые физически не
-- успели доехать, и без отсечки правый край воронки всегда провален.
--
-- Пять проходов по одной вьюхе вместо одного с arrayJoin — сознательно:
-- на объёмах MVP разница не измерима, а читается это как определение
-- воронки, а не как трюк.
CREATE OR REPLACE VIEW marketplace_marts.order_funnel_stages AS
SELECT 'Создано' AS stage, 1 AS stage_order, count() AS cnt
FROM marketplace_marts.orders WHERE is_mature
UNION ALL
SELECT 'Оплачено', 2, countIf(paid_at IS NOT NULL)
FROM marketplace_marts.orders WHERE is_mature
UNION ALL
SELECT 'Отправлено', 3, countIf(shipped_at IS NOT NULL)
FROM marketplace_marts.orders WHERE is_mature
UNION ALL
SELECT 'Готово к выдаче', 4, countIf(ready_for_pickup_at IS NOT NULL)
FROM marketplace_marts.orders WHERE is_mature
UNION ALL
SELECT 'Доставлено', 5, countIf(delivered_at IS NOT NULL)
FROM marketplace_marts.orders WHERE is_mature;

-- Заказ с измерениями покупателя — основной факт для чартов.
--
-- Именно эта витрина, а не marketplace_marts.orders, стоит под всеми
-- чартами заказов: фильтры дашборда по городу и каналу применяются к
-- измерениям, а измерения есть только здесь. Пока KPI считались по
-- orders, выбор города пересчитывал часть показателей и не трогал
-- остальные — на экране получалась смесь двух разных выборок.
--
-- Поэтому здесь отдаются все вехи заказа, а не только оплата и отмена:
-- иначе чарт, которому нужен returned_at, вынужден идти мимо витрины и
-- терять измерения.
CREATE OR REPLACE VIEW marketplace_marts.orders_with_customer_dim AS
SELECT
    o.order_id               AS order_id,
    o.customer_id            AS customer_id,
    o.created_at             AS created_at,
    o.amount_kopecks         AS amount_kopecks,
    o.status                 AS status,
    o.status_ru              AS status_ru,
    o.paid_at                AS paid_at,
    o.shipped_at             AS shipped_at,
    o.ready_for_pickup_at    AS ready_for_pickup_at,
    o.delivered_at           AS delivered_at,
    o.cancelled_at           AS cancelled_at,
    o.cancellation_reason    AS cancellation_reason,
    o.cancellation_reason_ru AS cancellation_reason_ru,
    o.returned_at            AS returned_at,
    o.refunded_at            AS refunded_at,
    o.is_mature              AS is_mature,
    c.city                   AS city,
    c.acquisition_channel    AS acquisition_channel,
    c.acquisition_channel_ru AS acquisition_channel_ru
FROM marketplace_marts.orders AS o
LEFT JOIN marketplace_marts.customers AS c
       ON c.customer_id = o.customer_id;

-- Первый заказ покупателя и задержка до него.
--
-- nullIf(..., toDateTime(0)) обязателен: join_use_nulls в ClickHouse по
-- умолчанию выключен, поэтому у покупателя без заказов LEFT JOIN даёт не
-- NULL, а значение по умолчанию — 1970-01-01. Без этого у не купивших
-- «первый заказ» приходился бы на начало эпохи, а days_to_first_order
-- уходил в минус на двадцать тысяч дней.
--
-- is_mature здесь про покупателя, а не про заказ: у когорты должен быть
-- полный горизонт наблюдения, иначе конверсия «регистрация -> заказ»
-- занижена на свежих регистрациях просто потому, что они ещё не успели.
CREATE OR REPLACE VIEW marketplace_marts.customer_first_order AS
SELECT
    c.customer_id       AS customer_id,
    c.registration_date AS registration_date,
    c.city              AS city,
    c.acquisition_channel_ru AS acquisition_channel_ru,
    countIf(o.order_id != '') AS orders_count,
    -- Явный признак, а не «дата первого заказа не пуста»: наличие
    -- заказа выводить из даты нельзя, потому что LEFT JOIN подставляет
    -- не NULL, а значение по умолчанию (см. nullIf ниже). Именно на
    -- этом конверсия показывала ровно 100% при любых данных.
    orders_count > 0 AS has_order,
    -- Вторая метрика, а не та же самая: «когда-нибудь купил» и «купил
    -- в окно наблюдения» отвечают на разные вопросы и расходятся тем
    -- сильнее, чем длиннее история. Окно то же, по которому когорта
    -- признаётся зрелой ниже, — иначе метрика считалась бы по клиентам,
    -- у которых это окно ещё не закрылось.
    countIf(
        o.order_id != ''
        AND o.created_at >= c.registration_date
        AND o.created_at
            < c.registration_date + INTERVAL {{CUSTOMER_MATURITY_DAYS}} DAY
    ) > 0 AS converted_within_window,
    nullIf(min(o.created_at), toDateTime(0)) AS first_order_at,
    dateDiff('day', c.registration_date, nullIf(min(o.created_at), toDateTime(0)))
        AS days_to_first_order,
    c.registration_date < (
        SELECT max(created_at) - INTERVAL {{CUSTOMER_MATURITY_DAYS}} DAY
        FROM marketplace_analytics.orders FINAL
    ) AS is_mature
FROM marketplace_marts.customers AS c
LEFT JOIN marketplace_marts.orders AS o
       ON o.customer_id = c.customer_id
GROUP BY
    c.customer_id,
    c.registration_date,
    c.city,
    c.acquisition_channel_ru;

-- Распределение задержки до первого заказа — замена «среднему по дате
-- регистрации», которое рисовало арку окна наблюдения вместо поведения
-- покупателей.
CREATE OR REPLACE VIEW marketplace_marts.first_order_delay_distribution AS
SELECT
    days_to_first_order,
    count() AS customers
FROM marketplace_marts.customer_first_order
WHERE is_mature
  AND days_to_first_order >= 0
GROUP BY days_to_first_order
ORDER BY days_to_first_order;

-- Распределение числа попыток оплаты на заказ.
--
-- Считаются все попытки, включая возмещённые: прежняя версия отбирала
-- только success и failed, и заказ, по которому позже прошёл возврат,
-- исчезал из распределения целиком вместе со своими попытками.
--
-- Колонка называется attempts, а не retries: одна попытка — это ноль
-- ретраев, и подпись «число ретраев = 1» для единственной попытки
-- вводила в заблуждение.
CREATE OR REPLACE VIEW marketplace_marts.payment_retries_distribution AS
SELECT attempts, count() AS orders_count
FROM (
    SELECT order_id, count() AS attempts
    FROM marketplace_marts.payments
    GROUP BY order_id
)
GROUP BY attempts
ORDER BY attempts;

-- Сырые длительности обработки рефанда, а не дневные средние: перцентиль
-- по средним — это не перцентиль по заказам.
CREATE OR REPLACE VIEW marketplace_marts.refund_processing_times AS
SELECT
    order_id,
    refunded_at,
    if(cancelled_at IS NOT NULL, 'Отмена', 'Возврат товара') AS refund_kind,
    dateDiff('second', coalesce(cancelled_at, returned_at), refunded_at)
        AS duration_seconds
FROM marketplace_marts.orders
WHERE refunded_at IS NOT NULL
  AND coalesce(cancelled_at, returned_at) IS NOT NULL
  AND refunded_at >= coalesce(cancelled_at, returned_at);

-- ---------------------------------------------------------------------
-- Витрины качества данных
-- ---------------------------------------------------------------------

-- Одна строка карантина может нести несколько причин через ' | ' (см.
-- add_dq_reason в transform.py), поэтому splitByString + arrayJoin:
-- иначе «CUSTOMER_ID_EMPTY | REGISTRATION_DATE_INVALID» считалось бы
-- отдельной категорией, а не двумя ошибками.
CREATE OR REPLACE VIEW marketplace_marts.dq_reason_breakdown AS
SELECT
    CASE reason
        WHEN 'CUSTOMER_ID_EMPTY'                THEN 'Пустой ID покупателя'
        WHEN 'CUSTOMER_ID_DUPLICATE_IN_LOAD'    THEN 'Дубликат ID покупателя в загрузке'
        WHEN 'CUSTOMER_ID_DUPLICATE_IN_HISTORY' THEN 'Дубликат ID покупателя в истории'
        WHEN 'REGISTRATION_DATE_INVALID'        THEN 'Некорректная дата регистрации'
        WHEN 'ORDER_ID_EMPTY'                   THEN 'Пустой ID заказа'
        WHEN 'ORDER_ID_DUPLICATE_IN_LOAD'       THEN 'Дубликат ID заказа в загрузке'
        WHEN 'CUSTOMER_NOT_FOUND'               THEN 'Покупатель не найден'
        WHEN 'CREATED_AT_INVALID'               THEN 'Некорректная дата создания'
        WHEN 'ORDER_AMOUNT_INVALID'             THEN 'Некорректная сумма заказа'
        WHEN 'ORDER_STATUS_INVALID'             THEN 'Некорректный статус заказа'
        WHEN 'CANCELLATION_REASON_INCONSISTENT' THEN 'Причина отмены не соответствует статусу'
        WHEN 'REFUND_TIMELINE_INVALID'          THEN 'Некорректные сроки возврата'
        WHEN 'PAYMENT_ID_EMPTY'                 THEN 'Пустой ID платежа'
        WHEN 'PAYMENT_ID_DUPLICATE_IN_LOAD'     THEN 'Дубликат ID платежа в загрузке'
        WHEN 'ORDER_NOT_FOUND'                  THEN 'Заказ не найден'
        WHEN 'PAYMENT_DATE_INVALID'             THEN 'Некорректная дата платежа'
        WHEN 'PAYMENT_AMOUNT_INVALID'           THEN 'Некорректная сумма платежа'
        WHEN 'PAYMENT_STATUS_INVALID'           THEN 'Некорректный статус платежа'
        WHEN 'PAYMENT_BEFORE_ORDER'             THEN 'Платёж раньше заказа'
        ELSE reason
    END AS reason,
    load_date,
    quarantined_rows
FROM (
    SELECT
        arrayJoin(splitByString(' | ', dq_reason)) AS reason,
        load_date,
        count() AS quarantined_rows
    FROM (
        SELECT dq_reason, load_date
        FROM marketplace_analytics.quarantine_customers
        UNION ALL
        SELECT dq_reason, load_date
        FROM marketplace_analytics.quarantine_orders
        UNION ALL
        SELECT dq_reason, load_date
        FROM marketplace_analytics.quarantine_payments
    )
    GROUP BY reason, load_date
);

CREATE OR REPLACE VIEW marketplace_marts.quarantine_volume AS
SELECT load_date, 'Покупатели' AS entity, count() AS quarantined_rows
FROM marketplace_analytics.quarantine_customers
GROUP BY load_date
UNION ALL
SELECT load_date, 'Заказы', count()
FROM marketplace_analytics.quarantine_orders
GROUP BY load_date
UNION ALL
SELECT load_date, 'Платежи', count()
FROM marketplace_analytics.quarantine_payments
GROUP BY load_date;

-- Свежесть пайплайна. _load_commits пишется только после того, как все
-- таблицы за load_date догружены целиком, поэтому это честный ответ на
-- «данные за какой день доступны», а не «какие файлы вроде на месте».
CREATE OR REPLACE VIEW marketplace_marts.load_freshness AS
SELECT
    max(load_date) AS last_load_date,
    dateDiff('hour', max(loaded_at), now()) AS hours_since_load,
    count() AS entities_loaded
FROM marketplace_analytics._load_commits
WHERE load_date = (
    SELECT max(load_date) FROM marketplace_analytics._load_commits
);
