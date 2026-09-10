"""DQ-слой на Spark: raw-партиция -> clean / quarantine + dq_metrics.

Каждая строка проходит набор проверок; провалившие уходят в quarantine
с полем dq_reason, остальные — в clean. Оба слоя пересчитываемые:
повторный запуск за ту же дату перезаписывает её партицию.
"""
import argparse
import logging
from datetime import date
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

VALID_ORDER_STATUSES = [
    "new",
    "paid",
    "shipped",
    "ready_for_pickup",
    "delivered",
    "cancelled",
    "returned",
    "refunded",
]

VALID_PAYMENT_STATUSES = ["success", "failed", "refunded"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--load-date",
        required=True,
        help="Дата обрабатываемой загрузки: YYYY-MM-DD",
    )

    parser.add_argument(
        "--data-dir",
        default="data",
        help="Корневая директория данных",
    )

    args = parser.parse_args()

    try:
        date.fromisoformat(args.load_date)
    except ValueError:
        parser.error("--load-date должен быть в формате YYYY-MM-DD")

    return args


def add_dq_reason(df, checks):
    """Добавляет поле dq_reason по списку проверок.

    checks — пары (условие ошибки как Spark Column, текст причины).
    У валидной строки dq_reason остаётся пустой строкой.
    """
    reason_columns = [
        F.when(condition, F.lit(reason))
        for condition, reason in checks
    ]

    return df.withColumn(
        "dq_reason",
        F.concat_ws(" | ", *reason_columns),
    )


# ---------------------------------------------------------------------
# DQ-правила
#
# Вынесены из transform_* отдельно и намеренно не трогают ни файлы, ни
# SparkSession: правило — это выражение над именами колонок, поэтому его
# можно проверить на DataFrame из нескольких строк (см.
# tests/test_transform_dq.py). Служебные колонки (duplicate_count,
# exists_in_history, customer_exists, order_exists,
# related_order_created_at) считают вызывающие функции — здесь они
# считаются уже присутствующими.
# ---------------------------------------------------------------------


def _is_blank(column: str):
    """NULL или строка из одних пробелов — для источника это одно и то же."""
    return F.col(column).isNull() | (F.trim(F.col(column)) == "")


def customer_dq_checks() -> list[tuple[Column, str]]:
    return [
        (_is_blank("customer_id"), "CUSTOMER_ID_EMPTY"),
        (F.col("duplicate_count") > 1, "CUSTOMER_ID_DUPLICATE_IN_LOAD"),
        (
            F.col("exists_in_history").isNotNull(),
            "CUSTOMER_ID_DUPLICATE_IN_HISTORY",
        ),
        (F.col("registration_date").isNull(), "REGISTRATION_DATE_INVALID"),
    ]


def order_dq_checks() -> list[tuple[Column, str]]:
    normalized_status = F.lower(F.trim(F.col("status")))

    # Причина отмены осмысленна только у cancelled/refunded, и наоборот:
    # cancelled без причины — потерянный при выгрузке атрибут. Сравнение
    # идёт с normalized_status, а не с сырым status: иначе источник,
    # приславший "Cancelled", получал бы ORDER_STATUS_INVALID = нет (эта
    # проверка регистронезависима), но CANCELLATION_REASON_INCONSISTENT
    # = да, и причина отмены была бы объявлена лишней у корректно
    # отменённого заказа.
    cancellation_reason_inconsistent = (
        F.col("cancellation_reason").isNotNull()
        & (~normalized_status.isin(["cancelled", "refunded"]))
    ) | (
        (normalized_status == "cancelled")
        & F.col("cancellation_reason").isNull()
    )

    # Деньги не могут вернуться раньше, чем заказ отменён или возвращён.
    refunded_before_cancelled = F.col("cancelled_at").isNotNull() & (
        F.col("refunded_at") < F.col("cancelled_at")
    )
    refunded_before_returned = F.col("returned_at").isNotNull() & (
        F.col("refunded_at") < F.col("returned_at")
    )
    refund_timeline_invalid = F.col("refunded_at").isNotNull() & (
        refunded_before_cancelled | refunded_before_returned
    )

    return [
        (_is_blank("order_id"), "ORDER_ID_EMPTY"),
        (F.col("duplicate_count") > 1, "ORDER_ID_DUPLICATE_IN_LOAD"),
        (_is_blank("customer_id"), "CUSTOMER_ID_EMPTY"),
        (F.col("customer_exists").isNull(), "CUSTOMER_NOT_FOUND"),
        (F.col("created_at").isNull(), "CREATED_AT_INVALID"),
        (
            F.col("amount_kopecks").isNull() | (F.col("amount_kopecks") <= 0),
            "ORDER_AMOUNT_INVALID",
        ),
        (
            _is_blank("status")
            | (~normalized_status.isin(VALID_ORDER_STATUSES)),
            "ORDER_STATUS_INVALID",
        ),
        (cancellation_reason_inconsistent, "CANCELLATION_REASON_INCONSISTENT"),
        (refund_timeline_invalid, "REFUND_TIMELINE_INVALID"),
    ]


def payment_dq_checks() -> list[tuple[Column, str]]:
    normalized_status = F.lower(F.trim(F.col("status")))

    return [
        (_is_blank("payment_id"), "PAYMENT_ID_EMPTY"),
        (F.col("duplicate_count") > 1, "PAYMENT_ID_DUPLICATE_IN_LOAD"),
        (_is_blank("order_id"), "ORDER_ID_EMPTY"),
        (F.col("order_exists").isNull(), "ORDER_NOT_FOUND"),
        (F.col("payment_date").isNull(), "PAYMENT_DATE_INVALID"),
        (
            F.col("amount_kopecks").isNull() | (F.col("amount_kopecks") <= 0),
            "PAYMENT_AMOUNT_INVALID",
        ),
        (
            _is_blank("status")
            | (~normalized_status.isin(VALID_PAYMENT_STATUSES)),
            "PAYMENT_STATUS_INVALID",
        ),
        (
            F.col("order_exists").isNotNull()
            & F.col("payment_date").isNotNull()
            & (F.col("payment_date") < F.col("related_order_created_at")),
            "PAYMENT_BEFORE_ORDER",
        ),
    ]


def split_valid_invalid(df):
    valid_df = df.filter(F.col("dq_reason") == "").drop("dq_reason")

    invalid_df = df.filter(
        F.col("dq_reason") != ""
    ).withColumn(
        "quarantine_timestamp",
        F.current_timestamp(),
    )

    return valid_df, invalid_df


def write_layer(
    df,
    root: Path,
    entity: str,
    load_date: str,
    row_count: int,
) -> None:
    """Пишет партицию пересчитываемого слоя (clean или quarantine).

    Перезаписывается только партиция за конкретный load_date.
    """

    target_path = root / entity / f"load_date={load_date}"

    (
        df.drop("load_date")
        .write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    logger.info(
        "[WRITE] %s: %s rows -> %s", entity, f"{row_count:,}", target_path
    )


def persist_write_and_count(
    valid_df,
    invalid_df,
    clean_root: Path,
    quarantine_root: Path,
    entity: str,
    load_date: str,
):
    """Кэширует результаты DQ-проверок и пишет оба слоя за один проход.

    Строки считаются один раз, clean/quarantine записываются без
    повторного пересчёта плана.
    """

    valid_df = valid_df.persist(StorageLevel.MEMORY_AND_DISK)
    invalid_df = invalid_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        write_layer(
            valid_df,
            clean_root,
            entity,
            load_date,
            valid_count,
        )

        write_layer(
            invalid_df,
            quarantine_root,
            entity,
            load_date,
            invalid_count,
        )

        return valid_count, invalid_count

    finally:
        valid_df.unpersist()
        invalid_df.unpersist()


DUPLICATE_LOOKBACK_DAYS = 365


def read_historical_keys(
    spark,
    clean_root: Path,
    entity: str,
    id_column: str,
    load_date: str,
    lookback_days: int = DUPLICATE_LOOKBACK_DAYS,
):
    """Возвращает ID из clean-слоя за последние `lookback_days` дней.

    Нужно, чтобы находить дубликаты относительно прошлых загрузок и
    безопасно перезапускать transform для той же даты.

    Окно, а не вся история: прежняя версия отбирала партиции условием
    `load_date != текущая`, под которое Spark не умеет отсекать
    партиции, — и читала весь clean-слой целиком на каждой ночной
    загрузке. На пятидесяти днях это незаметно, на трёх годах каждая
    ночь становится дороже предыдущей.

    Цена компромисса названа прямо: повторно выданный ID, чей оригинал
    старше окна, проверкой не поймается. Год выбран как заведомо
    достаточный срок для переиспользования идентификатора в учётной
    системе; в самом хранилище такие строки всё равно схлопнутся по
    ключу при чтении через FINAL.
    """

    entity_root = clean_root / entity

    if not entity_root.exists():
        return None

    current_date = F.lit(load_date).cast("date")
    earliest = F.date_sub(current_date, lookback_days)

    return (
        spark.read.parquet(str(entity_root))
        # Нижняя граница стоит первой и намеренно: именно она даёт
        # отсечение партиций, ради которого всё и затевалось.
        .filter(F.col("load_date") >= earliest)
        .filter(F.col("load_date") != current_date)
        .select(id_column)
        .where(F.col(id_column).isNotNull())
        .distinct()
        .withColumn("exists_in_history", F.lit(True))
    )


def read_raw(spark, raw_root: Path, entity: str, load_date: str):
    path = raw_root / entity / f"load_date={load_date}"

    if not path.exists():
        raise FileNotFoundError(f"Raw-партиция не найдена: {path}")

    return (
        spark.read.parquet(str(path))
        .withColumn("load_date", F.lit(load_date).cast("date"))
    )


def transform_customers(spark, raw_root, clean_root, quarantine_root, load_date):
    logger.info("--- Processing customers ---")

    customers = read_raw(spark, raw_root, "customers", load_date)

    customers = customers.withColumn(
        "registration_date",
        F.to_timestamp("registration_date"),
    )

    duplicate_window = Window.partitionBy("customer_id")

    customers = customers.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    historical_customer_keys = read_historical_keys(
        spark=spark,
        clean_root=clean_root,
        entity="customers",
        id_column="customer_id",
        load_date=load_date,
    )

    if historical_customer_keys is not None:
        customers = customers.join(
            historical_customer_keys,
            on="customer_id",
            how="left",
        )
    else:
        customers = customers.withColumn(
            "exists_in_history",
            F.lit(None).cast("boolean"),
        )

    customers = add_dq_reason(customers, customer_dq_checks()).drop(
        "duplicate_count",
        "exists_in_history",
    )

    valid_df, invalid_df = split_valid_invalid(customers)

    return persist_write_and_count(
        valid_df=valid_df,
        invalid_df=invalid_df,
        clean_root=clean_root,
        quarantine_root=quarantine_root,
        entity="customers",
        load_date=load_date,
    )


def transform_orders(spark, raw_root, clean_root, quarantine_root, load_date):
    logger.info("--- Processing orders ---")

    orders = read_raw(spark, raw_root, "orders", load_date)

    orders = (
        orders
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("amount_kopecks", F.col("amount_kopecks").cast("long"))
        .withColumn("paid_at", F.to_timestamp("paid_at"))
        .withColumn("shipped_at", F.to_timestamp("shipped_at"))
        .withColumn("ready_for_pickup_at", F.to_timestamp("ready_for_pickup_at"))
        .withColumn("delivered_at", F.to_timestamp("delivered_at"))
        .withColumn("cancelled_at", F.to_timestamp("cancelled_at"))
        .withColumn("returned_at", F.to_timestamp("returned_at"))
        .withColumn("refunded_at", F.to_timestamp("refunded_at"))
    )

    clean_customers = spark.read.parquet(str(clean_root / "customers"))

    customer_keys = (
        clean_customers
        .select("customer_id")
        .distinct()
        .withColumn("customer_exists", F.lit(True))
    )

    orders = orders.join(
        customer_keys,
        on="customer_id",
        how="left",
    )

    # Дубликат order_id проверяется только В ПРЕДЕЛАХ ОДНОГО батча:
    # заказ легитимно переиздаётся с тем же order_id при каждой смене
    # статуса (upsert-модель, версии разрешает ReplacingMergeTree по
    # load_date). А вот две строки с одним order_id внутри одного
    # снапшота — DQ-проблема источника.
    duplicate_window = Window.partitionBy("order_id")

    orders = orders.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    orders = add_dq_reason(orders, order_dq_checks()).drop(
        "duplicate_count",
        "customer_exists",
    )

    valid_df, invalid_df = split_valid_invalid(orders)

    return persist_write_and_count(
        valid_df=valid_df,
        invalid_df=invalid_df,
        clean_root=clean_root,
        quarantine_root=quarantine_root,
        entity="orders",
        load_date=load_date,
    )


def transform_payments(spark, raw_root, clean_root, quarantine_root, load_date):
    logger.info("--- Processing payments ---")

    payments = read_raw(spark, raw_root, "payments", load_date)

    payments = (
        payments
        .withColumn("payment_date", F.to_timestamp("payment_date"))
        .withColumn("amount_kopecks", F.col("amount_kopecks").cast("long"))
    )

    clean_orders = spark.read.parquet(str(clean_root / "orders"))

    order_keys = (
        clean_orders
        .select(
            "order_id",
            F.col("created_at").alias("related_order_created_at"),
        )
        .dropDuplicates(["order_id"])
        .withColumn("order_exists", F.lit(True))
    )

    payments = payments.join(
        order_keys,
        on="order_id",
        how="left",
    )

    # Как и у orders, дубликат проверяется только внутри батча: платёж,
    # ставший refunded, переиздаётся с тем же payment_id (pay_{order_id},
    # детерминированный от order_id).
    duplicate_window = Window.partitionBy("payment_id")

    payments = payments.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    payments = add_dq_reason(payments, payment_dq_checks()).drop(
        "duplicate_count",
        "order_exists",
        "related_order_created_at",
    )

    valid_df, invalid_df = split_valid_invalid(payments)

    return persist_write_and_count(
        valid_df=valid_df,
        invalid_df=invalid_df,
        clean_root=clean_root,
        quarantine_root=quarantine_root,
        entity="payments",
        load_date=load_date,
    )


def write_dq_metrics(spark, metrics, dq_root: Path, load_date: str) -> None:
    metrics_df = spark.createDataFrame(metrics)

    target_path = dq_root / f"load_date={load_date}"

    (
        metrics_df.drop("load_date")
        .write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    logger.info("[DQ] Metrics saved -> %s", target_path)


def main() -> None:
    args = parse_args()

    data_dir = Path(args.data_dir)

    raw_root = data_dir / "raw"
    clean_root = data_dir / "clean"
    quarantine_root = data_dir / "quarantine"
    dq_root = data_dir / "dq_metrics"

    spark = (
        SparkSession.builder
        .appName(f"daily-sales-transform-{args.load_date}")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        customers_valid, customers_invalid = transform_customers(
            spark,
            raw_root,
            clean_root,
            quarantine_root,
            args.load_date,
        )

        orders_valid, orders_invalid = transform_orders(
            spark,
            raw_root,
            clean_root,
            quarantine_root,
            args.load_date,
        )

        payments_valid, payments_invalid = transform_payments(
            spark,
            raw_root,
            clean_root,
            quarantine_root,
            args.load_date,
        )

        metrics = [
            {
                "load_date": args.load_date,
                "entity": "customers",
                "valid_rows": customers_valid,
                "invalid_rows": customers_invalid,
                "total_rows": customers_valid + customers_invalid,
            },
            {
                "load_date": args.load_date,
                "entity": "orders",
                "valid_rows": orders_valid,
                "invalid_rows": orders_invalid,
                "total_rows": orders_valid + orders_invalid,
            },
            {
                "load_date": args.load_date,
                "entity": "payments",
                "valid_rows": payments_valid,
                "invalid_rows": payments_invalid,
                "total_rows": payments_valid + payments_invalid,
            },
        ]

        write_dq_metrics(
            spark=spark,
            metrics=metrics,
            dq_root=dq_root,
            load_date=args.load_date,
        )

        logger.info("Spark transformation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
