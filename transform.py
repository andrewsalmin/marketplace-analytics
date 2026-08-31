import argparse
from datetime import date
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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


def parse_args():
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
    """
    checks: список пар вида:
    (условие ошибки Spark Column, текст причины)

    Создаёт поле dq_reason.
    Если строка валидна, dq_reason будет пустой строкой.
    """
    reason_columns = [
        F.when(condition, F.lit(reason))
        for condition, reason in checks
    ]

    return df.withColumn(
        "dq_reason",
        F.concat_ws(" | ", *reason_columns),
    )


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
):
    """
    Clean и quarantine — пересчитываемые слои.
    Перезаписывается только конкретная партиция load_date.
    """

    target_path = root / entity / f"load_date={load_date}"

    (
        df.drop("load_date")
        .write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    print(f"[WRITE] {entity}: {row_count:,} rows -> {target_path}")


def persist_write_and_count(
    valid_df,
    invalid_df,
    clean_root: Path,
    quarantine_root: Path,
    entity: str,
    load_date: str,
):
    """
    Кэширует результаты DQ-проверок, считает строки один раз
    и записывает clean/quarantine без повторного пересчёта.
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


def read_historical_keys(
    spark,
    clean_root: Path,
    entity: str,
    id_column: str,
    load_date: str,
):
    """
    Возвращает ID из clean-слоя всех загрузок, кроме текущей.

    Это позволяет:
    - находить дубликаты относительно прошлых загрузок;
    - безопасно повторно запускать transform для той же даты.
    """

    entity_root = clean_root / entity

    if not entity_root.exists():
        return None

    current_date = F.lit(load_date).cast("date")

    return (
        spark.read.parquet(str(entity_root))
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
    print("\n--- Processing customers ---")

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

    customers = add_dq_reason(
        customers,
        [
            (
                F.col("customer_id").isNull() |
                (F.trim(F.col("customer_id")) == ""),
                "CUSTOMER_ID_EMPTY",
            ),
            (
                F.col("duplicate_count") > 1,
                "CUSTOMER_ID_DUPLICATE_IN_LOAD",
            ),
            (
                F.col("exists_in_history").isNotNull(),
                "CUSTOMER_ID_DUPLICATE_IN_HISTORY",
            ),
            (
                F.col("registration_date").isNull(),
                "REGISTRATION_DATE_INVALID",
            ),
        ],
    ).drop(
        "duplicate_count",
        "exists_in_history",
    )

    valid_df, invalid_df = split_valid_invalid(customers)

    valid_count, invalid_count = persist_write_and_count(
        valid_df=valid_df,
        invalid_df=invalid_df,
        clean_root=clean_root,
        quarantine_root=quarantine_root,
        entity="customers",
        load_date=load_date,
    )

    return valid_count, invalid_count


def transform_orders(spark, raw_root, clean_root, quarantine_root, load_date):
    print("\n--- Processing orders ---")

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

    # ORDER_ID_DUPLICATE_IN_HISTORY больше не проверяется: заказ теперь
    # легитимно переиздаётся строкой с тем же order_id при каждом
    # изменении статуса (upsert-модель, ReplacingMergeTree в ClickHouse
    # сам разрешает версии по ingested_at) — повторный order_id в истории
    # больше не ошибка. Дубликат В ПРЕДЕЛАХ ОДНОГО батча (duplicate_count)
    # остаётся ошибкой — это по-прежнему один и тот же снапшот, две строки
    # с одним order_id в нём означают DQ-проблему источника.
    duplicate_window = Window.partitionBy("order_id")

    orders = orders.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    normalized_status = F.lower(F.trim(F.col("status")))

    cancellation_reason_inconsistent = (
        F.col("cancellation_reason").isNotNull()
        & (~F.col("status").isin(["cancelled", "refunded"]))
    ) | (
        (F.col("status") == "cancelled")
        & F.col("cancellation_reason").isNull()
    )

    refunded_before_cancelled = F.col("cancelled_at").isNotNull() & (
        F.col("refunded_at") < F.col("cancelled_at")
    )
    refunded_before_returned = F.col("returned_at").isNotNull() & (
        F.col("refunded_at") < F.col("returned_at")
    )
    refund_timeline_invalid = F.col("refunded_at").isNotNull() & (
        refunded_before_cancelled | refunded_before_returned
    )

    orders = add_dq_reason(
        orders,
        [
            (
                F.col("order_id").isNull() |
                (F.trim(F.col("order_id")) == ""),
                "ORDER_ID_EMPTY",
            ),
            (
                F.col("duplicate_count") > 1,
                "ORDER_ID_DUPLICATE_IN_LOAD",
            ),
            (
                F.col("customer_id").isNull() |
                (F.trim(F.col("customer_id")) == ""),
                "CUSTOMER_ID_EMPTY",
            ),
            (
                F.col("customer_exists").isNull(),
                "CUSTOMER_NOT_FOUND",
            ),
            (
                F.col("created_at").isNull(),
                "CREATED_AT_INVALID",
            ),
            (
                F.col("amount_kopecks").isNull() |
                (F.col("amount_kopecks") <= 0),
                "ORDER_AMOUNT_INVALID",
            ),
            (
                F.col("status").isNull() |
                (F.trim(F.col("status")) == "") |
                (~normalized_status.isin(VALID_ORDER_STATUSES)),
                "ORDER_STATUS_INVALID",
            ),
            (
                cancellation_reason_inconsistent,
                "CANCELLATION_REASON_INCONSISTENT",
            ),
            (
                refund_timeline_invalid,
                "REFUND_TIMELINE_INVALID",
            ),
        ],
    ).drop(
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
    print("\n--- Processing payments ---")

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

    # PAYMENT_ID_DUPLICATE_IN_HISTORY больше не проверяется: платёж,
    # ставший refunded, легитимно переиздаётся с тем же payment_id
    # (pay_{order_id}), сформированным детерминированно из order_id —
    # это тот же upsert-паттерн, что и у orders.
    duplicate_window = Window.partitionBy("payment_id")

    payments = payments.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    normalized_status = F.lower(F.trim(F.col("status")))

    payments = add_dq_reason(
        payments,
        [
            (
                F.col("payment_id").isNull() |
                (F.trim(F.col("payment_id")) == ""),
                "PAYMENT_ID_EMPTY",
            ),
            (
                F.col("duplicate_count") > 1,
                "PAYMENT_ID_DUPLICATE_IN_LOAD",
            ),
            (
                F.col("order_id").isNull() |
                (F.trim(F.col("order_id")) == ""),
                "ORDER_ID_EMPTY",
            ),
            (
                F.col("order_exists").isNull(),
                "ORDER_NOT_FOUND",
            ),
            (
                F.col("payment_date").isNull(),
                "PAYMENT_DATE_INVALID",
            ),
            (
                F.col("amount_kopecks").isNull() |
                (F.col("amount_kopecks") <= 0),
                "PAYMENT_AMOUNT_INVALID",
            ),
            (
                F.col("status").isNull() |
                (F.trim(F.col("status")) == "") |
                (~normalized_status.isin(VALID_PAYMENT_STATUSES)),
                "PAYMENT_STATUS_INVALID",
            ),
            (
                F.col("order_exists").isNotNull() &
                F.col("payment_date").isNotNull() &
                (F.col("payment_date") < F.col("related_order_created_at")),
                "PAYMENT_BEFORE_ORDER",
            ),
        ],
    ).drop(
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


def write_dq_metrics(spark, metrics, dq_root: Path, load_date: str):
    metrics_df = spark.createDataFrame(metrics)

    target_path = dq_root / f"load_date={load_date}"

    (
        metrics_df.drop("load_date")
        .write
        .mode("overwrite")
        .parquet(str(target_path))
    )

    print(f"\n[DQ] Metrics saved -> {target_path}")


def main():
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

        print("\nSpark transformation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()