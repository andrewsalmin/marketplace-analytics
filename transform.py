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
    "delivered",
    "cancelled",
    "returned",
    "refunded",
]
VALID_PAYMENT_STATUSES = ["pending", "success", "failed", "refunded", "chargeback"]


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


def transform_clients(spark, raw_root, clean_root, quarantine_root, load_date):
    print("\n--- Processing clients ---")

    clients = read_raw(spark, raw_root, "clients", load_date)

    clients = clients.withColumn(
        "registration_date",
        F.to_date("registration_date"),
    )

    duplicate_window = Window.partitionBy("client_id")

    clients = clients.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    historical_client_keys = read_historical_keys(
        spark=spark,
        clean_root=clean_root,
        entity="clients",
        id_column="client_id",
        load_date=load_date,
    )

    if historical_client_keys is not None:
        clients = clients.join(
            historical_client_keys,
            on="client_id",
            how="left",
        )
    else:
        clients = clients.withColumn(
            "exists_in_history",
            F.lit(None).cast("boolean"),
        )

    clients = add_dq_reason(
        clients,
        [
            (
                F.col("client_id").isNull() |
                (F.trim(F.col("client_id")) == ""),
                "CLIENT_ID_EMPTY",
            ),
            (
                F.col("duplicate_count") > 1,
                "CLIENT_ID_DUPLICATE_IN_LOAD",
            ),
            (
                F.col("exists_in_history").isNotNull(),
                "CLIENT_ID_DUPLICATE_IN_HISTORY",
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

    valid_df, invalid_df = split_valid_invalid(clients)

    valid_count, invalid_count = persist_write_and_count(
        valid_df=valid_df,
        invalid_df=invalid_df,
        clean_root=clean_root,
        quarantine_root=quarantine_root,
        entity="clients",
        load_date=load_date,
    )

    return valid_count, invalid_count


def transform_orders(spark, raw_root, clean_root, quarantine_root, load_date):
    print("\n--- Processing orders ---")

    orders = read_raw(spark, raw_root, "orders", load_date)

    orders = (
        orders
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("amount_kopecks", F.col("amount_kopecks").cast("long"))
    )

    clean_clients = spark.read.parquet(str(clean_root / "clients"))

    client_keys = (
        clean_clients
        .select("client_id")
        .distinct()
        .withColumn("client_exists", F.lit(True))
    )

    orders = orders.join(
        client_keys,
        on="client_id",
        how="left",
    )

    duplicate_window = Window.partitionBy("order_id")

    orders = orders.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    historical_order_keys = read_historical_keys(
        spark=spark,
        clean_root=clean_root,
        entity="orders",
        id_column="order_id",
        load_date=load_date,
    )

    if historical_order_keys is not None:
        orders = orders.join(
            historical_order_keys,
            on="order_id",
            how="left",
        )
    else:
        orders = orders.withColumn(
            "exists_in_history",
            F.lit(None).cast("boolean"),
        )

    normalized_status = F.lower(F.trim(F.col("status")))

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
                F.col("exists_in_history").isNotNull(),
                "ORDER_ID_DUPLICATE_IN_HISTORY",
            ),
            (
                F.col("client_id").isNull() |
                (F.trim(F.col("client_id")) == ""),
                "CLIENT_ID_EMPTY",
            ),
            (
                F.col("client_exists").isNull(),
                "CLIENT_NOT_FOUND",
            ),
            (
                F.col("order_date").isNull(),
                "ORDER_DATE_INVALID",
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
        ],
    ).drop(
        "duplicate_count",
        "exists_in_history",
        "client_exists",
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
        .withColumn("payment_date", F.to_date("payment_date"))
        .withColumn("amount_kopecks", F.col("amount_kopecks").cast("long"))
    )

    clean_orders = spark.read.parquet(str(clean_root / "orders"))

    order_keys = (
        clean_orders
        .select(
            "order_id",
            F.col("order_date").alias("related_order_date"),
        )
        .dropDuplicates(["order_id"])
        .withColumn("order_exists", F.lit(True))
    )

    payments = payments.join(
        order_keys,
        on="order_id",
        how="left",
    )

    duplicate_window = Window.partitionBy("payment_id")

    payments = payments.withColumn(
        "duplicate_count",
        F.count("*").over(duplicate_window),
    )

    historical_payment_keys = read_historical_keys(
        spark=spark,
        clean_root=clean_root,
        entity="payments",
        id_column="payment_id",
        load_date=load_date,
    )

    if historical_payment_keys is not None:
        payments = payments.join(
            historical_payment_keys,
            on="payment_id",
            how="left",
        )
    else:
        payments = payments.withColumn(
            "exists_in_history",
            F.lit(None).cast("boolean"),
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
                F.col("exists_in_history").isNotNull(),
                "PAYMENT_ID_DUPLICATE_IN_HISTORY",
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
                (F.col("payment_date") < F.col("related_order_date")),
                "PAYMENT_BEFORE_ORDER",
            ),
        ],
    ).drop(
        "duplicate_count",
        "exists_in_history",
        "order_exists",
        "related_order_date",
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
        clients_valid, clients_invalid = transform_clients(
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
                "entity": "clients",
                "valid_rows": clients_valid,
                "invalid_rows": clients_invalid,
                "total_rows": clients_valid + clients_invalid,
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