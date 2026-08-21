import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--load-date",
        required=True,
        help="Дата загрузки в формате YYYY-MM-DD",
    )

    parser.add_argument(
        "--data-dir",
        default="data",
        help="Корневая директория данных",
    )

    parser.add_argument(
        "--clickhouse-host",
        default="localhost",
    )

    parser.add_argument(
        "--clickhouse-port",
        default="8123",
    )

    parser.add_argument(
        "--clickhouse-database",
        default="analytics",
    )

    parser.add_argument(
        "--clickhouse-user",
        default="analytics_user",
    )

    parser.add_argument(
        "--clickhouse-password",
        required=True,
    )

    parser.add_argument(
        "--spark-clickhouse-connector",
        default=(
            "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0,"
            "com.clickhouse:clickhouse-jdbc:0.6.5:all"
        ),
        help=(
            "Maven-координаты JAR'а коннектора ClickHouse для Spark "
            "(через запятую, идёт в spark.jars.packages). Без него "
            "SparkSession.builder упадёт с ClassNotFoundException на "
            "com.clickhouse.spark.ClickHouseCatalog. Дефолт подобран под "
            "pyspark 3.5 / Scala 2.12 — если версия успела устареть на "
            "Maven Central, переопредели этим флагом, без правки кода."
        ),
    )

    args = parser.parse_args()

    try:
        date.fromisoformat(args.load_date)
    except ValueError:
        parser.error("--load-date должен быть в формате YYYY-MM-DD")

    return args


def read_parquet_partition(spark, root: Path, entity: str, load_date: str):
    path = root / entity / f"load_date={load_date}"

    if not path.exists():
        raise FileNotFoundError(f"Партиция не найдена: {path}")

    return (
        spark.read.parquet(str(path))
        .withColumn("load_date", F.lit(load_date).cast("date"))
    )


def write_clickhouse(df, table: str, args):
    (
        df.writeTo(
            f"clickhouse.{args.clickhouse_database}.{table}"
        )
        .append()
    )


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(f"clickhouse-load-{args.load_date}")
        # Требует сетевого доступа к Maven Central (или настроенному
        # зеркалу) в момент первого запуска — Spark/Ivy резолвит и
        # кэширует JAR локально. Если удалённая машина без интернета,
        # нужно заранее скачать JAR'ы и подключить через spark.jars с
        # локальным путём вместо spark.jars.packages.
        .config("spark.jars.packages", args.spark_clickhouse_connector)
        .config(
            "spark.sql.catalog.clickhouse",
            "com.clickhouse.spark.ClickHouseCatalog",
        )
        .config(
            "spark.sql.catalog.clickhouse.host",
            args.clickhouse_host,
        )
        .config(
            "spark.sql.catalog.clickhouse.protocol",
            "http",
        )
        .config(
            "spark.sql.catalog.clickhouse.http_port",
            args.clickhouse_port,
        )
        .config(
            "spark.sql.catalog.clickhouse.user",
            args.clickhouse_user,
        )
        .config(
            "spark.sql.catalog.clickhouse.password",
            args.clickhouse_password,
        )

        # ВАЖНО: глобальные spark.clickhouse.* настройки (не catalog-prefixed).
        .config(
            "spark.clickhouse.read.compression.codec",
            "none",
        )
        .config(
            "spark.clickhouse.write.compression.codec",
            "none",
        )
        .getOrCreate()
    )

    try:
        data_dir = Path(args.data_dir)

        clean_root = data_dir / "clean"
        quarantine_root = data_dir / "quarantine"
        dq_root = data_dir / "dq_metrics"

        clients = read_parquet_partition(
            spark, clean_root, "clients", args.load_date
        )

        orders = read_parquet_partition(
            spark, clean_root, "orders", args.load_date
        )

        payments = read_parquet_partition(
            spark, clean_root, "payments", args.load_date
        )

        write_clickhouse(clients, "clients", args)
        write_clickhouse(orders, "orders", args)
        write_clickhouse(payments, "payments", args)

        quarantine_clients = read_parquet_partition(
            spark, quarantine_root, "clients", args.load_date
        )

        quarantine_orders = read_parquet_partition(
            spark, quarantine_root, "orders", args.load_date
        )

        quarantine_payments = read_parquet_partition(
            spark, quarantine_root, "payments", args.load_date
        )

        write_clickhouse(
            quarantine_clients,
            "quarantine_clients",
            args,
        )
        write_clickhouse(
            quarantine_orders,
            "quarantine_orders",
            args,
        )
        write_clickhouse(
            quarantine_payments,
            "quarantine_payments",
            args,
        )

        dq_metrics = (
            spark.read.parquet(
                str(dq_root / f"load_date={args.load_date}")
            )
            .withColumn(
                "load_date",
                F.lit(args.load_date).cast("date"),
            )
        )

        write_clickhouse(dq_metrics, "dq_metrics", args)

        print("ClickHouse load completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()