import argparse
import base64
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Все таблицы, партиционированные по load_date (см. clickhouse_schema.sql) —
# именно этот список чистится через DROP PARTITION при --force-reload.
PARTITIONED_TABLES = [
    "customers",
    "orders",
    "payments",
    "quarantine_customers",
    "quarantine_orders",
    "quarantine_payments",
    "dq_metrics",
]


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
        default="marketplace_analytics",
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
        "--force-reload",
        action="store_true",
        help=(
            "Перезагрузить load_date, даже если он уже отмечен как "
            "загруженный в _load_commits: перед загрузкой выполняется "
            "DROP PARTITION по всем партиционированным таблицам за эту "
            "дату. Без флага повторный запуск для уже загруженного "
            "load_date — безопасный no-op (см. is_load_date_committed)."
        ),
    )

    parser.add_argument(
        "--spark-clickhouse-connector",
        default=(
            "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.10.0,"
            "com.clickhouse:clickhouse-jdbc:0.10.0,"
            "org.apache.httpcomponents.client5:httpclient5:5.3.1"
        ),
        help=(
            "Maven-координаты JAR'ов для Spark (через запятую, идёт в "
            "spark.jars.packages — этот механизм поддерживает только "
            "3-частный формат groupId:artifactId:version, БЕЗ classifier "
            "вроде ':all'). httpclient5 добавлен явно — без него "
            "clickhouse-jdbc молча откатывается на встроенный Java "
            "HttpURLConnection, который может не пережить чуть нестандартный "
            "HTTP-ответ ClickHouse (споткнулись именно на этом на практике). "
            "Версии connector/jdbc — 0.10.0, сверено с maven-metadata.xml "
            "на Maven Central на момент отладки (мои изначальные версии по "
            "памяти оказались устаревшими и давали рассинхрон по LZ4-сжатию "
            "с ClickHouse 26.7). Без connector/jdbc SparkSession.builder "
            "упадёт с ClassNotFoundException на "
            "com.clickhouse.spark.ClickHouseCatalog. Если и эта версия "
            "успела устареть на Maven Central, переопредели этим флагом, "
            "без правки кода."
        ),
    )

    args = parser.parse_args()

    try:
        date.fromisoformat(args.load_date)
    except ValueError:
        parser.error("--load-date должен быть в формате YYYY-MM-DD")

    return args


def ch_execute(args, sql: str, use_database: bool = True) -> str:
    """
    Выполняет произвольный SQL через HTTP-интерфейс ClickHouse (порт из
    --clickhouse-port), в обход Spark — нужен для маркера идемпотентности
    и DROP PARTITION до того, как вообще поднимается SparkSession (чтобы
    no-op на уже загруженный load_date не тратил время на старт Spark и
    резолв JAR'ов).

    use_database=False — для CREATE DATABASE IF NOT EXISTS на чистом
    кластере: если передать ?database=marketplace_analytics до того, как
    эта база вообще создана, ClickHouse откажет с "Database
    marketplace_analytics doesn't exist" ещё до выполнения самого запроса.
    """
    url = f"http://{args.clickhouse_host}:{args.clickhouse_port}/"
    if use_database:
        url += f"?database={args.clickhouse_database}"

    credentials = base64.b64encode(
        f"{args.clickhouse_user}:{args.clickhouse_password}".encode("utf-8")
    ).decode("ascii")

    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        headers={"Authorization": f"Basic {credentials}"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request) as response:
            return response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse-запрос упал: {sql!r}\n{body}") from exc


def ensure_schema(args) -> None:
    schema_path = Path(__file__).parent / "clickhouse_schema.sql"
    statements = schema_path.read_text(encoding="utf-8").split(";")

    for statement in statements:
        statement = statement.strip()

        if not statement:
            continue

        # startswith тут не годится: ведущий SQL-комментарий (-- ...)
        # склеивается с CREATE DATABASE в один statement при split(";"),
        # поэтому ищем подстроку по всему тексту, а не только в начале.
        ch_execute(
            args,
            statement,
            use_database="CREATE DATABASE" not in statement.upper(),
        )


def is_load_date_committed(args) -> bool:
    result = ch_execute(
        args,
        "SELECT count() FROM _load_commits WHERE load_date = "
        f"'{args.load_date}'",
    )
    return int(result.strip()) > 0


def drop_existing_partitions(args) -> None:
    for table in PARTITIONED_TABLES:
        # IF EXISTS — партиции может не быть вовсе (например, за этот
        # день quarantine_customers пуст), это не ошибка.
        ch_execute(
            args,
            f"ALTER TABLE {table} DROP PARTITION IF EXISTS "
            f"'{args.load_date}'",
        )

    print(f"[RELOAD] Партиции load_date={args.load_date} сброшены "
          f"по {len(PARTITIONED_TABLES)} таблицам.")


def record_commit(args, entity_counts: dict[str, int]) -> None:
    """
    Пишется ПОСЛЕДНИМ шагом, только после того, как все таблицы за
    load_date успешно загружены — тот же принцип, что commit.json в
    generate_data.py (publish_batch): если упасть раньше этой записи,
    is_load_date_committed() на следующем запуске честно скажет "не
    загружено", и load_to_clickhouse.py просто догрузит день заново, а
    не решит, что всё уже есть.
    """
    values = ", ".join(
        f"('{args.load_date}', '{entity}', {count})"
        for entity, count in entity_counts.items()
    )

    ch_execute(
        args,
        "INSERT INTO _load_commits (load_date, entity, rows) VALUES "
        f"{values}",
    )

    print(f"[COMMIT] load_date={args.load_date} отмечен загруженным "
          f"({sum(entity_counts.values()):,} строк суммарно).")


def read_parquet_partition(spark, root: Path, entity: str, load_date: str):
    path = root / entity / f"load_date={load_date}"

    if not path.exists():
        raise FileNotFoundError(f"Партиция не найдена: {path}")

    return (
        spark.read.parquet(str(path))
        .withColumn("load_date", F.lit(load_date).cast("date"))
    )


def write_clickhouse(df, table: str, args) -> int:
    df = df.persist()
    row_count = df.count()

    (
        df.writeTo(
            f"clickhouse.{args.clickhouse_database}.{table}"
        )
        .append()
    )

    df.unpersist()

    return row_count


def main():
    args = parse_args()

    ensure_schema(args)

    if is_load_date_committed(args):
        if not args.force_reload:
            print(
                f"[SKIP] load_date={args.load_date} уже загружен в "
                "ClickHouse (см. _load_commits) — повторная загрузка "
                "не требуется. Передай --force-reload, чтобы принудительно "
                "перезаписать этот день."
            )
            return

        drop_existing_partitions(args)

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

        customers = read_parquet_partition(
            spark, clean_root, "customers", args.load_date
        )

        orders = read_parquet_partition(
            spark, clean_root, "orders", args.load_date
        )

        payments = read_parquet_partition(
            spark, clean_root, "payments", args.load_date
        )

        entity_counts = {
            "customers": write_clickhouse(customers, "customers", args),
            "orders": write_clickhouse(orders, "orders", args),
            "payments": write_clickhouse(payments, "payments", args),
        }

        quarantine_customers = read_parquet_partition(
            spark, quarantine_root, "customers", args.load_date
        )

        quarantine_orders = read_parquet_partition(
            spark, quarantine_root, "orders", args.load_date
        )

        quarantine_payments = read_parquet_partition(
            spark, quarantine_root, "payments", args.load_date
        )

        entity_counts["quarantine_customers"] = write_clickhouse(
            quarantine_customers,
            "quarantine_customers",
            args,
        )
        entity_counts["quarantine_orders"] = write_clickhouse(
            quarantine_orders,
            "quarantine_orders",
            args,
        )
        entity_counts["quarantine_payments"] = write_clickhouse(
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

        entity_counts["dq_metrics"] = write_clickhouse(
            dq_metrics, "dq_metrics", args
        )

        print("ClickHouse load completed successfully.")

    finally:
        spark.stop()

    # Пишется ПОСЛЕ spark.stop(): маркер — это лёгкий HTTP-запрос сам по
    # себе, не часть Spark-транзакции, и ему незачем ждать остановки
    # сессии. Важен порядок относительно записи данных выше — если
    # какая-либо из entity_counts[...] выше бросит исключение, до этой
    # строки выполнение не дойдёт, и is_load_date_committed() на
    # следующем запуске честно вернёт False.
    record_commit(args, entity_counts)


if __name__ == "__main__":
    main()