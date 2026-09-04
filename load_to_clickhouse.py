import argparse
import base64
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from growth import customer_maturity_days, load_growth_config, maturity_days

logger = logging.getLogger(__name__)

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


def _load_json_config(name: str) -> dict:
    return json.loads(
        (Path(__file__).parent / name).read_text(encoding="utf-8")
    )


def load_clickhouse_defaults() -> dict:
    """Читает host/port/database/user из clickhouse_config.json.

    Этот файл — единый источник правды для ручного запуска,
    run_downstream_pipeline.sh и Airflow DAG'а, чтобы параметры
    подключения не расходились между ними. Пароль в нём не хранится:
    он приходит из переменной окружения CLICKHOUSE_PASSWORD (или, для
    ручных запусков, из --clickhouse-password).
    """
    return _load_json_config("clickhouse_config.json")


def parse_args() -> argparse.Namespace:
    defaults = load_clickhouse_defaults()

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
        default=defaults["host"],
    )

    parser.add_argument(
        "--clickhouse-port",
        default=str(defaults["port"]),
    )

    parser.add_argument(
        "--clickhouse-database",
        default=defaults["database"],
    )

    parser.add_argument(
        "--clickhouse-user",
        default=defaults["user"],
    )

    # Пароль по умолчанию берётся из окружения, а не из аргумента:
    # значение CLI-флага видно в списке процессов любому пользователю
    # машины. run_downstream_pipeline.sh и Airflow DAG передают его
    # именно переменной; флаг оставлен для ручных запусков.
    parser.add_argument(
        "--clickhouse-password",
        default=os.environ.get("CLICKHOUSE_PASSWORD"),
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
            "Maven-координаты JAR'ов для Spark, через запятую. Уходят в "
            "spark.jars.packages, который поддерживает только 3-частный "
            "формат groupId:artifactId:version, без classifier вроде "
            "':all'. Версии сверены с Maven Central; если они устареют, "
            "новые задаются этим флагом, без правки кода."
        ),
    )

    args = parser.parse_args()

    try:
        date.fromisoformat(args.load_date)
    except ValueError:
        parser.error("--load-date должен быть в формате YYYY-MM-DD")

    if not args.clickhouse_password:
        parser.error(
            "нужен пароль ClickHouse: переменная окружения "
            "CLICKHOUSE_PASSWORD или --clickhouse-password"
        )

    return args


def ch_execute(args, sql: str, use_database: bool = True) -> str:
    """Выполняет SQL через HTTP-интерфейс ClickHouse, в обход Spark.

    Нужен для маркера идемпотентности и DROP PARTITION до старта
    SparkSession: no-op на уже загруженный load_date не должен тратить
    время на подъём Spark и резолв JAR'ов.

    use_database=False — для CREATE DATABASE IF NOT EXISTS на чистом
    кластере: с ?database=<имя> до создания самой базы ClickHouse
    откажет с "Database ... doesn't exist" ещё до выполнения запроса.
    """
    url = f"http://{args.clickhouse_host}:{args.clickhouse_port}/"
    if use_database:
        url += f"?database={args.clickhouse_database}"

    credentials = base64.b64encode(
        f"{args.clickhouse_user}:{args.clickhouse_password}".encode()
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


def _execute_sql_script(args, script: str) -> int:
    executed = 0

    for statement in script.split(";"):
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
        executed += 1

    return executed


def ensure_schema(args) -> None:
    _execute_sql_script(
        args,
        (Path(__file__).parent / "clickhouse_schema.sql").read_text(
            encoding="utf-8"
        ),
    )


def ensure_marts(args) -> None:
    """Пересоздаёт семантический слой (база marketplace_marts).

    Витрины — обычные VIEW, вычисляются на лету, поэтому их можно просто
    переналивать на каждом запуске: изменившееся бизнес-правило в
    growth_config.json доезжает до дашборда без отдельного шага.

    Горизонты зрелости подставляются сюда из growth_config.json, а не
    записаны в SQL: иначе у хранилища появилась бы собственная копия
    бизнес-правил, расходящаяся с генератором и Airflow DAG'ом.
    """
    growth_config = load_growth_config()

    script = (Path(__file__).parent / "clickhouse_marts.sql").read_text(
        encoding="utf-8"
    )
    script = script.replace(
        "{{MATURITY_DAYS}}",
        str(maturity_days(growth_config)),
    )
    script = script.replace(
        "{{CUSTOMER_MATURITY_DAYS}}",
        str(customer_maturity_days(growth_config)),
    )

    # Незакрытый плейсхолдер уехал бы в ClickHouse как синтаксическая
    # ошибка посреди CREATE VIEW — понятнее упасть здесь и по делу.
    if "{{" in script:
        leftover = script[script.index("{{"):][:40]
        raise RuntimeError(f"В clickhouse_marts.sql остался плейсхолдер: {leftover}")

    count = _execute_sql_script(args, script)

    logger.info("[MARTS] %d objects in marketplace_marts are up to date.", count)


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

    logger.info(
        "[RELOAD] Dropped load_date=%s partitions across %d tables.",
        args.load_date,
        len(PARTITIONED_TABLES),
    )


def record_commit(args, entity_counts: dict[str, int]) -> None:
    """Отмечает load_date как загруженный целиком.

    Вызывается последним шагом, после успешной записи всех таблиц за
    день — тот же принцип, что commit.json в publish_batch()
    generate_data.py. Падение до этой записи оставляет день неотмеченным,
    и следующий запуск догрузит его заново, а не сочтёт готовым.
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

    logger.info(
        "[COMMIT] load_date=%s marked as loaded (%s rows total).",
        args.load_date,
        f"{sum(entity_counts.values()):,}",
    )


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


def main() -> None:
    args = parse_args()

    ensure_schema(args)
    ensure_marts(args)

    if is_load_date_committed(args):
        if not args.force_reload:
            logger.info(
                "[SKIP] load_date=%s is already loaded into ClickHouse "
                "(see _load_commits); nothing to do. Pass --force-reload "
                "to overwrite this day.",
                args.load_date,
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

        logger.info("ClickHouse load completed successfully.")

    finally:
        spark.stop()

    # После spark.stop(): маркер — обычный HTTP-запрос, не часть
    # Spark-транзакции. Важен только порядок относительно записи данных
    # выше: исключение в любом write_clickhouse() до этой строки не
    # доходит, и день остаётся неотмеченным.
    record_commit(args, entity_counts)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
