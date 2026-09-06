"""Схема и семантический слой ClickHouse.

Отдельный модуль, а не часть load_to_clickhouse.py, потому что тому
нужен pyspark: применить витрины — это несколько HTTP-запросов, и
тянуть ради них Spark-окружение неправильно. Здесь только стандартная
библиотека, так что модуль запускается где угодно:

    python clickhouse_ddl.py            # схема + витрины
    python clickhouse_ddl.py --marts    # только витрины

Подключение — из clickhouse_config.json, пароль — из
CLICKHOUSE_PASSWORD. Загрузчик импортирует те же функции и передаёт
свои разобранные аргументы.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

from growth import (
    customer_maturity_days,
    load_growth_config,
    maturity_days,
    payment_settlement_days,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent


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
    script = script.replace(
        "{{PAYMENT_SETTLEMENT_DAYS}}",
        str(payment_settlement_days(growth_config)),
    )

    # Незакрытый плейсхолдер уехал бы в ClickHouse как синтаксическая
    # ошибка посреди CREATE VIEW — понятнее упасть здесь и по делу.
    if "{{" in script:
        leftover = script[script.index("{{"):][:40]
        raise RuntimeError(f"В clickhouse_marts.sql остался плейсхолдер: {leftover}")

    count = _execute_sql_script(args, script)

    logger.info("[MARTS] %d objects in marketplace_marts are up to date.", count)


def _password_from_client_config() -> str:
    """Пароль из ~/.clickhouse-client/config.xml, если он там есть.

    Тот же файл читает сам clickhouse-client, поэтому там, где ручные
    запросы уже работают, модуль заводится без переменных окружения — и
    пароль не приходится держать в окружении процесса.
    """
    path = Path.home() / ".clickhouse-client" / "config.xml"
    if not path.exists():
        return ""

    match = re.search(
        r"<password>(.*?)</password>", path.read_text(encoding="utf-8"), re.S
    )
    return match.group(1).strip() if match else ""


def connection_from_config() -> SimpleNamespace:
    """Параметры подключения из clickhouse_config.json и окружения.

    Той же формы, что разобранные аргументы загрузчика, — функции выше
    принимают и то, и другое, и дублировать их не приходится.
    """
    config = json.loads(
        (REPO_ROOT / "clickhouse_config.json").read_text(encoding="utf-8")
    )
    password = os.environ.get("CLICKHOUSE_PASSWORD") or _password_from_client_config()
    if not password:
        raise SystemExit(
            "Пароль не найден: задай CLICKHOUSE_PASSWORD либо положи его в "
            "~/.clickhouse-client/config.xml."
        )

    return SimpleNamespace(
        clickhouse_host=config.get("host", "localhost"),
        clickhouse_port=config.get("port", 8123),
        clickhouse_database=config.get("database", "marketplace_analytics"),
        clickhouse_user=config.get("user", "default"),
        clickhouse_password=password,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--marts",
        action="store_true",
        help="только витрины, без схемы сырого слоя",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    connection = connection_from_config()

    if not args.marts:
        ensure_schema(connection)
        logger.info("[SCHEMA] marketplace_analytics is up to date.")

    ensure_marts(connection)
    return 0


if __name__ == "__main__":
    sys.exit(main())
