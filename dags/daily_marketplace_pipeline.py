"""
Ежедневный пайплайн маркетплейса: generate_data.py -> ingest_csv_to_raw.py
-> transform.py -> load_to_clickhouse.py для одного logical_date.

Объединяет то, что вручную делают backfill_history.sh (генерация
source-слоя с реалистичной кривой роста) и run_downstream_pipeline.sh
(raw -> clean/quarantine -> ClickHouse), но на регулярном расписании,
один день за раз.

Источники правды — общие с обоими shell-скриптами и load_to_clickhouse.py,
не дублируются здесь:
- growth_config.json — бизнес-параметры кривой роста, недельная
  сезонность, дедлайны/вероятности (см. комментарий в backfill_history.sh).
- clickhouse_config.json — host/port/database/user (без пароля).

Пароль ClickHouse НЕ хранится ни в одном файле репозитория — берётся из
Airflow Connection `clickhouse_default` (поле password), которую нужно
завести в Airflow (Admin -> Connections) перед первым запуском.

Порядок дней важен и здесь: generate_data.py накапливает клиентов между
запусками (load_existing_customers), а transform.py проверяет дубли
customer_id относительно уже обработанной истории (read_historical_keys).
Поэтому max_active_runs=1 — DAG-раны идут строго последовательно, day N+1
не стартует, пока day N не завершился (успешно или с ошибкой).
"""
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = str(REPO_ROOT / "data")

GROWTH_CONFIG = json.loads(
    (REPO_ROOT / "growth_config.json").read_text(encoding="utf-8")
)
CLICKHOUSE_CONFIG = json.loads(
    (REPO_ROOT / "clickhouse_config.json").read_text(encoding="utf-8")
)

# tzinfo=utc задан явно: naive datetime Airflow трактовал бы в
# default_timezone из airflow.cfg конкретного инстанса (обычно utc, но
# не гарантированно), а расписание ниже (@daily = "0 0 * * *") должно
# запускаться в 00:00 UTC независимо от конфига инсталляции.
START_DATE = datetime.fromisoformat(GROWTH_CONFIG["start_date"]).replace(
    tzinfo=timezone.utc
)
RAMP_DAYS = GROWTH_CONFIG["ramp_days"]


def _volume_for_day(day_index: int) -> tuple[int, int]:
    """
    Тот же расчёт customers_count/orders_count, что в backfill_history.sh:
    фаза 1 (day_index <= RAMP_DAYS) — линейный рост от мягкого запуска,
    фаза 2 — объём фиксируется на уровне конца рампы + медленный
    органический рост. day_index — число дней с start_date (0-indexed),
    как переменная `i` в backfill_history.sh, НЕ 1-indexed day_number.
    Коэффициенты — из growth_config.json, общие с backfill_history.sh.
    """
    base_customers = GROWTH_CONFIG["base_customers"]
    customers_ramp_per_day = GROWTH_CONFIG["customers_ramp_per_day"]
    customers_plateau_per_day = GROWTH_CONFIG["customers_plateau_per_day"]
    base_orders = GROWTH_CONFIG["base_orders"]
    orders_ramp_per_day = GROWTH_CONFIG["orders_ramp_per_day"]
    orders_plateau_per_day = GROWTH_CONFIG["orders_plateau_per_day"]

    if day_index <= RAMP_DAYS:
        customers_count = base_customers + day_index * customers_ramp_per_day
        orders_count = base_orders + day_index * orders_ramp_per_day
    else:
        plateau_day = day_index - RAMP_DAYS
        customers_count = (
            base_customers
            + RAMP_DAYS * customers_ramp_per_day
            + plateau_day * customers_plateau_per_day
        )
        orders_count = (
            base_orders
            + RAMP_DAYS * orders_ramp_per_day
            + plateau_day * orders_plateau_per_day
        )

    return customers_count, orders_count


def _run(args: list[str]) -> None:
    subprocess.run(args, cwd=REPO_ROOT, check=True)


@dag(
    dag_id="daily_marketplace_pipeline",
    schedule="@daily",
    start_date=START_DATE,
    catchup=True,
    max_active_runs=1,
    tags=["marketplace"],
)
def daily_marketplace_pipeline():

    @task
    def generate(ds: str) -> None:
        load_date = datetime.fromisoformat(ds)
        day_index = (load_date - START_DATE).days

        customers_count, orders_count = _volume_for_day(day_index)

        # Недельная сезонность — weekday_multipliers в growth_config.json,
        # ключ ISO-номер дня недели строкой (1=понедельник..7=воскресенье),
        # как и в _weekday_multiplier() в backfill_history.sh.
        weekday_multiplier = GROWTH_CONFIG["weekday_multipliers"][
            str(load_date.isoweekday())
        ]
        customers_count = round(customers_count * weekday_multiplier)
        orders_count = round(orders_count * weekday_multiplier)

        payments_count = (
            orders_count * GROWTH_CONFIG["payments_margin_pct"] // 100
        )

        _run([
            "python", "generate_data.py",
            "--load-date", ds,
            "--data-dir", DATA_DIR,
            "--customers-count", str(customers_count),
            "--orders-count", str(orders_count),
            "--payments-count", str(payments_count),
            "--error-rate", str(GROWTH_CONFIG["error_rate"]),
            "--launch-date", GROWTH_CONFIG["start_date"],
            "--payment-deadline-hours", str(GROWTH_CONFIG["payment_deadline_hours"]),
            "--shipping-deadline-hours", str(GROWTH_CONFIG["shipping_deadline_hours"]),
            "--delivery-deadline-days", str(GROWTH_CONFIG["delivery_deadline_days"]),
            "--pickup-deadline-days", str(GROWTH_CONFIG["pickup_deadline_days"]),
            "--return-window-days", str(GROWTH_CONFIG["return_window_days"]),
            "--refund-processing-hours", str(GROWTH_CONFIG["refund_processing_hours"]),
            "--cancel-before-payment-rate", str(GROWTH_CONFIG["cancel_before_payment_rate"]),
            "--cancel-after-payment-rate", str(GROWTH_CONFIG["cancel_after_payment_rate"]),
            "--return-rate", str(GROWTH_CONFIG["return_rate"]),
        ])

    @task
    def ingest(ds: str) -> None:
        _run([
            "python", "ingest_csv_to_raw.py",
            "--load-date", ds,
            "--data-dir", DATA_DIR,
        ])

    @task
    def transform(ds: str) -> None:
        _run([
            "python", "transform.py",
            "--load-date", ds,
            "--data-dir", DATA_DIR,
        ])

    @task
    def load(ds: str) -> None:
        # Пароль читается из Airflow Connection, а не из growth_config.json/
        # clickhouse_config.json — не оседает ни в git, ни в рендере
        # шаблонов задачи (в отличие от BashOperator с командой-строкой).
        password = BaseHook.get_connection("clickhouse_default").password

        _run([
            "python", "load_to_clickhouse.py",
            "--load-date", ds,
            "--data-dir", DATA_DIR,
            "--clickhouse-host", CLICKHOUSE_CONFIG["host"],
            "--clickhouse-port", str(CLICKHOUSE_CONFIG["port"]),
            "--clickhouse-database", CLICKHOUSE_CONFIG["database"],
            "--clickhouse-user", CLICKHOUSE_CONFIG["user"],
            "--clickhouse-password", password,
        ])

    generate(ds="{{ ds }}") >> ingest(ds="{{ ds }}") >> transform(ds="{{ ds }}") >> load(ds="{{ ds }}")


daily_marketplace_pipeline()
