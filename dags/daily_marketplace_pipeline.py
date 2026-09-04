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
Отсюда две настройки, а не одна: max_active_runs=1 не даёт дням идти
параллельно, а depends_on_past=True не даёт day N+1 стартовать после
УПАВШЕГО day N. Без второй настройки пропуск дня посреди истории
разошёлся бы тихо: генератор не увидел бы клиентов пропущенного дня, а
у части заказов не оказалось бы дня, в который они должны были
продвинуться по state machine.
"""
import json
import logging
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = str(REPO_ROOT / "data")

# Явный путь до venv проекта, а не просто "python": subprocess.run ниже
# резолвит "python" через PATH процесса Airflow-воркера, а тот запущен
# из отдельного airflow-venv (только requirements-airflow.txt, без
# pandas и прочего из requirements-spark.txt), поэтому голое "python"
# там резолвится не в то окружение и падает с ImportError.
_VENV_BIN = "Scripts" if os.name == "nt" else "bin"
PYTHON_BIN = str(REPO_ROOT / "venv" / _VENV_BIN / "python")

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

# Ключи growth_config.json, которые уходят в generate_data.py один в один:
# имя ключа = имя CLI-флага (подчёркивания -> дефисы). Список явный, а не
# «все ключи конфига», потому что параметры кривой роста (base_orders и
# т.п.) генератору не передаются — они сворачиваются в customers_count/
# orders_count функцией _volume_for_day().
BUSINESS_RULE_KEYS = [
    "error_rate",
    "payment_deadline_hours",
    "shipping_deadline_hours",
    "delivery_deadline_days",
    "pickup_deadline_days",
    "return_window_days",
    "refund_processing_hours",
    "cancel_before_payment_rate",
    "cancel_after_payment_rate",
    "return_rate",
    "never_order_rate",
]


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


def _run(args: list[str], env: dict[str, str] | None = None) -> None:
    subprocess.run(
        args,
        cwd=REPO_ROOT,
        check=True,
        env={**os.environ, **env} if env else None,
    )


def _alert_on_failure(context: dict) -> None:
    """Единая точка подключения алертинга.

    Сейчас пишет структурированную запись в лог планировщика: рабочего
    Slack/PagerDuty у этой установки нет, а email_on_failure без
    настроенного SMTP молча ничего не делает и создаёт ложное ощущение,
    что уведомления есть. Интеграция подключается здесь, не трогая сами
    задачи.
    """
    task_instance = context.get("task_instance")
    logger.error(
        "Задача %s упала на logical_date=%s (попытка %s): %s",
        getattr(task_instance, "task_id", "?"),
        context.get("ds"),
        getattr(task_instance, "try_number", "?"),
        context.get("exception"),
    )


# retries по умолчанию — 2: сеть до Maven Central, старт Spark и HTTP до
# ClickHouse отваливаются достаточно часто, чтобы одна повторная попытка
# окупалась. Задачи, для которых повтор НЕ безопасен, переопределяют это
# у себя (см. ingest).
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": _alert_on_failure,
}


@dag(
    dag_id="daily_marketplace_pipeline",
    schedule="@daily",
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["marketplace"],
)
def daily_marketplace_pipeline():

    # Повтор безопасен: publish_batch() атомарен (либо сущности +
    # commit-маркер оказываются в source_root, либо не остаётся следов
    # этого load_date), так что упавшая попытка ничего за собой не
    # оставляет.
    @task(execution_timeout=timedelta(hours=1))
    def generate(ds: str) -> None:
        load_date = datetime.fromisoformat(ds)
        day_index = (load_date.date() - START_DATE.date()).days

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

        business_rules = [
            arg
            for key in BUSINESS_RULE_KEYS
            for arg in (f"--{key.replace('_', '-')}", str(GROWTH_CONFIG[key]))
        ]

        _run([
            PYTHON_BIN, "generate_data.py",
            "--load-date", ds,
            "--data-dir", DATA_DIR,
            "--customers-count", str(customers_count),
            "--orders-count", str(orders_count),
            "--payments-count", str(payments_count),
            "--launch-date", GROWTH_CONFIG["start_date"],
            *business_rules,
        ])

    # retries=0 намеренно: ingest_csv_to_raw.py не идемпотентен —
    # ensure_not_exists() запрещает перезапись, а отката уже записанных
    # партиций у него нет. Если он упал на второй из трёх сущностей,
    # повтор гарантированно упадёт снова с "партиция уже существует" и
    # только съест retry_delay. Такой день чинится руками.
    @task(retries=0, execution_timeout=timedelta(minutes=30))
    def ingest(ds: str) -> None:
        _run([
            PYTHON_BIN, "ingest_csv_to_raw.py",
            "--load-date", ds,
            "--data-dir", DATA_DIR,
        ])

    # Повтор безопасен: clean/quarantine пишутся в mode("overwrite") по
    # своей партиции load_date.
    @task(execution_timeout=timedelta(hours=3))
    def transform(ds: str) -> None:
        _run([
            PYTHON_BIN, "transform.py",
            "--load-date", ds,
            "--data-dir", DATA_DIR,
        ])

    # Повтор безопасен: маркер _load_commits пишется последним, и уже
    # загруженный день превращается в no-op (is_load_date_committed).
    @task(execution_timeout=timedelta(hours=3))
    def load(ds: str) -> None:
        # Пароль читается из Airflow Connection, а не из growth_config.json/
        # clickhouse_config.json — не оседает ни в git, ни в рендере
        # шаблонов задачи (в отличие от BashOperator с командой-строкой).
        # Дальше он уходит переменной окружения, а не флагом: аргументы
        # командной строки видны в ps любому пользователю воркера.
        password = BaseHook.get_connection("clickhouse_default").password

        _run(
            [
                PYTHON_BIN, "load_to_clickhouse.py",
                "--load-date", ds,
                "--data-dir", DATA_DIR,
                "--clickhouse-host", CLICKHOUSE_CONFIG["host"],
                "--clickhouse-port", str(CLICKHOUSE_CONFIG["port"]),
                "--clickhouse-database", CLICKHOUSE_CONFIG["database"],
                "--clickhouse-user", CLICKHOUSE_CONFIG["user"],
            ],
            env={"CLICKHOUSE_PASSWORD": password},
        )

    (
        generate(ds="{{ ds }}")
        >> ingest(ds="{{ ds }}")
        >> transform(ds="{{ ds }}")
        >> load(ds="{{ ds }}")
    )


daily_marketplace_pipeline()
