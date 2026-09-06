"""Тесты семантического слоя: clickhouse_marts.sql и его применение.

Проверяется не SQL как таковой — для этого нужен ClickHouse, — а
согласованность трёх мест, которые обязаны говорить об одном и том же:
файла витрин, списка датасетов в скрипте Superset и подстановки
бизнес-констант из growth_config.json.

Именно здесь ломается семантический слой на практике: витрину
переименовали, а датасет в BI остался смотреть на старое имя.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

import growth

REPO_ROOT = Path(__file__).resolve().parent.parent
MARTS_SQL_PATH = REPO_ROOT / "clickhouse_marts.sql"
SUPERSET_SCRIPT = REPO_ROOT / "superset" / "apply_review_fixes.py"

MARTS_SCHEMA = "marketplace_marts"


@pytest.fixture(scope="module")
def marts_sql() -> str:
    return MARTS_SQL_PATH.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def rendered_sql(marts_sql: str) -> str:
    """SQL с подставленными константами — как его увидит ClickHouse."""
    config = growth.load_growth_config()

    return (
        marts_sql
        .replace("{{MATURITY_DAYS}}", str(growth.maturity_days(config)))
        .replace(
            "{{CUSTOMER_MATURITY_DAYS}}",
            str(growth.customer_maturity_days(config)),
        )
    )


def without_comments(sql: str) -> str:
    """SQL без строк-комментариев.

    Иначе упоминание таблицы в комментарии считается за обращение к ней.
    """
    return "\n".join(
        line for line in sql.splitlines()
        if not line.strip().startswith("--")
    )


def view_names(sql: str) -> set[str]:
    pattern = rf"CREATE OR REPLACE VIEW {MARTS_SCHEMA}\.(\w+)"
    return set(re.findall(pattern, sql))


def dataset_names() -> set[str]:
    """Ключи DATASETS из скрипта Superset, без его импорта.

    Импортировать нельзя: модуль требует requests и завершает процесс,
    если его нет, — прямо посреди прогона тестов. Разбор через ast
    обходится stdlib и не запускает чужой код.
    """
    tree = ast.parse(SUPERSET_SCRIPT.read_text(encoding="utf-8"))

    for node in tree.body:
        targets = getattr(node, "targets", []) or [getattr(node, "target", None)]
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "DATASETS":
                return {ast.literal_eval(key) for key in node.value.keys}

    raise AssertionError("В apply_review_fixes.py не найден словарь DATASETS")


# ---------------------------------------------------------------------
# Согласованность витрин и датасетов
# ---------------------------------------------------------------------

def test_every_dataset_has_a_view(marts_sql):
    missing = dataset_names() - view_names(marts_sql)
    assert not missing, (
        f"датасеты Superset ссылаются на несуществующие витрины: {sorted(missing)}"
    )


def test_every_view_is_used_by_a_dataset(marts_sql):
    orphaned = view_names(marts_sql) - dataset_names()
    assert not orphaned, (
        f"витрины не подключены ни к одному датасету: {sorted(orphaned)}"
    )


def test_marts_database_is_created_before_views(marts_sql):
    create_db = marts_sql.index(f"CREATE DATABASE IF NOT EXISTS {MARTS_SCHEMA}")
    first_view = marts_sql.index(f"CREATE OR REPLACE VIEW {MARTS_SCHEMA}.")
    assert create_db < first_view


# ---------------------------------------------------------------------
# Подстановка бизнес-констант
# ---------------------------------------------------------------------

def test_all_placeholders_are_substituted(rendered_sql):
    assert "{{" not in rendered_sql


def test_maturity_horizon_is_the_sum_of_deadlines(rendered_sql):
    # На дефолтах — 34 дня, а не круглая неделя: возврат возможен вплоть
    # до конца окна обработки рефанда.
    expected = growth.maturity_days()
    assert expected == 34
    assert f"INTERVAL {expected} DAY" in rendered_sql


def test_customer_horizon_comes_from_config(rendered_sql):
    expected = growth.customer_maturity_days()
    assert f"INTERVAL {expected} DAY" in rendered_sql


def test_no_deadline_is_hardcoded_in_sql(marts_sql):
    """В самом файле горизонты только плейсхолдерами.

    Иначе у хранилища появилась бы собственная копия бизнес-правил,
    молча расходящаяся с growth_config.json.
    """
    intervals = set(re.findall(r"INTERVAL (\S+) DAY", marts_sql))
    assert intervals == {"{{MATURITY_DAYS}}", "{{CUSTOMER_MATURITY_DAYS}}"}


# ---------------------------------------------------------------------
# Разбор файла на statements
# ---------------------------------------------------------------------

def test_script_splits_into_create_statements_only(rendered_sql):
    """Так же, как его режет _execute_sql_script в load_to_clickhouse.py.

    Точка с запятой внутри строкового литерала развалила бы файл на
    куски, которые ClickHouse не примет.
    """
    statements = [part.strip() for part in rendered_sql.split(";") if part.strip()]

    assert len(statements) == 1 + len(view_names(rendered_sql))

    for statement in statements:
        body = "\n".join(
            line for line in statement.splitlines()
            if not line.strip().startswith("--")
        ).strip()
        assert body.upper().startswith("CREATE"), body[:80]


def test_final_is_used_on_every_replacing_merge_tree_read(marts_sql):
    """Витрины первого уровня обязаны читать источник с FINAL.

    customers/orders/payments — ReplacingMergeTree: одна сущность лежит в
    нескольких версиях, и без FINAL возвращённый рефанд считается дважды.
    """
    code = without_comments(marts_sql)

    for table in ("customers", "orders", "payments"):
        reads = re.findall(
            rf"marketplace_analytics\.{table}\b(\s+FINAL)?", code
        )
        assert reads, f"витрины не читают marketplace_analytics.{table}"
        assert all(match.strip() == "FINAL" for match in reads), (
            f"marketplace_analytics.{table} читается без FINAL"
        )


# ---------------------------------------------------------------------
# Разбор результата проверок на данных
#
# Сами проверки требуют ClickHouse и потому пропускаются в CI. Функция,
# которая решает «прошло или нет», работает без него — и обязана быть
# проверена, иначе набор из четырнадцати запросов молча зазеленеет
# целиком при первой же ошибке в разборе.
# ---------------------------------------------------------------------


def _evaluate():
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "marts_data", Path(__file__).resolve().parent / "test_marts_data.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.evaluate


def test_check_passes_on_one():
    _evaluate()("проверка", "почему", "1\tвсё сошлось")


def test_check_fails_on_zero():
    import pytest as _pytest

    with _pytest.raises(AssertionError, match="разошлось"):
        _evaluate()("проверка", "почему", "0\tразошлось")


def test_check_fails_on_empty_answer():
    import pytest as _pytest

    with _pytest.raises(AssertionError, match="ничего не вернул"):
        _evaluate()("проверка", "почему", "")


def test_failure_message_explains_why_it_matters():
    import pytest as _pytest

    with _pytest.raises(AssertionError, match="дедупликация сломалась"):
        _evaluate()("проверка", "дедупликация сломалась", "0\tдеталь")
