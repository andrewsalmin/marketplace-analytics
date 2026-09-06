#!/usr/bin/env python3
"""Применение правок ревью дашборда «Аналитика маркетплейса» к Superset.

Скрипт правит датасеты, чарты и метаданные дашборда через REST API. Он
идемпотентен: повторный запуск приводит инстанс в то же состояние, а не
накапливает изменения.

SQL витрин здесь нет — он живёт в clickhouse_marts.sql как объекты базы
marketplace_marts, и скрипт только переводит датасеты Superset на них
(sql=None, схема marketplace_marts). Датасет при этом не пересоздаётся и
не переименовывается, поэтому чарты, ссылающиеся на него по id, ничего
не замечают. Витрины должны быть применены раньше — это делает
load_to_clickhouse.py (ensure_marts) при любой загрузке.

Фазы соответствуют приоритетам ревью:

    p0   цифры неверны        (FINAL, определение GMV, незрелый хвост, ...)
    p1   читается неправильно (форматы осей, проценты, сортировки, язык)
    p2   структура и гигиена  (фильтры, KPI-строка, DQ-вкладка, словари)

По умолчанию ничего не пишется — печатается план. Применение:

    python superset/apply_review_fixes.py --username admin --phases p0
        --url http://superset.example.com:8088

Пароль скрипт спросит скрытым вводом — в командной строке его лучше не
передавать, оттуда он попадает в историю оболочки и в список процессов.
Для CI, где спросить некого, есть SUPERSET_PASSWORD и --password.
Добавь --apply, чтобы вместо плана записать изменения.

Перед первой записью скрипт выгружает текущее состояние дашборда в
./superset_backup_<timestamp>.zip — это и есть путь отката (импорт ZIP
через UI или /api/v1/dashboard/import/ с overwrite=true).

Требует, чтобы в Superset была заведена база с доступом к схеме
marketplace_marts: датасеты создаются в той же базе Superset, что и
существующий датасет orders, но в другой схеме ClickHouse.

Целевая версия Superset — 4.1+ (чарты echarts_*, heatmap_v2, matrixify в
form_data). Неизвестные ключи form_data Superset игнорирует молча, так
что несовпадение версии проявится как «правка не подействовала», а не
как поломка.
"""

from __future__ import annotations

import argparse
import copy
import getpass
import json
import os
import pathlib
import re
import sys
from datetime import datetime, timezone
from typing import Any

# Скрипт лежит в подкаталоге, а growth.py — в корне репозитория: проект
# не устанавливается пакетом, поэтому корень добавляется в путь так же,
# как это делает conftest.py для тестов.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from growth import maturity_days  # noqa: E402

try:
    import requests
except ImportError:  # pragma: no cover - подсказка вместо трейсбека
    sys.exit("Нужен requests: pip install requests")


# ---------------------------------------------------------------------------
# Общие SQL-фрагменты
# ---------------------------------------------------------------------------

# База семантического слоя в ClickHouse (clickhouse_marts.sql). Имена
# витрин там совпадают с именами датасетов здесь один в один — именно
# поэтому перевод датасета на витрину не требует его переименования, и
# чарты, ссылающиеся на датасет по id, ничего не замечают.
MARTS_SCHEMA = "marketplace_marts"

# Горизонт зрелости заказа нужен здесь только для подписей чартов —
# сама отсечка делается колонкой is_mature, которую считает витрина.
# Значение берётся из growth_config.json тем же кодом, что и в
# load_to_clickhouse.py, чтобы текст под графиком не разошёлся с тем,
# что реально посчитано.
MATURITY_DAYS = maturity_days()

# Раньше отсечка незрелых заказов жила прямо в фильтре чарта подзапросом.
# Superset такие фильтры не исполняет (ALLOW_ADHOC_SUBQUERY=False) и
# рисует «Custom SQL fields cannot contain sub-queries» вместо графика.
# Строка нужна, чтобы вычистить фильтр из чартов, где он ещё остался.
OBSOLETE_FILTERS = [
    "created_at < (SELECT max(created_at) - INTERVAL "
    f"{MATURITY_DAYS} DAY FROM marketplace_analytics.orders FINAL)"
]

MATURE_FILTER = ("is_mature = 1", "is_mature")

PAID = "paid_at IS NOT NULL"
# «Чистый» — заказ, который дошёл до покупателя и остался у него:
# ни отмены, ни возврата товара, ни возврата денег. Возврат товара
# (returned_at) раньше в условие не входил, и 1 749 возвращённых
# заказов на 3,53 млн ₽ считались чистой выручкой.
NOT_CANCELLED = (
    "cancelled_at IS NULL AND returned_at IS NULL AND refunded_at IS NULL"
)


# ---------------------------------------------------------------------------
# Клиент Superset
# ---------------------------------------------------------------------------


class SupersetError(RuntimeError):
    pass


class Superset:
    """Минимальный клиент REST API: логин, CSRF, GET/POST/PUT."""

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"
        self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        resp = self.session.post(
            f"{self.base}/api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider": "db",
                "refresh": True,
            },
            timeout=30,
        )
        if resp.status_code == 401:
            raise SupersetError(
                f"Superset не принял пару «{username}» + пароль. Проверь её "
                "в веб-интерфейсе: это должна быть учётка Superset, а не "
                "сервера или базы."
            )
        if resp.status_code != 200:
            raise SupersetError(f"Логин не прошёл: {resp.status_code} {resp.text}")
        self.session.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"

        csrf = self.session.get(
            f"{self.base}/api/v1/security/csrf_token/", timeout=30
        )
        if csrf.status_code == 200:
            self.session.headers["X-CSRFToken"] = csrf.json()["result"]
            self.session.headers["Referer"] = self.base

    def _check(self, resp: requests.Response, what: str) -> dict[str, Any]:
        if resp.status_code >= 400:
            raise SupersetError(f"{what}: HTTP {resp.status_code} {resp.text[:600]}")
        return resp.json() if resp.content else {}

    def get(self, path: str, **kw: Any) -> dict[str, Any]:
        return self._check(
            self.session.get(f"{self.base}{path}", timeout=60, **kw), f"GET {path}"
        )

    def put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._check(
            self.session.put(f"{self.base}{path}", json=payload, timeout=60),
            f"PUT {path}",
        )

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._check(
            self.session.post(f"{self.base}{path}", json=payload, timeout=60),
            f"POST {path}",
        )

    def export_dashboard(self, dashboard_id: int, dest: str) -> str:
        resp = self.session.get(
            f"{self.base}/api/v1/dashboard/export/",
            params={"q": json.dumps([dashboard_id])},
            timeout=120,
        )
        if resp.status_code >= 400:
            raise SupersetError(f"Экспорт не удался: HTTP {resp.status_code}")
        with open(dest, "wb") as fh:
            fh.write(resp.content)
        return dest

    # --- роли и права ---------------------------------------------------

    def role_by_name(self, name: str) -> dict[str, Any] | None:
        query = json.dumps(
            {"filters": [{"col": "name", "opr": "eq", "value": name}]}
        )
        rows = self.get(f"/api/v1/security/roles/?q={query}")["result"]
        return rows[0] if rows else None

    def role_permission_ids(self, role_id: int) -> set[int]:
        query = json.dumps({"page_size": 1000})
        result = self.get(
            f"/api/v1/security/roles/{role_id}/permissions/?q={query}"
        )["result"]
        return {row["id"] for row in result}

    def datasource_permissions(self, schema: str) -> dict[str, int]:
        """Разрешения datasource_access на витрины схемы: имя -> id.

        Superset называет их по шаблону [база].[схема].[таблица](id:N),
        поэтому отбор идёт по подстроке `.[схема].`, а не по точному
        совпадению: имя базы в разных инсталляциях своё.
        """
        found: dict[str, int] = {}
        page = 0
        while True:
            query = json.dumps({"page_size": 100, "page": page})
            result = self.get(
                f"/api/v1/security/permissions-resources/?q={query}"
            )["result"]
            if not result:
                break
            for row in result:
                permission = (row.get("permission") or {}).get("name")
                view_menu = (row.get("view_menu") or {}).get("name") or ""
                if permission != "datasource_access":
                    continue
                marker = f".[{schema}]."
                if marker not in view_menu:
                    continue
                table = view_menu.split(marker, 1)[1].split("]")[0].lstrip("[")
                found[table] = row["id"]
            page += 1
            if page > 50:  # предохранитель от бесконечной страницы
                break
        return found

    def set_role_permissions(self, role_id: int, ids: set[int]) -> None:
        self.post(
            f"/api/v1/security/roles/{role_id}/permissions",
            {"permission_view_menu_ids": sorted(ids)},
        )

    # --- справочники ---------------------------------------------------

    def datasets(self) -> dict[str, dict[str, Any]]:
        query = json.dumps(
            {"columns": ["id", "table_name", "sql", "schema", "kind"], "page_size": 100}
        )
        rows = self.get(f"/api/v1/dataset/?q={query}")["result"]
        return {r["table_name"]: r for r in rows}

    def charts(self, dashboard_id: int) -> dict[str, dict[str, Any]]:
        rows = self.get(f"/api/v1/dashboard/{dashboard_id}/charts")["result"]
        return {r["slice_name"]: r for r in rows}


# ---------------------------------------------------------------------------
# Хелперы для form_data
# ---------------------------------------------------------------------------


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:40]


def metric(label: str, expression: str) -> dict[str, Any]:
    """Adhoc-метрика на сыром SQL с человекочитаемой подписью."""
    return {
        "expressionType": "SQL",
        "sqlExpression": expression,
        "label": label,
        "hasCustomLabel": True,
        "optionName": f"metric_{_slug(label)}",
        "aggregate": None,
        "column": None,
    }


def sql_filter(expression: str, subject: str) -> dict[str, Any]:
    return {
        "clause": "WHERE",
        "expressionType": "SQL",
        "sqlExpression": expression,
        "subject": subject,
        "filterOptionName": f"filter_{_slug(subject)}_{abs(hash(expression)) % 10**8}",
    }


def drop_filters(filters: list[Any], expressions: list[str]) -> list[Any]:
    """Убирает фильтры с перечисленными SQL-выражениями.

    Нужно, чтобы правка была самоисправляющейся: без этого фильтр,
    записанный прошлой версией скрипта, остался бы в чарте рядом с новым.
    """
    unwanted = {" ".join(e.split()) for e in expressions}
    return [
        f
        for f in filters
        if not (
            isinstance(f, dict)
            and " ".join(str(f.get("sqlExpression") or "").split()) in unwanted
        )
    ]


def ensure_filter(filters: list[Any], expression: str, subject: str) -> list[Any]:
    """Добавляет SQL-фильтр, если такого выражения ещё нет (идемпотентность)."""
    normalised = " ".join(expression.split())
    for existing in filters:
        if isinstance(existing, dict):
            current = " ".join(str(existing.get("sqlExpression") or "").split())
            if current == normalised:
                return filters
    return [*filters, sql_filter(expression, subject)]


def rolling_mean(days: int = 7) -> dict[str, Any]:
    """P1-07: скользящее среднее вместо суточной пилы."""
    return {
        "rolling_type": "mean",
        "rolling_periods": days,
        "min_periods": days,
    }


DATE_AXIS = {"x_axis_time_format": "%d.%m"}  # P1-01


def sort_by_first_metric(label: str) -> dict[str, Any]:
    """P0-07: явная сортировка категориальной оси по метрике."""
    return {
        "x_axis_sort": label,
        "x_axis_sort_asc": False,
        "order_desc": True,
    }


# ---------------------------------------------------------------------------
# Патчи витрин
# ---------------------------------------------------------------------------

# Датасеты Superset и витрины, на которые они смотрят. SQL здесь больше
# нет: он живёт в clickhouse_marts.sql как объекты хранилища. Имя
# датасета = имя витрины, схема у всех одна (MARTS_SCHEMA), поэтому
# таблица ниже — это список, а не отображение.
#
# phase сохранён: витрины фазы p0 должны появиться раньше чартов, которые
# на их колонки ссылаются.
DATASETS: dict[str, dict[str, str]] = {
    "customers": {
        "phase": "p0",
        "note": "P0-01 FINAL + P2-06 словарь каналов",
    },
    "orders": {
        "phase": "p0",
        "note": "P0-01 FINAL + P2-06 словарь статусов + P0-03 is_mature",
    },
    "payments": {
        "phase": "p0",
        "note": "P0-01 FINAL + P2-06 словари статуса и способа оплаты",
    },
    "order_funnel_stages": {
        "phase": "p0",
        "note": "P0-03 воронка только по зрелым заказам",
    },
    "orders_with_customer_dim": {
        "phase": "p0",
        "note": "P0-02 признаки оплаты + P1-11 русский канал",
    },
    "customer_first_order": {
        "phase": "p0",
        "note": "P0-05 признак зрелой когорты покупателей",
    },
    "first_order_delay_distribution": {
        "phase": "p0",
        "note": "P0-04 распределение задержки первого заказа",
    },
    "refund_processing_times": {
        "phase": "p0",
        "note": "P0-06 сырые длительности обработки рефанда",
    },
    "dq_reason_breakdown": {
        "phase": "p2",
        "note": "P2-04 разбивка DQ-ошибок с load_date",
    },
    "quarantine_volume": {
        "phase": "p2",
        "note": "P2-04 объём карантина по дням и сущностям",
    },
    "load_freshness": {
        "phase": "p2",
        "note": "P2-04 свежесть загрузки из _load_commits",
    },
    "payment_retries_distribution": {
        "phase": "p0",
        "note": "1.5 попытки оплаты вместе с возмещёнными",
    },
}


# ---------------------------------------------------------------------------
# Патчи чартов
# ---------------------------------------------------------------------------

GMV_PAID = f"SUM(if({PAID}, amount_kopecks, 0)) / 100"
GMV_NET = f"SUM(if({PAID} AND {NOT_CANCELLED}, amount_kopecks, 0)) / 100"
AOV_PAID = f"{GMV_PAID} / countIf({PAID})"


def chart_patches() -> dict[str, dict[str, Any]]:
    """Патчи form_data по имени чарта.

    Каждый патч: phase, note, set (ключи form_data), опционально
    filters (SQL-выражения для adhoc_filters), slice_name, description,
    viz_type, dataset (переключение витрины), unset.
    """
    return {
        # ---------------- P0 ----------------
        "Изменение GMV": {
            "phase": "p0",
            "dataset": "orders_with_customer_dim",
            "note": "P0-02 GMV только по оплаченным + P0-03 зрелый хвост",
            "slice_name": "Динамика GMV",
            "description": (
                "«Оплачено» — сумма заказов с paid_at. «Чистый» дополнительно "
                f"исключает отменённые и возвращённые. Последние "
                f"{MATURITY_DAYS} дней отрезаны: заказ может менять статус "
                "до истечения всех дедлайнов."
            ),
            "set": {
                "metrics": [
                    metric("Оплачено", GMV_PAID),
                    metric("Чистый", GMV_NET),
                ],
                "y_axis_title": "GMV, ₽",
                **DATE_AXIS,
                **rolling_mean(),
            },
            "filters": [MATURE_FILTER],
        },
        "Изменение среднего чека (AOV)": {
            "phase": "p0",
            "dataset": "orders_with_customer_dim",
            "note": "P0-02 средний чек по оплаченным заказам",
            "slice_name": "Динамика среднего чека (AOV)",
            "description": "Сумма оплаченных заказов, делённая на их число.",
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                **DATE_AXIS,
                **rolling_mean(),
            },
            "filters": [MATURE_FILTER],
        },
        "Изменение долей отмен и возвратов": {
            "phase": "p0",
            "dataset": "orders_with_customer_dim",
            "note": "P0-03 зрелый хвост + P1-05 проценты",
            "slice_name": "Динамика долей отмен и возвратов",
            "description": (
                f"Доли считаются от числа созданных заказов. Последние "
                f"{MATURITY_DAYS} дней отрезаны: свежий заказ ещё не успел "
                "отмениться или вернуться."
            ),
            "set": {"y_axis_format": ".1%", **DATE_AXIS, **rolling_mean()},
            "filters": [MATURE_FILTER],
        },
        "Изменение структуры статусов заказов": {
            "phase": "p0",
            "dataset": "orders_with_customer_dim",
            "note": "P0-03 зрелый хвост",
            "slice_name": "Динамика структуры статусов заказов",
            "set": {"groupby": ["status_ru"], **DATE_AXIS},
            "filters": [MATURE_FILTER],
        },
        "Время до первого заказа (дней с регистрации)": {
            "phase": "p0",
            "note": "P0-04 распределение вместо среднего по дате регистрации",
            "slice_name": "Распределение времени до первого заказа",
            "description": (
                "Сколько клиентов сделали первый заказ через N дней после "
                "регистрации. Только когорты старше 30 дней."
            ),
            "dataset": "first_order_delay_distribution",
            "viz_type": "echarts_timeseries_bar",
            "set": {
                "x_axis": "days_to_first_order",
                "metrics": [metric("Число клиентов", "SUM(customers)")],
                "groupby": [],
                "x_axis_title": "Дней с регистрации",
                "y_axis_title": "Число клиентов",
                "x_axis_sort": "days_to_first_order",
                "x_axis_sort_asc": True,
                "row_limit": 200,
                "time_grain_sqla": None,
            },
            "replace_filters": [],
        },
        "Конверсия: регистрация → первый заказ": {
            "phase": "p0",
            "note": "1.1 явный признак заказа вместо даты",
            "description": (
                "Доля покупателей, сделавших хотя бы один заказ. Считается "
                "по когортам с полным горизонтом наблюдения — у регистраций "
                "последних недель заказ ещё впереди."
            ),
            "set": {
                "metric": metric(
                    "Конверсия в первый заказ", "sum(has_order) / count()"
                ),
                "y_axis_format": ".1%",
                "subheader": "Хотя бы один заказ за всю историю",
            },
            "filters": [("is_mature = 1", "is_mature")],
        },
        "Время обработки рефанда": {
            "phase": "p0",
            "note": "P0-06 перцентили по сырым длительностям вместо AVG",
            "slice_name": "Время обработки рефанда (перцентили)",
            "description": (
                "Перцентили по отдельным возвратам, а не по дневным средним. "
                "Разрез — отмена против возврата товара."
            ),
            "dataset": "refund_processing_times",
            "viz_type": "echarts_timeseries_bar",
            "set": {
                "x_axis": "refund_kind",
                "metrics": [
                    metric("p50", "quantile(0.5)(duration_seconds) / 3600"),
                    metric("p90", "quantile(0.9)(duration_seconds) / 3600"),
                    metric("p99", "quantile(0.99)(duration_seconds) / 3600"),
                ],
                "groupby": [],
                "x_axis_title": "",
                "y_axis_title": "Время, ч",
                "row_limit": 100,
                "time_grain_sqla": None,
            },
            "replace_filters": [],
        },
        "Средний чек по городам (топ-10)": {
            "phase": "p0",
            "note": "P0-07 сортировка по метрике + P0-02 оплаченные заказы",
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                **sort_by_first_metric("Средний чек, ₽"),
            },
            "filters": [MATURE_FILTER],
        },
        "GMV по городам (топ-10)": {
            "phase": "p0",
            "note": "P0-07 сортировка по метрике + P0-02 оплаченные заказы",
            "set": {
                "metrics": [metric("GMV, ₽", GMV_PAID)],
                **sort_by_first_metric("GMV, ₽"),
            },
            "filters": [MATURE_FILTER],
        },
        "Разбивка DQ-ошибок по типам": {
            "phase": "p0",
            "note": "P0-07 сортировка по числу строк в карантине",
            "set": {
                "y_axis_title": "Строк в карантине",
                **sort_by_first_metric("Строк в карантине"),
            },
        },
        "GMV по каналам привлечения": {
            "phase": "p0",
            "note": "P0-02 оплаченные заказы + P0-07 сортировка + P1-11 язык",
            "set": {
                "metrics": [metric("GMV, ₽", GMV_PAID)],
                "x_axis": "acquisition_channel_ru",
                **sort_by_first_metric("GMV, ₽"),
            },
            "filters": [MATURE_FILTER],
        },
        "Средний чек по каналам привлечения": {
            "phase": "p0",
            "note": "P0-02 оплаченные заказы + P0-07 сортировка + P1-11 язык",
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                "x_axis": "acquisition_channel_ru",
                **sort_by_first_metric("Средний чек, ₽"),
            },
            "filters": [MATURE_FILTER],
        },
        # ---------------- P1 ----------------
        "Воронка заказа": {
            "phase": "p1",
            "note": "P1-02 нативная воронка вместо баров с выпадающими подписями",
            "description": "Конверсия между этапами по зрелым заказам.",
            "viz_type": "funnel",
            "set": {
                "groupby": ["stage"],
                "metric": {
                    "expressionType": "SIMPLE",
                    "aggregate": "SUM",
                    "column": {"column_name": "cnt", "type": "UInt64"},
                    "label": "Число заказов",
                    "hasCustomLabel": True,
                    "optionName": "metric_funnel_cnt",
                },
                "sort_by_metric": True,
                "number_format": "SMART_NUMBER",
                "tooltip_label_type": "key_value_percent",
                "show_labels": True,
                "show_legend": False,
                "row_limit": 100,
            },
        },
        "Число покупателей по городам": {
            "phase": "p1",
            "note": "P1-02 горизонтальные бары + сортировка",
            "set": {
                "orientation": "horizontal",
                **sort_by_first_metric("Число покупателей"),
            },
        },
        "Причины отмены заказов": {
            "phase": "p1",
            "dataset": "orders_with_customer_dim",
            "note": "P1-03 подписи внутри пирога + P2-06 словарь из витрины",
            "set": {
                "groupby": ["cancellation_reason_ru"],
                "labels_outside": False,
                "label_line": False,
                "label_type": "key_percent",
                "show_labels": True,
                "show_labels_threshold": 1,
                "number_format": "SMART_NUMBER",
            },
        },
        "Доли покупателей по каналам привлечения": {
            "phase": "p1",
            "note": "P1-03 подписи внутри пирога + P2-06 словарь из витрины",
            "set": {
                "groupby": ["acquisition_channel_ru"],
                "labels_outside": False,
                "label_line": False,
                "label_type": "key_percent",
                "show_labels": True,
                "show_labels_threshold": 1,
            },
        },
        "Изменение причин отмены": {
            "phase": "p1",
            "dataset": "orders_with_customer_dim",
            "note": "P1-04 легенда сбоку + P1-01 формат оси",
            "slice_name": "Динамика причин отмены",
            "set": {
                "legendOrientation": "right",
                "show_legend": True,
                **DATE_AXIS,
            },
        },
        "Изменение success rate оплаты": {
            "phase": "p1",
            "note": "1.5 исход попытки не переписывается возвратом",
            "slice_name": "Динамика success rate оплаты",
            "description": (
                "Доля успешно проведённых попыток оплаты. Возврат денег "
                "не отменяет того, что платёж прошёл: иначе показатель за "
                "июнь менялся бы задним числом в сентябре."
            ),
            "set": {
                "metrics": [
                    metric(
                        "Доля успешных оплат",
                        "countIf(attempt_succeeded) / count()",
                    )
                ],
                "y_axis_format": ".1%",
                "truncateYAxis": True,
                "y_axis_bounds": [0.5, 1],
                **DATE_AXIS,
                **rolling_mean(),
            },
        },
        "Тренд DQ-ошибок по сущностям": {
            "phase": "p1",
            "note": "P1-05 проценты + P2-04 порог 5%",
            "set": {
                "y_axis_format": ".2%",
                "y_axis_title": "Доля невалидных строк",
                **DATE_AXIS,
                "annotation_layers": [
                    {
                        "name": "Порог 5%",
                        "annotationType": "FORMULA",
                        "sourceType": "",
                        "value": "0.05",
                        "style": "dashed",
                        "width": 1,
                        "opacity": "",
                        "color": "#E04355",
                        "show": True,
                        "showLabel": True,
                        "showMarkers": False,
                        "hideLine": False,
                    }
                ],
            },
        },
        "Тайминги воронки заказа (перцентили)": {
            "phase": "p1",
            "note": "P1-08 порядок перцентилей p50 → p90 → p99",
            "set": {
                "metrics": [
                    metric("p50", "quantile(0.5)(duration_seconds) / 3600"),
                    metric("p90", "quantile(0.9)(duration_seconds) / 3600"),
                    metric("p99", "quantile(0.99)(duration_seconds) / 3600"),
                ],
                "x_axis_sort": "stage",
                "x_axis_sort_asc": True,
            },
        },
        "Количество заказов по часам и дням недели": {
            "phase": "p1",
            "dataset": "orders_with_customer_dim",
            "note": "P1-09 понедельник сверху",
            "set": {"sort_y_axis": "alpha_desc", "sort_x_axis": "alpha_asc"},
        },
        "Retention по когортам регистрации (по неделям)": {
            "phase": "p1",
            "note": "P1-10 короткие подписи когорт + отсечение недели −1",
            "description": (
                "Доля когорты, сделавшая заказ через N недель после "
                "регистрации. Только зрелые ячейки (is_mature)."
            ),
            "set": {
                "groupby": {
                    "expressionType": "SQL",
                    "label": "Когорта (неделя регистрации)",
                    "sqlExpression": "formatDateTime(cohort_week, '%m-%d')",
                },
                "y_axis_format": ".0%",
                "sort_y_axis": "alpha_desc",
            },
            "filters": [("weeks_since_signup >= 0", "weeks_since_signup")],
        },
        "Изменение числа заказов": {
            "phase": "p1",
            "dataset": "orders_with_customer_dim",
            "note": "P1-01 формат оси + P1-07 сглаживание + P1-11 заголовок",
            "slice_name": "Динамика числа заказов",
            "set": {**DATE_AXIS, **rolling_mean()},
        },
        "Изменение числа новых покупателей": {
            "phase": "p1",
            "note": "P1-01 формат оси + P1-07 сглаживание + P1-11 заголовок",
            "slice_name": "Динамика числа новых покупателей",
            "set": {**DATE_AXIS, **rolling_mean()},
        },
        "Изменение числа новых и повторных заказов": {
            "phase": "p1",
            "note": "P1-01 формат оси + P1-11 заголовок",
            "slice_name": "Динамика новых и повторных заказов",
            "set": DATE_AXIS,
        },
        "Изменение суммы рефандов": {
            "phase": "p1",
            "dataset": "orders_with_customer_dim",
            "note": "P1-01 формат оси + P1-11 заголовок",
            "slice_name": "Динамика суммы рефандов",
            "set": DATE_AXIS,
        },
        # ---------------- P2 ----------------
        "Способы оплаты и статус": {
            "phase": "p2",
            "note": "P2-07 сортировка и подписи после перевода витрины на FINAL",
            "description": (
                "Платежи после дедупликации FINAL: возвращённый платёж "
                "учитывается один раз, в статусе «Возврат»."
            ),
            "set": {
                "x_axis": "payment_method_ru",
                "groupby": ["status_ru"],
                "stack": "Stack",
                **sort_by_first_metric("Платежи"),
            },
        },
        "Распределение ретраев оплаты": {
            "phase": "p0",
            "note": "1.5 попытки вместо ретраев, с учётом возмещённых",
            "slice_name": "Распределение попыток оплаты",
            "dataset": "payment_retries_distribution",
            "description": (
                "Сколько заказов потребовали N попыток оплаты: одна "
                "попытка — это оплата с первого раза. Ось Y "
                "логарифмическая, иначе хвост не виден."
            ),
            "set": {
                "x_axis": "attempts",
                "metrics": [metric("Число заказов", "SUM(orders_count)")],
                "x_axis_title": "Попыток оплаты на заказ",
                "y_axis_title": "Число заказов",
                "x_axis_sort": "attempts",
                "x_axis_sort_asc": True,
            },
        },
        "Распределение числа заказов на клиента": {
            "phase": "p2",
            "note": "P2-05 сортировка оси",
            "set": {"x_axis_sort": "orders_count", "x_axis_sort_asc": True},
        },
    }


# P2-02. KPI-строка на Overview: числа, которых на вкладке не было вообще.
KPI_CHARTS: list[dict[str, Any]] = [
    {
        "name": "KPI · GMV",
        "dataset": "orders_with_customer_dim",
        "metric": metric("GMV, ₽", GMV_PAID),
        "format": "SMART_NUMBER",
        "subheader": "Оплаченные заказы за период",
        "filters": [MATURE_FILTER],
    },
    {
        "name": "KPI · Заказы",
        "dataset": "orders_with_customer_dim",
        "metric": metric("Заказов", "count()"),
        "format": "SMART_NUMBER",
        "subheader": "Создано заказов",
        "filters": [MATURE_FILTER],
    },
    {
        "name": "KPI · Средний чек",
        "dataset": "orders_with_customer_dim",
        "metric": metric("Средний чек, ₽", AOV_PAID),
        "format": ",.0f",
        "subheader": "На оплаченный заказ",
        "filters": [MATURE_FILTER],
    },
    {
        "name": "KPI · Доля отмен",
        "dataset": "orders_with_customer_dim",
        "metric": metric("Доля отмен", "countIf(cancelled_at IS NOT NULL) / count()"),
        "format": ".1%",
        "subheader": "От числа созданных заказов",
        "filters": [MATURE_FILTER],
    },
    {
        "name": "KPI · Невалидных строк",
        "dataset": "dq_metrics",
        "metric": metric(
            "Доля невалидных строк", "SUM(invalid_rows) / SUM(total_rows)"
        ),
        "format": ".2%",
        "subheader": "Доля строк в карантине",
        "filters": [],
    },
]

# P2-04. Свежесть пайплайна на вкладке качества данных.
DQ_CHARTS: list[dict[str, Any]] = [
    {
        "name": "Часов с последней загрузки",
        "dataset": "load_freshness",
        "metric": metric("Часов с последней загрузки", "max(hours_since_load)"),
        "format": ",.0f",
        "subheader": "По маркеру _load_commits",
        "filters": [],
    },
]

TAB_NAMES = {  # P1-11
    "TAB-overview": "Обзор",
    "TAB-orderops": "Заказы",
    "TAB-payments": "Платежи",
    "TAB-clients": "Покупатели",
    "TAB-dq": "Качество данных",
}

# P2-03. Overview дублировал Orders — эти размещения убираются с обзора,
# сами чарты остаются на своих тематических вкладках.
OVERVIEW_DUPLICATES = ["CHART-explore-20-2", "CHART-explore-2-2"]


# ---------------------------------------------------------------------------
# Применение
# ---------------------------------------------------------------------------


class Runner:
    def __init__(self, client: Superset, dashboard_id: int, apply: bool) -> None:
        self.client = client
        self.dashboard_id = dashboard_id
        self.apply = apply
        self.planned: list[str] = []
        self.datasets = client.datasets()

    def log(self, line: str) -> None:
        self.planned.append(line)
        print(("  ✓ " if self.apply else "  · ") + line)

    # --- витрины -------------------------------------------------------

    def sync_datasets(self, phases: set[str]) -> None:
        """Переводит датасеты на витрины ClickHouse.

        Существующий датасет не пересоздаётся и не переименовывается, а
        переключается на витрину: sql=None делает его физическим, схема
        меняется на marketplace_marts. Id датасета при этом сохраняется,
        поэтому чарты, ссылающиеся на него, продолжают работать без
        единой правки.

        Идемпотентно: датасет, уже смотрящий на витрину, пропускается.
        """
        expected = {n for n, sp in DATASETS.items() if sp["phase"] in phases}
        unseen = expected - set(self.datasets)
        if unseen and len(unseen) == len(expected):
            # Ни одного ожидаемого датасета не видно. Создавать их с нуля
            # опаснее, чем остановиться: скорее всего дело в правах или в
            # том, что список пришёл урезанным, а не в пустом инстансе.
            raise SupersetError(
                f"Не видно ни одного из {len(expected)} ожидаемых датасетов. "
                f"Всего видно {len(self.datasets)}: "
                f"{', '.join(sorted(self.datasets)) or '—'}. "
                "Создание новых остановлено, чтобы не наплодить дубликаты."
            )
        if unseen:
            print(
                f"  ! не найдено датасетов: {', '.join(sorted(unseen))} "
                f"(видно всего {len(self.datasets)})"
            )

        for name, spec in DATASETS.items():
            if spec["phase"] not in phases:
                continue

            existing = self.datasets.get(name)

            if existing is None:
                self._create_dataset(name, spec)
                continue

            already_physical = not (existing.get("sql") or "").strip()
            if already_physical and existing.get("schema") == MARTS_SCHEMA:
                continue

            self.log(f"{spec['note']}: витрина {name} -> {MARTS_SCHEMA}.{name}")
            if self.apply:
                self.client.put(
                    f"/api/v1/dataset/{existing['id']}",
                    {
                        "sql": None,
                        "schema": MARTS_SCHEMA,
                        "table_name": name,
                    },
                )
                # Без refresh Superset продолжает показывать колонки,
                # снятые со старого SQL: новых (status_ru, is_mature) в
                # списке не будет, и чарты по ним не соберутся.
                self.client.put(f"/api/v1/dataset/{existing['id']}/refresh", {})

    def _create_dataset(self, name: str, spec: dict[str, str]) -> None:
        self.log(f"{spec['note']}: новый датасет {MARTS_SCHEMA}.{name}")

        if not self.apply:
            return

        self.client.post(
            "/api/v1/dataset/",
            {
                "database": self._database_id(),
                "schema": MARTS_SCHEMA,
                "table_name": name,
            },
        )
        self.datasets = self.client.datasets()

    def _database_id(self) -> int:
        """Id базы Superset, в которой заводить новые датасеты.

        Берётся у любого существующего датасета: нужен именно id базы, а
        не конкретная витрина. Прежняя версия требовала датасет с именем
        `orders` и падала, если тот почему-то не виден, — хотя рядом
        лежал десяток других с той же базой.
        """
        source = self.datasets.get("orders") or next(
            iter(sorted(self.datasets.values(), key=lambda d: d["id"])), None
        )
        if source is None:
            raise SupersetError(
                "В Superset не видно ни одного датасета, поэтому неизвестно, "
                "в какой базе заводить новые. Проверь права учётной записи "
                "и что база подключена."
            )
        detail = self.client.get(f"/api/v1/dataset/{source['id']}")["result"]
        return detail["database"]["id"]

    def grant_public_access(self, role_name: str) -> None:
        """Выдаёт роли право читать витрины после смены схемы датасета.

        Права на датасет в Superset выданы поимённо:
        `datasource access on [база].[схема].[таблица]`. Перевод датасета
        в другую схему создаёт новый объект прав, а выданное указывает в
        никуда — анонимный посетитель мгновенно теряет доступ ко всему
        переехавшему, хотя сами датасеты на месте.

        Эндпоинт роли не добавляет права, а ЗАМЕНЯЕТ весь список,
        поэтому здесь только объединение с текущими: ошибка в отборе
        оставила бы роль вообще без прав, включая доступ к дашборду.
        """
        role = self.client.role_by_name(role_name)
        if role is None:
            print(f"  ! роль «{role_name}» не найдена — права не тронуты")
            return

        current = self.client.role_permission_ids(role["id"])
        available = self.client.datasource_permissions(MARTS_SCHEMA)

        needed = {name: available[name] for name in DATASETS if name in available}
        absent = [name for name in DATASETS if name not in available]
        missing = {name: pid for name, pid in needed.items() if pid not in current}

        if absent:
            print(
                f"  ! нет объектов прав на витрины: {', '.join(absent)} — "
                "похоже, датасеты ещё не переведены на схему"
            )
        if not missing:
            return

        self.log(
            f"Роль «{role_name}»: доступ к витринам "
            f"({len(missing)} из {len(needed)}) — " + ", ".join(sorted(missing))
        )
        if not self.apply:
            return

        # Снимок текущих прав до записи: восстановить роль по списку id
        # проще, чем вспоминать, что в ней было.
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = f"superset_role_{role_name}_{stamp}.json"
        with open(backup, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "role": role_name,
                    "id": role["id"],
                    "permission_ids": sorted(current),
                },
                fh,
                ensure_ascii=False,
                indent=2,
            )
        print(f"    снимок прав роли: {backup}")

        self.client.set_role_permissions(role["id"], current | set(missing.values()))

        after = self.client.role_permission_ids(role["id"])
        lost = current - after
        if lost:
            raise SupersetError(
                f"Роль «{role_name}» потеряла {len(lost)} прав при записи. "
                f"Восстанови по снимку {backup}"
            )
        still_missing = [n for n, pid in missing.items() if pid not in after]
        if still_missing:
            raise SupersetError("Права не записались: " + ", ".join(still_missing))

    # --- чарты ---------------------------------------------------------

    def patch_charts(self, phases: set[str]) -> None:
        charts = self.client.charts(self.dashboard_id)
        for name, patch in chart_patches().items():
            if patch["phase"] not in phases:
                continue
            # Искать надо и по новому имени: после первого прогона чарт
            # называется уже так, как его переименовал сам скрипт, и по
            # исходному ключу не находится.
            chart = charts.get(name) or charts.get(patch.get("slice_name", ""))
            if not chart:
                print(f"  ! чарт «{name}» не найден — пропуск")
                continue
            payload = self._build_chart_payload(chart, patch)
            if not payload:
                continue
            self.log(f"{patch['note']}: чарт «{name}»")
            if self.apply:
                self.client.put(f"/api/v1/chart/{chart['id']}", payload)

    def _build_chart_payload(
        self, chart: dict[str, Any], patch: dict[str, Any]
    ) -> dict[str, Any] | None:
        form_data = copy.deepcopy(chart.get("form_data") or {})
        before = json.dumps(form_data, sort_keys=True, ensure_ascii=False)

        if "dataset" in patch:
            target = self.datasets.get(patch["dataset"])
            if not target:
                print(f"  ! витрина {patch['dataset']} ещё не создана — пропуск")
                return None
            form_data["datasource"] = f"{target['id']}__table"

        if "viz_type" in patch:
            form_data["viz_type"] = patch["viz_type"]

        for key, value in patch.get("set", {}).items():
            if value is None:
                form_data.pop(key, None)
            else:
                form_data[key] = value

        for key in patch.get("unset", []):
            form_data.pop(key, None)

        if "replace_filters" in patch:
            form_data["adhoc_filters"] = list(patch["replace_filters"])
        form_data["adhoc_filters"] = drop_filters(
            form_data.get("adhoc_filters") or [], OBSOLETE_FILTERS
        )
        for expression, subject in patch.get("filters", []):
            form_data["adhoc_filters"] = ensure_filter(
                form_data.get("adhoc_filters") or [], expression, subject
            )

        changed_form = (
            json.dumps(form_data, sort_keys=True, ensure_ascii=False) != before
        )
        changed_name = patch.get("slice_name", chart["slice_name"]) != chart[
            "slice_name"
        ]
        changed_desc = "description" in patch and patch["description"] != (
            chart.get("description") or ""
        )
        if not (changed_form or changed_name or changed_desc):
            return None

        payload: dict[str, Any] = {
            "params": json.dumps(form_data, ensure_ascii=False),
            # query_context кэширует старый form_data; пустая строка
            # заставляет Superset пересобрать запрос из params.
            "query_context": "",
        }
        if "slice_name" in patch:
            payload["slice_name"] = patch["slice_name"]
        if "description" in patch:
            payload["description"] = patch["description"]
        if "viz_type" in patch:
            payload["viz_type"] = patch["viz_type"]
        if "dataset" in patch:
            payload["datasource_id"] = self.datasets[patch["dataset"]]["id"]
            payload["datasource_type"] = "table"
        return payload

    # --- новые чарты ---------------------------------------------------

    def create_big_numbers(self, specs: list[dict[str, Any]]) -> dict[str, int]:
        existing = {c["slice_name"]: c["id"] for c in self._all_charts()}
        created: dict[str, int] = {}
        for spec in specs:
            if spec["name"] in existing:
                created[spec["name"]] = existing[spec["name"]]
                continue
            dataset = self.datasets.get(spec["dataset"])
            if not dataset:
                print(f"  ! витрина {spec['dataset']} не найдена — пропуск KPI")
                continue
            filters: list[Any] = []
            for expression, subject in spec["filters"]:
                filters = ensure_filter(filters, expression, subject)
            form_data = {
                "viz_type": "big_number_total",
                "datasource": f"{dataset['id']}__table",
                "metric": spec["metric"],
                "y_axis_format": spec["format"],
                "subheader": spec["subheader"],
                "adhoc_filters": filters,
            }
            self.log(f"P2-02 новый чарт «{spec['name']}»")
            if self.apply:
                result = self.client.post(
                    "/api/v1/chart/",
                    {
                        "slice_name": spec["name"],
                        "viz_type": "big_number_total",
                        "datasource_id": dataset["id"],
                        "datasource_type": "table",
                        "params": json.dumps(form_data, ensure_ascii=False),
                        "dashboards": [self.dashboard_id],
                    },
                )
                created[spec["name"]] = result["id"]
        return created

    def _all_charts(self) -> list[dict[str, Any]]:
        query = json.dumps({"columns": ["id", "slice_name"], "page_size": 100})
        return self.client.get(f"/api/v1/chart/?q={query}")["result"]

    # --- дашборд -------------------------------------------------------

    def patch_dashboard(self, phases: set[str], kpi_ids: dict[str, int]) -> None:
        detail = self.client.get(f"/api/v1/dashboard/{self.dashboard_id}")["result"]
        position = json.loads(detail.get("position_json") or "{}")
        metadata = json.loads(detail.get("json_metadata") or "{}")
        before = (
            json.dumps(position, sort_keys=True),
            json.dumps(metadata, sort_keys=True),
        )

        # Синхронизация подписей нужна после любой фазы, которая
        # переименовывает чарты, — а это и p0, и p1.
        self._sync_slice_names(position)
        if "p1" in phases:
            self._rename_tabs(position)
        if "p2" in phases:
            self._native_filters(metadata)
            self._dashboard_look(metadata)
            self._kpi_row(position, kpi_ids)
            self._dq_row(position, kpi_ids)
            self._drop_overview_duplicates(position)

        after = (
            json.dumps(position, sort_keys=True),
            json.dumps(metadata, sort_keys=True),
        )
        if before == after:
            return
        self.log("Обновление раскладки и метаданных дашборда")
        if self.apply:
            self.client.put(
                f"/api/v1/dashboard/{self.dashboard_id}",
                {
                    "position_json": json.dumps(position, ensure_ascii=False),
                    "json_metadata": json.dumps(metadata, ensure_ascii=False),
                },
            )

    @staticmethod
    def _sync_slice_names(position: dict[str, Any]) -> None:
        """Подтягивает подписи чартов в раскладке за их новыми именами.

        Раскладка хранит собственную копию имени и показывает её, а не
        имя чарта, — без этой синхронизации переименование видно только
        в списке чартов, но не на дашборде. Копий две: sliceName и
        sliceNameOverride, причём показывается вторая, если она задана,
        так что обновлять надо обе.

        Осмысленные ручные подписи («Заказы по дням» на обзоре) не
        трогаются: заменяется лишь то, что дословно совпало со старым
        именем чарта.
        """
        renamed = {
            old: patch["slice_name"]
            for old, patch in chart_patches().items()
            if "slice_name" in patch
        }
        for node in position.values():
            if not isinstance(node, dict) or node.get("type") != "CHART":
                continue
            meta = node.get("meta", {})
            for field in ("sliceName", "sliceNameOverride"):
                if meta.get(field) in renamed:
                    meta[field] = renamed[meta[field]]

    def _rename_tabs(self, position: dict[str, Any]) -> None:
        for tab_id, title in TAB_NAMES.items():
            node = position.get(tab_id)
            if node and node.get("meta", {}).get("text") != title:
                node["meta"]["text"] = title

    def _dashboard_look(self, metadata: dict[str, Any]) -> None:
        # P2-08 и P1-08: единая палитра плюс фиксированные цвета перцентилей.
        metadata["color_scheme"] = "supersetColors"
        metadata["refresh_frequency"] = 3600
        label_colors = dict(metadata.get("label_colors") or {})
        label_colors.update({"p50": "#8FD3E8", "p90": "#3AA6C4", "p99": "#1B6E87"})
        metadata["label_colors"] = label_colors

    def _native_filters(self, metadata: dict[str, Any]) -> None:
        # P2-01. Фильтр периода применяется к собственной временной колонке
        # каждого чарта — у них у всех уже стоит TEMPORAL_RANGE «No filter».
        filters = list(metadata.get("native_filter_configuration") or [])
        existing = {f.get("id") for f in filters}
        if "NATIVE_FILTER-period" not in existing:
            filters.append(
                {
                    "id": "NATIVE_FILTER-period",
                    "name": "Период",
                    "filterType": "filter_time",
                    "type": "NATIVE_FILTER",
                    "targets": [{}],
                    "defaultDataMask": {
                        "extraFormData": {},
                        "filterState": {"value": "No filter"},
                        "ownState": {},
                    },
                    "controlValues": {},
                    "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    "description": "Применяется к временной колонке каждого чарта",
                }
            )
        dim_dataset = self.datasets.get("orders_with_customer_dim")
        if dim_dataset:
            for key, column, title in (
                ("city", "city", "Город"),
                ("channel", "acquisition_channel_ru", "Канал привлечения"),
            ):
                filter_id = f"NATIVE_FILTER-{key}"
                if filter_id in existing:
                    continue
                filters.append(
                    {
                        "id": filter_id,
                        "name": title,
                        "filterType": "filter_select",
                        "type": "NATIVE_FILTER",
                        "targets": [
                            {
                                "datasetId": dim_dataset["id"],
                                "column": {"name": column},
                            }
                        ],
                        "defaultDataMask": {
                            "extraFormData": {},
                            "filterState": {},
                            "ownState": {},
                        },
                        "controlValues": {
                            "multiSelect": True,
                            "enableEmptyFilter": False,
                            "searchAllOptions": False,
                            "inverseSelection": False,
                        },
                        "cascadeParentIds": [],
                        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                    }
                )
        metadata["native_filter_configuration"] = filters

    def _kpi_row(self, position: dict[str, Any], kpi_ids: dict[str, int]) -> None:
        ids = [kpi_ids.get(spec["name"]) for spec in KPI_CHARTS]
        if not all(ids) or "ROW-ov-kpi" in position:
            return
        self._insert_row(
            position,
            tab_id="TAB-overview",
            row_id="ROW-ov-kpi",
            at_index=0,
            charts=[
                (f"CHART-kpi-{index}", spec["name"], chart_id, 2, 30)
                for index, (spec, chart_id) in enumerate(
                    zip(KPI_CHARTS, ids, strict=True)
                )
            ],
        )

    def _dq_row(self, position: dict[str, Any], kpi_ids: dict[str, int]) -> None:
        chart_id = kpi_ids.get("Часов с последней загрузки")
        if not chart_id or "ROW-dq-freshness" in position:
            return
        self._insert_row(
            position,
            tab_id="TAB-dq",
            row_id="ROW-dq-freshness",
            at_index=0,
            charts=[
                ("CHART-dq-freshness", "Часов с последней загрузки", chart_id, 4, 30)
            ],
        )

    @staticmethod
    def _insert_row(
        position: dict[str, Any],
        tab_id: str,
        row_id: str,
        at_index: int,
        charts: list[tuple[str, str, int, int, int]],
    ) -> None:
        tab = position.get(tab_id)
        if not tab:
            return
        row_parents = [*tab["parents"], tab_id]
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [key for key, *_ in charts],
            "parents": row_parents,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for key, name, chart_id, width, height in charts:
            position[key] = {
                "type": "CHART",
                "id": key,
                "children": [],
                "parents": [*row_parents, row_id],
                "meta": {
                    "chartId": chart_id,
                    "sliceName": name,
                    "width": width,
                    "height": height,
                },
            }
        tab["children"].insert(at_index, row_id)

    @staticmethod
    def _drop_overview_duplicates(position: dict[str, Any]) -> None:
        for key in OVERVIEW_DUPLICATES:
            node = position.pop(key, None)
            if not node:
                continue
            parent_id = node["parents"][-1]
            parent = position.get(parent_id)
            if parent and key in parent.get("children", []):
                parent["children"].remove(key)
            if parent and not parent["children"]:
                grandparent = position.get(parent["parents"][-1])
                if grandparent and parent_id in grandparent.get("children", []):
                    grandparent["children"].remove(parent_id)
                position.pop(parent_id, None)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phases",
        default="p0",
        help="через запятую: p0, p1, p2 (по умолчанию p0)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="писать изменения (без флага печатается только план)",
    )
    parser.add_argument("--dashboard", type=int, default=1, help="id дашборда")
    parser.add_argument(
        "--diagnose",
        action="store_true",
        help="показать, что API отдаёт по датасетам, и выйти",
    )
    parser.add_argument(
        "--public-role",
        default="Public",
        help="роль, которой выдать доступ к витринам (пусто — не трогать)",
    )
    parser.add_argument("--url", default=os.environ.get("SUPERSET_URL"))
    parser.add_argument("--username", default=os.environ.get("SUPERSET_USERNAME"))
    parser.add_argument("--password", default=os.environ.get("SUPERSET_PASSWORD"))
    args = parser.parse_args()

    if not (args.url and args.username):
        return _fail(
            "Нужны SUPERSET_URL и SUPERSET_USERNAME (или --url/--username)"
        )

    # Пароль в командной строке оседает в истории оболочки и виден в
    # списке процессов, поэтому по умолчанию спрашиваем его скрытым
    # вводом. --password и SUPERSET_PASSWORD остаются для CI, где
    # интерактивного ввода нет.
    password = args.password
    if not password:
        # Проверяются оба потока: на Windows stdin.isatty() возвращает
        # True даже при перенаправленном вводе, и на одном stdin скрипт
        # молча вис бы на приглашении вместо внятной ошибки.
        if not (sys.stdin.isatty() and sys.stdout.isatty()):
            return _fail(
                "Пароль не задан, а спросить его негде (ввод или вывод "
                "перенаправлены): передай SUPERSET_PASSWORD или --password"
            )
        try:
            password = getpass.getpass(f"Пароль {args.username} в Superset: ")
        except (EOFError, KeyboardInterrupt):
            return _fail("Ввод пароля прерван")
    if not password:
        return _fail("Пустой пароль")

    phases = {p.strip().lower() for p in args.phases.split(",") if p.strip()}
    unknown = phases - {"p0", "p1", "p2"}
    if unknown:
        return _fail(f"Неизвестные фазы: {', '.join(sorted(unknown))}")

    client = Superset(args.url, args.username, password)

    if args.diagnose:
        return diagnose(client, args.username, password)

    if args.apply:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup = client.export_dashboard(
            args.dashboard, f"superset_backup_{stamp}.zip"
        )
        print(f"Бэкап дашборда: {backup}\n")

    mode = "ПРИМЕНЕНИЕ" if args.apply else "ПЛАН (ничего не пишется)"
    print(f"{mode} · фазы: {', '.join(sorted(phases))}\n")

    runner = Runner(client, args.dashboard, args.apply)

    print("Витрины:")
    runner.sync_datasets(phases)

    print("\nЧарты:")
    runner.patch_charts(phases)

    kpi_ids: dict[str, int] = {}
    if "p2" in phases:
        print("\nНовые чарты:")
        kpi_ids = runner.create_big_numbers(KPI_CHARTS + DQ_CHARTS)

    print("\nДашборд:")
    runner.patch_dashboard(phases, kpi_ids)

    if args.public_role:
        print("\nПрава:")
        runner.grant_public_access(args.public_role)

    print(f"\nИтого изменений: {len(runner.planned)}")
    if not args.apply and runner.planned:
        print("Повтори с --apply, чтобы записать.")
    return 0


def diagnose(client: Superset, username: str, password: str) -> int:
    """Показывает, что API отдаёт по датасетам и кто мы для Superset.

    Нужен, когда интерфейс показывает одно, а скрипт видит другое:
    разницу между `count` и длиной `result` невозможно объяснить, не
    увидев обе цифры.
    """
    try:
        me = client.get("/api/v1/me/")["result"]
        print(f"Пользователь: {me.get('username')} ({me.get('email')})")
        print(f"  поля /api/v1/me/: {', '.join(sorted(me))}")
        if me.get("roles"):
            print(f"  роли: {me['roles']}")
    except SupersetError as exc:
        print(f"Пользователь: не удалось узнать — {exc}")

    # Ключевой вопрос: есть ли у ролей all_datasource_access. Без него
    # Superset отдаёт только датасеты, выданные поимённо или по схеме, —
    # ровно то, что мы и наблюдаем.
    try:
        roles = client.get("/api/v1/security/roles/?q=" + json.dumps(
            {"page_size": 100}
        ))["result"]
        print(f"\nРолей в инстансе: {len(roles)}")
        for role in roles:
            perms = client.role_permission_ids(role["id"])
            print(f"  {role['name']}: разрешений {len(perms)}")
    except SupersetError as exc:
        print(f"Роли: не удалось прочитать — {exc}")

    try:
        wide, page, total = [], 0, 0
        while page < 100:
            rows = client.get(
                "/api/v1/security/permissions-resources/?q="
                + json.dumps({"page_size": 100, "page": page})
            )["result"]
            if not rows:
                break
            total += len(rows)
            wide += [
                row
                for row in rows
                if (row.get("permission") or {}).get("name")
                in {"all_datasource_access", "all_database_access"}
            ]
            page += 1
        print(f"Разрешений всего: {total}, широкого доступа: {len(wide)}")
    except SupersetError as exc:
        print(f"Широкий доступ: не удалось прочитать — {exc}")

    variants = {
        "как в скрипте": json.dumps(
            {
                "columns": ["id", "table_name", "sql", "schema", "kind"],
                "page_size": 100,
            }
        ),
        "только page_size": json.dumps({"page_size": 100}),
        "без q": None,
    }
    for label, query in variants.items():
        path = "/api/v1/dataset/" + (f"?q={query}" if query else "")
        try:
            payload = client.get(path)
        except SupersetError as exc:
            print(f"  {label:18} ошибка: {exc}")
            continue
        rows = payload.get("result") or []
        print(
            f"  {label:18} count={payload.get('count')} "
            f"строк в ответе={len(rows)}"
        )

    # Интерфейс ходит с сессионной кукой, скрипт — с JWT-токеном. Если
    # ответы различаются, дело не в правах роли, а в способе входа.
    try:
        fresh = requests.Session()
        page_html = fresh.get(client.base + "/login/", timeout=30).text
        token = re.search(
            'name="csrf_token"[^>]*value="([^"]+)"', page_html
        )
        fresh.post(
            client.base + "/login/",
            data={
                "username": username,
                "password": password,
                "csrf_token": token.group(1) if token else "",
            },
            timeout=30,
        )
        by_cookie = fresh.get(
            client.base + "/api/v1/dataset/",
            headers={"Accept": "application/json"},
            timeout=30,
        )
        if by_cookie.status_code == 200:
            body = by_cookie.json()
            print(
                "Тот же запрос с сессионной кукой: "
                f"count={body.get('count')} "
                f"строк={len(body.get('result') or [])}"
            )
        else:
            print(f"Сессионный вход не удался: HTTP {by_cookie.status_code}")
    except Exception as exc:  # noqa: BLE001 - диагностика не должна падать
        print(f"Сравнение с кукой не получилось: {exc}")

    print("\nПостранично, без выбора колонок:")
    seen: list[str] = []
    for page in range(20):
        query = json.dumps({"page_size": 100, "page": page})
        rows = client.get(f"/api/v1/dataset/?q={query}").get("result") or []
        if not rows:
            break
        seen.extend(f"{r.get('schema')}.{r.get('table_name')}" for r in rows)
    print(f"  всего собрано: {len(seen)}")
    for name in sorted(seen):
        print(f"    {name}")
    return 0


def _fail(message: str) -> int:
    print(f"Ошибка: {message}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    # Ошибки API — это нормальный исход (не тот пароль, недоступный
    # сервер), а не дефект скрипта: сообщение полезнее трейсбека.
    try:
        exit_code = main()
    except SupersetError as exc:
        exit_code = _fail(str(exc))
    raise SystemExit(exit_code)
