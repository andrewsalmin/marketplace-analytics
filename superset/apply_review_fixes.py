#!/usr/bin/env python3
"""Применение правок ревью дашборда «Аналитика маркетплейса» к Superset.

Скрипт правит витрины, чарты и метаданные дашборда через REST API. Он
идемпотентен: повторный запуск приводит инстанс в то же состояние, а не
накапливает изменения.

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
import re
import sys
from datetime import datetime, timezone
from typing import Any

try:
    import requests
except ImportError:  # pragma: no cover - подсказка вместо трейсбека
    sys.exit("Нужен requests: pip install requests")


# ---------------------------------------------------------------------------
# Общие SQL-фрагменты
# ---------------------------------------------------------------------------

ORDERS = "marketplace_analytics.orders FINAL"

# P0-03. Заказ считается «зрелым», если с его создания прошло больше недели:
# только тогда он успел отмениться, вернуться или доехать. Отсчёт от
# максимума в данных, а не от now(), чтобы пайплайн можно было не трогать.
MATURE_ORDERS = (
    f"created_at < (SELECT max(created_at) - INTERVAL 7 DAY FROM {ORDERS})"
)

# P0-04/P0-05. Клиент «зрелый» через 30 дней после регистрации.
MATURE_REGISTRATION = (
    "registration_date < "
    f"(SELECT max(created_at) - INTERVAL 30 DAY FROM {ORDERS})"
)

PAID = "paid_at IS NOT NULL"
NOT_CANCELLED = "cancelled_at IS NULL AND refunded_at IS NULL"

CHANNEL_RU_CASE = """CASE {col}
        WHEN 'telegram_ads'  THEN 'Реклама в Telegram'
        WHEN 'social'        THEN 'Соцсети'
        WHEN 'organic'       THEN 'Органика'
        WHEN 'vk_ads'        THEN 'Реклама ВКонтакте'
        WHEN 'email'         THEN 'Email-рассылка'
        WHEN 'referral'      THEN 'Рефералы'
        WHEN 'yandex_direct' THEN 'Яндекс.Директ'
        ELSE {col}
    END"""

# P2-06. Словари, которые до этого дублировались inline в form_data чартов.
ORDER_STATUS_RU_CASE = """CASE status
        WHEN 'new'              THEN 'Новый'
        WHEN 'paid'             THEN 'Оплачен'
        WHEN 'shipped'          THEN 'Отправлен'
        WHEN 'ready_for_pickup' THEN 'Готов к выдаче'
        WHEN 'delivered'        THEN 'Доставлен'
        WHEN 'cancelled'        THEN 'Отменён'
        WHEN 'refunded'         THEN 'Возврат средств'
        WHEN 'returned'         THEN 'Возврат товара'
        ELSE status
    END"""

PAYMENT_STATUS_RU_CASE = """CASE status
        WHEN 'success'  THEN 'Успешно'
        WHEN 'refunded' THEN 'Возврат'
        WHEN 'failed'   THEN 'Отклонён'
        ELSE status
    END"""

PAYMENT_METHOD_RU_CASE = """CASE payment_method
        WHEN 'card'    THEN 'Карта'
        WHEN 'cash'    THEN 'Наличные'
        WHEN 'sbp'     THEN 'СБП'
        WHEN 'mir_pay' THEN 'Mir Pay'
        ELSE payment_method
    END"""


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

DATASET_SQL: dict[str, dict[str, str]] = {
    # P0-01. customers и payments были physical — без дедупликации
    # ReplacingMergeTree. Платёж после рефанда переиздаётся под тем же
    # payment_id, поэтому без FINAL он считался дважды.
    "customers": {
        "phase": "p0",
        "note": "P0-01 FINAL для customers + P2-06 словарь каналов",
        "sql": f"""
SELECT
    *,
    {CHANNEL_RU_CASE.format(col="acquisition_channel")} AS acquisition_channel_ru
FROM marketplace_analytics.customers FINAL
""".strip(),
    },
    "payments": {
        "phase": "p0",
        "note": "P0-01 FINAL для payments + P2-06 словари статуса и способа",
        "sql": f"""
SELECT
    *,
    {PAYMENT_STATUS_RU_CASE} AS status_ru,
    {PAYMENT_METHOD_RU_CASE} AS payment_method_ru
FROM marketplace_analytics.payments FINAL
""".strip(),
    },
    # P2-06. Словарь статусов переезжает в витрину: до этого один и тот же
    # CASE был переписан заново в form_data каждого чарта, и версии уже
    # разошлись между собой. Фаза p0, потому что чарты фазы p0 уже
    # ссылаются на status_ru — колонка должна появиться раньше них.
    "orders": {
        "phase": "p0",
        "note": "P2-06 словарь статусов в витрине заказов",
        "sql": f"""
SELECT
    *,
    {ORDER_STATUS_RU_CASE} AS status_ru
FROM marketplace_analytics.orders FINAL
""".strip(),
    },
    # P0-03. Воронка считалась по всем заказам, включая недельной свежести,
    # которые физически не успели доехать.
    "order_funnel_stages": {
        "phase": "p0",
        "note": "P0-03 воронка только по зрелым заказам",
        "sql": f"""
SELECT 'Создано' AS stage, 1 AS stage_order, count() AS cnt
FROM {ORDERS} WHERE {MATURE_ORDERS}
UNION ALL
SELECT 'Оплачено', 2, countIf(paid_at IS NOT NULL)
FROM {ORDERS} WHERE {MATURE_ORDERS}
UNION ALL
SELECT 'Отправлено', 3, countIf(shipped_at IS NOT NULL)
FROM {ORDERS} WHERE {MATURE_ORDERS}
UNION ALL
SELECT 'Готово к выдаче', 4, countIf(ready_for_pickup_at IS NOT NULL)
FROM {ORDERS} WHERE {MATURE_ORDERS}
UNION ALL
SELECT 'Доставлено', 5, countIf(delivered_at IS NOT NULL)
FROM {ORDERS} WHERE {MATURE_ORDERS}
""".strip(),
    },
    # P0-02 + P1-11. Витрина не отдавала признаков оплаты, поэтому GMV по
    # городам и каналам считался по всем заказам; канал отдавался сырым
    # enum'ом, хотя рядом на вкладке пирог показывал русские подписи.
    "orders_with_customer_dim": {
        "phase": "p0",
        "note": "P0-02 признаки оплаты + P1-11 русский канал",
        "sql": f"""
SELECT
    o.order_id            AS order_id,
    o.created_at          AS created_at,
    o.amount_kopecks      AS amount_kopecks,
    o.status              AS status,
    o.paid_at             AS paid_at,
    o.cancelled_at        AS cancelled_at,
    o.refunded_at         AS refunded_at,
    c.city                AS city,
    c.acquisition_channel AS acquisition_channel,
    {CHANNEL_RU_CASE.format(col="c.acquisition_channel")} AS acquisition_channel_ru
FROM marketplace_analytics.orders AS o FINAL
LEFT JOIN marketplace_analytics.customers AS c FINAL
       ON c.customer_id = o.customer_id
""".strip(),
    },
    # P0-05. Признак зрелости клиента, чтобы конверсия считалась по когорте
    # с полным горизонтом наблюдения.
    "customer_first_order": {
        "phase": "p0",
        "note": "P0-05 признак зрелой когорты",
        "sql": f"""
SELECT
    c.customer_id       AS customer_id,
    c.registration_date AS registration_date,
    min(o.created_at)   AS first_order_at,
    dateDiff('day', c.registration_date, min(o.created_at)) AS days_to_first_order,
    c.registration_date < (SELECT max(created_at) - INTERVAL 30 DAY FROM {ORDERS})
        AS is_mature
FROM marketplace_analytics.customers AS c FINAL
LEFT JOIN marketplace_analytics.orders AS o FINAL
       ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.registration_date
""".strip(),
    },
    # P2-04. Разбивка DQ-ошибок была всевременной и не фильтровалась периодом.
    "dq_reason_breakdown": {
        "phase": "p2",
        "note": "P2-04 load_date в разбивку DQ-ошибок",
        "sql": """
SELECT
  CASE reason
    WHEN 'CUSTOMER_NOT_FOUND' THEN 'Покупатель не найден'
    WHEN 'CANCELLATION_REASON_INCONSISTENT'
        THEN 'Причина отмены не соответствует статусу'
    WHEN 'ORDER_NOT_FOUND' THEN 'Заказ не найден'
    WHEN 'REFUND_TIMELINE_INVALID' THEN 'Некорректные сроки возврата'
    WHEN 'CUSTOMER_ID_EMPTY' THEN 'Пустой ID покупателя'
    WHEN 'ORDER_AMOUNT_INVALID' THEN 'Некорректная сумма заказа'
    WHEN 'PAYMENT_AMOUNT_INVALID' THEN 'Некорректная сумма платежа'
    WHEN 'ORDER_STATUS_INVALID' THEN 'Некорректный статус заказа'
    WHEN 'PAYMENT_ID_DUPLICATE_IN_LOAD' THEN 'Дубликат ID платежа в загрузке'
    WHEN 'PAYMENT_STATUS_INVALID' THEN 'Некорректный статус платежа'
    WHEN 'PAYMENT_BEFORE_ORDER' THEN 'Платёж раньше заказа'
    WHEN 'REGISTRATION_DATE_INVALID' THEN 'Некорректная дата регистрации'
    WHEN 'CUSTOMER_ID_DUPLICATE_IN_LOAD' THEN 'Дубликат ID покупателя в загрузке'
    WHEN 'CUSTOMER_ID_DUPLICATE_IN_HISTORY' THEN 'Дубликат ID покупателя в истории'
    WHEN 'ORDER_ID_DUPLICATE_IN_LOAD' THEN 'Дубликат ID заказа в загрузке'
    ELSE reason
  END AS reason,
  load_date,
  quarantined_rows
FROM (
  SELECT
      arrayJoin(splitByString(' | ', dq_reason)) AS reason,
      load_date,
      count() AS quarantined_rows
  FROM (
      SELECT dq_reason, load_date FROM marketplace_analytics.quarantine_customers
      UNION ALL
      SELECT dq_reason, load_date FROM marketplace_analytics.quarantine_orders
      UNION ALL
      SELECT dq_reason, load_date FROM marketplace_analytics.quarantine_payments
  )
  GROUP BY reason, load_date
)
""".strip(),
    },
}


NEW_DATASETS: dict[str, dict[str, str]] = {
    # P0-04. Замена «среднему по дате регистрации», которое рисовало арку
    # окна наблюдения вместо поведения клиентов.
    "first_order_delay_distribution": {
        "phase": "p0",
        "note": "P0-04 распределение задержки первого заказа",
        "sql": f"""
SELECT days_to_first_order, count() AS customers
FROM (
    SELECT
        c.customer_id AS customer_id,
        dateDiff('day', c.registration_date, min(o.created_at))
            AS days_to_first_order
    FROM marketplace_analytics.customers AS c FINAL
    INNER JOIN marketplace_analytics.orders AS o FINAL
            ON o.customer_id = c.customer_id
    WHERE c.{MATURE_REGISTRATION}
    GROUP BY c.customer_id, c.registration_date
)
WHERE days_to_first_order >= 0
GROUP BY days_to_first_order
ORDER BY days_to_first_order
""".strip(),
    },
    # P0-06. Сырые длительности вместо AVG, чтобы перцентили считались по
    # заказам, а не по дневным средним.
    "refund_processing_times": {
        "phase": "p0",
        "note": "P0-06 сырые длительности обработки рефанда",
        "sql": f"""
SELECT
    order_id,
    refunded_at,
    if(cancelled_at IS NOT NULL, 'Отмена', 'Возврат товара') AS refund_kind,
    dateDiff('second', coalesce(cancelled_at, returned_at), refunded_at)
        AS duration_seconds
FROM {ORDERS}
WHERE refunded_at IS NOT NULL
  AND coalesce(cancelled_at, returned_at) IS NOT NULL
  AND refunded_at >= coalesce(cancelled_at, returned_at)
""".strip(),
    },
    # P2-04. Свежесть пайплайна: _load_commits пишется только после того,
    # как все таблицы за load_date догружены целиком.
    "load_freshness": {
        "phase": "p2",
        "note": "P2-04 свежесть загрузки из _load_commits",
        "sql": """
SELECT
    max(load_date) AS last_load_date,
    dateDiff('hour', max(loaded_at), now()) AS hours_since_load,
    count() AS entities_loaded
FROM marketplace_analytics._load_commits
WHERE load_date = (
    SELECT max(load_date) FROM marketplace_analytics._load_commits
)
""".strip(),
    },
    "quarantine_volume": {
        "phase": "p2",
        "note": "P2-04 объём карантина по дням и сущностям",
        "sql": """
SELECT load_date, 'Покупатели' AS entity, count() AS quarantined_rows
FROM marketplace_analytics.quarantine_customers GROUP BY load_date
UNION ALL
SELECT load_date, 'Заказы', count()
FROM marketplace_analytics.quarantine_orders GROUP BY load_date
UNION ALL
SELECT load_date, 'Платежи', count()
FROM marketplace_analytics.quarantine_payments GROUP BY load_date
""".strip(),
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
            "note": "P0-02 GMV только по оплаченным + P0-03 зрелый хвост",
            "slice_name": "Динамика GMV",
            "description": (
                "«Оплачено» — сумма заказов с paid_at. «Чистый» дополнительно "
                "исключает отменённые и возвращённые. Последняя неделя "
                "отрезана: заказы ещё не успели закрыться."
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
            "filters": [(MATURE_ORDERS, "created_at")],
        },
        "Изменение среднего чека (AOV)": {
            "phase": "p0",
            "note": "P0-02 средний чек по оплаченным заказам",
            "slice_name": "Динамика среднего чека (AOV)",
            "description": "Сумма оплаченных заказов, делённая на их число.",
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                **DATE_AXIS,
                **rolling_mean(),
            },
            "filters": [(MATURE_ORDERS, "created_at")],
        },
        "Изменение долей отмен и возвратов": {
            "phase": "p0",
            "note": "P0-03 зрелый хвост + P1-05 проценты",
            "slice_name": "Динамика долей отмен и возвратов",
            "description": (
                "Доли считаются от числа созданных заказов. Последняя неделя "
                "отрезана: свежий заказ ещё не успел отмениться или вернуться."
            ),
            "set": {"y_axis_format": ".1%", **DATE_AXIS, **rolling_mean()},
            "filters": [(MATURE_ORDERS, "created_at")],
        },
        "Изменение структуры статусов заказов": {
            "phase": "p0",
            "note": "P0-03 зрелый хвост",
            "slice_name": "Динамика структуры статусов заказов",
            "set": {"groupby": ["status_ru"], **DATE_AXIS},
            "filters": [(MATURE_ORDERS, "created_at")],
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
            "note": "P0-05 только зрелые когорты",
            "description": (
                "Доля клиентов, сделавших хотя бы один заказ, по когортам "
                "старше 30 дней. Часть регистраций не конвертируется никогда "
                "(--never-order-rate в generate_data.py), доля зависит от "
                "канала привлечения."
            ),
            "set": {"y_axis_format": ".1%"},
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
            "filters": [(MATURE_ORDERS, "created_at")],
        },
        "GMV по городам (топ-10)": {
            "phase": "p0",
            "note": "P0-07 сортировка по метрике + P0-02 оплаченные заказы",
            "set": {
                "metrics": [metric("GMV, ₽", GMV_PAID)],
                **sort_by_first_metric("GMV, ₽"),
            },
            "filters": [(MATURE_ORDERS, "created_at")],
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
            "filters": [(MATURE_ORDERS, "created_at")],
        },
        "Средний чек по каналам привлечения": {
            "phase": "p0",
            "note": "P0-02 оплаченные заказы + P0-07 сортировка + P1-11 язык",
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                "x_axis": "acquisition_channel_ru",
                **sort_by_first_metric("Средний чек, ₽"),
            },
            "filters": [(MATURE_ORDERS, "created_at")],
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
            "note": "P1-05 проценты, P1-06 границы оси, P1-07 сглаживание",
            "slice_name": "Динамика success rate оплаты",
            "description": (
                "Доля success среди завершённых попыток (success + failed). "
                "Скользящее среднее за 7 дней."
            ),
            "set": {
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
            "phase": "p2",
            "note": "P2-05 описание логарифмической шкалы",
            "description": (
                "Сколько заказов потребовали N попыток оплаты. Ось Y "
                "логарифмическая — иначе хвост не виден."
            ),
            "set": {"x_axis_sort": "retries", "x_axis_sort_asc": True},
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
        "dataset": "orders",
        "metric": metric("GMV, ₽", GMV_PAID),
        "format": "SMART_NUMBER",
        "subheader": "Оплаченные заказы за период",
        "filters": [(MATURE_ORDERS, "created_at")],
    },
    {
        "name": "KPI · Заказы",
        "dataset": "orders",
        "metric": metric("Заказов", "count()"),
        "format": "SMART_NUMBER",
        "subheader": "Создано заказов",
        "filters": [(MATURE_ORDERS, "created_at")],
    },
    {
        "name": "KPI · Средний чек",
        "dataset": "orders",
        "metric": metric("Средний чек, ₽", AOV_PAID),
        "format": ",.0f",
        "subheader": "На оплаченный заказ",
        "filters": [(MATURE_ORDERS, "created_at")],
    },
    {
        "name": "KPI · Доля отмен",
        "dataset": "orders",
        "metric": metric("Доля отмен", "countIf(cancelled_at IS NOT NULL) / count()"),
        "format": ".1%",
        "subheader": "От числа созданных заказов",
        "filters": [(MATURE_ORDERS, "created_at")],
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

    def patch_datasets(self, phases: set[str]) -> None:
        for name, spec in DATASET_SQL.items():
            if spec["phase"] not in phases:
                continue
            existing = self.datasets.get(name)
            if not existing:
                print(f"  ! витрина {name} не найдена — пропуск")
                continue
            if " ".join(str(existing.get("sql") or "").split()) == " ".join(
                spec["sql"].split()
            ):
                continue
            self.log(f"{spec['note']}: витрина {name}")
            if self.apply:
                self.client.put(
                    f"/api/v1/dataset/{existing['id']}", {"sql": spec["sql"]}
                )
                self.client.put(f"/api/v1/dataset/{existing['id']}/refresh", {})

    def create_datasets(self, phases: set[str]) -> None:
        database_id = self._database_id()
        for name, spec in NEW_DATASETS.items():
            if spec["phase"] not in phases:
                continue
            if name in self.datasets:
                continue
            self.log(f"{spec['note']}: новая витрина {name}")
            if self.apply:
                self.client.post(
                    "/api/v1/dataset/",
                    {
                        "database": database_id,
                        "schema": "marketplace_analytics",
                        "table_name": name,
                        "sql": spec["sql"],
                    },
                )
                self.datasets = self.client.datasets()

    def _database_id(self) -> int:
        orders = self.datasets.get("orders")
        if not orders:
            raise SupersetError("Не найдена витрина orders — не от чего оттолкнуться")
        detail = self.client.get(f"/api/v1/dataset/{orders['id']}")["result"]
        return detail["database"]["id"]

    # --- чарты ---------------------------------------------------------

    def patch_charts(self, phases: set[str]) -> None:
        charts = self.client.charts(self.dashboard_id)
        for name, patch in chart_patches().items():
            if patch["phase"] not in phases:
                continue
            chart = charts.get(name)
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
    runner.create_datasets(phases)
    runner.patch_datasets(phases)

    print("\nЧарты:")
    runner.patch_charts(phases)

    kpi_ids: dict[str, int] = {}
    if "p2" in phases:
        print("\nНовые чарты:")
        kpi_ids = runner.create_big_numbers(KPI_CHARTS + DQ_CHARTS)

    print("\nДашборд:")
    runner.patch_dashboard(phases, kpi_ids)

    print(f"\nИтого изменений: {len(runner.planned)}")
    if not args.apply and runner.planned:
        print("Повтори с --apply, чтобы записать.")
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
