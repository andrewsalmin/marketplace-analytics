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
import hashlib
import json
import os
import pathlib
import re
import sys
from datetime import datetime, timezone
from typing import Any

from growth import maturity_days, payment_settlement_days

try:
    import requests
except ImportError as exc:  # pragma: no cover - подсказка вместо голого ImportError
    # raise, а не sys.exit: SystemExit на уровне модуля убивает не
    # скрипт, а того, кто его импортирует. Тесты грузят этот файл через
    # importlib, и выход отсюда ронял весь прогон целиком — 178 тестов
    # не запускались вовсе, с сообщением «no tests ran».
    raise ImportError(
        "Нужен requests: pip install -r requirements-superset.txt"
    ) from exc


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
PAYMENT_SETTLEMENT_DAYS = payment_settlement_days()

# Раньше отсечка незрелых заказов жила прямо в фильтре чарта подзапросом.
# Superset такие фильтры не исполняет (ALLOW_ADHOC_SUBQUERY=False) и
# рисует «Custom SQL fields cannot contain sub-queries» вместо графика.
# Строка нужна, чтобы вычистить фильтр из чартов, где он ещё остался.
OBSOLETE_FILTERS = [
    "created_at < (SELECT max(created_at) - INTERVAL "
    f"{MATURITY_DAYS} DAY FROM marketplace_analytics.orders FINAL)"
]

# 6.6. Ровно то, что нужно анонимному посетителю, чтобы открыть дашборд
# и пользоваться фильтрами. Список получен не из документации, а из
# наблюдения: какие запросы делает браузер при загрузке дашборда под
# анонимной сессией.
#
# Каждая строка — с причиной, потому что снимать права опаснее, чем
# выдавать: лишнее право это риск, недостающее — пустой экран.
PUBLIC_REQUIRED_PERMISSIONS = [
    # Собственно страница дашборда.
    ("can_dashboard", "Superset"),
    # Список чартов и раскладка: GET /api/v1/dashboard/{id}.
    ("can_read", "Dashboard"),
    # Данные чартов: POST /api/v1/chart/data.
    ("can_read", "Chart"),
    # Диалог периода превращает «Last week» в конкретные даты запросом
    # к /api/v1/time_range/; без доступа туда он получает 401, кнопка
    # APPLY гаснет, и период не выбрать вовсе.
    #
    # Ресурс называется Api, а не TimeRangeRestApi: несколько мелких
    # эндпоинтов зарегистрированы под общим именем. Такие пары надо
    # смотреть выгрузкой прав (--diagnose), а не угадывать по имени
    # класса. Право узкое — разбирает выражение дат и данных не отдаёт;
    # соседние can_query и can_query_form_data на том же ресурсе анониму
    # сознательно не выдаются.
    ("can_time_range", "Api"),
    # Словарь переводов. Без него анонимный посетитель получает вместо
    # JSON страницу логина, разбор падает, и не рисуется ни один чарт —
    # при том, что запросы данных отвечают 200. Дашборд выглядит пустым
    # без единой ошибки на экране; видно только в консоли браузера.
    ("can_language_pack", "Superset"),
]

# Права, которые у роли есть, но дашборду не нужны, — и почему их не
# должно быть у анонима:
#
#   can_read on Dataset          — GET /api/v1/dataset/ отдаёт описания
#                                  датасетов вместе с SQL виртуальных
#   can_explore_json on Superset — legacy-эндпоинт данных; дашборд
#                                  ходит в /api/v1/chart/data
#   can_get on Datasource        — служебное для конструктора чартов
#   can_external_metadata on ... — то же, читает метаданные источника
#   schema_access on [...].[marketplace_analytics]
#                                — ковровый доступ ко всей сырой схеме,
#                                  включая карантин и _load_commits

# Горизонт зависит от того, что метрика измеряет, и путать их дорого.
#
# SETTLED — оплачен заказ или нет; ясно на следующий день. Этим отсекают
# GMV и средний чек.
#
# MATURE — исход заказа целиком: отменён, возвращён, доставлен. Этим
# отсекают доли отмен и возвратов, чистый GMV, структуру статусов и
# воронку.
#
# Счётчики созданных заказов и регистраций не отсекаются вовсе: событие
# уже произошло, ждать нечего. Раньше на всём стоял один MATURE, и
# карточка «создано заказов» показывала 76 тысяч там, где график рядом
# рисовал 132 — без единого слова о том, почему.
MATURE_FILTER = ("is_mature = 1", "is_mature")
SETTLED_FILTER = ("is_payment_settled = 1", "is_payment_settled")

PAID = "paid_at IS NOT NULL"
# «Чистый» — заказ, который дошёл до покупателя и остался у него:
# ни отмены, ни возврата товара, ни возврата денег. Возврат товара
# (returned_at) раньше в условие не входил, и 1 749 возвращённых
# заказов на 3,53 млн ₽ считались чистой выручкой.
NOT_CANCELLED = (
    "cancelled_at IS NULL AND returned_at IS NULL AND refunded_at IS NULL"
)


CREDENTIALS_PATH = pathlib.Path.home() / ".superset" / "credentials.json"


def load_credentials(path: pathlib.Path) -> dict[str, str]:
    """Читает url/username/password из файла, если он есть.

    Существует затем, чтобы скрипт можно было запускать без участия
    человека, не передавая пароль ни аргументом, ни переменной
    окружения: аргументы видны в списке процессов, окружение — в
    /proc и в дампах. Тот же приём, что у clickhouse-client с его
    ~/.clickhouse-client/config.xml.
    """
    if not path.exists():
        return {}

    # Файл с паролем, доступный кому-то ещё, — это не защита, а её
    # видимость. Лучше отказаться, чем молча воспользоваться.
    mode = path.stat().st_mode
    if mode & 0o077:
        raise SupersetError(
            f"{path} доступен не только владельцу (права {mode & 0o777:o}). "
            "Выполни chmod 600 и повтори."
        )

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except ValueError as exc:
        raise SupersetError(f"{path}: не разбирается как JSON — {exc}") from exc

    return {k: str(v) for k, v in data.items() if k in {"url", "username", "password"}}


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
        """Вход сессионной кукой, как это делает веб-интерфейс.

        Не JWT, хотя у Superset есть /api/v1/security/login и он проще.
        При входе по токену список датасетов приходит урезанным: под
        одной и той же учёткой /api/v1/dataset/ отдаёт 6 записей по
        токену и 20 по куке. Роли из токена разворачиваются не полностью,
        проверка all_datasource_access не срабатывает, и остаются только
        датасеты с явно выданным доступом. Скрипт из-за этого считал
        переехавшие витрины несуществующими и шёл создавать дубликаты.
        """
        page = self.session.get(f"{self.base}/login/", timeout=30)
        if page.status_code != 200:
            raise SupersetError(
                f"Не открылась форма входа: HTTP {page.status_code}"
            )
        token = re.search('name="csrf_token"[^>]*value="([^"]+)"', page.text)

        resp = self.session.post(
            f"{self.base}/login/",
            data={
                "username": username,
                "password": password,
                "csrf_token": token.group(1) if token else "",
            },
            timeout=30,
        )
        if resp.status_code >= 400:
            raise SupersetError(f"Логин не прошёл: HTTP {resp.status_code}")

        # Форма отвечает 200 и на неверный пароль — она просто снова
        # рисует себя. Единственный надёжный признак входа — что API
        # начал отвечать от имени пользователя.
        me = self.session.get(
            f"{self.base}/api/v1/me/",
            headers={"Accept": "application/json"},
            timeout=30,
        )
        if me.status_code != 200:
            raise SupersetError(
                f"Superset не принял пару «{username}» + пароль. Проверь её "
                "в веб-интерфейсе: это должна быть учётка Superset, а не "
                "сервера или базы."
            )

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

    def delete(self, path: str) -> dict[str, Any]:
        return self._check(
            self.session.delete(f"{self.base}{path}", timeout=60), f"DELETE {path}"
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

    def _permission_rows(self) -> list[dict[str, Any]]:
        """Все разрешения инстанса, постранично."""
        rows: list[dict[str, Any]] = []
        page = 0
        while page < 50:  # предохранитель от бесконечной страницы
            query = json.dumps({"page_size": 100, "page": page})
            chunk = self.get(
                f"/api/v1/security/permissions-resources/?q={query}"
            )["result"]
            if not chunk:
                break
            rows += chunk
            page += 1
        return rows

    def named_permissions(self, wanted: list[tuple[str, str]]) -> dict[str, int]:
        """Разрешения по паре (право, ресурс): «право on ресурс» -> id.

        Ключ — пара целиком, а не ресурс: на одном ресурсе Api висят и
        can_time_range, и can_query, и can_query_form_data, и путать их
        нельзя — выдать лишнее анониму означает открыть ему выполнение
        запросов.
        """
        targets = {pair: None for pair in wanted}
        for row in self._permission_rows():
            key = (
                (row.get("permission") or {}).get("name"),
                (row.get("view_menu") or {}).get("name"),
            )
            if key in targets:
                targets[key] = row["id"]
        return {
            f"{permission} on {view}": pid
            for (permission, view), pid in targets.items()
            if pid is not None
        }

    def datasource_permissions(self) -> dict[int, int]:
        """Разрешения datasource_access: id датасета -> id разрешения.

        Сопоставление идёт по `(id:N)` в конце имени, а не по разбору
        `[база].[схема].[таблица]`: схемы в имени нет вовсе — Superset
        называет разрешение `[база].[таблица](id:N)`. Разбор по схеме
        не находил ничего, и скрипт считал, что прав на витрины не
        существует. Привязка к id заодно переживает переименование
        датасета и смену имени базы.
        """
        found: dict[int, int] = {}
        for row in self._permission_rows():
            if (row.get("permission") or {}).get("name") != "datasource_access":
                continue
            view_menu = (row.get("view_menu") or {}).get("name") or ""
            match = re.search(r"\(id:(\d+)\)\s*$", view_menu)
            if match:
                found[int(match.group(1))] = row["id"]
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
    """Короткий стабильный идентификатор из произвольной строки.

    Запасной хвост из хеша нужен из-за кириллицы: правило оставляет
    только [a-z0-9], а в «Заказы» таких символов нет вовсе, и слаг
    выходил пустым. Совпадали при этом не строки, а идентификаторы —
    у всех русскоязычных метрик optionName был один и тот же «metric_»,
    а две карточки на вкладке качества данных получили общий ключ в
    раскладке, и вторая затёрла первую.

    Хеш берётся от исходного текста, поэтому одно и то же имя даёт
    один и тот же слаг между запусками: иначе сверка «совпадает с
    описанием» видела бы различие каждый раз.
    """
    slug = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")[:40]
    if slug:
        return slug
    return "x" + hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]


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
        # sha1, а не hash(): встроенный hash солится на каждый процесс,
        # поэтому имя фильтра менялось от запуска к запуску. Сверка
        # «совпадает ли чарт с описанием» всегда видела различие и
        # переписывала карточки на каждом прогоне — идемпотентность была
        # мнимой, а тест этого не ловил, потому что внутри одного
        # процесса hash стабилен.
        "filterOptionName": (
            f"filter_{_slug(subject)}_"
            + hashlib.sha1(expression.encode("utf-8")).hexdigest()[:8]
        ),
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
    """Скользящее среднее вместо суточной пилы.

    min_periods=1, а не days: при days первые шесть дат исчезают с
    графика молча, и период выглядит короче, чем он есть.

    Применять только к счётчикам и суммам. У доли скользящее среднее
    даёт среднее дневных долей, а это не доля за неделю: день с тремя
    заказами весит столько же, сколько день с тысячей.
    """
    return {
        "rolling_type": "mean",
        "rolling_periods": days,
        "min_periods": 1,
    }


DATE_AXIS = {"x_axis_time_format": "%d.%m"}  # P1-01


def horizontal_bars(label: str) -> dict[str, Any]:
    """Категории вдоль вертикали, значения — вдоль горизонтали.

    Семь каналов привлечения и десять городов не помещаются подписями
    под вертикальными столбцами: ECharts прячет те, что налезают друг
    на друга, и из семи подписей на графике оставалось три. Поворот на
    45° это лечит наполовину — подписи читаются, но косо и с обрезкой
    по нижнему краю.

    Развёрнутый график снимает вопрос: подпись лежит строкой слева,
    места ей ровно столько, сколько нужно, и поворачивать нечего.

    Сортировка при этом переворачивается. ECharts рисует первую
    категорию у начала оси, а начало вертикальной оси — внизу, поэтому
    сортировка по возрастанию даёт убывание сверху вниз, то есть то,
    как читают такой график.
    """
    return {
        "orientation": "horizontal",
        "xAxisLabelRotation": 0,
        "x_axis_sort": label,
        "x_axis_sort_asc": True,
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
    # Три витрины, заменившие виртуальные датасеты. Те жили внутри
    # Superset и читали marketplace_analytics.orders FINAL напрямую: их
    # SQL не лежал в репозитории, не проходил ревью и не покрывался
    # тестами, а FINAL произносился мимо семантического слоя — ровно то,
    # ради чего слой и заводили.
    "order_stage_durations": {
        "phase": "p2",
        "note": "тайминги этапов переехали из виртуального датасета",
    },
    "orders_with_sequence": {
        "phase": "p2",
        "note": "номер заказа у покупателя переехал в витрину",
    },
    "customer_order_counts": {
        "phase": "p2",
        "note": "число заказов на покупателя переехало в витрину",
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
    "orders_by_hour_dow": {
        "phase": "p1",
        "note": "4.1 календарный знаменатель тепловой карты",
    },
    "customer_cohort_retention": {
        "phase": "p1",
        "note": "4.2 retention с полной сеткой и нулями",
    },
}


# Колонки, тип которых Superset определил неверно.
#
# cohort_week — подпись когорты, строка «06-01». Superset пометил её
# временной и заодно назначил главной временной колонкой датасета,
# поэтому тепловая карта форматировала её как дату и на всех строках
# стояло «01.01.1970»: столько получается, если прочитать «06-01» как
# метку времени. Проценты в ячейках при этом были верные — сломаны были
# только подписи строк.
#
# Настоящая дата когорты лежит рядом, в cohort_week_date; она и
# становится временной колонкой датасета.
DATASET_COLUMNS: dict[str, dict[str, Any]] = {
    "customer_cohort_retention": {
        "main_dttm_col": "cohort_week_date",
        "is_dttm": {"cohort_week": False, "cohort_week_date": True},
    },
}

# Поля колонки, которые принимает PUT /api/v1/dataset. Всё остальное,
# что отдаёт GET (type_generic, id датасета, флаги сертификации),
# схема отвергает с 400, поэтому список именно белый.
COLUMN_FIELDS = (
    "id",
    "column_name",
    "verbose_name",
    "description",
    "expression",
    "filterable",
    "groupby",
    "is_active",
    "is_dttm",
    "python_date_format",
    "type",
    "advanced_data_type",
    "extra",
    "uuid",
)


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
            "aliases": ["Динамика GMV"],
            "dataset": "orders_with_customer_dim",
            "note": "P0-02 GMV только по оплаченным + P0-03 зрелый хвост",
            "slice_name": "Динамика GMV (среднее за 7 дней)",
            "description": (
                "«Оплачено» — сумма оплаченных заказов, «Чистый» дополнительно "
                "исключает отменённые и возвращённые. Линия сглажена "
                f"скользящим средним за 7 дней. Последние "
                f"{PAYMENT_SETTLEMENT_DAYS} дня отрезаны: у заказа ещё не "
                "истёк срок оплаты."
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
            "filters": [SETTLED_FILTER],
        },
        "Изменение среднего чека (AOV)": {
            "phase": "p0",
            "aliases": ["Динамика среднего чека (AOV)"],
            "dataset": "orders_with_customer_dim",
            "note": "P0-02 средний чек по оплаченным заказам",
            "slice_name": "Динамика среднего чека (среднее за 7 дней)",
            "description": (
                "Сумма оплаченных заказов, делённая на их число. Линия "
                "сглажена скользящим средним за 7 дней."
            ),
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                **DATE_AXIS,
                **rolling_mean(),
            },
            "filters": [SETTLED_FILTER],
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
            # Без сглаживания: скользящее среднее доли усредняет дневные
            # доли, а не считает долю за неделю — день с тремя заказами
            # весил бы столько же, сколько день с тысячей.
            "set": {"y_axis_format": ".1%", **DATE_AXIS},
            "filters": [MATURE_FILTER],
        },
        "Изменение структуры статусов заказов": {
            "phase": "p0",
            "dataset": "orders_with_customer_dim",
            "note": "P0-03 зрелый хвост",
            "slice_name": "Динамика структуры статусов заказов",
            "description": (
                "Часть заказов остаётся в открытых статусах даже после "
                "всех сроков: сообщение о том, что заказ закрыт, пришло "
                "с ошибками и было отклонено проверками, поэтому "
                "последним известным остался открытый статус."
            ),
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
                # Ось числовая — это дни с регистрации, а не даты. На
                # чарте от прежней, датовой версии остался формат
                # «%d.%m.%Y»: описание его не задавало, а form_data
                # обновляется через update() и старое значение не
                # вычищает. Задаём явно, чтобы формат соответствовал
                # тому, что на оси на самом деле.
                "x_axis_time_format": "smart_date",
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
                **PERCENTILE_ORDER,
            },
            # От прежней, временной версии чарта в измерениях остался
            # refunded_at. В запрос эта колонка не попадает — разрез
            # теперь по refund_kind, — но пост-обработка на неё
            # ссылается, и чарт отвечал «Referenced columns not
            # available in DataFrame» вместо графика. Ключ снимается, а
            # не переписывается: update() умеет добавлять, но не
            # удалять, и старое значение пережило бы правку описания.
            "unset": ["columns"],
            "replace_filters": [],
        },
        "Средний чек по городам (топ-10)": {
            "phase": "p0",
            "description": (
                "Средний оплаченный заказ по городу покупателя, десять "
                "крупнейших по обороту."
            ),
            "note": "P0-07 сортировка по метрике + P0-02 оплаченные заказы",
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                **horizontal_bars("Средний чек, ₽"),
            },
            "filters": [MATURE_FILTER],
        },
        "GMV по городам (топ-10)": {
            "phase": "p0",
            "description": (
                "Оборот по городу покупателя, десять крупнейших. Город берётся "
                "из профиля покупателя, а не из адреса доставки."
            ),
            "note": "P0-07 сортировка по метрике + P0-02 оплаченные заказы",
            "set": {
                "metrics": [metric("GMV, ₽", GMV_PAID)],
                **horizontal_bars("GMV, ₽"),
            },
            "filters": [MATURE_FILTER],
        },
        "Разбивка DQ-ошибок по типам": {
            "phase": "p0",
            "note": "P0-07 сортировка + 4.3 подпись про срабатывания",
            "slice_name": "Отклонённые строки по типам ошибок",
            "aliases": ["Срабатывания DQ-проверок по типам"],
            "description": (
                "Считаются найденные ошибки, а не строки: одна строка "
                "может нарушать несколько правил сразу, поэтому сумма по "
                "типам больше числа отклонённых строк."
            ),
            "set": {
                # «Карантин» — наше внутреннее слово для отсева, и на
                # дашборде ему делать нечего: посетитель видит подпись
                # оси, а не устройство пайплайна.
                "metrics": [metric("Отклонённых строк", "SUM(quarantined_rows)")],
                "y_axis_title": "Найдено ошибок",
                **horizontal_bars("Отклонённых строк"),
            },
        },
        "GMV по каналам привлечения": {
            "phase": "p0",
            "description": (
                "Оборот по каналу, из которого пришёл покупатель. Канал "
                "закреплён за покупателем при регистрации и потом не меняется. "
                "Это оборот, а не прибыльность: расходов на привлечение в "
                "данных нет, поэтому назвать канал выгодным по этому графику "
                "нельзя."
            ),
            "note": "P0-02 оплаченные заказы + P0-07 сортировка + P1-11 язык",
            "set": {
                "metrics": [metric("GMV, ₽", GMV_PAID)],
                "x_axis": "acquisition_channel_ru",
                **horizontal_bars("GMV, ₽"),
            },
            "filters": [MATURE_FILTER],
        },
        "Средний чек по каналам привлечения": {
            "phase": "p0",
            "description": (
                "Средний оплаченный заказ покупателей, пришедших из канала. "
                "Высокий чек сам по себе не делает канал выгодным: сколько "
                "стоило привести этих покупателей, данные не знают."
            ),
            "note": "P0-02 оплаченные заказы + P0-07 сортировка + P1-11 язык",
            "set": {
                "metrics": [metric("Средний чек, ₽", AOV_PAID)],
                "x_axis": "acquisition_channel_ru",
                **horizontal_bars("Средний чек, ₽"),
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
            "slice_name": "Регистрации по городам",
            "description": (
                "Регистрации по городу покупателя. Это не число купивших: "
                "часть аккаунтов остаётся без заказов."
            ),
            "note": "P1-02 горизонтальные бары + сортировка",
            "set": horizontal_bars("Число покупателей"),
        },
        "Причины отмены заказов": {
            "phase": "p1",
            "description": (
                "Из чего складываются отмены. Считаются только заказы, у "
                "которых истекли все сроки: у свежих исход ещё не известен."
            ),
            "dataset": "orders_with_customer_dim",
            "note": "P1-03 подписи вынесены наружу + P2-06 словарь из витрины",
            "set": {
                "groupby": ["cancellation_reason_ru"],
                **PIE_LABELS,
                # Диаграмма стоит на всю ширину, и подписям хватает
                # места по бокам даже при радиусе по умолчанию:
                # «Отменён покупателем после оплаты» — самая длинная
                # подпись на дашборде.
                "outerRadius": 70,
                "number_format": "SMART_NUMBER",
            },
        },
        "Доли покупателей по каналам привлечения": {
            "phase": "p1",
            "slice_name": "Доли регистраций по каналам привлечения",
            "description": (
                "Доли регистраций, а не покупателей: канал приводит "
                "аккаунты, а купят они или нет — отдельный вопрос."
            ),
            "note": "P1-03 подписи вынесены наружу + P2-06 словарь из витрины",
            "set": {
                "groupby": ["acquisition_channel_ru"],
                **PIE_LABELS,
            },
        },
        "Изменение причин отмены": {
            "phase": "p1",
            "description": (
                "Те же причины, что и на круговой диаграмме, но по дням: видно, "
                "меняется ли со временем структура отмен, а не только их доля."
            ),
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
            "slice_name": "Динамика доли успешных оплат",
            "aliases": ["Динамика success rate оплаты"],
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
            },
        },
        "Тренд DQ-ошибок по сущностям": {
            "phase": "p1",
            "note": "P1-05 проценты + P2-04 порог 5% + 6.3 язык подписей",
            "slice_name": "Доля отклонённых строк по дням",
            "description": (
                "Доля строк, которые не прошли проверки при загрузке и не "
                "попали в отчётность. Считается отдельно по покупателям, "
                "заказам и платежам."
            ),
            "set": {
                "y_axis_format": ".2%",
                "y_axis_title": "Доля отклонённых строк",
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
            "description": (
                "Сколько времени занимает каждый переход заказа: оплата, "
                "отправка, доставка, получение. p50 — половина заказов "
                "уложилась в это время, p99 — почти все; разрыв между ними "
                "показывает длину хвоста."
            ),
            "note": "P1-08 порядок перцентилей + переезд на витрину",
            # Был виртуальный датасет orders_funnel_stages_unpivoted с
            # SQL внутри Superset; теперь витрина, у которой то же
            # определение лежит в репозитории и проверяется тестами.
            "dataset": "order_stage_durations",
            "set": {
                "metrics": [
                    metric("p50", "quantile(0.5)(duration_seconds) / 3600"),
                    metric("p90", "quantile(0.9)(duration_seconds) / 3600"),
                    metric("p99", "quantile(0.99)(duration_seconds) / 3600"),
                ],
                "x_axis_sort": "stage",
                "x_axis_sort_asc": True,
                **PERCENTILE_ORDER,
            },
        },
        "Количество заказов по часам и дням недели": {
            "phase": "p1",
            "dataset": "orders_by_hour_dow",
            "note": "4.1 календарный знаменатель + P1-09 понедельник сверху",
            "description": (
                "Среднее число заказов в час. Знаменатель — все дни периода, "
                "включая те, когда в этот час заказов не было: иначе редкая "
                "ячейка показывает среднее по паре удачных дней."
            ),
            "set": {
                # Не avg(orders_per_day): среднее уже посчитанных средних
                # не складывается по городам. Делим отфильтрованную сумму
                # на число дней — оно одинаково внутри дня недели.
                "metric": metric(
                    "Заказов в час (в среднем)", "sum(orders) / max(days)"
                ),
                "groupby": "dow_label",
                "x_axis": "hour_of_day",
                "y_axis_format": ",.2f",
                "sort_y_axis": "alpha_desc",
                "sort_x_axis": "alpha_asc",
                "time_grain_sqla": None,
            },
            "replace_filters": [],
        },
        "Retention по когортам регистрации (по неделям)": {
            "phase": "p1",
            "dataset": "customer_cohort_retention",
            "note": "P1-10 короткие подписи когорт + отсечение недели −1",
            "description": (
                "Доля когорты, сделавшая хотя бы один заказ на N-й неделе "
                "после регистрации. Ноль означает, что не вернулся никто; "
                "пустая ячейка — что неделя ещё не прожита до конца. "
                "Активность считается по созданному заказу."
            ),
            "set": {
                "metric": metric("Retention", "avg(retention_rate)"),
                "groupby": "cohort_week",
                "x_axis": {
                    "expressionType": "SQL",
                    "label": "Недель с регистрации",
                    "sqlExpression": "leftPad(toString(weeks_since_signup), 2, '0')",
                },
                "y_axis_format": ".0%",
                "sort_y_axis": "alpha_desc",
                "sort_x_axis": "alpha_asc",
                "time_grain_sqla": None,
            },
            "replace_filters": [],
            "filters": [("is_mature = 1", "is_mature")],
        },
        "Изменение числа заказов": {
            "phase": "p1",
            "aliases": ["Динамика числа заказов"],
            "dataset": "orders_with_customer_dim",
            "note": "P1-01 формат оси + P1-07 сглаживание + P1-11 заголовок",
            "slice_name": "Динамика числа заказов (среднее за 7 дней)",
            "description": (
                "Созданные заказы, сглажено скользящим средним за 7 дней. "
                "Горизонт зрелости здесь не применяется: заказ уже создан, "
                "ждать нечего."
            ),
            "set": {**DATE_AXIS, **rolling_mean()},
        },
        "Изменение числа новых покупателей": {
            "phase": "p1",
            "aliases": ["Динамика числа новых покупателей"],
            "note": "P1-01 формат оси + P1-07 сглаживание + P1-11 заголовок",
            "slice_name": "Динамика регистраций (среднее за 7 дней)",
            "description": (
                "Считаются регистрации по дате создания аккаунта, а не "
                "покупатели: часть из них не сделает ни одного заказа."
            ),
            "set": {**DATE_AXIS, **rolling_mean()},
        },
        "Изменение числа новых и повторных заказов": {
            "phase": "p1",
            "description": (
                "Первый заказ покупателя считается новым, все последующие — "
                "повторными. Разбивка по дате заказа, а не по дате регистрации."
            ),
            "note": "P1-01 формат оси + P1-11 заголовок",
            "slice_name": "Динамика новых и повторных заказов",
            "set": DATE_AXIS,
        },
        "Изменение суммы рефандов": {
            "phase": "p1",
            "description": (
                "Сумма возвращённых покупателям денег по дате возврата, а не по "
                "дате заказа: деньги ушли тогда, когда ушли."
            ),
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
                "Каждый платёж считается один раз, по своему последнему "
                "состоянию: возвращённый попадает в «Возврат», а не в "
                "«Успешно» и «Возврат» одновременно."
            ),
            "set": {
                "x_axis": "payment_method_ru",
                "groupby": ["status_ru"],
                "stack": "Stack",
                # «Банковская карта» под вертикальным столбцом не
                # помещается, и ECharts прятал её вместе с соседками:
                # из четырёх способов оплаты подписаны были два.
                **horizontal_bars("Платежи"),
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
            "description": (
                "Сколько покупателей сделали ровно один заказ, сколько два и "
                "так далее. Покупатели без заказов сюда не попадают — их долю "
                "показывает конверсия в первый заказ."
            ),
            "note": "P2-05 сортировка оси",
            "set": {"x_axis_sort": "orders_count", "x_axis_sort_asc": True},
        },
    }


# P2-02. KPI-строка на Overview: числа, которых на вкладке не было вообще.
# Форматы чисел на карточках. Раньше их было четыре штуки на пять
# карточек — SMART_NUMBER, ",.0f", ".1%", ".2%", — и рядом стояли «230M»
# и «2,021» без намёка на то, что это рубли.
#
# Правило простое и одно на весь дашборд: то, что считают поштучно,
# показывается целиком; то, что не прочитать по цифрам, сокращается;
# доли — всегда с одним знаком. Разнобой в знаках после запятой создаёт
# ложное впечатление, будто одна доля измерена точнее другой.
COUNT_FORMAT = ",.0f"  # 133 741 — заказы считают штуками
LARGE_MONEY_FORMAT = ",.1f"  # 237,1 — в миллионах, единица в подписи
MONEY_FORMAT = ",.0f"  # 2 021 — средний чек читают целиком
SHARE_FORMAT = ".1%"  # 16.2% — один знак у всех долей

# Единица измерения живёт в подписи под числом, а не в поле валюты
# Superset. Поле валюты пробовали: оно принимает код ISO и печатает
# «230M RUB», потому что символ берётся не из настройки чарта, а из
# локали фронтенда. Передать туда «₽» напрямую нельзя — Intl не знает
# такого кода и выбрасывает суффикс молча, вместе с валютой.
#
# Русская локаль это тоже не чинит: языкового пакета ru в сборке нет
# (эндпоинт переводов отвечает 404), интерфейс остаётся английским, а
# сокращения k/M/G в d3 зашиты латиницей и от локали не зависят вовсе.
#
# Подпись же — обычный текст, который мы задаём сами. Поэтому GMV
# делится на миллион и читается как «237,1» плюс «млн ₽» строкой ниже.
MILLION = 1_000_000

# P1-08. Порядок серий на графиках перцентилей.
#
# Задать метрики в порядке p50, p90, p99 недостаточно: Superset
# пересортировывает серии по значению, а p99 по определению больше
# всех — легенда выходила задом наперёд, p99 → p90 → p50. Сортировка по
# имени возвращает смысловой порядок, и работает она именно потому, что
# имена подобраны так, что сортируются как числа.
PERCENTILE_ORDER = {
    "sort_series_type": "name",
    "sort_series_ascending": True,
}

# P1-03. Подписи пирогов.
#
# Легенды у пирогов больше нет. Она забирала правую треть — а на
# «Долях регистраций» и вовсе две трети, потому что чарт стоял в
# колонке шириной в четверть экрана, — и кругу оставалось меньше
# места, чем списку названий рядом с ним. Читателю при этом всё равно
# приходилось водить глазами между сектором и строкой в списке.
#
# Теперь название и доля стоят у самого сектора, а освободившуюся
# ширину занял круг. Выносные подписи требуют места по сторонам,
# поэтому оба пирога переехали в более широкие ячейки раскладки, а
# радиус уменьшен: подпись должна помещаться целиком, иначе она
# обрежется («Email-рассы…») — ровно тем, чем плоха была первая
# попытка вынести подписи наружу.
PIE_LABELS = {
    "labels_outside": True,
    "label_line": True,
    "label_type": "key_percent",
    "show_labels": True,
    # Порог в 1%, а не 5%: снаружи места хватает всем, а сектор в 3%
    # без подписи выглядел ошибкой отрисовки — доля есть, названия нет,
    # и посмотреть его теперь негде, легенды не осталось.
    "show_labels_threshold": 1,
    "show_legend": False,
    # Меньше умолчания (70): остаток ширины уходит под выносные
    # подписи, а они длинные — «Отменён покупателем после оплаты».
    "outerRadius": 50,
}


# Настройки, которые скрипт когда-то записывал в карточки и больше не
# должен там оставлять.
#
# Убрать ключ из описания недостаточно: карточка обновляется через
# update(), а тот перезаписывает и добавляет, но никогда не удаляет.
# Снятое поле осталось бы жить в чарте, и «230M RUB» рисовалось бы
# дальше при том, что в коде про валюту уже ни слова.
RETIRED_CARD_KEYS = ["currency_format"]

# Размеры карточек. Ширина — в колонках двенадцатиколоночной сетки,
# высота — в единицах по 8 px.
#
# Было 2 колонки на карточку при пяти карточках в ряд: 149 px на экране
# 1280 px, где заголовок обрезался у всех пяти, а из 240 px высоты
# содержимое занимало 155. Четыре карточки по три колонки дают ровно
# двенадцать и вдвое больше места под заголовок.
# Высота блока выводов в единицах по 8 px. Три пункта по две-три
# строки — примерно 200 px; при нехватке Superset прячет текст под
# прокрутку внутри блока, чего на обзоре быть не должно.
MARKDOWN_HEIGHT = 26

# Доля высоты карточки под подпись. Значение из того же диапазона, что
# Superset использует для остальных кеглей big_number.
SUBHEADER_FONT_SIZE = 0.125

KPI_CARD_WIDTH = 3
KPI_CARD_HEIGHT = 20
# Карточки качества данных чуть просторнее: их имена длиннее («Отставание
# данных» против «GMV»), и на трёх колонках заголовок переносился на
# вторую строку, вытесняя подпись за нижний край. Шесть колонок при этом
# уже перебор — Superset подгоняет кегль под размер карточки, и подпись
# вырастала почти до размера самого числа.
DQ_CARD_WIDTH = 4
DQ_CARD_HEIGHT = 22

KPI_CHARTS: list[dict[str, Any]] = [
    {
        # Префикс «KPI ·» съедал треть заголовка и не сообщал ничего:
        # карточки и так стоят строкой в самом верху обзора. На 1280 px
        # из-за него все пять имён обрезались до «KPI ·…».
        "name": "GMV",
        "description": (
            "Сумма оплаченных заказов за период. Считается по факту "
            "оплаты, а не по созданию заказа: неоплаченный заказ денег "
            "не принёс. Последние дни, у которых ещё не истёк срок "
            "оплаты, в расчёт не берутся."
        ),
        "aliases": ["KPI · GMV"],
        "dataset": "orders_with_customer_dim",
        "metric": metric("GMV, млн ₽", f"({GMV_PAID}) / {MILLION}"),
        "format": LARGE_MONEY_FORMAT,
        "subheader": "млн ₽ · оплаченные заказы за период",
        "filters": [SETTLED_FILTER],
    },
    {
        "name": "Заказы",
        "description": (
            "Число созданных заказов. Горизонт зрелости здесь не "
            "применяется: заказ уже создан, и его дальнейшая судьба на "
            "счётчик не влияет."
        ),
        "aliases": ["KPI · Заказы"],
        "dataset": "orders_with_customer_dim",
        "metric": metric("Заказов", "count()"),
        # Не SMART_NUMBER: тот показывал «134k» вместо 133 741. Заказы
        # пересчитывают и сверяют с выгрузкой, округление тут мешает.
        "format": COUNT_FORMAT,
        "subheader": "Создано заказов",
        # Без горизонта: заказ создан, его судьба на счётчик не влияет.
        # С MATURE карточка показывала 76 тысяч там, где график рядом
        # рисовал 132.
        "filters": [],
    },
    {
        "name": "Средний чек",
        "description": (
            "Оборот, делённый на число оплаченных заказов. Знаменатель "
            "— оплаченные, а не все созданные: иначе неоплаченные "
            "занижали бы чек, не добавив ни рубля в числитель."
        ),
        "aliases": ["KPI · Средний чек"],
        "dataset": "orders_with_customer_dim",
        "metric": metric("Средний чек, ₽", AOV_PAID),
        "format": MONEY_FORMAT,
        "subheader": "₽ · на оплаченный заказ",
        "filters": [SETTLED_FILTER],
    },
    {
        "name": "Доля отмен",
        "description": (
            "Отменённые заказы к числу созданных. Только по заказам, у "
            "которых истекли все сроки: у свежих судьба ещё не "
            "определена, и они занижали бы долю."
        ),
        "aliases": ["KPI · Доля отмен"],
        "dataset": "orders_with_customer_dim",
        "metric": metric("Доля отмен", "countIf(cancelled_at IS NOT NULL) / count()"),
        "format": SHARE_FORMAT,
        "subheader": f"От созданных заказов старше {MATURITY_DAYS} дней",
        "filters": [MATURE_FILTER],
    },
]

# P2-04. Свежесть пайплайна на вкладке качества данных.
#
# Доля невалидных строк переехала сюда из строки KPI. Причина не только
# в ширине: обзор отвечает на вопрос «как идут дела у бизнеса», а доля
# строк в карантине — свойство пайплайна, и её место рядом с отставанием
# загрузки, на вкладке качества данных.
DQ_CHARTS: list[dict[str, Any]] = [
    {
        # Имя — заголовок карточки, а не предложение: «Часов с последней
        # загрузки» не помещается в 231 px и обрезается многоточием на
        # второй строке. Единица измерения ушла в подпись, где для неё
        # есть место.
        "name": "Отставание данных",
        "description": (
            "Сколько часов прошло с последней успешной загрузки. "
            "Берётся максимум по всем источникам, а не среднее: "
            "отстал один — отстали данные целиком."
        ),
        "aliases": ["Часов с последней загрузки"],
        "dataset": "load_freshness",
        "metric": metric("Часов с последней загрузки", "max(hours_since_load)"),
        "format": COUNT_FORMAT,
        # max, а не min: отстал один источник — отстали данные целиком.
        "subheader": "Часов по самым отставшим данным",
        "filters": [],
    },
    {
        "name": "Отклонено при загрузке",
        "description": (
            "Доля строк, не прошедших проверки при загрузке. Такие "
            "строки не попадают в отчётность — они отложены отдельно, "
            "и разбор по типам ошибок рядом."
        ),
        "aliases": ["KPI · Невалидных строк", "Невалидных строк"],
        "dataset": "dq_metrics",
        "metric": metric(
            "Доля отклонённых строк", "SUM(invalid_rows) / SUM(total_rows)"
        ),
        "format": SHARE_FORMAT,
        "subheader": "Доля строк, не прошедших проверки",
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
# CHART-explore-20-2 отсюда убран намеренно: это тренд GMV, и его место
# как раз на обзоре — оборот и есть главный вопрос к дашборду. Теперь он
# стоит там под собственным ключом, см. OVERVIEW_TRENDS.
OVERVIEW_DUPLICATES = ["CHART-explore-2-2"]

# 6.4. Обзор — короткий экран, а не свалка.
#
# Было четыре карточки и шесть графиков подряд, причём половина из них
# дублировала содержимое других вкладок: регистрации живут в
# «Покупателях», отбраковка — в «Качестве данных». Осталось четыре
# тренда, отвечающих на четыре разных вопроса: сколько денег, сколько
# заказов, доходят ли платежи, много ли теряем на отменах.
OVERVIEW_TRENDS: list[tuple[str, list[tuple[str, str, int, int]]]] = [
    (
        "ROW-ov-trends-1",
        [
            ("CHART-ov-gmv", "Динамика GMV (среднее за 7 дней)", 6, 50),
            ("CHART-ov-orders", "Динамика числа заказов (среднее за 7 дней)", 6, 50),
        ],
    ),
    (
        "ROW-ov-trends-2",
        [
            ("CHART-ov-payments", "Динамика доли успешных оплат", 6, 50),
            ("CHART-ov-cancel", "Динамика долей отмен и возвратов", 6, 50),
        ],
    ),
]


# Строки тематических вкладок, ширину которых задаём мы, а не история
# перетаскиваний в UI.
#
# Заявляется только то, что переложено, а не вкладка целиком: в отличие
# от обзора, эти вкладки не зарастают дубликатами, и переписывать их
# состав ради двух строк — значит взять на себя и всё остальное.
#
# Формат: вкладка, ключ строки, ключ строки, после которой её вставить
# (None — в конец), и сами чарты: ключ размещения, имя чарта, ширина в
# колонках из двенадцати, высота в единицах по 8 px.
TAB_ROWS: list[tuple[str, str, str | None, list[tuple[str, str, int, int]]]] = [
    # Причины отмены занимали половину ширины на двоих: у гистограммы
    # столбцы сжимались в частокол, а легенда сбоку обрезала названия
    # причин; у круговой диаграммы те же названия, вынесенные к
    # секторам вместо легенды, не помещались по бокам. Обе получают всю
    # ширину, а динамики отмен и рефандов встают парой ниже.
    (
        "TAB-orderops",
        "ROW-oo-2",
        None,
        [("CHART-oo-cancel-reasons", "Причины отмены заказов", 12, 50)],
    ),
    (
        "TAB-orderops",
        "ROW-oo-cancel-mix",
        "ROW-oo-2",
        [("CHART-oo-cancel-mix", "Динамика причин отмены", 12, 50)],
    ),
    (
        "TAB-orderops",
        "ROW-oo-refunds-status",
        "ROW-oo-cancel-mix",
        [
            ("CHART-oo-cancel-share", "Динамика долей отмен и возвратов", 6, 50),
            ("CHART-oo-refunds", "Динамика суммы рефандов", 6, 50),
        ],
    ),
    # Три чарта в ряд давали каждому по четверти экрана. Городам этого
    # мало — шестнадцать названий не помещаются, — а пирогу каналов не
    # хватало места на подписи, которые заменили легенду. Города
    # получают всю ширину, остальные двое — по половине.
    (
        "TAB-clients",
        "ROW-cl-1",
        None,
        [
            ("CHART-cl-signups", "Динамика регистраций (среднее за 7 дней)", 6, 50),
            (
                "CHART-cl-channels",
                "Доли регистраций по каналам привлечения",
                6,
                50,
            ),
        ],
    ),
    (
        "TAB-clients",
        "ROW-cl-cities",
        "ROW-cl-1",
        [("CHART-cl-cities", "Регистрации по городам", 12, 50)],
    ),
]


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

    def sync_dataset_columns(self, phases: set[str]) -> None:
        """Правит метаданные колонок там, где Superset ошибся с типом.

        Колонки уходят на сервер списком целиком: PUT сверяет присланное
        с тем, что есть, и колонку, которой в списке нет, удаляет. Даже
        одна правка флага требует переслать все остальные как есть.
        """
        for name, fixes in DATASET_COLUMNS.items():
            spec = DATASETS.get(name)
            if spec is None or spec["phase"] not in phases:
                continue
            existing = self.datasets.get(name)
            if existing is None:
                continue

            detail = self.client.get(f"/api/v1/dataset/{existing['id']}")["result"]
            wanted = fixes.get("is_dttm", {})
            columns = []
            changed = []
            for column in detail.get("columns", []):
                trimmed = {k: column[k] for k in COLUMN_FIELDS if k in column}
                target = wanted.get(column["column_name"])
                if target is not None and bool(column.get("is_dttm")) != target:
                    trimmed["is_dttm"] = target
                    changed.append(column["column_name"])
                columns.append(trimmed)

            payload: dict[str, Any] = {}
            main = fixes.get("main_dttm_col")
            if main and detail.get("main_dttm_col") != main:
                payload["main_dttm_col"] = main
                changed.append(f"главная временная колонка -> {main}")
            if not changed:
                continue

            payload["columns"] = columns
            self.log(f"тип колонок датасета {name}: {', '.join(changed)}")
            if self.apply:
                self.client.put(f"/api/v1/dataset/{existing['id']}", payload)

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
        needed, absent = self._public_targets()
        missing = {name: pid for name, pid in needed.items() if pid not in current}

        if absent:
            print(
                f"  ! нет объектов прав на витрины: {', '.join(absent)} — "
                "датасета либо нет, либо Superset не завёл на него разрешение"
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

    def _public_targets(self) -> tuple[dict[str, int], list[str]]:
        """Права, которые нужны анонимному посетителю. Один источник.

        Считается по чартам дашборда, а не по списку датасетов в коде:
        доступ нужен ровно к тому, что чарты читают.

        Общий для выдачи и для сужения намеренно. Раньше выдача брала
        все датасеты подряд, а сужение — только используемые, и обычный
        прогон бесшумно возвращал роли то, что сужение сняло: два
        расходящихся определения «нужного» вместо одного.
        """
        available = self.client.datasource_permissions()
        target: dict[str, int] = {}
        absent: list[str] = []

        used_ids = set()
        for chart in self.client.charts(self.dashboard_id).values():
            source = str(chart["form_data"].get("datasource") or "")
            if source:
                used_ids.add(int(source.split("__")[0]))

        for dataset_id in sorted(used_ids):
            perm_id = available.get(dataset_id)
            if perm_id is None:
                absent.append(f"датасет {dataset_id}")
            else:
                target[f"датасет {dataset_id}"] = perm_id

        named = self.client.named_permissions(PUBLIC_REQUIRED_PERMISSIONS)
        for permission, view in PUBLIC_REQUIRED_PERMISSIONS:
            key = f"{permission} on {view}"
            if key in named:
                target[key] = named[key]
            else:
                absent.append(key)

        return target, absent

    def narrow_public_access(self, role_name: str) -> None:
        """Оставляет роли ровно то, что нужно для просмотра дашборда.

        Обратная операция к grant_public_access, и куда опаснее: та
        только добавляла, а эта снимает. Поэтому набор считается не «всё
        текущее минус подозрительное», а с нуля — какие датасеты реально
        используют чарты этого дашборда плюс перечисленные поимённо
        функциональные права. Что не попало в набор, снимается.

        Состав датасетов берётся из самих чартов, а не из списка в коде:
        список рассохнется при первом же новом чарте, а чарты — это и
        есть определение того, что дашборду нужно.
        """
        role = self.client.role_by_name(role_name)
        if role is None:
            print(f"  ! роль «{role_name}» не найдена — права не тронуты")
            return

        current = self.client.role_permission_ids(role["id"])
        target, absent = self._public_targets()

        if absent:
            raise SupersetError(
                "Не найдены объекты прав: " + ", ".join(absent) + ". "
                "Сужение остановлено, чтобы не оставить роль без доступа."
            )

        keep = set(target.values())
        extra = current - keep
        rows = {r["id"]: r for r in self.client._permission_rows()}

        def describe(pid: int) -> str:
            row = rows.get(pid)
            if not row:
                return f"право {pid}"
            perm = row.get("permission", {}).get("name")
            view = row.get("view_menu", {}).get("name")
            return f"{perm} on {view}"

        added = keep - current
        if not extra and not added:
            print(f"  Роль «{role_name}»: набор прав уже точный ({len(keep)}).")
            return

        # Показываются обе стороны. Снятие коврового schema_access почти
        # всегда идёт вместе с выдачей точечных прав на те датасеты,
        # которые этим ковром и были прикрыты, — и умолчать о выдаче
        # значило бы отчитаться только о половине операции.
        self.log(
            f"Роль «{role_name}»: снять {len(extra)}, выдать {len(added)}, "
            f"итого {len(keep)}"
        )
        for pid in sorted(extra, key=describe):
            print(f"      − {describe(pid)}")
        for pid in sorted(added, key=describe):
            print(f"      + {describe(pid)}")

        if not self.apply:
            return

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

        self.client.set_role_permissions(role["id"], keep)

        after = self.client.role_permission_ids(role["id"])
        if after != keep:
            raise SupersetError(
                f"Роль «{role_name}» после записи содержит не то, что "
                f"ожидалось. Восстанови по снимку {backup}"
            )

    # --- чарты ---------------------------------------------------------

    def patch_charts(self, phases: set[str]) -> None:
        charts = self.client.charts(self.dashboard_id)
        for name, patch in chart_patches().items():
            if patch["phase"] not in phases:
                continue
            chart = self._find_chart(charts, name, patch)
            if not chart:
                print(f"  ! чарт «{name}» не найден — пропуск")
                continue
            payload = self._build_chart_payload(chart, patch)
            if not payload:
                continue
            self.log(f"{patch['note']}: чарт «{name}»")
            if self.apply:
                self.client.put(f"/api/v1/chart/{chart['id']}", payload)

    @staticmethod
    def _find_chart(
        charts: dict[str, Any], name: str, patch: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Ищет чарт по всем именам, которые он у нас носил.

        Ключ словаря — исходное имя, `slice_name` — то, как чарт
        называется после правок, `aliases` — промежуточные варианты.
        Без последнего каждое переименование делает чарт невидимым для
        следующего прогона: на инстансе он уже под новым именем, а в
        коде появилось ещё более новое. Так уже дважды пропускались
        правки, причём молча.
        """
        for candidate in (name, patch.get("slice_name"), *patch.get("aliases", [])):
            if candidate and candidate in charts:
                return charts[candidate]
        return None

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

    def sync_big_numbers(self, specs: list[dict[str, Any]]) -> dict[str, int]:
        """Создаёт KPI-карточки и приводит существующие к описанию.

        Прежняя версия умела только создавать: если карточка уже была,
        она пропускалась целиком. Поэтому изменившийся горизонт, формат
        или подпись до неё не доезжали, а прогон бодро рапортовал «0
        изменений» — правки были в коде и не были на дашборде.

        Карточки ищутся среди чартов ДАШБОРДА, а не в общем списке
        инстанса. В общем списке они когда-то задвоились — прогон под
        учёткой, видевшей инстанс урезанно, счёл их отсутствующими и
        создал вторые, — а список отдаётся в порядке изменения. Скрипт
        брал то одну копию, то другую: обновит одну, она уезжает вверх,
        следующий прогон берёт вторую. Правки половину времени уходили
        в карточку, которой на дашборде нет, и «приведено к описанию»
        повторялось бесконечно.
        """
        existing = self.client.charts(self.dashboard_id)
        elsewhere = {c["slice_name"] for c in self._all_charts()}
        ids: dict[str, int] = {}

        for spec in specs:
            dataset = self.datasets.get(spec["dataset"])
            if not dataset:
                print(f"  ! витрина {spec['dataset']} не найдена — пропуск KPI")
                continue

            filters: list[Any] = []
            for expression, subject in spec["filters"]:
                filters = ensure_filter(filters, expression, subject)
            wanted = {
                "viz_type": "big_number_total",
                "datasource": f"{dataset['id']}__table",
                "metric": spec["metric"],
                "y_axis_format": spec["format"],
                "subheader": spec["subheader"],
                "adhoc_filters": filters,
                # Кегль подписи задан явно. Без него Superset растягивает
                # текст под ширину карточки, и в одном ряду «Создано
                # заказов» выходило вдвое крупнее, чем «От созданных
                # заказов старше 34 дней», — разный размер читается как
                # разная важность, хотя это просто разная длина строки.
                "subheader_font_size": SUBHEADER_FONT_SIZE,
            }
            chart, was_named = self._named_or_renamed(existing, spec)
            if chart is None:
                if spec["name"] in elsewhere:
                    # Чарт с таким именем есть, но не на дашборде.
                    # Создать второй — это ровно то, из-за чего карточки
                    # и задвоились: лучше остановиться и сказать.
                    print(
                        f"  ! «{spec['name']}» существует, но не на дашборде "
                        "— карточка не создана, чтобы не плодить дубликаты"
                    )
                    continue
                self.log(f"новый чарт «{spec['name']}»")
                if self.apply:
                    result = self.client.post(
                        "/api/v1/chart/",
                        {
                            "slice_name": spec["name"],
                            "viz_type": "big_number_total",
                            "datasource_id": dataset["id"],
                            "datasource_type": "table",
                            "params": json.dumps(wanted, ensure_ascii=False),
                            "description": spec.get("description", ""),
                            "dashboards": [self.dashboard_id],
                        },
                    )
                    ids[spec["name"]] = result["id"]
                continue

            ids[spec["name"]] = chart["id"]
            form_data = copy.deepcopy(chart.get("form_data") or {})
            before = json.dumps(form_data, sort_keys=True, ensure_ascii=False)
            form_data.update(wanted)
            for key in RETIRED_CARD_KEYS:
                form_data.pop(key, None)
            after = json.dumps(form_data, sort_keys=True, ensure_ascii=False)
            changed = after != before

            # Описание — это подсказка по значку ⓘ в заголовке карточки.
            # Хранится оно не в form_data, а рядом, поэтому и сравнивать
            # его надо отдельно: без этого правка текста не доезжала.
            wanted_note = spec.get("description", "")
            note_changed = (chart.get("description") or "") != wanted_note

            if not changed and not note_changed and was_named:
                continue

            payload: dict[str, Any] = {
                "params": json.dumps(form_data, ensure_ascii=False),
                "query_context": "",
                "datasource_id": dataset["id"],
                "datasource_type": "table",
                "description": wanted_note,
            }
            if not was_named:
                self.log(
                    f"карточка «{chart['slice_name']}» переименована "
                    f"в «{spec['name']}»"
                )
                payload["slice_name"] = spec["name"]
            # Отчитываться надо и о правке одной только подсказки: иначе
            # план показывает «0 изменений», а применение всё равно
            # пишет — расхождение, из-за которого плану перестаёшь верить.
            if changed:
                self.log(f"карточка «{spec['name']}» приведена к описанию")
            elif note_changed:
                self.log(f"карточка «{spec['name']}»: обновлена подсказка")

            if self.apply:
                self.client.put(f"/api/v1/chart/{chart['id']}", payload)
        return ids

    @staticmethod
    def _named_or_renamed(
        existing: dict[str, dict[str, Any]], spec: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, bool]:
        """Ищет карточку по текущему имени, затем по прежним.

        Без поиска по прежним именам переименование карточки означало бы
        не переименование, а создание второй: скрипт не нашёл бы «GMV»,
        решил, что её нет, и завёл новую рядом с «KPI · GMV». Ровно так
        карточки уже задваивались однажды.

        Возвращает вторым значением признак «нашлась под нынешним
        именем» — иначе вызывающий обязан ещё и переименовать её.
        """
        chart = existing.get(spec["name"])
        if chart is not None:
            return chart, True
        for alias in spec.get("aliases", []):
            chart = existing.get(alias)
            if chart is not None:
                return chart, False
        return None, True

    def _all_charts(self) -> list[dict[str, Any]]:
        # form_data нужен, чтобы сравнить текущее состояние карточки с
        # описанием и не переписывать её на каждом прогоне.
        query = json.dumps(
            {"columns": ["id", "slice_name", "params"], "page_size": 100}
        )
        rows = self.client.get(f"/api/v1/chart/?q={query}")["result"]
        for row in rows:
            try:
                row["form_data"] = json.loads(row.get("params") or "{}")
            except ValueError:
                row["form_data"] = {}
        return rows

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
            self._overview_layout(position)
            self._place_rows(position)

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
        renamed = {}
        for old, patch in chart_patches().items():
            if "slice_name" not in patch:
                continue
            for previous in (old, *patch.get("aliases", [])):
                renamed[previous] = patch["slice_name"]
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
        self._sync_row(
            position,
            tab_id="TAB-overview",
            row_id="ROW-ov-kpi",
            at_index=0,
            charts=[
                (
                    f"CHART-kpi-{index}",
                    spec["name"],
                    kpi_ids.get(spec["name"]),
                    KPI_CARD_WIDTH,
                    KPI_CARD_HEIGHT,
                )
                for index, spec in enumerate(KPI_CHARTS)
            ],
        )

    def _dq_row(self, position: dict[str, Any], kpi_ids: dict[str, int]) -> None:
        self._sync_row(
            position,
            tab_id="TAB-dq",
            row_id="ROW-dq-freshness",
            at_index=0,
            charts=[
                (
                    f"CHART-dq-{_slug(spec['name'])}",
                    spec["name"],
                    kpi_ids.get(spec["name"]),
                    DQ_CARD_WIDTH,
                    DQ_CARD_HEIGHT,
                )
                for spec in DQ_CHARTS
            ],
        )

    def _overview_layout(self, position: dict[str, Any]) -> None:
        """Приводит вкладку «Обзор» к объявленному составу.

        Обзор задаётся целиком, а не правится по кусочкам: иначе всё
        снова зарастает. Порядок сверху вниз — карточки, выводы, четыре
        тренда; всё остальное с вкладки убирается.

        Чарты при этом не удаляются, только снимаются с обзора: каждый
        из них живёт ещё и на своей тематической вкладке, а удаление
        чарта необратимо.
        """
        tab = position.get("TAB-overview")
        if not tab:
            return

        by_name = {name: ch["id"] for name, ch in self.client.charts(
            self.dashboard_id
        ).items()}

        wanted: list[str] = ["ROW-ov-kpi", "ROW-ov-findings"]
        for row_id, charts in OVERVIEW_TRENDS:
            if all(by_name.get(name) for _, name, _, _ in charts):
                wanted.append(row_id)

        self._sync_markdown(
            position,
            tab_id="TAB-overview",
            row_id="ROW-ov-findings",
            block_id="MARKDOWN-ov-findings",
            code=self._findings_markdown(position),
        )
        for row_id, charts in OVERVIEW_TRENDS:
            self._sync_row(
                position,
                tab_id="TAB-overview",
                row_id=row_id,
                at_index=len(tab["children"]),
                charts=[
                    (key, name, by_name.get(name), width, height)
                    for key, name, width, height in charts
                ],
            )

        present = [key for key in wanted if key in position]
        for key in list(tab.get("children", [])):
            if key in present:
                continue
            self._detach(position, key)
        tab["children"] = present

    def _place_rows(self, position: dict[str, Any]) -> None:
        """Приводит заявленные строки вкладок к описанной ширине."""
        by_name = {
            name: chart["id"]
            for name, chart in self.client.charts(self.dashboard_id).items()
        }
        for tab_id, row_id, after, charts in TAB_ROWS:
            tab = position.get(tab_id)
            if not tab:
                continue
            children = tab["children"]
            # Новая строка встаёт сразу за той, к которой привязана;
            # без якоря — в конец вкладки. У существующей строки место
            # не меняется: at_index читается только при вставке.
            at_index = (
                children.index(after) + 1
                if after in children
                else len(children)
            )
            self._sync_row(
                position,
                tab_id=tab_id,
                row_id=row_id,
                at_index=at_index,
                charts=[
                    (key, name, by_name.get(name), width, height)
                    for key, name, width, height in charts
                ],
            )

    @staticmethod
    def _findings_markdown(position: dict[str, Any]) -> str:
        """Текст блока выводов: считается по данным, а не хранится.

        Числа относятся к периоду данных и после перегенерации меняются.
        Написанные руками, они устаревали бы молча — на обзоре осталось
        бы уверенное утверждение с точностью до десятой процента,
        которое из данных больше не следует. Читатель его проверит, там
        для этого сказано где, и расхождение подорвёт доверие ко всему
        остальному на экране.

        Без доступа к ClickHouse (например, при запуске с ноутбука)
        прежний текст остаётся нетронутым: показать устаревшие числа
        плохо, но затереть их заглушкой — хуже.
        """
        import overview_findings

        try:
            if overview_findings.available():
                return overview_findings.render()
        except Exception as exc:  # noqa: BLE001 - причина не меняет решения
            print(f"  ! выводы не пересчитаны ({exc}) — текст оставлен как был")
            return position.get("MARKDOWN-ov-findings", {}).get("meta", {}).get(
                "code", ""
            )

        print("  ! ClickHouse недоступен — выводы оставлены как были")
        return position.get("MARKDOWN-ov-findings", {}).get("meta", {}).get(
            "code", ""
        )

    @staticmethod
    def _detach(position: dict[str, Any], key: str) -> None:
        """Убирает узел раскладки вместе с потомками."""
        node = position.pop(key, None)
        if not node:
            return
        for child in node.get("children", []):
            Runner._detach(position, child)

    @staticmethod
    def _sync_markdown(
        position: dict[str, Any],
        tab_id: str,
        row_id: str,
        block_id: str,
        code: str,
    ) -> None:
        tab = position.get(tab_id)
        if not tab:
            return
        row_parents = [*tab["parents"], tab_id]
        row = position.get(row_id)
        if row is None:
            row = position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [block_id],
                "parents": row_parents,
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
        row["children"] = [block_id]
        row["parents"] = row_parents

        block = position.get(block_id)
        if block is None:
            block = position[block_id] = {
                "type": "MARKDOWN",
                "id": block_id,
                "children": [],
                "parents": [],
                "meta": {},
            }
        block["parents"] = [*row_parents, row_id]
        block["meta"].update({"width": 12, "height": MARKDOWN_HEIGHT, "code": code})

    def _sync_row(
        self,
        position: dict[str, Any],
        tab_id: str,
        row_id: str,
        at_index: int,
        charts: list[tuple[str, str, int | None, int, int]],
    ) -> None:
        """Приводит строку карточек к описанию: состав, размеры, подписи.

        Прежняя версия умела только создавать строку и выходила, если та
        уже есть, — та же болезнь, что была у самих карточек: изменившая-
        ся ширина оставалась в коде и не доезжала до дашборда. Из-за
        этого пять карточек так и стояли по две колонки из двенадцати,
        то есть по 149 px на 1280 px экрана, где заголовок обрезался у
        всех до единого.

        Карточка, пропавшая из описания, удаляется из раскладки, но не
        из Superset: она может понадобиться на другой вкладке, а
        удаление чарта необратимо.
        """
        if any(chart_id is None for _, _, chart_id, _, _ in charts):
            # Хотя бы одну карточку не нашли и не создали. Перекладывать
            # строку по неполному списку — значит выкинуть из неё живую
            # карточку из-за чужой ошибки.
            return

        if row_id not in position:
            self._insert_row(position, tab_id, row_id, at_index, charts)
            return

        row = position[row_id]
        expected = [key for key, *_ in charts]

        for key in list(position):
            node = position.get(key)
            if (
                isinstance(node, dict)
                and node.get("type") == "CHART"
                and row_id in node.get("parents", [])
                and key not in expected
            ):
                del position[key]

        for key, name, chart_id, width, height in charts:
            node = position.get(key)
            if node is None:
                node = position[key] = {
                    "type": "CHART",
                    "id": key,
                    "children": [],
                    "parents": [*row["parents"], row_id],
                    "meta": {},
                }
            node["parents"] = [*row["parents"], row_id]
            meta = node["meta"]
            meta.update({"chartId": chart_id, "width": width, "height": height})
            # sliceNameOverride, если он задан, показывается вместо
            # sliceName — переименование без него видно только в списке
            # чартов, но не на дашборде.
            meta["sliceName"] = name
            if "sliceNameOverride" in meta:
                meta["sliceNameOverride"] = name

        row["children"] = expected

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
        "--credentials",
        default=str(CREDENTIALS_PATH),
        help=f"файл с url/username/password (по умолчанию {CREDENTIALS_PATH})",
    )
    parser.add_argument(
        "--delete-charts",
        default="",
        help="удалить чарты по id через запятую; ссылающиеся на дашборд не трогает",
    )
    parser.add_argument(
        "--narrow-public",
        default="",
        metavar="РОЛЬ",
        help="оставить роли только права, нужные для просмотра дашборда",
    )
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

    # Порядок: явный аргумент, потом окружение, потом файл кредов.
    # Файл — последний, чтобы разовый запуск с другими параметрами не
    # требовал его править.
    try:
        stored = load_credentials(pathlib.Path(args.credentials).expanduser())
    except SupersetError as exc:
        return _fail(str(exc))

    args.url = args.url or stored.get("url")
    args.username = args.username or stored.get("username")
    args.password = args.password or stored.get("password")

    if not (args.url and args.username):
        return _fail(
            "Нужны SUPERSET_URL и SUPERSET_USERNAME (или --url/--username, "
            f"или {args.credentials})"
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

    if args.delete_charts:
        try:
            ids = [int(x) for x in args.delete_charts.split(",") if x.strip()]
        except ValueError:
            return _fail("--delete-charts принимает id через запятую")
        print("Удаление чартов:")
        return delete_charts(client, args.dashboard, ids, args.apply)

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
    runner.sync_dataset_columns(phases)

    print("\nЧарты:")
    runner.patch_charts(phases)

    kpi_ids: dict[str, int] = {}
    if "p2" in phases:
        print("\nKPI-карточки:")
        kpi_ids = runner.sync_big_numbers(KPI_CHARTS + DQ_CHARTS)

    print("\nДашборд:")
    runner.patch_dashboard(phases, kpi_ids)

    if args.public_role:
        print("\nПрава:")
        runner.grant_public_access(args.public_role)

    if args.narrow_public:
        print("\nСужение прав:")
        runner.narrow_public_access(args.narrow_public)

    print(f"\nИтого изменений: {len(runner.planned)}")
    if not args.apply and runner.planned:
        print("Повтори с --apply, чтобы записать.")
    return 0


def delete_charts(
    client: Superset, dashboard_id: int, ids: list[int], apply: bool
) -> int:
    """Удаляет чарты по явному списку id.

    Только по id и только по явному списку: удаление необратимо, и
    угадывать «лишнее» по имени тут нельзя. Чарт, на который ссылается
    раскладка дашборда, не удаляется ни при каких условиях — именно так
    выглядела бы опечатка в списке.
    """
    detail = client.get(f"/api/v1/dashboard/{dashboard_id}")["result"]
    position = json.loads(detail.get("position_json") or "{}")
    in_use = {
        node["meta"]["chartId"]
        for node in position.values()
        if isinstance(node, dict)
        and node.get("type") == "CHART"
        and node.get("meta", {}).get("chartId")
    }

    query = json.dumps({"columns": ["id", "slice_name"], "page_size": 100})
    names = {
        row["id"]: row["slice_name"]
        for row in client.get(f"/api/v1/chart/?q={query}")["result"]
    }

    doomed = []
    for chart_id in ids:
        if chart_id not in names:
            print(f"  ! чарта {chart_id} нет — пропуск")
            continue
        if chart_id in in_use:
            print(
                f"  ! чарт {chart_id} «{names[chart_id]}» стоит на дашборде "
                "— не удаляю"
            )
            continue
        doomed.append(chart_id)
        print(f"  {'✓' if apply else '·'} {chart_id} «{names[chart_id]}»")

    if not doomed:
        print("Удалять нечего.")
        return 0

    if not apply:
        print(f"\nБудет удалено: {len(doomed)}. Повтори с --apply.")
        return 0

    for chart_id in doomed:
        client.delete(f"/api/v1/chart/{chart_id}")
    print(f"\nУдалено: {len(doomed)}")
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

    # Полная выгрузка: имена ресурсов различаются между версиями, и
    # искать нужное по одному предположению за раз слишком дорого.
    try:
        rows = client._permission_rows()
        roles_dump = {}
        for role in client.get(
            "/api/v1/security/roles/?q=" + json.dumps({"page_size": 100})
        )["result"]:
            roles_dump[role["name"]] = sorted(
                client.role_permission_ids(role["id"])
            )
        dump = {
            "permissions": [
                {
                    "id": r.get("id"),
                    "permission": (r.get("permission") or {}).get("name"),
                    "resource": (r.get("view_menu") or {}).get("name"),
                }
                for r in rows
            ],
            "roles": roles_dump,
        }
        with open("superset_permissions.json", "w", encoding="utf-8") as fh:
            json.dump(dump, fh, ensure_ascii=False, indent=1)
        print(
            f"Полная выгрузка прав: superset_permissions.json "
            f"({len(rows)} разрешений, {len(roles_dump)} ролей)"
        )
    except (SupersetError, AttributeError, OSError) as exc:
        print(f"Выгрузка прав не удалась — {exc}")

    # Имя ресурса, отвечающего за разбор выражений периода, различается
    # между версиями Superset. Печатаем всё похожее, чтобы не угадывать.
    try:
        rows = client._permission_rows()
        interesting = sorted(
            {
                f"{(r.get('permission') or {}).get('name')} on "
                f"{(r.get('view_menu') or {}).get('name')}"
                for r in rows
                if "time" in ((r.get("view_menu") or {}).get("name") or "").lower()
                or "range" in ((r.get("view_menu") or {}).get("name") or "").lower()
            }
        )
        print(f"Похожие на «период» ресурсы ({len(interesting)}):")
        for line in interesting:
            print(f"    {line}")
    except (SupersetError, AttributeError) as exc:
        print(f"Ресурсы периода: не удалось прочитать — {exc}")

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
