"""Проверки семантического слоя на реальных данных.

`tests/test_marts.py` проверяет текст SQL — что FINAL на месте, что
дедлайны не захардкожены. Здесь проверяется результат: что витрины
считают то, что обещают. Каждая проверка соответствует ошибке, которая
уже была допущена и исправлена, — они существуют, чтобы та же ошибка не
вернулась молча.

Модуль, а не тест, потому что запускать это нужно в первую очередь из
пайплайна: разъехавшаяся витрина должна ронять загрузку, а не тихо
доезжать до дашборда. `tests/test_marts_data.py` оборачивает те же
проверки в pytest для ручного прогона.

    python marts_checks.py

Подключение берётся из clickhouse_config.json. Пароль — из
CLICKHOUSE_PASSWORD либо из настроек clickhouse-client, если он есть в
PATH: на сервере креды уже лежат в ~/.clickhouse-client/config.xml.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
MARTS = "marketplace_marts"


# ---------------------------------------------------------------------
# Доступ к ClickHouse
# ---------------------------------------------------------------------


def config() -> dict:
    path = REPO_ROOT / "clickhouse_config.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def query_via_client(sql: str) -> str:
    done = subprocess.run(
        ["clickhouse-client", "--query", sql],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if done.returncode != 0:
        # Не CalledProcessError: её текст сообщает только код выхода, а
        # разбираться приходится по тому, что сказал ClickHouse.
        # ClickHouse печатает эхо запроса последней строкой, а само
        # сообщение выше — берём строку с кодом ошибки, если она есть.
        lines = [ln.strip() for ln in (done.stderr or "").splitlines() if ln.strip()]
        detail = next(
            (ln for ln in lines if "Code:" in ln or "Exception" in ln),
            lines[0] if lines else f"код выхода {done.returncode}",
        )
        raise RuntimeError(detail)
    return done.stdout


def query_via_http(sql: str) -> str:
    import urllib.request

    cfg = config()
    url = f"http://{cfg.get('host', 'localhost')}:{cfg.get('port', 8123)}/"
    request = urllib.request.Request(url, data=sql.encode("utf-8"))
    request.add_header("X-ClickHouse-User", cfg.get("user", "default"))
    request.add_header("X-ClickHouse-Key", os.environ.get("CLICKHOUSE_PASSWORD", ""))
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read().decode("utf-8")


def query(sql: str) -> str:
    if shutil.which("clickhouse-client"):
        return query_via_client(sql)
    return query_via_http(sql)


def available() -> bool:
    try:
        return query(f"SELECT count() FROM {MARTS}.orders").strip().isdigit()
    except Exception:  # noqa: BLE001 - любая беда означает «данных нет»
        return False




# ---------------------------------------------------------------------
# Проверки
#
# Каждая возвращает одну строку: признак 0/1 и человекочитаемая деталь,
# которая попадёт в сообщение упавшего теста. Формат один на все,
# поэтому список можно прогнать и без pytest.
# ---------------------------------------------------------------------

CHECKS: dict[str, tuple[str, str]] = {
    "дедупликация: одна строка на заказ": (
        "заказ, переизданный при продвижении статуса, не должен "
        "удваиваться — ради этого во всём слое стоит FINAL",
        f"""
        SELECT count() = uniqExact(order_id),
               toString(count()) || ' строк / ' || toString(uniqExact(order_id))
                   || ' order_id'
        FROM {MARTS}.orders
        """,
    ),
    "конверсия: не равна единице": (
        "ровно 100% означало бы, что признак заказа снова выводится из "
        "даты, а LEFT JOIN подставляет туда эпоху вместо NULL",
        f"""
        SELECT sum(has_order) < count(),
               toString(round(100 * sum(has_order) / count(), 2)) || '%'
        FROM {MARTS}.customer_first_order WHERE is_mature
        """,
    ),
    "конверсия: признак согласован с датой": (
        "has_order и «первый заказ известен» обязаны совпадать до строки",
        f"""
        SELECT countIf(has_order != (first_order_at IS NOT NULL)) = 0,
               toString(countIf(has_order != (first_order_at IS NOT NULL)))
                   || ' расхождений'
        FROM {MARTS}.customer_first_order
        """,
    ),
    "конверсия: окно уже всей истории": (
        "купивших за окно наблюдения не может быть больше, чем купивших "
        "когда-либо",
        f"""
        SELECT countIf(converted_within_window) <= countIf(has_order),
               toString(countIf(converted_within_window)) || ' <= '
                   || toString(countIf(has_order))
        FROM {MARTS}.customer_first_order WHERE is_mature
        """,
    ),
    "даты: в первом заказе нет эпохи": (
        "1970-01-01 у не купившего клиента — след того самого LEFT JOIN",
        f"""
        SELECT countIf(first_order_at = toDateTime(0)) = 0,
               toString(countIf(first_order_at = toDateTime(0))) || ' дат 1970'
        FROM {MARTS}.customer_first_order
        """,
    ),
    "измерения: джойн не размножил заказы": (
        "дубль в customers раздул бы факт, и GMV вырос бы на ровном месте",
        f"""
        SELECT (SELECT count() FROM {MARTS}.orders_with_customer_dim)
                   = (SELECT count() FROM {MARTS}.orders),
               toString((SELECT count() FROM {MARTS}.orders_with_customer_dim))
                   || ' vs ' || toString((SELECT count() FROM {MARTS}.orders))
        """,
    ),
    "измерения: у каждого заказа есть город": (
        "заказ без города выпадет из разреза, и сумма по городам "
        "перестанет сходиться с общей",
        f"""
        SELECT countIf(city = '') = 0,
               toString(countIf(city = '')) || ' заказов без города'
        FROM {MARTS}.orders_with_customer_dim
        """,
    ),
    "GMV: чистый меньше оплаченного": (
        "если равны — возвраты и отмены снова считаются выручкой",
        f"""
        SELECT sumIf(amount_kopecks, paid_at IS NOT NULL
                     AND cancelled_at IS NULL AND returned_at IS NULL
                     AND refunded_at IS NULL)
               < sumIf(amount_kopecks, paid_at IS NOT NULL),
               toString(round(sumIf(amount_kopecks, paid_at IS NOT NULL) / 100))
                   || ' -> ' || toString(round(sumIf(amount_kopecks,
                        paid_at IS NOT NULL AND cancelled_at IS NULL
                        AND returned_at IS NULL AND refunded_at IS NULL) / 100))
        FROM {MARTS}.orders_with_customer_dim WHERE is_mature
        """,
    ),
    "оплата: возврат не стирает успешную попытку": (
        "после FINAL возмещённый платёж меняет статус, и старая формула "
        "теряла факт успешной авторизации задним числом",
        f"""
        SELECT countIf(attempt_succeeded)
                   = countIf(status IN ('success', 'refunded')),
               toString(round(100 * countIf(attempt_succeeded) / count(), 2))
                   || '% против '
                   || toString(round(100 * countIf(status = 'success')
                        / countIf(status IN ('success', 'failed')), 2)) || '%'
        FROM {MARTS}.payments
        """,
    ),
    "попытки: ни один заказ не потерян": (
        "прежняя версия отбирала только success и failed, и заказ с "
        "возвратом исчезал из распределения целиком",
        f"""
        SELECT (SELECT sum(orders_count) FROM {MARTS}.payment_retries_distribution)
                   = (SELECT uniqExact(order_id) FROM {MARTS}.payments),
               toString((SELECT sum(orders_count)
                         FROM {MARTS}.payment_retries_distribution))
                   || ' vs ' || toString((SELECT uniqExact(order_id)
                                          FROM {MARTS}.payments))
        """,
    ),
    "попытки: минимум одна": (
        "ноль попыток означал бы заказ без единого платежа в витрине "
        "платежей — такого быть не может",
        f"""
        SELECT min(attempts) >= 1, 'минимум ' || toString(min(attempts))
        FROM {MARTS}.payment_retries_distribution
        """,
    ),
    "воронка: этапы монотонно убывают": (
        "оплаченных не бывает больше созданных; нарушение означает "
        "рассинхрон отсечки по зрелости между этапами",
        f"""
        SELECT groupArray(cnt) = arrayReverseSort(groupArray(cnt)),
               arrayStringConcat(arrayMap(x -> toString(x),
                   groupArray(cnt)), ' >= ')
        FROM (SELECT cnt FROM {MARTS}.order_funnel_stages ORDER BY stage_order)
        """,
    ),
    "время: платёж не раньше заказа": (
        "платёж, датированный раньше своего заказа, — сломанная "
        "хронология, а не редкий случай",
        f"""
        SELECT countIf(p.payment_date < o.created_at) = 0,
               toString(countIf(p.payment_date < o.created_at)) || ' платежей'
        FROM {MARTS}.payments AS p
        INNER JOIN {MARTS}.orders AS o ON o.order_id = p.order_id
        """,
    ),
    "тепловая карта: ни один заказ не потерян": (
        "витрина предагрегирована по дню недели, часу, городу и каналу; "
        "лишнее измерение в группировке размножило бы заказы, "
        "недостающее — потеряло",
        f"""
        SELECT (SELECT sum(orders) FROM {MARTS}.orders_by_hour_dow)
                   = (SELECT count() FROM {MARTS}.orders),
               toString((SELECT sum(orders) FROM {MARTS}.orders_by_hour_dow))
                   || ' vs ' || toString((SELECT count() FROM {MARTS}.orders))
        """,
    ),
    "retention: зрелые ячейки образуют полную сетку": (
        "с INNER JOIN зрелая неделя без активности исчезала вместо нуля; "
        "сетка обязана содержать все смещения от нуля до максимума",
        f"""
        SELECT countIf(retention_rate > 1) = 0
               AND min(weeks_since_signup) = 0,
               'смещения с ' || toString(min(weeks_since_signup)) || ' по '
                   || toString(max(weeks_since_signup)) || ', вне [0..1]: '
                   || toString(countIf(retention_rate > 1))
        FROM {MARTS}.customer_cohort_retention
        """,
    ),
    "SLA: незакрытые зрелые заказы объяснены карантином": (
        "заказ старше горизонта обязан быть в конечном статусе. "
        "Исключение одно и оно законное: закрывающая строка несла "
        "внедрённый DQ-дефект, transform.py её отбраковал, и в "
        "хранилище осталась последняя валидная версия — открытая. "
        "Незакрытый заказ БЕЗ строки в карантине означал бы настоящую "
        "дыру в state machine генератора",
        f"""
        SELECT countIf(q.order_id = '') = 0,
               toString(countIf(q.order_id = '')) || ' без объяснения из '
                   || toString(count()) || ' незакрытых'
        FROM (
            SELECT order_id FROM {MARTS}.orders
            WHERE is_mature AND status NOT IN ('delivered', 'cancelled', 'refunded')
        ) AS o
        LEFT JOIN (
            SELECT DISTINCT order_id FROM marketplace_analytics.quarantine_orders
        ) AS q ON q.order_id = o.order_id
        """,
    ),
    "качество: причины разложены на атомарные": (
        "составная причина «A | B» — это две ошибки, а не третья "
        "категория; arrayJoin обязан их разделить",
        f"""
        SELECT countIf(position(reason, ' | ') > 0) = 0,
               toString(countIf(position(reason, ' | ') > 0)) || ' составных'
        FROM {MARTS}.dq_reason_breakdown
        """,
    ),
}


def evaluate(name: str, why: str, raw: str) -> None:
    """Поднимает AssertionError, если проверка не прошла.

    Вынесено из теста, потому что тест, неспособный упасть, хуже
    отсутствующего: он создаёт видимость контроля. Эта функция
    покрыта в test_marts.py и потому проверяется даже там, где
    ClickHouse недоступен.
    """
    raw = (raw or "").strip()
    assert raw, f"{name}: запрос ничего не вернул"

    ok, _, detail = raw.partition("\t")
    assert ok == "1", f"{name}: {detail or raw}\n  почему важно: {why}"


def run_all() -> list[str]:
    """Прогоняет все проверки, возвращает описания непрошедших."""
    failures = []
    for name, (why, sql) in CHECKS.items():
        try:
            evaluate(name, why, query(sql))
        except AssertionError as exc:
            failures.append(str(exc))
        except Exception as exc:  # noqa: BLE001
            # Витрину удалили, SQL сломали, база не отвечает — для
            # пайплайна это такое же расхождение, как неверная цифра.
            # Трейсбек здесь ничего не добавит: важно, какая проверка и
            # что ответило хранилище.
            failures.append(f"{name}: запрос не выполнился — {exc}")
        else:
            print(f"  OK  {name}")
    return failures


def main() -> int:
    if not available():
        print(
            "ClickHouse с витринами недоступен — проверять нечего.",
            file=sys.stderr,
        )
        return 2

    print(f"Проверок: {len(CHECKS)}")
    failures = run_all()
    if failures:
        print(f"\nНе прошло: {len(failures)}", file=sys.stderr)
        for line in failures:
            print(f"  ОШИБКА {line}", file=sys.stderr)
        return 1

    print("Все проверки прошли.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
