"""Выводы для обзорного экрана дашборда — считаются, а не пишутся.

Блок «Что видно в данных» несёт конкретные числа: конверсию, долю
отмен по причинам, вклад Москвы в оборот. Числа относятся к периоду
данных, и после каждой перегенерации они меняются.

Написанные руками, они устаревают молча: на дашборде остаётся уверенное
утверждение с точностью до десятой доли процента, которое больше не
следует из данных. Проверить его читатель может (там сказано, где), и
именно поэтому расхождение особенно неприятно — оно подрывает доверие
ко всему остальному.

Поэтому здесь только формулировки и SQL, а числа подставляются при
применении правок:

    python overview_findings.py          # напечатать блок

Формулировки — намеренно описательные. Сказать «канал X самый выгодный»
нельзя, не зная расходов на привлечение, а их в данных нет вовсе: это
был бы не вывод, а догадка, поданная с точностью до процента.
"""

from __future__ import annotations

import sys

from marts_checks import MARTS, available, query

# Каждый вывод: как звучит, чем считается, где перепроверить.
#
# «Где перепроверить» — обязательная часть, а не вежливость: утверждение,
# которое нельзя сверить с графиком рядом, на дашборде бесполезно.
FINDINGS: list[dict[str, str]] = [
    {
        "claim": "Конверсия в первый заказ — {value}%",
        "sql": f"""
            SELECT round(sum(has_order) / count() * 100, 1)
            FROM {MARTS}.customer_first_order
            WHERE is_mature = 1
        """,
        "detail": (
            "Из {total} покупателей, зарегистрировавшихся достаточно давно, "
            "чтобы успеть сделать заказ, его сделали {converted}; "
            "остальные {rest} не сделали ни одного."
        ),
        "detail_sql": f"""
            SELECT count(), sum(has_order), count() - sum(has_order)
            FROM {MARTS}.customer_first_order
            WHERE is_mature = 1
        """,
        "where": "Проверяется на вкладке «Покупатели».",
    },
    {
        "claim": "Две трети отмен — это неоплата в срок",
        "sql": f"""
            SELECT round(
                countIf(cancellation_reason_ru = 'Не оплачен вовремя')
                / countIf(cancelled_at IS NOT NULL) * 100, 1)
            FROM {MARTS}.orders_with_customer_dim
            WHERE is_mature = 1 AND cancelled_at IS NOT NULL
        """,
        "detail": (
            "{value}% отменённых заказов отменены с причиной "
            "«Не оплачен вовремя»."
        ),
        "where": (
            "Проверяется на вкладке «Заказы», график «Причины отмены "
            "заказов»."
        ),
    },
    {
        "claim": "Треть оборота даёт Москва — {value}% GMV",
        "sql": f"""
            SELECT round(
                SUMIf(amount_kopecks, city = 'Москва' AND paid_at IS NOT NULL)
                / SUMIf(amount_kopecks, paid_at IS NOT NULL) * 100, 1)
            FROM {MARTS}.orders_with_customer_dim
            WHERE is_payment_settled = 1
        """,
        "where": (
            "Проверяется на вкладке «Покупатели», график «GMV по городам "
            "(топ-10)»."
        ),
    },
]

PERIOD_SQL = f"""
    SELECT min(toDate(created_at)), max(toDate(created_at))
    FROM {MARTS}.orders_with_customer_dim
"""

MONTHS = [
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
]


def _ru_date(iso: str) -> str:
    """«2026-06-01» → «1 июня 2026»."""
    year, month, day = (int(part) for part in iso.split("-"))
    return f"{day} {MONTHS[month - 1]} {year}"


def _ru_number(text: str) -> str:
    """Разделители по-русски: запятая в дроби, пробел в разрядах.

    Тот же вид, что у чисел на карточках рядом (их так рисует D3_FORMAT
    из конфигурации Superset). Разнобой в одном экране заметен сразу.
    """
    text = text.strip()
    if "." in text:
        whole, _, fraction = text.partition(".")
        return f"{_ru_number(whole)},{fraction}"
    digits = text.lstrip("-")
    grouped = f"{int(digits):,}".replace(",", " ") if digits.isdigit() else text
    return ("-" if text.startswith("-") else "") + grouped


def render() -> str:
    """Собирает markdown блока выводов по живым данным."""
    first, last = query(PERIOD_SQL).strip().split("\t")
    lines = [f"### Что видно в данных за {_ru_date(first)} — {_ru_date(last)}", ""]

    for finding in FINDINGS:
        value = _ru_number(query(finding["sql"]).strip())
        claim = finding["claim"].format(value=value)
        parts = [f"- **{claim}.**"]

        detail = finding.get("detail")
        if detail:
            numbers = {"value": value}
            if finding.get("detail_sql"):
                raw = query(finding["detail_sql"]).strip().split("\t")
                total, converted, rest = (_ru_number(x) for x in raw)
                numbers |= {
                    "total": total,
                    "converted": converted,
                    "rest": rest,
                }
            parts.append(detail.format(**numbers))

        parts.append(finding["where"])
        lines.append(" ".join(parts))

    return "\n".join(lines)


def main() -> int:
    if not available():
        print(
            "ClickHouse недоступен — выводы посчитать не из чего.",
            file=sys.stderr,
        )
        return 1
    print(render())
    return 0


if __name__ == "__main__":
    sys.exit(main())
