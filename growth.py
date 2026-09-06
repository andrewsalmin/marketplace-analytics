"""Чтение бизнес-правил из growth_config.json.

Модуль намеренно без зависимостей: его импортируют и загрузчик
(load_to_clickhouse.py, тянущий за собой pyspark), и скрипт правок
Superset (superset/apply_review_fixes.py, которому нужен только
requests). Общий здесь только stdlib, поэтому ни один из них не
навязывает другому своё окружение.

generate_data.py свой горизонт считает сам (compute_lookback_days) — он
работает не от этого файла, а от аргументов CLI, которые могут
переопределить любое правило для конкретного прогона. Формула там та же.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "growth_config.json"


def load_growth_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def maturity_days(growth_config: dict | None = None) -> int:
    """Через сколько дней судьба заказа гарантированно определена.

    Сумма всех дедлайнов подряд — худший случай, при котором заказ
    проходит все состояния до последнего. На дефолтах это 34 дня, а не
    круглая неделя: отмена «не забрали вовремя» срабатывает на 17-й день,
    а возврат возможен вплоть до 34-го. До истечения этого горизонта
    заказ ещё может отмениться или вернуться, поэтому доли отмен и
    возвратов на свежем хвосте занижены (см. README.md, «Известные
    ограничения»).
    """
    config = growth_config if growth_config is not None else load_growth_config()

    return (
        math.ceil(config["payment_deadline_hours"] / 24)
        + math.ceil(config["shipping_deadline_hours"] / 24)
        + config["delivery_deadline_days"]
        + config["pickup_deadline_days"]
        + config["return_window_days"]
        + math.ceil(config["refund_processing_hours"] / 24)
    )


def payment_settlement_days(growth_config: dict | None = None) -> int:
    """Через сколько дней ясно, оплачен заказ или нет.

    Отдельный, гораздо более короткий горизонт, чем maturity_days: судьба
    оплаты решается дедлайном оплаты, а не суммой всех дедлайнов вплоть до
    окна возврата. Мерить оплаченный GMV с отсечкой в 34 дня значит
    выбрасывать месяц данных ради вопроса, ответ на который известен на
    следующий день.

    День про запас — на заказы, созданные под конец суток: их дедлайн
    истекает уже в следующей календарной дате.
    """
    config = growth_config if growth_config is not None else load_growth_config()

    return math.ceil(config["payment_deadline_hours"] / 24) + 1


def customer_maturity_days(growth_config: dict | None = None) -> int:
    """Через сколько дней после регистрации покупателя можно оценивать.

    В отличие от горизонта заказа, это не сумма дедлайнов, а выбранное
    окно наблюдения: конверсия «регистрация -> первый заказ» на свежих
    регистрациях занижена просто потому, что они ещё не успели купить.
    """
    config = growth_config if growth_config is not None else load_growth_config()

    return config["customer_maturity_days"]
