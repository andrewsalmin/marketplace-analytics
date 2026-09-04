from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import shutil
import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Статусы / причины
# ---------------------------------------------------------------------

VALID_ORDER_STATUSES = [
    "new",
    "paid",
    "shipped",
    "ready_for_pickup",
    "delivered",
    "cancelled",
    "returned",
    "refunded",
]

# Только статусы, которые реально производит код генерации платежей.
VALID_PAYMENT_STATUSES = ["success", "failed", "refunded"]

# Ровно одна причина на каждую ветку кода, которая может привести к
# cancelled — без "other"/"out_of_stock"/"fraud_suspected": в датасете
# нет ни товарного измерения, ни фрод-модели, такие причины были бы
# decoration без опоры на реальные данные.
CANCELLATION_REASONS = [
    "not_paid_in_time",
    "cancelled_by_customer_before_payment",
    "cancelled_by_customer_after_payment",
    "not_picked_up_in_time",
]

# Города-миллионники России
CITIES = [
    "Москва",
    "Санкт-Петербург",
    "Новосибирск",
    "Екатеринбург",
    "Казань",
    "Нижний Новгород",
    "Челябинск",
    "Красноярск",
    "Самара",
    "Уфа",
    "Ростов-на-Дону",
    "Омск",
    "Краснодар",
    "Воронеж",
    "Пермь",
    "Волгоград",
]

# Грубые относительные доли клиентов по городам (население + онлайн-
# платёжеспособный спрос) — без них rng.choice брал бы города
# равновероятно, что нереалистично: Москва и СПб непропорционально
# доминируют в e-commerce РФ. Не обязаны суммироваться в 1.0 —
# нормализуются в generate_customers().
CITY_WEIGHTS = [
    0.30,   # Москва
    0.14,   # Санкт-Петербург
    0.055,  # Новосибирск
    0.05,   # Екатеринбург
    0.045,  # Казань
    0.04,   # Нижний Новгород
    0.035,  # Челябинск
    0.035,  # Красноярск
    0.035,  # Самара
    0.035,  # Уфа
    0.035,  # Ростов-на-Дону
    0.03,   # Омск
    0.03,   # Краснодар
    0.025,  # Воронеж
    0.025,  # Пермь
    0.025,  # Волгоград
]

ACQUISITION_CHANNELS = [
    "organic",
    "yandex_direct",
    "vk_ads",
    "telegram_ads",
    "social",
    "referral",
    "email",
]  # google_ads не используется — недоступен в РФ с 2022

# Реалистичные доли клиентов по каналам привлечения (в сумме 1.0),
# вместо равновероятного выбора — иначе распределение на дашборде
# получается плоским, чего не бывает в реальном маркетинг-миксе.
ACQUISITION_CHANNEL_WEIGHTS = [
    0.35,  # organic
    0.22,  # yandex_direct
    0.10,  # vk_ads
    0.07,  # telegram_ads
    0.13,  # social
    0.10,  # referral
    0.03,  # email
]

# Множитель к весу «частоты заказов» (_customer_order_weights) по каналу
# привлечения. Органика/рефералы/email — уже вовлечённая или доверяющая
# бренду аудитория (искала сама, пришла по рекомендации, уже подписана) —
# выше склонность к повторным заказам. Платный social (vk/telegram/social
# ads) — типичный «один клик — один заказ», ниже LTV. yandex_direct
# (performance-поиск с явным интентом) — посередине. Соответствует
# типичному распределению качества трафика по каналам в e-commerce.
ACQUISITION_CHANNEL_LOYALTY_MULTIPLIER = {
    "organic": 1.3,
    "referral": 1.4,
    "email": 1.2,
    "yandex_direct": 1.0,
    "vk_ads": 0.6,
    "telegram_ads": 0.6,
    "social": 0.7,
}

# Множитель к базовой доле клиентов, которые не сделают ни одного заказа
# (--never-order-rate). Регистрация — ещё не покупка: часть аудитории
# заводит аккаунт и уходит, и без этого конверсия «регистрация → первый
# заказ» в данных всегда равна 100%. Логика качества трафика та же, что
# в LOYALTY_MULTIPLIER выше, но с другого конца воронки: платный social
# приводит «мёртвых» регистраций заметно больше, чем органика и
# рефералы, — поэтому множители тут обратны по смыслу.
ACQUISITION_CHANNEL_NON_CONVERSION_MULTIPLIER = {
    "organic": 0.7,
    "referral": 0.6,
    "email": 0.8,
    "yandex_direct": 1.0,
    "vk_ads": 1.6,
    "telegram_ads": 1.6,
    "social": 1.4,
}

PAYMENT_METHODS = ["card", "sbp", "mir_pay", "cash"]
# apple_pay/google_pay не используются — NFC-платежи недоступны в РФ
# с 2022, mir_pay — реальная российская NFC-альтернатива

CUSTOMER_COLUMNS = [
    "customer_id",
    "registration_date",
    "city",
    "acquisition_channel",
    "email",
]

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "created_at",
    "amount_kopecks",
    "status",
    "cancellation_reason",
    "payment_method",
    "paid_at",
    "shipped_at",
    "ready_for_pickup_at",
    "delivered_at",
    "cancelled_at",
    "returned_at",
    "refunded_at",
]

ORDER_TIMESTAMP_COLUMNS = [
    "created_at",
    "paid_at",
    "shipped_at",
    "ready_for_pickup_at",
    "delivered_at",
    "cancelled_at",
    "returned_at",
    "refunded_at",
]

PAYMENT_COLUMNS = [
    "payment_id",
    "order_id",
    "payment_date",
    "amount_kopecks",
    "payment_method",
    "status",
]

# Распределение времени суток: пик день/вечер, минимум ночь. Используется
# для created_at заказов, момента попытки оплаты и registration_date
# клиентов. Веса не обязаны суммироваться в 1.0 — нормализуются в
# _realistic_timestamps(). Будни и выходные используют разные профили
# (см. WEEKEND_HOUR_WEIGHTS) — иначе почасовая структура всех семи дней
# получается статистически неотличимой (корреляция >99%), что для
# реального маркетплейса неправдоподобно.
WEEKDAY_HOUR_WEIGHTS = [
    0.006, 0.005, 0.005, 0.005, 0.006, 0.010,  # 00-05: ночь
    0.022, 0.035, 0.045, 0.040,                # 06-09: утренний рывок перед работой
    0.035, 0.038, 0.070, 0.060,                # 10-13: рабочий провал + обед. всплеск
    0.040, 0.038, 0.045, 0.060,                # 14-17: рабочий день, разгон к вечеру
    0.085, 0.095, 0.090, 0.075,                # 18-21: вечерний пик после работы
    0.050, 0.025,                              # 22-23: спад
]

# Выходные: нет рабочего провала и обеденного всплеска — активность
# растянута по дню без острого пика, начинается позже (люди высыпаются)
# и держится ровнее до вечера.
WEEKEND_HOUR_WEIGHTS = [
    0.010, 0.008, 0.006, 0.005, 0.005, 0.006,  # 00-05: ночь (чуть дольше не спят)
    0.010, 0.015, 0.025, 0.035,                # 06-09: медленный старт, без будильника
    0.050, 0.060, 0.065, 0.065,                # 10-13: позднее утро/полдень разгоняются
    0.070, 0.075, 0.075, 0.070,                # 14-17: дневное плато — досуговый шопинг
    0.070, 0.065, 0.060, 0.050,                # 18-21: вечер держится, без острого пика
    0.040, 0.025,                              # 22-23: спад
]

# Нижние границы сдвига для каждого перехода state machine — без них
# формально допускался бы нулевой/мгновенный переход (оплата в ту же
# секунду, что создание заказа, и т.п.), что нереалистично.
PAYMENT_MIN_GAP = timedelta(seconds=30)     # минимум на чек-аут
SHIPPING_MIN_GAP = timedelta(hours=4)       # складская обработка перед отгрузкой
DELIVERY_MIN_GAP = timedelta(hours=12)      # физический транзит до пункта выдачи
PICKUP_MIN_GAP = timedelta(hours=1)         # клиенту нужно время доехать до ПВЗ
RETURN_MIN_GAP = timedelta(hours=2)         # клиент не вернёт мгновенно при получении
REFUND_MIN_GAP = timedelta(hours=1)         # обработка возврата денег не мгновенна

# Вероятность успеха одной попытки оплаты для заказа в статусе "new" —
# близко к реальной успешности онлайн-оплаты картой/СБП.
PAYMENT_SUCCESS_RATE = 0.95

# Ежедневная вероятность того, что заказ, ожидающий гарантированного (не
# вероятностного) перехода (отправка/доставка/забор/рефанд), продвинется
# именно сегодня, а не в один из следующих дней. Переход всё равно
# форсируется на дедлайне, так что это влияет только на РАСПРЕДЕЛЕНИЕ
# дня перехода внутри окна, не на факт, что он произойдёт.
DAILY_ADVANCE_PROBABILITY = 0.6

DQ_ENTITIES = ["customers", "orders", "payments"]

# ---------------------------------------------------------------------
# Реалистичная динамика error_rate (включается флагом --launch-date)
#
# Молодая площадка стартует с более высоким уровнем брака в данных —
# интеграции ещё не обкатаны — который сглаживается к базовому по мере
# взросления (RAMP_ERROR_*), плюс день-в-день логнормальный шум
# (ERROR_RATE_NOISE_SIGMA) и редкие инциденты у отдельных источников
# (сорвался партнёрский API, деплой с багом и т.п. — INCIDENT_*). Без
# --launch-date ничего из этого не применяется, error_rate остаётся
# ровно тем, что передали в --error-rate (обратная совместимость).
# ---------------------------------------------------------------------

RAMP_ERROR_INITIAL_MULTIPLIER = 4.0  # во сколько раз error_rate выше в день запуска
RAMP_ERROR_DECAY_DAYS = 21  # характерное время спада к базовому уровню

ERROR_RATE_NOISE_SIGMA = 0.15  # логнормальный день-в-день джиттер вокруг тренда

# Календарь инцидентов строится на фиксированном сиде, НЕ зависящем от
# load_date, поэтому каждый отдельный batch-запуск (генератор вызывается
# по одному разу на день — см. backfill_history.sh) детерминированно
# восстанавливает ОДИН И ТОТ ЖЕ календарь и корректно видит многодневные
# инциденты без какого-либо состояния между запусками.
INCIDENT_SCHEDULE_SEED = 20260601
INCIDENT_HORIZON_DAYS = 730
INCIDENT_DAILY_PROBABILITY = 0.02
INCIDENT_DURATION_DAYS_RANGE = (2, 4)
INCIDENT_MULTIPLIER_RANGE = (3.0, 6.0)

# Мультипликативный день-в-день логнормальный шум для целевого объёма
# (--customers-count/--orders-count/--payments-count), включается вместе с
# --launch-date по тому же принципу, что и ERROR_RATE_NOISE_SIGMA: без
# него бэкафилл (backfill_history.sh) даёт идеально гладкую детерминированную
# кривую роста, что визуально не похоже на реальные данные.
VOLUME_NOISE_SIGMA = 0.15

# ---------------------------------------------------------------------
# Реалистичная динамика PAYMENT_SUCCESS_RATE (включается флагом
# --launch-date)
#
# Молодая платёжная интеграция стартует с более высокой долей отказов
# оплаты (карта/СБП ещё не обкатаны), которая сглаживается к базовому
# уровню по мере взросления, плюс день-в-день шум. Реализовано через
# failure_rate = 1 - PAYMENT_SUCCESS_RATE, чтобы переиспользовать
# _ramp_multiplier как есть (та же форма кривой, что и у error_rate,
# только со своими константами и без re-derive формулы). Инцидентный
# всплеск переиспользует уже существующий календарь сущности "payments" —
# сбой платёжной инфраструктуры правдоподобно бьёт и по DQ, и по
# конверсии оплаты одновременно. Без --launch-date ничего из этого не
# применяется, PAYMENT_SUCCESS_RATE остаётся ровно базовой константой.
# ---------------------------------------------------------------------

RAMP_PAYMENT_FAILURE_INITIAL_MULTIPLIER = 3.0  # во сколько раз выше доля отказов оплаты в день запуска
RAMP_PAYMENT_FAILURE_DECAY_DAYS = 14  # платёжный процессинг обкатывается быстрее, чем DQ-интеграции

PAYMENT_FAILURE_NOISE_SIGMA = 0.15  # логнормальный день-в-день джиттер вокруг тренда

# ---------------------------------------------------------------------
# Календарь дней особого объёма (включается флагом --launch-date)
#
# В отличие от календаря инцидентов (_build_incident_calendar) — это не
# случайные редкие сбои, а ДЕТЕРМИНИРОВАННОЕ правило по самой дате:
# праздник/зарплата/промо. daily_marketplace_pipeline (Airflow DAG)
# крутится ежедневно бессрочно, поэтому даты заданы как (месяц, день)
# правила, а не жёстко прибитый список конкретных годов — работают на
# любую будущую дату без сопровождения.
# ---------------------------------------------------------------------

# Гос. праздники РФ, официально нерабочие дни (месяц, день) — без учёта
# переносов между конкретными годами (см. _holiday_dates_for_year).
PUBLIC_HOLIDAYS_MD = [
    (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8),  # НГ + Рождество
    (2, 23),
    (3, 8),
    (5, 1),
    (5, 9),
    (6, 12),
    (11, 4),
]

HOLIDAY_VOLUME_MULTIPLIER = 0.75  # активность и логистика проседают в нерабочий день
PAYDAY_VOLUME_MULTIPLIER = 1.15   # 5-е/20-е число — зарплата/аванс, есть на что тратить
PROMO_VOLUME_MULTIPLIER = 2.5     # 11.11 и «чёрная пятница» — ради чего ждут этот день

# Множитель на failure_rate/долю отмен от перегрузки в дни, когда
# фактический объём заказов (после шума и календаря) выше базового —
# склад/платёжный шлюз не резиновые. Работает только "вверх"
# (load_ratio > 1): тихий день сам по себе не улучшает конверсию, только
# перегруженный ухудшает её.
LOAD_STRESS_SENSITIVITY = 0.4


def _holiday_dates_for_year(year: int) -> set[date]:
    """
    Набор нерабочих дат в данном году: все PUBLIC_HOLIDAYS_MD плюс
    перенос на ближайший будний день вперёд для каждого праздника,
    выпавшего на выходные — упрощённая имитация реального переноса
    выходных Правительством РФ (который не сводится к чистому правилу
    "следующий понедельник" и публикуется отдельным постановлением
    на каждый год — см. README, раздел «Известные ограничения»).
    """
    dates: set[date] = set()

    for month, day in PUBLIC_HOLIDAYS_MD:
        holiday = date(year, month, day)
        dates.add(holiday)

        if holiday.weekday() >= 5:  # суббота/воскресенье
            shifted = holiday + timedelta(days=1)
            while shifted.weekday() >= 5:
                shifted += timedelta(days=1)
            dates.add(shifted)

    return dates


def _is_public_holiday(d: date) -> bool:
    return d in _holiday_dates_for_year(d.year)


def _is_payday(d: date) -> bool:
    return d.day in (5, 20)


def _black_friday(year: int) -> date:
    """Последняя пятница ноября."""
    d = date(year, 11, 30)

    while d.weekday() != 4:  # пятница
        d -= timedelta(days=1)

    return d


def _is_promo_day(d: date) -> bool:
    return (d.month, d.day) == (11, 11) or d == _black_friday(d.year)


def resolve_calendar_day_type(load_date: date) -> str:
    """
    Тип дня по календарю: 'promo' > 'holiday' > 'payday' > 'normal' —
    приоритет, а не перемножение эффектов, чтобы при совпадении дат
    (например, зарплата пришлась на праздник) не накручивать множитель
    сверх правдоподобного.
    """
    if _is_promo_day(load_date):
        return "promo"
    if _is_public_holiday(load_date):
        return "holiday"
    if _is_payday(load_date):
        return "payday"
    return "normal"


def resolve_calendar_multiplier(load_date: date) -> float:
    """Множитель объёма по типу дня — см. resolve_calendar_day_type()."""
    return {
        "promo": PROMO_VOLUME_MULTIPLIER,
        "holiday": HOLIDAY_VOLUME_MULTIPLIER,
        "payday": PAYDAY_VOLUME_MULTIPLIER,
        "normal": 1.0,
    }[resolve_calendar_day_type(load_date)]


def resolve_load_stress_multiplier(load_ratio: float) -> float:
    """
    load_ratio — отношение фактического (после шума и календаря) объёма
    заказов к базовому за день. > 1 означает, что сегодня заказов больше,
    чем система в среднем рассчитана обрабатывать.
    """
    return 1.0 + LOAD_STRESS_SENSITIVITY * max(load_ratio - 1.0, 0.0)


SCHEMA_VERSION = "4.0.0"
GENERATOR_VERSION = "4.0.0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--load-date", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--customers-count", type=int, default=100)
    parser.add_argument("--orders-count", type=int, default=15_000)
    # Запас над --orders-count (по умолчанию 15000) подобран эмпирически:
    # ~1% оставляет реалистичную долю заказов (~1-2%), не оплаченных в
    # срок (status=cancelled, reason=not_paid_in_time) — без запаса
    # вообще этот тип отмены практически никогда бы не встречался в
    # данных, с большим запасом (5%+) он тоже почти обнуляется.
    parser.add_argument("--payments-count", type=int, default=15_150)
    parser.add_argument("--error-rate", type=float, default=0.01)
    # Опционально: включает реалистичную динамику error_rate (ramp-up
    # после запуска + шум + инциденты) вместо буквального --error-rate
    # каждый день — см. resolve_error_rate().
    parser.add_argument("--launch-date", default=None)

    parser.add_argument("--payment-deadline-hours", type=int, default=24)
    parser.add_argument("--shipping-deadline-hours", type=int, default=48)
    parser.add_argument("--delivery-deadline-days", type=int, default=7)
    parser.add_argument("--pickup-deadline-days", type=int, default=7)
    parser.add_argument("--return-window-days", type=int, default=14)
    parser.add_argument("--refund-processing-hours", type=int, default=72)
    parser.add_argument("--cancel-before-payment-rate", type=float, default=0.02)
    parser.add_argument("--cancel-after-payment-rate", type=float, default=0.01)
    parser.add_argument("--return-rate", type=float, default=0.05)
    # Доля зарегистрировавшихся, которые не сделают ни одного заказа.
    # 0.18 — консервативная оценка для маркетплейса с низким порогом
    # регистрации; при 0 конверсия «регистрация -> первый заказ» в
    # данных всегда 100%, и метрика перестаёт что-либо измерять.
    parser.add_argument("--never-order-rate", type=float, default=0.18)

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    for field in ["customers_count", "orders_count", "payments_count"]:
        value = getattr(args, field)
        if value < 0:
            raise ValueError(f"{field} must be >= 0")

    if not 0 <= args.error_rate <= 1:
        raise ValueError("--error-rate must be between 0 and 1")

    if args.launch_date is not None:
        try:
            date.fromisoformat(args.launch_date)
        except ValueError as exc:
            raise ValueError(
                "--launch-date must be an ISO date (YYYY-MM-DD)"
            ) from exc

    for field in [
        "payment_deadline_hours",
        "shipping_deadline_hours",
        "delivery_deadline_days",
        "pickup_deadline_days",
        "return_window_days",
        "refund_processing_hours",
    ]:
        value = getattr(args, field)
        if value <= 0:
            raise ValueError(f"--{field.replace('_', '-')} must be > 0")

    for field in [
        "cancel_before_payment_rate",
        "cancel_after_payment_rate",
        "return_rate",
        "never_order_rate",
    ]:
        value = getattr(args, field)
        if not 0 <= value <= 1:
            raise ValueError(f"--{field.replace('_', '-')} must be between 0 and 1")

    if args.orders_count > 0 and args.customers_count == 0:
        raise ValueError(
            "Cannot generate orders when customers-count=0. "
            "Generate customers first or set orders-count=0."
        )


def compute_lookback_days(args: argparse.Namespace) -> int:
    """
    Максимальное число дней, которое заказ может провести в открытом
    состоянии (new/paid/shipped/ready_for_pickup/delivered-в-окне-возврата/
    cancelled-или-returned-ожидающий-рефанда) — сумма всех окон подряд,
    худший случай. За этим горизонтом любой заказ уже гарантированно
    закрыт (delivered без возврата, либо refunded).
    """
    return (
        math.ceil(args.payment_deadline_hours / 24)
        + math.ceil(args.shipping_deadline_hours / 24)
        + args.delivery_deadline_days
        + args.pickup_deadline_days
        + args.return_window_days
        + math.ceil(args.refund_processing_hours / 24)
    )


def make_rng(load_date: date) -> np.random.Generator:
    return np.random.default_rng(int(load_date.strftime("%Y%m%d")))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def error_count(total_rows: int, error_rate: float, error_types: int) -> int:
    """
    Возвращает количество ошибок каждого типа.

    Ошибки распределяются по непересекающимся строкам, поэтому
    итоговый размер набора всегда остаётся равным requested count.
    """
    if total_rows == 0 or error_rate == 0:
        return 0

    requested = int(total_rows * error_rate)

    if requested == 0:
        requested = 1

    if total_rows < error_types:
        return 0

    per_type = requested // error_types

    if per_type == 0:
        per_type = 1

    return min(per_type, total_rows // error_types)


def _ramp_multiplier(
    day_index: int,
    initial_multiplier: float = RAMP_ERROR_INITIAL_MULTIPLIER,
    decay_days: int = RAMP_ERROR_DECAY_DAYS,
) -> float:
    """
    Множитель по дням с момента запуска площадки: максимум
    `initial_multiplier` в день запуска, экспоненциальный спад к 1.0
    (базовый уровень) с характерным временем `decay_days`. day_index <= 0
    (день запуска или раньше) даёт максимум как есть — без экспоненты,
    чтобы не улетать в бесконечность на отрицательных днях. Параметры по
    умолчанию — ramp error_rate; resolve_payment_success_rate()
    переиспользует эту же функцию со своими RAMP_PAYMENT_FAILURE_*.
    """
    if day_index <= 0:
        return initial_multiplier

    return 1.0 + (initial_multiplier - 1.0) * math.exp(-day_index / decay_days)


def _build_incident_calendar() -> dict[str, list[tuple[int, int, float]]]:
    """
    Строит календарь редких DQ-инцидентов по каждой сущности на
    INCIDENT_HORIZON_DAYS вперёд от launch_date: список
    (день_старта, длительность_дней, множитель_error_rate) на сущность.
    Окна внутри одной сущности не пересекаются.
    """
    rng = np.random.default_rng(INCIDENT_SCHEDULE_SEED)
    calendar: dict[str, list[tuple[int, int, float]]] = {}

    for entity in DQ_ENTITIES:
        windows: list[tuple[int, int, float]] = []
        day = 0

        while day < INCIDENT_HORIZON_DAYS:
            if rng.random() < INCIDENT_DAILY_PROBABILITY:
                duration = int(
                    rng.integers(
                        INCIDENT_DURATION_DAYS_RANGE[0],
                        INCIDENT_DURATION_DAYS_RANGE[1] + 1,
                    )
                )
                multiplier = float(rng.uniform(*INCIDENT_MULTIPLIER_RANGE))
                windows.append((day, duration, multiplier))
                day += duration
            else:
                day += 1

        calendar[entity] = windows

    return calendar


def _incident_multiplier(
    calendar: dict[str, list[tuple[int, int, float]]],
    entity: str,
    day_index: int,
) -> float:
    for start, duration, multiplier in calendar.get(entity, []):
        if start <= day_index < start + duration:
            return multiplier

    return 1.0


def resolve_error_rate(
    entity: str,
    load_date: date,
    base_rate: float,
    rng: np.random.Generator,
    launch_date: date | None,
    incident_calendar: dict[str, list[tuple[int, int, float]]] | None = None,
) -> float:
    """
    Эффективный error_rate на день для конкретной сущности: базовый
    уровень (`base_rate`), умноженный на ramp-затухание с момента запуска,
    день-в-день логнормальный шум и, если сегодня попадает в календарь
    инцидентов этой сущности, дополнительный множитель всплеска.

    Без launch_date возвращает base_rate как есть — весь реализм-слой
    выключен (обратная совместимость с прямыми вызовами generate_*).
    """
    if launch_date is None:
        return base_rate

    day_index = (load_date - launch_date).days

    ramp = _ramp_multiplier(day_index)
    noise = float(rng.lognormal(mean=0.0, sigma=ERROR_RATE_NOISE_SIGMA))
    incident = _incident_multiplier(incident_calendar or {}, entity, day_index)

    return min(base_rate * ramp * noise * incident, 1.0)


def resolve_payment_success_rate(
    load_date: date,
    rng: np.random.Generator,
    launch_date: date | None,
    incident_calendar: dict[str, list[tuple[int, int, float]]] | None = None,
    load_ratio: float = 1.0,
) -> float:
    """
    Эффективный PAYMENT_SUCCESS_RATE на день. Без launch_date возвращает
    базовую константу как есть.

    С launch_date считается через failure_rate = 1 - PAYMENT_SUCCESS_RATE,
    к которому применяется тот же ramp+noise+incident механизм, что и в
    resolve_error_rate (свои RAMP_PAYMENT_FAILURE_*/
    PAYMENT_FAILURE_NOISE_SIGMA константы, но переиспользованная запись
    "payments" из календаря инцидентов — один и тот же сбой платёжной
    инфраструктуры правдоподобно бьёт и по DQ, и по конверсии оплаты),
    плюс load_ratio (см. resolve_load_stress_multiplier) — перегрузка от
    аномально высокого объёма дня тоже просаживает конверсию оплаты,
    затем конвертируется обратно в success rate.
    """
    if launch_date is None:
        return PAYMENT_SUCCESS_RATE

    base_failure_rate = 1.0 - PAYMENT_SUCCESS_RATE
    day_index = (load_date - launch_date).days

    ramp = _ramp_multiplier(
        day_index,
        RAMP_PAYMENT_FAILURE_INITIAL_MULTIPLIER,
        RAMP_PAYMENT_FAILURE_DECAY_DAYS,
    )
    noise = float(rng.lognormal(mean=0.0, sigma=PAYMENT_FAILURE_NOISE_SIGMA))
    incident = _incident_multiplier(incident_calendar or {}, "payments", day_index)
    stress = resolve_load_stress_multiplier(load_ratio)

    failure_rate = min(base_failure_rate * ramp * noise * incident * stress, 1.0)
    return 1.0 - failure_rate


def resolve_count(
    base_count: int,
    rng: np.random.Generator,
    launch_date: date | None,
    load_date: date | None = None,
) -> int:
    """
    Применяет мультипликативный логнормальный шум и, если передан
    load_date, календарный множитель дня (см. resolve_calendar_multiplier)
    к целевому объёму (--customers-count/--orders-count) — сама кривая
    роста детерминированно считается в backfill_history.sh, здесь только
    день-в-день джиттер и тип дня поверх неё. Без launch_date возвращает
    base_count как есть (обратная совместимость с прямыми вызовами
    generate_data.py вне бэкафилла).
    """
    if launch_date is None or base_count == 0:
        return base_count

    noise = float(rng.lognormal(mean=0.0, sigma=VOLUME_NOISE_SIGMA))
    calendar_multiplier = (
        resolve_calendar_multiplier(load_date) if load_date is not None else 1.0
    )
    return max(1, round(base_count * noise * calendar_multiplier))


def ensure_batch_does_not_exist(source_root: Path, load_date: date) -> None:
    load_date_str = load_date.isoformat()

    paths = [
        source_root / "customers" / f"load_date={load_date_str}",
        source_root / "orders" / f"load_date={load_date_str}",
        source_root / "payments" / f"load_date={load_date_str}",
        source_root / "_commits" / f"load_date={load_date_str}",
    ]

    existing = [str(path) for path in paths if path.exists()]

    if existing:
        raise FileExistsError(
            "Load date already exists and cannot be overwritten:\n"
            + "\n".join(existing)
        )


def _concat_frames(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
    """
    Обычный `pd.concat` на пустых/all-NA фреймах эмитит FutureWarning в
    текущих версиях pandas (правила определения итогового dtype при
    конкатенации с пустыми фреймами скоро изменятся) — отфильтровываем
    пустые фреймы заранее, а не полагаемся на нынешнее (устаревающее)
    поведение. На первом батче (нет ни истории, ни текущих данных)
    пустые фреймы — обычное дело, не исключение.
    """
    non_empty = [df for df in frames if not df.empty]

    if not non_empty:
        return pd.DataFrame(columns=columns)

    return pd.concat(non_empty, ignore_index=True)


def _realistic_timestamps(days: list[date], rng: np.random.Generator) -> list[datetime]:
    """Реалистичное время суток для каждого дня в `days`, взвешено по
    WEEKDAY_HOUR_WEIGHTS/WEEKEND_HOUR_WEIGHTS в зависимости от дня недели."""
    n = len(days)

    if n == 0:
        return []

    weekdays = np.array([d.weekday() for d in days])
    hours = np.empty(n, dtype=np.int64)

    for is_weekend in (False, True):
        mask = (weekdays >= 5) == is_weekend
        group_size = int(mask.sum())

        if group_size == 0:
            continue

        raw_weights = WEEKEND_HOUR_WEIGHTS if is_weekend else WEEKDAY_HOUR_WEIGHTS
        weights = np.array(raw_weights, dtype=float)
        weights = weights / weights.sum()

        hours[mask] = rng.choice(24, size=group_size, p=weights)

    minutes = rng.integers(0, 60, size=n)
    seconds = rng.integers(0, 60, size=n)

    return [
        datetime.combine(d, time(hour=int(h), minute=int(m), second=int(s)))
        for d, h, m, s in zip(days, hours, minutes, seconds, strict=True)
    ]


def _uniform_between(
    earliest: datetime,
    latest: datetime,
    rng: np.random.Generator,
) -> datetime:
    span = (latest - earliest).total_seconds()

    if span <= 0:
        return earliest

    return earliest + timedelta(seconds=float(rng.uniform(0, span)))


def _resolve_forced_transition(
    base: datetime,
    min_gap: timedelta,
    window: timedelta,
    now: datetime,
    day_start: datetime,
    rng: np.random.Generator,
) -> datetime | None:
    """
    Разыгрывает, произойдёт ли гарантированный (не вероятностный) переход
    сегодня. Возвращает None, если ещё не сегодня. Переход всегда строго
    меньше `base + window` — форсируется на дедлайне, если ещё не
    произошёл раньше по DAILY_ADVANCE_PROBABILITY.
    """
    deadline = base + window

    if now >= deadline:
        return deadline - timedelta(seconds=1)

    if rng.random() < DAILY_ADVANCE_PROBABILITY:
        earliest = max(base + min_gap, day_start)
        latest = min(now, deadline)

        if earliest < latest:
            return _uniform_between(earliest, latest, rng)

    return None


def _last_milestone_at(row: dict[str, Any]) -> datetime:
    """Момент последней достигнутой вехи happy path — точка отсчёта
    для клиентской отмены."""
    for field in ("ready_for_pickup_at", "shipped_at", "paid_at"):
        value = row.get(field)

        if pd.notna(value):
            return value

    return row["created_at"]


def load_existing_customers(source_root: Path, load_date: date) -> pd.DataFrame:
    """
    Читает только клиентов с registration_date <= load_date.

    Это позволяет использовать ранее созданных клиентов при генерации
    новых заказов и не использовать клиентов из будущих загрузок.
    """
    customers_root = source_root / "customers"

    if not customers_root.exists():
        return pd.DataFrame(columns=CUSTOMER_COLUMNS)

    files = sorted(customers_root.glob("load_date=*/customers.csv"))

    if not files:
        return pd.DataFrame(columns=CUSTOMER_COLUMNS)

    frames = []

    for file in files:
        df = pd.read_csv(file, parse_dates=["registration_date"])
        frames.append(df)

    customers = _concat_frames(frames, CUSTOMER_COLUMNS)
    customers["registration_date"] = pd.to_datetime(customers["registration_date"])

    customers = customers[
        customers["registration_date"] <= pd.Timestamp(load_date) + pd.Timedelta(days=1)
    ].drop_duplicates("customer_id")

    return customers.reset_index(drop=True)


def load_existing_orders(
    source_root: Path,
    load_date: date,
    lookback_days: int,
) -> pd.DataFrame:
    """
    Читает историю orders.csv за последние `lookback_days` дней, берёт
    последнюю версию на order_id (upsert-модель — заказ переиздаётся
    строкой при каждом изменении статуса), возвращает только заказы,
    которые ещё не закрыты навсегда (не delivered-без-возврата, не
    refunded).

    Глубина скана намеренно ограничена: за пределами lookback_days любой
    заказ гарантированно уже закрыт (см. compute_lookback_days), поэтому
    нет смысла читать более старую историю.
    """
    orders_root = source_root / "orders"

    if not orders_root.exists():
        return pd.DataFrame(columns=ORDER_COLUMNS)

    earliest_relevant = load_date - timedelta(days=lookback_days)

    frames = []

    for file in sorted(orders_root.glob("load_date=*/orders.csv")):
        file_load_date = date.fromisoformat(file.parent.name.split("=", 1)[1])

        if not (earliest_relevant <= file_load_date <= load_date):
            continue

        df = pd.read_csv(file, parse_dates=ORDER_TIMESTAMP_COLUMNS)
        df["_source_load_date"] = file_load_date
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=ORDER_COLUMNS)

    combined = _concat_frames(frames, [*ORDER_COLUMNS, "_source_load_date"])
    combined = combined.sort_values("_source_load_date", kind="stable")
    combined = combined.drop_duplicates("order_id", keep="last")
    combined = combined.drop(columns=["_source_load_date"])

    # Заказ считается всё ещё "открытым", если он либо не в терминальном
    # статусе вообще (new/paid/shipped/ready_for_pickup), либо delivered
    # (может ещё вернуться в течение return_window_days — сам факт того,
    # что файл вообще попал в lookback-окно, уже гарантирует, что окно не
    # истекло), либо cancelled/returned с ожидающим рефандом.
    open_mask = combined["status"].isin(
        ["new", "paid", "shipped", "ready_for_pickup", "delivered"]
    ) | (
        combined["status"].isin(["cancelled", "returned"])
        & combined["paid_at"].notna()
    )

    return combined[open_mask].reset_index(drop=True)


def generate_customers(
    load_date: date,
    count: int,
    rng: np.random.Generator,
    error_rate: float = 0.0,
    existing_customer_ids: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if count == 0:
        return pd.DataFrame(columns=CUSTOMER_COLUMNS), {}

    # ru_RU: email строится из русских имён, транслитерированных в
    # латиницу (например Иван Петров -> ivan.petrov@mail.ru), а не из
    # американских имён — консистентно с городами ниже.
    fake = Faker("ru_RU")
    fake.seed_instance(int(load_date.strftime("%Y%m%d")))

    # registration_date = load_date этого батча: каждый батч моделирует
    # клиентов, зарегистрировавшихся именно сегодня — так же, как
    # ежедневный пайплайн видит только сегодняшние регистрации.
    registration_dates = _realistic_timestamps([load_date] * count, rng)
    city_weights = np.array(CITY_WEIGHTS, dtype=float)
    city_weights = city_weights / city_weights.sum()
    cities = rng.choice(CITIES, size=count, p=city_weights)
    channels = rng.choice(ACQUISITION_CHANNELS, size=count, p=ACQUISITION_CHANNEL_WEIGHTS)
    emails = [fake.unique.email() for _ in range(count)]

    customer_ids = [f"cus_{load_date:%Y%m%d}_{i + 1:08d}" for i in range(count)]

    df = pd.DataFrame(
        dict(
            zip(
                CUSTOMER_COLUMNS,
                [customer_ids, registration_dates, cities, channels, emails],
                strict=True,
            )
        )
    )

    per_type = error_count(total_rows=count, error_rate=error_rate, error_types=4)

    expected = {
        "customers.null_customer_id": per_type,
        "customers.duplicate_customer_id_rows": per_type,
        "customers.duplicate_customer_id_in_history": (
            per_type if existing_customer_ids else 0
        ),
        "customers.invalid_registration_date": per_type,
    }

    if per_type == 0:
        return df, expected

    indices = rng.permutation(df.index).tolist()
    cursor = 0

    null_id_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    dup_in_load_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    dup_in_history_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    invalid_reg_idx = indices[cursor:cursor + per_type]

    df.loc[null_id_idx, "customer_id"] = None

    # Пул-источник для дублирования исключает и dup_in_load_idx (сама
    # цель копирования), и null_id_idx — иначе источником дубля мог бы
    # случайно стать уже обнулённый customer_id, и одна строка попала бы
    # сразу в два разных счётчика DQ.
    source_ids = df.loc[
        df.index.difference(dup_in_load_idx).difference(null_id_idx),
        "customer_id",
    ].sample(
        n=per_type,
        replace=False,
        random_state=int(rng.integers(1, 1_000_000)),
    ).tolist()
    df.loc[dup_in_load_idx, "customer_id"] = source_ids

    if existing_customer_ids:
        reused = rng.choice(existing_customer_ids, size=per_type).tolist()
        df.loc[dup_in_history_idx, "customer_id"] = reused

    df.loc[invalid_reg_idx, "registration_date"] = pd.NaT

    return df, expected


def generate_order_amounts(
    count: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """
    Логнормальное распределение сумм заказов (большинство чеков небольшие,
    крупные — редкий хвост), клип в диапазон [15_000, 15_000_000] копеек
    (150–150 000 ₽). Калибровано под универсальный маркетплейс смешанных
    категорий (одежда/электроника/товары для дома, а-ля Ozon/Wildberries):
    mean=11.9, sigma=0.8 даёт медиану ~1475₽ и средний чек ~2000₽ —
    большинство заказов недорогие, но хвост уходит в крупную бытовую
    технику/электронику.
    """
    raw = rng.lognormal(mean=11.9, sigma=0.8, size=count)
    clipped = np.clip(raw, 15_000, 15_000_000)
    return clipped.astype(np.int64)


def _customer_never_orders(
    customers: pd.DataFrame,
    never_order_rate: float,
) -> np.ndarray:
    """
    Булев признак «клиент не сделает ни одного заказа» — детерминированный
    из customer_id, как и вес частоты заказов, чтобы решение не
    перевыбиралось на каждом батче: зарегистрировавшийся и ушедший клиент
    должен остаться ушедшим и завтра.

    Берётся другой срез md5, чем в _customer_order_weights: на одном и том
    же срезе «не купит никогда» и «покупает редко» оказались бы одним
    признаком, и неконвертящиеся были бы просто хвостом распределения
    частоты, а не отдельным поведением.

    Доля зависит от канала привлечения
    (ACQUISITION_CHANNEL_NON_CONVERSION_MULTIPLIER) и клипается сверху
    на 0.95 — полностью мёртвый канал не моделируем.
    """
    if never_order_rate <= 0 or customers.empty:
        return np.zeros(len(customers), dtype=bool)

    digests = customers["customer_id"].map(
        lambda cid: int(hashlib.md5(cid.encode()).hexdigest()[12:24], 16)
    )
    uniform = (digests % 10_000_000).to_numpy() / 10_000_000

    channel_multiplier = (
        customers["acquisition_channel"]
        .map(ACQUISITION_CHANNEL_NON_CONVERSION_MULTIPLIER)
        .fillna(1.0)
        .to_numpy()
    )
    threshold = np.clip(never_order_rate * channel_multiplier, 0.0, 0.95)

    return uniform < threshold


def _customer_order_weights(
    customers: pd.DataFrame,
    never_order_rate: float = 0.0,
) -> np.ndarray:
    """
    Вес «частоты заказов» по клиенту — детерминированный (из md5 хеша
    customer_id, а не из rng текущего батча), поэтому один и тот же
    клиент остаётся одинаково активным/пассивным изо дня в день, а не
    перевыбирается заново равновероятно на каждом батче. Экспоненциальный
    хвост весов даёт реалистичный перекос повторных покупок: небольшая
    доля клиентов формирует основную массу заказов, а не все клиенты
    покупают одинаково часто (как было при равновероятном выборе).

    Домножается на ACQUISITION_CHANNEL_LOYALTY_MULTIPLIER — канал
    привлечения клиента влияет на его склонность к повторным заказам
    (см. константу выше), а не остаётся статичной меткой без последствий.

    Клиенты, помеченные _customer_never_orders(), получают нулевой вес:
    они зарегистрировались и не купили ничего никогда.
    """
    digests = customers["customer_id"].map(
        lambda cid: int(hashlib.md5(cid.encode()).hexdigest()[:12], 16)
    )
    uniform = np.clip((digests % 10_000_000) / 10_000_000, 1e-9, 1 - 1e-9)
    base_weight = -np.log(1 - uniform.to_numpy())

    channel_multiplier = (
        customers["acquisition_channel"]
        .map(ACQUISITION_CHANNEL_LOYALTY_MULTIPLIER)
        .to_numpy()
    )

    weights = base_weight * channel_multiplier

    never_orders = _customer_never_orders(customers, never_order_rate)
    if never_orders.all():
        # Вырожденный случай: все известные клиенты — неконвертящиеся.
        # Заказы всё равно надо кому-то приписать, иначе батч упадёт на
        # делении на нулевую сумму весов. На реальных объёмах не
        # встречается (0.18^N), но маленькие тестовые батчи ловит.
        return weights

    weights[never_orders] = 0.0

    return weights


def generate_orders(
    load_date: date,
    count: int,
    customers_df: pd.DataFrame,
    rng: np.random.Generator,
    error_rate: float,
    never_order_rate: float = 0.0,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Создаёт `count` НОВЫХ заказов, все в статусе "new", created_at —
    сегодня с реалистичным временем суток. Дальнейшее продвижение по
    state machine — забота advance_open_orders(), не этой функции.

    `never_order_rate` — доля клиентов, которые не сделают ни одного
    заказа никогда (см. _customer_never_orders). При 0.0 заказ рано или
    поздно получает каждый клиент, и конверсия «регистрация → первый
    заказ» в данных равна 100%.
    """
    if count == 0:
        return pd.DataFrame(columns=ORDER_COLUMNS), {}

    if customers_df.empty:
        raise ValueError("Cannot generate orders without customers")

    # customers_df может содержать строки с customer_id=None, намеренно
    # внесённые generate_customers для DQ-метрики customers.null_customer_id
    # — такие клиенты не должны участвовать в выборе получателя заказа
    # (у заказов свой независимый orders.null_customer_id, см. ниже).
    customers = customers_df.dropna(subset=["customer_id"]).reset_index(drop=True)
    if customers.empty:
        raise ValueError("Cannot generate orders without customers")

    weights = _customer_order_weights(customers, never_order_rate)
    weights = weights / weights.sum()
    customer_idx = rng.choice(len(customers), size=count, p=weights)
    chosen_customers = customers.iloc[customer_idx].reset_index(drop=True)

    created_at = _realistic_timestamps([load_date] * count, rng)

    order_ids = [f"ord_{load_date:%Y%m%d}_{i + 1:08d}" for i in range(count)]

    df = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": chosen_customers["customer_id"].tolist(),
            "created_at": created_at,
            "amount_kopecks": generate_order_amounts(count, rng),
            "status": "new",
            "cancellation_reason": None,
            "payment_method": None,
            "paid_at": pd.NaT,
            "shipped_at": pd.NaT,
            "ready_for_pickup_at": pd.NaT,
            "delivered_at": pd.NaT,
            "cancelled_at": pd.NaT,
            "returned_at": pd.NaT,
            "refunded_at": pd.NaT,
        }
    )

    per_type = error_count(total_rows=count, error_rate=error_rate, error_types=4)

    expected = {
        "orders.null_customer_id": per_type,
        "orders.negative_amount": per_type,
        "orders.invalid_status": per_type,
        "orders.duplicate_order_id_rows": per_type,
    }

    if per_type == 0:
        return df, expected

    indices = rng.permutation(df.index).tolist()
    cursor = 0

    null_customer_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    negative_amount_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    invalid_status_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    duplicate_idx = indices[cursor:cursor + per_type]

    df.loc[null_customer_idx, "customer_id"] = None
    df.loc[negative_amount_idx, "amount_kopecks"] *= -1
    df.loc[invalid_status_idx, "status"] = "unknown_status"

    source_ids = df.loc[
        df.index.difference(duplicate_idx),
        "order_id",
    ].sample(
        n=per_type,
        replace=False,
        random_state=int(rng.integers(1, 1_000_000)),
    ).tolist()

    df.loc[duplicate_idx, "order_id"] = source_ids

    return df, expected


def advance_open_orders(
    load_date: date,
    candidate_orders_df: pd.DataFrame,
    rng: np.random.Generator,
    payment_deadline_hours: int,
    shipping_deadline_hours: int,
    delivery_deadline_days: int,
    pickup_deadline_days: int,
    return_window_days: int,
    refund_processing_hours: int,
    cancel_before_payment_rate: float,
    cancel_after_payment_rate: float,
    return_rate: float,
    error_rate: float,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """
    Продвигает пул заказов (сегодняшних новых + исторических открытых) на
    один день. Возвращает (updated_orders_df, refund_payments_df,
    dq_counts) — updated_orders_df содержит ТОЛЬКО строки, чьё состояние
    реально изменилось сегодня.
    """
    if candidate_orders_df.empty:
        return (
            pd.DataFrame(columns=ORDER_COLUMNS),
            pd.DataFrame(columns=PAYMENT_COLUMNS),
            {
                "orders.cancellation_reason_without_cancelled_status": 0,
                "orders.refund_before_cancellation": 0,
            },
        )

    now = datetime.combine(load_date, time(23, 59, 59))
    day_start = datetime.combine(load_date, time.min)

    payment_deadline = timedelta(hours=payment_deadline_hours)
    shipping_window = timedelta(hours=shipping_deadline_hours)
    delivery_window = timedelta(days=delivery_deadline_days)
    pickup_window = timedelta(days=pickup_deadline_days)
    return_window = timedelta(days=return_window_days)
    refund_window = timedelta(hours=refund_processing_hours)

    daily_return_hazard = (
        1 - (1 - return_rate) ** (1 / return_window_days)
        if return_window_days > 0
        else return_rate
    )

    updated_rows: list[dict[str, Any]] = []
    refund_rows: list[dict[str, Any]] = []

    for row in candidate_orders_df.to_dict("records"):
        status = row["status"]
        changed = False

        if status in ("paid", "shipped", "ready_for_pickup") and (
            rng.random() < cancel_after_payment_rate
        ):
            row["status"] = "cancelled"
            row["cancellation_reason"] = "cancelled_by_customer_after_payment"
            row["cancelled_at"] = _uniform_between(
                max(_last_milestone_at(row), day_start), now, rng
            )
            changed = True

        elif status == "new":
            created_at = row["created_at"]
            deadline_at = created_at + payment_deadline

            if now >= deadline_at:
                row["status"] = "cancelled"
                row["cancellation_reason"] = "not_paid_in_time"
                row["cancelled_at"] = deadline_at
                changed = True
            elif rng.random() < cancel_before_payment_rate:
                latest = min(now, deadline_at)
                earliest = max(created_at + PAYMENT_MIN_GAP, day_start)

                if earliest < latest:
                    row["status"] = "cancelled"
                    row["cancellation_reason"] = "cancelled_by_customer_before_payment"
                    row["cancelled_at"] = _uniform_between(earliest, latest, rng)
                    changed = True

        elif status == "paid":
            transition_at = _resolve_forced_transition(
                row["paid_at"], SHIPPING_MIN_GAP, shipping_window, now, day_start, rng
            )
            if transition_at is not None:
                row["shipped_at"] = transition_at
                row["status"] = "shipped"
                changed = True

        elif status == "shipped":
            transition_at = _resolve_forced_transition(
                row["shipped_at"],
                DELIVERY_MIN_GAP,
                delivery_window,
                now,
                day_start,
                rng,
            )
            if transition_at is not None:
                row["ready_for_pickup_at"] = transition_at
                row["status"] = "ready_for_pickup"
                changed = True

        elif status == "ready_for_pickup":
            deadline_at = row["ready_for_pickup_at"] + pickup_window

            if now >= deadline_at:
                row["status"] = "cancelled"
                row["cancellation_reason"] = "not_picked_up_in_time"
                row["cancelled_at"] = deadline_at
                changed = True
            elif rng.random() < DAILY_ADVANCE_PROBABILITY:
                earliest = max(row["ready_for_pickup_at"] + PICKUP_MIN_GAP, day_start)
                latest = min(now, deadline_at)

                if earliest < latest:
                    row["delivered_at"] = _uniform_between(earliest, latest, rng)
                    row["status"] = "delivered"
                    changed = True

        elif status == "delivered":
            window_end = row["delivered_at"] + return_window

            if now < window_end and rng.random() < daily_return_hazard:
                earliest = max(row["delivered_at"] + RETURN_MIN_GAP, day_start)
                latest = min(now, window_end)

                if earliest < latest:
                    row["returned_at"] = _uniform_between(earliest, latest, rng)
                    row["status"] = "returned"
                    changed = True

        elif status == "cancelled" and pd.notna(row.get("paid_at")):
            transition_at = _resolve_forced_transition(
                row["cancelled_at"], REFUND_MIN_GAP, refund_window, now, day_start, rng
            )
            if transition_at is not None:
                row["refunded_at"] = transition_at
                row["status"] = "refunded"
                changed = True
                refund_rows.append(_build_refund_payment_row(row))

        elif status == "returned":
            transition_at = _resolve_forced_transition(
                row["returned_at"], REFUND_MIN_GAP, refund_window, now, day_start, rng
            )
            if transition_at is not None:
                row["refunded_at"] = transition_at
                row["status"] = "refunded"
                changed = True
                refund_rows.append(_build_refund_payment_row(row))

        if changed:
            updated_rows.append(row)

    dq_counts = _inject_state_machine_dq(updated_rows, error_rate, rng)

    updated_df = (
        pd.DataFrame(updated_rows, columns=ORDER_COLUMNS)
        if updated_rows
        else pd.DataFrame(columns=ORDER_COLUMNS)
    )
    refund_df = (
        pd.DataFrame(refund_rows, columns=PAYMENT_COLUMNS)
        if refund_rows
        else pd.DataFrame(columns=PAYMENT_COLUMNS)
    )

    return updated_df, refund_df, dq_counts


def _build_refund_payment_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "payment_id": f"pay_{row['order_id']}",
        "order_id": row["order_id"],
        "payment_date": row["paid_at"],
        "amount_kopecks": int(row["amount_kopecks"]),
        "payment_method": row.get("payment_method"),
        "status": "refunded",
    }


def _inject_state_machine_dq(
    updated_rows: list[dict[str, Any]],
    error_rate: float,
    rng: np.random.Generator,
) -> dict[str, int]:
    """
    DQ-инжекция для полей state machine — портит несколько строк из уже
    продвинутых сегодня заказов, чтобы transform.py было на чём проверять
    новые консистентность-проверки.
    """
    per_type = error_count(
        total_rows=len(updated_rows),
        error_rate=error_rate,
        error_types=2,
    )

    dq_counts = {
        "orders.cancellation_reason_without_cancelled_status": 0,
        "orders.refund_before_cancellation": 0,
    }

    if per_type == 0 or not updated_rows:
        return dq_counts

    cancelled_candidates = [
        i for i, r in enumerate(updated_rows) if r["status"] == "cancelled"
    ]
    refunded_candidates = [
        i for i, r in enumerate(updated_rows) if r["status"] == "refunded"
    ]

    n1 = min(per_type, len(cancelled_candidates))
    if n1:
        for idx in rng.choice(cancelled_candidates, size=n1, replace=False):
            # status меняем на заведомо несовместимый с уже проставленной
            # cancellation_reason, саму причину не трогаем.
            updated_rows[int(idx)]["status"] = "paid"
        dq_counts["orders.cancellation_reason_without_cancelled_status"] = n1

    n2 = min(per_type, len(refunded_candidates))
    if n2:
        for idx in rng.choice(refunded_candidates, size=n2, replace=False):
            row = updated_rows[int(idx)]
            base = (
                row["cancelled_at"]
                if pd.notna(row.get("cancelled_at"))
                else row["returned_at"]
            )
            row["refunded_at"] = base - timedelta(hours=1)
        dq_counts["orders.refund_before_cancellation"] = n2

    return dq_counts


def generate_payments(
    load_date: date,
    new_status_orders_df: pd.DataFrame,
    count: int,
    rng: np.random.Generator,
    error_rate: float,
    now: datetime,
    success_rate: float = PAYMENT_SUCCESS_RATE,
) -> tuple[pd.DataFrame, dict[str, int], pd.DataFrame]:
    """
    Сэмплирует `count` попыток оплаты с возвращением из пула заказов в
    статусе "new" (сегодняшних и старых). Возвращает (payments_df,
    dq_counts, paid_orders_df) — paid_orders_df содержит только те
    заказы из пула, которые перешли в status=paid по результату успешной
    попытки.
    """
    empty_payments = pd.DataFrame(columns=PAYMENT_COLUMNS)
    empty_orders = pd.DataFrame(columns=ORDER_COLUMNS)

    if count == 0 or new_status_orders_df.empty:
        return empty_payments, {}, empty_orders

    orders = new_status_orders_df.drop_duplicates("order_id").set_index(
        "order_id", drop=False
    )

    day_start = datetime.combine(load_date, time.min)

    payment_methods = rng.choice(PAYMENT_METHODS, size=count)
    success_rolls = rng.random(count)

    rows: list[dict[str, Any]] = []
    paid_orders: dict[str, dict[str, Any]] = {}

    # Пул заказов, ещё не оплаченных в ЭТОМ батче — сжимается по мере
    # успешных оплат (swap-to-end + pop, O(1) на удаление), а не просто
    # фильтруется постфактум: сэмплирование с возвращением из фиксированного
    # списка при скромном запасе --payments-count над --orders-count по
    # чистой комбинаторике ("coupon collector") оставляло бы значительную
    # долю заказов вообще без единой попытки оплаты. Удаление из пула
    # гарантирует, что каждая попытка либо решает ещё не оплаченный заказ,
    # либо тратится на легитимный повторный ретрай после отказа.
    pool = list(orders.index)
    pool_position = {order_id: i for i, order_id in enumerate(pool)}

    for i in range(count):
        if not pool:
            break  # все заказы в пуле уже оплачены — обслуживать больше некого

        idx = int(rng.integers(0, len(pool)))
        order_id = pool[idx]

        order = orders.loc[order_id]
        created_at = order["created_at"]

        earliest = max(created_at + PAYMENT_MIN_GAP, day_start)
        latest = now

        if earliest >= latest:
            latest = earliest + timedelta(seconds=1)

        attempt_at = _uniform_between(earliest, latest, rng)
        payment_method = str(payment_methods[i])

        if success_rolls[i] < success_rate:
            last = pool.pop()
            if idx < len(pool):
                pool[idx] = last
                pool_position[last] = idx
            del pool_position[order_id]

            updated = dict(order)
            updated["status"] = "paid"
            updated["paid_at"] = attempt_at
            updated["payment_method"] = payment_method
            paid_orders[order_id] = updated

            payment_status = "success"
            payment_id = f"pay_{order_id}"
        else:
            payment_status = "failed"
            payment_id = f"pay_{load_date:%Y%m%d}_{i + 1:08d}"

        rows.append(
            {
                "payment_id": payment_id,
                "order_id": order_id,
                "payment_date": attempt_at,
                "amount_kopecks": int(order["amount_kopecks"]),
                "payment_method": payment_method,
                "status": payment_status,
            }
        )

    df = pd.DataFrame(rows, columns=PAYMENT_COLUMNS) if rows else empty_payments
    paid_orders_df = (
        pd.DataFrame(list(paid_orders.values()), columns=ORDER_COLUMNS)
        if paid_orders
        else empty_orders
    )

    per_type = error_count(total_rows=len(df), error_rate=error_rate, error_types=5)

    expected = {
        "payments.missing_order_id": per_type,
        "payments.negative_amount": per_type,
        "payments.invalid_status": per_type,
        "payments.payment_before_order": per_type,
        "payments.duplicate_payment_id_rows": per_type,
    }

    if per_type == 0 or df.empty:
        return df, expected, paid_orders_df

    indices = rng.permutation(df.index).tolist()
    cursor = 0

    missing_order_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    negative_amount_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    invalid_status_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    date_error_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    duplicate_idx = indices[cursor:cursor + per_type]

    df.loc[missing_order_idx, "order_id"] = [
        f"ord_missing_{load_date:%Y%m%d}_{i:08d}" for i in range(per_type)
    ]

    df.loc[negative_amount_idx, "amount_kopecks"] *= -1
    df.loc[invalid_status_idx, "status"] = "invalid_payment_status"

    for idx in date_error_idx:
        order_id = df.loc[idx, "order_id"]

        if order_id in orders.index:
            order_created_at = orders.loc[order_id, "created_at"]
            df.loc[idx, "payment_date"] = order_created_at - timedelta(hours=1)

    source_ids = df.loc[
        df.index.difference(duplicate_idx),
        "payment_id",
    ].sample(
        n=per_type,
        replace=False,
        random_state=int(rng.integers(1, 1_000_000)),
    ).tolist()

    df.loc[duplicate_idx, "payment_id"] = source_ids

    return df, expected, paid_orders_df


def write_staged_csv(
    df: pd.DataFrame,
    staging_root: Path,
    entity: str,
) -> Path:
    entity_dir = staging_root / entity
    entity_dir.mkdir(parents=True, exist_ok=False)

    target = entity_dir / f"{entity}.csv"

    df.to_csv(
        target,
        index=False,
        encoding="utf-8",
    )

    return target


def publish_batch(
    source_root: Path,
    staging_root: Path,
    load_date: date,
    manifest: dict[str, Any],
) -> None:
    """
    Полной POSIX-атомарности между несколькими директориями нет.

    Каждый успешный replace() запоминается, и если что-то падает до
    создания commit-маркера, уже перемещённые директории откатываются
    обратно в staging (а не остаются в source_root), после чего
    исключение re-raise'ится. finally в main() затем удаляет staging_root
    целиком. Итоговый инвариант: либо все три сущности + commit-маркер
    оказываются в source_root, либо в source_root не остаётся никаких
    следов данного load_date.
    """
    load_date_str = load_date.isoformat()
    moved: list[tuple[Path, Path]] = []  # (target, staging_original)

    try:
        for entity in ["customers", "orders", "payments"]:
            source_entity_dir = staging_root / entity
            target_entity_dir = (
                source_root / entity / f"load_date={load_date_str}"
            )

            target_entity_dir.parent.mkdir(parents=True, exist_ok=True)
            source_entity_dir.replace(target_entity_dir)
            moved.append((target_entity_dir, source_entity_dir))

        commit_dir = source_root / "_commits" / f"load_date={load_date_str}"
        commit_dir.mkdir(parents=True, exist_ok=False)

        with (commit_dir / "commit.json").open("w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

    except Exception:
        for target_entity_dir, original_staging_dir in reversed(moved):
            if target_entity_dir.exists():
                target_entity_dir.replace(original_staging_dir)
        raise


def main() -> None:
    args = parse_args()
    validate_args(args)

    load_date = date.fromisoformat(args.load_date)
    data_dir = Path(args.data_dir)
    source_root = data_dir / "source"

    ensure_batch_does_not_exist(source_root, load_date)

    rng = make_rng(load_date)
    now = datetime.combine(load_date, time(23, 59, 59))

    launch_date = (
        date.fromisoformat(args.launch_date) if args.launch_date else None
    )
    incident_calendar = _build_incident_calendar() if launch_date else {}

    effective_error_rates = {
        entity: resolve_error_rate(
            entity=entity,
            load_date=load_date,
            base_rate=args.error_rate,
            rng=rng,
            launch_date=launch_date,
            incident_calendar=incident_calendar,
        )
        for entity in DQ_ENTITIES
    }

    effective_customers_count = resolve_count(
        args.customers_count, rng, launch_date, load_date
    )
    effective_orders_count = resolve_count(
        args.orders_count, rng, launch_date, load_date
    )
    effective_payments_count = resolve_count(
        args.payments_count, rng, launch_date, load_date
    )

    # Насколько сегодняшний фактический объём заказов (после шума и
    # календаря) выше базового — сигнал "нагрузки" для просадки конверсии
    # оплаты и учащения отмен ниже. Без launch_date effective==base всегда,
    # так что load_ratio==1.0 и ничего не меняется (обратная совместимость).
    load_ratio = (
        effective_orders_count / args.orders_count if args.orders_count > 0 else 1.0
    )
    load_stress = resolve_load_stress_multiplier(load_ratio)

    effective_payment_success_rate = resolve_payment_success_rate(
        load_date=load_date,
        rng=rng,
        launch_date=launch_date,
        incident_calendar=incident_calendar,
        load_ratio=load_ratio,
    )
    effective_cancel_before_payment_rate = min(
        args.cancel_before_payment_rate * load_stress, 1.0
    )
    effective_cancel_after_payment_rate = min(
        args.cancel_after_payment_rate * load_stress, 1.0
    )

    existing_customers_df = load_existing_customers(source_root, load_date)

    new_customers_df, customers_dq = generate_customers(
        load_date=load_date,
        count=effective_customers_count,
        rng=rng,
        error_rate=effective_error_rates["customers"],
        existing_customer_ids=existing_customers_df["customer_id"].dropna().tolist()
        if not existing_customers_df.empty
        else None,
    )

    all_customers_df = _concat_frames(
        [existing_customers_df, new_customers_df],
        CUSTOMER_COLUMNS,
    ).drop_duplicates("customer_id")

    # Только для лога: сколько накопленных клиентов не купят ничего
    # никогда. Считается по тому же признаку, что и веса заказов, так
    # что цифра сходится с тем, что увидит дашборд.
    never_ordering_customers = _customer_never_orders(
        all_customers_df.dropna(subset=["customer_id"]),
        args.never_order_rate,
    ).sum()

    new_orders_df, orders_dq = generate_orders(
        load_date=load_date,
        count=effective_orders_count,
        customers_df=all_customers_df,
        rng=rng,
        error_rate=effective_error_rates["orders"],
        never_order_rate=args.never_order_rate,
    )

    lookback_days = compute_lookback_days(args)
    historical_open_df = load_existing_orders(source_root, load_date, lookback_days)

    candidate_pool_df = _concat_frames(
        [new_orders_df, historical_open_df],
        ORDER_COLUMNS,
    )

    advanced_df, refund_payments_df, state_machine_dq = advance_open_orders(
        load_date=load_date,
        candidate_orders_df=candidate_pool_df,
        rng=rng,
        payment_deadline_hours=args.payment_deadline_hours,
        shipping_deadline_hours=args.shipping_deadline_hours,
        delivery_deadline_days=args.delivery_deadline_days,
        pickup_deadline_days=args.pickup_deadline_days,
        return_window_days=args.return_window_days,
        refund_processing_hours=args.refund_processing_hours,
        cancel_before_payment_rate=effective_cancel_before_payment_rate,
        cancel_after_payment_rate=effective_cancel_after_payment_rate,
        return_rate=args.return_rate,
        error_rate=effective_error_rates["orders"],
    )

    advanced_ids = set(advanced_df["order_id"]) if not advanced_df.empty else set()

    still_new_pool_df = candidate_pool_df[
        (candidate_pool_df["status"] == "new")
        & (~candidate_pool_df["order_id"].isin(advanced_ids))
    ]

    payments_df, payments_dq, paid_orders_df = generate_payments(
        load_date=load_date,
        new_status_orders_df=still_new_pool_df,
        count=effective_payments_count,
        rng=rng,
        error_rate=effective_error_rates["payments"],
        now=now,
        success_rate=effective_payment_success_rate,
    )

    paid_ids = set(paid_orders_df["order_id"]) if not paid_orders_df.empty else set()
    touched_ids = advanced_ids | paid_ids

    untouched_new_orders_df = new_orders_df[
        ~new_orders_df["order_id"].isin(touched_ids)
    ]

    orders_to_write_df = _concat_frames(
        [untouched_new_orders_df, advanced_df, paid_orders_df],
        ORDER_COLUMNS,
    )
    payments_to_write_df = _concat_frames(
        [payments_df, refund_payments_df],
        PAYMENT_COLUMNS,
    )

    all_dq = {**customers_dq, **orders_dq, **state_machine_dq, **payments_dq}
    skipped_dq_types = [name for name, cnt in all_dq.items() if cnt == 0]

    if args.error_rate > 0 and skipped_dq_types:
        logger.warning(
            "error-rate > 0, but batch is too small to inject "
            "the following DQ issue types: %s",
            ", ".join(skipped_dq_types),
        )

    batch_id = str(uuid.uuid4())
    staging_root = source_root / "_staging" / batch_id

    try:
        customers_file = write_staged_csv(new_customers_df, staging_root, "customers")
        orders_file = write_staged_csv(orders_to_write_df, staging_root, "orders")
        payments_file = write_staged_csv(
            payments_to_write_df, staging_root, "payments"
        )

        manifest = {
            "batch_id": batch_id,
            "load_date": load_date.isoformat(),
            "generator_version": GENERATOR_VERSION,
            "schema_version": SCHEMA_VERSION,
            "seed": int(load_date.strftime("%Y%m%d")),
            "configured_error_rate": args.error_rate,
            "launch_date": launch_date.isoformat() if launch_date else None,
            "effective_error_rate": effective_error_rates,
            "configured_customers_count": args.customers_count,
            "effective_customers_count": effective_customers_count,
            "configured_orders_count": args.orders_count,
            "effective_orders_count": effective_orders_count,
            "configured_payments_count": args.payments_count,
            "effective_payments_count": effective_payments_count,
            "configured_payment_success_rate": PAYMENT_SUCCESS_RATE,
            "effective_payment_success_rate": effective_payment_success_rate,
            "calendar_day_type": resolve_calendar_day_type(load_date)
            if launch_date
            else None,
            "load_ratio": load_ratio,
            "configured_cancel_before_payment_rate": args.cancel_before_payment_rate,
            "effective_cancel_before_payment_rate": (
                effective_cancel_before_payment_rate
            ),
            "configured_cancel_after_payment_rate": args.cancel_after_payment_rate,
            "effective_cancel_after_payment_rate": effective_cancel_after_payment_rate,
            "entities": {
                "customers": {
                    "rows": len(new_customers_df),
                    "file": "customers.csv",
                    "sha256": sha256_file(customers_file),
                },
                "orders": {
                    "rows": len(orders_to_write_df),
                    "file": "orders.csv",
                    "sha256": sha256_file(orders_file),
                },
                "payments": {
                    "rows": len(payments_to_write_df),
                    "file": "payments.csv",
                    "sha256": sha256_file(payments_file),
                },
            },
            "expected_quality_issues": all_dq,
            "skipped_quality_issue_types": skipped_dq_types,
            "consumer_contract": (
                "Read only load_date partitions with a matching "
                "_commits/load_date=YYYY-MM-DD/commit.json marker."
            ),
        }

        publish_batch(
            source_root=source_root,
            staging_root=staging_root,
            load_date=load_date,
            manifest=manifest,
        )

    finally:
        if staging_root.exists():
            shutil.rmtree(staging_root, ignore_errors=True)

    logger.info("Generation completed successfully.")
    logger.info("Load date: %s", load_date)
    logger.info("Customers:        %s", f"{len(new_customers_df):,}")
    logger.info(
        "  never order:    %s of %s known",
        f"{int(never_ordering_customers):,}",
        f"{len(all_customers_df):,}",
    )
    logger.info("Orders written:   %s", f"{len(orders_to_write_df):,}")
    logger.info("  brand new:      %s", f"{len(new_orders_df):,}")
    logger.info("  advanced:       %s", f"{len(advanced_df):,}")
    logger.info("  paid today:     %s", f"{len(paid_orders_df):,}")
    logger.info("Payments written: %s", f"{len(payments_to_write_df):,}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
