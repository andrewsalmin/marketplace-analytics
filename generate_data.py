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

# pending/chargeback убраны: ни одна ветка кода их не производит — были
# мёртвыми значениями, оставшимися от старой (не событийной) модели.
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
]  # города-миллионники России

ACQUISITION_CHANNELS = [
    "organic",
    "yandex_direct",
    "vk_ads",
    "telegram_ads",
    "social",
    "referral",
    "email",
]  # google_ads не используется — недоступен в РФ с 2022

PAYMENT_METHODS = ["card", "sbp", "mir_pay", "cash"]
# apple_pay/google_pay не используются — NFC-платежи недоступны в РФ
# с 2022, mir_pay — реальная российская NFC-альтернатива

CLIENT_COLUMNS = [
    "client_id",
    "registration_date",
    "city",
    "acquisition_channel",
    "email",
]

ORDER_COLUMNS = [
    "order_id",
    "client_id",
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
# _realistic_timestamps().
HOUR_WEIGHTS = [
    0.008, 0.008, 0.008, 0.008, 0.008, 0.008,  # 00-05: ночь
    0.017, 0.025, 0.033, 0.042,                # 06-09: утро, рост
    0.050, 0.050, 0.058, 0.058,                # 10-13: день
    0.050, 0.050, 0.058, 0.067,                # 14-17: день
    0.075, 0.083, 0.083, 0.075,                # 18-21: вечерний пик
    0.050, 0.025,                              # 22-23: спад
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

SCHEMA_VERSION = "4.0.0"
GENERATOR_VERSION = "4.0.0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--load-date", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--clients-count", type=int, default=100)
    parser.add_argument("--orders-count", type=int, default=15_000)
    parser.add_argument("--payments-count", type=int, default=16_000)
    parser.add_argument("--error-rate", type=float, default=0.01)

    parser.add_argument("--payment-deadline-hours", type=int, default=24)
    parser.add_argument("--shipping-deadline-hours", type=int, default=48)
    parser.add_argument("--delivery-deadline-days", type=int, default=7)
    parser.add_argument("--pickup-deadline-days", type=int, default=7)
    parser.add_argument("--return-window-days", type=int, default=14)
    parser.add_argument("--refund-processing-hours", type=int, default=72)
    parser.add_argument("--cancel-before-payment-rate", type=float, default=0.02)
    parser.add_argument("--cancel-after-payment-rate", type=float, default=0.01)
    parser.add_argument("--return-rate", type=float, default=0.05)

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    for field in ["clients_count", "orders_count", "payments_count"]:
        value = getattr(args, field)
        if value < 0:
            raise ValueError(f"{field} must be >= 0")

    if not 0 <= args.error_rate <= 1:
        raise ValueError("--error-rate must be between 0 and 1")

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
    ]:
        value = getattr(args, field)
        if not 0 <= value <= 1:
            raise ValueError(f"--{field.replace('_', '-')} must be between 0 and 1")

    if args.orders_count > 0 and args.clients_count == 0:
        raise ValueError(
            "Cannot generate orders when clients-count=0. "
            "Generate clients first or set orders-count=0."
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


def ensure_batch_does_not_exist(source_root: Path, load_date: date) -> None:
    load_date_str = load_date.isoformat()

    paths = [
        source_root / "clients" / f"load_date={load_date_str}",
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


def _realistic_timestamps(days: list[date], rng: np.random.Generator) -> list[datetime]:
    """Реалистичное время суток (взвешено по HOUR_WEIGHTS) для каждого дня в `days`."""
    n = len(days)

    if n == 0:
        return []

    weights = np.array(HOUR_WEIGHTS, dtype=float)
    weights = weights / weights.sum()

    hours = rng.choice(24, size=n, p=weights)
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


def load_existing_clients(source_root: Path, load_date: date) -> pd.DataFrame:
    """
    Читает только клиентов с registration_date <= load_date.

    Это позволяет использовать ранее созданных клиентов при генерации
    новых заказов и не использовать клиентов из будущих загрузок.
    """
    clients_root = source_root / "clients"

    if not clients_root.exists():
        return pd.DataFrame(columns=CLIENT_COLUMNS)

    files = sorted(clients_root.glob("load_date=*/clients.csv"))

    if not files:
        return pd.DataFrame(columns=CLIENT_COLUMNS)

    frames = []

    for file in files:
        df = pd.read_csv(file, parse_dates=["registration_date"])
        frames.append(df)

    clients = pd.concat(frames, ignore_index=True)
    clients["registration_date"] = pd.to_datetime(clients["registration_date"])

    clients = clients[
        clients["registration_date"] <= pd.Timestamp(load_date) + pd.Timedelta(days=1)
    ].drop_duplicates("client_id")

    return clients.reset_index(drop=True)


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

    combined = pd.concat(frames, ignore_index=True)
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


def generate_clients(
    load_date: date,
    count: int,
    rng: np.random.Generator,
    error_rate: float = 0.0,
    existing_client_ids: list[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if count == 0:
        return pd.DataFrame(columns=CLIENT_COLUMNS), {}

    # ru_RU: email строится из русских имён, транслитерированных в
    # латиницу (например Иван Петров -> ivan.petrov@mail.ru), а не из
    # американских имён — консистентно с городами ниже.
    fake = Faker("ru_RU")
    fake.seed_instance(int(load_date.strftime("%Y%m%d")))

    offsets = rng.integers(0, 365, size=count)
    registration_days = [load_date - timedelta(days=int(d)) for d in offsets]
    registration_dates = _realistic_timestamps(registration_days, rng)
    cities = rng.choice(CITIES, size=count)
    channels = rng.choice(ACQUISITION_CHANNELS, size=count)
    emails = [fake.unique.email() for _ in range(count)]

    client_ids = [f"cl_{load_date:%Y%m%d}_{i + 1:08d}" for i in range(count)]

    df = pd.DataFrame(
        dict(
            zip(
                CLIENT_COLUMNS,
                [client_ids, registration_dates, cities, channels, emails],
                strict=True,
            )
        )
    )

    per_type = error_count(total_rows=count, error_rate=error_rate, error_types=4)

    expected = {
        "clients.null_client_id": per_type,
        "clients.duplicate_client_id_rows": per_type,
        "clients.duplicate_client_id_in_history": (
            per_type if existing_client_ids else 0
        ),
        "clients.invalid_registration_date": per_type,
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

    df.loc[null_id_idx, "client_id"] = None

    # Пул-источник для дублирования исключает и dup_in_load_idx (сама
    # цель копирования), и null_id_idx — иначе источником дубля мог бы
    # случайно стать уже обнулённый client_id, и одна строка попала бы
    # сразу в два разных счётчика DQ.
    source_ids = df.loc[
        df.index.difference(dup_in_load_idx).difference(null_id_idx),
        "client_id",
    ].sample(
        n=per_type,
        replace=False,
        random_state=int(rng.integers(1, 1_000_000)),
    ).tolist()
    df.loc[dup_in_load_idx, "client_id"] = source_ids

    if existing_client_ids:
        reused = rng.choice(existing_client_ids, size=per_type).tolist()
        df.loc[dup_in_history_idx, "client_id"] = reused

    df.loc[invalid_reg_idx, "registration_date"] = pd.NaT

    return df, expected


def generate_order_amounts(
    count: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """
    Логнормальное распределение сумм заказов (большинство чеков небольшие,
    крупные — редкий хвост), клип в диапазон [500, 100_000] копеек.
    """
    raw = rng.lognormal(mean=8.0, sigma=0.9, size=count)
    clipped = np.clip(raw, 500, 100_000)
    return clipped.astype(np.int64)


def generate_orders(
    load_date: date,
    count: int,
    clients_df: pd.DataFrame,
    rng: np.random.Generator,
    error_rate: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """
    Создаёт `count` НОВЫХ заказов, все в статусе "new", created_at —
    сегодня с реалистичным временем суток. Дальнейшее продвижение по
    state machine — забота advance_open_orders(), не этой функции.
    """
    if count == 0:
        return pd.DataFrame(columns=ORDER_COLUMNS), {}

    if clients_df.empty:
        raise ValueError("Cannot generate orders without clients")

    clients = clients_df.reset_index(drop=True)

    client_idx = rng.integers(0, len(clients), size=count)
    chosen_clients = clients.iloc[client_idx].reset_index(drop=True)

    created_at = _realistic_timestamps([load_date] * count, rng)

    order_ids = [f"ord_{load_date:%Y%m%d}_{i + 1:08d}" for i in range(count)]

    df = pd.DataFrame(
        {
            "order_id": order_ids,
            "client_id": chosen_clients["client_id"].tolist(),
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
        "orders.null_client_id": per_type,
        "orders.negative_amount": per_type,
        "orders.invalid_status": per_type,
        "orders.duplicate_order_id_rows": per_type,
    }

    if per_type == 0:
        return df, expected

    indices = rng.permutation(df.index).tolist()
    cursor = 0

    null_client_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    negative_amount_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    invalid_status_idx = indices[cursor:cursor + per_type]
    cursor += per_type

    duplicate_idx = indices[cursor:cursor + per_type]

    df.loc[null_client_idx, "client_id"] = None
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
    order_ids = orders.index.tolist()

    day_start = datetime.combine(load_date, time.min)

    chosen_order_ids = rng.choice(order_ids, size=count)
    payment_methods = rng.choice(PAYMENT_METHODS, size=count)
    success_rolls = rng.random(count)

    rows: list[dict[str, Any]] = []
    paid_orders: dict[str, dict[str, Any]] = {}
    resolved: set[str] = set()

    # Сэмплирование с возвращением технически может выбрать один и тот же
    # order_id несколько раз за прогон. Обрабатываем попытки
    # последовательно и как только заказ попадает в `resolved` —
    # дальнейшие попытки на него просто пропускаются (строка не
    # генерируется вовсе), иначе получили бы несколько success-платежей
    # на один заказ, что ломает инвариант "не более одного успешного
    # платежа на заказ".
    for i in range(count):
        order_id = str(chosen_order_ids[i])

        if order_id in resolved:
            continue

        order = orders.loc[order_id]
        created_at = order["created_at"]

        earliest = max(created_at + PAYMENT_MIN_GAP, day_start)
        latest = now

        if earliest >= latest:
            latest = earliest + timedelta(seconds=1)

        attempt_at = _uniform_between(earliest, latest, rng)

        if success_rolls[i] < PAYMENT_SUCCESS_RATE:
            resolved.add(order_id)
            payment_method = str(payment_methods[i])

            updated = dict(order)
            updated["status"] = "paid"
            updated["paid_at"] = attempt_at
            updated["payment_method"] = payment_method
            paid_orders[order_id] = updated

            payment_status = "success"
            payment_id = f"pay_{order_id}"
        else:
            payment_method = str(payment_methods[i])
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
        for entity in ["clients", "orders", "payments"]:
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

    existing_clients_df = load_existing_clients(source_root, load_date)

    new_clients_df, clients_dq = generate_clients(
        load_date=load_date,
        count=args.clients_count,
        rng=rng,
        error_rate=args.error_rate,
        existing_client_ids=existing_clients_df["client_id"].dropna().tolist()
        if not existing_clients_df.empty
        else None,
    )

    all_clients_df = pd.concat(
        [existing_clients_df, new_clients_df],
        ignore_index=True,
    ).drop_duplicates("client_id")

    new_orders_df, orders_dq = generate_orders(
        load_date=load_date,
        count=args.orders_count,
        clients_df=all_clients_df,
        rng=rng,
        error_rate=args.error_rate,
    )

    lookback_days = compute_lookback_days(args)
    historical_open_df = load_existing_orders(source_root, load_date, lookback_days)

    candidate_pool_df = pd.concat(
        [new_orders_df, historical_open_df],
        ignore_index=True,
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
        cancel_before_payment_rate=args.cancel_before_payment_rate,
        cancel_after_payment_rate=args.cancel_after_payment_rate,
        return_rate=args.return_rate,
        error_rate=args.error_rate,
    )

    advanced_ids = set(advanced_df["order_id"]) if not advanced_df.empty else set()

    still_new_pool_df = candidate_pool_df[
        (candidate_pool_df["status"] == "new")
        & (~candidate_pool_df["order_id"].isin(advanced_ids))
    ]

    payments_df, payments_dq, paid_orders_df = generate_payments(
        load_date=load_date,
        new_status_orders_df=still_new_pool_df,
        count=args.payments_count,
        rng=rng,
        error_rate=args.error_rate,
        now=now,
    )

    paid_ids = set(paid_orders_df["order_id"]) if not paid_orders_df.empty else set()
    touched_ids = advanced_ids | paid_ids

    untouched_new_orders_df = new_orders_df[
        ~new_orders_df["order_id"].isin(touched_ids)
    ]

    orders_to_write_df = pd.concat(
        [untouched_new_orders_df, advanced_df, paid_orders_df],
        ignore_index=True,
    )
    payments_to_write_df = pd.concat(
        [payments_df, refund_payments_df],
        ignore_index=True,
    )

    all_dq = {**clients_dq, **orders_dq, **state_machine_dq, **payments_dq}
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
        clients_file = write_staged_csv(new_clients_df, staging_root, "clients")
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
            "entities": {
                "clients": {
                    "rows": len(new_clients_df),
                    "file": "clients.csv",
                    "sha256": sha256_file(clients_file),
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
    logger.info("Clients:          %s", f"{len(new_clients_df):,}")
    logger.info("Orders written:   %s", f"{len(orders_to_write_df):,}")
    logger.info("  brand new:      %s", f"{len(new_orders_df):,}")
    logger.info("  advanced:       %s", f"{len(advanced_df):,}")
    logger.info("  paid today:     %s", f"{len(paid_orders_df):,}")
    logger.info("Payments written: %s", f"{len(payments_to_write_df):,}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
