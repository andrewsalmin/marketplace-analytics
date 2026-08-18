from __future__ import annotations

import argparse
import hashlib
import json
import logging
import shutil
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger(__name__)


VALID_ORDER_STATUSES = [
    "new",
    "paid",
    "shipped",
    "delivered",
    "cancelled",
    "returned",
    "refunded",
]
VALID_PAYMENT_STATUSES = ["pending", "success", "failed", "refunded", "chargeback"]

# Funnel-веса генерации статуса заказа (сумма = 1.0), индексы совпадают
# с VALID_ORDER_STATUSES. Порядок отражает жизненный цикл:
# new -> paid -> shipped -> delivered, с терминальными ветками
# cancelled / returned / refunded.
ORDER_STATUS_WEIGHTS = [0.05, 0.10, 0.10, 0.55, 0.10, 0.06, 0.04]

# Согласованность order.status <-> payment.status.
# Ключ — статус заказа, значение — вероятности типов платежа, которые
# логически с ним совместимы. Это не полноценная state machine с явными
# переходами (одна строка orders = один финальный статус, без истории
# переходов), но она исключает физически невозможные комбинации вроде
# "success-платёж на заказ в статусе new" или "refunded-платёж на
# delivered-заказ без единого success до этого".
#
# Известное упрощение: если заказ вернулся к "cancelled"/"returned"/
# "refunded" уже ПОСЛЕ того как был оплачен, мы не храним историю
# промежуточных статусов — заказ в источнике фиксируется только в
# финальном статусе, как это обычно и происходит при выгрузке снапшота
# из транзакционной системы (а не event-лога).

CITIES = [
    "Москва",
    "Санкт-Петербург",
    "Казань",
    "Новосибирск",
    "Екатеринбург",
    "Самара",
    "Краснодар",
]

ACQUISITION_CHANNELS = [
    "organic",
    "google_ads",
    "yandex_direct",
    "social",
    "referral",
    "email",
]

PAYMENT_METHODS = [
    "card",
    "sbp",
    "apple_pay",
    "google_pay",
    "cash",
]

CLIENT_COLUMNS = [
    "client_id",
    "registration_date",
    "city",
    "acquisition_channel",
    "email",
]

SCHEMA_VERSION = "3.0.0"
GENERATOR_VERSION = "3.0.0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--load-date", required=True)
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--clients-count", type=int, default=100)
    parser.add_argument("--orders-count", type=int, default=15_000)
    parser.add_argument("--payments-count", type=int, default=16_000)
    parser.add_argument("--error-rate", type=float, default=0.01)

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    for field in ["clients_count", "orders_count", "payments_count"]:
        value = getattr(args, field)
        if value < 0:
            raise ValueError(f"{field} must be >= 0")

    if not 0 <= args.error_rate <= 1:
        raise ValueError("--error-rate must be between 0 and 1")

    if args.orders_count > 0 and args.clients_count == 0:
        raise ValueError(
            "Cannot generate orders when clients-count=0. "
            "Generate clients first or set orders-count=0."
        )

    # ИСПРАВЛЕНО (найдено тестом test_raises_without_valid_orders): в
    # отличие от clients (которые накапливаются между запусками — новый
    # заказ может ссылаться на клиента из ЛЮБОГО прошлого load_date),
    # payments в этом генераторе ссылаются ТОЛЬКО на orders_df ТЕКУЩЕГО
    # запуска (см. main() — generate_payments получает orders_df именно
    # из generate_orders этого же запуска, а не накопленную историю).
    # Поэтому эта проверка, в отличие от clients_count выше, всегда
    # корректна: при orders-count=0 валидных заказов для оплаты в этом
    # запуске не будет физически ни при каких исторических данных.
    if args.payments_count > 0 and args.orders_count == 0:
        raise ValueError(
            "Cannot generate payments when orders-count=0. "
            "Generate orders first or set payments-count=0."
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

    ИСПРАВЛЕНО: раньше при total_rows // error_types == 0 (маленький батч
    с ненулевым error_rate) функция тихо возвращала 0 и DQ-инжекция
    отключалась незаметно для вызывающего кода. Теперь при requested > 0
    гарантируется минимум 1 ошибка каждого типа, если для этого хватает строк
    (total_rows >= error_types), и явно возвращается 0 только когда строк
    физически не хватает даже на по одной ошибке каждого типа — это
    подходящее место, чтобы вызывающий код мог залогировать предупреждение.
    """
    if total_rows == 0 or error_rate == 0:
        return 0

    requested = int(total_rows * error_rate)

    if requested == 0:
        requested = 1

    if total_rows < error_types:
        # Строк физически не хватает, чтобы инжектировать хотя бы по одной
        # ошибке каждого типа в непересекающиеся строки.
        return 0

    per_type = requested // error_types

    if per_type == 0:
        # requested > 0, но его не хватает, чтобы дать по 1 на каждый тип
        # при делении. Гарантируем минимум 1 на тип, за счёт округления
        # вниз по числу доступных строк.
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
    clients["registration_date"] = pd.to_datetime(
        clients["registration_date"]
    ).dt.date

    clients = clients[
        clients["registration_date"] <= load_date
    ].drop_duplicates("client_id")

    return clients.reset_index(drop=True)


def generate_clients(
    load_date: date,
    count: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    if count == 0:
        return pd.DataFrame(columns=CLIENT_COLUMNS)

    # ru_RU: email строится из русских имён, транслитерированных в
    # латиницу (например Иван Петров -> ivan.petrov@mail.ru), а не из
    # американских имён — консистентно с городами ниже.
    fake = Faker("ru_RU")
    fake.seed_instance(int(load_date.strftime("%Y%m%d")))

    # ИСПРАВЛЕНО: векторизованная генерация вместо построчного цикла.
    # Faker.unique.email() не векторизуется (у него собственное состояние
    # уникальности), поэтому email оставлен в цикле — это единственная
    # часть, которая физически не может быть векторизована без замены
    # библиотеки. Остальные поля генерируются numpy-массивами разом.
    offsets = rng.integers(0, 365, size=count)
    registration_dates = [load_date - timedelta(days=int(d)) for d in offsets]
    cities = rng.choice(CITIES, size=count)
    channels = rng.choice(ACQUISITION_CHANNELS, size=count)
    emails = [fake.unique.email() for _ in range(count)]

    client_ids = [
        f"cl_{load_date:%Y%m%d}_{i + 1:08d}" for i in range(count)
    ]

    return pd.DataFrame(
        dict(
            zip(
                CLIENT_COLUMNS,
                [client_ids, registration_dates, cities, channels, emails],
                strict=True,
            )
        )
    )


def generate_order_amounts(
    count: int,
    rng: np.random.Generator,
) -> np.ndarray:
    """
    ИСПРАВЛЕНО: суммы заказов раньше были равномерно распределены между
    500 и 100_000 копеек, что нереалистично для e-commerce (в реальности
    большинство чеков небольшие, а крупные — редкий хвост). Заменено на
    логнормальное распределение с последующим клипом в тот же диапазон,
    чтобы даунстрим-контракт (границы значений) не менялся.
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
    if count == 0:
        return pd.DataFrame(), {}

    if clients_df.empty:
        raise ValueError("Cannot generate orders without clients")

    clients = clients_df.copy()
    clients["registration_date"] = pd.to_datetime(
        clients["registration_date"]
    ).dt.date
    clients = clients.reset_index(drop=True)

    # ИСПРАВЛЕНО: векторизованный выбор клиентов и дат заказов вместо
    # построчного iloc + integers в цикле. allowed_days зависит от клиента,
    # поэтому даты всё ещё считаются поэлементно, но без обращения к
    # DataFrame через .iloc на каждой итерации (это была основная стоимость).
    client_idx = rng.integers(0, len(clients), size=count)
    chosen_clients = clients.iloc[client_idx].reset_index(drop=True)

    earliest_order_dates = chosen_clients["registration_date"].apply(
        lambda reg: max(reg, load_date - timedelta(days=29))
    )
    allowed_days = earliest_order_dates.apply(
        lambda d: (load_date - d).days
    )
    day_offsets = np.array(
        [int(rng.integers(0, ad + 1)) for ad in allowed_days]
    )
    order_dates = [
        earliest + timedelta(days=int(off))
        for earliest, off in zip(earliest_order_dates, day_offsets, strict=True)
    ]

    order_ids = [
        f"ord_{load_date:%Y%m%d}_{i + 1:08d}" for i in range(count)
    ]

    df = pd.DataFrame(
        {
            "order_id": order_ids,
            # ИСПРАВЛЕНО: .values иногда отдаёт read-only ndarray (зависит
            # от pandas string backend), из-за чего дальнейший
            # df.loc[idx, "client_id"] = None падает с
            # "assignment destination is read-only". .tolist() гарантирует
            # свежий изменяемый список.
            "client_id": chosen_clients["client_id"].tolist(),
            "order_date": order_dates,
            "amount_kopecks": generate_order_amounts(count, rng),
            "status": rng.choice(
                VALID_ORDER_STATUSES,
                size=count,
                p=ORDER_STATUS_WEIGHTS,
            ),
        }
    )

    per_type = error_count(
        total_rows=count,
        error_rate=error_rate,
        error_types=4,
    )

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


def draw_payment_status(
    order_status: str,
    already_succeeded: bool,
    primary_roll: float,
    secondary_roll: float,
    chargeback_roll: float,
) -> str:
    """
    Возвращает статус платежа, СОГЛАСОВАННЫЙ со статусом заказа.

    Правила согласованности (упрощённая state machine):
    - "new": заказ ещё не оплачен по определению -> платёж только
      pending (ждём оплату) или failed (попытка не прошла, заказ
      поэтому и остался в new). Никогда не success.
    - "paid" / "shipped" / "delivered": заказ прогрессирует по воронке
      только если оплата прошла -> платёж почти всегда success (один
      раз на заказ, дальше already_succeeded это гарантирует), редкие
      pending/failed — это дополнительные строки-попытки (retries),
      которые в реальных системах тоже остаются в логе платежей.
      Небольшая доля success дополнительно оборачивается в chargeback
      (банк отменил платёж постфактум) — это НЕ меняет order.status,
      что соответствует известному упрощению, описанному выше.
    - "cancelled": в основном платёж failed (типичная причина отмены),
      меньшая доля refunded (заказ успели оплатить, потом отменили).
    - "returned" / "refunded": деньги в итоге возвращены -> платёж
      refunded. Малая доля "returned"-заказов может дать одну success-
      строку (это как бы исходный платёж до момента возврата).
    """
    if order_status == "new":
        return "pending" if primary_roll < 0.7 else "failed"

    if order_status == "cancelled":
        return "refunded" if primary_roll < 0.2 else "failed"

    if order_status == "returned":
        if not already_succeeded and primary_roll < 0.3:
            return "success"
        return "refunded"

    if order_status == "refunded":
        return "refunded" if primary_roll < 0.85 else "chargeback"

    # paid / shipped / delivered
    if not already_succeeded and primary_roll < 0.85:
        if chargeback_roll < 0.03:
            return "chargeback"
        return "success"

    return "pending" if secondary_roll < 0.5 else "failed"


def generate_payments(
    load_date: date,
    orders_df: pd.DataFrame,
    count: int,
    rng: np.random.Generator,
    error_rate: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if count == 0:
        return pd.DataFrame(), {}

    # ИСПРАВЛЕНО (найдено тестом test_raises_without_valid_orders):
    # orders_df, возвращённый generate_orders(count=0), это pd.DataFrame()
    # БЕЗ КОЛОНОК ВООБЩЕ. Без этой проверки следующий фильтр падал с сырым
    # KeyError: 'order_id' вместо понятного ValueError — например при
    # запуске с --orders-count 0 --payments-count 100.
    if orders_df.empty:
        raise ValueError("Cannot generate payments without valid orders")

    valid_orders = orders_df[
        orders_df["order_id"].notna()
        & orders_df["client_id"].notna()
        & (orders_df["amount_kopecks"] > 0)
        & orders_df["status"].isin(VALID_ORDER_STATUSES)
    ].drop_duplicates("order_id")

    if valid_orders.empty:
        raise ValueError("Cannot generate payments without valid orders")

    order_map = valid_orders.set_index("order_id").to_dict("index")
    order_ids = list(order_map.keys())

    # Логика "успешная оплата максимум один раз на заказ" по своей природе
    # последовательна (нужно помнить состояние successfully_paid_orders
    # между строками одного и того же order_id), поэтому полностью
    # векторизовать цикл нельзя без изменения бизнес-правила. Оставлено
    # построчным, но обращение к order_map — это O(1) dict lookup, а не
    # DataFrame.iloc, так что стоимость на порядок ниже, чем в исходнике.
    rows: list[dict[str, Any]] = []
    successfully_paid_orders: set[str] = set()

    chosen_order_ids = rng.choice(order_ids, size=count)
    primary_rolls = rng.random(count)
    secondary_rolls = rng.random(count)
    chargeback_rolls = rng.random(count)
    day_offset_rolls = rng.random(count)
    payment_methods = rng.choice(PAYMENT_METHODS, size=count)

    for i in range(count):
        order_id = str(chosen_order_ids[i])
        order = order_map[order_id]

        order_date = pd.Timestamp(order["order_date"]).date()
        order_status = order["status"]

        max_days_after_order = min((load_date - order_date).days, 7)
        payment_date = order_date + timedelta(
            days=int(day_offset_rolls[i] * (max_days_after_order + 1))
        )

        payment_status = draw_payment_status(
            order_status=order_status,
            already_succeeded=order_id in successfully_paid_orders,
            primary_roll=primary_rolls[i],
            secondary_roll=secondary_rolls[i],
            chargeback_roll=chargeback_rolls[i],
        )

        if payment_status == "success":
            successfully_paid_orders.add(order_id)

        rows.append(
            {
                "payment_id": f"pay_{load_date:%Y%m%d}_{i + 1:08d}",
                "order_id": order_id,
                "payment_date": payment_date,
                "amount_kopecks": int(order["amount_kopecks"]),
                "payment_method": str(payment_methods[i]),
                "status": payment_status,
            }
        )

    df = pd.DataFrame(rows)

    per_type = error_count(
        total_rows=count,
        error_rate=error_rate,
        error_types=5,
    )

    expected = {
        "payments.missing_order_id": per_type,
        "payments.negative_amount": per_type,
        "payments.invalid_status": per_type,
        "payments.payment_before_order": per_type,
        "payments.duplicate_payment_id_rows": per_type,
    }

    if per_type == 0:
        return df, expected

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
        f"ord_missing_{load_date:%Y%m%d}_{i:08d}"
        for i in range(per_type)
    ]

    df.loc[negative_amount_idx, "amount_kopecks"] *= -1
    df.loc[invalid_status_idx, "status"] = "invalid_payment_status"

    for idx in date_error_idx:
        order_id = df.loc[idx, "order_id"]

        if order_id in order_map:
            order_date = pd.Timestamp(
                order_map[order_id]["order_date"]
            ).date()

            df.loc[idx, "payment_date"] = order_date - timedelta(days=1)

    source_ids = df.loc[
        df.index.difference(duplicate_idx),
        "payment_id",
    ].sample(
        n=per_type,
        replace=False,
        random_state=int(rng.integers(1, 1_000_000)),
    ).tolist()

    df.loc[duplicate_idx, "payment_id"] = source_ids

    return df, expected


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
        date_format="%Y-%m-%d",
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

    ИСПРАВЛЕНО: раньше сущности перемещались из staging последовательно,
    и при падении процесса между вторым и третьим Path.replace() на диске
    оставались "осиротевшие" директории entity/load_date=... без
    commit-маркера, которые ensure_batch_does_not_exist считает "уже
    существующим load_date" — то есть batch навсегда застревал и не мог
    быть перегенерирован без ручной зачистки.

    Теперь каждый успешный replace() запоминается, и если что-то падает
    до создания commit-маркера, уже перемещённые директории откатываются
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
        # Откатываем всё, что уже успело переехать в source_root, обратно
        # в staging, чтобы source_root не содержал частично опубликованных
        # данных без commit-маркера.
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

    existing_clients_df = load_existing_clients(source_root, load_date)

    new_clients_df = generate_clients(
        load_date=load_date,
        count=args.clients_count,
        rng=rng,
    )

    all_clients_df = pd.concat(
        [existing_clients_df, new_clients_df],
        ignore_index=True,
    ).drop_duplicates("client_id")

    orders_df, orders_dq = generate_orders(
        load_date=load_date,
        count=args.orders_count,
        clients_df=all_clients_df,
        rng=rng,
        error_rate=args.error_rate,
    )

    payments_df, payments_dq = generate_payments(
        load_date=load_date,
        orders_df=orders_df,
        count=args.payments_count,
        rng=rng,
        error_rate=args.error_rate,
    )

    # ИСПРАВЛЕНО: раньше если error_rate > 0, но батч слишком мал, чтобы
    # инжектировать хотя бы одну ошибку каждого типа, DQ-тестирование
    # молча отключалось. Теперь это явно предупреждается в stdout и
    # фиксируется в манифесте, чтобы потребитель манифеста мог это
    # обнаружить программно, а не только по логам запуска генератора.
    all_dq = {**orders_dq, **payments_dq}
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
        clients_file = write_staged_csv(
            new_clients_df,
            staging_root,
            "clients",
        )
        orders_file = write_staged_csv(
            orders_df,
            staging_root,
            "orders",
        )
        payments_file = write_staged_csv(
            payments_df,
            staging_root,
            "payments",
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
                    "rows": len(orders_df),
                    "file": "orders.csv",
                    "sha256": sha256_file(orders_file),
                },
                "payments": {
                    "rows": len(payments_df),
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
    logger.info("Clients:   %s", f"{len(new_clients_df):,}")
    logger.info("Orders:    %s", f"{len(orders_df):,}")
    logger.info("Payments:  %s", f"{len(payments_df):,}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
