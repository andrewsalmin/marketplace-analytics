from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker


VALID_ORDER_STATUSES = ["new", "paid", "shipped", "cancelled"]
VALID_PAYMENT_STATUSES = ["success", "failed", "pending"]

CITIES = [
    "Moscow",
    "Saint Petersburg",
    "Kazan",
    "Novosibirsk",
    "Yekaterinburg",
    "Samara",
    "Krasnodar",
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

SCHEMA_VERSION = "2.0.0"
GENERATOR_VERSION = "2.0.0"


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

    return min(requested, total_rows // error_types)


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
        return pd.DataFrame(
            columns=[
                "client_id",
                "registration_date",
                "city",
                "acquisition_channel",
                "email",
            ]
        )

    files = sorted(clients_root.glob("load_date=*/clients.csv"))

    if not files:
        return pd.DataFrame(
            columns=[
                "client_id",
                "registration_date",
                "city",
                "acquisition_channel",
                "email",
            ]
        )

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
    fake = Faker("en_US")
    fake.seed_instance(int(load_date.strftime("%Y%m%d")))

    rows: list[dict[str, Any]] = []

    for i in range(count):
        registration_date = load_date - timedelta(
            days=int(rng.integers(0, 365))
        )

        rows.append(
            {
                # ID зависит только от даты загрузки и номера строки.
                "client_id": f"cl_{load_date:%Y%m%d}_{i + 1:08d}",
                "registration_date": registration_date,
                "city": str(rng.choice(CITIES)),
                "acquisition_channel": str(
                    rng.choice(ACQUISITION_CHANNELS)
                ),
                "email": fake.unique.email(),
            }
        )

    return pd.DataFrame(rows)


def choose_order_status(rng: np.random.Generator) -> str:
    """
    Взвешенное распределение ближе к реальному e-commerce:
    большинство заказов оплачены или отправлены.
    """
    return str(
        rng.choice(
            VALID_ORDER_STATUSES,
            p=[0.08, 0.42, 0.40, 0.10],
        )
    )


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

    rows: list[dict[str, Any]] = []

    for i in range(count):
        client = clients.iloc[int(rng.integers(0, len(clients)))]
        registration_date: date = client["registration_date"]

        earliest_order_date = max(
            registration_date,
            load_date - timedelta(days=29),
        )

        allowed_days = (load_date - earliest_order_date).days
        order_date = earliest_order_date + timedelta(
            days=int(rng.integers(0, allowed_days + 1))
        )

        rows.append(
            {
                "order_id": f"ord_{load_date:%Y%m%d}_{i + 1:08d}",
                "client_id": client["client_id"],
                "order_date": order_date,
                "amount_kopecks": int(rng.integers(500, 100_000)),
                "status": choose_order_status(rng),
            }
        )

    df = pd.DataFrame(rows)

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

    # Дубликат создаётся заменой order_id, без увеличения общего числа строк.
    # Каждая duplicate-строка ссылается на существующий валидный order_id.
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


def generate_payments(
    load_date: date,
    orders_df: pd.DataFrame,
    count: int,
    rng: np.random.Generator,
    error_rate: float,
) -> tuple[pd.DataFrame, dict[str, int]]:
    if count == 0:
        return pd.DataFrame(), {}

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

    rows: list[dict[str, Any]] = []
    successfully_paid_orders: set[str] = set()

    for i in range(count):
        order_id = str(rng.choice(order_ids))
        order = order_map[order_id]

        order_date = pd.Timestamp(order["order_date"]).date()
        status = order["status"]

        max_days_after_order = min((load_date - order_date).days, 7)
        payment_date = order_date + timedelta(
            days=int(rng.integers(0, max_days_after_order + 1))
        )

        # Успешная оплата возможна один раз и только для paid/shipped.
        if (
            status in {"paid", "shipped"}
            and order_id not in successfully_paid_orders
            and rng.random() < 0.75
        ):
            payment_status = "success"
            successfully_paid_orders.add(order_id)
        elif rng.random() < 0.55:
            payment_status = "failed"
        else:
            payment_status = "pending"

        rows.append(
            {
                "payment_id": f"pay_{load_date:%Y%m%d}_{i + 1:08d}",
                "order_id": order_id,
                "payment_date": payment_date,
                "amount_kopecks": int(order["amount_kopecks"]),
                "payment_method": str(rng.choice(PAYMENT_METHODS)),
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

    Поэтому контракт такой:
    - сущности перемещаются из staging;
    - загрузка считается доступной только после commit marker;
    - downstream должен читать только committed load_date.
    """
    load_date_str = load_date.isoformat()

    for entity in ["clients", "orders", "payments"]:
        source_entity_dir = staging_root / entity
        target_entity_dir = (
            source_root / entity / f"load_date={load_date_str}"
        )

        target_entity_dir.parent.mkdir(parents=True, exist_ok=True)
        source_entity_dir.replace(target_entity_dir)

    commit_dir = source_root / "_commits" / f"load_date={load_date_str}"
    commit_dir.mkdir(parents=True, exist_ok=False)

    with (commit_dir / "commit.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


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
            "expected_quality_issues": {
                **orders_dq,
                **payments_dq,
            },
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

    print("Generation completed successfully.")
    print(f"Load date: {load_date}")
    print(f"Clients:   {len(new_clients_df):,}")
    print(f"Orders:    {len(orders_df):,}")
    print(f"Payments:  {len(payments_df):,}")


if __name__ == "__main__":
    main()