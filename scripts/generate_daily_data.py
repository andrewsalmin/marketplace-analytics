from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

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


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--load-date",
        required=True,
        help="Дата загрузки в формате YYYY-MM-DD",
    )

    parser.add_argument(
        "--data-dir",
        default="data",
        help="Корневая директория данных",
    )

    parser.add_argument(
        "--clients-count",
        type=int,
        default=100,
        help="Количество новых клиентов за запуск",
    )

    parser.add_argument(
        "--orders-count",
        type=int,
        default=15000,
        help="Количество заказов за запуск",
    )

    parser.add_argument(
        "--payments-count",
        type=int,
        default=16000,
        help="Количество платежей за запуск",
    )

    parser.add_argument(
        "--error-rate",
        type=float,
        default=0.01,
        help="Доля контролируемых ошибок, например 0.01 = 1%",
    )

    return parser.parse_args()


def make_rng(load_date: date) -> np.random.Generator:
    """
    Детерминированный random seed:
    при одинаковой дате генератор создаёт одинаковые данные.
    """
    seed = int(load_date.strftime("%Y%m%d"))
    return np.random.default_rng(seed)


def ensure_partition_not_exists(path: Path):
    if path.exists():
        raise FileExistsError(
            f"Партиция уже существует: {path}\n"
            "Raw-слой нельзя молча перезаписывать. "
            "Используйте другую дату или удалите партицию вручную."
        )


def write_source_csv(
    df: pd.DataFrame,
    source_root: Path,
    entity: str,
    load_date: date,
):
    partition_dir = source_root / entity / f"load_date={load_date.isoformat()}"
    ensure_partition_not_exists(partition_dir)

    partition_dir.mkdir(parents=True, exist_ok=False)

    target_file = partition_dir / f"{entity}.csv"
    temp_file = partition_dir / f".{entity}.csv.tmp"

    df.to_csv(
        temp_file,
        index=False,
        encoding="utf-8",
        date_format="%Y-%m-%d",
    )

    temp_file.rename(target_file)

    print(f"[SOURCE] Saved {len(df):,} rows -> {target_file}")


def read_clients_registry(state_dir: Path) -> pd.DataFrame:
    registry_path = state_dir / "clients_registry.parquet"

    if not registry_path.exists():
        return pd.DataFrame(columns=["client_id"])

    return pd.read_parquet(registry_path)


def save_clients_registry(state_dir: Path, registry_df: pd.DataFrame):
    state_dir.mkdir(parents=True, exist_ok=True)

    registry_path = state_dir / "clients_registry.parquet"
    temp_path = state_dir / ".clients_registry.parquet.tmp"

    registry_df.to_parquet(
        temp_path,
        engine="pyarrow",
        compression="zstd",
        index=False,
    )

    temp_path.replace(registry_path)


def generate_clients(
    load_date: date,
    count: int,
    existing_clients_count: int,
    rng: np.random.Generator,
) -> pd.DataFrame:
    fake = Faker("en_US")
    fake.seed_instance(int(load_date.strftime("%Y%m%d")))

    rows = []

    for i in range(count):
        sequence_number = existing_clients_count + i + 1
        registration_date = load_date - timedelta(
            days=int(rng.integers(0, 365))
        )

        rows.append(
            {
                "client_id": f"cl_{sequence_number:010d}",
                "registration_date": registration_date,
                "city": rng.choice(CITIES),
                "acquisition_channel": rng.choice(ACQUISITION_CHANNELS),
                "email": fake.email(),
            }
        )

    return pd.DataFrame(rows)


def generate_orders(
    load_date: date,
    count: int,
    all_client_ids: list[str],
    rng: np.random.Generator,
    error_rate: float,
) -> pd.DataFrame:
    rows = []

    for i in range(count):
        order_date = load_date - timedelta(days=int(rng.integers(0, 30)))

        rows.append(
            {
                "order_id": f"ord_{load_date.strftime('%Y%m%d')}_{i + 1:08d}",
                "client_id": rng.choice(all_client_ids),
                "order_date": order_date,
                "amount_kopecks": int(rng.integers(500, 100_000)),
                "status": rng.choice(VALID_ORDER_STATUSES),
            }
        )

    df = pd.DataFrame(rows)

    error_count = max(1, int(count * error_rate))

    # 1. Пустой client_id
    null_client_idx = rng.choice(df.index, size=error_count, replace=False)
    df.loc[null_client_idx, "client_id"] = None

    # 2. Отрицательные суммы
    negative_amount_idx = rng.choice(df.index, size=error_count, replace=False)
    df.loc[negative_amount_idx, "amount_kopecks"] *= -1

    # 3. Неизвестный статус
    invalid_status_idx = rng.choice(df.index, size=error_count, replace=False)
    df.loc[invalid_status_idx, "status"] = "unknown_status"

    # 4. Дубли заказов
    duplicate_rows = df.sample(
        n=error_count,
        random_state=int(rng.integers(1, 1_000_000)),
    ).copy()

    df = pd.concat([df, duplicate_rows], ignore_index=True)

    return df


def generate_payments(
    load_date: date,
    orders_df: pd.DataFrame,
    count: int,
    rng: np.random.Generator,
    error_rate: float,
) -> pd.DataFrame:
    valid_orders = orders_df.drop_duplicates("order_id").copy()

    order_dates = dict(
        zip(valid_orders["order_id"], valid_orders["order_date"])
    )

    order_amounts = dict(
        zip(valid_orders["order_id"], valid_orders["amount_kopecks"])
    )

    valid_order_ids = list(order_dates.keys())

    rows = []

    for i in range(count):
        order_id = rng.choice(valid_order_ids)
        order_date = order_dates[order_id]

        max_days_after_order = max((load_date - order_date).days, 0)
        payment_date = order_date + timedelta(
            days=int(rng.integers(0, min(max_days_after_order, 7) + 1))
        )

        original_amount = abs(int(order_amounts[order_id]))
        payment_amount = original_amount

        rows.append(
            {
                "payment_id": f"pay_{load_date.strftime('%Y%m%d')}_{i + 1:08d}",
                "order_id": order_id,
                "payment_date": payment_date,
                "amount_kopecks": payment_amount,
                "payment_method": rng.choice(PAYMENT_METHODS),
                "status": rng.choice(VALID_PAYMENT_STATUSES),
            }
        )

    df = pd.DataFrame(rows)
    error_count = max(1, int(count * error_rate))

    # 1. Несуществующий order_id
    missing_order_idx = rng.choice(df.index, size=error_count, replace=False)
    df.loc[missing_order_idx, "order_id"] = [
        f"ord_missing_{i:08d}" for i in range(error_count)
    ]

    # 2. Отрицательная сумма
    negative_amount_idx = rng.choice(df.index, size=error_count, replace=False)
    df.loc[negative_amount_idx, "amount_kopecks"] *= -1

    # 3. Неизвестный payment status
    invalid_status_idx = rng.choice(df.index, size=error_count, replace=False)
    df.loc[invalid_status_idx, "status"] = "invalid_payment_status"

    # 4. Платёж раньше заказа
    date_error_idx = rng.choice(df.index, size=error_count, replace=False)

    for idx in date_error_idx:
        order_id = df.loc[idx, "order_id"]

        if order_id in order_dates:
            df.loc[idx, "payment_date"] = (
                order_dates[order_id] - timedelta(days=1)
            )

    # 5. Дубли payment_id
    duplicate_rows = df.sample(
        n=error_count,
        random_state=int(rng.integers(1, 1_000_000)),
    ).copy()

    df = pd.concat([df, duplicate_rows], ignore_index=True)

    return df


def save_generation_manifest(
    source_root: Path,
    load_date: date,
    clients_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    payments_df: pd.DataFrame,
    error_rate: float,
):
    manifest_dir = source_root / "_manifests" / f"load_date={load_date.isoformat()}"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "load_date": load_date.isoformat(),
        "entities": {
            "clients": {"rows": len(clients_df)},
            "orders": {"rows": len(orders_df)},
            "payments": {"rows": len(payments_df)},
        },
        "configured_error_rate": error_rate,
    }

    with open(manifest_dir / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def main():
    args = parse_args()

    load_date = date.fromisoformat(args.load_date)
    data_dir = Path(args.data_dir)

    source_root = data_dir / "source"
    state_dir = data_dir / "_generator_state"

    rng = make_rng(load_date)

    existing_clients_df = read_clients_registry(state_dir)

    clients_df = generate_clients(
        load_date=load_date,
        count=args.clients_count,
        existing_clients_count=len(existing_clients_df),
        rng=rng,
    )

    all_clients_df = pd.concat(
        [existing_clients_df, clients_df[["client_id"]]],
        ignore_index=True,
    )

    orders_df = generate_orders(
        load_date=load_date,
        count=args.orders_count,
        all_client_ids=all_clients_df["client_id"].tolist(),
        rng=rng,
        error_rate=args.error_rate,
    )

    payments_df = generate_payments(
        load_date=load_date,
        orders_df=orders_df,
        count=args.payments_count,
        rng=rng,
        error_rate=args.error_rate,
    )

    # Source-слой содержит исходные CSV, включая намеренно добавленные ошибки.
    write_source_csv(clients_df, source_root, "clients", load_date)
    write_source_csv(orders_df, source_root, "orders", load_date)
    write_source_csv(payments_df, source_root, "payments", load_date)

    save_generation_manifest(
        source_root=source_root,
        load_date=load_date,
        clients_df=clients_df,
        orders_df=orders_df,
        payments_df=payments_df,
        error_rate=args.error_rate,
    )

    save_clients_registry(
        state_dir=state_dir,
        registry_df=all_clients_df.drop_duplicates("client_id"),
    )

    print("\nGeneration completed successfully.")
    print(f"Load date: {load_date}")
    print(f"Clients:   {len(clients_df):,}")
    print(f"Orders:    {len(orders_df):,}")
    print(f"Payments:  {len(payments_df):,}")


if __name__ == "__main__":
    main()