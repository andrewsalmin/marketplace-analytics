import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd


ENTITIES = {
    "clients": {
        "string_columns": [
            "client_id",
            "city",
            "acquisition_channel",
            "email",
        ],
        "date_columns": ["registration_date"],
    },
    "orders": {
        "string_columns": [
            "order_id",
            "client_id",
            "status",
        ],
        "date_columns": ["order_date"],
    },
    "payments": {
        "string_columns": [
            "payment_id",
            "order_id",
            "payment_method",
            "status",
        ],
        "date_columns": ["payment_date"],
    },
}


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

    return parser.parse_args()


def ensure_not_exists(path: Path, layer_name: str):
    if path.exists():
        raise FileExistsError(
            f"{layer_name}-партиция уже существует: {path}\n"
            "Перезапись запрещена."
        )


def read_source_csv(source_file: Path, entity: str) -> pd.DataFrame:
    """Читает CSV и приводит только технические типы данных.

    Важно: здесь не исправляются DQ-ошибки:
    дубли, NULL, отрицательные суммы и некорректные статусы сохраняются.
    """
    config = ENTITIES[entity]

    dtype = {
        column: "string"
        for column in (
                config["string_columns"]
                + config["date_columns"]
        )
    }

    df = pd.read_csv(
        source_file,
        dtype=dtype,
        encoding="utf-8",
    )

    if df.empty:
        raise ValueError(f"Файл пустой: {source_file}")

    # amount_kopecks остаётся целочисленным типом.
    # Nullable Int64 позволяет сохранить возможные NULL в будущих данных.
    if "amount_kopecks" in df.columns:
        df["amount_kopecks"] = df["amount_kopecks"].astype("Int64")

    return df


def write_raw_parquet(
    df: pd.DataFrame,
    raw_root: Path,
    entity: str,
    load_date: date,
):
    partition_dir = raw_root / entity / f"load_date={load_date.isoformat()}"
    ensure_not_exists(partition_dir, "Raw")

    partition_dir.mkdir(parents=True, exist_ok=False)

    target_file = partition_dir / f"{entity}.parquet"
    temp_file = partition_dir / f".{entity}.parquet.tmp"

    df.to_parquet(
        temp_file,
        engine="pyarrow",
        compression="zstd",
        index=False,
    )

    temp_file.rename(target_file)

    print(f"[RAW] Saved {len(df):,} rows -> {target_file}")


def save_ingest_manifest(
    raw_root: Path,
    load_date: date,
    results: dict,
):
    manifest_dir = raw_root / "_manifests" / f"load_date={load_date.isoformat()}"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "load_date": load_date.isoformat(),
        "source_format": "csv",
        "target_format": "parquet",
        "entities": results,
    }

    with open(
        manifest_dir / "ingest_manifest.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(manifest, file, ensure_ascii=False, indent=2)


def main():
    args = parse_args()

    load_date = date.fromisoformat(args.load_date)
    data_dir = Path(args.data_dir)

    source_root = data_dir / "source"
    raw_root = data_dir / "raw"

    results = {}

    for entity in ENTITIES:
        source_file = (
            source_root
            / entity
            / f"load_date={load_date.isoformat()}"
            / f"{entity}.csv"
        )

        if not source_file.exists():
            raise FileNotFoundError(
                f"Не найден исходный CSV: {source_file}"
            )

        df = read_source_csv(source_file, entity)

        write_raw_parquet(
            df=df,
            raw_root=raw_root,
            entity=entity,
            load_date=load_date,
        )

        results[entity] = {
            "source_file": str(source_file),
            "rows": len(df),
        }

    save_ingest_manifest(
        raw_root=raw_root,
        load_date=load_date,
        results=results,
    )

    print("\nCSV ingestion completed successfully.")


if __name__ == "__main__":
    main()