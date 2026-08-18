"""
Тесты покрывают в первую очередь то, что реально ломалось в процессе
разработки (см. историю ревью): скрытое обнуление DQ-инжекции на
маленьких батчах, неатомарную публикацию батча, и согласованность
order.status <-> payment.status после введения funnel-статусов.

Запуск: pytest -v   (из корня репозитория)
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import generate_data as gd

# ---------------------------------------------------------------------
# error_count
# ---------------------------------------------------------------------


class TestErrorCount:
    def test_zero_rows_gives_zero(self):
        assert gd.error_count(total_rows=0, error_rate=0.1, error_types=4) == 0

    def test_zero_error_rate_gives_zero(self):
        assert gd.error_count(total_rows=1000, error_rate=0.0, error_types=4) == 0

    def test_normal_case_splits_evenly(self):
        # 1000 rows * 10% = 100 errors requested, / 4 types = 25 per type
        assert gd.error_count(total_rows=1000, error_rate=0.10, error_types=4) == 25

    def test_too_few_rows_for_any_error_type_returns_zero(self):
        # Раньше это тихо "работало" и просто возвращало 0 без объяснений.
        # Сейчас это тоже 0, но main() теперь явно предупреждает об этом
        # (см. test_main_warns_about_skipped_dq_types).
        assert gd.error_count(total_rows=2, error_rate=0.9, error_types=4) == 0

    def test_small_batch_still_gets_at_least_one_error_per_type(self):
        # РЕГРЕССИЯ: раньше total_rows=10, error_rate=0.01, error_types=4
        # давало int(10*0.01)=0 -> requested=1 (форсировано) -> 1 // 4 = 0,
        # то есть DQ-инжекция незаметно отключалась даже при
        # положительном error_rate и достаточном числе строк.
        result = gd.error_count(total_rows=10, error_rate=0.01, error_types=4)
        assert result == 1

    def test_never_exceeds_total_rows_divided_by_types(self):
        result = gd.error_count(total_rows=100, error_rate=1.0, error_types=4)
        assert result <= 100 // 4


# ---------------------------------------------------------------------
# validate_args
# ---------------------------------------------------------------------


class TestValidateArgs:
    def _args(self, **overrides):
        defaults = dict(
            clients_count=100,
            orders_count=1000,
            payments_count=1000,
            error_rate=0.01,
        )
        defaults.update(overrides)

        class Args:
            pass

        args = Args()
        for k, v in defaults.items():
            setattr(args, k, v)
        return args

    def test_valid_args_pass(self):
        gd.validate_args(self._args())  # should not raise

    def test_negative_count_rejected(self):
        with pytest.raises(ValueError):
            gd.validate_args(self._args(orders_count=-1))

    def test_error_rate_out_of_range_rejected(self):
        with pytest.raises(ValueError):
            gd.validate_args(self._args(error_rate=1.5))

    def test_orders_without_clients_rejected(self):
        with pytest.raises(ValueError):
            gd.validate_args(self._args(clients_count=0, orders_count=100))

    def test_zero_orders_and_zero_clients_is_fine(self):
        gd.validate_args(
            self._args(clients_count=0, orders_count=0, payments_count=0)
        )

    def test_payments_without_orders_rejected(self):
        # РЕГРЕССИЯ: найдено при прогоне test_raises_without_valid_orders —
        # без этой проверки --orders-count 0 --payments-count 100 падал бы
        # посреди пайплайна с сырым KeyError вместо понятной ошибки на
        # этапе валидации аргументов.
        with pytest.raises(ValueError):
            gd.validate_args(self._args(orders_count=0, payments_count=100))

    def test_zero_payments_and_zero_orders_is_fine(self):
        gd.validate_args(
            self._args(clients_count=0, orders_count=0, payments_count=0)
        )


# ---------------------------------------------------------------------
# generate_clients
# ---------------------------------------------------------------------


class TestGenerateClients:
    def test_zero_count_returns_empty_with_correct_columns(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df = gd.generate_clients(load_date=date(2026, 1, 1), count=0, rng=rng)
        assert df.empty
        assert list(df.columns) == [
            "client_id",
            "registration_date",
            "city",
            "acquisition_channel",
            "email",
        ]

    def test_generates_requested_row_count(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df = gd.generate_clients(load_date=date(2026, 1, 1), count=50, rng=rng)
        assert len(df) == 50

    def test_client_ids_are_unique(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df = gd.generate_clients(load_date=date(2026, 1, 1), count=200, rng=rng)
        assert df["client_id"].is_unique

    def test_cities_come_from_known_list(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df = gd.generate_clients(load_date=date(2026, 1, 1), count=200, rng=rng)
        assert set(df["city"].unique()) <= set(gd.CITIES)

    def test_same_seed_is_deterministic(self):
        df1 = gd.generate_clients(
            load_date=date(2026, 1, 1), count=30, rng=gd.make_rng(date(2026, 1, 1))
        )
        df2 = gd.generate_clients(
            load_date=date(2026, 1, 1), count=30, rng=gd.make_rng(date(2026, 1, 1))
        )
        pd.testing.assert_frame_equal(df1, df2)


# ---------------------------------------------------------------------
# generate_orders
# ---------------------------------------------------------------------


@pytest.fixture
def sample_clients() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "client_id": [f"cl_test_{i:03d}" for i in range(20)],
            "registration_date": [date(2026, 1, 1)] * 20,
            "city": ["Москва"] * 20,
            "acquisition_channel": ["organic"] * 20,
            "email": [f"user{i}@example.com" for i in range(20)],
        }
    )


class TestGenerateOrders:
    def test_raises_without_clients(self):
        rng = gd.make_rng(date(2026, 2, 1))
        with pytest.raises(ValueError):
            gd.generate_orders(
                load_date=date(2026, 2, 1),
                count=10,
                clients_df=pd.DataFrame(),
                rng=rng,
                error_rate=0.0,
            )

    def test_zero_count_returns_empty(self, sample_clients):
        rng = gd.make_rng(date(2026, 2, 1))
        df, dq = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=0,
            clients_df=sample_clients,
            rng=rng,
            error_rate=0.1,
        )
        assert df.empty
        assert dq == {}

    def test_no_errors_when_error_rate_zero(self, sample_clients):
        rng = gd.make_rng(date(2026, 2, 1))
        df, dq = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=500,
            clients_df=sample_clients,
            rng=rng,
            error_rate=0.0,
        )
        assert df["client_id"].notna().all()
        assert (df["amount_kopecks"] > 0).all()
        assert df["status"].isin(gd.VALID_ORDER_STATUSES).all()
        assert all(v == 0 for v in dq.values())

    def test_dq_counts_match_actual_injected_rows(self, sample_clients):
        rng = gd.make_rng(date(2026, 2, 1))
        df, dq = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=1000,
            clients_df=sample_clients,
            rng=rng,
            error_rate=0.02,
        )
        assert (df["client_id"].isna().sum()) == dq["orders.null_client_id"]
        assert (df["amount_kopecks"] < 0).sum() == dq["orders.negative_amount"]
        assert (
            ~df["status"].isin(gd.VALID_ORDER_STATUSES)
        ).sum() == dq["orders.invalid_status"]
        assert (
            df["order_id"].duplicated().sum()
        ) == dq["orders.duplicate_order_id_rows"]

    def test_order_dates_never_before_client_registration(self, sample_clients):
        clients = sample_clients.copy()
        clients.loc[0, "registration_date"] = date(2026, 1, 25)
        rng = gd.make_rng(date(2026, 2, 1))
        df, _ = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=500,
            clients_df=clients,
            rng=rng,
            error_rate=0.0,
        )
        merged = df.merge(clients, on="client_id", suffixes=("_order", "_client"))
        assert (
            pd.to_datetime(merged["order_date"])
            >= pd.to_datetime(merged["registration_date"])
        ).all()


# ---------------------------------------------------------------------
# draw_payment_status — чистая функция, поэтому тестируется точечно,
# без генерации целых датафреймов.
# ---------------------------------------------------------------------


class TestDrawPaymentStatus:
    def test_new_order_never_yields_success(self):
        for roll in np.linspace(0, 0.999, 50):
            status = gd.draw_payment_status(
                order_status="new",
                already_succeeded=False,
                primary_roll=roll,
                secondary_roll=0.5,
                chargeback_roll=1.0,
            )
            assert status in {"pending", "failed"}

    def test_cancelled_order_never_yields_success(self):
        for roll in np.linspace(0, 0.999, 50):
            status = gd.draw_payment_status(
                order_status="cancelled",
                already_succeeded=False,
                primary_roll=roll,
                secondary_roll=0.5,
                chargeback_roll=1.0,
            )
            assert status in {"refunded", "failed"}

    def test_returned_and_refunded_never_yield_pending(self):
        for order_status in ("returned", "refunded"):
            for roll in np.linspace(0, 0.999, 50):
                status = gd.draw_payment_status(
                    order_status=order_status,
                    already_succeeded=False,
                    primary_roll=roll,
                    secondary_roll=0.5,
                    chargeback_roll=1.0,
                )
                assert status != "pending"

    def test_delivered_order_can_succeed_once_but_not_twice(self):
        first = gd.draw_payment_status(
            order_status="delivered",
            already_succeeded=False,
            primary_roll=0.0,  # forces the success branch
            secondary_roll=0.5,
            chargeback_roll=1.0,  # forces "not a chargeback"
        )
        assert first == "success"

        second = gd.draw_payment_status(
            order_status="delivered",
            already_succeeded=True,  # caller already marked it as paid
            primary_roll=0.0,
            secondary_roll=0.5,
            chargeback_roll=1.0,
        )
        assert second != "success"

    def test_delivered_success_can_become_chargeback(self):
        status = gd.draw_payment_status(
            order_status="delivered",
            already_succeeded=False,
            primary_roll=0.0,
            secondary_roll=0.5,
            chargeback_roll=0.0,  # forces the chargeback branch
        )
        assert status == "chargeback"


# ---------------------------------------------------------------------
# generate_payments — сквозные инварианты согласованности
# ---------------------------------------------------------------------


@pytest.fixture
def sample_orders() -> pd.DataFrame:
    rng = gd.make_rng(date(2026, 3, 1))
    clients = pd.DataFrame(
        {
            "client_id": [f"cl_test_{i:03d}" for i in range(30)],
            "registration_date": [date(2026, 1, 1)] * 30,
            "city": ["Москва"] * 30,
            "acquisition_channel": ["organic"] * 30,
            "email": [f"user{i}@example.com" for i in range(30)],
        }
    )
    df, _ = gd.generate_orders(
        load_date=date(2026, 3, 1),
        count=800,
        clients_df=clients,
        rng=rng,
        error_rate=0.0,  # чистые заказы, без DQ-шума, для проверки инвариантов
    )
    return df


class TestGeneratePaymentsInvariants:
    def test_raises_without_valid_orders(self):
        rng = gd.make_rng(date(2026, 3, 1))
        with pytest.raises(ValueError):
            gd.generate_payments(
                load_date=date(2026, 3, 1),
                orders_df=pd.DataFrame(),
                count=10,
                rng=rng,
                error_rate=0.0,
            )

    def test_new_orders_never_have_a_success_payment(self, sample_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            orders_df=sample_orders,
            count=1000,
            rng=rng,
            error_rate=0.0,
        )
        merged = payments.merge(
            sample_orders[["order_id", "status"]].rename(
                columns={"status": "order_status"}
            ),
            on="order_id",
        )
        bad = merged[
            (merged["order_status"] == "new") & (merged["status"] == "success")
        ]
        assert bad.empty

    def test_at_most_one_success_payment_per_order(self, sample_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            orders_df=sample_orders,
            count=1000,
            rng=rng,
            error_rate=0.0,
        )
        success_counts = (
            payments[payments["status"] == "success"].groupby("order_id").size()
        )
        assert (success_counts <= 1).all()

    def test_returned_and_refunded_orders_have_no_pending_payment(
        self, sample_orders
    ):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            orders_df=sample_orders,
            count=1000,
            rng=rng,
            error_rate=0.0,
        )
        merged = payments.merge(
            sample_orders[["order_id", "status"]].rename(
                columns={"status": "order_status"}
            ),
            on="order_id",
        )
        bad = merged[
            merged["order_status"].isin(["returned", "refunded"])
            & (merged["status"] == "pending")
        ]
        assert bad.empty

    def test_payment_amount_matches_order_amount(self, sample_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            orders_df=sample_orders,
            count=500,
            rng=rng,
            error_rate=0.0,
        )
        merged = payments.merge(
            sample_orders[["order_id", "amount_kopecks"]],
            on="order_id",
            suffixes=("_payment", "_order"),
        )
        assert (
            merged["amount_kopecks_payment"] == merged["amount_kopecks_order"]
        ).all()


# ---------------------------------------------------------------------
# publish_batch — атомарность и откат при сбое
# ---------------------------------------------------------------------


class TestPublishBatchAtomicity:
    def _make_staging(self, staging_root: Path) -> None:
        for entity in ["clients", "orders", "payments"]:
            d = staging_root / entity
            d.mkdir(parents=True)
            (d / f"{entity}.csv").write_text("col\n1\n")

    def test_happy_path_publishes_all_entities_and_commit_marker(
        self, tmp_path: Path
    ):
        source_root = tmp_path / "source"
        staging_root = source_root / "_staging" / "batch-1"
        self._make_staging(staging_root)

        load_date = date(2026, 4, 1)
        gd.publish_batch(
            source_root, staging_root, load_date, {"batch_id": "batch-1"}
        )

        for entity in ["clients", "orders", "payments"]:
            assert (source_root / entity / "load_date=2026-04-01").exists()
        commit_file = (
            source_root / "_commits" / "load_date=2026-04-01" / "commit.json"
        )
        assert commit_file.exists()
        assert json.loads(commit_file.read_text())["batch_id"] == "batch-1"

    def test_failure_after_partial_move_rolls_back_cleanly(self, tmp_path: Path):
        # РЕГРЕССИЯ: раньше при падении на третьей сущности (payments)
        # clients/orders уже переехавшие в source_root оставались там
        # НАВСЕГДА без commit-маркера, и ensure_batch_does_not_exist
        # считала бы этот load_date "уже существующим" при повторном
        # запуске, хотя он фактически не опубликован.
        source_root = tmp_path / "source"
        staging_root = source_root / "_staging" / "batch-1"
        self._make_staging(staging_root)

        load_date = date(2026, 4, 1)

        # Саботаж: заранее создаём непустую целевую директорию payments,
        # чтобы Path.replace() на третьей сущности упал.
        blocker = source_root / "payments" / "load_date=2026-04-01"
        blocker.mkdir(parents=True)
        (blocker / "blocker.txt").write_text("occupied")

        with pytest.raises(OSError):
            gd.publish_batch(
                source_root, staging_root, load_date, {"batch_id": "batch-1"}
            )

        # Ни clients, ни orders не должны остаться в source_root...
        assert not (source_root / "clients" / "load_date=2026-04-01").exists()
        assert not (source_root / "orders" / "load_date=2026-04-01").exists()
        # ...и commit-маркера тоже быть не должно.
        assert not (source_root / "_commits" / "load_date=2026-04-01").exists()
        # ...а откаченные директории должны вернуться в staging.
        assert (staging_root / "clients").exists()
        assert (staging_root / "orders").exists()

    def test_ensure_batch_does_not_exist_rejects_duplicate_load_date(
        self, tmp_path: Path
    ):
        source_root = tmp_path / "source"
        (source_root / "clients" / "load_date=2026-04-01").mkdir(parents=True)

        with pytest.raises(FileExistsError):
            gd.ensure_batch_does_not_exist(source_root, date(2026, 4, 1))

    def test_ensure_batch_does_not_exist_allows_new_load_date(self, tmp_path: Path):
        source_root = tmp_path / "source"
        gd.ensure_batch_does_not_exist(source_root, date(2026, 4, 1))  # no raise


# ---------------------------------------------------------------------
# sha256_file / write_staged_csv
# ---------------------------------------------------------------------


class TestFileHelpers:
    def test_sha256_is_stable_for_same_content(self, tmp_path: Path):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("hello world")
        f2.write_text("hello world")
        assert gd.sha256_file(f1) == gd.sha256_file(f2)

    def test_sha256_differs_for_different_content(self, tmp_path: Path):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("hello world")
        f2.write_text("goodbye world")
        assert gd.sha256_file(f1) != gd.sha256_file(f2)

    def test_write_staged_csv_roundtrips(self, tmp_path: Path):
        df = pd.DataFrame({"a": [1, 2], "b": ["Москва", "Казань"]})
        target = gd.write_staged_csv(df, tmp_path, "widgets")
        assert target.exists()
        reloaded = pd.read_csv(target)
        pd.testing.assert_frame_equal(df, reloaded)
