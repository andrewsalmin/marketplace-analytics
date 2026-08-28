"""
Тесты покрывают в первую очередь то, что реально ломалось в процессе
разработки: скрытое обнуление DQ-инжекции на маленьких батчах,
неатомарную публикацию батча, и корректность event-driven state
machine заказа (дедлайны, причины отмены, защита от повторного success
на одном заказе за батч).

Запуск: pytest -v   (из корня репозитория)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path

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
        assert gd.error_count(total_rows=1000, error_rate=0.10, error_types=4) == 25

    def test_too_few_rows_for_any_error_type_returns_zero(self):
        assert gd.error_count(total_rows=2, error_rate=0.9, error_types=4) == 0

    def test_small_batch_still_gets_at_least_one_error_per_type(self):
        result = gd.error_count(total_rows=10, error_rate=0.01, error_types=4)
        assert result == 1

    def test_never_exceeds_total_rows_divided_by_types(self):
        result = gd.error_count(total_rows=100, error_rate=1.0, error_types=4)
        assert result <= 100 // 4


# ---------------------------------------------------------------------
# resolve_error_rate / _ramp_multiplier / _incident_multiplier /
# _build_incident_calendar
# ---------------------------------------------------------------------


class TestRampMultiplier:
    def test_launch_day_is_at_peak(self):
        assert gd._ramp_multiplier(0) == gd.RAMP_ERROR_INITIAL_MULTIPLIER

    def test_before_launch_clamped_to_peak(self):
        assert gd._ramp_multiplier(-5) == gd.RAMP_ERROR_INITIAL_MULTIPLIER

    def test_decays_toward_baseline_over_time(self):
        early = gd._ramp_multiplier(1)
        mid = gd._ramp_multiplier(gd.RAMP_ERROR_DECAY_DAYS)
        late = gd._ramp_multiplier(gd.RAMP_ERROR_DECAY_DAYS * 10)

        assert early > mid > late
        assert late == pytest.approx(1.0, abs=0.01)

    def test_custom_initial_multiplier_is_used_at_launch_day(self):
        assert gd._ramp_multiplier(0, initial_multiplier=3.0, decay_days=14) == 3.0

    def test_custom_decay_days_changes_the_curve(self):
        default = gd._ramp_multiplier(10)
        custom = gd._ramp_multiplier(10, initial_multiplier=3.0, decay_days=14)
        assert custom != default


class TestIncidentMultiplier:
    def test_no_calendar_gives_no_multiplier(self):
        assert gd._incident_multiplier({}, "payments", 10) == 1.0

    def test_day_inside_window_gets_multiplier(self):
        calendar = {"payments": [(5, 3, 4.5)]}
        assert gd._incident_multiplier(calendar, "payments", 6) == 4.5

    def test_day_outside_window_unaffected(self):
        calendar = {"payments": [(5, 3, 4.5)]}
        assert gd._incident_multiplier(calendar, "payments", 20) == 1.0

    def test_other_entity_unaffected(self):
        calendar = {"payments": [(5, 3, 4.5)]}
        assert gd._incident_multiplier(calendar, "orders", 6) == 1.0


class TestBuildIncidentCalendar:
    def test_covers_all_entities(self):
        calendar = gd._build_incident_calendar()
        assert set(calendar.keys()) == set(gd.DQ_ENTITIES)

    def test_windows_are_non_overlapping_and_in_range(self):
        calendar = gd._build_incident_calendar()

        for windows in calendar.values():
            prev_end = -1

            for start, duration, multiplier in windows:
                assert start >= prev_end
                assert 0 < duration
                assert (
                    gd.INCIDENT_MULTIPLIER_RANGE[0]
                    <= multiplier
                    <= gd.INCIDENT_MULTIPLIER_RANGE[1]
                )
                prev_end = start + duration

    def test_deterministic_across_calls(self):
        assert gd._build_incident_calendar() == gd._build_incident_calendar()


class TestResolveErrorRate:
    def test_without_launch_date_returns_base_rate_unchanged(self):
        rng = gd.make_rng(date(2026, 6, 1))
        rate = gd.resolve_error_rate(
            entity="orders",
            load_date=date(2026, 6, 1),
            base_rate=0.01,
            rng=rng,
            launch_date=None,
        )
        assert rate == 0.01

    def test_with_launch_date_on_launch_day_is_elevated(self):
        rng = gd.make_rng(date(2026, 6, 1))
        rate = gd.resolve_error_rate(
            entity="orders",
            load_date=date(2026, 6, 1),
            base_rate=0.01,
            rng=rng,
            launch_date=date(2026, 6, 1),
            incident_calendar={},
        )
        # ramp-множитель на день запуска = RAMP_ERROR_INITIAL_MULTIPLIER,
        # шум логнормальный вокруг 1.0 — берём широкий, но не бесконечный
        # диапазон, чтобы не ловить флаки на хвостах распределения.
        assert 0.01 < rate < 0.01 * gd.RAMP_ERROR_INITIAL_MULTIPLIER * 5

    def test_never_exceeds_one(self):
        rng = gd.make_rng(date(2026, 6, 1))
        rate = gd.resolve_error_rate(
            entity="orders",
            load_date=date(2026, 6, 1),
            base_rate=1.0,
            rng=rng,
            launch_date=date(2026, 6, 1),
            incident_calendar={"orders": [(0, 5, 6.0)]},
        )
        assert rate <= 1.0


class TestResolveCount:
    def test_without_launch_date_returns_base_count_unchanged(self):
        rng = gd.make_rng(date(2026, 6, 1))
        count = gd.resolve_count(base_count=200, rng=rng, launch_date=None)
        assert count == 200

    def test_zero_base_count_stays_zero(self):
        rng = gd.make_rng(date(2026, 6, 1))
        count = gd.resolve_count(
            base_count=0, rng=rng, launch_date=date(2026, 6, 1)
        )
        assert count == 0

    def test_with_launch_date_applies_noise_within_reasonable_range(self):
        rng = gd.make_rng(date(2026, 6, 1))
        count = gd.resolve_count(
            base_count=200, rng=rng, launch_date=date(2026, 6, 1)
        )
        # логнормальный шум вокруг 1.0 — берём широкий, но не бесконечный
        # диапазон, чтобы не ловить флаки на хвостах распределения.
        assert 1 <= count <= 200 * 5

    def test_never_goes_below_one(self):
        rng = gd.make_rng(date(2026, 6, 1))
        count = gd.resolve_count(
            base_count=1, rng=rng, launch_date=date(2026, 6, 1)
        )
        assert count >= 1


class TestResolvePaymentSuccessRate:
    def test_without_launch_date_returns_base_rate_unchanged(self):
        rng = gd.make_rng(date(2026, 6, 1))
        rate = gd.resolve_payment_success_rate(
            load_date=date(2026, 6, 1), rng=rng, launch_date=None
        )
        assert rate == gd.PAYMENT_SUCCESS_RATE

    def test_with_launch_date_on_launch_day_is_depressed(self):
        rng = gd.make_rng(date(2026, 6, 1))
        rate = gd.resolve_payment_success_rate(
            load_date=date(2026, 6, 1),
            rng=rng,
            launch_date=date(2026, 6, 1),
            incident_calendar={},
        )
        # На день запуска failure_rate поднят RAMP_PAYMENT_FAILURE_
        # INITIAL_MULTIPLIER-кратно — success rate должен быть заметно
        # ниже базового, но не отрицательным.
        assert 0.0 <= rate < gd.PAYMENT_SUCCESS_RATE

    def test_never_goes_below_zero(self):
        rng = gd.make_rng(date(2026, 6, 1))
        rate = gd.resolve_payment_success_rate(
            load_date=date(2026, 6, 1),
            rng=rng,
            launch_date=date(2026, 6, 1),
            incident_calendar={"payments": [(0, 5, 6.0)]},
        )
        assert rate >= 0.0

    def test_decays_toward_base_rate_over_time(self):
        launch_date = date(2026, 6, 1)
        early_rng = gd.make_rng(launch_date)
        late_rng = gd.make_rng(launch_date)

        early = gd.resolve_payment_success_rate(
            load_date=launch_date,
            rng=early_rng,
            launch_date=launch_date,
            incident_calendar={},
        )
        late = gd.resolve_payment_success_rate(
            load_date=launch_date
            + timedelta(days=gd.RAMP_PAYMENT_FAILURE_DECAY_DAYS * 10),
            rng=late_rng,
            launch_date=launch_date,
            incident_calendar={},
        )
        assert late > early


# ---------------------------------------------------------------------
# validate_args
# ---------------------------------------------------------------------


class TestValidateArgs:
    def _args(self, **overrides):
        defaults = dict(
            customers_count=100,
            orders_count=1000,
            payments_count=1000,
            error_rate=0.01,
            launch_date=None,
            payment_deadline_hours=24,
            shipping_deadline_hours=48,
            delivery_deadline_days=7,
            pickup_deadline_days=7,
            return_window_days=14,
            refund_processing_hours=72,
            cancel_before_payment_rate=0.02,
            cancel_after_payment_rate=0.01,
            return_rate=0.05,
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

    def test_valid_launch_date_passes(self):
        gd.validate_args(self._args(launch_date="2026-06-01"))  # should not raise

    def test_malformed_launch_date_rejected(self):
        with pytest.raises(ValueError):
            gd.validate_args(self._args(launch_date="not-a-date"))

    def test_orders_without_customers_rejected(self):
        with pytest.raises(ValueError):
            gd.validate_args(self._args(customers_count=0, orders_count=100))

    def test_zero_orders_and_zero_customers_is_fine(self):
        gd.validate_args(
            self._args(customers_count=0, orders_count=0, payments_count=0)
        )

    def test_payments_without_orders_now_allowed(self):
        # Раньше это было отклонено, но теперь payments могут ссылаться на
        # ОТКРЫТЫЕ заказы из истории, даже если orders-count=0 в этом
        # конкретном прогоне — проверка больше не актуальна.
        gd.validate_args(self._args(orders_count=0, payments_count=100))

    @pytest.mark.parametrize(
        "field",
        [
            "payment_deadline_hours",
            "shipping_deadline_hours",
            "delivery_deadline_days",
            "pickup_deadline_days",
            "return_window_days",
            "refund_processing_hours",
        ],
    )
    def test_non_positive_deadline_rejected(self, field):
        with pytest.raises(ValueError):
            gd.validate_args(self._args(**{field: 0}))

    @pytest.mark.parametrize(
        "field",
        ["cancel_before_payment_rate", "cancel_after_payment_rate", "return_rate"],
    )
    def test_rate_out_of_range_rejected(self, field):
        with pytest.raises(ValueError):
            gd.validate_args(self._args(**{field: 1.5}))


# ---------------------------------------------------------------------
# compute_lookback_days
# ---------------------------------------------------------------------


class TestComputeLookbackDays:
    def test_matches_sum_of_all_windows(self):
        class Args:
            pass

        args = Args()
        args.payment_deadline_hours = 24
        args.shipping_deadline_hours = 48
        args.delivery_deadline_days = 7
        args.pickup_deadline_days = 7
        args.return_window_days = 14
        args.refund_processing_hours = 72

        assert gd.compute_lookback_days(args) == 1 + 2 + 7 + 7 + 14 + 3

    def test_rounds_hours_up_to_whole_days(self):
        class Args:
            pass

        args = Args()
        args.payment_deadline_hours = 1  # ceil(1/24) = 1, не 0
        args.shipping_deadline_hours = 1
        args.delivery_deadline_days = 0
        args.pickup_deadline_days = 0
        args.return_window_days = 0
        args.refund_processing_hours = 1

        assert gd.compute_lookback_days(args) == 3


# ---------------------------------------------------------------------
# generate_customers
# ---------------------------------------------------------------------


class TestGenerateCustomers:
    def test_zero_count_returns_empty_with_correct_columns(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, dq = gd.generate_customers(load_date=date(2026, 1, 1), count=0, rng=rng)
        assert df.empty
        assert list(df.columns) == gd.CUSTOMER_COLUMNS
        assert dq == {}

    def test_generates_requested_row_count(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, _ = gd.generate_customers(load_date=date(2026, 1, 1), count=50, rng=rng)
        assert len(df) == 50

    def test_customer_ids_are_unique(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, _ = gd.generate_customers(load_date=date(2026, 1, 1), count=200, rng=rng)
        assert df["customer_id"].is_unique

    def test_cities_come_from_known_list(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, _ = gd.generate_customers(load_date=date(2026, 1, 1), count=200, rng=rng)
        assert set(df["city"].unique()) <= set(gd.CITIES)

    def test_same_seed_is_deterministic(self):
        df1, _ = gd.generate_customers(
            load_date=date(2026, 1, 1), count=30, rng=gd.make_rng(date(2026, 1, 1))
        )
        df2, _ = gd.generate_customers(
            load_date=date(2026, 1, 1), count=30, rng=gd.make_rng(date(2026, 1, 1))
        )
        pd.testing.assert_frame_equal(df1, df2)

    def test_registration_date_has_realistic_time_component(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, _ = gd.generate_customers(load_date=date(2026, 1, 1), count=300, rng=rng)
        times = pd.to_datetime(df["registration_date"]).dt.time
        midnight = pd.Timestamp("2026-01-01 00:00:00").time()
        # хотя бы часть клиентов должна иметь ненулевое время суток —
        # иначе регистрация не переехала на DateTime по-настоящему
        assert (times != midnight).any()

    def test_dq_counts_match_actual_injected_rows(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, dq = gd.generate_customers(
            load_date=date(2026, 1, 1), count=1000, rng=rng, error_rate=0.02
        )
        assert df["customer_id"].isna().sum() == dq["customers.null_customer_id"]
        assert (
            df["registration_date"].isna().sum()
            == dq["customers.invalid_registration_date"]
        )

    def test_no_errors_when_error_rate_zero(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, dq = gd.generate_customers(
            load_date=date(2026, 1, 1), count=500, rng=rng, error_rate=0.0
        )
        assert df["customer_id"].notna().all()
        assert all(v == 0 for v in dq.values())

    def test_duplicate_in_history_reuses_existing_id(self):
        rng = gd.make_rng(date(2026, 1, 1))
        existing_ids = [f"cl_existing_{i:03d}" for i in range(10)]
        df, dq = gd.generate_customers(
            load_date=date(2026, 1, 1),
            count=1000,
            rng=rng,
            error_rate=0.02,
            existing_customer_ids=existing_ids,
        )
        assert dq["customers.duplicate_customer_id_in_history"] > 0
        assert set(df["customer_id"]) & set(existing_ids)

    def test_no_duplicate_in_history_type_without_existing_ids(self):
        rng = gd.make_rng(date(2026, 1, 1))
        df, dq = gd.generate_customers(
            load_date=date(2026, 1, 1),
            count=1000,
            rng=rng,
            error_rate=0.02,
            existing_customer_ids=None,
        )
        assert dq["customers.duplicate_customer_id_in_history"] == 0


# ---------------------------------------------------------------------
# generate_orders — теперь всегда создаёт заказы в статусе "new";
# продвижение по воронке — забота advance_open_orders.
# ---------------------------------------------------------------------


@pytest.fixture
def sample_customers() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": [f"cl_test_{i:03d}" for i in range(20)],
            "registration_date": [datetime(2026, 1, 1)] * 20,
            "city": ["Москва"] * 20,
            "acquisition_channel": ["organic"] * 20,
            "email": [f"user{i}@example.com" for i in range(20)],
        }
    )


class TestGenerateOrders:
    def test_raises_without_customers(self):
        rng = gd.make_rng(date(2026, 2, 1))
        with pytest.raises(ValueError):
            gd.generate_orders(
                load_date=date(2026, 2, 1),
                count=10,
                customers_df=pd.DataFrame(),
                rng=rng,
                error_rate=0.0,
            )

    def test_zero_count_returns_empty(self, sample_customers):
        rng = gd.make_rng(date(2026, 2, 1))
        df, dq = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=0,
            customers_df=sample_customers,
            rng=rng,
            error_rate=0.1,
        )
        assert df.empty
        assert dq == {}

    def test_new_orders_start_clean_in_new_status(self, sample_customers):
        rng = gd.make_rng(date(2026, 2, 1))
        df, _ = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=500,
            customers_df=sample_customers,
            rng=rng,
            error_rate=0.0,
        )
        assert (df["status"] == "new").all()
        assert df["paid_at"].isna().all()
        assert df["cancellation_reason"].isna().all()

    def test_created_at_matches_load_date(self, sample_customers):
        rng = gd.make_rng(date(2026, 2, 1))
        df, _ = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=500,
            customers_df=sample_customers,
            rng=rng,
            error_rate=0.0,
        )
        created_dates = pd.to_datetime(df["created_at"]).dt.date
        assert (created_dates == date(2026, 2, 1)).all()

    def test_no_errors_when_error_rate_zero(self, sample_customers):
        rng = gd.make_rng(date(2026, 2, 1))
        df, dq = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=500,
            customers_df=sample_customers,
            rng=rng,
            error_rate=0.0,
        )
        assert df["customer_id"].notna().all()
        assert (df["amount_kopecks"] > 0).all()
        assert all(v == 0 for v in dq.values())

    def test_dq_counts_match_actual_injected_rows(self, sample_customers):
        rng = gd.make_rng(date(2026, 2, 1))
        df, dq = gd.generate_orders(
            load_date=date(2026, 2, 1),
            count=1000,
            customers_df=sample_customers,
            rng=rng,
            error_rate=0.02,
        )
        assert (df["customer_id"].isna().sum()) == dq["orders.null_customer_id"]
        assert (df["amount_kopecks"] < 0).sum() == dq["orders.negative_amount"]
        assert (df["status"] == "unknown_status").sum() == dq["orders.invalid_status"]
        assert (
            df["order_id"].duplicated().sum()
        ) == dq["orders.duplicate_order_id_rows"]


# ---------------------------------------------------------------------
# generate_payments — сэмплирование попыток оплаты для "new"-заказов
# ---------------------------------------------------------------------


@pytest.fixture
def sample_new_orders() -> pd.DataFrame:
    rng = gd.make_rng(date(2026, 3, 1))
    customers = pd.DataFrame(
        {
            "customer_id": [f"cl_test_{i:03d}" for i in range(30)],
            "registration_date": [datetime(2026, 1, 1)] * 30,
            "city": ["Москва"] * 30,
            "acquisition_channel": ["organic"] * 30,
            "email": [f"user{i}@example.com" for i in range(30)],
        }
    )
    df, _ = gd.generate_orders(
        load_date=date(2026, 3, 1),
        count=200,
        customers_df=customers,
        rng=rng,
        error_rate=0.0,
    )
    return df


NOW = datetime(2026, 3, 1, 23, 59, 59)


class TestGeneratePayments:
    def test_empty_pool_returns_empty(self):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, dq, paid = gd.generate_payments(
            load_date=date(2026, 3, 1),
            new_status_orders_df=pd.DataFrame(),
            count=10,
            rng=rng,
            error_rate=0.0,
            now=NOW,
        )
        assert payments.empty
        assert paid.empty
        assert dq == {}

    def test_successful_payments_move_orders_to_paid(self, sample_new_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _, paid_orders = gd.generate_payments(
            load_date=date(2026, 3, 1),
            new_status_orders_df=sample_new_orders,
            count=1000,
            rng=rng,
            error_rate=0.0,
            now=NOW,
        )
        assert (paid_orders["status"] == "paid").all()
        assert paid_orders["paid_at"].notna().all()
        assert paid_orders["payment_method"].notna().all()

        success_ids = set(payments[payments["status"] == "success"]["order_id"])
        assert success_ids == set(paid_orders["order_id"])

    def test_at_most_one_success_payment_per_order(self, sample_new_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        # count намного больше числа заказов — форсирует множественные
        # попытки на одни и те же order_id через сэмплирование с
        # возвращением.
        payments, _, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            new_status_orders_df=sample_new_orders,
            count=3000,
            rng=rng,
            error_rate=0.0,
            now=NOW,
        )
        success_counts = (
            payments[payments["status"] == "success"].groupby("order_id").size()
        )
        assert (success_counts <= 1).all()

    def test_payment_amount_matches_order_amount(self, sample_new_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            new_status_orders_df=sample_new_orders,
            count=150,
            rng=rng,
            error_rate=0.0,
            now=NOW,
        )
        merged = payments.merge(
            sample_new_orders[["order_id", "amount_kopecks"]],
            on="order_id",
            suffixes=("_payment", "_order"),
        )
        assert (
            merged["amount_kopecks_payment"] == merged["amount_kopecks_order"]
        ).all()

    def test_payment_date_within_bounds(self, sample_new_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            new_status_orders_df=sample_new_orders,
            count=150,
            rng=rng,
            error_rate=0.0,
            now=NOW,
        )
        merged = payments.merge(
            sample_new_orders[["order_id", "created_at"]], on="order_id"
        )
        payment_dates = pd.to_datetime(merged["payment_date"])
        created_ats = pd.to_datetime(merged["created_at"])
        assert (payment_dates >= created_ats).all()
        assert (payment_dates <= NOW).all()

    def test_dq_counts_match_actual_injected_rows(self, sample_new_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, dq, _ = gd.generate_payments(
            load_date=date(2026, 3, 1),
            new_status_orders_df=sample_new_orders,
            count=1000,
            rng=rng,
            error_rate=0.02,
            now=NOW,
        )
        assert (
            payments["order_id"].str.startswith("ord_missing_").sum()
            == dq["payments.missing_order_id"]
        )
        assert (payments["amount_kopecks"] < 0).sum() == dq["payments.negative_amount"]
        assert (
            payments["status"] == "invalid_payment_status"
        ).sum() == dq["payments.invalid_status"]
        assert (
            payments["payment_id"].duplicated().sum()
        ) == dq["payments.duplicate_payment_id_rows"]

    def test_custom_success_rate_is_honored(self, sample_new_orders):
        rng = gd.make_rng(date(2026, 3, 1))
        payments, _, paid_orders = gd.generate_payments(
            load_date=date(2026, 3, 1),
            new_status_orders_df=sample_new_orders,
            count=len(sample_new_orders),
            rng=rng,
            error_rate=0.0,
            now=NOW,
            success_rate=0.0,
        )
        # success_rate=0.0 — ни одна попытка не должна пройти успешно.
        assert (payments["status"] == "success").sum() == 0
        assert paid_orders.empty


# ---------------------------------------------------------------------
# advance_open_orders — сердце state machine
# ---------------------------------------------------------------------


def _base_order(**overrides) -> dict:
    row = {
        "order_id": "ord_test_00000001",
        "customer_id": "cl_test_001",
        "created_at": datetime(2026, 5, 1, 10, 0, 0),
        "amount_kopecks": 5000,
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
    row.update(overrides)
    return row


def _advance_kwargs(**overrides) -> dict:
    defaults = dict(
        payment_deadline_hours=24,
        shipping_deadline_hours=48,
        delivery_deadline_days=7,
        pickup_deadline_days=7,
        return_window_days=14,
        refund_processing_hours=72,
        cancel_before_payment_rate=0.0,
        cancel_after_payment_rate=0.0,
        return_rate=0.0,
        error_rate=0.0,
    )
    defaults.update(overrides)
    return defaults


class TestAdvanceOpenOrders:
    def test_empty_pool_returns_empty(self):
        rng = gd.make_rng(date(2026, 5, 2))
        updated, refunds, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 2),
            candidate_orders_df=pd.DataFrame(),
            rng=rng,
            **_advance_kwargs(),
        )
        assert updated.empty
        assert refunds.empty

    def test_new_order_auto_cancelled_after_payment_deadline(self):
        order = _base_order(created_at=datetime(2026, 5, 1, 10, 0, 0))
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 3))  # дедлайн 24ч давно истёк

        updated, _, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 3),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(payment_deadline_hours=24),
        )

        assert len(updated) == 1
        assert updated.iloc[0]["status"] == "cancelled"
        assert updated.iloc[0]["cancellation_reason"] == "not_paid_in_time"

    def test_new_order_within_deadline_and_zero_cancel_rate_untouched(self):
        order = _base_order(created_at=datetime(2026, 5, 1, 23, 0, 0))
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 1))

        updated, _, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 1),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(
                payment_deadline_hours=24, cancel_before_payment_rate=0.0
            ),
        )

        assert updated.empty

    def test_shipped_reaches_ready_for_pickup_within_deadline(self):
        order = _base_order(
            status="shipped",
            paid_at=datetime(2026, 5, 1, 11, 0, 0),
            shipped_at=datetime(2026, 5, 1, 15, 0, 0),
        )
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 10))  # далеко за дедлайном -> форс

        updated, _, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 10),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(delivery_deadline_days=7),
        )

        assert len(updated) == 1
        assert updated.iloc[0]["status"] == "ready_for_pickup"
        assert updated.iloc[0]["ready_for_pickup_at"] < (
            order["shipped_at"] + timedelta(days=7)
        )

    def test_ready_for_pickup_auto_cancelled_after_pickup_deadline(self):
        order = _base_order(
            status="ready_for_pickup",
            paid_at=datetime(2026, 5, 1, 11, 0, 0),
            shipped_at=datetime(2026, 5, 1, 15, 0, 0),
            ready_for_pickup_at=datetime(2026, 5, 2, 0, 0, 0),
        )
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 12))  # далеко за дедлайном на забор

        updated, _, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 12),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(pickup_deadline_days=7),
        )

        assert len(updated) == 1
        assert updated.iloc[0]["status"] == "cancelled"
        assert updated.iloc[0]["cancellation_reason"] == "not_picked_up_in_time"

    def test_cancelled_with_payment_eventually_refunded(self):
        order = _base_order(
            status="cancelled",
            cancellation_reason="cancelled_by_customer_after_payment",
            paid_at=datetime(2026, 5, 1, 11, 0, 0),
            cancelled_at=datetime(2026, 5, 1, 12, 0, 0),
            payment_method="card",
        )
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 10))  # далеко за окном рефанда -> форс

        updated, refund_payments, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 10),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(refund_processing_hours=72),
        )

        assert len(updated) == 1
        assert updated.iloc[0]["status"] == "refunded"
        assert updated.iloc[0]["cancellation_reason"] == (
            "cancelled_by_customer_after_payment"
        )
        assert len(refund_payments) == 1
        assert refund_payments.iloc[0]["payment_id"] == "pay_ord_test_00000001"
        assert refund_payments.iloc[0]["status"] == "refunded"

    def test_cancelled_without_payment_stays_terminal(self):
        order = _base_order(
            status="cancelled",
            cancellation_reason="not_paid_in_time",
            cancelled_at=datetime(2026, 5, 1, 12, 0, 0),
            paid_at=pd.NaT,
        )
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 10))

        updated, refund_payments, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 10),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(),
        )

        assert updated.empty
        assert refund_payments.empty

    def test_delivered_terminal_when_return_rate_zero(self):
        order = _base_order(
            status="delivered",
            created_at=datetime(2026, 4, 1, 10, 0, 0),
            paid_at=datetime(2026, 4, 1, 11, 0, 0),
            shipped_at=datetime(2026, 4, 1, 15, 0, 0),
            ready_for_pickup_at=datetime(2026, 4, 2, 0, 0, 0),
            delivered_at=datetime(2026, 4, 3, 0, 0, 0),
        )
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 1))

        updated, _, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 1),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(return_rate=0.0, return_window_days=14),
        )

        assert updated.empty

    def test_cancellation_reason_null_outside_cancelled(self):
        order = _base_order(status="paid", paid_at=datetime(2026, 5, 1, 11, 0, 0))
        df = pd.DataFrame([order])
        rng = gd.make_rng(date(2026, 5, 1))

        updated, _, _ = gd.advance_open_orders(
            load_date=date(2026, 5, 1),
            candidate_orders_df=df,
            rng=rng,
            **_advance_kwargs(),
        )

        for row in updated.to_dict("records"):
            if row["status"] not in ("cancelled", "refunded"):
                assert pd.isna(row["cancellation_reason"])


# ---------------------------------------------------------------------
# load_existing_orders
# ---------------------------------------------------------------------


ORDER_CSV_TEMPLATE = {
    "order_id": "ord_x",
    "customer_id": "cl_1",
    "created_at": "2026-04-01 10:00:00",
    "amount_kopecks": 1000,
    "status": "new",
    "cancellation_reason": "",
    "payment_method": "",
    "paid_at": "",
    "shipped_at": "",
    "ready_for_pickup_at": "",
    "delivered_at": "",
    "cancelled_at": "",
    "returned_at": "",
    "refunded_at": "",
}


def _write_orders_csv(source_root: Path, load_date_str: str, rows: list[dict]) -> None:
    target_dir = source_root / "orders" / f"load_date={load_date_str}"
    target_dir.mkdir(parents=True)
    pd.DataFrame(rows).to_csv(target_dir / "orders.csv", index=False)


class TestLoadExistingOrders:
    def test_no_orders_root_returns_empty(self, tmp_path):
        result = gd.load_existing_orders(tmp_path, date(2026, 5, 1), lookback_days=10)
        assert result.empty

    def test_reads_latest_version_per_order_id(self, tmp_path):
        row_v1 = dict(ORDER_CSV_TEMPLATE)
        _write_orders_csv(tmp_path, "2026-05-01", [row_v1])

        row_v2 = dict(ORDER_CSV_TEMPLATE)
        row_v2["status"] = "paid"
        row_v2["paid_at"] = "2026-05-02 09:00:00"
        _write_orders_csv(tmp_path, "2026-05-02", [row_v2])

        result = gd.load_existing_orders(tmp_path, date(2026, 5, 2), lookback_days=10)

        assert len(result) == 1
        assert result.iloc[0]["status"] == "paid"

    def test_delivered_without_pending_refund_still_open(self, tmp_path):
        row = dict(ORDER_CSV_TEMPLATE)
        row.update(
            status="delivered",
            payment_method="card",
            paid_at="2026-04-01 11:00:00",
            shipped_at="2026-04-01 15:00:00",
            ready_for_pickup_at="2026-04-02 00:00:00",
            delivered_at="2026-04-03 00:00:00",
        )
        _write_orders_csv(tmp_path, "2026-04-01", [row])

        result = gd.load_existing_orders(tmp_path, date(2026, 5, 1), lookback_days=40)

        assert (result["status"] == "delivered").any()

    def test_refunded_orders_excluded(self, tmp_path):
        row = dict(ORDER_CSV_TEMPLATE)
        row.update(
            status="refunded",
            cancellation_reason="cancelled_by_customer_after_payment",
            payment_method="card",
            paid_at="2026-04-01 11:00:00",
            cancelled_at="2026-04-01 12:00:00",
            refunded_at="2026-04-02 12:00:00",
        )
        _write_orders_csv(tmp_path, "2026-04-01", [row])

        result = gd.load_existing_orders(tmp_path, date(2026, 5, 1), lookback_days=40)

        assert result.empty

    def test_cancelled_without_payment_excluded(self, tmp_path):
        row = dict(ORDER_CSV_TEMPLATE)
        row.update(
            status="cancelled",
            cancellation_reason="not_paid_in_time",
            cancelled_at="2026-04-02 10:00:00",
        )
        _write_orders_csv(tmp_path, "2026-04-01", [row])

        result = gd.load_existing_orders(tmp_path, date(2026, 5, 1), lookback_days=40)

        assert result.empty

    def test_cancelled_with_pending_refund_still_open(self, tmp_path):
        row = dict(ORDER_CSV_TEMPLATE)
        row.update(
            status="cancelled",
            cancellation_reason="cancelled_by_customer_after_payment",
            payment_method="card",
            paid_at="2026-04-01 11:00:00",
            cancelled_at="2026-04-01 12:00:00",
        )
        _write_orders_csv(tmp_path, "2026-04-01", [row])

        result = gd.load_existing_orders(tmp_path, date(2026, 5, 1), lookback_days=40)

        assert len(result) == 1

    def test_files_outside_lookback_window_ignored(self, tmp_path):
        row = dict(ORDER_CSV_TEMPLATE)
        _write_orders_csv(tmp_path, "2026-01-01", [row])

        result = gd.load_existing_orders(tmp_path, date(2026, 5, 1), lookback_days=10)

        assert result.empty


# ---------------------------------------------------------------------
# publish_batch — атомарность и откат при сбое
# ---------------------------------------------------------------------


class TestPublishBatchAtomicity:
    def _make_staging(self, staging_root: Path) -> None:
        for entity in ["customers", "orders", "payments"]:
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

        for entity in ["customers", "orders", "payments"]:
            assert (source_root / entity / "load_date=2026-04-01").exists()
        commit_file = (
            source_root / "_commits" / "load_date=2026-04-01" / "commit.json"
        )
        assert commit_file.exists()
        assert json.loads(commit_file.read_text())["batch_id"] == "batch-1"

    def test_failure_after_partial_move_rolls_back_cleanly(self, tmp_path: Path):
        source_root = tmp_path / "source"
        staging_root = source_root / "_staging" / "batch-1"
        self._make_staging(staging_root)

        load_date = date(2026, 4, 1)

        blocker = source_root / "payments" / "load_date=2026-04-01"
        blocker.mkdir(parents=True)
        (blocker / "blocker.txt").write_text("occupied")

        with pytest.raises(OSError):
            gd.publish_batch(
                source_root, staging_root, load_date, {"batch_id": "batch-1"}
            )

        assert not (source_root / "customers" / "load_date=2026-04-01").exists()
        assert not (source_root / "orders" / "load_date=2026-04-01").exists()
        assert not (source_root / "_commits" / "load_date=2026-04-01").exists()
        assert (staging_root / "customers").exists()
        assert (staging_root / "orders").exists()

    def test_ensure_batch_does_not_exist_rejects_duplicate_load_date(
        self, tmp_path: Path
    ):
        source_root = tmp_path / "source"
        (source_root / "customers" / "load_date=2026-04-01").mkdir(parents=True)

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
