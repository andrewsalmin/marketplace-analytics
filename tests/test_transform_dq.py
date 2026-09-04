"""Тесты DQ-правил transform.py.

Правила проверяются в изоляции от файлов и партиций: каждый тест берёт
заведомо валидную строку, меняет ровно одно поле и смотрит, какие
причины окажутся в dq_reason. Служебные колонки (duplicate_count,
customer_exists, order_exists, related_order_created_at) в этом слое
считаются уже посчитанными — их готовят transform_*, и это отдельная
зона ответственности.

Требуют pyspark (requirements-spark.txt); без него файл пропускается,
чтобы прогон тестов генератора не тянул тяжёлую зависимость.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="нужен requirements-spark.txt")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import transform

NOW = datetime(2026, 6, 1, 12, 0, 0)
EARLIER = datetime(2026, 5, 30, 12, 0, 0)


@pytest.fixture(scope="session")
def spark():
    # Без явного PYSPARK_PYTHON Spark запускает воркеры через "python3"
    # из PATH — в venv на Windows такого имени нет, и задача падает с
    # "Python worker failed to connect back". setdefault, чтобы среда
    # исполнения (CI, кластер) могла задать своё значение.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    session = (
        SparkSession.builder
        .appName("transform-dq-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        # Драйвер слушает петлю: на машине с несколькими интерфейсами
        # (VPN, WSL, docker0) Spark иначе выбирает не тот адрес, и
        # воркер не достукивается обратно.
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ---------------------------------------------------------------------
# Схемы и эталонно валидные строки
# ---------------------------------------------------------------------

CUSTOMER_SCHEMA = T.StructType([
    T.StructField("customer_id", T.StringType()),
    T.StructField("registration_date", T.TimestampType()),
    T.StructField("duplicate_count", T.LongType()),
    T.StructField("exists_in_history", T.BooleanType()),
])

VALID_CUSTOMER = {
    "customer_id": "cust_1",
    "registration_date": NOW,
    "duplicate_count": 1,
    "exists_in_history": None,
}

ORDER_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("amount_kopecks", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("cancellation_reason", T.StringType()),
    T.StructField("cancelled_at", T.TimestampType()),
    T.StructField("returned_at", T.TimestampType()),
    T.StructField("refunded_at", T.TimestampType()),
    T.StructField("duplicate_count", T.LongType()),
    T.StructField("customer_exists", T.BooleanType()),
])

VALID_ORDER = {
    "order_id": "ord_1",
    "customer_id": "cust_1",
    "created_at": NOW,
    "amount_kopecks": 150_000,
    "status": "delivered",
    "cancellation_reason": None,
    "cancelled_at": None,
    "returned_at": None,
    "refunded_at": None,
    "duplicate_count": 1,
    "customer_exists": True,
}

PAYMENT_SCHEMA = T.StructType([
    T.StructField("payment_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("payment_date", T.TimestampType()),
    T.StructField("amount_kopecks", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("duplicate_count", T.LongType()),
    T.StructField("order_exists", T.BooleanType()),
    T.StructField("related_order_created_at", T.TimestampType()),
])

VALID_PAYMENT = {
    "payment_id": "pay_ord_1",
    "order_id": "ord_1",
    "payment_date": NOW,
    "amount_kopecks": 150_000,
    "status": "success",
    "duplicate_count": 1,
    "order_exists": True,
    "related_order_created_at": EARLIER,
}


def one_row(spark, schema, row):
    """Однострочный DataFrame из типизированных литералов.

    Намеренно не через createDataFrame: тот сериализует питоновские
    объекты и поднимает Python-воркер на каждую задачу — на полсотни
    тестов это минуты и лишняя точка отказа. Литералы считаются целиком
    в JVM, и весь файл укладывается в секунды.
    """
    df = spark.range(1)

    for field in schema.fields:
        df = df.withColumn(
            field.name,
            F.lit(row[field.name]).cast(field.dataType),
        )

    return df.drop("id")


def reasons(spark, schema, baseline, checks, **overrides) -> list[str]:
    """dq_reason для одной строки, разложенный в список причин."""
    unknown = set(overrides) - {field.name for field in schema.fields}
    assert not unknown, f"в схеме нет колонок: {sorted(unknown)}"

    df = one_row(spark, schema, {**baseline, **overrides})
    dq_reason = transform.add_dq_reason(df, checks).collect()[0]["dq_reason"]

    return [part for part in dq_reason.split(" | ") if part]


def customer_reasons(spark, **overrides) -> list[str]:
    return reasons(
        spark,
        CUSTOMER_SCHEMA,
        VALID_CUSTOMER,
        transform.customer_dq_checks(),
        **overrides,
    )


def order_reasons(spark, **overrides) -> list[str]:
    return reasons(
        spark,
        ORDER_SCHEMA,
        VALID_ORDER,
        transform.order_dq_checks(),
        **overrides,
    )


def payment_reasons(spark, **overrides) -> list[str]:
    return reasons(
        spark,
        PAYMENT_SCHEMA,
        VALID_PAYMENT,
        transform.payment_dq_checks(),
        **overrides,
    )


# ---------------------------------------------------------------------
# customers
# ---------------------------------------------------------------------

def test_valid_customer_has_no_reasons(spark):
    assert customer_reasons(spark) == []


@pytest.mark.parametrize("customer_id", [None, "", "   "])
def test_blank_customer_id(spark, customer_id):
    assert customer_reasons(spark, customer_id=customer_id) == [
        "CUSTOMER_ID_EMPTY"
    ]


def test_customer_duplicate_within_load(spark):
    assert customer_reasons(spark, duplicate_count=2) == [
        "CUSTOMER_ID_DUPLICATE_IN_LOAD"
    ]


def test_customer_duplicate_in_history(spark):
    assert customer_reasons(spark, exists_in_history=True) == [
        "CUSTOMER_ID_DUPLICATE_IN_HISTORY"
    ]


def test_customer_without_registration_date(spark):
    assert customer_reasons(spark, registration_date=None) == [
        "REGISTRATION_DATE_INVALID"
    ]


def test_customer_reasons_accumulate(spark):
    assert customer_reasons(
        spark,
        customer_id=None,
        registration_date=None,
    ) == ["CUSTOMER_ID_EMPTY", "REGISTRATION_DATE_INVALID"]


# ---------------------------------------------------------------------
# orders
# ---------------------------------------------------------------------

def test_valid_order_has_no_reasons(spark):
    assert order_reasons(spark) == []


@pytest.mark.parametrize("order_id", [None, "", "  "])
def test_blank_order_id(spark, order_id):
    assert order_reasons(spark, order_id=order_id) == ["ORDER_ID_EMPTY"]


def test_order_duplicate_within_load(spark):
    assert order_reasons(spark, duplicate_count=3) == [
        "ORDER_ID_DUPLICATE_IN_LOAD"
    ]


def test_order_without_known_customer(spark):
    assert order_reasons(spark, customer_exists=None) == ["CUSTOMER_NOT_FOUND"]


def test_order_without_created_at(spark):
    assert order_reasons(spark, created_at=None) == ["CREATED_AT_INVALID"]


@pytest.mark.parametrize("amount", [None, 0, -1])
def test_order_amount_must_be_positive(spark, amount):
    assert order_reasons(spark, amount_kopecks=amount) == [
        "ORDER_AMOUNT_INVALID"
    ]


def test_order_status_outside_vocabulary(spark):
    # Ровно то, что подсовывает инжектор DQ в generate_data.py.
    assert order_reasons(spark, status="unknown_status") == [
        "ORDER_STATUS_INVALID"
    ]


def test_order_status_is_case_insensitive(spark):
    assert order_reasons(spark, status="Delivered") == []


def test_cancellation_reason_without_cancelled_status(spark):
    assert order_reasons(spark, cancellation_reason="not_paid_in_time") == [
        "CANCELLATION_REASON_INCONSISTENT"
    ]


def test_cancelled_status_without_cancellation_reason(spark):
    assert order_reasons(
        spark,
        status="cancelled",
        cancelled_at=NOW,
        cancellation_reason=None,
    ) == ["CANCELLATION_REASON_INCONSISTENT"]


def test_cancelled_order_with_reason_is_valid(spark):
    assert order_reasons(
        spark,
        status="cancelled",
        cancelled_at=NOW,
        cancellation_reason="not_paid_in_time",
    ) == []


def test_refunded_order_keeps_reason_without_complaint(spark):
    assert order_reasons(
        spark,
        status="refunded",
        cancelled_at=EARLIER,
        refunded_at=NOW,
        cancellation_reason="cancelled_by_customer_after_payment",
    ) == []


def test_cancellation_rule_matches_status_case_insensitively(spark):
    """Регрессия: правило причины отмены сравнивало сырой status.

    Из-за этого источник, приславший "Cancelled", проходил проверку
    словаря статусов (она приводит к нижнему регистру), но получал
    CANCELLATION_REASON_INCONSISTENT — причина отмены объявлялась лишней
    у корректно отменённого заказа.
    """
    assert order_reasons(
        spark,
        status="Cancelled",
        cancelled_at=NOW,
        cancellation_reason="not_paid_in_time",
    ) == []


def test_refund_before_cancellation(spark):
    assert order_reasons(
        spark,
        status="refunded",
        cancellation_reason="cancelled_by_customer_after_payment",
        cancelled_at=NOW,
        refunded_at=EARLIER,
    ) == ["REFUND_TIMELINE_INVALID"]


def test_refund_before_return(spark):
    assert order_reasons(
        spark,
        status="refunded",
        returned_at=NOW,
        refunded_at=EARLIER,
    ) == ["REFUND_TIMELINE_INVALID"]


def test_refund_after_return_is_valid(spark):
    assert order_reasons(
        spark,
        status="refunded",
        returned_at=EARLIER,
        refunded_at=NOW,
    ) == []


def test_order_reasons_accumulate_in_declaration_order(spark):
    assert order_reasons(
        spark,
        order_id=None,
        customer_id=None,
        customer_exists=None,
        amount_kopecks=-5,
    ) == [
        "ORDER_ID_EMPTY",
        "CUSTOMER_ID_EMPTY",
        "CUSTOMER_NOT_FOUND",
        "ORDER_AMOUNT_INVALID",
    ]


# ---------------------------------------------------------------------
# payments
# ---------------------------------------------------------------------

def test_valid_payment_has_no_reasons(spark):
    assert payment_reasons(spark) == []


@pytest.mark.parametrize("payment_id", [None, "", " "])
def test_blank_payment_id(spark, payment_id):
    assert payment_reasons(spark, payment_id=payment_id) == [
        "PAYMENT_ID_EMPTY"
    ]


def test_payment_duplicate_within_load(spark):
    assert payment_reasons(spark, duplicate_count=2) == [
        "PAYMENT_ID_DUPLICATE_IN_LOAD"
    ]


def test_payment_without_known_order(spark):
    # order_exists=None гасит и PAYMENT_BEFORE_ORDER: сравнивать не с чем.
    assert payment_reasons(
        spark,
        order_exists=None,
        related_order_created_at=None,
    ) == ["ORDER_NOT_FOUND"]


def test_payment_without_date(spark):
    assert payment_reasons(spark, payment_date=None) == [
        "PAYMENT_DATE_INVALID"
    ]


@pytest.mark.parametrize("amount", [None, 0, -100])
def test_payment_amount_must_be_positive(spark, amount):
    assert payment_reasons(spark, amount_kopecks=amount) == [
        "PAYMENT_AMOUNT_INVALID"
    ]


def test_payment_status_outside_vocabulary(spark):
    assert payment_reasons(spark, status="invalid_payment_status") == [
        "PAYMENT_STATUS_INVALID"
    ]


def test_payment_status_is_case_insensitive(spark):
    assert payment_reasons(spark, status="SUCCESS") == []


def test_payment_before_its_order(spark):
    assert payment_reasons(
        spark,
        payment_date=EARLIER,
        related_order_created_at=NOW,
    ) == ["PAYMENT_BEFORE_ORDER"]


def test_payment_exactly_at_order_creation_is_valid(spark):
    assert payment_reasons(
        spark,
        payment_date=NOW,
        related_order_created_at=NOW,
    ) == []
