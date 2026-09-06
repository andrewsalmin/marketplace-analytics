"""Проверки витрин на данных, обёрнутые в pytest.

Сами проверки живут в marts_checks.py — их запускает пайплайн, и им
незачем зависеть от pytest. Здесь только обёртка для ручного прогона:

    pytest tests/test_marts_data.py -v

Нужен живой ClickHouse с загруженными данными; без него модуль
пропускается, как test_transform_dq.py без pyspark.
"""

from __future__ import annotations

import pytest

import marts_checks

pytestmark = pytest.mark.skipif(
    not marts_checks.available(),
    reason="нужен ClickHouse с загруженными витринами",
)


@pytest.mark.parametrize("name", list(marts_checks.CHECKS))
def test_mart_invariant(name: str) -> None:
    why, sql = marts_checks.CHECKS[name]
    marts_checks.evaluate(name, why, marts_checks.query(sql))
