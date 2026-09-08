"""Выводы для обзора: формулировки, числа и отказ от причинности.

Живого ClickHouse здесь нет — запрос подменяется заглушкой, поэтому
проверяются формат и правила, а не значения.
"""

from __future__ import annotations

import pytest

import overview_findings as of


@pytest.fixture
def answers(monkeypatch):
    """Ответы ClickHouse по порядку обращения."""

    def stub(sql):
        text = " ".join(sql.split())
        if "min(toDate" in text:
            return "2026-06-01\t2026-09-07\n"
        if "sum(has_order) / count()" in text:
            return "73.8\n"
        if "count(), sum(has_order)" in text:
            return "11396\t8406\t2990\n"
        if "Не оплачен вовремя" in text:
            return "66.2\n"
        if "Москва" in text:
            return "32.5\n"
        raise AssertionError(f"неожиданный запрос: {text[:80]}")

    monkeypatch.setattr(of, "query", stub)


class TestRendering:
    def test_period_is_named_in_words(self, answers):
        assert "за 1 июня 2026 — 7 сентября 2026" in of.render()

    def test_numbers_use_russian_separators(self, answers):
        """Рядом стоят карточки с «237,5» — разнобой виден сразу."""
        text = of.render()
        assert "73,8%" in text
        assert "11 396" in text.replace(" ", " ")
        assert "73.8" not in text

    def test_every_finding_says_where_to_check_it(self, answers):
        """Утверждение, которое нельзя сверить, на дашборде бесполезно."""
        assert of.render().count("Проверяется") == len(of.FINDINGS)

    def test_every_finding_carries_a_number(self, answers):
        for line in of.render().splitlines():
            if line.startswith("- "):
                assert any(ch.isdigit() for ch in line), line


class TestNoCausality:
    """Причинность в выводах запрещена, и это не стилистика.

    Расходов на привлечение в данных нет вовсе, поэтому «канал X самый
    выгодный» было бы не выводом, а догадкой, поданной с точностью до
    десятой процента.
    """

    FORBIDDEN = [
        "выгодн",
        "рентабельн",
        "эффективн",
        "приводит к",
        "из-за",
        "благодаря",
        "поэтому клиенты",
    ]

    def test_claims_stay_descriptive(self, answers):
        text = of.render().lower()
        found = [word for word in self.FORBIDDEN if word in text]
        assert not found, f"вывод стал причинным: {found}"
