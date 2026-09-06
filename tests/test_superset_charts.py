"""Поиск чарта по имени при повторных переименованиях.

Скрипт переименовывает чарты и потому обязан узнавать их под всеми
именами, которые они у нас носили. Дважды случалось иначе: после
переименования чарт переставал находиться, правка молча пропускалась, и
на дашборде оставалась старая версия — без единой ошибки в выводе.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "apply_review_fixes", REPO_ROOT / "superset" / "apply_review_fixes.py"
)
arf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(arf)

find = arf.Runner._find_chart


class TestFindChart:
    def test_finds_by_the_original_name(self):
        charts = {"Изменение GMV": {"id": 1}}
        patch = {"slice_name": "Динамика GMV"}
        assert find(charts, "Изменение GMV", patch)["id"] == 1

    def test_finds_by_the_target_name(self):
        """Второй прогон: чарт уже переименован скриптом."""
        charts = {"Динамика GMV": {"id": 2}}
        patch = {"slice_name": "Динамика GMV"}
        assert find(charts, "Изменение GMV", patch)["id"] == 2

    def test_finds_by_a_previous_name(self):
        """Переименовали дважды — на инстансе промежуточный вариант."""
        charts = {"Динамика GMV": {"id": 3}}
        patch = {
            "slice_name": "Динамика GMV (среднее за 7 дней)",
            "aliases": ["Динамика GMV"],
        }
        assert find(charts, "Изменение GMV", patch)["id"] == 3

    def test_returns_none_when_nothing_matches(self):
        charts = {"Совсем другой чарт": {"id": 4}}
        patch = {"slice_name": "Динамика GMV", "aliases": ["Что-то ещё"]}
        assert find(charts, "Изменение GMV", patch) is None

    def test_original_name_wins_over_alias(self):
        """Порядок важен: сначала точное совпадение с ключом."""
        charts = {"Изменение GMV": {"id": 5}, "Динамика GMV": {"id": 6}}
        patch = {"slice_name": "Новое", "aliases": ["Динамика GMV"]}
        assert find(charts, "Изменение GMV", patch)["id"] == 5


class TestRenameChainIsComplete:
    """Каждое переименование обязано оставлять след в aliases."""

    def test_renamed_charts_keep_their_previous_names(self):
        patches = arf.chart_patches()
        # Чарты, переименованные дважды: ключ, промежуточное имя, текущее.
        twice_renamed = {
            "Изменение GMV": "Динамика GMV",
            "Изменение среднего чека (AOV)": "Динамика среднего чека (AOV)",
            "Изменение числа заказов": "Динамика числа заказов",
            "Изменение числа новых покупателей": "Динамика числа новых покупателей",
        }
        for key, previous in twice_renamed.items():
            aliases = patches[key].get("aliases", [])
            assert previous in aliases, (
                f"«{key}» переименован дважды, но промежуточное имя "
                f"«{previous}» не записано в aliases — следующий прогон "
                "не найдёт чарт и молча пропустит правку"
            )

    def test_aliases_never_equal_the_current_name(self):
        """Алиас, совпавший с текущим именем, — забытая чистка."""
        for name, patch in arf.chart_patches().items():
            for alias in patch.get("aliases", []):
                assert alias != patch.get("slice_name"), (
                    f"«{name}»: алиас совпадает с текущим именем"
                )


class TestSyncBigNumbers:
    """KPI-карточки должны не только создаваться, но и обновляться."""

    class _Client:
        def __init__(self, charts):
            self.charts = charts
            self.puts = []
            self.posts = []

        def get(self, path):
            return {"result": self.charts}

        def put(self, path, payload):
            self.puts.append((path, payload))
            return {}

        def post(self, path, payload):
            self.posts.append((path, payload))
            return {"id": 999}

    def _runner(self, charts, apply=True):
        runner = arf.Runner.__new__(arf.Runner)
        runner.client = self._Client(charts)
        runner.apply = apply
        runner.planned = []
        runner.datasets = {"orders": {"id": 15}}
        runner.dashboard_id = 1
        return runner

    def _spec(self, **over):
        spec = {
            "name": "KPI · Заказы",
            "dataset": "orders",
            "metric": arf.metric("Заказов", "count()"),
            "format": "SMART_NUMBER",
            "subheader": "Создано заказов",
            "filters": [],
        }
        spec.update(over)
        return spec

    def test_existing_card_is_brought_in_line(self):
        """Изменился горизонт или подпись — карточка обязана обновиться."""
        stale = {
            "id": 7,
            "slice_name": "KPI · Заказы",
            "params": '{"subheader": "устаревшая подпись"}',
        }
        runner = self._runner([stale])

        runner.sync_big_numbers([self._spec()])

        assert runner.client.puts, "существующая карточка не обновилась"
        written = json.loads(runner.client.puts[0][1]["params"])
        assert written["subheader"] == "Создано заказов"

    def test_card_matching_the_spec_is_left_alone(self):
        runner = self._runner([])
        runner.sync_big_numbers([self._spec()])
        created = json.loads(runner.client.posts[0][1]["params"])

        settled = self._runner(
            [{"id": 7, "slice_name": "KPI · Заказы", "params": json.dumps(created)}]
        )
        settled.sync_big_numbers([self._spec()])

        assert settled.client.puts == [], "идемпотентность: повтор не пишет"
        assert settled.planned == []

    def test_missing_card_is_created(self):
        runner = self._runner([])
        ids = runner.sync_big_numbers([self._spec()])
        assert runner.client.posts, "карточки не было — её надо создать"
        assert ids["KPI · Заказы"] == 999
