"""Поиск чарта по имени при повторных переименованиях.

Скрипт переименовывает чарты и потому обязан узнавать их под всеми
именами, которые они у нас носили. Дважды случалось иначе: после
переименования чарт переставал находиться, правка молча пропускалась, и
на дашборде оставалась старая версия — без единой ошибки в выводе.
"""

from __future__ import annotations

import json

import pytest

# requests нужен самому скрипту, который тесты грузят ниже. Без пропуска
# импорт падал бы на машине без requirements-superset.txt — например, в
# окружении Spark на сервере.
pytest.importorskip("requests", reason="нужен requirements-superset.txt")

from superset import apply_review_fixes as arf

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
        """Карточки дашборда и общий список инстанса — разные вещи."""

        def __init__(self, dashboard_charts, elsewhere=None):
            self.dashboard_charts = dashboard_charts
            self.elsewhere = [] if elsewhere is None else elsewhere
            self.puts = []
            self.posts = []

        def charts(self, dashboard_id):
            return {c["slice_name"]: c for c in self.dashboard_charts}

        def get(self, path):
            return {"result": self.elsewhere}

        def put(self, path, payload):
            self.puts.append((path, payload))
            return {}

        def post(self, path, payload):
            self.posts.append((path, payload))
            return {"id": 999}

    def _runner(self, dashboard_charts, elsewhere=None, apply=True):
        runner = arf.Runner.__new__(arf.Runner)
        runner.client = self._Client(dashboard_charts, elsewhere)
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
            "form_data": {"subheader": "устаревшая подпись"},
        }
        runner = self._runner([stale])

        runner.sync_big_numbers([self._spec()])

        assert runner.client.puts, "существующая карточка не обновилась"
        written = json.loads(runner.client.puts[0][1]["params"])
        assert written["subheader"] == "Создано заказов"

    def test_card_matching_the_spec_is_left_alone(self):
        created = self._runner([])
        created.sync_big_numbers([self._spec()])
        params = json.loads(created.client.posts[0][1]["params"])

        settled = self._runner(
            [{"id": 7, "slice_name": "KPI · Заказы", "form_data": params}]
        )
        settled.sync_big_numbers([self._spec()])

        assert settled.client.puts == [], "идемпотентность: повтор не пишет"
        assert settled.planned == []

    def test_missing_card_is_created(self):
        runner = self._runner([])
        ids = runner.sync_big_numbers([self._spec()])
        assert runner.client.posts, "карточки не было — её надо создать"
        assert ids["KPI · Заказы"] == 999

    def test_card_under_the_old_name_is_renamed_not_duplicated(self):
        """Переименование обязано находить карточку по прежнему имени.

        Иначе это не переименование, а размножение: скрипт не найдёт
        «Заказы», решит, что карточки нет, и создаст вторую рядом со
        старой. Так карточки уже задваивались однажды, и правки после
        этого уходили то в одну копию, то в другую.
        """
        old = {"id": 7, "slice_name": "KPI · Заказы", "form_data": {}}
        runner = self._runner([old])

        ids = runner.sync_big_numbers(
            [self._spec(name="Заказы", aliases=["KPI · Заказы"])]
        )

        assert runner.client.posts == [], "новую карточку создавать нельзя"
        assert ids["Заказы"] == 7, "должна использоваться прежняя карточка"
        path, payload = runner.client.puts[0]
        assert path == "/api/v1/chart/7"
        assert payload["slice_name"] == "Заказы"

    def test_renaming_stops_once_the_new_name_is_in_place(self):
        """Второй прогон не должен переписывать уже переименованное."""
        created = self._runner([])
        spec = self._spec(name="Заказы", aliases=["KPI · Заказы"])
        created.sync_big_numbers([spec])
        params = json.loads(created.client.posts[0][1]["params"])

        settled = self._runner(
            [{"id": 7, "slice_name": "Заказы", "form_data": params}]
        )
        settled.sync_big_numbers([spec])

        assert settled.client.puts == [], "идемпотентность: повтор не пишет"
        assert settled.planned == []

    def test_currency_is_written_only_where_asked(self):
        """Пустая валюта — не то же самое, что её отсутствие.

        Superset, увидев ключ currency_format, рисует суффикс; с пустым
        значением это пустой суффикс и лишний отступ у числа.
        """
        plain = self._runner([])
        plain.sync_big_numbers([self._spec()])
        assert "currency_format" not in json.loads(plain.client.posts[0][1]["params"])

        money = self._runner([])
        money.sync_big_numbers(
            [self._spec(currency={"symbol": "RUB", "symbolPosition": "suffix"})]
        )
        written = json.loads(money.client.posts[0][1]["params"])
        assert written["currency_format"]["symbol"] == "RUB"

    def test_duplicate_outside_the_dashboard_is_not_recreated(self, capsys):
        """Одноимённый чарт вне дашборда — повод остановиться.

        Именно так карточки и задвоились: прогон не нашёл их на
        дашборде и создал вторые, после чего правки уходили то в одну
        копию, то в другую.
        """
        runner = self._runner([], elsewhere=[{"id": 99, "slice_name": "KPI · Заказы"}])

        runner.sync_big_numbers([self._spec()])

        assert runner.client.posts == [], "дубликат создавать нельзя"
        assert "не на дашборде" in capsys.readouterr().out


class TestSlugsAreUnique:
    """Идентификаторы из русских имён обязаны различаться.

    Правило оставляет только [a-z0-9], а в кириллице таких символов нет:
    слаг выходил пустым, и разные имена получали один идентификатор. На
    дашборде это выглядело так, что две карточки вкладки «Качество
    данных» встали под одним ключом раскладки и вторая затёрла первую.
    """

    def test_cyrillic_names_do_not_collide(self):
        a = arf._slug("Часов с последней загрузки")
        b = arf._slug("Невалидных строк")
        assert a and b, "слаг не может быть пустым"
        assert a != b

    def test_same_name_gives_the_same_slug(self):
        """Иначе сверка с описанием видит различие на каждом прогоне."""
        assert arf._slug("Невалидных строк") == arf._slug("Невалидных строк")

    def test_latin_names_stay_readable(self):
        assert arf._slug("Payment success rate") == "payment_success_rate"


class TestFilterNamesAreStable:
    """Имя фильтра обязано быть одинаковым от запуска к запуску.

    Иначе сверка «чарт совпадает с описанием» всегда видит различие и
    переписывает его на каждом прогоне. Встроенный hash() для этого не
    годится: он солится на каждый процесс. Значение здесь прибито
    гвоздями — тест внутри одного процесса нестабильность не поймает.
    """

    def test_option_name_is_pinned(self):
        got = arf.sql_filter("is_mature = 1", "is_mature")["filterOptionName"]
        assert got == "filter_is_mature_190d569c", got

    def test_different_expressions_differ(self):
        a = arf.sql_filter("is_mature = 1", "x")["filterOptionName"]
        b = arf.sql_filter("is_payment_settled = 1", "x")["filterOptionName"]
        assert a != b


class TestDeleteCharts:
    """Удаление необратимо — защита от сноса нужного обязана быть."""

    class _Client:
        def __init__(self, position, charts):
            self.position = position
            self.charts = charts
            self.deleted = []

        def get(self, path):
            if path.startswith("/api/v1/dashboard/"):
                return {"result": {"position_json": json.dumps(self.position)}}
            return {"result": self.charts}

        def delete(self, path):
            self.deleted.append(int(path.rsplit("/", 1)[1]))
            return {}

    def _client(self):
        position = {
            "CHART-a": {"type": "CHART", "meta": {"chartId": 38}},
            "ROW-x": {"type": "ROW", "children": ["CHART-a"]},
        }
        charts = [
            {"id": 38, "slice_name": "KPI · GMV"},
            {"id": 44, "slice_name": "KPI · GMV"},
        ]
        return self._Client(position, charts)

    def test_chart_on_the_dashboard_is_never_deleted(self, capsys):
        client = self._client()
        arf.delete_charts(client, 1, [38], apply=True)
        assert client.deleted == [], "чарт с дашборда удалять нельзя"
        assert "стоит на дашборде" in capsys.readouterr().out

    def test_duplicate_outside_the_dashboard_is_deleted(self):
        client = self._client()
        arf.delete_charts(client, 1, [44], apply=True)
        assert client.deleted == [44]

    def test_plan_mode_deletes_nothing(self):
        client = self._client()
        arf.delete_charts(client, 1, [44], apply=False)
        assert client.deleted == []

    def test_unknown_id_is_reported(self, capsys):
        client = self._client()
        arf.delete_charts(client, 1, [999], apply=True)
        assert client.deleted == []
        assert "нет" in capsys.readouterr().out

    def test_mixed_list_deletes_only_the_safe_ones(self):
        client = self._client()
        arf.delete_charts(client, 1, [38, 44, 999], apply=True)
        assert client.deleted == [44]
