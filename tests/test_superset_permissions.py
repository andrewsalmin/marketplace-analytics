"""Выдача роли прав на витрины после смены схемы датасета.

Тесты держат под контролем единственное по-настоящему опасное место
скрипта: эндпоинт прав роли в Superset не добавляет разрешения, а
заменяет весь список. Ошибка в отборе оставила бы Public без доступа к
самому дашборду, и заметили бы это уже посетители.

Живого Superset здесь нет — клиент подменяется заглушкой, поэтому
проверяются разбор имён и арифметика множеств, а не HTTP.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "apply_review_fixes", REPO_ROOT / "superset" / "apply_review_fixes.py"
)
arf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(arf)


class FakeClient:
    """Заглушка Superset: помнит права роли и записанные вызовы."""

    def __init__(self, permissions_by_schema=None, role_permissions=None):
        self.permissions_by_schema = permissions_by_schema or {}
        self.role_permissions = set(role_permissions or [])
        self.written: list[set[int]] = []

    def role_by_name(self, name):
        return {"id": 7, "name": name} if name == "Public" else None

    def role_permission_ids(self, role_id):
        return set(self.role_permissions)

    def datasource_permissions(self, schema):
        return dict(self.permissions_by_schema.get(schema, {}))

    def set_role_permissions(self, role_id, ids):
        self.written.append(set(ids))
        self.role_permissions = set(ids)


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    """Снимок прав пишется в текущий каталог — не в репозиторий."""
    monkeypatch.chdir(tmp_path)


def make_runner(client, apply=True):
    runner = arf.Runner.__new__(arf.Runner)
    runner.client = client
    runner.apply = apply
    runner.planned = []
    runner.datasets = {}
    return runner


# ---------------------------------------------------------------------
# Разбор имён прав
# ---------------------------------------------------------------------


class TestDatasourcePermissionParsing:
    def _client(self, rows):
        pages = [rows, []]

        class C(arf.Superset):
            def __init__(self):
                pass

            def get(self, path):
                return {"result": pages.pop(0) if pages else []}

        return C()

    def test_picks_only_datasource_access_in_the_marts_schema(self):
        rows = [
            {
                "id": 1,
                "permission": {"name": "datasource_access"},
                "view_menu": {
                    "name": "[ClickHouse].[marketplace_marts].[orders](id:5)"
                },
            },
            {
                "id": 2,
                "permission": {"name": "datasource_access"},
                "view_menu": {
                    "name": "[ClickHouse].[marketplace_analytics].[orders](id:2)"
                },
            },
            {
                "id": 3,
                "permission": {"name": "can_read"},
                "view_menu": {"name": "[ClickHouse].[marketplace_marts].[payments]"},
            },
        ]
        found = self._client(rows).datasource_permissions("marketplace_marts")
        assert found == {"orders": 1}, "чужая схема или чужое право не должны попасть"

    def test_handles_names_without_the_id_suffix(self):
        rows = [
            {
                "id": 9,
                "permission": {"name": "datasource_access"},
                "view_menu": {"name": "[CH].[marketplace_marts].[load_freshness]"},
            }
        ]
        assert self._client(rows).datasource_permissions("marketplace_marts") == {
            "load_freshness": 9
        }

    def test_survives_rows_without_permission_or_view_menu(self):
        rows = [{"id": 4, "permission": None, "view_menu": None}]
        assert self._client(rows).datasource_permissions("marketplace_marts") == {}


# ---------------------------------------------------------------------
# Выдача прав
# ---------------------------------------------------------------------


class TestGrantPublicAccess:
    def _available(self, ids_from=100):
        return {
            "marketplace_marts": {
                name: ids_from + i for i, name in enumerate(arf.DATASETS)
            }
        }

    def test_existing_permissions_are_never_dropped(self):
        """Главное свойство: роль не должна потерять то, что у неё было."""
        unrelated = {1, 2, 3}  # доступ к дашборду, чартам и прочему
        client = FakeClient(self._available(), role_permissions=unrelated)

        make_runner(client).grant_public_access("Public")

        assert client.written, "запись должна была произойти"
        written = client.written[0]
        snapshots = list(Path.cwd().glob("superset_role_Public_*.json"))
        assert snapshots, "перед записью обязан появиться снимок прав"
        assert unrelated <= written, "прежние права обязаны уцелеть"
        assert set(self._available()["marketplace_marts"].values()) <= written

    def test_nothing_is_written_when_access_is_already_granted(self):
        available = self._available()
        client = FakeClient(
            available, role_permissions=set(available["marketplace_marts"].values())
        )
        runner = make_runner(client)

        runner.grant_public_access("Public")

        assert client.written == [], "идемпотентность: второй прогон молчит"
        assert runner.planned == []

    def test_plan_mode_does_not_write(self):
        client = FakeClient(self._available(), role_permissions={1})
        runner = make_runner(client, apply=False)

        runner.grant_public_access("Public")

        assert client.written == []
        assert runner.planned, "но в план правка попасть должна"

    def test_unknown_role_is_reported_and_skipped(self):
        client = FakeClient(self._available(), role_permissions={1})
        make_runner(client).grant_public_access("НетТакойРоли")
        assert client.written == []

    def test_raises_when_the_write_loses_permissions(self):
        """Если Superset заменил список вместо объединения — падаем громко."""
        client = FakeClient(self._available(), role_permissions={1, 2})

        def losing_write(role_id, ids):
            client.written.append(set(ids))
            client.role_permissions = {next(iter(ids))}  # «потеряли» остальные

        client.set_role_permissions = losing_write

        with pytest.raises(arf.SupersetError, match="потеряла"):
            make_runner(client).grant_public_access("Public")

    def test_missing_marts_are_reported_not_silently_skipped(self, capsys):
        """Датасеты ещё не переведены — об этом надо сказать, а не молчать."""
        partial = {"marketplace_marts": {"orders": 100}}
        client = FakeClient(partial, role_permissions=set())

        make_runner(client).grant_public_access("Public")

        printed = capsys.readouterr().out
        assert "не переведены" in printed
        assert "customers" in printed
