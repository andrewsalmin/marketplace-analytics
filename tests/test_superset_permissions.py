"""Выдача роли прав на витрины после смены схемы датасета.

Тесты держат под контролем единственное по-настоящему опасное место
скрипта: эндпоинт прав роли в Superset не добавляет разрешения, а
заменяет весь список. Ошибка в отборе оставила бы Public без доступа к
самому дашборду, и заметили бы это уже посетители.

Второе, что здесь закреплено, — формат имени разрешения. Superset
называет его `[база].[таблица](id:N)`, без схемы; попытка разбирать имя
по схеме находила ноль прав, и скрипт молча решал, что выдавать нечего.

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

    def __init__(self, permissions=None, role_permissions=None, named=None):
        self.permissions = dict(permissions or {})
        self.role_permissions = set(role_permissions or [])
        # Именованные права вроде can_read на TimeRangeRestApi. По
        # умолчанию есть — их отсутствие проверяется отдельным тестом.
        self.named = {"TimeRangeRestApi": 900} if named is None else dict(named)
        self.written: list[set[int]] = []

    def role_by_name(self, name):
        return {"id": 7, "name": name} if name == "Public" else None

    def role_permission_ids(self, role_id):
        return set(self.role_permissions)

    def datasource_permissions(self):
        return dict(self.permissions)

    def named_permissions(self, wanted):
        return dict(self.named)

    def set_role_permissions(self, role_id, ids):
        self.written.append(set(ids))
        self.role_permissions = set(ids)


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    """Снимок прав пишется в текущий каталог — не в репозиторий."""
    monkeypatch.chdir(tmp_path)


def _all_datasets(first_id=1):
    """Датасеты витрин так, как их видит скрипт: имя -> запись с id."""
    return {
        name: {"id": first_id + i, "table_name": name}
        for i, name in enumerate(arf.DATASETS)
    }


def _all_permissions(datasets, first_perm=500):
    """Разрешение datasource_access на каждый датасет: id -> id."""
    return {d["id"]: first_perm + i for i, d in enumerate(datasets.values())}


def make_runner(client, datasets=None, apply=True):
    runner = arf.Runner.__new__(arf.Runner)
    runner.client = client
    runner.apply = apply
    runner.planned = []
    runner.datasets = datasets if datasets is not None else _all_datasets()
    return runner


# ---------------------------------------------------------------------
# Разбор имён разрешений
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

    def test_matches_by_dataset_id_not_by_schema(self):
        """Схемы в имени нет — сопоставление идёт по (id:N)."""
        rows = [
            {
                "id": 11,
                "permission": {"name": "datasource_access"},
                "view_menu": {"name": "[Marketplace Analytics].[orders](id:42)"},
            },
            {
                "id": 12,
                "permission": {"name": "can_read"},
                "view_menu": {"name": "[Marketplace Analytics].[payments](id:43)"},
            },
        ]
        assert self._client(rows).datasource_permissions() == {42: 11}

    def test_ignores_names_without_an_id(self):
        rows = [
            {
                "id": 13,
                "permission": {"name": "datasource_access"},
                "view_menu": {"name": "[Marketplace Analytics].[legacy]"},
            }
        ]
        assert self._client(rows).datasource_permissions() == {}

    def test_survives_rows_without_permission_or_view_menu(self):
        rows = [{"id": 4, "permission": None, "view_menu": None}]
        assert self._client(rows).datasource_permissions() == {}


# ---------------------------------------------------------------------
# Выдача прав
# ---------------------------------------------------------------------


class TestGrantPublicAccess:
    def test_existing_permissions_are_never_dropped(self):
        """Главное свойство: роль не должна потерять то, что у неё было."""
        datasets = _all_datasets()
        perms = _all_permissions(datasets)
        unrelated = {1, 2, 3}  # доступ к дашборду, чартам и прочему
        client = FakeClient(perms, role_permissions=unrelated)

        make_runner(client, datasets).grant_public_access("Public")

        assert client.written, "запись должна была произойти"
        written = client.written[0]
        assert list(
            Path.cwd().glob("superset_role_Public_*.json")
        ), "перед записью обязан появиться снимок прав"
        assert unrelated <= written, "прежние права обязаны уцелеть"
        assert set(perms.values()) <= written

    def test_nothing_is_written_when_access_is_already_granted(self):
        datasets = _all_datasets()
        perms = _all_permissions(datasets)
        already = set(perms.values()) | {900}
        client = FakeClient(perms, role_permissions=already)
        runner = make_runner(client, datasets)

        runner.grant_public_access("Public")

        assert client.written == [], "идемпотентность: второй прогон молчит"
        assert runner.planned == []

    def test_plan_mode_does_not_write(self):
        datasets = _all_datasets()
        client = FakeClient(_all_permissions(datasets), role_permissions={1})
        runner = make_runner(client, datasets, apply=False)

        runner.grant_public_access("Public")

        assert client.written == []
        assert runner.planned, "но в план правка попасть должна"

    def test_unknown_role_is_reported_and_skipped(self):
        datasets = _all_datasets()
        client = FakeClient(_all_permissions(datasets), role_permissions={1})
        make_runner(client, datasets).grant_public_access("НетТакойРоли")
        assert client.written == []

    def test_raises_when_the_write_loses_permissions(self):
        """Если Superset заменил список вместо объединения — падаем громко."""
        datasets = _all_datasets()
        client = FakeClient(_all_permissions(datasets), role_permissions={1, 2})

        def losing_write(role_id, ids):
            client.written.append(set(ids))
            client.role_permissions = {next(iter(ids))}  # «потеряли» остальные

        client.set_role_permissions = losing_write

        with pytest.raises(arf.SupersetError, match="потеряла"):
            make_runner(client, datasets).grant_public_access("Public")

    def test_dataset_without_a_permission_object_is_reported(self, capsys):
        """Права на датасет ещё не заведены — сказать, а не промолчать."""
        datasets = _all_datasets()
        perms = _all_permissions(datasets)
        perms.pop(datasets["customers"]["id"])
        client = FakeClient(perms, role_permissions=set())

        make_runner(client, datasets).grant_public_access("Public")

        printed = capsys.readouterr().out
        assert "customers" in printed
        assert "нет объектов прав" in printed

    def test_dataset_missing_entirely_is_reported(self, capsys):
        """Датасета нет вовсе — повод сказать, а не упасть по KeyError."""
        datasets = _all_datasets()
        datasets.pop("orders")
        client = FakeClient(_all_permissions(datasets), role_permissions=set())

        make_runner(client, datasets).grant_public_access("Public")

        assert "orders" in capsys.readouterr().out


class TestExtraPublicPermissions:
    """Права, без которых аноним не может пользоваться дашбордом."""

    def test_time_range_permission_is_granted_alongside_the_marts(self):
        """Диалог периода без TimeRangeRestApi отвечает анониму 401."""
        datasets = _all_datasets()
        client = FakeClient(_all_permissions(datasets), role_permissions={1})

        make_runner(client, datasets).grant_public_access("Public")

        assert 900 in client.written[0], "право на разбор периода не выдано"

    def test_absent_named_permission_is_reported(self, capsys):
        datasets = _all_datasets()
        perms = _all_permissions(datasets)
        client = FakeClient(
            perms, role_permissions=set(perms.values()), named={}
        )

        make_runner(client, datasets).grant_public_access("Public")

        assert "TimeRangeRestApi" in capsys.readouterr().out
