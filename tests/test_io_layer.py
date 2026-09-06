"""Тесты слоя ввода-вывода: ingest, DDL и маркер загрузки.

DQ-правила покрыты отдельно (`test_transform_dq.py`), а здесь то, что
до сих пор не проверялось вовсе: запись партиций, разбор SQL-скрипта на
выражения и признак «день загружен целиком». Именно в этих местах живут
идемпотентность и атомарность, то есть самое хрупкое во всём пайплайне —
их поломка не даёт неверную цифру, она тихо теряет или задваивает
целый день.

Spark нужен только маркеру загрузки — не потому, что тот считает
что-то на Spark, а потому, что живёт в модуле загрузчика; остальное
работает на pandas, а обращения к ClickHouse подменяются заглушкой.
"""

from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

import clickhouse_ddl
import ingest_csv_to_raw as ingest

# ---------------------------------------------------------------------
# Контракт партиционирования: перезапись запрещена
# ---------------------------------------------------------------------


class TestPartitionContract:
    def test_existing_partition_is_refused(self, tmp_path):
        """Повторная запись того же дня обязана падать, а не дописывать.

        Иначе повторный прогон удваивает день, и заметно это станет
        только на витрине, где-то через неделю.
        """
        partition = tmp_path / "orders" / "load_date=2026-06-01"
        partition.mkdir(parents=True)

        with pytest.raises(FileExistsError, match="уже существует"):
            ingest.ensure_not_exists(partition, "Raw")

    def test_absent_partition_passes(self, tmp_path):
        ingest.ensure_not_exists(tmp_path / "нет-такой", "Raw")

    def test_write_refuses_to_overwrite(self, tmp_path):
        frame = pd.DataFrame({"order_id": ["ord_1"]})
        ingest.write_raw_parquet(frame, tmp_path, "orders", date(2026, 6, 1))

        with pytest.raises(FileExistsError):
            ingest.write_raw_parquet(frame, tmp_path, "orders", date(2026, 6, 1))

    def test_written_partition_reads_back_intact(self, tmp_path):
        frame = pd.DataFrame(
            {"order_id": ["ord_1", "ord_2"], "amount_kopecks": [100, 200]}
        )
        ingest.write_raw_parquet(frame, tmp_path, "orders", date(2026, 6, 1))

        written = pd.read_parquet(
            tmp_path / "orders" / "load_date=2026-06-01" / "orders.parquet"
        )
        assert list(written["order_id"]) == ["ord_1", "ord_2"]

    def test_no_temporary_file_survives(self, tmp_path):
        """Запись идёт во временный файл и переименовывается.

        Оборванная запись не должна оставить недописанный parquet,
        который следующий шаг примет за готовую партицию.
        """
        ingest.write_raw_parquet(
            pd.DataFrame({"order_id": ["ord_1"]}), tmp_path, "orders", date(2026, 6, 1)
        )
        partition = tmp_path / "orders" / "load_date=2026-06-01"
        assert [p.name for p in partition.iterdir()] == ["orders.parquet"]


class TestIngestManifest:
    def test_manifest_records_what_was_ingested(self, tmp_path):
        ingest.save_ingest_manifest(
            tmp_path, date(2026, 6, 1), {"orders": {"rows": 42}}
        )
        path = (
            tmp_path / "_manifests" / "load_date=2026-06-01" / "ingest_manifest.json"
        )
        manifest = json.loads(path.read_text(encoding="utf-8"))

        assert manifest["load_date"] == "2026-06-01"
        assert manifest["entities"]["orders"]["rows"] == 42

    def test_manifest_is_rewritable(self, tmp_path):
        """Манифест перезаписывается: сам он контрактом не защищён.

        Защита от перезаписи живёт на партиции данных, и дублировать её
        здесь не нужно — иначе повторный прогон падал бы дважды и по
        разным причинам.
        """
        for rows in (1, 2):
            ingest.save_ingest_manifest(
                tmp_path, date(2026, 6, 1), {"orders": {"rows": rows}}
            )
        path = (
            tmp_path / "_manifests" / "load_date=2026-06-01" / "ingest_manifest.json"
        )
        assert json.loads(path.read_text(encoding="utf-8"))["entities"]["orders"][
            "rows"
        ] == 2


# ---------------------------------------------------------------------
# Разбор SQL-скрипта
# ---------------------------------------------------------------------


class TestSqlScriptSplitting:
    """Разбиение на выражения — место, где уже ломались дважды."""

    def _spy(self, monkeypatch):
        calls = []

        def fake(args, sql, use_database=True):
            calls.append((sql, use_database))
            return ""

        monkeypatch.setattr(clickhouse_ddl, "ch_execute", fake)
        return calls

    def test_statements_are_split_and_counted(self, monkeypatch):
        calls = self._spy(monkeypatch)
        executed = clickhouse_ddl._execute_sql_script(
            None, "SELECT 1; SELECT 2; SELECT 3;"
        )
        assert executed == 3
        assert len(calls) == 3

    def test_empty_fragments_are_skipped(self, monkeypatch):
        calls = self._spy(monkeypatch)
        assert clickhouse_ddl._execute_sql_script(None, "SELECT 1;;;  ;") == 1
        assert len(calls) == 1

    def test_create_database_runs_without_a_database(self, monkeypatch):
        """С ?database=... до создания базы ClickHouse откажет."""
        calls = self._spy(monkeypatch)
        clickhouse_ddl._execute_sql_script(None, "CREATE DATABASE IF NOT EXISTS x;")
        assert calls[0][1] is False

    def test_leading_comment_does_not_hide_create_database(self, monkeypatch):
        """Комментарий склеивается с выражением при разбиении по «;».

        Поэтому распознавание идёт по всему тексту, а не по началу
        строки — startswith здесь дал бы ложное «обычный запрос».
        """
        calls = self._spy(monkeypatch)
        clickhouse_ddl._execute_sql_script(
            None, "-- заводим базу\nCREATE DATABASE IF NOT EXISTS x;"
        )
        assert calls[0][1] is False

    def test_ordinary_statement_keeps_the_database(self, monkeypatch):
        calls = self._spy(monkeypatch)
        clickhouse_ddl._execute_sql_script(
            None, "CREATE OR REPLACE VIEW v AS SELECT 1;"
        )
        assert calls[0][1] is True


# ---------------------------------------------------------------------
# Маркер «день загружен целиком»
# ---------------------------------------------------------------------


class TestLoadCommitMarker:
    """Импорт отложен: load_to_clickhouse тянет pyspark на уровне модуля.

    Сама логика маркера — три HTTP-запроса и склейка строки, Spark ей не
    нужен, но модуль без него не импортируется. Пропуск здесь честнее
    заглушки sys.modules: та проверяла бы подделку, а не код.
    """

    @pytest.fixture
    def loader(self):
        return pytest.importorskip(
            "load_to_clickhouse", reason="нужен requirements-spark.txt"
        )

    def _args(self):
        return SimpleNamespace(load_date="2026-06-01")

    def test_day_with_a_marker_is_committed(self, loader, monkeypatch):
        monkeypatch.setattr(loader, "ch_execute", lambda args, sql: "3\n")
        assert loader.is_load_date_committed(self._args()) is True

    def test_day_without_a_marker_is_not(self, loader, monkeypatch):
        monkeypatch.setattr(loader, "ch_execute", lambda args, sql: "0\n")
        assert loader.is_load_date_committed(self._args()) is False

    def test_marker_is_written_for_every_entity(self, loader, monkeypatch):
        sent = []
        monkeypatch.setattr(
            loader, "ch_execute", lambda args, sql: sent.append(sql) or ""
        )

        loader.record_commit(self._args(), {"orders": 10, "payments": 20})

        assert len(sent) == 1, "одна вставка на весь день, а не по строке"
        assert "('2026-06-01', 'orders', 10)" in sent[0]
        assert "('2026-06-01', 'payments', 20)" in sent[0]

    def test_reload_drops_every_partitioned_table(self, loader, monkeypatch):
        sent = []
        monkeypatch.setattr(
            loader, "ch_execute", lambda args, sql: sent.append(sql) or ""
        )

        loader.drop_existing_partitions(self._args())

        assert len(sent) == len(loader.PARTITIONED_TABLES)
        # IF EXISTS обязателен: за день, где карантин пуст, партиции нет,
        # и это не ошибка.
        assert all("DROP PARTITION IF EXISTS" in sql for sql in sent)
