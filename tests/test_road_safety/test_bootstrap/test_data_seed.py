from __future__ import annotations

from pathlib import Path

import road_safety.bootstrap.data_seed as seed_mod


class FakeCursor:
    def __init__(self, row):
        self.row = row
        self.executed = []
        self.closed = False

    def execute(self, query: str) -> None:
        self.executed.append(query)

    def fetchone(self):
        return self.row

    def close(self) -> None:
        self.closed = True


class FakeConn:
    def __init__(self, row):
        self.cursor_obj = FakeCursor(row=row)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def test_is_pytest_running(monkeypatch):
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    assert seed_mod._is_pytest_running() is False

    monkeypatch.setenv("PYTEST_CURRENT_TEST", "yes")
    assert seed_mod._is_pytest_running() is True


def test_ensure_accidents_loaded_returns_early_during_pytest(monkeypatch):
    called = {"dotenv": 0}
    monkeypatch.setattr(seed_mod, "_is_pytest_running", lambda: True)
    monkeypatch.setattr(
        seed_mod,
        "load_dotenv_if_present",
        lambda: called.__setitem__("dotenv", called["dotenv"] + 1),
    )

    seed_mod.ensure_accidents_loaded()

    # Should return before trying to load env / DB.
    assert called["dotenv"] == 0


def test_ensure_accidents_loaded_returns_when_auto_load_disabled(monkeypatch):
    monkeypatch.setattr(seed_mod, "_is_pytest_running", lambda: False)
    monkeypatch.setattr(seed_mod, "load_dotenv_if_present", lambda: None)
    monkeypatch.setenv("RS_AUTO_LOAD", "0")

    called = {"db": 0}
    monkeypatch.setattr(
        seed_mod,
        "establish_connection",
        lambda: called.__setitem__("db", called["db"] + 1),
    )

    seed_mod.ensure_accidents_loaded()

    assert called["db"] == 0


def test_ensure_accidents_loaded_returns_when_db_unreachable(monkeypatch):
    monkeypatch.setattr(seed_mod, "_is_pytest_running", lambda: False)
    monkeypatch.setattr(seed_mod, "load_dotenv_if_present", lambda: None)
    monkeypatch.setenv("RS_AUTO_LOAD", "1")
    monkeypatch.setattr(seed_mod, "establish_connection", lambda: None)

    # No exception expected.
    seed_mod.ensure_accidents_loaded()


def test_ensure_accidents_loaded_skips_when_table_not_empty(monkeypatch):
    monkeypatch.setattr(seed_mod, "_is_pytest_running", lambda: False)
    monkeypatch.setattr(seed_mod, "load_dotenv_if_present", lambda: None)
    monkeypatch.setenv("RS_AUTO_LOAD", "1")

    conn = FakeConn(row=(5,))
    monkeypatch.setattr(seed_mod, "establish_connection", lambda: conn)

    called = {"load": 0, "prepare": 0, "insert": 0}
    monkeypatch.setattr(
        seed_mod, "load_csv_data", lambda _: called.__setitem__("load", 1)
    )
    monkeypatch.setattr(
        seed_mod, "prepare_data_for_insertion", lambda _: called.__setitem__("prepare", 1)
    )
    monkeypatch.setattr(
        seed_mod, "insert_accidents", lambda _: called.__setitem__("insert", 1)
    )

    seed_mod.ensure_accidents_loaded()

    assert "SELECT COUNT(*) FROM raw.accidents;" in conn.cursor_obj.executed[0]
    assert conn.closed is True
    assert called["load"] == 0
    assert called["prepare"] == 0
    assert called["insert"] == 0


def test_ensure_accidents_loaded_treats_none_count_row_as_empty(monkeypatch):
    monkeypatch.setattr(seed_mod, "_is_pytest_running", lambda: False)
    monkeypatch.setattr(seed_mod, "load_dotenv_if_present", lambda: None)
    monkeypatch.setenv("RS_AUTO_LOAD", "1")

    conn = FakeConn(row=None)
    monkeypatch.setattr(seed_mod, "establish_connection", lambda: conn)

    called = {}
    monkeypatch.setattr(
        seed_mod,
        "load_csv_data",
        lambda path: called.__setitem__("csv_path", path) or "raw",
    )
    monkeypatch.setattr(
        seed_mod,
        "prepare_data_for_insertion",
        lambda raw: called.__setitem__("prepared_from", raw) or "clean",
    )
    monkeypatch.setattr(
        seed_mod,
        "insert_accidents",
        lambda clean: called.__setitem__("inserted", clean) or 1,
    )

    seed_mod.ensure_accidents_loaded()

    assert Path(called["csv_path"]).as_posix().endswith(
        "src/road_safety/data/accident_idf.csv"
    )
    assert called["prepared_from"] == "raw"
    assert called["inserted"] == "clean"


def test_ensure_accidents_loaded_loads_csv_when_table_empty(monkeypatch):
    monkeypatch.setattr(seed_mod, "_is_pytest_running", lambda: False)
    monkeypatch.setattr(seed_mod, "load_dotenv_if_present", lambda: None)
    monkeypatch.setenv("RS_AUTO_LOAD", "1")

    conn = FakeConn(row=(0,))
    monkeypatch.setattr(seed_mod, "establish_connection", lambda: conn)

    called = {"load": 0, "prepare": 0, "insert": 0}
    monkeypatch.setattr(
        seed_mod,
        "load_csv_data",
        lambda path: called.__setitem__("load", called["load"] + 1) or ["raw"],
    )
    monkeypatch.setattr(
        seed_mod,
        "prepare_data_for_insertion",
        lambda data: called.__setitem__("prepare", called["prepare"] + 1) or ["clean"],
    )
    monkeypatch.setattr(
        seed_mod,
        "insert_accidents",
        lambda rows: called.__setitem__("insert", called["insert"] + 1) or len(rows),
    )

    seed_mod.ensure_accidents_loaded()

    assert called["load"] == 1
    assert called["prepare"] == 1
    assert called["insert"] == 1
