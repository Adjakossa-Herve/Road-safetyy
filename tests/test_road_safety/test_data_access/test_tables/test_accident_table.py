from __future__ import annotations

import road_safety.data_access.tables.accident_table as table_mod


class FakeCursor:
    def __init__(self, execute_error: Exception | None = None):
        self.execute_error = execute_error
        self.executed = []
        self.closed = False

    def execute(self, query: str):
        self.executed.append(query)
        if self.execute_error is not None:
            raise self.execute_error

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, execute_error: Exception | None = None):
        self.cursor_obj = FakeCursor(execute_error=execute_error)
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


def test_create_accident_table_success(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(table_mod, "establish_connection", lambda: conn)

    ok = table_mod.create_accident_table()

    assert ok is True
    assert conn.commits == 1
    assert conn.closed is True
    assert conn.cursor_obj.closed is True
    assert "CREATE TABLE IF NOT EXISTS raw.accidents" in conn.cursor_obj.executed[0]


def test_create_accident_table_returns_false_when_no_connection(monkeypatch):
    monkeypatch.setattr(table_mod, "establish_connection", lambda: None)

    ok = table_mod.create_accident_table()

    assert ok is False


def test_create_accident_table_handles_execution_error(monkeypatch, capsys):
    conn = FakeConn(execute_error=RuntimeError("cannot create table"))
    monkeypatch.setattr(table_mod, "establish_connection", lambda: conn)

    ok = table_mod.create_accident_table()

    out = capsys.readouterr().out
    assert ok is False
    assert "Error creating table" in out
    assert conn.closed is True
