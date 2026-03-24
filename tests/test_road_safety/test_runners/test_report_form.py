import builtins

import pytest
import road_safety.runners.report_form as rf


def test_choose_from_list_selects_option(monkeypatch):
    inputs = iter(["2"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    out = rf.choose_from_list("Test", ["A", "B", "C"], allow_other=False)
    assert out == "B"


def test_run_report_form_builds_and_inserts(monkeypatch):
    # Mock dropdown options (no DB)
    monkeypatch.setattr(rf, "list_distinct", lambda col, limit=30: ["X", "Y"])

    # Capture inserted report
    captured = {}

    def fake_insert(r):
        captured["full_name"] = r.full_name
        captured["commune"] = r.commune
        captured["type_collision"] = r.type_collision

    monkeypatch.setattr(rf, "insert_report", fake_insert)

    # Inputs:
    # full_name
    # commune -> choose 1
    # type_collision -> choose 2
    # intersection -> choose 1
    # categorie_route -> choose 1
    # luminosite -> choose 1
    # cond_atmos -> choose 1
    # gravite -> choose 1
    # location_text
    # vehicle_type
    # notes
    inputs = iter(
        [
            "Jean Dupont",
            "1",
            "2",
            "1",
            "1",
            "1",
            "1",
            "1",
            "Rue X",
            "VL",
            "RAS",
        ]
    )
    monkeypatch.setattr(builtins, "input", lambda prompt="": next(inputs))

    rf.run_report_form()

    assert captured["full_name"] == "Jean Dupont"
    assert captured["commune"] == "X"
    assert captured["type_collision"] == "Y"


# ---------------------------------------------------------------------------
# Additional tests for 100% coverage
# ---------------------------------------------------------------------------


class TestFetchAll:
    def test_raises_runtime_error_when_no_connection(self, monkeypatch):
        monkeypatch.setattr(rf, "establish_connection", lambda: None)
        with pytest.raises(RuntimeError, match="Database connection failed"):
            rf.fetch_all("SELECT 1")

    def test_returns_rows_from_fake_connection(self, monkeypatch):
        from dataclasses import dataclass
        from typing import Any, List, Tuple

        @dataclass
        class FakeCur:
            _rows: List[Tuple[Any, ...]]

            def execute(self, q, p=()):
                pass

            def fetchall(self):
                return self._rows

            def close(self):
                pass

        @dataclass
        class FakeConn:
            cur: FakeCur

            def cursor(self):
                return self.cur

            def close(self):
                pass

        conn = FakeConn(cur=FakeCur(_rows=[("Paris", 5)]))
        monkeypatch.setattr(rf, "establish_connection", lambda: conn)
        rows = rf.fetch_all("SELECT commune, total FROM raw.accidents")
        assert rows == [("Paris", 5)]


class TestExecute:
    def test_executes_and_commits(self, monkeypatch):
        committed = []

        from dataclasses import dataclass

        @dataclass
        class FakeCur:
            def execute(self, q, p=()):
                pass

            def close(self):
                pass

        @dataclass
        class FakeConn:
            def cursor(self):
                return FakeCur()

            def commit(self):
                committed.append(True)

            def close(self):
                pass

        monkeypatch.setattr(rf, "establish_connection", lambda: FakeConn())
        rf.execute("INSERT INTO raw.user_reports VALUES (%s)", ("data",))
        assert committed == [True]

    def test_raises_runtime_error_when_no_connection(self, monkeypatch):
        monkeypatch.setattr(rf, "establish_connection", lambda: None)
        with pytest.raises(RuntimeError, match="Database connection failed"):
            rf.execute("SELECT 1")


class TestListDistinct:
    def test_returns_distinct_values(self, monkeypatch):
        monkeypatch.setattr(rf, "fetch_all", lambda q, p=(): [("Paris",), ("Lyon",)])
        vals = rf.list_distinct("commune", limit=10)
        assert vals == ["Paris", "Lyon"]

    def test_returns_unknown_when_empty(self, monkeypatch):
        monkeypatch.setattr(rf, "fetch_all", lambda q, p=(): [])
        vals = rf.list_distinct("commune")
        assert vals == ["UNKNOWN"]


class TestChooseFromListEdgeCases:
    def test_allow_other_free_text_returns_typed_value(self, monkeypatch):
        inputs = iter(["0", "Marseille"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        result = rf.choose_from_list("Commune", ["Paris", "Lyon"], allow_other=True)
        assert result == "Marseille"

    def test_allow_other_empty_free_text_returns_unknown(self, monkeypatch):
        inputs = iter(["0", ""])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        result = rf.choose_from_list("Commune", ["Paris", "Lyon"], allow_other=True)
        assert result == "UNKNOWN"

    def test_invalid_input_prints_error_then_accepts_valid(self, monkeypatch, capsys):
        inputs = iter(["xyz", "1"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        result = rf.choose_from_list("Commune", ["Paris", "Lyon"], allow_other=False)
        out = capsys.readouterr().out
        assert "invalide" in out.lower()
        assert result == "Paris"


class TestRunReportFormEdgeCases:
    def test_empty_name_prints_cancel_message(self, monkeypatch, capsys):
        inputs = iter([""])
        monkeypatch.setattr(builtins, "input", lambda prompt="": next(inputs))
        rf.run_report_form()
        out = capsys.readouterr().out
        assert "Annulé" in out


class TestInsertReport:
    def test_calls_execute_with_correct_params(self, monkeypatch):
        called = []
        monkeypatch.setattr(rf, "execute", lambda q, p: called.append(p))

        report = rf.UserReport(
            full_name="Test User",
            commune="Paris",
            location_text="Rue A",
            vehicle_type="VL",
            categorie_route="Nationale",
            intersection="Carrefour",
            type_collision="Frontal",
            luminosite="Jour",
            cond_atmos="Normale",
            gravite_usager="Leger",
            notes="RAS",
        )
        rf.insert_report(report)

        assert len(called) == 1
        params = called[0]
        assert params[0] == "Test User"
        assert params[1] == "Paris"


class TestListReports:
    def test_returns_formatted_rows(self, monkeypatch):
        monkeypatch.setattr(
            rf,
            "fetch_all",
            lambda q, p=(): [("2026-01-01", "Jean D.", "Paris", "Tue")],
        )
        rows = rf.list_reports(limit=5)
        assert rows == [("2026-01-01", "Jean D.", "Paris", "Tue")]
