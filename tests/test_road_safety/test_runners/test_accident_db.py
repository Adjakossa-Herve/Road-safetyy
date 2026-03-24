"""Tests unitaires pour road_safety.runners.accident_db.

Toutes les fonctions sont testées sans connexion réelle à PostgreSQL :
- fetch_all reçoit connect_func en paramètre → on passe directement un faux conn.
- Les fonctions de haut niveau (list_top_fatal_communes, etc.) appellent fetch_all
  en interne → on patche accident_db.fetch_all via monkeypatch.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Tuple

import pytest

import road_safety.runners.accident_db as db


# ---------------------------------------------------------------------------
# Infrastructure de faux objets DB
# ---------------------------------------------------------------------------


@dataclass
class FakeCursor:
    rows: List[Tuple[Any, ...]]
    executed: List[Tuple[str, tuple]] = field(default_factory=list)

    def execute(self, query: str, params: tuple = ()):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


@dataclass
class FakeConn:
    cursor_obj: FakeCursor

    def cursor(self):
        return self.cursor_obj

    def close(self):
        pass


def make_fake_conn(rows: List[Tuple[Any, ...]]) -> FakeConn:
    return FakeConn(cursor_obj=FakeCursor(rows=rows))


# ---------------------------------------------------------------------------
# fetch_all
# ---------------------------------------------------------------------------


class TestFetchAll:
    def test_returns_rows_from_connection(self):
        conn = make_fake_conn([("Paris", 42)])
        result = db.fetch_all("SELECT 1", connect_func=lambda: conn)
        assert result == [("Paris", 42)]

    def test_raises_runtime_error_when_connection_is_none(self):
        with pytest.raises(RuntimeError, match="Database connection failed"):
            db.fetch_all("SELECT 1", connect_func=lambda: None)

    def test_passes_params_to_cursor(self):
        conn = make_fake_conn([])
        db.fetch_all("SELECT %s", params=("hello",), connect_func=lambda: conn)
        _, params = conn.cursor_obj.executed[0]
        assert params == ("hello",)


# ---------------------------------------------------------------------------
# print_table
# ---------------------------------------------------------------------------


class TestPrintTable:
    def test_prints_no_results_when_empty(self, capsys):
        db.print_table(["col_a", "col_b"], [])
        assert "(no results)" in capsys.readouterr().out

    def test_prints_headers_and_rows(self, capsys):
        db.print_table(["commune", "total"], [("Paris", 100)])
        out = capsys.readouterr().out
        assert "commune" in out
        assert "Paris" in out
        assert "100" in out

    def test_prints_separator_line(self, capsys):
        db.print_table(["a", "b"], [("x", "y")])
        out = capsys.readouterr().out
        assert "-" in out


# ---------------------------------------------------------------------------
# fetch_table_columns
# ---------------------------------------------------------------------------


class TestFetchTableColumns:
    def test_passes_schema_and_table_as_params(self):
        conn = make_fake_conn([("col1", "text"), ("col2", "integer")])
        result = db.fetch_table_columns("raw", "accidents", connect_func=lambda: conn)
        assert result == [("col1", "text"), ("col2", "integer")]
        _, params = conn.cursor_obj.executed[0]
        assert params == ("raw", "accidents")

    def test_sql_queries_information_schema(self):
        conn = make_fake_conn([])
        db.fetch_table_columns("raw", "accidents", connect_func=lambda: conn)
        sql, _ = conn.cursor_obj.executed[0]
        assert "information_schema.columns" in sql


# ---------------------------------------------------------------------------
# compute_severity_breakdown
# ---------------------------------------------------------------------------


class TestComputeSeverityBreakdown:
    def test_returns_typed_list(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q: [("Tue", 50), ("Leger", 200)])
        result = db.compute_severity_breakdown()
        assert result == [("Tue", 50), ("Leger", 200)]

    def test_empty_result(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q: [])
        assert db.compute_severity_breakdown() == []


# ---------------------------------------------------------------------------
# compute_fatal_rate
# ---------------------------------------------------------------------------


class TestComputeFatalRate:
    def test_returns_correct_tuple(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [(1.5, 10, 700)])
        rate, fatalities, total = db.compute_fatal_rate()
        assert rate == 1.5
        assert fatalities == 10
        assert total == 700

    def test_handles_null_rate(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [(None, 0, 0)])
        rate, fatalities, total = db.compute_fatal_rate()
        assert rate == 0.0
        assert fatalities == 0
        assert total == 0

    def test_passes_fatal_label_in_params(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return [(0.0, 0, 0)]

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_fatal_rate()
        assert db.FATAL_LABEL in captured["p"]


# ---------------------------------------------------------------------------
# list_collision_types
# ---------------------------------------------------------------------------


class TestListCollisionTypes:
    def test_returns_typed_list(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q: [("Frontale", 300)])
        result = db.list_collision_types()
        assert result == [("Frontale", 300)]


# ---------------------------------------------------------------------------
# list_top_communes
# ---------------------------------------------------------------------------


class TestListTopCommunes:
    def test_return_values(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [("Paris", 200)])
        result = db.list_top_communes(5)
        assert result == [("Paris", 200)]

    def test_passes_limit_in_params(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.list_top_communes(20)
        assert 20 in captured["p"]


# ---------------------------------------------------------------------------
# list_top_fatal_communes  (nouvelle fonction)
# ---------------------------------------------------------------------------


class TestListTopFatalCommunes:
    def test_returns_typed_rows(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [("Paris", 5)])
        result = db.list_top_fatal_communes(5)
        assert result == [("Paris", 5)]

    def test_passes_limit_and_fatal_label(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.list_top_fatal_communes(7)
        assert 7 in captured["p"]
        assert db.FATAL_LABEL in captured["p"]


# ---------------------------------------------------------------------------
# list_top_severe_communes  (nouvelle fonction)
# ---------------------------------------------------------------------------


class TestListTopSevereCommunes:
    def test_returns_typed_rows(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [("Nanterre", 8)])
        result = db.list_top_severe_communes(5)
        assert result == [("Nanterre", 8)]

    def test_passes_both_severity_labels(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.list_top_severe_communes(5)
        assert db.FATAL_LABEL in captured["p"]
        assert db.SEVERE_LABEL in captured["p"]

    def test_passes_limit(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.list_top_severe_communes(12)
        assert 12 in captured["p"]


# ---------------------------------------------------------------------------
# compute_risk_score_by_commune  (nouvelle fonction)
# ---------------------------------------------------------------------------


class TestComputeRiskScoreByCommune:
    def test_returns_five_tuple_list(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [("Paris", 3, 10, 20, 49)])
        result = db.compute_risk_score_by_commune(5)
        assert result == [("Paris", 3, 10, 20, 49)]

    def test_passes_limit_in_params(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_risk_score_by_commune(15)
        assert 15 in captured["p"]

    def test_passes_all_three_severity_labels(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_risk_score_by_commune(5)
        assert db.FATAL_LABEL in captured["p"]
        assert db.SEVERE_LABEL in captured["p"]
        assert db.LIGHT_LABEL in captured["p"]


# ---------------------------------------------------------------------------
# compute_commune_risk_score  (nouvelle fonction)
# ---------------------------------------------------------------------------


class TestComputeCommuneRiskScore:
    def test_returns_four_ints(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [(2, 5, 10, 26)])
        result = db.compute_commune_risk_score("Paris")
        assert result == (2, 5, 10, 26)

    def test_handles_null_values(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [(None, None, None, None)])
        result = db.compute_commune_risk_score("Paris")
        assert result == (0, 0, 0, 0)

    def test_passes_commune_name_in_params(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return [(1, 1, 1, 5)]

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_commune_risk_score("Suresnes")
        assert "Suresnes" in captured["p"]


# ---------------------------------------------------------------------------
# compute_trend_days  (nouvelle fonction)
# ---------------------------------------------------------------------------


class TestComputeTrendDays:
    def test_without_commune_returns_typed_list(self, monkeypatch):
        monkeypatch.setattr(db, "fetch_all", lambda q, p=(): [("2026-01-01", 5)])
        result = db.compute_trend_days("2026-01-01", "2026-01-31")
        assert result == [("2026-01-01", 5)]

    def test_without_commune_passes_two_date_params(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_trend_days("2026-01-01", "2026-01-31", None)
        assert captured["p"] == ("2026-01-01", "2026-01-31")

    def test_with_commune_passes_three_params(self, monkeypatch):
        captured = {}

        def fake_fetch(q, p=()):
            captured["p"] = p
            return [("2026-01-01", 2)]

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_trend_days("2026-01-01", "2026-01-31", "Paris")
        assert "Paris" in captured["p"]
        assert len(captured["p"]) == 3

    def test_with_commune_uses_different_sql_branch(self, monkeypatch):
        """La branche WITH commune doit inclure un filtre LOWER(commune)."""
        sqls = []

        def fake_fetch(q, p=()):
            sqls.append(q)
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_trend_days("2026-01-01", "2026-01-31", "Clichy")
        assert "LOWER(commune)" in sqls[0]

    def test_without_commune_sql_has_no_commune_filter(self, monkeypatch):
        sqls = []

        def fake_fetch(q, p=()):
            sqls.append(q)
            return []

        monkeypatch.setattr(db, "fetch_all", fake_fetch)
        db.compute_trend_days("2026-01-01", "2026-01-31")
        assert "commune" not in sqls[0].lower() or "LOWER(commune)" not in sqls[0]
