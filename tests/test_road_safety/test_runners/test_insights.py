"""Unit tests for road_safety.runners.insights."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Tuple

import pytest

import road_safety.runners.insights as insights


# ---------------------------------------------------------------------------
# Fake DB helpers
# ---------------------------------------------------------------------------


@dataclass
class FakeCursor:
    rows: List[Tuple[Any, ...]]
    executed: List[Tuple[str, tuple]]

    def execute(self, query: str, params: tuple = ()):
        self.executed.append((query, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self):
        pass


@dataclass
class FakeConn:
    cursor_obj: FakeCursor

    def cursor(self):
        return self.cursor_obj

    def close(self):
        pass


def make_fake_conn(rows: List[Tuple[Any, ...]]):
    return FakeConn(cursor_obj=FakeCursor(rows=rows, executed=[]))


# ---------------------------------------------------------------------------
# Tests for find_most_dangerous_hour
# ---------------------------------------------------------------------------


class TestFindMostDangerousHour:
    def test_returns_hour_and_count(self, monkeypatch):
        conn = make_fake_conn([(17, 320)])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        result = insights.find_most_dangerous_hour()

        assert result == (17, 320)
        sql, _ = conn.cursor_obj.executed[0]
        assert "EXTRACT(HOUR" in sql
        assert "heure_acc" in sql

    def test_returns_none_when_no_data(self, monkeypatch):
        conn = make_fake_conn([])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        result = insights.find_most_dangerous_hour()

        assert result is None

    def test_raises_on_connection_failure(self, monkeypatch):
        monkeypatch.setattr(insights, "establish_connection", lambda: None)

        with pytest.raises(RuntimeError, match="Database connection failed"):
            insights.find_most_dangerous_hour()


# ---------------------------------------------------------------------------
# Tests for find_most_dangerous_weather
# ---------------------------------------------------------------------------


class TestFindMostDangerousWeather:
    def test_returns_condition_and_count(self, monkeypatch):
        conn = make_fake_conn([("Normale", 500)])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        result = insights.find_most_dangerous_weather()

        assert result == ("Normale", 500)
        sql, _ = conn.cursor_obj.executed[0]
        assert "cond_atmos" in sql

    def test_returns_none_when_no_data(self, monkeypatch):
        conn = make_fake_conn([])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        assert insights.find_most_dangerous_weather() is None


# ---------------------------------------------------------------------------
# Tests for find_most_dangerous_commune
# ---------------------------------------------------------------------------


class TestFindMostDangerousCommune:
    def test_returns_commune_and_count(self, monkeypatch):
        conn = make_fake_conn([("Reims", 1200)])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        result = insights.find_most_dangerous_commune()

        assert result == ("Reims", 1200)
        sql, _ = conn.cursor_obj.executed[0]
        assert "commune" in sql

    def test_returns_none_when_no_data(self, monkeypatch):
        conn = make_fake_conn([])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        assert insights.find_most_dangerous_commune() is None


# ---------------------------------------------------------------------------
# Tests for find_most_dangerous_intersection
# ---------------------------------------------------------------------------


class TestFindMostDangerousIntersection:
    def test_returns_intersection_and_count(self, monkeypatch):
        conn = make_fake_conn([("Hors intersection", 800)])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        result = insights.find_most_dangerous_intersection()

        assert result == ("Hors intersection", 800)
        sql, _ = conn.cursor_obj.executed[0]
        assert "intersection" in sql

    def test_returns_none_when_no_data(self, monkeypatch):
        conn = make_fake_conn([])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        assert insights.find_most_dangerous_intersection() is None


# ---------------------------------------------------------------------------
# Tests for find_most_fatal_commune
# ---------------------------------------------------------------------------


class TestFindMostFatalCommune:
    def test_returns_commune_and_fatal_count(self, monkeypatch):
        conn = make_fake_conn([("Paris", 45)])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        result = insights.find_most_fatal_commune()

        assert result == ("Paris", 45)
        sql, params = conn.cursor_obj.executed[0]
        assert "gravite_usager" in sql
        assert params == (insights.FATAL_LABEL,)

    def test_returns_none_when_no_data(self, monkeypatch):
        conn = make_fake_conn([])
        monkeypatch.setattr(insights, "establish_connection", lambda: conn)

        assert insights.find_most_fatal_commune() is None


# ---------------------------------------------------------------------------
# Tests for print_insights (output)
# ---------------------------------------------------------------------------


class TestPrintInsights:
    def test_print_insights_shows_all_sections(self, monkeypatch, capsys):
        monkeypatch.setattr(insights, "find_most_dangerous_hour", lambda: (17, 320))
        monkeypatch.setattr(
            insights, "find_most_dangerous_weather", lambda: ("Pluie", 200)
        )
        monkeypatch.setattr(
            insights, "find_most_dangerous_commune", lambda: ("Reims", 1200)
        )
        monkeypatch.setattr(insights, "find_most_fatal_commune", lambda: ("Paris", 45))
        monkeypatch.setattr(
            insights,
            "find_most_dangerous_intersection",
            lambda: ("Hors intersection", 800),
        )

        insights.print_insights()

        out = capsys.readouterr().out
        assert "Road Safety Insights" in out
        assert "17:00" in out
        assert "Pluie" in out
        assert "Reims" in out
        assert "Paris" in out
        assert "Hors intersection" in out

    def test_print_insights_handles_none_values(self, monkeypatch, capsys):
        monkeypatch.setattr(insights, "find_most_dangerous_hour", lambda: None)
        monkeypatch.setattr(insights, "find_most_dangerous_weather", lambda: None)
        monkeypatch.setattr(insights, "find_most_dangerous_commune", lambda: None)
        monkeypatch.setattr(insights, "find_most_fatal_commune", lambda: None)
        monkeypatch.setattr(insights, "find_most_dangerous_intersection", lambda: None)

        insights.print_insights()

        out = capsys.readouterr().out
        assert "N/A" in out

    def test_run_insights_calls_print_insights(self, monkeypatch, capsys):
        called = {"n": 0}
        monkeypatch.setattr(
            insights, "print_insights", lambda: called.__setitem__("n", called["n"] + 1)
        )

        insights.run_insights()

        assert called["n"] == 1
