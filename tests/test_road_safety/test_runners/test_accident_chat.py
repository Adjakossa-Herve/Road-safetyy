import builtins
import pytest
from dataclasses import dataclass
from typing import Any, List, Tuple


import road_safety.runners.accident_chat as chat


# ---------------------------------------------------------------------
# Fake DB objects (no real PostgreSQL required)
# ---------------------------------------------------------------------


@dataclass
class FakeCursor:
    rows: List[Tuple[Any, ...]]
    executed: List[Tuple[str, tuple]]

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


def make_fake_conn(rows: List[Tuple[Any, ...]]):
    cur = FakeCursor(rows=rows, executed=[])
    return FakeConn(cursor_obj=cur)


# ---------------------------------------------------------------------
# Introspection tests
# ---------------------------------------------------------------------


class TestFetchTableColumns:
    def test_fetch_table_columns_executes_expected_sql(self, monkeypatch):
        conn = make_fake_conn([("col1", "text"), ("col2", "integer")])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        result = chat.fetch_table_columns("raw", "accidents")

        assert result == [("col1", "text"), ("col2", "integer")]
        assert len(conn.cursor_obj.executed) == 1
        sql, params = conn.cursor_obj.executed[0]
        assert "information_schema.columns" in sql
        assert params == ("raw", "accidents")


# ---------------------------------------------------------------------
# Compute/list function tests
# ---------------------------------------------------------------------


class TestComputeSeverityBreakdown:
    def test_compute_severity_breakdown_returns_typed_values(self, monkeypatch):
        conn = make_fake_conn([("Tué", 5), ("Blessé léger", 10)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_severity_breakdown()

        assert rows == [("Tué", 5), ("Blessé léger", 10)]
        sql, _ = conn.cursor_obj.executed[0]
        assert "GROUP BY" in sql
        assert "gravite_usager" in sql


class TestComputeFatalRate:
    def test_compute_fatal_rate_returns_tuple(self, monkeypatch):
        conn = make_fake_conn([(1.234, 7, 567)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rate, fatalities, total = chat.compute_fatal_rate()

        assert rate == 1.234
        assert fatalities == 7
        assert total == 567
        sql, _ = conn.cursor_obj.executed[0]
        assert "fatal_rate_percent" in sql or "ROUND" in sql


class TestListTopCommunes:
    def test_list_top_communes_uses_limit_param(self, monkeypatch):
        conn = make_fake_conn([("Paris", 100), ("Reims", 50)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.list_top_communes(10)

        assert rows[0] == ("Paris", 100)
        sql, params = conn.cursor_obj.executed[0]
        assert "LIMIT %s" in sql
        assert params == (10,)


class TestComputeCommuneKpis:
    def test_compute_commune_kpis_filters_case_insensitive(self, monkeypatch):
        conn = make_fake_conn([(200, 3, 20)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        total, fatalities, severe = chat.compute_commune_kpis("Paris")

        assert (total, fatalities, severe) == (200, 3, 20)
        sql, params = conn.cursor_obj.executed[0]
        assert "LOWER(commune) = LOWER(%s)" in sql
        assert params[-1] == "Paris"


class TestComputeRiskScoreByCommune:
    def test_compute_risk_score_by_commune_returns_expected_shape(self, monkeypatch):
        conn = make_fake_conn([("Paris", 3, 10, 20, 49)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_risk_score_by_commune(5)

        assert rows == [("Paris", 3, 10, 20, 49)]
        sql, params = conn.cursor_obj.executed[0]
        assert "risk_score" in sql
        assert params[-1] == 5


class TestComputeCommuneRiskScore:
    def test_compute_commune_risk_score_returns_tuple(self, monkeypatch):
        conn = make_fake_conn([(1, 2, 3, 10)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        fatalities, severe, light, score = chat.compute_commune_risk_score("Paris")

        assert (fatalities, severe, light, score) == (1, 2, 3, 10)
        sql, params = conn.cursor_obj.executed[0]
        assert "WHERE LOWER(commune) = LOWER(%s)" in sql
        assert params[-1] == "Paris"


class TestComputeTrendDays:
    def test_compute_trend_days_without_commune(self, monkeypatch):
        conn = make_fake_conn([("2026-01-01", 10), ("2026-01-02", 12)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_trend_days("2026-01-01", "2026-01-31", None)

        assert rows == [("2026-01-01", 10), ("2026-01-02", 12)]
        sql, params = conn.cursor_obj.executed[0]
        assert params == ("2026-01-01", "2026-01-31")
        assert "GROUP BY" in sql

    def test_compute_trend_days_with_commune(self, monkeypatch):
        conn = make_fake_conn([("2026-01-01", 2)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_trend_days("2026-01-01", "2026-01-31", "Paris")

        assert rows == [("2026-01-01", 2)]
        sql, params = conn.cursor_obj.executed[0]
        assert params == ("2026-01-01", "2026-01-31", "Paris")
        assert "LOWER(commune) = LOWER(%s)" in sql


# ---------------------------------------------------------------------
# Wrapper (q_*) output tests (prints)
# ---------------------------------------------------------------------


class TestQFunctionsOutput:
    def test_q_overview_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "compute_severity_breakdown", lambda: [("Tué", 1)])
        chat.q_overview()
        out = capsys.readouterr().out
        assert "gravite_usager" in out
        assert "Tué" in out

    def test_q_risk_score_commune_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "compute_commune_risk_score", lambda _: (1, 2, 3, 10))
        chat.q_risk_score_commune("Paris")
        out = capsys.readouterr().out
        assert "risk_score" in out
        assert "Paris" in out


# ---------------------------------------------------------------------
# REPL routing tests (run_chat)
# ---------------------------------------------------------------------


class TestRunChatRouting:
    def test_run_chat_routes_fixed_commands(self, monkeypatch):
        called = {"overview": 0, "fatal": 0}

        monkeypatch.setattr(
            chat,
            "q_overview",
            lambda: called.__setitem__("overview", called["overview"] + 1),
        )
        monkeypatch.setattr(
            chat,
            "q_fatal_rate",
            lambda: called.__setitem__("fatal", called["fatal"] + 1),
        )

        inputs = iter(["overview", "fatal_rate", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

        chat.run_chat()

        assert called["overview"] == 1
        assert called["fatal"] == 1

    def test_run_chat_routes_parameterized_commands(self, monkeypatch):
        called = {
            "top": 0,
            "top_fatal": 0,
            "top_severe": 0,
            "top_risk": 0,
            "stats": 0,
            "risk_commune": 0,
            "trend": 0,
            "cols": 0,
        }

        monkeypatch.setattr(
            chat,
            "q_top_communes",
            lambda n: called.__setitem__("top", called["top"] + n),
        )
        monkeypatch.setattr(
            chat,
            "q_top_fatal_communes",
            lambda n: called.__setitem__("top_fatal", called["top_fatal"] + n),
        )
        monkeypatch.setattr(
            chat,
            "q_top_severe_communes",
            lambda n: called.__setitem__("top_severe", called["top_severe"] + n),
        )
        monkeypatch.setattr(
            chat,
            "q_risk_score_communes",
            lambda n: called.__setitem__("top_risk", called["top_risk"] + n),
        )
        monkeypatch.setattr(
            chat,
            "q_stats_commune",
            lambda c: called.__setitem__(
                "stats", called["stats"] + (1 if c == "Paris" else 0)
            ),
        )
        monkeypatch.setattr(
            chat,
            "q_risk_score_commune",
            lambda c: called.__setitem__(
                "risk_commune", called["risk_commune"] + (1 if c == "Paris" else 0)
            ),
        )
        monkeypatch.setattr(
            chat,
            "q_trend_days",
            lambda d1, d2, c: called.__setitem__(
                "trend",
                called["trend"]
                + (1 if (d1, d2, c) == ("2026-01-01", "2026-01-31", "Paris") else 0),
            ),
        )
        monkeypatch.setattr(
            chat,
            "q_columns",
            lambda s, t: called.__setitem__(
                "cols", called["cols"] + (1 if (s, t) == ("raw", "accidents") else 0)
            ),
        )

        inputs = iter(
            [
                "top_communes 10",
                "top_fatal_communes 3",
                "top_severe_communes 5",
                "risk_score_communes 8",
                "stats commune Paris",
                "risk_score commune Paris",
                "trend_days 2026-01-01 2026-01-31 commune Paris",
                "columns raw accidents",
                "exit",
            ]
        )
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

        chat.run_chat()

        assert called["top"] == 10
        assert called["top_fatal"] == 3
        assert called["top_severe"] == 5
        assert called["top_risk"] == 8
        assert called["stats"] == 1
        assert called["risk_commune"] == 1
        assert called["trend"] == 1
        assert called["cols"] == 1

    def test_run_chat_unknown_command_prints_message(self, monkeypatch, capsys):
        inputs = iter(["unknown_cmd", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

        chat.run_chat()
        out = capsys.readouterr().out
        assert "Unknown command" in out

    def test_run_chat_extended_disabled(self, monkeypatch, capsys):
        """Extended commands sont toujours disponibles (flag supprimé)."""
        called = {"top_fatal": 0, "top_risk": 0}
        monkeypatch.setattr(
            chat, "q_top_fatal_communes", lambda n: called.__setitem__("top_fatal", n)
        )
        monkeypatch.setattr(
            chat, "q_risk_score_communes", lambda n: called.__setitem__("top_risk", n)
        )
        inputs = iter(["top_fatal_communes 3", "risk_score_communes 5", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

        chat.run_chat()

        assert called["top_fatal"] == 3
        assert called["top_risk"] == 5


# ---------------------------------------------------------------------
# Additional compute function tests for newer commands
# ---------------------------------------------------------------------


class TestComputeHourlyDistribution:
    def test_compute_hourly_distribution_executes_expected_sql(self, monkeypatch):
        conn = make_fake_conn([(0, 5), (1, 7)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_hourly_distribution()

        assert rows == [(0, 5), (1, 7)]
        sql, _ = conn.cursor_obj.executed[0]
        assert "EXTRACT" in sql and "NULLIF" in sql and "heure_acc" in sql


class TestComputeDayVsNightStats:
    def test_compute_day_vs_night_stats_executes_expected_sql(self, monkeypatch):
        conn = make_fake_conn([("DAY", 10, 2, 3)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_day_vs_night_stats()

        assert rows == [("DAY", 10, 2, 3)]
        sql, params = conn.cursor_obj.executed[0]
        assert "luminosite" in sql
        assert params == (chat.FATAL_LABEL, chat.FATAL_LABEL, chat.SEVERE_LABEL)


class TestComputeMonthlyDistribution:
    def test_compute_monthly_distribution_executes_expected_sql(self, monkeypatch):
        conn = make_fake_conn([("2026-01", 20)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_monthly_distribution()

        assert rows == [("2026-01", 20)]
        sql, _ = conn.cursor_obj.executed[0]
        assert "TO_CHAR" in sql and "YYYY-MM" in sql


class TestComputeWeekendSeverityGap:
    def test_compute_weekend_severity_gap_executes_expected_sql(self, monkeypatch):
        conn = make_fake_conn([("WEEKEND", 30, 5, 10)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)

        rows = chat.compute_weekend_severity_gap()

        assert rows == [("WEEKEND", 30, 5, 10)]
        sql, params = conn.cursor_obj.executed[0]
        assert "EXTRACT(DOW" in sql or "WEEKEND" in sql
        assert params == (chat.FATAL_LABEL, chat.FATAL_LABEL, chat.SEVERE_LABEL)


class TestQWrapperAdditional:
    def test_q_by_hour_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "compute_hourly_distribution", lambda: [(0, 1)])
        chat.q_by_hour()
        out = capsys.readouterr().out
        assert "hour" in out
        assert "0" in out

    def test_q_day_vs_night_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(
            chat, "compute_day_vs_night_stats", lambda: [("DAY", 1, 0, 0)]
        )
        chat.q_day_vs_night()
        out = capsys.readouterr().out
        assert "luminosite" in out
        assert "DAY" in out


# =====================================================================
# Additional tests for 100% coverage
# =====================================================================


class TestFetchAllNoConnection:
    def test_raises_runtime_error_when_no_connection(self, monkeypatch):
        monkeypatch.setattr(chat, "establish_connection", lambda: None)
        with pytest.raises(RuntimeError, match="Database connection failed"):
            chat.fetch_all("SELECT 1")


class TestPrintTableEmpty:
    def test_prints_no_results_for_empty_rows(self, capsys):
        chat.print_table(["col1", "col2"], [])
        out = capsys.readouterr().out
        assert "(no results)" in out


class TestPrintKv:
    def test_prints_key_value_rows(self, capsys):
        chat.print_kv("Stats:", [("clé1", "val1"), ("clé2", "val2")])
        out = capsys.readouterr().out
        assert "Stats:" in out
        assert "clé1: val1" in out


class TestListCollisionTypes:
    def test_returns_collision_type_and_count(self, monkeypatch):
        conn = make_fake_conn([("Frontal", 100)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)
        rows = chat.list_collision_types()
        assert rows == [("Frontal", 100)]


class TestListGravityValues:
    def test_returns_gravity_values_with_limit(self, monkeypatch):
        conn = make_fake_conn([("Tue", 50), ("Leger", 200)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)
        rows = chat.list_gravity_values(10)
        assert rows == [("Tue", 50), ("Leger", 200)]
        _, params = conn.cursor_obj.executed[0]
        assert params == (10,)


class TestListTopFatalCommunes:
    def test_returns_fatal_communes(self, monkeypatch):
        conn = make_fake_conn([("Paris", 20)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)
        rows = chat.list_top_fatal_communes(5)
        assert rows == [("Paris", 20)]


class TestListTopSevereCommunes:
    def test_returns_severe_communes(self, monkeypatch):
        conn = make_fake_conn([("Lyon", 30)])
        monkeypatch.setattr(chat, "establish_connection", lambda: conn)
        rows = chat.list_top_severe_communes(5)
        assert rows == [("Lyon", 30)]


class TestQWrappersAll:
    def test_q_gravity_values_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "list_gravity_values", lambda n: [("Tue", 5)])
        chat.q_gravity_values(5)
        out = capsys.readouterr().out
        assert "gravite_usager" in out
        assert "Tue" in out

    def test_q_by_month_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(
            chat, "compute_monthly_distribution", lambda: [("2026-01", 10)]
        )
        chat.q_by_month()
        out = capsys.readouterr().out
        assert "month" in out
        assert "2026-01" in out

    def test_q_weekend_vs_week_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(
            chat, "compute_weekend_severity_gap", lambda: [("WEEKEND", 30, 5, 10)]
        )
        chat.q_weekend_vs_week()
        out = capsys.readouterr().out
        assert "period" in out
        assert "WEEKEND" in out

    def test_q_top_communes_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "list_top_communes", lambda n: [("Paris", 100)])
        chat.q_top_communes(5)
        out = capsys.readouterr().out
        assert "commune" in out
        assert "Paris" in out

    def test_q_stats_commune_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "compute_commune_kpis", lambda c: (100, 5, 20))
        chat.q_stats_commune("Paris")
        out = capsys.readouterr().out
        assert "Paris" in out
        assert "100" in out

    def test_q_top_fatal_communes_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "list_top_fatal_communes", lambda n: [("Paris", 10)])
        chat.q_top_fatal_communes(5)
        out = capsys.readouterr().out
        assert "fatalities" in out
        assert "Paris" in out

    def test_q_top_severe_communes_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(
            chat, "list_top_severe_communes", lambda n: [("Lyon", 15)]
        )
        chat.q_top_severe_communes(5)
        out = capsys.readouterr().out
        assert "severe_accidents" in out
        assert "Lyon" in out

    def test_q_risk_score_communes_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(
            chat,
            "compute_risk_score_by_commune",
            lambda n: [("Paris", 10, 20, 30, 100)],
        )
        chat.q_risk_score_communes(5)
        out = capsys.readouterr().out
        assert "risk_score" in out
        assert "Paris" in out

    def test_q_trend_days_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(
            chat, "compute_trend_days", lambda d1, d2, c: [("2026-01-05", 3)]
        )
        chat.q_trend_days("2026-01-01", "2026-01-31", None)
        out = capsys.readouterr().out
        assert "day" in out
        assert "2026-01-05" in out

    def test_q_columns_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(
            chat, "fetch_table_columns", lambda s, t: [("id", "integer")]
        )
        chat.q_columns("raw", "accidents")
        out = capsys.readouterr().out
        assert "column_name" in out
        assert "id" in out


class TestFetchOperationalStats:
    def test_runs_all_ten_sections(self, monkeypatch, capsys):
        call_count = [0]

        def fake_fetch_all(query, params=()):
            call_count[0] += 1
            if call_count[0] == 10:
                # moyennes section expects 5-element tuple
                return [(1.5, 2.0, 30.0, 0.5, 0.3)]
            return [("Value", 42)]

        monkeypatch.setattr(chat, "fetch_all", fake_fetch_all)
        monkeypatch.setattr(chat, "print_table", lambda h, r: None)

        chat.fetch_operational_stats()

        out = capsys.readouterr().out
        assert "Usagers/accident" in out
        assert call_count[0] == 10


class TestQStatsUrgences:
    def test_calls_fetch_operational_stats(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            chat, "fetch_operational_stats", lambda: called.append(True)
        )
        chat.q_stats_urgences()
        assert called == [True]


class TestRunChatAdditionalBranches:
    def test_skips_empty_input(self, monkeypatch, capsys):
        inputs = iter(["", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        out = capsys.readouterr().out
        assert "Bye." in out

    def test_prints_help_text(self, monkeypatch, capsys):
        inputs = iter(["help", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        out = capsys.readouterr().out
        assert "Road Safety interactive CLI" in out

    def test_by_hour_branch(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "q_by_hour", lambda: print("by_hour_ok"))
        inputs = iter(["by_hour", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "by_hour_ok" in capsys.readouterr().out

    def test_day_vs_night_branch(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "q_day_vs_night", lambda: print("day_vs_night_ok"))
        inputs = iter(["day_vs_night", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "day_vs_night_ok" in capsys.readouterr().out

    def test_by_month_branch(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "q_by_month", lambda: print("by_month_ok"))
        inputs = iter(["by_month", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "by_month_ok" in capsys.readouterr().out

    def test_weekend_vs_week_branch(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "q_weekend_vs_week", lambda: print("weekend_ok"))
        inputs = iter(["weekend_vs_week", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "weekend_ok" in capsys.readouterr().out

    def test_stats_urgences_branch(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "q_stats_urgences", lambda: print("urgences_ok"))
        inputs = iter(["stats_urgences", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "urgences_ok" in capsys.readouterr().out

    def test_menu_branch(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "run_menu", lambda: print("menu_ok"))
        inputs = iter(["menu", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "menu_ok" in capsys.readouterr().out

    def test_gravity_values_command(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "q_gravity_values", lambda n: print(f"grav_{n}"))
        inputs = iter(["gravity_values 7", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "grav_7" in capsys.readouterr().out

    def test_trend_days_without_commune(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            chat,
            "q_trend_days",
            lambda d1, d2, c: captured.update({"commune": c}),
        )
        inputs = iter(["trend_days 2026-01-01 2026-01-31", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert captured.get("commune") is None

    def test_collisions_branch(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "q_collisions", lambda: print("collisions_ok"))
        inputs = iter(["collisions", "exit"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        chat.run_chat()
        assert "collisions_ok" in capsys.readouterr().out


class TestQFatalRateAndCollisions:
    def test_q_fatal_rate_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "compute_fatal_rate", lambda: (1.5, 3, 200))
        chat.q_fatal_rate()
        out = capsys.readouterr().out
        assert "fatal_rate_%" in out
        assert "1.5" in out

    def test_q_collisions_prints_table(self, monkeypatch, capsys):
        monkeypatch.setattr(chat, "list_collision_types", lambda: [("Frontal", 50)])
        chat.q_collisions()
        out = capsys.readouterr().out
        assert "type_collision" in out
        assert "Frontal" in out
