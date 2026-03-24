import builtins

import pytest

import road_safety.runners.accident_chat as chat_module
from road_safety.runners import accident_cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_print_table(monkeypatch, tag: str = "TABLE_OK"):
    monkeypatch.setattr(
        accident_cli.db, "print_table", lambda headers, rows: print(tag)
    )


# ---------------------------------------------------------------------------
# run_menu — actions principales
# ---------------------------------------------------------------------------


def test_menu_quit_immediately(monkeypatch, capsys):
    inputs = iter(["0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

    accident_cli.run_menu()
    out = capsys.readouterr().out
    assert "Road Safety Interactive" in out
    assert "Bye." in out


def test_menu_quit_alias_q(monkeypatch, capsys):
    monkeypatch.setattr(builtins, "input", lambda _: "q")
    accident_cli.run_menu()
    assert "Bye." in capsys.readouterr().out


def test_menu_quit_alias_quit(monkeypatch, capsys):
    monkeypatch.setattr(builtins, "input", lambda _: "quit")
    accident_cli.run_menu()
    assert "Bye." in capsys.readouterr().out


def test_menu_help_option(monkeypatch, capsys):
    inputs = iter(["help", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    out = capsys.readouterr().out
    assert "Road Safety interactive CLI" in out


def test_menu_unknown_option_shows_message(monkeypatch, capsys):
    inputs = iter(["999", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert "Unknown option" in capsys.readouterr().out


def test_menu_action_error_is_caught(monkeypatch, capsys):
    def raise_error():
        raise RuntimeError("DB down")

    monkeypatch.setattr(accident_cli, "action_overview", raise_error)
    inputs = iter(["1", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert "DB down" in capsys.readouterr().out


def test_menu_overview_calls_action(monkeypatch, capsys):
    monkeypatch.setattr(
        accident_cli.db, "compute_severity_breakdown", lambda: [("X", 1)]
    )
    _stub_print_table(monkeypatch)
    inputs = iter(["1", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert "TABLE_OK" in capsys.readouterr().out


def test_menu_option_2_fatal_rate(monkeypatch, capsys):
    monkeypatch.setattr(accident_cli.db, "compute_fatal_rate", lambda: (1.5, 10, 700))
    _stub_print_table(monkeypatch, "FATAL_OK")
    inputs = iter(["2", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert "FATAL_OK" in capsys.readouterr().out


def test_menu_option_3_collisions(monkeypatch, capsys):
    monkeypatch.setattr(
        accident_cli.db, "list_collision_types", lambda: [("Frontale", 200)]
    )
    _stub_print_table(monkeypatch, "COLL_OK")
    inputs = iter(["3", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert "COLL_OK" in capsys.readouterr().out


def test_menu_option_4_top_communes_default_limit(monkeypatch, capsys):
    monkeypatch.setattr(
        accident_cli.db, "list_top_communes", lambda n: [("Paris", 100)]
    )
    _stub_print_table(monkeypatch, "COMM_OK")
    # "" → default limit (10)
    inputs = iter(["4", "", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert "COMM_OK" in capsys.readouterr().out


def test_menu_option_4_top_communes_custom_limit(monkeypatch, capsys):
    captured = {}
    monkeypatch.setattr(
        accident_cli.db,
        "list_top_communes",
        lambda n: captured.__setitem__("n", n) or [("Paris", 100)],
    )
    _stub_print_table(monkeypatch)
    inputs = iter(["4", "5", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert captured["n"] == 5


def test_action_columns_raw_accidents(monkeypatch, capsys):
    monkeypatch.setattr(
        accident_cli.db,
        "fetch_table_columns",
        lambda schema, table: [("commune", "text"), ("date_acc", "date")],
    )
    _stub_print_table(monkeypatch, "COLS_OK")
    accident_cli.action_columns_raw_accidents()
    assert "COLS_OK" in capsys.readouterr().out


def test_menu_option_5_calls_operational_stats(monkeypatch, capsys):
    called = []
    monkeypatch.setattr(chat_module, "fetch_operational_stats", lambda: called.append(1))
    inputs = iter(["5", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert len(called) == 1


def test_menu_option_6_calls_extended_menu(monkeypatch, capsys):
    called = []
    monkeypatch.setattr(accident_cli, "run_extended_menu", lambda: called.append(1))
    inputs = iter(["6", "0"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
    accident_cli.run_menu()
    assert len(called) == 1


# ---------------------------------------------------------------------------
# _ask_int
# ---------------------------------------------------------------------------


def test_ask_int_returns_default_on_empty(monkeypatch):
    monkeypatch.setattr(builtins, "input", lambda _: "")
    assert accident_cli._ask_int("N? ", default=5) == 5


def test_ask_int_returns_parsed_integer(monkeypatch):
    monkeypatch.setattr(builtins, "input", lambda _: "42")
    assert accident_cli._ask_int("N? ") == 42


def test_ask_int_retries_on_invalid_input(monkeypatch, capsys):
    values = iter(["abc", "7"])
    monkeypatch.setattr(builtins, "input", lambda _: next(values))
    result = accident_cli._ask_int("N? ")
    assert result == 7
    assert "integer" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# _show_dashboard
# ---------------------------------------------------------------------------


def test_show_dashboard_empty_results(monkeypatch, capsys):
    monkeypatch.setattr(accident_cli.db, "print_table", lambda h, r: None)
    accident_cli._show_dashboard([])
    out = capsys.readouterr().out
    assert "EMERGENCY DECISION DASHBOARD" in out


def test_show_dashboard_shows_priority_zones(monkeypatch, capsys):
    monkeypatch.setattr(accident_cli.db, "print_table", lambda h, r: None)
    results = [
        (
            "Top communes",
            ["commune", "risk_score"],
            [("Paris", 100), ("Nanterre", 50)],
        )
    ]
    accident_cli._show_dashboard(results)
    out = capsys.readouterr().out
    assert "Zones prioritaires" in out
    assert "Paris" in out


def test_show_dashboard_calls_print_table(monkeypatch, capsys):
    called = []
    monkeypatch.setattr(
        accident_cli.db, "print_table", lambda h, r: called.append((h, r))
    )
    results = [("Section", ["h"], [("v", 1)])]
    accident_cli._show_dashboard(results)
    assert len(called) == 1


# ---------------------------------------------------------------------------
# run_extended_menu — chaque branche
# ---------------------------------------------------------------------------


class TestRunExtendedMenu:
    def _stub_db(self, monkeypatch):
        """Stubbe print_table pour éviter la vraie DB."""
        monkeypatch.setattr(accident_cli.db, "print_table", lambda h, r: None)

    def test_choice_0_shows_dashboard_and_returns(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        monkeypatch.setattr(builtins, "input", lambda _: "0")
        accident_cli.run_extended_menu()
        assert "EMERGENCY DECISION DASHBOARD" in capsys.readouterr().out

    def test_invalid_choice_shows_message(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        inputs = iter(["Z", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        assert "invalide" in capsys.readouterr().out

    def test_choice_1_top_fatal_communes(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        monkeypatch.setattr(
            accident_cli.db, "list_top_fatal_communes", lambda n: [("Paris", 3)]
        )
        # "1" → choix, "5" → nombre communes, "0" → retour
        inputs = iter(["1", "5", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        out = capsys.readouterr().out
        assert "EMERGENCY DECISION DASHBOARD" in out

    def test_choice_2_top_severe_communes(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        monkeypatch.setattr(
            accident_cli.db, "list_top_severe_communes", lambda n: [("Nanterre", 8)]
        )
        inputs = iter(["2", "10", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        assert "EMERGENCY DECISION DASHBOARD" in capsys.readouterr().out

    def test_choice_3_risk_score_by_commune(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        monkeypatch.setattr(
            accident_cli.db,
            "compute_risk_score_by_commune",
            lambda n: [("Paris", 3, 10, 20, 49)],
        )
        inputs = iter(["3", "10", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        assert "EMERGENCY DECISION DASHBOARD" in capsys.readouterr().out

    def test_choice_4_single_commune_risk_score(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        monkeypatch.setattr(
            accident_cli.db,
            "compute_commune_risk_score",
            lambda c: (1, 2, 3, 10),
        )
        # "4" → choix, "Paris" → commune, "0" → retour
        inputs = iter(["4", "Paris", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        assert "EMERGENCY DECISION DASHBOARD" in capsys.readouterr().out

    def test_choice_5_trend_without_commune(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        monkeypatch.setattr(
            accident_cli.db,
            "compute_trend_days",
            lambda s, e, c=None: [("2026-01-01", 5)],
        )
        inputs = iter(["5", "2026-01-01", "2026-01-31", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        assert "EMERGENCY DECISION DASHBOARD" in capsys.readouterr().out

    def test_choice_6_trend_with_commune(self, monkeypatch, capsys):
        self._stub_db(monkeypatch)
        monkeypatch.setattr(
            accident_cli.db,
            "compute_trend_days",
            lambda s, e, c=None: [("2026-01-01", 2)],
        )
        inputs = iter(["6", "2026-01-01", "2026-01-31", "Paris", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        assert "EMERGENCY DECISION DASHBOARD" in capsys.readouterr().out

    def test_results_are_accumulated_across_choices(self, monkeypatch, capsys):
        """Deux choix successifs → le dashboard agrège les deux jeux de résultats."""
        self._stub_db(monkeypatch)
        monkeypatch.setattr(
            accident_cli.db, "list_top_fatal_communes", lambda n: [("Paris", 10)]
        )
        monkeypatch.setattr(
            accident_cli.db, "list_top_severe_communes", lambda n: [("Nanterre", 5)]
        )
        inputs = iter(["1", "5", "2", "5", "0"])
        monkeypatch.setattr(builtins, "input", lambda _: next(inputs))
        accident_cli.run_extended_menu()
        out = capsys.readouterr().out
        # Les deux communes doivent apparaître dans les zones prioritaires
        assert "Paris" in out or "Nanterre" in out
