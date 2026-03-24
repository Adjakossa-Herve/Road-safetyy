from __future__ import annotations

import json
from pathlib import Path

import pytest

import road_safety.runners.pulse as pulse_mod


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []
        self.closed = False

    def execute(self, query: str, params: tuple = ()):
        self.executed.append((query, params))

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, rows):
        self.cursor_obj = FakeCursor(rows=rows)
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def close(self):
        self.closed = True


def test_fetch_monthly_totals_raises_when_no_connection(monkeypatch):
    monkeypatch.setattr(pulse_mod, "establish_connection", lambda: None)

    with pytest.raises(RuntimeError, match="Database connection failed"):
        pulse_mod.fetch_monthly_totals()


def test_fetch_monthly_totals_returns_chronological_values(monkeypatch):
    conn = FakeConn(rows=[("2010-03", 30), ("2010-02", 20), ("2010-01", 10)])
    monkeypatch.setattr(pulse_mod, "establish_connection", lambda: conn)

    result = pulse_mod.fetch_monthly_totals(limit_months=3)

    assert result == [("2010-01", 10), ("2010-02", 20), ("2010-03", 30)]
    sql, params = conn.cursor_obj.executed[0]
    assert "GROUP BY ym" in sql
    assert params == (3,)
    assert conn.closed is True


def test_fetch_top_rising_communes_raises_when_no_connection(monkeypatch):
    monkeypatch.setattr(pulse_mod, "establish_connection", lambda: None)

    with pytest.raises(RuntimeError, match="Database connection failed"):
        pulse_mod.fetch_top_rising_communes()


def test_fetch_top_rising_communes_returns_typed_rows(monkeypatch):
    rows = [("Paris", 50, 40, 10), ("Lyon", 10, 20, -10)]
    conn = FakeConn(rows=rows)
    monkeypatch.setattr(pulse_mod, "establish_connection", lambda: conn)

    result = pulse_mod.fetch_top_rising_communes(limit=2)

    assert result == [("Paris", 50, 40, 10), ("Lyon", 10, 20, -10)]
    sql, params = conn.cursor_obj.executed[0]
    assert "FULL OUTER JOIN previous" in sql
    assert params == (2,)
    assert conn.closed is True


def test_sparkline_handles_empty_input():
    assert pulse_mod._sparkline([]) == "(no data)"


def test_sparkline_handles_flat_series():
    assert pulse_mod._sparkline([7, 7, 7]) == "==="


def test_sparkline_handles_variable_series():
    s = pulse_mod._sparkline([10, 20, 15, 30])
    assert len(s) == 4
    assert s != "===="


def test_forecast_next_handles_empty_and_single_point():
    assert pulse_mod._forecast_next([]) == 0
    assert pulse_mod._forecast_next([42]) == 42


def test_forecast_next_clamps_to_zero_on_negative_drift():
    assert pulse_mod._forecast_next([10, 0]) == 0


def test_build_snapshot_handles_no_data():
    snapshot = pulse_mod.build_snapshot(monthly=[], rising=[])

    assert snapshot["latest_month"] == "n/a"
    assert snapshot["latest_total"] == 0
    assert snapshot["trend"] == "stable"
    assert snapshot["forecast_next_month"] == 0
    assert snapshot["sparkline"] == "(no data)"
    assert snapshot["monthly"] == []
    assert snapshot["rising_communes"] == []
    assert "generated_at" in snapshot


def test_build_snapshot_computes_trend_and_momentum():
    snapshot = pulse_mod.build_snapshot(
        monthly=[("2010-01", 100), ("2010-02", 120)],
        rising=[("Paris", 15, 10, 5)],
    )

    assert snapshot["latest_month"] == "2010-02"
    assert snapshot["latest_total"] == 120
    assert snapshot["trend"] == "up"
    assert snapshot["delta_vs_previous_month"] == 20
    assert snapshot["momentum_pct"] == 20.0
    assert snapshot["forecast_next_month"] == 140
    assert snapshot["rising_communes"][0]["commune"] == "Paris"


def test_build_snapshot_handles_downward_trend():
    snapshot = pulse_mod.build_snapshot(
        monthly=[("2010-01", 120), ("2010-02", 100)],
        rising=[],
    )

    assert snapshot["trend"] == "down"
    assert snapshot["delta_vs_previous_month"] == -20
    assert snapshot["momentum_pct"] == -16.67


def test_load_history_returns_empty_when_file_missing(tmp_path):
    missing = tmp_path / "missing.jsonl"
    assert pulse_mod.load_history(str(missing)) == []


def test_load_history_skips_invalid_lines(tmp_path):
    history_file = tmp_path / "pulse_history.jsonl"
    history_file.write_text(
        "\n".join(
            [
                json.dumps({"latest_total": 10}),
                "{invalid json}",
                "   ",
                json.dumps({"latest_total": 12}),
            ]
        ),
        encoding="utf-8",
    )

    history = pulse_mod.load_history(str(history_file))

    assert history == [{"latest_total": 10}, {"latest_total": 12}]


def test_save_snapshot_appends_json_line(tmp_path):
    history_file = tmp_path / "nested" / "pulse_history.jsonl"
    pulse_mod.save_snapshot(str(history_file), {"latest_total": 99})

    lines = history_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["latest_total"] == 99


def test_compare_to_previous_returns_baseline():
    comparison = pulse_mod.compare_to_previous([], {"latest_total": 5})
    assert comparison == {
        "run_index": 1,
        "delta_vs_previous_run": 0,
        "status": "baseline",
    }


@pytest.mark.parametrize(
    "previous_total,current_total,status,delta",
    [
        (10, 15, "higher", 5),
        (15, 10, "lower", -5),
        (10, 10, "equal", 0),
    ],
)
def test_compare_to_previous_statuses(previous_total, current_total, status, delta):
    history = [{"latest_total": previous_total}]
    snapshot = {"latest_total": current_total}

    comparison = pulse_mod.compare_to_previous(history, snapshot)

    assert comparison["run_index"] == 2
    assert comparison["status"] == status
    assert comparison["delta_vs_previous_run"] == delta


def test_render_pulse_report_with_no_rising_communes(tmp_path):
    snapshot = {
        "latest_month": "2010-02",
        "latest_total": 120,
        "trend": "up",
        "delta_vs_previous_month": 20,
        "momentum_pct": 20.0,
        "forecast_next_month": 140,
        "sparkline": ".-#",
        "rising_communes": [],
    }
    comparison = {"status": "higher", "delta_vs_previous_run": 5}
    report = pulse_mod.render_pulse_report(snapshot, comparison, str(tmp_path / "h.jsonl"))

    assert "ROAD SAFETY PULSE" in report
    assert "no rising commune found" in report
    assert "History snapshot appended to:" in report


def test_render_pulse_report_with_rising_communes(tmp_path):
    snapshot = {
        "latest_month": "2010-02",
        "latest_total": 120,
        "trend": "up",
        "delta_vs_previous_month": 20,
        "momentum_pct": 20.0,
        "forecast_next_month": 140,
        "sparkline": ".-#",
        "rising_communes": [
            {
                "commune": "Paris",
                "recent_count": 15,
                "previous_count": 10,
                "delta": 5,
            }
        ],
    }
    comparison = {"status": "equal", "delta_vs_previous_run": 0}
    report = pulse_mod.render_pulse_report(snapshot, comparison, str(tmp_path / "h.jsonl"))

    assert "Paris: 15 (prev 10, delta +5)" in report


def test_run_pulse_handles_runtime_error(monkeypatch, capsys):
    monkeypatch.setattr(
        pulse_mod,
        "fetch_monthly_totals",
        lambda limit_months: (_ for _ in ()).throw(RuntimeError("db down")),
    )

    pulse_mod.run_pulse(history_path="/tmp/ignored.jsonl", months=12, top=5)

    out = capsys.readouterr().out
    assert "⚠️" in out
    assert "db down" in out


def test_run_pulse_success_path(tmp_path, monkeypatch, capsys):
    history_file = tmp_path / "pulse_history.jsonl"
    monkeypatch.setattr(
        pulse_mod, "fetch_monthly_totals", lambda limit_months: [("2010-01", 10), ("2010-02", 12)]
    )
    monkeypatch.setattr(
        pulse_mod, "fetch_top_rising_communes", lambda limit: [("Paris", 6, 4, 2)]
    )
    monkeypatch.setattr(pulse_mod, "load_history", lambda history_path: [{"latest_total": 11}])

    pulse_mod.run_pulse(history_path=str(history_file), months=12, top=5)

    out = capsys.readouterr().out
    assert "Building dynamic pulse" in out
    assert "ROAD SAFETY PULSE" in out
    assert history_file.exists()
    assert Path(history_file).read_text(encoding="utf-8").strip() != ""
