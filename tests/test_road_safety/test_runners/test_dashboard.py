"""Unit tests for road_safety.runners.dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Tuple


import road_safety.runners.dashboard as dashboard


# ---------------------------------------------------------------------------
# Fake DB helpers
# ---------------------------------------------------------------------------


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
    return FakeConn(cursor_obj=FakeCursor(rows=rows, executed=[]))


# ---------------------------------------------------------------------------
# Tests for fetch_accidents_by_year
# ---------------------------------------------------------------------------


class TestFetchAccidentsByYear:
    def test_returns_year_and_count(self, monkeypatch):
        conn = make_fake_conn([("2021", 500), ("2022", 600)])

        def fake_establish():
            return conn

        monkeypatch.setattr(
            "road_safety.data_access.utils.establish_connection",
            fake_establish,
        )
        monkeypatch.setattr(
            dashboard, "_fetch_all", lambda q, p=(): [("2021", 500), ("2022", 600)]
        )

        result = dashboard.fetch_accidents_by_year()

        assert result == [("2021", 500), ("2022", 600)]

    def test_sql_contains_year_grouping(self, monkeypatch):
        queries = []

        def capture_fetch(query, params=()):
            queries.append(query)
            return [("2021", 100)]

        monkeypatch.setattr(dashboard, "_fetch_all", capture_fetch)

        dashboard.fetch_accidents_by_year()

        assert queries
        assert "YYYY" in queries[0]
        assert "date_acc" in queries[0]


# ---------------------------------------------------------------------------
# Tests for fetch_accidents_by_commune
# ---------------------------------------------------------------------------


class TestFetchAccidentsByCommune:
    def test_returns_commune_and_count(self, monkeypatch):
        monkeypatch.setattr(
            dashboard, "_fetch_all", lambda q, p=(): [("Reims", 1200), ("Paris", 900)]
        )

        result = dashboard.fetch_accidents_by_commune(limit=2)

        assert result == [("Reims", 1200), ("Paris", 900)]

    def test_limit_passed_to_query(self, monkeypatch):
        captured = {}

        def capture(query, params=()):
            captured["params"] = params
            return [("Reims", 1200)]

        monkeypatch.setattr(dashboard, "_fetch_all", capture)

        dashboard.fetch_accidents_by_commune(limit=5)

        assert captured["params"] == (5,)


# ---------------------------------------------------------------------------
# Tests for fetch_accidents_by_hour
# ---------------------------------------------------------------------------


class TestFetchAccidentsByHour:
    def test_returns_hour_and_count(self, monkeypatch):
        monkeypatch.setattr(
            dashboard, "_fetch_all", lambda q, p=(): [(8, 100), (17, 200)]
        )

        result = dashboard.fetch_accidents_by_hour()

        assert result == [(8, 100), (17, 200)]

    def test_sql_extracts_hour(self, monkeypatch):
        queries = []

        def capture(query, params=()):
            queries.append(query)
            return [(8, 100)]

        monkeypatch.setattr(dashboard, "_fetch_all", capture)
        dashboard.fetch_accidents_by_hour()

        assert "EXTRACT(HOUR" in queries[0]
        assert "heure_acc" in queries[0]


# ---------------------------------------------------------------------------
# Tests for fetch_accidents_by_weather
# ---------------------------------------------------------------------------


class TestFetchAccidentsByWeather:
    def test_returns_weather_and_count(self, monkeypatch):
        monkeypatch.setattr(
            dashboard, "_fetch_all", lambda q, p=(): [("Normale", 500), ("Pluie", 300)]
        )

        result = dashboard.fetch_accidents_by_weather()

        assert result == [("Normale", 500), ("Pluie", 300)]

    def test_sql_uses_cond_atmos(self, monkeypatch):
        queries = []

        def capture(query, params=()):
            queries.append(query)
            return [("Normale", 500)]

        monkeypatch.setattr(dashboard, "_fetch_all", capture)
        dashboard.fetch_accidents_by_weather()

        assert "cond_atmos" in queries[0]


# ---------------------------------------------------------------------------
# Tests for fetch_severity_distribution
# ---------------------------------------------------------------------------


class TestFetchSeverityDistribution:
    def test_returns_severity_and_count(self, monkeypatch):
        monkeypatch.setattr(
            dashboard,
            "_fetch_all",
            lambda q, p=(): [("Blessee Leger", 800), ("Tue", 50)],
        )

        result = dashboard.fetch_severity_distribution()

        assert result == [("Blessee Leger", 800), ("Tue", 50)]

    def test_sql_uses_gravite_usager(self, monkeypatch):
        queries = []

        def capture(query, params=()):
            queries.append(query)
            return [("Tue", 50)]

        monkeypatch.setattr(dashboard, "_fetch_all", capture)
        dashboard.fetch_severity_distribution()

        assert "gravite_usager" in queries[0]


# ---------------------------------------------------------------------------
# Tests for _fetch_all (data-layer helper)
# ---------------------------------------------------------------------------


class TestFetchAllHelper:
    def test_returns_rows_from_fake_connection(self, monkeypatch):
        from dataclasses import dataclass
        from typing import Any, List, Tuple

        @dataclass
        class _Cur:
            _rows: List[Tuple[Any, ...]]
            executed: List = None

            def __post_init__(self):
                if self.executed is None:
                    self.executed = []

            def execute(self, q, p=()):
                self.executed.append((q, p))

            def fetchall(self):
                return self._rows

            def close(self):
                pass

        @dataclass
        class _Conn:
            cur: _Cur

            def cursor(self):
                return self.cur

            def close(self):
                pass

        conn = _Conn(cur=_Cur(_rows=[("2026", 100)]))
        monkeypatch.setattr(
            "road_safety.data_access.utils.establish_connection", lambda: conn
        )
        # Import the local helper via module path
        import importlib
        import road_safety.data_access.utils as utils_mod

        monkeypatch.setattr(utils_mod, "establish_connection", lambda: conn)
        result = dashboard._fetch_all("SELECT 1")
        assert result == [("2026", 100)]

    def test_raises_runtime_error_when_no_connection(self, monkeypatch):
        import road_safety.data_access.utils as utils_mod

        monkeypatch.setattr(utils_mod, "establish_connection", lambda: None)

        import pytest

        with pytest.raises(RuntimeError, match="Database connection failed"):
            dashboard._fetch_all("SELECT 1")


# ---------------------------------------------------------------------------
# Tests for _render_dashboard
# ---------------------------------------------------------------------------


class TestRenderDashboard:
    def _make_st_mock(self):
        from unittest.mock import MagicMock
        mock_st = MagicMock()
        mock_st.columns.return_value = (MagicMock(), MagicMock())
        return mock_st

    def _swap_modules(self, mock_st, mock_pd):
        import sys
        orig = {k: sys.modules.get(k) for k in ("streamlit", "pandas")}
        sys.modules["streamlit"] = mock_st
        sys.modules["pandas"] = mock_pd
        return orig

    def _restore_modules(self, orig):
        import sys
        for k, v in orig.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v

    def test_renders_without_error_with_mocked_streamlit(self, monkeypatch):
        from unittest.mock import MagicMock

        mock_st = self._make_st_mock()
        mock_pd = MagicMock()

        monkeypatch.setattr(dashboard, "fetch_accidents_by_year", lambda: [("2026", 10)])
        monkeypatch.setattr(
            dashboard, "fetch_accidents_by_hour", lambda: [(8, 50), (9, 60)]
        )
        monkeypatch.setattr(
            dashboard,
            "fetch_severity_distribution",
            lambda: [("Tue", 5), ("Leger", 100)],
        )
        monkeypatch.setattr(
            dashboard, "fetch_accidents_by_commune", lambda limit: [("Paris", 200)]
        )
        monkeypatch.setattr(
            dashboard, "fetch_accidents_by_weather", lambda: [("Normale", 300)]
        )

        orig = self._swap_modules(mock_st, mock_pd)
        try:
            dashboard._render_dashboard()
        finally:
            self._restore_modules(orig)

        mock_st.set_page_config.assert_called_once()
        mock_st.title.assert_called_once()

    def test_renders_no_data_branches(self, monkeypatch):
        """All data functions return [] → st.info('No data available.') called for each section."""
        from unittest.mock import MagicMock

        mock_st = self._make_st_mock()
        mock_pd = MagicMock()

        monkeypatch.setattr(dashboard, "fetch_accidents_by_year", lambda: [])
        monkeypatch.setattr(dashboard, "fetch_accidents_by_hour", lambda: [])
        monkeypatch.setattr(dashboard, "fetch_severity_distribution", lambda: [])
        monkeypatch.setattr(dashboard, "fetch_accidents_by_commune", lambda limit: [])
        monkeypatch.setattr(dashboard, "fetch_accidents_by_weather", lambda: [])

        orig = self._swap_modules(mock_st, mock_pd)
        try:
            dashboard._render_dashboard()
        finally:
            self._restore_modules(orig)

        # st.info must have been called at least once for the empty data paths
        assert mock_st.info.call_count >= 5

    def test_renders_exception_branches(self, monkeypatch):
        """All data functions raise → st.error(...) called for each section."""
        from unittest.mock import MagicMock

        mock_st = self._make_st_mock()
        mock_pd = MagicMock()

        def _raise():
            raise RuntimeError("DB down")

        monkeypatch.setattr(
            dashboard, "fetch_accidents_by_year", lambda: _raise()
        )
        monkeypatch.setattr(
            dashboard, "fetch_accidents_by_hour", lambda: _raise()
        )
        monkeypatch.setattr(
            dashboard, "fetch_severity_distribution", lambda: _raise()
        )
        monkeypatch.setattr(
            dashboard, "fetch_accidents_by_commune", lambda limit: _raise()
        )
        monkeypatch.setattr(
            dashboard, "fetch_accidents_by_weather", lambda: _raise()
        )

        orig = self._swap_modules(mock_st, mock_pd)
        try:
            dashboard._render_dashboard()
        finally:
            self._restore_modules(orig)

        assert mock_st.error.call_count >= 5

    def test_raises_import_error_when_streamlit_unavailable(self, monkeypatch):
        import sys
        import builtins
        import pytest

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name in ("streamlit", "pandas"):
                raise ImportError(f"No module named '{name}'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        cached = {k: sys.modules.pop(k) for k in ("streamlit", "pandas") if k in sys.modules}
        try:
            with pytest.raises(ImportError, match="streamlit"):
                dashboard._render_dashboard()
        finally:
            sys.modules.update(cached)


# ---------------------------------------------------------------------------
# Tests for run_dashboard
# ---------------------------------------------------------------------------


class TestRunDashboard:
    def test_run_dashboard_success(self, monkeypatch, capsys):
        import subprocess

        monkeypatch.setattr(
            subprocess, "run", lambda args, check: None
        )
        dashboard.run_dashboard()
        out = capsys.readouterr().out
        assert "Launching" in out

    def test_run_dashboard_streamlit_not_installed(self, monkeypatch, capsys):
        import subprocess

        def raise_fnf(args, check):
            raise FileNotFoundError("streamlit not found")

        monkeypatch.setattr(subprocess, "run", raise_fnf)
        dashboard.run_dashboard()
        out = capsys.readouterr().out
        assert "not installed" in out.lower() or "streamlit" in out.lower()

    def test_run_dashboard_keyboard_interrupt(self, monkeypatch, capsys):
        import subprocess

        def raise_ki(args, check):
            raise KeyboardInterrupt()

        monkeypatch.setattr(subprocess, "run", raise_ki)
        dashboard.run_dashboard()
        out = capsys.readouterr().out
        assert "stopped" in out.lower()
