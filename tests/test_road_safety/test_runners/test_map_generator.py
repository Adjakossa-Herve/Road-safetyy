"""Unit tests for road_safety.runners.map_generator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Tuple
from unittest.mock import MagicMock, patch

import pytest

import road_safety.runners.map_generator as map_gen


# ---------------------------------------------------------------------------
# Fake DB helpers
# ---------------------------------------------------------------------------


@dataclass
class FakeCursor:
    rows: List[Tuple[Any, ...]]
    executed: List[Tuple[str, tuple]]
    execute_error: Exception | None = None

    def execute(self, query: str, params: tuple = ()):
        self.executed.append((query, params))
        if self.execute_error is not None:
            raise self.execute_error

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
# Tests for fetch_coordinates
# ---------------------------------------------------------------------------


class TestFetchCoordinates:
    def test_returns_list_of_float_pairs(self, monkeypatch):
        conn = make_fake_conn([(48.85, 2.35), (43.30, 5.37)])
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        result = map_gen.fetch_coordinates(limit=100)

        assert result == [(48.85, 2.35), (43.30, 5.37)]
        sql, params = conn.cursor_obj.executed[0]
        assert "latitude" in sql
        assert "longitude" in sql
        assert params == (100,)

    def test_returns_empty_list_when_no_rows(self, monkeypatch):
        conn = make_fake_conn([])
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        result = map_gen.fetch_coordinates()

        assert result == []

    def test_raises_on_connection_failure(self, monkeypatch):
        monkeypatch.setattr(map_gen, "establish_connection", lambda: None)

        with pytest.raises(RuntimeError, match="Database connection failed"):
            map_gen.fetch_coordinates()

    def test_filters_null_coordinates_via_sql(self, monkeypatch):
        conn = make_fake_conn([])
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        map_gen.fetch_coordinates()

        sql, _ = conn.cursor_obj.executed[0]
        assert "IS NOT NULL" in sql
        assert "BETWEEN" in sql

    def test_raises_helpful_error_when_latitude_column_missing(self, monkeypatch):
        conn = make_fake_conn([])
        conn.cursor_obj.execute_error = Exception('column "latitude" does not exist')
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        with pytest.raises(RuntimeError, match="Missing GPS columns"):
            map_gen.fetch_coordinates()

    def test_raises_helpful_error_when_longitude_column_missing(self, monkeypatch):
        conn = make_fake_conn([])
        conn.cursor_obj.execute_error = Exception('column "longitude" does not exist')
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        with pytest.raises(RuntimeError, match="Missing GPS columns"):
            map_gen.fetch_coordinates()

    def test_reraises_generic_sql_error(self, monkeypatch):
        conn = make_fake_conn([])
        conn.cursor_obj.execute_error = Exception("db exploded")
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        with pytest.raises(Exception, match="db exploded"):
            map_gen.fetch_coordinates()


# ---------------------------------------------------------------------------
# Tests for build_map
# ---------------------------------------------------------------------------


class TestBuildMap:
    def test_raises_import_error_when_folium_missing(self, monkeypatch):
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "folium":
                raise ImportError("No module named 'folium'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)

        with pytest.raises(ImportError, match="folium"):
            map_gen.build_map([(48.85, 2.35)])

    def test_builds_map_with_folium(self, monkeypatch):
        mock_marker = MagicMock()
        mock_map_instance = MagicMock()
        mock_folium = MagicMock()
        mock_folium.Map.return_value = mock_map_instance
        mock_folium.CircleMarker.return_value = mock_marker

        with patch.dict("sys.modules", {"folium": mock_folium}):
            result = map_gen.build_map([(48.85, 2.35), (43.30, 5.37)])

        mock_folium.Map.assert_called_once_with(location=[46.5, 2.5], zoom_start=6)
        assert mock_folium.CircleMarker.call_count == 2
        assert result is mock_map_instance


# ---------------------------------------------------------------------------
# Tests for save_map
# ---------------------------------------------------------------------------


class TestSaveMap:
    def test_calls_save_on_map(self):
        mock_map = MagicMock()
        map_gen.save_map(mock_map, "/tmp/test_map.html")
        mock_map.save.assert_called_once_with("/tmp/test_map.html")


# ---------------------------------------------------------------------------
# Tests for generate_map
# ---------------------------------------------------------------------------


class TestGenerateMap:
    def test_generate_map_calls_pipeline(self, monkeypatch):
        called = {}

        monkeypatch.setattr(map_gen, "fetch_coordinates", lambda limit: [(1.0, 2.0)])
        monkeypatch.setattr(
            map_gen,
            "build_map",
            lambda coords: called.__setitem__("map", True) or MagicMock(),
        )
        monkeypatch.setattr(
            map_gen, "save_map", lambda m, p: called.__setitem__("saved", p)
        )

        result = map_gen.generate_map(output_path="/tmp/out.html", limit=50)

        assert result == "/tmp/out.html"
        assert called.get("saved") == "/tmp/out.html"


# ---------------------------------------------------------------------------
# Tests for run_map
# ---------------------------------------------------------------------------


class TestRunMap:
    def test_run_map_prints_success(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen, "generate_map", lambda output_path, limit: output_path
        )

        map_gen.run_map(output_path="/tmp/accidents_map.html", limit=100)

        out = capsys.readouterr().out
        assert "accidents_map.html" in out

    def test_run_map_handles_import_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen,
            "generate_map",
            lambda **kwargs: (_ for _ in ()).throw(ImportError("folium not installed")),
        )

        map_gen.run_map()

        out = capsys.readouterr().out
        assert "folium" in out.lower() or "⚠️" in out

    def test_run_map_handles_runtime_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen,
            "generate_map",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("DB error")),
        )

        map_gen.run_map()

        out = capsys.readouterr().out
        assert "⚠️" in out


# ---------------------------------------------------------------------------
# Tests for build_heatmap
# ---------------------------------------------------------------------------


class TestBuildHeatmap:
    def test_raises_import_error_when_folium_missing(self, monkeypatch):
        import sys
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "folium":
                raise ImportError("No module named 'folium'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        cached = {k: sys.modules.pop(k) for k in list(sys.modules) if k == "folium"}
        try:
            with pytest.raises(ImportError, match="folium"):
                map_gen.build_heatmap([(48.85, 2.35)])
        finally:
            sys.modules.update(cached)

    def test_builds_map_with_mocked_folium(self, monkeypatch):
        import sys
        from unittest.mock import MagicMock

        mock_folium = MagicMock()
        mock_plugins = MagicMock()
        mock_map = MagicMock()
        mock_folium.Map.return_value = mock_map
        mock_heatmap_instance = MagicMock()
        mock_plugins.HeatMap.return_value = mock_heatmap_instance

        sys.modules["folium"] = mock_folium
        sys.modules["folium.plugins"] = mock_plugins
        try:
            result = map_gen.build_heatmap([(48.85, 2.35), (48.90, 2.30)])
        finally:
            sys.modules.pop("folium", None)
            sys.modules.pop("folium.plugins", None)

        mock_folium.Map.assert_called_once()
        mock_heatmap_instance.add_to.assert_called_once_with(mock_map)
        assert result is mock_map


# ---------------------------------------------------------------------------
# Tests for generate_heatmap
# ---------------------------------------------------------------------------


class TestGenerateHeatmap:
    def test_returns_output_path(self, monkeypatch):
        monkeypatch.setattr(
            map_gen, "fetch_coordinates", lambda limit: [(48.85, 2.35)]
        )
        fake_map = MagicMock()
        monkeypatch.setattr(map_gen, "build_heatmap", lambda coords: fake_map)
        saved = {}
        monkeypatch.setattr(
            map_gen, "save_map", lambda m, p: saved.__setitem__("path", p)
        )

        result = map_gen.generate_heatmap(output_path="/tmp/heat.html", limit=500)

        assert result == "/tmp/heat.html"
        assert saved["path"] == "/tmp/heat.html"


# ---------------------------------------------------------------------------
# Tests for run_heatmap
# ---------------------------------------------------------------------------


class TestRunHeatmap:
    def test_prints_success_message(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen, "generate_heatmap", lambda output_path, limit: output_path
        )
        map_gen.run_heatmap(output_path="/tmp/heat.html", limit=100)
        out = capsys.readouterr().out
        assert "heat.html" in out

    def test_handles_import_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen,
            "generate_heatmap",
            lambda **kwargs: (_ for _ in ()).throw(ImportError("folium missing")),
        )
        map_gen.run_heatmap()
        out = capsys.readouterr().out
        assert "⚠️" in out

    def test_handles_runtime_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen,
            "generate_heatmap",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("DB down")),
        )
        map_gen.run_heatmap()
        out = capsys.readouterr().out
        assert "⚠️" in out


# ---------------------------------------------------------------------------
# Tests for fetch_commune_stats
# ---------------------------------------------------------------------------


class TestFetchCommuneStats:
    def test_returns_commune_rows(self, monkeypatch):
        row = ("Paris", 48.85, 2.35, 1000, 5, 150)
        conn = make_fake_conn([row])
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        result = map_gen.fetch_commune_stats()

        assert len(result) == 1
        commune, lat, lon, total, fatals, rs = result[0]
        assert commune == "Paris"
        assert lat == 48.85
        assert total == 1000

    def test_raises_runtime_error_when_no_connection(self, monkeypatch):
        monkeypatch.setattr(map_gen, "establish_connection", lambda: None)
        with pytest.raises(RuntimeError, match="Database connection failed"):
            map_gen.fetch_commune_stats()

    def test_reraises_generic_sql_error(self, monkeypatch):
        conn = make_fake_conn([])
        conn.cursor_obj.execute_error = Exception("sql failed")
        monkeypatch.setattr(map_gen, "establish_connection", lambda: conn)

        with pytest.raises(Exception, match="sql failed"):
            map_gen.fetch_commune_stats()


# ---------------------------------------------------------------------------
# Tests for build_commune_map
# ---------------------------------------------------------------------------


class TestBuildCommuneMap:
    def test_returns_empty_map_for_empty_stats(self, monkeypatch):
        import sys
        from unittest.mock import MagicMock

        mock_folium = MagicMock()
        mock_map = MagicMock()
        mock_folium.Map.return_value = mock_map

        sys.modules["folium"] = mock_folium
        try:
            result = map_gen.build_commune_map([])
        finally:
            sys.modules.pop("folium", None)

        mock_folium.Map.assert_called_once()
        assert result is mock_map

    def test_raises_import_error_when_folium_missing(self, monkeypatch):
        import sys
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "folium":
                raise ImportError("No module named 'folium'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        cached = {k: sys.modules.pop(k) for k in list(sys.modules) if k == "folium"}
        try:
            with pytest.raises(ImportError, match="folium"):
                map_gen.build_commune_map([("Paris", 48.85, 2.35, 100, 5, 50)])
        finally:
            sys.modules.update(cached)

    def test_creates_circle_markers_for_all_color_tiers(self, monkeypatch):
        """Covers red (>=0.66), orange (0.33-0.66), yellow (<0.33) colour tiers."""
        import sys
        from unittest.mock import MagicMock

        mock_folium = MagicMock()
        mock_map = MagicMock()
        mock_folium.Map.return_value = mock_map
        mock_folium.CircleMarker.return_value = MagicMock()
        mock_folium.Popup.return_value = MagicMock()

        sys.modules["folium"] = mock_folium
        try:
            # max_risk = 100, ratios: 1.0 (red), 0.50 (orange), 0.10 (yellow)
            stats = [
                ("Paris", 48.85, 2.35, 500, 20, 100),   # ratio 1.0  → red
                ("Lyon", 45.75, 4.85, 200, 8, 50),       # ratio 0.50 → orange
                ("Nantes", 47.22, -1.55, 80, 2, 10),     # ratio 0.10 → yellow
            ]
            result = map_gen.build_commune_map(stats)
        finally:
            sys.modules.pop("folium", None)

        assert mock_folium.CircleMarker.call_count == 3
        # Collect color args
        colors = [
            call.kwargs.get("color") or call.args[1]
            if len(call.args) > 1
            else call.kwargs.get("color")
            for call in mock_folium.CircleMarker.call_args_list
        ]
        # Use keyword args
        kw_colors = [
            c.kwargs.get("color") for c in mock_folium.CircleMarker.call_args_list
        ]
        assert "#c0392b" in kw_colors   # rouge
        assert "#e67e22" in kw_colors   # orange
        assert "#f1c40f" in kw_colors   # jaune


# ---------------------------------------------------------------------------
# Tests for generate_commune_map
# ---------------------------------------------------------------------------


class TestGenerateCommuneMap:
    def test_returns_output_path(self, monkeypatch):
        monkeypatch.setattr(
            map_gen, "fetch_commune_stats", lambda: [("Paris", 48.85, 2.35, 100, 5, 50)]
        )
        fake_map = MagicMock()
        monkeypatch.setattr(map_gen, "build_commune_map", lambda stats: fake_map)
        saved = {}
        monkeypatch.setattr(
            map_gen, "save_map", lambda m, p: saved.__setitem__("path", p)
        )

        result = map_gen.generate_commune_map(output_path="/tmp/commune.html")

        assert result == "/tmp/commune.html"
        assert saved["path"] == "/tmp/commune.html"


# ---------------------------------------------------------------------------
# Tests for run_commune_map
# ---------------------------------------------------------------------------


class TestRunCommuneMap:
    def test_prints_success_message(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen, "generate_commune_map", lambda output_path: output_path
        )
        map_gen.run_commune_map(output_path="/tmp/commune.html")
        out = capsys.readouterr().out
        assert "commune.html" in out

    def test_handles_import_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen,
            "generate_commune_map",
            lambda **kwargs: (_ for _ in ()).throw(ImportError("folium missing")),
        )
        map_gen.run_commune_map()
        out = capsys.readouterr().out
        assert "⚠️" in out

    def test_handles_runtime_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            map_gen,
            "generate_commune_map",
            lambda **kwargs: (_ for _ in ()).throw(RuntimeError("DB down")),
        )
        map_gen.run_commune_map()
        out = capsys.readouterr().out
        assert "⚠️" in out
