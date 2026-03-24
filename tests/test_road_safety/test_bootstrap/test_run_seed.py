from __future__ import annotations

import importlib
import sys
import types


def test_run_seed_invokes_ensure_accidents_loaded(monkeypatch, capsys):
    fake_module = types.ModuleType("road_safety.bootstrap.data_seed")
    called = {"count": 0}

    def fake_ensure():
        called["count"] += 1

    fake_module.ensure_accidents_loaded = fake_ensure
    monkeypatch.setitem(sys.modules, "road_safety.bootstrap.data_seed", fake_module)
    sys.modules.pop("road_safety.bootstrap.run_seed", None)

    importlib.import_module("road_safety.bootstrap.run_seed")

    out = capsys.readouterr().out
    assert "Démarrage du seed..." in out
    assert "Seed terminé." in out
    assert called["count"] == 1
