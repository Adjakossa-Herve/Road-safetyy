import road_safety.main as main_mod


def test_main_chat_routes_to_chat(monkeypatch):
    called = {"ok": False}

    def fake_run_chat():
        called["ok"] = True

    monkeypatch.setattr(main_mod, "run_chat", fake_run_chat)
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "chat"])

    rc = main_mod.main()
    assert rc == 0
    assert called["ok"] is True


def test_main_insights_routes_to_insights(monkeypatch):
    called = {"ok": False}

    monkeypatch.setattr(
        main_mod, "run_insights", lambda: called.__setitem__("ok", True)
    )
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "insights"])

    rc = main_mod.main()
    assert rc == 0
    assert called["ok"] is True


def test_main_map_routes_to_map(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_map",
        lambda output_path, limit: called.__setitem__("args", (output_path, limit)),
    )
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "map"])

    rc = main_mod.main()
    assert rc == 0
    assert called["args"] == ("accidents_map.html", 2000)


def test_main_map_accepts_custom_path_and_limit(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_map",
        lambda output_path, limit: called.__setitem__("args", (output_path, limit)),
    )
    monkeypatch.setattr(
        main_mod.sys, "argv", ["road-safety", "map", "/tmp/my.html", "500"]
    )

    rc = main_mod.main()
    assert rc == 0
    assert called["args"] == ("/tmp/my.html", 500)


def test_main_dashboard_routes_to_dashboard(monkeypatch):
    called = {"ok": False}

    monkeypatch.setattr(
        main_mod, "run_dashboard", lambda: called.__setitem__("ok", True)
    )
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "dashboard"])

    rc = main_mod.main()
    assert rc == 0
    assert called["ok"] is True


def test_main_pulse_routes_default_args(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_pulse",
        lambda history_path, months, top: called.__setitem__(
            "args", (history_path, months, top)
        ),
    )
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "pulse"])

    rc = main_mod.main()
    assert rc == 0
    assert called["args"] == ("src/road_safety/data/pulse_history.jsonl", 12, 5)


def test_main_pulse_accepts_custom_args(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_pulse",
        lambda history_path, months, top: called.__setitem__(
            "args", (history_path, months, top)
        ),
    )
    monkeypatch.setattr(
        main_mod.sys,
        "argv",
        ["road-safety", "pulse", "/tmp/pulse.jsonl", "18", "7"],
    )

    rc = main_mod.main()
    assert rc == 0
    assert called["args"] == ("/tmp/pulse.jsonl", 18, 7)


def test_main_no_args_returns_error(monkeypatch, capsys):
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety"])

    rc = main_mod.main()

    assert rc == 1
    out = capsys.readouterr().out
    assert "Usage" in out


def test_main_unknown_command_returns_error(monkeypatch, capsys):
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "nonexistent"])

    rc = main_mod.main()

    assert rc == 1
    out = capsys.readouterr().out
    assert "Unknown command" in out


def test_main_heatmap_routes_default_args(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_heatmap",
        lambda output_path, limit: called.__setitem__("args", (output_path, limit)),
    )
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "heatmap"])

    rc = main_mod.main()
    assert rc == 0
    assert called["args"] == ("accidents_heatmap.html", 10000)


def test_main_heatmap_accepts_custom_path_and_limit(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_heatmap",
        lambda output_path, limit: called.__setitem__("args", (output_path, limit)),
    )
    monkeypatch.setattr(
        main_mod.sys, "argv", ["road-safety", "heatmap", "/tmp/h.html", "500"]
    )

    rc = main_mod.main()
    assert rc == 0
    assert called["args"] == ("/tmp/h.html", 500)


def test_main_commune_map_routes_default_args(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_commune_map",
        lambda output_path: called.__setitem__("path", output_path),
    )
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "commune-map"])

    rc = main_mod.main()
    assert rc == 0
    assert called["path"] == "accidents_commune_map.html"


def test_main_commune_map_accepts_custom_path(monkeypatch):
    called = {}

    monkeypatch.setattr(
        main_mod,
        "run_commune_map",
        lambda output_path: called.__setitem__("path", output_path),
    )
    monkeypatch.setattr(
        main_mod.sys, "argv", ["road-safety", "commune-map", "/tmp/c.html"]
    )

    rc = main_mod.main()
    assert rc == 0
    assert called["path"] == "/tmp/c.html"


def test_main_chat_run_menu_is_none_returns_error(monkeypatch, capsys):
    """If run_menu is None and mode == 'menu', main returns 1."""
    import os

    monkeypatch.setattr(main_mod, "run_menu", None)
    monkeypatch.setenv("ROAD_SAFETY_MODE", "menu")
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "chat"])

    rc = main_mod.main()

    monkeypatch.delenv("ROAD_SAFETY_MODE", raising=False)
    assert rc == 1
