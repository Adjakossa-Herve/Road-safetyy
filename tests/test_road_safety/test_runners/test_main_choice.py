import builtins
import road_safety.main as main_mod


def test_main_chat_choice_menu(monkeypatch):
    # Simulate "road-safety chat"
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "chat"])

    # Simulate choice "1" (menu)
    inputs = iter(["1"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

    called = {"menu": 0, "free": 0}

    # Monkeypatch run_menu / run_chat
    monkeypatch.setattr(
        main_mod, "run_menu", lambda: called.__setitem__("menu", called["menu"] + 1)
    )
    monkeypatch.setattr(
        main_mod, "run_chat", lambda: called.__setitem__("free", called["free"] + 1)
    )

    rc = main_mod.main()
    assert rc == 0
    assert called["menu"] == 1
    assert called["free"] == 0


def test_main_chat_choice_free(monkeypatch):
    monkeypatch.setattr(main_mod.sys, "argv", ["road-safety", "chat"])

    inputs = iter(["2"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

    called = {"menu": 0, "free": 0}
    monkeypatch.setattr(
        main_mod, "run_menu", lambda: called.__setitem__("menu", called["menu"] + 1)
    )
    monkeypatch.setattr(
        main_mod, "run_chat", lambda: called.__setitem__("free", called["free"] + 1)
    )

    rc = main_mod.main()
    assert rc == 0
    assert called["menu"] == 0
    assert called["free"] == 1


def test_choose_mode_returns_forced_from_env(monkeypatch):
    """ROAD_SAFETY_MODE env var forces the mode without prompting."""
    monkeypatch.setenv("ROAD_SAFETY_MODE", "menu")

    mode = main_mod._choose_mode()

    monkeypatch.delenv("ROAD_SAFETY_MODE", raising=False)
    assert mode == "menu"


def test_choose_mode_invalid_then_valid_prints_invalid_choice(monkeypatch, capsys):
    """An invalid choice prints the error message; a valid choice returns correctly."""
    inputs = iter(["xyz", "1"])
    monkeypatch.setattr(builtins, "input", lambda _: next(inputs))

    mode = main_mod._choose_mode()

    out = capsys.readouterr().out
    assert "Invalid choice" in out
    assert mode == "menu"
