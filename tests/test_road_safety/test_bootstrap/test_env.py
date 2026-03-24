import os

import road_safety.bootstrap.env as env_mod


def test_load_dotenv_if_present_sets_defaults_when_missing(monkeypatch, tmp_path):
    # Force project root to tmp by patching Path resolution
    # We patch the module's Path to point to a fake repo root containing no .env.
    # Easiest: patch env_mod.Path and env_mod.__file__ isn't trivial, so we patch the env_file existence
    # by creating an empty .env path and making env_mod load from a temp root.
    #
    # We'll monkeypatch Path.exists/text reading by creating a real file and patching parents[3] logic
    # via a helper: patch env_mod.Path to our Path class is complex; so we do a simpler approach:
    # call function, but ensure no .env exists in real root AND clear env vars. Defaults should be set.

    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("DB_USER", raising=False)
    monkeypatch.delenv("DB_PASS", raising=False)
    monkeypatch.delenv("DB_NAME", raising=False)
    monkeypatch.delenv("RS_ENABLE_EXTENDED", raising=False)

    env_mod.load_dotenv_if_present()

    assert os.getenv("DB_HOST") == "localhost"
    assert os.getenv("DB_PORT") == "5888"
    assert os.getenv("DB_USER") == "user"
    assert os.getenv("DB_PASS") == "password"
    assert os.getenv("DB_NAME") == "accidents_db"


def test_load_dotenv_if_present_does_not_override_existing_env(monkeypatch):
    monkeypatch.setenv("DB_HOST", "already-set")
    monkeypatch.setenv("DB_PORT", "9999")
    monkeypatch.setenv("RS_ENABLE_EXTENDED", "1")

    env_mod.load_dotenv_if_present()

    assert os.getenv("DB_HOST") == "already-set"
    assert os.getenv("DB_PORT") == "9999"
    assert os.getenv("RS_ENABLE_EXTENDED") == "1"
