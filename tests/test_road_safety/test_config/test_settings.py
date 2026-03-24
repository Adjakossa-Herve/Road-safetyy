from __future__ import annotations

import importlib

import dotenv
import road_safety.config.settings as settings_mod


def _clear_db_env(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("DB_PORT", raising=False)
    monkeypatch.delenv("POSTGRES_USER", raising=False)
    monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    monkeypatch.delenv("DB_SCHEMA", raising=False)


def test_settings_uses_defaults_when_env_missing(monkeypatch):
    _clear_db_env(monkeypatch)
    monkeypatch.setattr(settings_mod, "load_dotenv", lambda: None)

    settings = settings_mod.Settings()

    assert settings.db_host == "road-safety-postgres"
    assert settings.db_port == 5432
    assert settings.db_user == "rjlee"
    assert settings.db_pass == "rjpassword"
    assert settings.db_name == "accident_idf"
    assert settings.db_schema == "raw"
    assert "host=road-safety-postgres" in settings.get_dsn()
    assert "port=5432" in settings.get_dsn()
    assert "postgresql+psycopg://rjlee:rjpassword@" in settings.get_database_url()
    assert "search_path%3Draw,public" in settings.get_database_url()


def test_settings_reads_environment_values(monkeypatch):
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5888")
    monkeypatch.setenv("POSTGRES_USER", "user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "password")
    monkeypatch.setenv("POSTGRES_DB", "accidents_db")
    monkeypatch.setenv("DB_SCHEMA", "analytics")
    monkeypatch.setattr(settings_mod, "load_dotenv", lambda: None)

    settings = settings_mod.Settings()

    assert settings.db_host == "localhost"
    assert settings.db_port == 5888
    assert settings.db_user == "user"
    assert settings.db_pass == "password"
    assert settings.db_name == "accidents_db"
    assert settings.db_schema == "analytics"
    assert "dbname=accidents_db" in settings.get_dsn()
    assert "options=-csearch_path=analytics,public" in settings.get_dsn()
    assert "@localhost:5888/accidents_db" in settings.get_database_url()


def test_module_level_settings_instance_is_created(monkeypatch):
    _clear_db_env(monkeypatch)
    monkeypatch.setattr(dotenv, "load_dotenv", lambda: None)

    reloaded = importlib.reload(settings_mod)

    assert isinstance(reloaded.settings, reloaded.Settings)
