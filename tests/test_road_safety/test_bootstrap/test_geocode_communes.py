from __future__ import annotations

import road_safety.bootstrap.geocode_communes as geo_mod


class FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeCursor:
    def __init__(self, communes):
        self.communes = communes
        self.executed = []
        self.closed = False

    def execute(self, query: str, params=None):
        self.executed.append((query, params))

    def fetchall(self):
        return self.communes

    def close(self):
        self.closed = True


class FakeConn:
    def __init__(self, communes):
        self.cursor_obj = FakeCursor(communes=communes)
        self.commits = 0
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


def test_geocode_commune_returns_coordinates(monkeypatch):
    payload = b'[{"lat":"48.8566","lon":"2.3522"}]'
    monkeypatch.setattr(
        geo_mod.urllib.request,
        "urlopen",
        lambda req, timeout=10: FakeResponse(payload),
    )

    coords = geo_mod.geocode_commune("Paris")

    assert coords == (48.8566, 2.3522)


def test_geocode_commune_returns_none_when_empty(monkeypatch):
    monkeypatch.setattr(
        geo_mod.urllib.request,
        "urlopen",
        lambda req, timeout=10: FakeResponse(b"[]"),
    )

    assert geo_mod.geocode_commune("Unknown") is None


def test_geocode_commune_handles_network_error(monkeypatch, capsys):
    def boom(*args, **kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(geo_mod.urllib.request, "urlopen", boom)

    assert geo_mod.geocode_commune("Paris") is None
    out = capsys.readouterr().out
    assert "Erreur géocodage" in out


def test_run_geocoding_handles_connection_failure(monkeypatch, capsys):
    monkeypatch.setattr(geo_mod, "establish_connection", lambda: None)

    geo_mod.run_geocoding()

    out = capsys.readouterr().out
    assert "impossible de se connecter" in out


def test_run_geocoding_when_all_communes_already_geocoded(monkeypatch, capsys):
    conn = FakeConn(communes=[])
    monkeypatch.setattr(geo_mod, "establish_connection", lambda: conn)

    geo_mod.run_geocoding()

    out = capsys.readouterr().out
    assert "Toutes les communes sont déjà géocodées" in out
    assert conn.commits == 1
    assert conn.closed is True
    assert any("ALTER TABLE raw.accidents" in q for q, _ in conn.cursor_obj.executed)


def test_run_geocoding_updates_rows_and_prints_summary(monkeypatch, capsys):
    conn = FakeConn(communes=[("Paris",), ("Lyon",)])
    monkeypatch.setattr(geo_mod, "establish_connection", lambda: conn)

    def fake_geocode(commune: str):
        if commune == "Paris":
            return (48.8566, 2.3522)
        return None

    monkeypatch.setattr(geo_mod, "geocode_commune", fake_geocode)
    monkeypatch.setattr(geo_mod.time, "sleep", lambda _: None)

    geo_mod.run_geocoding()

    out = capsys.readouterr().out
    assert "Géocodage terminé : 1 OK — 1 échecs" in out
    updates = [
        (q, params)
        for q, params in conn.cursor_obj.executed
        if "UPDATE raw.accidents" in q
    ]
    assert len(updates) == 1
    assert updates[0][1] == (48.8566, 2.3522, "Paris")
    assert conn.commits == 2
