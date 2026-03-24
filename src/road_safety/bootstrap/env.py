import os
from pathlib import Path


def load_dotenv_if_present() -> None:
    """
    Charge .env si présent (racine du projet), puis définit des valeurs par défaut.
    Ne surcharge pas les variables déjà définies.
    """
    root = Path(__file__).resolve().parents[3]  # project root
    env_file = root / ".env"

    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

    os.environ.setdefault("DB_HOST", "localhost")
    os.environ.setdefault("DB_PORT", "5888")
    os.environ.setdefault("DB_USER", "user")
    os.environ.setdefault("DB_PASS", "password")
    os.environ.setdefault("DB_NAME", "accidents_db")
    os.environ.setdefault("RS_AUTO_LOAD", "1")
