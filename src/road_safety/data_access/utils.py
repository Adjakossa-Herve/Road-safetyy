import psycopg
from pathlib import Path
from dotenv import load_dotenv
from ..config import Settings


def load_dotenv_if_present() -> None:
    """Loads .env file if it exists."""
    env_file = Path(".env")
    if env_file.exists():
        load_dotenv(env_file)


def establish_connection():
    """Establishes a connection to the PostgreSQL database."""
    load_dotenv_if_present()
    settings = Settings()
    try:
        conn = psycopg.connect(settings.get_dsn())
        return conn
    except Exception as e:
        print(f"Connection error: {e}")
        return None
