import os
from dotenv import load_dotenv


class Settings:
    def __init__(self) -> None:
        load_dotenv()

        # Docker / PostgreSQL configuration
        self.db_host: str = os.getenv("DB_HOST", "road-safety-postgres")
        self.db_port: int = int(os.getenv("DB_PORT", "5432"))

        self.db_user: str = os.getenv("POSTGRES_USER", "rjlee")
        self.db_pass: str = os.getenv("POSTGRES_PASSWORD", "rjpassword")
        self.db_name: str = os.getenv("POSTGRES_DB", "accident_idf")

        # Default schema used by the application
        self.db_schema: str = os.getenv("DB_SCHEMA", "raw")

    def get_dsn(self) -> str:
        """
        DSN string for direct connection using psycopg3.
        The search_path is configured to use the 'raw' schema by default.
        """
        return (
            f"host={self.db_host} "
            f"port={self.db_port} "
            f"dbname={self.db_name} "
            f"user={self.db_user} "
            f"password={self.db_pass} "
            f"options=-csearch_path={self.db_schema},public"
        )

    def get_database_url(self) -> str:
        """
        SQLAlchemy database URL using psycopg3.
        Forces the PostgreSQL search_path to the 'raw' schema.
        """
        return (
            f"postgresql+psycopg://{self.db_user}:{self.db_pass}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
            f"?options=-csearch_path%3D{self.db_schema},public"
        )


settings = Settings()
