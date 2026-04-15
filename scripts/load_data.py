import logging
import os
import sys
import uuid
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from logging_utils import setup_logging
from schema_contracts import validate_dataframe, SCHEMA_CONTRACTS

_log = logging.getLogger("f1_analytics")

try:
    from config import DB_CONFIG, DATA_PATHS
except ImportError:
    _log.warning("config.py not found; using SQLite defaults. Copy scripts/config.example.py to scripts/config.py.")
    DB_CONFIG = {
        "type": "sqlite",
        "filename": "f1_analytics.db",
    }
    DATA_PATHS = {
        "processed_data": "data/processed/",
    }


def _build_connection_string(config: dict) -> str:
    """Return a SQLAlchemy connection string from a DB_CONFIG dict."""
    if config.get("type") == "sqlite":
        return f"sqlite:///{config.get('filename', 'f1_analytics.db')}"
    return (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )


class F1DataLoader:
    """Load transformed F1 data into a relational database."""

    def __init__(
        self,
        config=None,
        processed_data_path=None,
        mode: str = "full_refresh",
        strict_schema: bool = True,
        run_id: str | None = None,
        source_url: str | None = None,
    ):
        self.config = config or DB_CONFIG
        self.processed_path = processed_data_path or DATA_PATHS.get("processed_data", "data/processed/")
        self.raw_path = os.path.normpath(os.path.join(self.processed_path, "..", "raw"))
        self.engine = None
        self.mode = mode
        self.strict_schema = strict_schema
        self.run_id = run_id or str(uuid.uuid4())
        self.source_url = source_url
        self.logger = setup_logging()
        self._connect()
        self._ensure_metadata_tables()

    def _connect(self) -> None:
        """Create a database engine and validate the connection."""
        try:
            if self.config.get("type") == "sqlite":
                db_file = self.config.get("filename", "f1_analytics.db")
                if self.mode == "full_refresh" and os.path.exists(db_file):
                    self.logger.warning("Full refresh: removing existing SQLite database %s.", db_file)
                    os.remove(db_file)
                if not os.path.isabs(db_file) and "/" in db_file:
                    os.makedirs(os.path.dirname(db_file), exist_ok=True)
                self.logger.info("Connecting to SQLite database at %s.", db_file)
            else:
                self.logger.info("Connecting to MySQL at %s.", self.config.get("host"))

            self.engine = create_engine(_build_connection_string(self.config))

            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            if self.config.get("type") == "sqlite":
                self._apply_sqlite_schema()

            self.logger.info("Database connection established.")

        except Exception as exc:
            self.logger.error("Error connecting to database: %s", exc)
            self.logger.error("Check your database configuration in scripts/config.py.")
            raise

    def _apply_sqlite_schema(self) -> None:
        schema_path = os.path.join(SCRIPT_DIR, "..", "database", "schema", "create_tables_sqlite.sql")
        schema_path = os.path.abspath(schema_path)
        if not os.path.exists(schema_path):
            self.logger.warning("SQLite schema file not found: %s", schema_path)
            return

        with open(schema_path, "r") as handle:
            schema_sql = handle.read()

        with self.engine.connect() as conn:
            for statement in schema_sql.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
            conn.commit()

    def _ensure_metadata_tables(self) -> None:
        if self.config.get("type") == "sqlite":
            return

        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        run_id VARCHAR(36) PRIMARY KEY,
                        started_at DATETIME,
                        ended_at DATETIME,
                        status VARCHAR(20),
                        source_url VARCHAR(255),
                        mode VARCHAR(20)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS pipeline_run_tables (
                        run_id VARCHAR(36),
                        table_name VARCHAR(50),
                        rows_loaded INT,
                        PRIMARY KEY (run_id, table_name)
                    )
                    """
                )
            )
            conn.commit()

    def _record_run_start(self) -> None:
        # Pipeline audit tables are only created for MySQL deployments.
        if self.config.get("type") == "sqlite":
            return
        started_at = datetime.now(timezone.utc).isoformat()
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO pipeline_runs (run_id, started_at, status, source_url, mode)
                    VALUES (:run_id, :started_at, :status, :source_url, :mode)
                    """
                ),
                {
                    "run_id": self.run_id,
                    "started_at": started_at,
                    "status": "running",
                    "source_url": self.source_url,
                    "mode": self.mode,
                },
            )
            conn.commit()

    def _record_run_end(self, status: str) -> None:
        if self.config.get("type") == "sqlite":
            return
        ended_at = datetime.now(timezone.utc).isoformat()
        with self.engine.connect() as conn:
            conn.execute(
                text(
                    """
                    UPDATE pipeline_runs
                    SET ended_at = :ended_at, status = :status
                    WHERE run_id = :run_id
                    """
                ),
                {"run_id": self.run_id, "ended_at": ended_at, "status": status},
            )
            conn.commit()

    def _record_table_load(self, table_name: str, rows: int) -> None:
        if self.config.get("type") == "sqlite":
            upsert_sql = (
                """
                INSERT INTO pipeline_run_tables (run_id, table_name, rows_loaded)
                VALUES (:run_id, :table_name, :rows_loaded)
                ON CONFLICT(run_id, table_name)
                DO UPDATE SET rows_loaded = excluded.rows_loaded
                """
            )
        else:
            upsert_sql = (
                """
                INSERT INTO pipeline_run_tables (run_id, table_name, rows_loaded)
                VALUES (:run_id, :table_name, :rows_loaded)
                ON DUPLICATE KEY UPDATE rows_loaded = VALUES(rows_loaded)
                """
            )

        with self.engine.connect() as conn:
            conn.execute(
                text(upsert_sql),
                {"run_id": self.run_id, "table_name": table_name, "rows_loaded": rows},
            )
            conn.commit()

    def _quote(self, identifier: str) -> str:
        if self.config.get("type") == "sqlite":
            return f'"{identifier}"'
        return f"`{identifier}`"

    def _validate_df(self, df: pd.DataFrame, table_name: str) -> None:
        issues = validate_dataframe(table_name, df)
        if issues:
            message = f"Schema validation issues for {table_name}: {issues}"
            if self.strict_schema:
                raise ValueError(message)
            self.logger.warning(message)

    def _coerce_df(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        contract = SCHEMA_CONTRACTS.get(table_name)
        if not contract or df.empty:
            return df

        for col in contract.get("string", []):
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        for col in contract.get("numeric", []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in contract.get("datetime", []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        return df

    def _load_table_full_refresh(self, df: pd.DataFrame, table_name: str) -> None:
        with self.engine.connect() as conn:
            conn.execute(text(f"DELETE FROM {self._quote(table_name)}"))
            conn.commit()
        df.to_sql(table_name, self.engine, if_exists="append", index=False)

    def _load_table_incremental(self, df: pd.DataFrame, table_name: str) -> None:
        staging_table = f"_stg_{table_name}"
        df.to_sql(staging_table, self.engine, if_exists="replace", index=False)

        columns = [self._quote(col) for col in df.columns]
        column_list = ", ".join(columns)
        select_list = ", ".join(columns)

        if self.config.get("type") == "sqlite":
            upsert_sql = (
                f"INSERT OR REPLACE INTO {table_name} ({column_list}) "
                f"SELECT {select_list} FROM {staging_table}"
            )
        else:
            update_clause = ", ".join([f"{self._quote(col)}=VALUES({self._quote(col)})" for col in df.columns])
            upsert_sql = (
                f"INSERT INTO {table_name} ({column_list}) "
                f"SELECT {select_list} FROM {staging_table} "
                f"ON DUPLICATE KEY UPDATE {update_clause}"
            )

        with self.engine.connect() as conn:
            conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
            conn.commit()

    def _load_table(self, df: pd.DataFrame, table_name: str) -> None:
        if df.empty:
            self.logger.info("Skipping %s: no rows to load.", table_name)
            return

        df = self._coerce_df(df, table_name)
        self._validate_df(df, table_name)

        try:
            if self.mode == "incremental":
                self._load_table_incremental(df, table_name)
            else:
                self._load_table_full_refresh(df, table_name)
            self.logger.info("Loaded %s rows into %s.", len(df), table_name)
            self._record_table_load(table_name, len(df))
        except Exception as exc:
            self.logger.error("Error loading %s: %s", table_name, exc)
            raise

    # Each spec: (table_name, csv_path, columns_to_keep, datetime_cols, fillna_defaults)
    # csv_path is relative to processed_path unless prefixed with "raw:"
    _TABLE_SPECS = [
        ("seasons",               "raw:seasons.csv",                 None,  [], {}),
        ("circuits",              "circuits_clean.csv",              None,  [], {}),
        ("constructors",          "raw:constructors.csv",            None,  [], {}),
        ("drivers",               "drivers_clean.csv",               None,  ["dob"], {}),
        ("races",                 "races_clean.csv",
            ["race_id", "year", "round", "circuit_id", "race_name", "race_date", "race_time", "url"],
            ["race_date"], {"race_time": "00:00:00"}),
        ("results",               "results_clean.csv",
            ["race_id", "driver_id", "constructor_id", "number", "grid", "position",
             "position_text", "position_order", "points", "laps", "time_result",
             "milliseconds", "fastest_lap", "fastest_lap_rank", "fastest_lap_time",
             "fastest_lap_speed", "status"],
            [], {}),
        ("qualifying",            "qualifying_clean.csv",
            ["race_id", "driver_id", "constructor_id", "number", "position", "q1", "q2", "q3"],
            [], {}),
        ("pit_stops",             "pit_stops_clean.csv",
            ["race_id", "driver_id", "stop", "lap", "time_of_day", "duration", "milliseconds"],
            [], {"time_of_day": "00:00:00"}),
        ("constructor_standings", "constructor_standings_clean.csv",
            ["race_id", "constructor_id", "points", "position", "position_text", "wins"],
            [], {}),
        ("driver_standings",      "driver_standings_clean.csv",
            ["race_id", "driver_id", "points", "position", "position_text", "wins"],
            [], {}),
    ]

    def _load_from_spec(self, table: str, csv_name: str, cols, datetime_cols, fillna_defaults) -> None:
        """Read a CSV and load it into `table`, applying column filtering and coercions."""
        if csv_name.startswith("raw:"):
            path = os.path.join(self.raw_path, csv_name[4:])
        else:
            path = os.path.join(self.processed_path, csv_name)

        if not os.path.exists(path) or os.path.getsize(path) < 10:
            self.logger.info("Skipping %s: file missing or empty.", table)
            return

        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            self.logger.warning("%s has no columns; skipping load.", csv_name)
            return

        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        for col, default in fillna_defaults.items():
            if col in df.columns:
                df[col] = df[col].fillna(default)
        if cols:
            df = df[[c for c in cols if c in df.columns]]

        self._load_table(df, table)

    def load_all(self) -> None:
        """Load all transformed data into the configured database."""
        self.logger.info("Starting data loading into database.")
        self._record_run_start()

        try:
            for spec in self._TABLE_SPECS:
                self.logger.info("Loading %s...", spec[0])
                self._load_from_spec(*spec)

            self._record_run_end("success")
            self.logger.info("All data loaded successfully into database.")

        except Exception:
            self._record_run_end("failed")
            self.logger.exception("Error during loading.")
            raise


def main() -> None:
    loader = F1DataLoader()
    loader.load_all()


if __name__ == "__main__":
    main()
