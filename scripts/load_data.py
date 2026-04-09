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

try:
    from config import DB_CONFIG, DATA_PATHS
except ImportError:
    print("config.py not found; using default database settings.")
    print("Copy scripts/config.example.py to scripts/config.py and configure your database.")
    DB_CONFIG = {
        "type": "sqlite",
        "filename": "f1_analytics.db",
    }
    DATA_PATHS = {
        "processed_data": "data/processed/",
    }


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

                connection_string = f"sqlite:///{db_file}"
                self.logger.info("Connecting to SQLite database at %s.", db_file)
            else:
                connection_string = (
                    f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
                    f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
                )
                self.logger.info("Connecting to MySQL at %s.", self.config.get("host"))

            self.engine = create_engine(connection_string)

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
            conn.execute(text(f"DELETE FROM {table_name}"))
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
            update_cols = [col for col in df.columns]
            update_clause = ", ".join([f"{self._quote(col)}=VALUES({self._quote(col)})" for col in update_cols])
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

    def load_circuits(self) -> None:
        self.logger.info("Loading circuits...")
        df = pd.read_csv(f"{self.processed_path}circuits_clean.csv")
        self._load_table(df, "circuits")

    def load_seasons(self) -> None:
        self.logger.info("Loading seasons...")
        df = pd.read_csv(f"{self.processed_path}../raw/seasons.csv")
        self._load_table(df, "seasons")

    def load_constructors(self) -> None:
        self.logger.info("Loading constructors...")
        df = pd.read_csv(f"{self.processed_path}../raw/constructors.csv")
        self._load_table(df, "constructors")

    def load_drivers(self) -> None:
        self.logger.info("Loading drivers...")
        df = pd.read_csv(f"{self.processed_path}drivers_clean.csv")

        if "dob" in df.columns:
            df["dob"] = pd.to_datetime(df["dob"], errors="coerce")

        self._load_table(df, "drivers")

    def load_races(self) -> None:
        self.logger.info("Loading races...")
        df = pd.read_csv(f"{self.processed_path}races_clean.csv")

        if "race_date" in df.columns:
            df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")
        if "race_time" in df.columns:
            df["race_time"] = df["race_time"].fillna("00:00:00")

        required_cols = [
            "race_id",
            "year",
            "round",
            "circuit_id",
            "race_name",
            "race_date",
            "race_time",
            "url",
        ]
        df = df[[col for col in required_cols if col in df.columns]]

        self._load_table(df, "races")

    def load_results(self) -> None:
        self.logger.info("Loading results...")
        try:
            df = pd.read_csv(f"{self.processed_path}results_clean.csv")
        except pd.errors.EmptyDataError:
            self.logger.warning("results_clean.csv has no columns; skipping load.")
            return

        required_cols = [
            "race_id",
            "driver_id",
            "constructor_id",
            "number",
            "grid",
            "position",
            "position_text",
            "position_order",
            "points",
            "laps",
            "time_result",
            "milliseconds",
            "fastest_lap",
            "fastest_lap_rank",
            "fastest_lap_time",
            "fastest_lap_speed",
            "status_id",
            "status",
        ]

        df = df[[col for col in required_cols if col in df.columns]]

        self._load_table(df, "results")

    def load_qualifying(self) -> None:
        self.logger.info("Loading qualifying...")
        try:
            df = pd.read_csv(f"{self.processed_path}qualifying_clean.csv")
        except pd.errors.EmptyDataError:
            self.logger.warning("qualifying_clean.csv has no columns; skipping load.")
            return

        required_cols = ["race_id", "driver_id", "constructor_id", "number", "position", "q1", "q2", "q3"]
        df = df[[col for col in required_cols if col in df.columns]]

        self._load_table(df, "qualifying")

    def load_pit_stops(self) -> None:
        self.logger.info("Loading pit stops...")
        df = pd.read_csv(f"{self.processed_path}pit_stops_clean.csv")

        if "time_of_day" in df.columns:
            df["time_of_day"] = df["time_of_day"].fillna("00:00:00")
        required_cols = ["race_id", "driver_id", "stop", "lap", "time_of_day", "duration", "milliseconds"]
        df = df[[col for col in required_cols if col in df.columns]]

        self._load_table(df, "pit_stops")

    def load_standings(self) -> None:
        self.logger.info("Loading standings...")

        const_path = f"{self.processed_path}constructor_standings_clean.csv"
        if not os.path.exists(const_path) or os.path.getsize(const_path) < 10:
            self.logger.info("Skipping constructor standings: no rows to load.")
            df_const = pd.DataFrame()
        else:
            df_const = pd.read_csv(const_path)
        required_cols = ["race_id", "constructor_id", "points", "position", "position_text", "wins"]
        if not df_const.empty:
            df_const = df_const[[col for col in required_cols if col in df_const.columns]]
            self._load_table(df_const, "constructor_standings")

        driver_path = f"{self.processed_path}driver_standings_clean.csv"
        if not os.path.exists(driver_path) or os.path.getsize(driver_path) < 10:
            self.logger.info("Skipping driver standings: no rows to load.")
            df_driver = pd.DataFrame()
        else:
            df_driver = pd.read_csv(driver_path)
        required_cols = ["race_id", "driver_id", "points", "position", "position_text", "wins"]
        if not df_driver.empty:
            df_driver = df_driver[[col for col in required_cols if col in df_driver.columns]]
            self._load_table(df_driver, "driver_standings")

    def load_all(self) -> None:
        """Load all transformed data into the configured database."""
        self.logger.info("Starting data loading into database.")
        self._record_run_start()

        try:
            self.load_seasons()
            self.load_circuits()
            self.load_constructors()
            self.load_drivers()
            self.load_races()
            self.load_results()
            self.load_qualifying()
            self.load_pit_stops()
            self.load_standings()

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
