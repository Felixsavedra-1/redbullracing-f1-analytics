"""
Data transformation utilities for F1 analytics.
"""

import os
import sys
import pandas as pd
from datetime import datetime


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from logging_utils import setup_logging


class F1DataTransformer:
    """Transform and clean F1 data for database loading."""

    def __init__(self, raw_data_path: str = "data/raw/", processed_data_path: str = "data/processed/") -> None:
        self.raw_path = raw_data_path
        self.processed_path = processed_data_path
        self.logger = setup_logging()
        os.makedirs(raw_data_path, exist_ok=True)
        os.makedirs(processed_data_path, exist_ok=True)

    def transform_circuits(self) -> pd.DataFrame:
        """Clean and normalize circuits data."""
        path = f"{self.raw_path}circuits.csv"
        if not os.path.exists(path) or os.path.getsize(path) < 10:
            self.logger.warning("circuits.csv is empty or missing; writing empty output.")
            empty = pd.DataFrame()
            empty.to_csv(f"{self.processed_path}circuits_clean.csv", index=False)
            return empty

        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            self.logger.warning("qualifying.csv has no columns; writing empty output.")
            empty = pd.DataFrame()
            empty.to_csv(f"{self.processed_path}qualifying_clean.csv", index=False)
            return empty
        df["circuit_id"] = range(1, len(df) + 1)
        df["altitude"] = df["altitude"].fillna(0)
        df = df[[
            "circuit_id",
            "circuit_ref",
            "circuit_name",
            "location",
            "country",
            "lat",
            "lng",
            "altitude",
            "url",
        ]]
        df.to_csv(f"{self.processed_path}circuits_clean.csv", index=False)
        self.logger.info("Transformed %s circuits.", len(df))
        return df

    def transform_drivers(self) -> pd.DataFrame:
        """Clean and normalize driver data."""
        df = pd.read_csv(f"{self.raw_path}drivers.csv")

        if "driver_id" not in df.columns:
            df["driver_id"] = range(1, len(df) + 1)

        if "dob" in df.columns:
            df["dob"] = pd.to_datetime(df["dob"], errors="coerce")

        if "driver_number" in df.columns:
            df["driver_number"] = df["driver_number"].fillna(0).astype(int)
        else:
            df["driver_number"] = 0

        required_cols = [
            "driver_id",
            "driver_ref",
            "driver_number",
            "code",
            "forename",
            "surname",
            "dob",
            "nationality",
            "url",
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = "" if col in {"code", "url"} else None

        df = df[required_cols]
        df.to_csv(f"{self.processed_path}drivers_clean.csv", index=False)
        self.logger.info("Transformed %s drivers.", len(df))
        return df

    def transform_races(self) -> pd.DataFrame:
        """Clean and normalize race data."""
        df = pd.read_csv(f"{self.raw_path}races.csv")

        if "race_date" in df.columns:
            df["race_date"] = pd.to_datetime(df["race_date"], errors="coerce")

        if "race_time" in df.columns:
            df["race_time"] = df["race_time"].fillna("00:00:00")
        else:
            df["race_time"] = "00:00:00"

        if "circuit_ref" in df.columns:
            try:
                circuits_df = pd.read_csv(f"{self.raw_path}circuits.csv")
                if "circuit_id" not in circuits_df.columns:
                    circuits_df["circuit_id"] = range(1, len(circuits_df) + 1)
                circuit_map = dict(zip(circuits_df["circuit_ref"], circuits_df["circuit_id"]))
                df["circuit_id"] = df["circuit_ref"].map(circuit_map).fillna(0).astype(int)
            except Exception:
                self.logger.warning("Could not map circuit_ref to circuit_id; defaulting to 0.")
                df["circuit_id"] = 0

        if "race_id" in df.columns:
            df["race_id"] = df["race_id"].astype(int)
        else:
            df["race_id"] = (
                df["year"].astype(str) + df["round"].astype(str).str.zfill(2)
            ).astype(int)

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
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
        df = df[required_cols]

        df.to_csv(f"{self.processed_path}races_clean.csv", index=False)
        self.logger.info("Transformed %s races.", len(df))
        return df

    def transform_results(self) -> pd.DataFrame:
        """Clean and normalize race results."""
        path = f"{self.raw_path}results.csv"
        results_columns = [
            "race_id",
            "driver_ref",
            "constructor_ref",
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
            "status",
        ]
        if not os.path.exists(path) or os.path.getsize(path) < 10:
            self.logger.warning("results.csv is empty or missing; writing empty output.")
            empty = pd.DataFrame(columns=results_columns)
            empty.to_csv(f"{self.processed_path}results_clean.csv", index=False)
            return empty

        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            self.logger.warning("results.csv has no columns; writing empty output.")
            empty = pd.DataFrame(columns=results_columns)
            empty.to_csv(f"{self.processed_path}results_clean.csv", index=False)
            return empty

        try:
            drivers_df = pd.read_csv(f"{self.raw_path}drivers.csv")
            if "driver_id" not in drivers_df.columns:
                drivers_df["driver_id"] = range(1, len(drivers_df) + 1)
            driver_map = dict(zip(drivers_df["driver_ref"], drivers_df["driver_id"]))
            df["driver_id"] = df["driver_ref"].map(driver_map)
        except Exception:
            self.logger.warning("Could not map driver_ref to driver_id; defaulting to 0.")
            df["driver_id"] = 0

        try:
            constructors_df = pd.read_csv(f"{self.raw_path}constructors.csv")
            constructor_map = dict(
                zip(constructors_df["constructor_ref"], constructors_df["constructor_id"])
            )
            df["constructor_id"] = df["constructor_ref"].map(constructor_map)
        except Exception:
            self.logger.warning("Could not map constructor_ref to constructor_id; defaulting to 0.")
            df["constructor_id"] = 0

        if "position" in df.columns:
            df["position"] = pd.to_numeric(df["position"], errors="coerce")

        if "position_text" in df.columns:
            df["position_text"] = df["position_text"].fillna("").astype(str)
        else:
            df["position_text"] = ""

        numeric_cols = [
            "points",
            "laps",
            "grid",
            "number",
            "fastest_lap",
            "fastest_lap_rank",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        if "fastest_lap_speed" in df.columns:
            df["fastest_lap_speed"] = df["fastest_lap_speed"].fillna("").astype(str)
        else:
            df["fastest_lap_speed"] = ""

        if "milliseconds" in df.columns:
            df["milliseconds"] = pd.to_numeric(
                df["milliseconds"], errors="coerce"
            ).fillna(0).astype(int)
        else:
            df["milliseconds"] = 0

        status_map = {
            "Finished": 1,
            "+1 Lap": 11,
            "+2 Laps": 12,
            "+3 Laps": 13,
            "Retired": 14,
            "Disqualified": 2,
            "Accident": 3,
            "Collision": 4,
            "Engine": 5,
        }
        if "status" in df.columns:
            df["status_id"] = df["status"].map(status_map).fillna(14)
        else:
            df["status_id"] = 1
            df["status"] = ""

        if "position_order" not in df.columns:
            df["position_order"] = df["position"].fillna(999)
        df["position_order"] = pd.to_numeric(
            df["position_order"], errors="coerce"
        ).fillna(999).astype(int)

        df.to_csv(f"{self.processed_path}results_clean.csv", index=False)
        self.logger.info("Transformed %s results.", len(df))
        return df

    def transform_qualifying(self) -> pd.DataFrame:
        """Clean and normalize qualifying data."""
        qualifying_columns = [
            "race_id",
            "driver_ref",
            "constructor_ref",
            "number",
            "position",
            "q1",
            "q2",
            "q3",
        ]
        path = f"{self.raw_path}qualifying.csv"
        if not os.path.exists(path) or os.path.getsize(path) < 10:
            self.logger.warning("qualifying.csv is empty or missing; writing empty output.")
            empty = pd.DataFrame(columns=qualifying_columns)
            empty.to_csv(f"{self.processed_path}qualifying_clean.csv", index=False)
            return empty

        try:
            df = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            self.logger.warning("qualifying.csv has no columns; writing empty output.")
            empty = pd.DataFrame(columns=qualifying_columns)
            empty.to_csv(f"{self.processed_path}qualifying_clean.csv", index=False)
            return empty

        try:
            drivers_df = pd.read_csv(f"{self.raw_path}drivers.csv")
            if "driver_id" not in drivers_df.columns:
                drivers_df["driver_id"] = range(1, len(drivers_df) + 1)
            driver_map = dict(zip(drivers_df["driver_ref"], drivers_df["driver_id"]))
            df["driver_id"] = df["driver_ref"].map(driver_map)
        except Exception:
            self.logger.warning("Could not map driver_ref to driver_id; defaulting to 0.")
            df["driver_id"] = 0

        try:
            constructors_df = pd.read_csv(f"{self.raw_path}constructors.csv")
            constructor_map = dict(
                zip(constructors_df["constructor_ref"], constructors_df["constructor_id"])
            )
            df["constructor_id"] = df["constructor_ref"].map(constructor_map)
        except Exception:
            self.logger.warning("Could not map constructor_ref to constructor_id; defaulting to 0.")
            df["constructor_id"] = 0

        for col in ["q1", "q2", "q3"]:
            if col in df.columns:
                df[col] = df[col].fillna("")
            else:
                df[col] = ""

        for col in ["position", "number"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        df.to_csv(f"{self.processed_path}qualifying_clean.csv", index=False)
        self.logger.info("Transformed %s qualifying results.", len(df))
        return df

    def transform_pit_stops(self) -> pd.DataFrame:
        """Clean and normalize pit stop data."""
        path = f"{self.raw_path}pit_stops.csv"
        if not os.path.exists(path) or os.path.getsize(path) < 10:
            self.logger.warning("pit_stops.csv is empty or missing; writing empty output.")
            empty = pd.DataFrame()
            empty.to_csv(f"{self.processed_path}pit_stops_clean.csv", index=False)
            return empty

        df = pd.read_csv(path)

        try:
            drivers_df = pd.read_csv(f"{self.raw_path}drivers.csv")
            if "driver_id" not in drivers_df.columns:
                drivers_df["driver_id"] = range(1, len(drivers_df) + 1)
            driver_map = dict(zip(drivers_df["driver_ref"], drivers_df["driver_id"]))
            df["driver_id"] = df["driver_ref"].map(driver_map)
        except Exception:
            self.logger.warning("Could not map driver_ref to driver_id; defaulting to 0.")
            df["driver_id"] = 0

        if "time_of_day" in df.columns:
            df["time_of_day"] = df["time_of_day"].fillna("00:00:00")
        else:
            df["time_of_day"] = "00:00:00"

        if "milliseconds" not in df.columns or df["milliseconds"].isna().all():
            if "duration" in df.columns:
                df["milliseconds"] = pd.to_numeric(
                    df["duration"], errors="coerce"
                ) * 1000
            else:
                df["milliseconds"] = 0

        df["milliseconds"] = pd.to_numeric(
            df["milliseconds"], errors="coerce"
        ).fillna(0).astype(int)

        df.to_csv(f"{self.processed_path}pit_stops_clean.csv", index=False)
        self.logger.info("Transformed %s pit stops.", len(df))
        return df

    def transform_standings(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Clean and normalize constructor and driver standings."""
        const_path = f"{self.raw_path}constructor_standings.csv"
        if not os.path.exists(const_path) or os.path.getsize(const_path) < 10:
            self.logger.warning("constructor_standings.csv is empty or missing; skipping.")
            df_const = pd.DataFrame()
        else:
            df_const = pd.read_csv(const_path)

        if not df_const.empty:
            try:
                constructors_df = pd.read_csv(f"{self.raw_path}constructors.csv")
                constructor_map = dict(
                    zip(constructors_df["constructor_ref"], constructors_df["constructor_id"])
                )
                df_const["constructor_id"] = df_const["constructor_ref"].map(constructor_map)
            except Exception:
                self.logger.warning("Could not map constructor_ref to constructor_id; defaulting to 0.")
                df_const["constructor_id"] = 0

            df_const["points"] = df_const["points"].fillna(0)
            df_const["wins"] = df_const["wins"].fillna(0)
            df_const.to_csv(
                f"{self.processed_path}constructor_standings_clean.csv", index=False
            )
            self.logger.info("Transformed %s constructor standings.", len(df_const))
        else:
            self.logger.info("No constructor standings to transform.")

        driver_path = f"{self.raw_path}driver_standings.csv"
        if not os.path.exists(driver_path) or os.path.getsize(driver_path) < 10:
            self.logger.warning("driver_standings.csv is empty or missing; skipping.")
            df_driver = pd.DataFrame()
        else:
            df_driver = pd.read_csv(driver_path)

        if not df_driver.empty:
            try:
                drivers_df = pd.read_csv(f"{self.raw_path}drivers.csv")
                if "driver_id" not in drivers_df.columns:
                    drivers_df["driver_id"] = range(1, len(drivers_df) + 1)
                driver_map = dict(zip(drivers_df["driver_ref"], drivers_df["driver_id"]))
                df_driver["driver_id"] = df_driver["driver_ref"].map(driver_map)
            except Exception:
                self.logger.warning("Could not map driver_ref to driver_id; defaulting to 0.")
                df_driver["driver_id"] = 0

            df_driver["points"] = df_driver["points"].fillna(0)
            df_driver["wins"] = df_driver["wins"].fillna(0)
            df_driver.to_csv(
                f"{self.processed_path}driver_standings_clean.csv", index=False
            )
            self.logger.info("Transformed %s driver standings.", len(df_driver))
        else:
            self.logger.info("No driver standings to transform.")

        return df_const, df_driver

    def transform_all(self) -> None:
        """Run all transformations in sequence."""
        self.logger.info("Starting data transformation.")

        try:
            self.transform_circuits()
            self.transform_drivers()
            self.transform_races()
            self.transform_results()
            self.transform_qualifying()
            self.transform_pit_stops()
            self.transform_standings()

            self.logger.info("All transformations completed.")
            self.logger.info("Cleaned data written to: %s", self.processed_path)
        except Exception as exc:
            self.logger.error("Error during transformation: %s", exc)
            import traceback

            traceback.print_exc()
            raise


def main() -> None:
    transformer = F1DataTransformer()
    transformer.transform_all()


if __name__ == "__main__":
    main()
