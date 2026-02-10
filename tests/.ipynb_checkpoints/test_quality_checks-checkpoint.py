import os
import tempfile
import unittest

from sqlalchemy import create_engine, text

from scripts.data_quality import run_quality_checks


def apply_sqlite_schema(engine) -> None:
    schema_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "database",
        "schema",
        "create_tables_sqlite.sql",
    )
    schema_path = os.path.abspath(schema_path)
    with open(schema_path, "r") as handle:
        schema_sql = handle.read()

    with engine.connect() as conn:
        for statement in schema_sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
        conn.commit()


class TestQualityChecks(unittest.TestCase):
    def test_quality_checks_pass_for_minimal_valid_data(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = os.path.join(tmp_dir, "quality.db")
            engine = create_engine(f"sqlite:///{db_path}")
            apply_sqlite_schema(engine)

            with engine.connect() as conn:
                conn.execute(
                    text(
                        "INSERT INTO circuits (circuit_id, circuit_ref, circuit_name, location, country, lat, lng, altitude, url) "
                        "VALUES (1, 'silverstone', 'Silverstone Circuit', 'Silverstone', 'UK', 52.07, -1.02, 0, 'http://example.com')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO seasons (year, url) VALUES (2024, 'http://example.com/season/2024')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO constructors (constructor_id, constructor_ref, constructor_name, nationality, url) "
                        "VALUES (1, 'red_bull', 'Red Bull', 'Austrian', 'http://example.com')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO drivers (driver_id, driver_ref, driver_number, code, forename, surname, dob, nationality, url) "
                        "VALUES (1, 'max_verstappen', 33, 'VER', 'Max', 'Verstappen', '1997-09-30', 'Dutch', 'http://example.com')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO races (race_id, year, round, circuit_id, race_name, race_date, race_time, url) "
                        "VALUES (202401, 2024, 1, 1, 'British Grand Prix', '2024-07-07', '14:00:00', 'http://example.com')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO results (race_id, driver_id, constructor_id, number, grid, position, position_text, position_order, points, laps, "
                        "time_result, milliseconds, fastest_lap, fastest_lap_rank, fastest_lap_time, fastest_lap_speed, status_id, status) "
                        "VALUES (202401, 1, 1, 33, 1, 1, '1', 1, 25, 52, '1:30:00', 5400000, 12, 1, '1:20.000', '220.5', 1, 'Finished')"
                    )
                )
                conn.execute(
                    text(
                        "INSERT INTO qualifying (race_id, driver_id, constructor_id, number, position, q1, q2, q3) "
                        "VALUES (202401, 1, 1, 33, 1, '1:21.0', '1:20.5', '1:20.0')"
                    )
                )
                conn.commit()

            failures = run_quality_checks(engine, start_year=2024, end_year=2024)
            self.assertEqual(failures, [])


if __name__ == "__main__":
    unittest.main()
