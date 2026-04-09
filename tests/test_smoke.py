import csv
import os
import sqlite3
import tempfile
import unittest

from scripts.transform_data import F1DataTransformer
from scripts.load_data import F1DataLoader


def write_csv(path, headers, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(headers)
        writer.writerows(rows)


class TestPipelineSmoke(unittest.TestCase):
    def test_transform_and_load_minimal_dataset(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            raw_dir = os.path.join(tmp_dir, "raw")
            processed_dir = os.path.join(tmp_dir, "processed")

            write_csv(
                os.path.join(raw_dir, "circuits.csv"),
                [
                    "circuit_ref",
                    "circuit_name",
                    "location",
                    "country",
                    "lat",
                    "lng",
                    "altitude",
                    "url",
                ],
                [
                    [
                        "silverstone",
                        "Silverstone Circuit",
                        "Silverstone",
                        "UK",
                        "52.07",
                        "-1.02",
                        "0",
                        "http://example.com/circuit",
                    ]
                ],
            )

            write_csv(
                os.path.join(raw_dir, "seasons.csv"),
                ["year", "url"],
                [[2024, "http://example.com/season/2024"]],
            )

            write_csv(
                os.path.join(raw_dir, "constructors.csv"),
                [
                    "constructor_id",
                    "constructor_ref",
                    "constructor_name",
                    "nationality",
                    "url",
                ],
                [
                    [
                        9,
                        "red_bull",
                        "Red Bull",
                        "Austrian",
                        "http://example.com/constructor",
                    ]
                ],
            )

            write_csv(
                os.path.join(raw_dir, "drivers.csv"),
                [
                    "driver_ref",
                    "driver_number",
                    "code",
                    "forename",
                    "surname",
                    "dob",
                    "nationality",
                    "url",
                ],
                [
                    [
                        "max_verstappen",
                        33,
                        "VER",
                        "Max",
                        "Verstappen",
                        "1997-09-30",
                        "Dutch",
                        "http://example.com/driver",
                    ]
                ],
            )

            write_csv(
                os.path.join(raw_dir, "races.csv"),
                [
                    "race_id",
                    "year",
                    "round",
                    "circuit_ref",
                    "race_name",
                    "race_date",
                    "race_time",
                    "url",
                ],
                [
                    [
                        202401,
                        2024,
                        1,
                        "silverstone",
                        "British Grand Prix",
                        "2024-07-07",
                        "14:00:00",
                        "http://example.com/race",
                    ]
                ],
            )

            write_csv(
                os.path.join(raw_dir, "results.csv"),
                [
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
                ],
                [
                    [
                        202401,
                        "max_verstappen",
                        "red_bull",
                        33,
                        1,
                        1,
                        "1",
                        1,
                        25,
                        52,
                        "1:30:00",
                        5400000,
                        12,
                        1,
                        "1:20.000",
                        "220.5",
                        "Finished",
                    ]
                ],
            )

            write_csv(
                os.path.join(raw_dir, "qualifying.csv"),
                [
                    "race_id",
                    "driver_ref",
                    "constructor_ref",
                    "number",
                    "position",
                    "q1",
                    "q2",
                    "q3",
                ],
                [[202401, "max_verstappen", "red_bull", 33, 1, "1:21.0", "1:20.5", "1:20.0"]],
            )

            write_csv(
                os.path.join(raw_dir, "pit_stops.csv"),
                [
                    "race_id",
                    "driver_ref",
                    "stop",
                    "lap",
                    "time_of_day",
                    "duration",
                    "milliseconds",
                ],
                [[202401, "max_verstappen", 1, 20, "14:40:00", "2.4", 2400]],
            )

            write_csv(
                os.path.join(raw_dir, "constructor_standings.csv"),
                [
                    "race_id",
                    "constructor_ref",
                    "points",
                    "position",
                    "position_text",
                    "wins",
                ],
                [[202401, "red_bull", 25, 1, "1", 1]],
            )

            write_csv(
                os.path.join(raw_dir, "driver_standings.csv"),
                [
                    "race_id",
                    "driver_ref",
                    "points",
                    "position",
                    "position_text",
                    "wins",
                ],
                [[202401, "max_verstappen", 25, 1, "1", 1]],
            )

            transformer = F1DataTransformer(raw_data_path=raw_dir + "/", processed_data_path=processed_dir + "/")
            transformer.transform_all()

            db_path = os.path.join(tmp_dir, "f1_analytics.db")
            loader = F1DataLoader(
                config={"type": "sqlite", "filename": db_path},
                processed_data_path=processed_dir + "/",
            )
            loader.load_all()

            self.assertTrue(os.path.exists(db_path))

            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM results")
                count = cur.fetchone()[0]

            self.assertGreater(count, 0)


if __name__ == "__main__":
    unittest.main()
