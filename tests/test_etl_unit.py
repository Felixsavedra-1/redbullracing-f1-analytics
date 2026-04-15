"""Unit tests for ETL edge cases: missing files, unmapped refs, schema violations, DNF sentinel."""
import csv
import os
import tempfile
import unittest

import pandas as pd

from scripts.transform_data import F1DataTransformer
from scripts.load_data import F1DataLoader
from scripts.constants import DNF_POSITION_ORDER


def write_csv(path, headers, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)


class TestReadCsvSafe(unittest.TestCase):
    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            t = F1DataTransformer(raw_data_path=tmp + "/", processed_data_path=tmp + "/")
            self.assertIsNone(t._read_csv_safe("does_not_exist.csv"))

    def test_empty_file_returns_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "empty.csv")
            open(path, "w").close()
            t = F1DataTransformer(raw_data_path=tmp + "/", processed_data_path=tmp + "/")
            self.assertIsNone(t._read_csv_safe("empty.csv"))

    def test_valid_file_returns_dataframe(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_csv(os.path.join(tmp, "data.csv"), ["a", "b"], [[1, 2]])
            t = F1DataTransformer(raw_data_path=tmp + "/", processed_data_path=tmp + "/")
            df = t._read_csv_safe("data.csv")
            self.assertIsNotNone(df)
            self.assertEqual(len(df), 1)


class TestTransformMissingFiles(unittest.TestCase):
    def _transformer(self, tmp):
        return F1DataTransformer(raw_data_path=tmp + "/", processed_data_path=tmp + "/")

    def test_transform_races_missing_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            df = self._transformer(tmp).transform_races()
            self.assertTrue(df.empty)

    def test_transform_circuits_missing_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            df = self._transformer(tmp).transform_circuits()
            self.assertTrue(df.empty)

    def test_transform_drivers_missing_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            df = self._transformer(tmp).transform_drivers()
            self.assertTrue(df.empty)


class TestApplyRefMap(unittest.TestCase):
    def test_unmapped_refs_become_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_csv(os.path.join(tmp, "constructors.csv"),
                      ["constructor_id", "constructor_ref"],
                      [[9, "red_bull"]])
            t = F1DataTransformer(raw_data_path=tmp + "/", processed_data_path=tmp + "/")
            df = pd.DataFrame({"constructor_ref": ["red_bull", "unknown_team"]})
            result = t._apply_ref_map(df, "constructor_ref", "constructor_id", "constructors.csv")
            self.assertEqual(result.loc[result["constructor_ref"] == "red_bull", "constructor_id"].iloc[0], 9)
            self.assertEqual(result.loc[result["constructor_ref"] == "unknown_team", "constructor_id"].iloc[0], 0)

    def test_known_refs_are_mapped(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_csv(os.path.join(tmp, "drivers.csv"),
                      ["driver_id", "driver_ref"],
                      [[1, "max_verstappen"], [2, "perez"]])
            t = F1DataTransformer(raw_data_path=tmp + "/", processed_data_path=tmp + "/")
            df = pd.DataFrame({"driver_ref": ["max_verstappen", "perez"]})
            result = t._apply_ref_map(df, "driver_ref", "driver_id", "drivers.csv")
            self.assertListEqual(result["driver_id"].tolist(), [1, 2])


class TestDNFSentinel(unittest.TestCase):
    def test_dnf_position_order_constant(self):
        self.assertEqual(DNF_POSITION_ORDER, 999)

    def test_non_finisher_gets_sentinel(self):
        with tempfile.TemporaryDirectory() as tmp:
            write_csv(
                os.path.join(tmp, "results.csv"),
                ["race_id", "driver_ref", "constructor_ref", "number", "grid",
                 "position", "position_text", "position_order", "points", "laps",
                 "time_result", "milliseconds", "fastest_lap", "fastest_lap_rank",
                 "fastest_lap_time", "fastest_lap_speed", "status"],
                [[202401, "max_verstappen", "red_bull", 33, 1,
                  "", "R", 999, 0, 10, "", 0, 0, 0, "", "", "Engine"]],
            )
            write_csv(os.path.join(tmp, "drivers.csv"),
                      ["driver_id", "driver_ref"], [[1, "max_verstappen"]])
            write_csv(os.path.join(tmp, "constructors.csv"),
                      ["constructor_id", "constructor_ref"], [[9, "red_bull"]])

            t = F1DataTransformer(raw_data_path=tmp + "/", processed_data_path=tmp + "/")
            df = t.transform_results()
            self.assertEqual(df["position_order"].iloc[0], DNF_POSITION_ORDER)


class TestStrictSchema(unittest.TestCase):
    def test_strict_schema_raises_on_violation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test.db")
            loader = F1DataLoader(
                config={"type": "sqlite", "filename": db_path},
                processed_data_path=tmp + "/",
                strict_schema=True,
            )
            # circuits contract requires circuit_id; omitting it should raise
            bad_df = pd.DataFrame({"circuit_name": ["Silverstone"]})
            with self.assertRaises(ValueError):
                loader._validate_df(bad_df, "circuits")

    def test_lenient_schema_warns_not_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "test.db")
            loader = F1DataLoader(
                config={"type": "sqlite", "filename": db_path},
                processed_data_path=tmp + "/",
                strict_schema=False,
            )
            bad_df = pd.DataFrame({"circuit_name": ["Silverstone"]})
            # should not raise
            loader._validate_df(bad_df, "circuits")


class TestNormalizeProgress(unittest.TestCase):
    def _extractor(self, tmp):
        from scripts.extract_data import F1DataExtractor
        return F1DataExtractor(output_path=tmp + "/")

    def test_canonical_format_roundtrips(self):
        with tempfile.TemporaryDirectory() as tmp:
            e = self._extractor(tmp)
            data = {"years": {"2020": [1, 2, 3]}, "skipped": {"2020": [4]}}
            result = e._normalize_progress(data, 2020, 2020)
            self.assertEqual(result["years"]["2020"], [1, 2, 3])
            self.assertEqual(result["skipped"]["2020"], [4])

    def test_legacy_flat_format(self):
        with tempfile.TemporaryDirectory() as tmp:
            e = self._extractor(tmp)
            data = {"2020": [1, 2]}
            result = e._normalize_progress(data, 2020, 2020)
            self.assertEqual(result["years"]["2020"], [1, 2])

    def test_deduplicates_and_sorts(self):
        with tempfile.TemporaryDirectory() as tmp:
            e = self._extractor(tmp)
            data = {"years": {"2020": [3, 1, 2, 1]}, "skipped": {}}
            result = e._normalize_progress(data, 2020, 2020)
            self.assertEqual(result["years"]["2020"], [1, 2, 3])

    def test_out_of_range_years_excluded(self):
        with tempfile.TemporaryDirectory() as tmp:
            e = self._extractor(tmp)
            data = {"years": {"2019": [1], "2020": [1], "2021": [1]}, "skipped": {}}
            result = e._normalize_progress(data, 2020, 2020)
            self.assertNotIn("2019", result["years"])
            self.assertNotIn("2021", result["years"])


if __name__ == "__main__":
    unittest.main()
