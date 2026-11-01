import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from databricks_migrate.databricks_src.train_forecast_models import (
    TrainConfig,
    build_train_config,
    chronological_split,
    parse_args,
    prepare_dataset,
)


class PrepareDatasetTests(unittest.TestCase):
    def setUp(self):
        self.config = TrainConfig(validation_periods=2, test_periods=2)

    def test_infers_features_and_excludes_labels_and_metadata(self):
        frame = pd.DataFrame(
            {
                "date": ["2024-01-15", "2024-02-15"],
                "stock_id": ["1101", "1101"],
                "feature_a": [1.0, 2.0],
                "feature_as_text": ["3.0", "4.0"],
                "rank": [0.2, 0.8],
                "return": [0.9, 1.1],
                "_run_id": ["a", "b"],
            }
        )

        prepared, features = prepare_dataset(frame, self.config, None)

        self.assertEqual(features, ["feature_a", "feature_as_text"])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(prepared["date"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(prepared["feature_as_text"]))

    def test_duplicate_date_stock_key_fails(self):
        frame = pd.DataFrame(
            {
                "date": ["2024-01-15", "2024-01-15"],
                "stock_id": ["1101", "1101"],
                "feature": [1.0, 2.0],
                "rank": [0.2, 0.8],
            }
        )

        with self.assertRaisesRegex(ValueError, "duplicate keys"):
            prepare_dataset(frame, self.config, ["feature"])

    def test_rank_outside_unit_interval_fails(self):
        frame = pd.DataFrame(
            {
                "date": ["2024-01-15", "2024-02-15"],
                "stock_id": ["1101", "1101"],
                "feature": [1.0, 2.0],
                "rank": [0.2, 1.2],
            }
        )

        with self.assertRaisesRegex(ValueError, r"within \[0, 1\]"):
            prepare_dataset(frame, self.config, ["feature"])


class ChronologicalSplitTests(unittest.TestCase):
    def test_split_uses_whole_periods_and_purges_both_boundaries(self):
        dates = pd.date_range("2024-01-15", periods=10, freq="MS")
        frame = pd.DataFrame(
            {
                "date": dates.repeat(2),
                "stock_id": ["1101", "2330"] * 10,
                "feature": range(20),
                "rank": [0.25, 0.75] * 10,
            }
        )
        config = TrainConfig(
            validation_periods=2,
            test_periods=2,
            label_horizon_periods=1,
        )

        split = chronological_split(frame, config)

        self.assertEqual(split.train["date"].nunique(), 4)
        self.assertEqual(split.validation["date"].nunique(), 2)
        self.assertEqual(split.test["date"].nunique(), 2)
        self.assertEqual(len(split.purged_dates), 2)
        self.assertLess(split.train["date"].max(), split.validation["date"].min())
        self.assertLess(split.validation["date"].max(), split.test["date"].min())
        used_dates = set(split.train["date"]) | set(split.validation["date"]) | set(
            split.test["date"]
        )
        self.assertTrue(set(split.purged_dates).isdisjoint(used_dates))


class TunedConfigTests(unittest.TestCase):
    def test_params_file_is_loaded_and_explicit_cli_wins(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "best_params.json"
            path.write_text(
                '{"training_config_overrides": {'
                '"lgb_num_leaves": 63, "rf_max_features": 0.5, '
                '"nn_hidden_units": [128, 64]}}',
                encoding="utf-8",
            )
            args = parse_args(
                [
                    "--input-path",
                    "unused.csv",
                    "--output-dir",
                    directory,
                    "--params-file",
                    str(path),
                    "--lgb-num-leaves",
                    "31",
                ]
            )

            config = build_train_config(args)

            self.assertEqual(config.lgb_num_leaves, 31)
            self.assertEqual(config.rf_max_features, 0.5)
            self.assertEqual(config.nn_hidden_units, (128, 64))

    def test_params_file_rejects_unknown_keys(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text('{"drop_table": true}', encoding="utf-8")
            args = parse_args(
                [
                    "--input-path",
                    "unused.csv",
                    "--output-dir",
                    directory,
                    "--params-file",
                    str(path),
                ]
            )
            with self.assertRaisesRegex(ValueError, "Unsupported keys"):
                build_train_config(args)


if __name__ == "__main__":
    unittest.main()
