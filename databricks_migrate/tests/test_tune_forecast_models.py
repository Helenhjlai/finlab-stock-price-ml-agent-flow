import unittest

import numpy as np
import pandas as pd

from databricks_migrate.databricks_src.tune_forecast_models import (
    make_walk_forward_folds,
    parse_models,
    score_predictions,
)


class WalkForwardFoldTests(unittest.TestCase):
    def setUp(self):
        dates = pd.date_range("2020-01-15", periods=30, freq="MS")
        self.frame = pd.DataFrame(
            {
                "date": dates.repeat(2),
                "stock_id": ["1101", "2330"] * len(dates),
                "rank": [0.25, 0.75] * len(dates),
            }
        )

    def test_folds_expand_and_keep_test_untouched(self):
        folds, test_dates = make_walk_forward_folds(
            self.frame,
            date_column="date",
            cv_splits=3,
            validation_periods=3,
            test_periods=6,
            label_horizon_periods=1,
        )

        self.assertEqual([fold.validation["date"].nunique() for fold in folds], [3, 3, 3])
        self.assertEqual(len(test_dates), 6)
        self.assertLess(folds[-1].validation["date"].max(), test_dates[0])
        self.assertLess(len(folds[0].train), len(folds[-1].train))
        for fold in folds:
            used = set(fold.train["date"]) | set(fold.validation["date"])
            self.assertTrue(set(fold.purged_dates).isdisjoint(used))

    def test_mean_period_ic_rewards_correct_cross_sectional_order(self):
        validation = self.frame.iloc[:6].copy()
        perfect = np.tile([0.1, 0.9], 3)
        score = score_predictions(
            validation,
            perfect,
            date_column="date",
            label_column="rank",
            objective="mean_period_ic",
        )
        self.assertAlmostEqual(score, 1.0)

    def test_model_parser_rejects_unknown_model(self):
        self.assertEqual(parse_models("lgb,rf,lgb"), ("lgb", "rf"))
        with self.assertRaisesRegex(ValueError, "must contain"):
            parse_models("lgb,xgboost")


if __name__ == "__main__":
    unittest.main()
