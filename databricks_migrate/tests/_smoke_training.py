"""Manual full-stack smoke test; intentionally excluded from unittest discovery."""

from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

from databricks_migrate.databricks_src.train_forecast_models import (
    ForecastModelBundle,
    TrainConfig,
    chronological_split,
    package_bundle,
    prepare_dataset,
    train_models,
)


dates = pd.date_range("2023-01-15", periods=12, freq="MS")
rows = []
for date_index, date in enumerate(dates):
    for stock_index in range(8):
        rows.append(
            {
                "date": date,
                "stock_id": f"{1100 + stock_index}",
                "mom1": 1 + date_index / 100 + stock_index / 1000,
                "pe": 10 + stock_index,
                "bias20": 0.9 + date_index / 100,
                "rank": (stock_index + 1) / 8,
                "return": 0.98 + stock_index / 100,
            }
        )

config = TrainConfig(
    validation_periods=2,
    test_periods=2,
    label_horizon_periods=1,
    nn_epochs=1,
    nn_batch_size=16,
    nn_patience=1,
    lgb_estimators=5,
    lgb_early_stopping_rounds=2,
    rf_estimators=5,
    rf_max_depth=3,
    rf_min_samples_leaf=1,
)
data, features = prepare_dataset(pd.DataFrame(rows), config, None)
split = chronological_split(data, config)
models, preprocessors, predictions = train_models(split, features, config)

with TemporaryDirectory(dir=Path(__file__).parent) as temporary:
    bundle_dir = package_bundle(
        output_root=Path(temporary),
        run_name="synthetic-smoke",
        source_description="synthetic",
        data=data,
        split=split,
        features=features,
        config=config,
        models=models,
        preprocessors=preprocessors,
        predictions=predictions,
    )
    output = ForecastModelBundle.load(bundle_dir).predict(split.test[features].head(3))
    assert output.shape == (3, 4)
    assert np.isfinite(output.to_numpy()).all()
    print(f"FULL_SMOKE_OK features={features} output_shape={output.shape}")
