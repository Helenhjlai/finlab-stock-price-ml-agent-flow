"""Train and package the FinLab monthly stock-rank ensemble.

The input is a *materialized feature table*, with one row per ``date`` and
``stock_id``.  It must contain the target (``rank`` by default) plus numeric
feature columns.  This module deliberately does not build features from the
bronze crawler tables; feature materialization and model training have
different refresh/lifecycle rules.

The script can read either a local Parquet/CSV file or a Databricks table.  It
performs a chronological train/validation/test split, fits preprocessing on
train only, trains Keras, LightGBM, and Random Forest regressors, evaluates the
three models and an equal-weight ensemble, and writes a self-contained,
versioned artifact bundle.  MLflow/Unity Catalog logging is optional.

Example (Databricks Spark Python task)::

    python train_forecast_models.py \
      --input-table finlab.gold.forecast_training_features \
      --output-dir /Volumes/finlab/ml/model_artifacts/stock_rank \
      --register-model-name finlab.ml.stock_rank_ensemble \
      --experiment-name /Shared/finlab-stock-rank

Example (local smoke run)::

    python train_forecast_models.py \
      --input-path data/forecast_training_features.parquet \
      --output-dir forecastModel/training_runs \
      --nn-epochs 3 --lgb-estimators 20 --rf-estimators 10
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import shutil
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import joblib
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler


BUNDLE_FORMAT_VERSION = 1
RESERVED_COLUMNS = {
    "date",
    "stock_id",
    "rank",
    "return",
    "result_1",
    "result_2",
    "result_3",
    "_source",
    "_ingested_at",
    "_run_id",
}


@dataclass(frozen=True)
class TrainConfig:
    date_column: str = "date"
    id_column: str = "stock_id"
    label_column: str = "rank"
    return_column: str | None = "return"
    validation_periods: int = 12
    test_periods: int = 24
    label_horizon_periods: int = 1
    seed: int = 42
    nn_epochs: int = 225
    nn_batch_size: int = 1000
    nn_patience: int = 20
    nn_dropout: float = 0.7
    nn_hidden_units: tuple[int, ...] = (100, 100)
    nn_learning_rate: float = 0.001
    lgb_estimators: int = 2000
    lgb_learning_rate: float = 0.03
    lgb_early_stopping_rounds: int = 100
    lgb_num_leaves: int = 31
    lgb_max_depth: int = -1
    lgb_min_child_samples: int = 20
    lgb_subsample: float = 0.8
    lgb_colsample_bytree: float = 0.8
    lgb_reg_alpha: float = 0.0
    lgb_reg_lambda: float = 0.0
    rf_estimators: int = 300
    rf_max_depth: int | None = 18
    rf_min_samples_leaf: int = 5
    rf_max_features: str | float = "sqrt"
    top_k: int = 20


@dataclass(frozen=True)
class DatasetSplit:
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    purged_dates: tuple[pd.Timestamp, ...]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input-path", help="Local/Volume .parquet or .csv feature data")
    source.add_argument("--input-table", help="Databricks table with prepared features")
    parser.add_argument("--output-dir", required=True, help="Root directory for versioned bundles")
    parser.add_argument("--run-name", help="Version folder; default is UTC timestamp")
    parser.add_argument(
        "--feature-cols",
        help="Comma-separated ordered features. If omitted, numeric-like columns are inferred.",
    )
    parser.add_argument(
        "--params-file",
        help="JSON produced by tune_forecast_models.py; explicit CLI hyperparameters win",
    )
    parser.add_argument("--label-column", default="rank")
    parser.add_argument("--return-column", default="return")
    parser.add_argument("--validation-periods", type=int, default=12)
    parser.add_argument("--test-periods", type=int, default=24)
    parser.add_argument("--label-horizon-periods", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--nn-epochs", type=int)
    parser.add_argument("--nn-batch-size", type=int)
    parser.add_argument("--nn-patience", type=int)
    parser.add_argument("--nn-dropout", type=float)
    parser.add_argument("--nn-hidden-units", help="Comma-separated Dense widths, e.g. 128,64")
    parser.add_argument("--nn-learning-rate", type=float)
    parser.add_argument("--lgb-estimators", type=int)
    parser.add_argument("--lgb-learning-rate", type=float)
    parser.add_argument("--lgb-early-stopping-rounds", type=int)
    parser.add_argument("--lgb-num-leaves", type=int)
    parser.add_argument("--lgb-max-depth", type=int)
    parser.add_argument("--lgb-min-child-samples", type=int)
    parser.add_argument("--lgb-subsample", type=float)
    parser.add_argument("--lgb-colsample-bytree", type=float)
    parser.add_argument("--lgb-reg-alpha", type=float)
    parser.add_argument("--lgb-reg-lambda", type=float)
    parser.add_argument("--rf-estimators", type=int)
    parser.add_argument("--rf-max-depth", type=int)
    parser.add_argument("--rf-min-samples-leaf", type=int)
    parser.add_argument("--rf-max-features")
    parser.add_argument("--top-k", type=int, default=20)
    parser.add_argument("--max-pandas-rows", type=int, default=2_000_000)
    parser.add_argument("--archive", action="store_true", help="Also create a .tar.gz bundle")
    parser.add_argument("--experiment-name", help="MLflow experiment; enables MLflow logging")
    parser.add_argument(
        "--register-model-name",
        help="Unity Catalog name, e.g. finlab.ml.stock_rank_ensemble",
    )
    return parser.parse_args(argv)


def _parse_feature_columns(value: str | None) -> list[str] | None:
    if not value:
        return None
    columns = [item.strip() for item in value.split(",") if item.strip()]
    if not columns:
        raise ValueError("--feature-cols was supplied but no column names were found")
    if len(columns) != len(set(columns)):
        raise ValueError("--feature-cols contains duplicate names")
    return columns


TUNABLE_CONFIG_FIELDS = {
    "nn_epochs",
    "nn_batch_size",
    "nn_patience",
    "nn_dropout",
    "nn_hidden_units",
    "nn_learning_rate",
    "lgb_estimators",
    "lgb_learning_rate",
    "lgb_early_stopping_rounds",
    "lgb_num_leaves",
    "lgb_max_depth",
    "lgb_min_child_samples",
    "lgb_subsample",
    "lgb_colsample_bytree",
    "lgb_reg_alpha",
    "lgb_reg_lambda",
    "rf_estimators",
    "rf_max_depth",
    "rf_min_samples_leaf",
    "rf_max_features",
}


def _parse_hidden_units(value: str | Sequence[int]) -> tuple[int, ...]:
    parts = value.split(",") if isinstance(value, str) else value
    units = tuple(int(item) for item in parts)
    if not units or any(item < 1 for item in units):
        raise ValueError("nn_hidden_units must contain positive integers")
    return units


def _parse_max_features(value: str | float) -> str | float:
    if not isinstance(value, str):
        return float(value)
    if value in {"sqrt", "log2"}:
        return value
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError("rf_max_features must be sqrt, log2, or a float") from exc


def load_tuned_config(path: str | Path) -> dict[str, Any]:
    """Load only whitelisted training overrides from a tuning result."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    values = payload.get("training_config_overrides", payload)
    if not isinstance(values, dict):
        raise ValueError("params file must contain a JSON object")
    unknown = set(values).difference(TUNABLE_CONFIG_FIELDS)
    if unknown:
        raise ValueError(f"Unsupported keys in params file: {sorted(unknown)}")
    result = dict(values)
    if "nn_hidden_units" in result:
        result["nn_hidden_units"] = _parse_hidden_units(result["nn_hidden_units"])
    if "rf_max_features" in result:
        result["rf_max_features"] = _parse_max_features(result["rf_max_features"])
    return result


def build_train_config(args: argparse.Namespace) -> TrainConfig:
    """Merge defaults, a tuning JSON, then explicit CLI hyperparameters."""
    values = asdict(TrainConfig())
    if args.params_file:
        values.update(load_tuned_config(args.params_file))

    for field in TUNABLE_CONFIG_FIELDS:
        cli_value = getattr(args, field, None)
        if cli_value is not None:
            values[field] = cli_value
    if args.nn_hidden_units is not None:
        values["nn_hidden_units"] = _parse_hidden_units(args.nn_hidden_units)
    if args.rf_max_features is not None:
        values["rf_max_features"] = _parse_max_features(args.rf_max_features)

    values.update(
        label_column=args.label_column,
        return_column=args.return_column or None,
        validation_periods=args.validation_periods,
        test_periods=args.test_periods,
        label_horizon_periods=args.label_horizon_periods,
        seed=args.seed,
        top_k=args.top_k,
    )
    values["nn_hidden_units"] = tuple(values["nn_hidden_units"])
    return TrainConfig(**values)


def load_dataset(
    *,
    input_path: str | None,
    input_table: str | None,
    max_pandas_rows: int,
) -> pd.DataFrame:
    if input_path:
        path = Path(input_path)
        if not path.exists():
            raise FileNotFoundError(path)
        suffixes = "".join(path.suffixes).lower()
        if suffixes.endswith(".parquet"):
            frame = pd.read_parquet(path)
        elif suffixes.endswith((".csv", ".csv.gz")):
            frame = pd.read_csv(path)
        else:
            raise ValueError("--input-path must end in .parquet, .csv, or .csv.gz")
    else:
        try:
            from pyspark.sql import SparkSession
        except ImportError as exc:
            raise RuntimeError("--input-table requires a Databricks/PySpark runtime") from exc
        spark = SparkSession.builder.getOrCreate()
        spark_frame = spark.table(str(input_table))
        row_count = spark_frame.count()
        if row_count > max_pandas_rows:
            raise ValueError(
                f"{input_table} has {row_count:,} rows, above --max-pandas-rows "
                f"({max_pandas_rows:,}). Materialize a smaller monthly training table first."
            )
        frame = spark_frame.toPandas()

    if frame.empty:
        raise ValueError("Training input is empty")
    if len(frame) > max_pandas_rows:
        raise ValueError(
            f"Input has {len(frame):,} rows, above --max-pandas-rows ({max_pandas_rows:,})"
        )
    return frame


def prepare_dataset(
    frame: pd.DataFrame,
    config: TrainConfig,
    requested_features: Sequence[str] | None,
) -> tuple[pd.DataFrame, list[str]]:
    required = {config.date_column, config.id_column, config.label_column}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Training input is missing required columns: {sorted(missing)}")

    data = frame.copy()
    data[config.date_column] = pd.to_datetime(data[config.date_column], errors="coerce")
    data[config.id_column] = data[config.id_column].astype("string")
    data[config.label_column] = pd.to_numeric(data[config.label_column], errors="coerce")
    data = data.dropna(subset=[config.date_column, config.id_column, config.label_column])
    if config.label_column == "rank" and not data[config.label_column].between(0, 1).all():
        raise ValueError("rank target must be within [0, 1]")

    duplicate_mask = data.duplicated([config.date_column, config.id_column], keep=False)
    if duplicate_mask.any():
        examples = data.loc[
            duplicate_mask, [config.date_column, config.id_column]
        ].head(5)
        raise ValueError(
            "Expected one row per date/stock_id; duplicate keys include "
            f"{examples.to_dict('records')}"
        )

    features = list(requested_features) if requested_features else _infer_features(data, config)
    missing_features = sorted(set(features).difference(data.columns))
    if missing_features:
        raise ValueError(f"Requested features are missing: {missing_features}")
    if not features:
        raise ValueError("No feature columns were selected")

    for column in features:
        converted = pd.to_numeric(data[column], errors="coerce")
        if converted.notna().sum() == 0:
            raise ValueError(f"Feature {column!r} has no numeric values")
        data[column] = converted.replace([np.inf, -np.inf], np.nan)

    data = data.sort_values([config.date_column, config.id_column]).reset_index(drop=True)
    if len(data) == 0:
        raise ValueError("No rows remain after date/id/label validation")
    return data, features


def _infer_features(data: pd.DataFrame, config: TrainConfig) -> list[str]:
    excluded = RESERVED_COLUMNS | {
        config.date_column,
        config.id_column,
        config.label_column,
    }
    if config.return_column:
        excluded.add(config.return_column)

    features: list[str] = []
    for column in data.columns:
        if column in excluded or column.startswith("_"):
            continue
        original_non_null = int(data[column].notna().sum())
        if original_non_null == 0:
            continue
        numeric_non_null = int(pd.to_numeric(data[column], errors="coerce").notna().sum())
        if numeric_non_null / original_non_null >= 0.95:
            features.append(column)
    return features


def chronological_split(data: pd.DataFrame, config: TrainConfig) -> DatasetSplit:
    """Split by whole dates and purge label-horizon dates at both boundaries."""
    if config.validation_periods < 1 or config.test_periods < 1:
        raise ValueError("validation_periods and test_periods must both be >= 1")
    if config.label_horizon_periods < 0:
        raise ValueError("label_horizon_periods must be >= 0")

    dates = pd.DatetimeIndex(sorted(data[config.date_column].drop_duplicates()))
    required = (
        config.validation_periods
        + config.test_periods
        + 2 * config.label_horizon_periods
        + 1
    )
    if len(dates) < required:
        raise ValueError(
            f"Only {len(dates)} unique periods; at least {required} are needed for the split"
        )

    test_start = len(dates) - config.test_periods
    validation_end = test_start - config.label_horizon_periods
    validation_start = validation_end - config.validation_periods
    train_end = validation_start - config.label_horizon_periods

    train_dates = set(dates[:train_end])
    validation_dates = set(dates[validation_start:validation_end])
    test_dates = set(dates[test_start:])
    purged = tuple(dates[train_end:validation_start]) + tuple(
        dates[validation_end:test_start]
    )

    result = DatasetSplit(
        train=data[data[config.date_column].isin(train_dates)].copy(),
        validation=data[data[config.date_column].isin(validation_dates)].copy(),
        test=data[data[config.date_column].isin(test_dates)].copy(),
        purged_dates=purged,
    )
    if result.train.empty or result.validation.empty or result.test.empty:
        raise AssertionError("Chronological split unexpectedly produced an empty partition")
    return result


def set_reproducible_seed(seed: int) -> None:
    os.environ.setdefault("PYTHONHASHSEED", str(seed))
    os.environ.setdefault("TF_DETERMINISTIC_OPS", "1")
    random.seed(seed)
    np.random.seed(seed)


def build_nn(input_width: int, config: TrainConfig) -> Any:
    try:
        import tensorflow as tf
    except ImportError as exc:
        raise RuntimeError("TensorFlow is required to train the neural network") from exc

    tf.keras.utils.set_random_seed(config.seed)
    layers: list[Any] = [tf.keras.layers.Input(shape=(input_width,), name="features")]
    for index, units in enumerate(config.nn_hidden_units):
        layers.append(
            tf.keras.layers.Dense(
                units,
                activation="relu",
                kernel_initializer=tf.keras.initializers.HeNormal(seed=config.seed + index),
            )
        )
    layers.extend(
        [
            tf.keras.layers.Dropout(config.nn_dropout, seed=config.seed),
            tf.keras.layers.Dense(1, activation="sigmoid", name="rank"),
        ]
    )
    model = tf.keras.Sequential(layers, name="stock_rank_nn")
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=config.nn_learning_rate),
        loss="mean_squared_error",
        metrics=["mae"],
    )
    return model


def train_models(
    split: DatasetSplit,
    features: Sequence[str],
    config: TrainConfig,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, pd.DataFrame]]:
    set_reproducible_seed(config.seed)
    try:
        import lightgbm as lgb
        import tensorflow as tf
        from sklearn.ensemble import RandomForestRegressor
    except ImportError as exc:
        raise RuntimeError(
            "Training requires tensorflow, lightgbm, and scikit-learn; install "
            "requirements-ml.txt in the Databricks Environment"
        ) from exc

    x_train = split.train[list(features)]
    x_validation = split.validation[list(features)]
    x_test = split.test[list(features)]
    y_train = split.train[config.label_column].to_numpy(dtype=np.float32)
    y_validation = split.validation[config.label_column].to_numpy(dtype=np.float32)

    tree_preprocessor = Pipeline([("imputer", SimpleImputer(strategy="median"))])
    nn_preprocessor = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", MinMaxScaler()),
        ]
    )
    x_train_tree = tree_preprocessor.fit_transform(x_train).astype(np.float32)
    x_validation_tree = tree_preprocessor.transform(x_validation).astype(np.float32)
    x_test_tree = tree_preprocessor.transform(x_test).astype(np.float32)
    x_train_nn = nn_preprocessor.fit_transform(x_train).astype(np.float32)
    x_validation_nn = nn_preprocessor.transform(x_validation).astype(np.float32)
    x_test_nn = nn_preprocessor.transform(x_test).astype(np.float32)

    nn_model = build_nn(len(features), config)
    nn_history = nn_model.fit(
        x_train_nn,
        y_train,
        validation_data=(x_validation_nn, y_validation),
        epochs=config.nn_epochs,
        batch_size=config.nn_batch_size,
        shuffle=True,
        verbose=2,
        callbacks=[
            tf.keras.callbacks.EarlyStopping(
                monitor="val_loss",
                patience=config.nn_patience,
                min_delta=1e-5,
                restore_best_weights=True,
            )
        ],
    )

    lgb_model = lgb.LGBMRegressor(
        objective="regression",
        n_estimators=config.lgb_estimators,
        learning_rate=config.lgb_learning_rate,
        num_leaves=config.lgb_num_leaves,
        max_depth=config.lgb_max_depth,
        min_child_samples=config.lgb_min_child_samples,
        subsample=config.lgb_subsample,
        subsample_freq=1,
        colsample_bytree=config.lgb_colsample_bytree,
        reg_alpha=config.lgb_reg_alpha,
        reg_lambda=config.lgb_reg_lambda,
        random_state=config.seed,
        n_jobs=-1,
        verbosity=-1,
    )
    lgb_model.fit(
        x_train_tree,
        y_train,
        eval_set=[(x_validation_tree, y_validation)],
        eval_metric="l2",
        callbacks=[
            lgb.early_stopping(config.lgb_early_stopping_rounds, verbose=True),
            lgb.log_evaluation(period=50),
        ],
    )

    rf_model = RandomForestRegressor(
        n_estimators=config.rf_estimators,
        max_depth=config.rf_max_depth,
        min_samples_leaf=config.rf_min_samples_leaf,
        max_features=config.rf_max_features,
        n_jobs=-1,
        random_state=config.seed,
    )
    rf_model.fit(x_train_tree, y_train)

    models = {"nn": nn_model, "lgb": lgb_model, "rf": rf_model}
    preprocessors = {"nn": nn_preprocessor, "tree": tree_preprocessor}
    transformed = {
        "validation": _prediction_frame(
            split.validation,
            config,
            nn_model.predict(x_validation_nn, verbose=0).reshape(-1),
            lgb_model.booster_.predict(x_validation_tree),
            rf_model.predict(x_validation_tree),
        ),
        "test": _prediction_frame(
            split.test,
            config,
            nn_model.predict(x_test_nn, verbose=0).reshape(-1),
            lgb_model.booster_.predict(x_test_tree),
            rf_model.predict(x_test_tree),
        ),
        "nn_history": pd.DataFrame(nn_history.history),
    }
    return models, preprocessors, transformed


def _prediction_frame(
    source: pd.DataFrame,
    config: TrainConfig,
    nn_prediction: Iterable[float],
    lgb_prediction: Iterable[float],
    rf_prediction: Iterable[float],
) -> pd.DataFrame:
    columns = [config.date_column, config.id_column, config.label_column]
    if config.return_column and config.return_column in source.columns:
        columns.append(config.return_column)
    result = source[columns].reset_index(drop=True).copy()
    result["nn_prediction"] = np.asarray(nn_prediction)
    result["lgb_prediction"] = np.asarray(lgb_prediction)
    result["rf_prediction"] = np.asarray(rf_prediction)
    result["ensemble_prediction"] = result[
        ["nn_prediction", "lgb_prediction", "rf_prediction"]
    ].mean(axis=1)
    return result


def _safe_spearman(actual: pd.Series, predicted: pd.Series) -> float:
    if len(actual) < 2 or actual.nunique() < 2 or predicted.nunique() < 2:
        return math.nan
    a = actual.rank(method="average").to_numpy(dtype=float)
    p = predicted.rank(method="average").to_numpy(dtype=float)
    return float(np.corrcoef(a, p)[0, 1])


def evaluate_predictions(
    predictions: pd.DataFrame,
    config: TrainConfig,
) -> dict[str, float]:
    metrics: dict[str, float] = {}
    actual = predictions[config.label_column]
    prediction_columns = [
        "nn_prediction",
        "lgb_prediction",
        "rf_prediction",
        "ensemble_prediction",
    ]
    for column in prediction_columns:
        name = column.removesuffix("_prediction")
        predicted = predictions[column]
        metrics[f"{name}_mse"] = float(mean_squared_error(actual, predicted))
        metrics[f"{name}_mae"] = float(mean_absolute_error(actual, predicted))
        metrics[f"{name}_spearman"] = _safe_spearman(actual, predicted)
        period_ics = predictions[
            [config.date_column, config.label_column, column]
        ].groupby(config.date_column, sort=True)[[config.label_column, column]].apply(
            lambda group: _safe_spearman(group[config.label_column], group[column])
        )
        metrics[f"{name}_mean_period_ic"] = float(period_ics.mean())
        metrics[f"{name}_ic_information_ratio"] = _safe_ratio(
            float(period_ics.mean()), float(period_ics.std(ddof=1))
        )

    if config.return_column and config.return_column in predictions.columns:
        returns = pd.to_numeric(predictions[config.return_column], errors="coerce")
        scored = predictions.assign(_return=returns).dropna(subset=["_return"])
        for column in prediction_columns:
            name = column.removesuffix("_prediction")
            top = scored[[config.date_column, column, "_return"]].groupby(
                config.date_column, sort=True
            )[[column, "_return"]].apply(
                lambda group: group.nlargest(config.top_k, column)["_return"].mean()
            )
            baseline = scored.groupby(config.date_column, sort=True)["_return"].mean()
            metrics[f"{name}_top_{config.top_k}_mean_return"] = float(top.mean())
            metrics[f"{name}_top_{config.top_k}_mean_excess_return"] = float(
                (top - baseline).mean()
            )
    return metrics


def _safe_ratio(numerator: float, denominator: float) -> float:
    if not np.isfinite(denominator) or denominator == 0:
        return math.nan
    return numerator / denominator


def _date_range(frame: pd.DataFrame, date_column: str) -> dict[str, Any]:
    return {
        "rows": int(len(frame)),
        "periods": int(frame[date_column].nunique()),
        "start": frame[date_column].min().isoformat(),
        "end": frame[date_column].max().isoformat(),
    }


def _dataset_fingerprint(
    data: pd.DataFrame, config: TrainConfig, features: Sequence[str]
) -> str:
    fingerprint_columns = [
        config.date_column,
        config.id_column,
        *features,
        config.label_column,
    ]
    values = data[fingerprint_columns].copy()
    row_hashes = pd.util.hash_pandas_object(values, index=False).to_numpy()
    return hashlib.sha256(row_hashes.tobytes()).hexdigest()


def package_bundle(
    *,
    output_root: Path,
    run_name: str,
    source_description: str,
    data: pd.DataFrame,
    split: DatasetSplit,
    features: Sequence[str],
    config: TrainConfig,
    models: dict[str, Any],
    preprocessors: dict[str, Any],
    predictions: dict[str, pd.DataFrame],
) -> Path:
    output_root.mkdir(parents=True, exist_ok=True)
    final_dir = output_root / run_name
    if final_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing bundle: {final_dir}")

    partial_dir = Path(tempfile.mkdtemp(prefix=f".{run_name}-", dir=output_root))
    try:
        (partial_dir / "models").mkdir()
        (partial_dir / "preprocessing").mkdir()
        (partial_dir / "evaluation").mkdir()

        models["nn"].save(str(partial_dir / "models" / "nn.keras"))
        models["lgb"].booster_.save_model(str(partial_dir / "models" / "lightgbm.txt"))
        joblib.dump(models["rf"], partial_dir / "models" / "random_forest.joblib", compress=3)
        joblib.dump(
            preprocessors["nn"], partial_dir / "preprocessing" / "nn_preprocessor.joblib"
        )
        joblib.dump(
            preprocessors["tree"],
            partial_dir / "preprocessing" / "tree_preprocessor.joblib",
        )

        validation_metrics = evaluate_predictions(predictions["validation"], config)
        test_metrics = evaluate_predictions(predictions["test"], config)
        _write_json(partial_dir / "evaluation" / "validation_metrics.json", validation_metrics)
        _write_json(partial_dir / "evaluation" / "test_metrics.json", test_metrics)
        predictions["validation"].to_csv(
            partial_dir / "evaluation" / "validation_predictions.csv.gz", index=False
        )
        predictions["test"].to_csv(
            partial_dir / "evaluation" / "test_predictions.csv.gz", index=False
        )
        predictions["nn_history"].to_csv(
            partial_dir / "evaluation" / "nn_history.csv", index=False
        )
        pd.DataFrame(
            {
                "feature": list(features),
                "lightgbm_importance_gain": models["lgb"].booster_.feature_importance(
                    importance_type="gain"
                ),
                "random_forest_importance": models["rf"].feature_importances_,
            }
        ).sort_values("lightgbm_importance_gain", ascending=False).to_csv(
            partial_dir / "evaluation" / "feature_importance.csv", index=False
        )

        manifest = {
            "bundle_format_version": BUNDLE_FORMAT_VERSION,
            "run_name": run_name,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "task": "next-period cross-sectional stock rank regression",
            "source": source_description,
            "dataset_fingerprint_sha256": _dataset_fingerprint(data, config, features),
            "feature_columns": list(features),
            "config": asdict(config),
            "split": {
                "train": _date_range(split.train, config.date_column),
                "validation": _date_range(split.validation, config.date_column),
                "test": _date_range(split.test, config.date_column),
                "purged_dates": [date.isoformat() for date in split.purged_dates],
            },
            "models": {
                "nn": "models/nn.keras",
                "lightgbm": "models/lightgbm.txt",
                "random_forest": "models/random_forest.joblib",
            },
            "preprocessing": {
                "nn": "preprocessing/nn_preprocessor.joblib",
                "tree": "preprocessing/tree_preprocessor.joblib",
                "fit_scope": "train_only",
            },
            "ensemble": {
                "method": "weighted_mean",
                "weights": {"nn": 1 / 3, "lgb": 1 / 3, "rf": 1 / 3},
            },
            "environment": _environment_versions(),
            "validation_metrics": validation_metrics,
            "test_metrics": test_metrics,
        }
        _write_json(partial_dir / "manifest.json", manifest)

        loaded = ForecastModelBundle.load(partial_dir)
        sample = split.test[list(features)].head(min(16, len(split.test)))
        smoke_predictions = loaded.predict(sample)
        if len(smoke_predictions) != len(sample) or not np.isfinite(
            smoke_predictions.to_numpy()
        ).all():
            raise RuntimeError("Packaged model failed the reload smoke test")
        _write_json(
            partial_dir / "evaluation" / "reload_smoke_test.json",
            {"status": "passed", "rows": len(sample)},
        )

        _write_checksums(partial_dir)
        partial_dir.replace(final_dir)
    except Exception:
        shutil.rmtree(partial_dir, ignore_errors=True)
        raise
    return final_dir


class ForecastModelBundle:
    """Load a packaged ensemble and enforce its original feature contract."""

    def __init__(self, root: Path, manifest: dict[str, Any]) -> None:
        self.root = root
        self.manifest = manifest
        self.features = list(manifest["feature_columns"])
        self.nn_preprocessor: Any = None
        self.tree_preprocessor: Any = None
        self.nn_model: Any = None
        self.lgb_model: Any = None
        self.rf_model: Any = None

    @classmethod
    def load(cls, path: str | Path) -> "ForecastModelBundle":
        root = Path(path)
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
        if manifest.get("bundle_format_version") != BUNDLE_FORMAT_VERSION:
            raise ValueError(f"Unsupported bundle format: {manifest.get('bundle_format_version')}")

        try:
            import lightgbm as lgb
            import tensorflow as tf
        except ImportError as exc:
            raise RuntimeError("Loading the bundle requires tensorflow and lightgbm") from exc

        bundle = cls(root, manifest)
        bundle.nn_preprocessor = joblib.load(
            root / manifest["preprocessing"]["nn"]
        )
        bundle.tree_preprocessor = joblib.load(
            root / manifest["preprocessing"]["tree"]
        )
        bundle.nn_model = tf.keras.models.load_model(root / manifest["models"]["nn"])
        bundle.lgb_model = lgb.Booster(model_file=str(root / manifest["models"]["lightgbm"]))
        bundle.rf_model = joblib.load(root / manifest["models"]["random_forest"])
        return bundle

    def predict(self, frame: pd.DataFrame) -> pd.DataFrame:
        missing = sorted(set(self.features).difference(frame.columns))
        if missing:
            raise ValueError(f"Inference input is missing features: {missing}")
        raw = frame[self.features].apply(pd.to_numeric, errors="coerce")
        raw = raw.replace([np.inf, -np.inf], np.nan)
        nn_x = self.nn_preprocessor.transform(raw).astype(np.float32)
        tree_x = self.tree_preprocessor.transform(raw).astype(np.float32)
        result = pd.DataFrame(index=frame.index)
        result["nn_prediction"] = self.nn_model.predict(nn_x, verbose=0).reshape(-1)
        result["lgb_prediction"] = self.lgb_model.predict(tree_x)
        result["rf_prediction"] = self.rf_model.predict(tree_x)
        weights = self.manifest["ensemble"]["weights"]
        result["ensemble_prediction"] = (
            result["nn_prediction"] * weights["nn"]
            + result["lgb_prediction"] * weights["lgb"]
            + result["rf_prediction"] * weights["rf"]
        )
        return result


def log_bundle_to_mlflow(
    *,
    bundle_dir: Path,
    experiment_name: str,
    registered_model_name: str | None,
    input_example: pd.DataFrame,
) -> str:
    try:
        import mlflow
        from mlflow.models import infer_signature
    except ImportError as exc:
        raise RuntimeError("MLflow logging requested, but mlflow is not installed") from exc

    if registered_model_name:
        mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment_name)

    class EnsemblePyfunc(mlflow.pyfunc.PythonModel):
        def load_context(self, context: Any) -> None:
            self.bundle = ForecastModelBundle.load(context.artifacts["bundle"])

        def predict(
            self, context: Any, model_input: pd.DataFrame, params: dict[str, Any] | None = None
        ) -> pd.DataFrame:
            return self.bundle.predict(model_input)

    local_bundle = ForecastModelBundle.load(bundle_dir)
    output_example = local_bundle.predict(input_example)
    signature = infer_signature(input_example, output_example)
    manifest = local_bundle.manifest

    with mlflow.start_run(run_name=manifest["run_name"]) as run:
        mlflow.log_params(
            {
                "label_column": manifest["config"]["label_column"],
                "feature_count": len(manifest["feature_columns"]),
                "validation_periods": manifest["config"]["validation_periods"],
                "test_periods": manifest["config"]["test_periods"],
                "label_horizon_periods": manifest["config"]["label_horizon_periods"],
            }
        )
        finite_metrics = {
            f"test_{key}": value
            for key, value in manifest["test_metrics"].items()
            if isinstance(value, (int, float)) and np.isfinite(value)
        }
        mlflow.log_metrics(finite_metrics)
        model_kwargs = {
            "python_model": EnsemblePyfunc(),
            "artifacts": {"bundle": str(bundle_dir)},
            "code_paths": [str(Path(__file__).resolve())],
            "input_example": input_example,
            "signature": signature,
            "registered_model_name": registered_model_name,
            "pip_requirements": _mlflow_requirements(),
        }
        try:
            model_info = mlflow.pyfunc.log_model(name="model", **model_kwargs)
        except TypeError:
            # MLflow 2.x uses artifact_path; MLflow 3.x uses name.
            model_info = mlflow.pyfunc.log_model(artifact_path="model", **model_kwargs)
        return f"run_id={run.info.run_id}, model_uri={model_info.model_uri}"


def _environment_versions() -> dict[str, str]:
    versions = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "numpy": np.__version__,
        "pandas": pd.__version__,
    }
    for module_name in ["scikit-learn", "joblib", "tensorflow", "lightgbm", "mlflow"]:
        try:
            from importlib.metadata import version

            versions[module_name] = version(module_name)
        except Exception:
            versions[module_name] = "not-installed"
    return versions


def _mlflow_requirements() -> list[str]:
    versions = _environment_versions()
    names = ["numpy", "pandas", "scikit-learn", "joblib", "tensorflow", "lightgbm"]
    return [f"{name}=={versions[name]}" for name in names if versions[name] != "not-installed"]


def _write_json(path: Path, value: Any) -> None:
    def json_safe(item: Any) -> Any:
        if isinstance(item, dict):
            return {key: json_safe(child) for key, child in item.items()}
        if isinstance(item, (list, tuple)):
            return [json_safe(child) for child in item]
        if isinstance(item, (float, np.floating)) and not np.isfinite(item):
            return None
        if isinstance(item, np.integer):
            return int(item)
        return item

    path.write_text(
        json.dumps(
            json_safe(value), ensure_ascii=False, indent=2, allow_nan=False, default=str
        )
        + "\n",
        encoding="utf-8",
    )


def _write_checksums(root: Path) -> None:
    lines: list[str] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == "checksums.sha256":
            continue
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        lines.append(f"{digest.hexdigest()}  {path.relative_to(root).as_posix()}")
    (root / "checksums.sha256").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _validate_config(config: TrainConfig) -> None:
    if not 0 <= config.nn_dropout < 1:
        raise ValueError("nn_dropout must be in [0, 1)")
    unit_interval = {
        "lgb_subsample": config.lgb_subsample,
        "lgb_colsample_bytree": config.lgb_colsample_bytree,
    }
    invalid_fractions = {
        key: value for key, value in unit_interval.items() if not 0 < value <= 1
    }
    if invalid_fractions:
        raise ValueError(f"These values must be in (0, 1]: {invalid_fractions}")
    if isinstance(config.rf_max_features, float) and not 0 < config.rf_max_features <= 1:
        raise ValueError("float rf_max_features must be in (0, 1]")
    positive = {
        "nn_epochs": config.nn_epochs,
        "nn_batch_size": config.nn_batch_size,
        "nn_patience": config.nn_patience,
        "lgb_estimators": config.lgb_estimators,
        "lgb_early_stopping_rounds": config.lgb_early_stopping_rounds,
        "lgb_num_leaves": config.lgb_num_leaves,
        "lgb_min_child_samples": config.lgb_min_child_samples,
        "rf_estimators": config.rf_estimators,
        "rf_min_samples_leaf": config.rf_min_samples_leaf,
        "top_k": config.top_k,
    }
    invalid = {key: value for key, value in positive.items() if value < 1}
    if invalid:
        raise ValueError(f"These configuration values must be positive: {invalid}")
    if config.nn_learning_rate <= 0 or config.lgb_learning_rate <= 0:
        raise ValueError("learning rates must be positive")
    if config.lgb_reg_alpha < 0 or config.lgb_reg_lambda < 0:
        raise ValueError("LightGBM regularization values cannot be negative")
    _parse_hidden_units(config.nn_hidden_units)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.register_model_name and not args.experiment_name:
        raise ValueError("--register-model-name also requires --experiment-name")
    config = build_train_config(args)
    _validate_config(config)
    set_reproducible_seed(config.seed)

    print("[1/6] Loading feature data", flush=True)
    raw = load_dataset(
        input_path=args.input_path,
        input_table=args.input_table,
        max_pandas_rows=args.max_pandas_rows,
    )
    data, features = prepare_dataset(raw, config, _parse_feature_columns(args.feature_cols))
    split = chronological_split(data, config)
    print(
        f"Selected {len(features)} features; rows train={len(split.train):,}, "
        f"validation={len(split.validation):,}, test={len(split.test):,}",
        flush=True,
    )

    print("[2/6] Fitting train-only preprocessors and neural network", flush=True)
    print("[3/6] Training LightGBM and Random Forest", flush=True)
    models, preprocessors, predictions = train_models(split, features, config)

    run_name = args.run_name or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    source_description = args.input_table or str(Path(args.input_path).resolve())
    print("[4/6] Evaluating and packaging models", flush=True)
    bundle_dir = package_bundle(
        output_root=Path(args.output_dir),
        run_name=run_name,
        source_description=source_description,
        data=data,
        split=split,
        features=features,
        config=config,
        models=models,
        preprocessors=preprocessors,
        predictions=predictions,
    )
    print(f"[5/6] Reload smoke test passed: {bundle_dir}", flush=True)

    if args.archive:
        archive = shutil.make_archive(str(bundle_dir), "gztar", bundle_dir.parent, bundle_dir.name)
        print(f"Created archive: {archive}", flush=True)

    if args.experiment_name or args.register_model_name:
        input_example = split.test[features].head(5)
        mlflow_result = log_bundle_to_mlflow(
            bundle_dir=bundle_dir,
            experiment_name=args.experiment_name,
            registered_model_name=args.register_model_name,
            input_example=input_example,
        )
        print(f"[6/6] MLflow logged: {mlflow_result}", flush=True)
    else:
        print("[6/6] MLflow skipped (no --experiment-name)", flush=True)

    print(json.dumps({"bundle_dir": str(bundle_dir)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
