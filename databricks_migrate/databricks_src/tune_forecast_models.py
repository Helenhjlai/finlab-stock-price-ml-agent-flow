"""Tune FinLab stock-rank models with purged expanding-window validation.

The final ``test_periods`` are reserved and never inspected by Optuna.  The
script writes ``best_params.json`` in the format accepted by
``train_forecast_models.py --params-file``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler

try:
    from .train_forecast_models import (
        TrainConfig,
        _parse_feature_columns,
        load_dataset,
        prepare_dataset,
        set_reproducible_seed,
    )
except ImportError:  # Direct Python script execution on Databricks.
    from train_forecast_models import (  # type: ignore[no-redef]
        TrainConfig,
        _parse_feature_columns,
        load_dataset,
        prepare_dataset,
        set_reproducible_seed,
    )


TUNING_FORMAT_VERSION = 1
SUPPORTED_MODELS = ("lgb", "rf", "nn")


@dataclass(frozen=True)
class WalkForwardFold:
    number: int
    train: pd.DataFrame
    validation: pd.DataFrame
    purged_dates: tuple[pd.Timestamp, ...]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input-path", help="Local/Volume .parquet or .csv feature data")
    source.add_argument("--input-table", help="Databricks prepared feature table")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--feature-cols", help="Comma-separated ordered features")
    parser.add_argument("--label-column", default="rank")
    parser.add_argument("--return-column", default="return")
    parser.add_argument("--models", default="lgb,rf", help="Comma-separated lgb,rf,nn")
    parser.add_argument("--objective", choices=("mean_period_ic", "mse"), default="mean_period_ic")
    parser.add_argument("--n-trials", type=int, default=30)
    parser.add_argument("--cv-splits", type=int, default=3)
    parser.add_argument("--validation-periods", type=int, default=6)
    parser.add_argument("--test-periods", type=int, default=24)
    parser.add_argument("--label-horizon-periods", type=int, default=1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout-minutes", type=float)
    parser.add_argument("--n-jobs", type=int, default=1)
    parser.add_argument("--storage", help="Optuna storage URL, e.g. sqlite:////Volumes/.../tuning.db")
    parser.add_argument("--study-name-prefix", default="finlab-stock-rank")
    parser.add_argument("--max-pandas-rows", type=int, default=2_000_000)
    parser.add_argument("--lgb-early-stopping-rounds", type=int, default=100)
    parser.add_argument("--nn-max-epochs", type=int, default=150)
    parser.add_argument("--nn-patience", type=int, default=15)
    return parser.parse_args(argv)


def parse_models(value: str) -> tuple[str, ...]:
    models = tuple(dict.fromkeys(item.strip().lower() for item in value.split(",") if item.strip()))
    unknown = set(models).difference(SUPPORTED_MODELS)
    if not models or unknown:
        raise ValueError(f"--models must contain only {SUPPORTED_MODELS}; got {value!r}")
    return models


def make_walk_forward_folds(
    data: pd.DataFrame,
    *,
    date_column: str,
    cv_splits: int,
    validation_periods: int,
    test_periods: int,
    label_horizon_periods: int,
) -> tuple[list[WalkForwardFold], tuple[pd.Timestamp, ...]]:
    """Create expanding folds while reserving the final test block untouched."""
    if min(cv_splits, validation_periods, test_periods) < 1:
        raise ValueError("cv_splits, validation_periods, and test_periods must be positive")
    if label_horizon_periods < 0:
        raise ValueError("label_horizon_periods cannot be negative")

    dates = pd.DatetimeIndex(sorted(data[date_column].drop_duplicates()))
    test_start = len(dates) - test_periods
    tuning_end = test_start - label_horizon_periods
    first_validation = tuning_end - cv_splits * validation_periods
    minimum_train_periods = 2
    if first_validation - label_horizon_periods < minimum_train_periods:
        required = (
            test_periods
            + label_horizon_periods
            + cv_splits * validation_periods
            + label_horizon_periods
            + minimum_train_periods
        )
        raise ValueError(
            f"Only {len(dates)} unique periods; at least {required} are needed for this tuning plan"
        )

    folds: list[WalkForwardFold] = []
    for index in range(cv_splits):
        validation_start = first_validation + index * validation_periods
        validation_end = validation_start + validation_periods
        train_end = validation_start - label_horizon_periods
        train_dates = set(dates[:train_end])
        validation_dates = set(dates[validation_start:validation_end])
        purged = tuple(dates[train_end:validation_start])
        folds.append(
            WalkForwardFold(
                number=index + 1,
                train=data[data[date_column].isin(train_dates)].copy(),
                validation=data[data[date_column].isin(validation_dates)].copy(),
                purged_dates=purged,
            )
        )

    return folds, tuple(dates[test_start:])


def _safe_spearman(actual: pd.Series, predicted: pd.Series) -> float:
    if len(actual) < 2 or actual.nunique() < 2 or predicted.nunique() < 2:
        return math.nan
    return float(actual.rank().corr(predicted.rank()))


def score_predictions(
    validation: pd.DataFrame,
    prediction: np.ndarray,
    *,
    date_column: str,
    label_column: str,
    objective: str,
) -> float:
    if objective == "mse":
        return float(mean_squared_error(validation[label_column], prediction))
    scored = validation[[date_column, label_column]].copy()
    scored["_prediction"] = prediction
    period_ic = scored.groupby(date_column, sort=True)[[label_column, "_prediction"]].apply(
        lambda group: _safe_spearman(group[label_column], group["_prediction"])
    )
    value = float(period_ic.mean())
    if not np.isfinite(value):
        raise ValueError("mean_period_ic is not finite; check label and feature coverage")
    return value


def suggest_params(model_name: str, trial: Any, args: argparse.Namespace) -> dict[str, Any]:
    if model_name == "lgb":
        return {
            "lgb_estimators": trial.suggest_int("lgb_estimators", 300, 2500, step=100),
            "lgb_learning_rate": trial.suggest_float("lgb_learning_rate", 0.005, 0.1, log=True),
            "lgb_num_leaves": trial.suggest_int("lgb_num_leaves", 15, 127),
            "lgb_max_depth": trial.suggest_categorical("lgb_max_depth", [-1, 5, 7, 9, 12]),
            "lgb_min_child_samples": trial.suggest_int("lgb_min_child_samples", 10, 100, step=10),
            "lgb_subsample": trial.suggest_float("lgb_subsample", 0.6, 1.0),
            "lgb_colsample_bytree": trial.suggest_float("lgb_colsample_bytree", 0.6, 1.0),
            "lgb_reg_alpha": trial.suggest_float("lgb_reg_alpha", 1e-8, 10.0, log=True),
            "lgb_reg_lambda": trial.suggest_float("lgb_reg_lambda", 1e-8, 10.0, log=True),
        }
    if model_name == "rf":
        return {
            "rf_estimators": trial.suggest_int("rf_estimators", 200, 1000, step=100),
            "rf_max_depth": trial.suggest_categorical("rf_max_depth", [8, 12, 16, 20, 24, None]),
            "rf_min_samples_leaf": trial.suggest_int("rf_min_samples_leaf", 2, 20),
            "rf_max_features": trial.suggest_categorical(
                "rf_max_features", ["sqrt", 0.3, 0.5, 0.8]
            ),
        }
    return {
        "nn_hidden_units": trial.suggest_categorical(
            "nn_hidden_units", ["64,64", "128,64", "128,128", "256,128,64"]
        ),
        "nn_dropout": trial.suggest_float("nn_dropout", 0.2, 0.8),
        "nn_learning_rate": trial.suggest_float("nn_learning_rate", 1e-4, 3e-3, log=True),
        "nn_batch_size": trial.suggest_categorical("nn_batch_size", [256, 512, 1000, 2048]),
        "nn_epochs": args.nn_max_epochs,
        "nn_patience": args.nn_patience,
    }


def _fit_predict(
    model_name: str,
    params: dict[str, Any],
    fold: WalkForwardFold,
    features: Sequence[str],
    config: TrainConfig,
    args: argparse.Namespace,
) -> tuple[np.ndarray, int | None]:
    x_train = fold.train[list(features)]
    x_validation = fold.validation[list(features)]
    y_train = fold.train[config.label_column].to_numpy(dtype=np.float32)
    y_validation = fold.validation[config.label_column].to_numpy(dtype=np.float32)

    if model_name == "nn":
        import tensorflow as tf

        set_reproducible_seed(config.seed + fold.number)
        preprocessor = Pipeline(
            [("imputer", SimpleImputer(strategy="median")), ("scaler", MinMaxScaler())]
        )
        train_values = preprocessor.fit_transform(x_train).astype(np.float32)
        validation_values = preprocessor.transform(x_validation).astype(np.float32)
        units = tuple(int(item) for item in params["nn_hidden_units"].split(","))
        layers: list[Any] = [tf.keras.layers.Input(shape=(len(features),))]
        for index, width in enumerate(units):
            layers.append(
                tf.keras.layers.Dense(
                    width,
                    activation="relu",
                    kernel_initializer=tf.keras.initializers.HeNormal(
                        seed=config.seed + fold.number + index
                    ),
                )
            )
        layers.extend(
            [
                tf.keras.layers.Dropout(params["nn_dropout"], seed=config.seed),
                tf.keras.layers.Dense(1, activation="sigmoid"),
            ]
        )
        model = tf.keras.Sequential(layers)
        model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=params["nn_learning_rate"]),
            loss="mean_squared_error",
        )
        history = model.fit(
            train_values,
            y_train,
            validation_data=(validation_values, y_validation),
            epochs=params["nn_epochs"],
            batch_size=params["nn_batch_size"],
            verbose=0,
            callbacks=[
                tf.keras.callbacks.EarlyStopping(
                    monitor="val_loss",
                    patience=params["nn_patience"],
                    min_delta=1e-5,
                    restore_best_weights=True,
                )
            ],
        )
        prediction = model.predict(validation_values, verbose=0).reshape(-1)
        best_epoch = int(np.argmin(history.history["val_loss"]) + 1)
        tf.keras.backend.clear_session()
        return prediction, best_epoch

    preprocessor = Pipeline([("imputer", SimpleImputer(strategy="median"))])
    train_values = preprocessor.fit_transform(x_train).astype(np.float32)
    validation_values = preprocessor.transform(x_validation).astype(np.float32)
    if model_name == "lgb":
        import lightgbm as lgb

        model = lgb.LGBMRegressor(
            objective="regression",
            n_estimators=params["lgb_estimators"],
            learning_rate=params["lgb_learning_rate"],
            num_leaves=params["lgb_num_leaves"],
            max_depth=params["lgb_max_depth"],
            min_child_samples=params["lgb_min_child_samples"],
            subsample=params["lgb_subsample"],
            subsample_freq=1,
            colsample_bytree=params["lgb_colsample_bytree"],
            reg_alpha=params["lgb_reg_alpha"],
            reg_lambda=params["lgb_reg_lambda"],
            random_state=config.seed,
            n_jobs=-1,
            verbosity=-1,
        )
        model.fit(
            train_values,
            y_train,
            eval_set=[(validation_values, y_validation)],
            eval_metric="l2",
            callbacks=[
                lgb.early_stopping(args.lgb_early_stopping_rounds, verbose=False),
                lgb.log_evaluation(period=0),
            ],
        )
        return model.predict(validation_values), int(model.best_iteration_)

    from sklearn.ensemble import RandomForestRegressor

    model = RandomForestRegressor(
        n_estimators=params["rf_estimators"],
        max_depth=params["rf_max_depth"],
        min_samples_leaf=params["rf_min_samples_leaf"],
        max_features=params["rf_max_features"],
        random_state=config.seed,
        n_jobs=-1,
    )
    model.fit(train_values, y_train)
    return model.predict(validation_values), None


def make_objective(
    *,
    model_name: str,
    folds: Sequence[WalkForwardFold],
    features: Sequence[str],
    config: TrainConfig,
    args: argparse.Namespace,
) -> Callable[[Any], float]:
    def objective(trial: Any) -> float:
        params = suggest_params(model_name, trial, args)
        scores: list[float] = []
        best_iterations: list[int] = []
        for fold in folds:
            prediction, best_iteration = _fit_predict(
                model_name, params, fold, features, config, args
            )
            score = score_predictions(
                fold.validation,
                prediction,
                date_column=config.date_column,
                label_column=config.label_column,
                objective=args.objective,
            )
            scores.append(score)
            if best_iteration is not None:
                best_iterations.append(best_iteration)
            trial.report(float(np.mean(scores)), step=fold.number)
            if trial.should_prune():
                import optuna

                raise optuna.TrialPruned()
        trial.set_user_attr("fold_scores", scores)
        if best_iterations:
            trial.set_user_attr("best_iterations", best_iterations)
        return float(np.mean(scores))

    return objective


def _normalized_best_params(model_name: str, study: Any) -> dict[str, Any]:
    params = dict(study.best_trial.params)
    if model_name == "nn":
        params["nn_hidden_units"] = [int(item) for item in params["nn_hidden_units"].split(",")]
        params["nn_epochs"] = max(
            1,
            int(round(np.median(study.best_trial.user_attrs.get("best_iterations", [150])))),
        )
    elif model_name == "lgb":
        iterations = study.best_trial.user_attrs.get("best_iterations", [])
        if iterations:
            params["lgb_estimators"] = max(1, int(round(np.median(iterations))))
    return params


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    models = parse_models(args.models)
    if args.n_trials < 1 or args.n_jobs < 1:
        raise ValueError("n_trials and n_jobs must be positive")
    set_reproducible_seed(args.seed)

    raw = load_dataset(
        input_path=args.input_path,
        input_table=args.input_table,
        max_pandas_rows=args.max_pandas_rows,
    )
    config = TrainConfig(
        label_column=args.label_column,
        return_column=args.return_column or None,
        validation_periods=args.validation_periods,
        test_periods=args.test_periods,
        label_horizon_periods=args.label_horizon_periods,
        seed=args.seed,
    )
    data, features = prepare_dataset(raw, config, _parse_feature_columns(args.feature_cols))
    folds, held_out_test_dates = make_walk_forward_folds(
        data,
        date_column=config.date_column,
        cv_splits=args.cv_splits,
        validation_periods=args.validation_periods,
        test_periods=args.test_periods,
        label_horizon_periods=args.label_horizon_periods,
    )

    import optuna

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timeout_seconds = args.timeout_minutes * 60 if args.timeout_minutes else None
    direction = "maximize" if args.objective == "mean_period_ic" else "minimize"
    summaries: dict[str, Any] = {}
    overrides: dict[str, Any] = {}

    for model_name in models:
        print(f"Tuning {model_name}: trials={args.n_trials}, folds={len(folds)}", flush=True)
        sampler = optuna.samplers.TPESampler(seed=args.seed)
        study = optuna.create_study(
            study_name=f"{args.study_name_prefix}-{model_name}",
            storage=args.storage,
            load_if_exists=bool(args.storage),
            direction=direction,
            sampler=sampler,
            pruner=optuna.pruners.MedianPruner(n_warmup_steps=1),
        )
        study.optimize(
            make_objective(
                model_name=model_name,
                folds=folds,
                features=features,
                config=config,
                args=args,
            ),
            n_trials=args.n_trials,
            timeout=timeout_seconds,
            n_jobs=args.n_jobs,
            gc_after_trial=True,
        )
        best_params = _normalized_best_params(model_name, study)
        overrides.update(best_params)
        summaries[model_name] = {
            "best_value": study.best_value,
            "best_trial_number": study.best_trial.number,
            "best_params": best_params,
            "fold_scores": study.best_trial.user_attrs.get("fold_scores", []),
        }
        study.trials_dataframe().to_csv(output_dir / f"trials_{model_name}.csv", index=False)

    selector = min if direction == "minimize" else max
    best_model = selector(summaries, key=lambda name: summaries[name]["best_value"])
    result = {
        "tuning_format_version": TUNING_FORMAT_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": args.input_table or str(Path(args.input_path).resolve()),
        "objective": args.objective,
        "direction": direction,
        "best_model": best_model,
        "feature_columns": list(features),
        "training_config_overrides": overrides,
        "studies": summaries,
        "cv": {
            "splits": args.cv_splits,
            "validation_periods": args.validation_periods,
            "label_horizon_periods": args.label_horizon_periods,
            "folds": [
                {
                    "number": fold.number,
                    "train_start": fold.train[config.date_column].min().isoformat(),
                    "train_end": fold.train[config.date_column].max().isoformat(),
                    "validation_start": fold.validation[config.date_column].min().isoformat(),
                    "validation_end": fold.validation[config.date_column].max().isoformat(),
                    "purged_dates": [date.isoformat() for date in fold.purged_dates],
                }
                for fold in folds
            ],
        },
        "untouched_test": {
            "periods": len(held_out_test_dates),
            "start": held_out_test_dates[0].isoformat(),
            "end": held_out_test_dates[-1].isoformat(),
        },
        "command_hint": (
            "python train_forecast_models.py --input-table "
            f"{args.input_table or '<same-input-path>'} --params-file "
            f"{(output_dir / 'best_params.json').as_posix()} --output-dir <bundle-dir>"
        ),
    }
    _write_json(output_dir / "best_params.json", result)
    print(json.dumps({"best_model": best_model, "studies": summaries}, indent=2), flush=True)
    print(f"Wrote {output_dir / 'best_params.json'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
