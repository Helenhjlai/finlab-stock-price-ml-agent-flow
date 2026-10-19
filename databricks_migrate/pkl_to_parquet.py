"""Convert historical FinLab pickle tables to Spark-compatible Parquet files.

The crawler history uses a ``(stock_id, date)`` MultiIndex.  This script turns
those index levels into columns, normalizes mixed Python object columns, and
writes microsecond-precision Parquet timestamps that Spark/Databricks can read.

Run from the project root:

    python databricks_migrate/pkl_to_parquet.py

Optional paths:

    python databricks_migrate/pkl_to_parquet.py \
        --source-dir history/tables \
        --target-dir databricks_migrate/export/parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "history" / "tables"
DEFAULT_TARGET_DIR = Path(__file__).resolve().parent / "export" / "parquet"


def prepare_for_parquet(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Return a DataFrame whose schema can be inferred safely by Arrow.

    Some crawler responses mix Python integers and strings in one object
    column, such as ``otc_cap_reduction.恢復買賣日期``.  Arrow may infer int64
    from the first values and then fail on a later string.  Object columns are
    therefore stored as nullable strings.  The canonical ``date`` column stays
    datetime64 and ``stock_id`` stays a string.
    """

    frame = df.reset_index()

    required_columns = {"stock_id", "date"}
    missing = required_columns.difference(frame.columns)
    if missing:
        raise KeyError(
            f"{table_name}: expected stock_id and date columns; "
            f"missing {sorted(missing)}"
        )

    frame["stock_id"] = frame["stock_id"].astype("string")
    frame["date"] = pd.to_datetime(frame["date"], errors="raise")

    for column in frame.select_dtypes(include=["object"]).columns:
        non_null = frame[column].dropna()
        python_types = sorted({type(value).__name__ for value in non_null})
        if len(python_types) > 1:
            print(f"  mixed object column {column!r}: {python_types} -> string")
        frame[column] = frame[column].astype("string")

    return frame.drop_duplicates(["stock_id", "date"], keep="last")


def verify_parquet(target: Path, expected_columns: list[str], expected_rows: int) -> None:
    """Fail if a written file has an incompatible or unexpected schema."""

    parquet_file = pq.ParquetFile(target)

    has_nanosecond_timestamp = any(
        "nanoseconds" in str(parquet_file.schema.column(index).logical_type).lower()
        for index in range(len(parquet_file.schema))
    )
    if has_nanosecond_timestamp:
        raise ValueError(f"{target.stem}: incompatible TIMESTAMP(NANOS) remains")

    if parquet_file.schema_arrow.names != expected_columns:
        raise ValueError(f"{target.stem}: Parquet columns changed after writing")

    if parquet_file.metadata.num_rows != expected_rows:
        raise ValueError(f"{target.stem}: Parquet row count changed after writing")


def convert_pickle(source: Path, target: Path) -> int:
    """Convert one pickle table and return the number of written rows."""

    print(f"reading {source}")
    frame = prepare_for_parquet(pd.read_pickle(source), source.stem)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Spark cannot infer TIMESTAMP(NANOS).  Store physical microseconds and
    # reject genuine sub-microsecond values rather than silently truncating.
    frame.to_parquet(
        target,
        index=False,
        engine="pyarrow",
        compression="snappy",
        version="2.4",
        coerce_timestamps="us",
        allow_truncated_timestamps=False,
        use_deprecated_int96_timestamps=False,
    )

    verify_parquet(target, list(frame.columns), len(frame))
    print(f"written {target} ({len(frame):,} rows)")
    return len(frame)


def convert_all(source_dir: Path, target_dir: Path) -> tuple[int, int]:
    """Convert every top-level ``*.pkl`` file in ``source_dir``."""

    if not source_dir.is_dir():
        raise FileNotFoundError(f"source directory does not exist: {source_dir}")

    sources = sorted(source_dir.glob("*.pkl"))
    if not sources:
        raise FileNotFoundError(f"no .pkl files found in: {source_dir}")

    total_rows = 0
    for source in sources:
        target = target_dir / f"{source.stem}.parquet"
        total_rows += convert_pickle(source, target)

    return len(sources), total_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert FinLab history pickle tables to Databricks-compatible Parquet."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=DEFAULT_SOURCE_DIR,
        help=f"directory containing .pkl files (default: {DEFAULT_SOURCE_DIR})",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=DEFAULT_TARGET_DIR,
        help=f"directory for .parquet files (default: {DEFAULT_TARGET_DIR})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_dir = args.source_dir.expanduser().resolve()
    target_dir = args.target_dir.expanduser().resolve()

    file_count, row_count = convert_all(source_dir, target_dir)
    print(f"completed: files={file_count:,}, rows={row_count:,}")


if __name__ == "__main__":
    main()
