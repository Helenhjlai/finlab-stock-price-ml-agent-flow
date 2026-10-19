"""Rewrite existing flat crawler Parquet files with Spark-compatible timestamps.

Reads bounded Arrow batches, writes microseconds to a separate output directory,
and rejects precision loss. Source files are never modified. Requires pyarrow.

    python databricks_migrate/repair_parquet_timestamps.py --datasets monthly_report

Defaults are relative to this script, regardless of the current directory.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq


def repair_file(source: Path, target: Path, batch_size: int = 100_000) -> int:
    source, target = Path(source).resolve(), Path(target).resolve()
    if source == target:
        raise ValueError("Source and target must be different; preserve the original file")
    if target.exists():
        raise FileExistsError(f"Output already exists: {target}. Choose a new output directory.")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{target.stem}-", suffix=".partial", dir=target.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with pq.ParquetFile(source) as reader:
            original_schema = reader.schema_arrow
            # Crawler exports are flat. Fail explicitly on nested fields because
            # a top-level cast would not fix a timestamp inside a struct/list.
            if any(pa.types.is_nested(field.type) for field in original_schema):
                raise ValueError("Expected flat crawler columns; nested types need explicit conversion")
            fields = [
                field.with_type(pa.timestamp("us", tz=field.type.tz))
                if pa.types.is_timestamp(field.type) else field
                for field in original_schema
            ]
            schema = pa.schema(fields, metadata=original_schema.metadata)
            expected_rows = reader.metadata.num_rows
            written_rows = 0
            with pq.ParquetWriter(
                temporary, schema, version="2.4", compression="snappy",
                coerce_timestamps="us", allow_truncated_timestamps=False,
                use_deprecated_int96_timestamps=False,
            ) as writer:
                for batch in reader.iter_batches(batch_size=batch_size):
                    table = pa.Table.from_batches([batch]).cast(schema, safe=True)
                    writer.write_table(table)
                    written_rows += table.num_rows

        with pq.ParquetFile(temporary) as checked:
            if checked.metadata.num_rows != expected_rows or written_rows != expected_rows:
                raise ValueError("Row count changed during timestamp repair")
            if checked.schema_arrow.names != original_schema.names:
                raise ValueError("Column names changed during timestamp repair")
            for index in range(len(checked.schema)):
                if "nanoseconds" in str(checked.schema.column(index).logical_type).lower():
                    raise ValueError("TIMESTAMP(NANOS) remains in the physical Parquet schema")

        # rename, rather than replace, also refuses an existing target on Windows.
        temporary.rename(target)
        return written_rows
    finally:
        temporary.unlink(missing_ok=True)


def main() -> None:
    base = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=base / "export" / "parquet")
    parser.add_argument("--output-dir", type=Path, default=base / "export" / "parquet_spark")
    parser.add_argument("--datasets", default="monthly_report", help="all or comma-separated stems")
    parser.add_argument("--batch-size", type=int, default=100_000)
    args = parser.parse_args()

    if args.datasets.strip().lower() == "all":
        sources = sorted(args.input_dir.glob("*.parquet"))
    else:
        names = [name.strip() for name in args.datasets.split(",") if name.strip()]
        if any(Path(name).name != name or "/" in name or "\\" in name for name in names):
            parser.error("--datasets must contain dataset names, not file paths")
        sources = [args.input_dir / f"{name}.parquet" for name in names]
    if not sources:
        parser.error("No Parquet files selected")
    for source in sources:
        if not source.is_file():
            parser.error(f"Missing source: {source}")
        if (args.output_dir / source.name).exists():
            parser.error(f"Output already exists: {args.output_dir / source.name}")

    for source in sources:
        target = args.output_dir / source.name
        count = repair_file(source, target, args.batch_size)
        print(f"{source.stem}: {count:,} rows -> {target} [timestamp microseconds]", flush=True)


if __name__ == "__main__":
    main()
