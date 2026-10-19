"""Regression tests for Spark's unsupported Parquet TIMESTAMP(NANOS) input."""

from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from databricks_migrate.repair_parquet_timestamps import repair_file


class TimestampRepairTests(unittest.TestCase):
    def test_preserves_values_nulls_and_columns_across_batches(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / "old.parquet", root / "new.parquet"
            table = pa.table({
                "stock_id": ["0050", "2330", None],
                "date": pa.array([1_000_001_000, None, 1_000_003_000], type=pa.timestamp("ns")),
                "恢復買賣日期": ["1140113", None, "1140114"],
                "value": [1.5, None, 3.0],
            })
            pq.write_table(table, source, version="2.6")
            self.assertEqual(repair_file(source, target, batch_size=1), 3)
            result = pq.read_table(target)
            expected = table.cast(pa.schema([
                pa.field("stock_id", pa.string()),
                pa.field("date", pa.timestamp("us")),
                pa.field("恢復買賣日期", pa.string()),
                pa.field("value", pa.float64()),
            ]))
            self.assertTrue(result.equals(expected))
            self.assertEqual(pq.read_schema(source).field("date").type, pa.timestamp("ns"))
            with pq.ParquetFile(target) as reader:
                self.assertIn("microseconds", str(reader.schema.column(1).logical_type).lower())

    def test_rejects_real_nanosecond_precision_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / "old.parquet", root / "new.parquet"
            pq.write_table(pa.table({
                "date": pa.array([1_000_000_001], type=pa.timestamp("ns"))
            }), source, version="2.6")
            with self.assertRaises(pa.ArrowInvalid):
                repair_file(source, target)
            self.assertFalse(target.exists())
            self.assertEqual(list(root.glob("*.partial")), [])

    def test_refuses_to_overwrite_original_or_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "original.parquet"
            target = Path(directory) / "existing.parquet"
            table = pa.table({"date": pa.array([1000], type=pa.timestamp("ns"))})
            pq.write_table(table, source)
            pq.write_table(table, target)
            with self.assertRaises(ValueError):
                repair_file(source, source)
            with self.assertRaises(FileExistsError):
                repair_file(source, target)


if __name__ == "__main__":
    unittest.main()
