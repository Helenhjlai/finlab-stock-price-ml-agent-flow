import datetime as dt
import unittest

import pandas as pd

from databricks_migrate.databricks_src.delta_writer import (
    _merge_condition,
    _uses_extended_provenance,
    max_event_time,
    normalize_pandas_frame,
    SchemaMismatchError,
)


class DeltaWriterContractTests(unittest.TestCase):
    def test_legacy_default_key_contract_is_unchanged(self) -> None:
        frame = pd.DataFrame(
            {
                "stock_id": ["2330", "2330"],
                "date": ["2026-10-22", "2026-10-22"],
                "close": [100, 101],
            }
        )
        result = normalize_pandas_frame(frame, "price")
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["close"], 101)
        self.assertEqual(str(result["stock_id"].dtype), "string")

    def test_generic_snapshot_contract_uses_declared_keys_and_time(self) -> None:
        frame = pd.DataFrame(
            {
                "snapshot_date": ["2026-10-23", "2026-10-23"],
                "market": ["TWSE", "TWSE"],
                "stock_id": ["2330", "2330"],
                "security_name": ["舊名稱", "台積電"],
            }
        )
        result = normalize_pandas_frame(
            frame,
            "security_master_snapshot",
            key_columns=("snapshot_date", "market", "stock_id"),
            event_time_column="snapshot_date",
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["security_name"], "台積電")
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["snapshot_date"]))

    def test_generic_merge_condition_quotes_each_declared_key(self) -> None:
        self.assertEqual(
            _merge_condition(("snapshot_date", "market", "stock_id")),
            "target.`snapshot_date` = source.`snapshot_date` AND "
            "target.`market` = source.`market` AND "
            "target.`stock_id` = source.`stock_id`",
        )

    def test_invalid_key_identifier_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            _merge_condition(("stock_id", "date; DROP TABLE x"))

    def test_max_event_time_accepts_custom_column(self) -> None:
        frame = pd.DataFrame(
            {"snapshot_date": ["2026-10-22", "2026-10-23"], "stock_id": ["1", "2"]}
        )
        value = max_event_time(frame, "snapshot_date")
        self.assertEqual(value, dt.datetime(2026, 10, 23))

    def test_new_table_always_uses_extended_provenance(self) -> None:
        self.assertTrue(
            _uses_extended_provenance(table_exists=False, existing_columns=set())
        )

    def test_legacy_table_waits_for_explicit_backfill(self) -> None:
        self.assertFalse(
            _uses_extended_provenance(
                table_exists=True,
                existing_columns={"stock_id", "date", "_source"},
            )
        )

    def test_backfilled_table_continues_extended_provenance(self) -> None:
        self.assertTrue(
            _uses_extended_provenance(
                table_exists=True,
                existing_columns={
                    "_source_record_id",
                    "_source_published_at",
                    "_payload_hash",
                },
            )
        )

    def test_partial_provenance_contract_fails_closed(self) -> None:
        with self.assertRaises(SchemaMismatchError):
            _uses_extended_provenance(
                table_exists=True,
                existing_columns={"_source_record_id"},
            )


if __name__ == "__main__":
    unittest.main()

