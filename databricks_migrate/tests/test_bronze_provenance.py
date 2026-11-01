import unittest

from databricks_migrate.databricks_src.bronze_provenance import (
    EXTENDED_PROVENANCE_COLUMNS,
    validate_column_names,
)


class BronzeProvenanceTests(unittest.TestCase):
    def test_v1_has_expected_columns(self) -> None:
        self.assertEqual(
            EXTENDED_PROVENANCE_COLUMNS,
            ("_source_record_id", "_source_published_at", "_payload_hash"),
        )

    def test_column_validation_rejects_sql_fragment(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsafe"):
            validate_column_names(("stock_id", "date; DROP TABLE x"))

    def test_column_validation_rejects_duplicates(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicates"):
            validate_column_names(("stock_id", "stock_id"))


if __name__ == "__main__":
    unittest.main()

