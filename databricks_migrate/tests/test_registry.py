import datetime as dt
import unittest

from databricks_migrate.databricks_src.crawler_registry import (
    Cadence,
    DatasetSpec,
    get_registry,
    get_output_spec,
    plan_requests,
)


class RegistryTests(unittest.TestCase):
    def test_financial_statement_is_disabled_by_default(self) -> None:
        names = {spec.name for spec in get_registry()}
        self.assertNotIn("financial_statement", names)

    def test_unknown_dataset_fails(self) -> None:
        with self.assertRaises(KeyError):
            get_registry(["does_not_exist"])

    def test_daily_plan_skips_weekend(self) -> None:
        spec = DatasetSpec("test", "crawl_test", Cadence.DAILY, True, 0)
        requests = plan_requests(
            spec,
            max_event_time=None,
            today=dt.date(2026, 7, 20),
            bootstrap_start=dt.date(2026, 7, 17),
        )
        self.assertEqual(requests, [dt.date(2026, 7, 17), dt.date(2026, 7, 20)])

    def test_snapshot_has_one_no_argument_request(self) -> None:
        spec = DatasetSpec("test", "crawl_test", Cadence.SNAPSHOT, False, 0)
        requests = plan_requests(
            spec,
            max_event_time=None,
            today=dt.date(2026, 7, 19),
            bootstrap_start=dt.date(2026, 7, 12),
        )
        self.assertEqual(requests, [None])

    def test_existing_datasets_keep_legacy_key_and_event_time_contract(self) -> None:
        for spec in get_registry(include_disabled=True):
            self.assertEqual(spec.primary_keys, ("stock_id", "date"))
            self.assertEqual(spec.event_time_column, "date")

    def test_output_table_resolves_to_parent_dataset_contract(self) -> None:
        spec = get_output_spec("balance_sheet")
        self.assertEqual(spec.name, "financial_statement")
        self.assertIn("balance_sheet", spec.outputs)

    def test_dataset_spec_rejects_duplicate_keys(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicates"):
            DatasetSpec(
                "test",
                "crawl_test",
                Cadence.DAILY,
                True,
                0,
                primary_keys=("stock_id", "stock_id"),
            )


if __name__ == "__main__":
    unittest.main()

