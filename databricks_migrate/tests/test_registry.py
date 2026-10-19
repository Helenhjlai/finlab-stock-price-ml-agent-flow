import datetime as dt
import unittest

from databricks_migrate.databricks_src.crawler_registry import (
    Cadence,
    DatasetSpec,
    get_registry,
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


if __name__ == "__main__":
    unittest.main()

