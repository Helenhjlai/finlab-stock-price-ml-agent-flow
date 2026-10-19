"""Dataset definitions, scheduling rules, and safe crawler invocation.

This module deliberately does not call ``update_table``, ``to_pickle``, or
``commit`` from ``finlab.crawler``.  It imports only the named crawl function
when a dataset is about to run.
"""

from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from enum import Enum
import importlib
import time
from typing import Any, Callable, Iterable


class Cadence(str, Enum):
    DAILY = "daily"
    MONTHLY = "monthly"
    SNAPSHOT = "snapshot"
    QUARTERLY = "quarterly"


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    crawler_function: str
    cadence: Cadence
    takes_date: bool
    lookback_days: int
    enabled: bool = True
    output_tables: tuple[str, ...] = ()
    note: str = ""

    @property
    def outputs(self) -> tuple[str, ...]:
        return self.output_tables or (self.name,)


# Execution order is intentional: finish the core daily datasets before the
# full-history snapshot endpoints.  The table name keeps the legacy spelling
# ``bargin_report`` so existing analysis code does not silently break.
DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec("price", "crawl_price", Cadence.DAILY, True, 7),
    DatasetSpec("bargin_report", "crawl_bargin", Cadence.DAILY, True, 7),
    DatasetSpec("pe", "crawl_pe", Cadence.DAILY, True, 7),
    DatasetSpec(
        "benchmark",
        "crawl_benchmark",
        Cadence.DAILY,
        True,
        3,
        note="date is an intraday timestamp; never truncate it to a day",
    ),
    DatasetSpec("monthly_report", "crawl_monthly_report", Cadence.MONTHLY, True, 62),
    DatasetSpec("twse_divide_ratio", "crawl_twse_divide_ratio", Cadence.SNAPSHOT, False, 0),
    DatasetSpec("otc_divide_ratio", "crawl_otc_divide_ratio", Cadence.SNAPSHOT, False, 0),
    DatasetSpec("twse_cap_reduction", "crawl_twse_cap_reduction", Cadence.SNAPSHOT, False, 0),
    DatasetSpec("otc_cap_reduction", "crawl_otc_cap_reduction", Cadence.SNAPSHOT, False, 0),
    DatasetSpec(
        "financial_statement",
        "crawl_finance_statement_by_date",
        Cadence.QUARTERLY,
        True,
        190,
        enabled=False,
        output_tables=(
            "balance_sheet",
            "cash_flows",
            "income_sheet",
            "income_sheet_cumulate",
        ),
        note=(
            "Disabled: the legacy function writes history/ files and rebuilds "
            "all financial-statement pickle tables instead of returning the "
            "four current-quarter DataFrames."
        ),
    ),
)


# The one-time loader includes the four financial tables even though the
# current financial crawler is disabled for daily execution.
HISTORICAL_TABLES: tuple[str, ...] = (
    "balance_sheet",
    "bargin_report",
    "benchmark",
    "cash_flows",
    "income_sheet",
    "income_sheet_cumulate",
    "monthly_report",
    "otc_cap_reduction",
    "otc_divide_ratio",
    "pe",
    "price",
    "twse_cap_reduction",
    "twse_divide_ratio",
)


def get_registry(
    selected: Iterable[str] | None = None,
    *,
    include_disabled: bool = False,
) -> tuple[DatasetSpec, ...]:
    selected_names = set(selected) if selected is not None else None
    known = {spec.name for spec in DATASETS}
    if selected_names is not None:
        unknown = selected_names - known
        if unknown:
            raise KeyError(f"Unknown dataset(s): {sorted(unknown)}")

    return tuple(
        spec
        for spec in DATASETS
        if (include_disabled or spec.enabled)
        and (selected_names is None or spec.name in selected_names)
    )


def _daily_dates(start: dt.date, end: dt.date) -> list[dt.date]:
    """Return weekdays; exchange holidays are handled as empty crawler results."""
    if start > end:
        return []
    return [
        start + dt.timedelta(days=offset)
        for offset in range((end - start).days + 1)
        if (start + dt.timedelta(days=offset)).weekday() < 5
    ]


def _month_starts(start: dt.date, end: dt.date) -> list[dt.date]:
    cursor = start.replace(day=1)
    end_month = end.replace(day=1)
    result: list[dt.date] = []
    while cursor <= end_month:
        result.append(cursor)
        cursor = (cursor.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return result


def plan_requests(
    spec: DatasetSpec,
    *,
    max_event_time: dt.datetime | None,
    today: dt.date,
    bootstrap_start: dt.date,
) -> list[dt.date | None]:
    """Translate state into concrete crawler calls.

    Historical data is loaded separately.  If state is missing, this function
    starts at ``bootstrap_start`` rather than accidentally crawling all history.
    """
    if not spec.enabled:
        raise ValueError(f"Dataset {spec.name!r} is disabled: {spec.note}")
    if spec.cadence is Cadence.SNAPSHOT:
        return [None]

    start = bootstrap_start
    if max_event_time is not None:
        start = max_event_time.date() - dt.timedelta(days=spec.lookback_days)

    if spec.cadence is Cadence.DAILY:
        return _daily_dates(start, today)
    if spec.cadence is Cadence.MONTHLY:
        return _month_starts(start, today)
    if spec.cadence is Cadence.QUARTERLY:
        raise NotImplementedError(
            "Financial-statement scheduling remains disabled until its crawler is pure."
        )
    raise ValueError(f"Unsupported cadence: {spec.cadence}")


def resolve_crawler(
    spec: DatasetSpec,
    module_name: str = "finlab.crawler",
) -> Callable[..., Any]:
    module = importlib.import_module(module_name)
    crawler = getattr(module, spec.crawler_function)
    if not callable(crawler):
        raise TypeError(f"{module_name}.{spec.crawler_function} is not callable")
    return crawler


def invoke_crawler(
    spec: DatasetSpec,
    requested_date: dt.date | None,
    *,
    module_name: str = "finlab.crawler",
    max_attempts: int = 2,
    retry_delay_seconds: int = 30,
) -> Any:
    """Call one legacy crawler with bounded retry and no persistence side effects."""
    if spec.takes_date and requested_date is None:
        raise ValueError(f"{spec.name} requires a requested date")
    if not spec.takes_date and requested_date is not None:
        raise ValueError(f"{spec.name} does not accept a requested date")

    crawler = resolve_crawler(spec, module_name)
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return crawler(requested_date) if spec.takes_date else crawler()
        except Exception as exc:  # the orchestrator records the final exception
            last_error = exc
            if attempt < max_attempts:
                time.sleep(retry_delay_seconds * attempt)

    assert last_error is not None
    raise last_error

