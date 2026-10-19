from __future__ import annotations

import unittest
from unittest.mock import patch

from requests.exceptions import ChunkedEncodingError

import finlab.crawler as crawler


class _FakeSession:
    def __init__(self, outcome):
        self.outcome = outcome
        self.closed = False

    def get(self, *args, **kwargs):
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome

    def close(self):
        self.closed = True


class CrawlerHttpTests(unittest.TestCase):
    def tearDown(self) -> None:
        if crawler.ses is not None:
            crawler.ses.close()
        crawler.ses = None

    def test_chunked_response_rebuilds_session_and_retries(self) -> None:
        response = object()
        broken = _FakeSession(ChunkedEncodingError("incomplete chunk"))
        healthy = _FakeSession(response)

        with (
            patch.object(crawler, "find_best_session", side_effect=[broken, healthy]),
            patch.object(crawler.time, "sleep"),
            patch.object(crawler.random, "uniform", return_value=0),
        ):
            crawler.ses = None
            result = crawler.requests_get(
                "https://example.invalid/data",
                max_attempts=2,
                retry_base_seconds=0,
            )

        self.assertIs(result, response)
        self.assertTrue(broken.closed)
        self.assertIs(crawler.ses, healthy)

    def test_final_chunked_error_is_not_converted_to_dataframe(self) -> None:
        first = _FakeSession(ChunkedEncodingError("first"))
        second = _FakeSession(ChunkedEncodingError("second"))

        with (
            patch.object(crawler, "find_best_session", side_effect=[first, second]),
            patch.object(crawler.time, "sleep"),
            patch.object(crawler.random, "uniform", return_value=0),
        ):
            crawler.ses = None
            with self.assertRaises(ChunkedEncodingError):
                crawler.requests_get(
                    "https://example.invalid/data",
                    max_attempts=2,
                    retry_base_seconds=0,
                )

        self.assertTrue(first.closed)
        self.assertTrue(second.closed)
        self.assertIsNone(crawler.ses)


if __name__ == "__main__":
    unittest.main()
