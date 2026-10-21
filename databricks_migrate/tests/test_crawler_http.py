from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

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

    def test_twse_cap_reduction_uses_eight_digit_start_date(self) -> None:
        csv_text = """109年01月01日 至 115年10月21日 股票減資恢復買賣參考價格
恢復買賣日期,股票代號,名稱,停止買賣前收盤價格,恢復買賣參考價,漲停價格,跌停價格,開盤競價基準,除權參考價,減資原因,詳細資料
115/10/21,1234,測試公司,100,50,55,45,50,50,彌補虧損,-
"""
        response = Mock(text=csv_text)

        with patch.object(crawler, "requests_get", return_value=response) as request:
            result = crawler.crawl_twse_cap_reduction()

        response.raise_for_status.assert_called_once_with()
        params = request.call_args.kwargs["params"]
        self.assertEqual(params["startDate"], "20200101")
        self.assertRegex(params["endDate"], r"^\d{8}$")
        self.assertEqual(result.index.names, ["stock_id", "date"])
        self.assertEqual(result.iloc[0]["twse_cap_divide_ratio"], 2.0)

    def test_twse_cap_reduction_reports_empty_response(self) -> None:
        response = Mock(text="  \n")

        with (
            patch.object(crawler, "requests_get", return_value=response),
            self.assertRaisesRegex(
                RuntimeError,
                "TWSE TWTAUU returned an empty response",
            ),
        ):
            crawler.crawl_twse_cap_reduction()

        response.raise_for_status.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
