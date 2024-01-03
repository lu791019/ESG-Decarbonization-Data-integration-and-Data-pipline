import datetime
import unittest
from unittest.mock import patch

from helpers.decarb_date import DecarbDate


class TestSourceToRaw(unittest.TestCase):

    @patch("helpers.decarb_date.get_now", return_value=datetime.datetime(2023, 12, 1))
    def test_start_date_given_month_12_should_get_lastMonth(self, mock_get_now):
        expected = '2023-11-01'

        result = DecarbDate.start_time()
        self.assertEqual(result, expected)

    @patch("helpers.decarb_date.get_now", return_value=datetime.datetime(2023, 1, 1))
    def test_start_date_given_month_1_should_get_last_dec(self, mock_get_now):
        expected = '2022-12-01'

        result = DecarbDate.start_time()
        self.assertEqual(result, expected)

    @patch("helpers.decarb_date.get_now", return_value=datetime.datetime(2023, 12, 1))
    def test_end_date_given_month_1_should_get_lastMonth(self, mock_get_now):
        expected = '2023-11-30'

        result = DecarbDate.end_time()
        self.assertEqual(result, expected)

    @patch("helpers.decarb_date.get_now", return_value=datetime.datetime(2023, 1, 1))
    def test_end_date_given_month_1_should_get_last_dec(self, mock_get_now):
        expected = '2022-12-31'

        result = DecarbDate.end_time()
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
