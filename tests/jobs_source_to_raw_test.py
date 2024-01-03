import unittest
from unittest.mock import patch

import pandas as pd

from jobs.source_to_raw.fem_ratio import main as fem_ratio
from jobs.source_to_raw.fem_ratio_solar import main as fem_ratio_solar
from jobs.source_to_raw.solar import main as solar
from jobs.source_to_raw.solar_ratio import main as solar_ratio


class TestSourceToRaw(unittest.TestCase):

    @patch('models.engine.execute_sql', return_value=[])
    @patch('models.engine.pd_read_sql', side_effect=[
        pd.DataFrame({"plant_code": ["plant_code1"],
                      "datadate": ["datadate1"], "power": [1]}),
        pd.DataFrame({"site": ["site1"], "plant": ["plant1"], "plant_code": ['plant_code1']}),
    ])
    @patch('models.engine.pd_to_sql', return_value=[])
    def test_fem_ratio(self, mock_pd_to_sql, mock_pd_read_sql, mock_execute_sql):
        expected = True
        result = fem_ratio()
        self.assertEqual(result, expected)

    @patch('models.engine.execute_sql', return_value=[])
    @patch('models.engine.pd_read_sql', side_effect=[
        pd.DataFrame({"plant_code": ["plant_code1"],
                      "datadate": ["datadate1"], "power": [1]}),
        pd.DataFrame({"site": ["site1"], "plant": ["plant1"], "plant_code": ['plant_code1']}),
    ])
    @patch('models.engine.pd_to_sql', return_value=[])
    def test_fem_ratio_solar(self, mock_pd_to_sql, mock_pd_read_sql, mock_execute_sql):
        expected = True
        result = fem_ratio_solar()
        self.assertEqual(result, expected)

    @patch('models.engine.execute_sql', return_value=[])
    @patch('models.engine.pd_read_sql', return_value=pd.DataFrame({"plant": ["plant1"],
                                                                   "amount": [1], "period_start": ['2023-09-13']}))
    @patch('models.engine.pd_to_sql', return_value=[])
    def test_solar_ratio(self, mock_pd_to_sql, mock_pd_read_sql, mock_execute_sql):
        expected = True
        result = solar_ratio()
        self.assertEqual(result, expected)

    @patch('models.engine.execute_sql', return_value=[])
    @patch('models.engine.pd_read_sql', return_value=pd.DataFrame({
        "ratio": [1], "plant": ["plant1"], "period_start": ['2023-09-13']}))
    @patch('models.engine.pd_to_sql', return_value=[])
    @patch('jobs.source_to_raw.solar.getDataFromWzsArmPrd', return_value={'datetime': [], 'actual': [], 'target': [], 'rate': []})
    def test_solar(self, mock_pd_to_sql, mock_pd_read_sql, mock_execute_sql, mock_getDataFromWzsArmPrd):
        expected = True
        result = solar()
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
