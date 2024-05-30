from __future__ import annotations

import datetime

import pytest

from gaarf import report


@pytest.fixture
def single_column_data():
  results = [[1], [2], [3]]
  columns = ['column_1']
  return report.GaarfReport(results, columns)


@pytest.fixture
def sample_data():
  results = [[1, 'two', [3, 4]]]
  columns = ['column_1', 'column_2', 'column_3']
  return report.GaarfReport(results, columns)


@pytest.fixture
def sample_data_with_dates():
  results = [
    [1, datetime.datetime.now(), datetime.datetime.now().date()],
  ]
  columns = ['column_1', 'datetime', 'date']
  return report.GaarfReport(results, columns)
