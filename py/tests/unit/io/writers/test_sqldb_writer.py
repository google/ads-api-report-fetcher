from __future__ import annotations

import pandas as pd
import pytest
from gaarf import report
from gaarf.io.writers import sqldb_writer

_TMP_NAME = 'test'


class TestSqlAlchemyWriter:
  @pytest.fixture
  def sql_writer(self, tmp_path):
    db_path = tmp_path / 'test.db'
    db_url = f'sqlite:///{db_path}'
    return sqldb_writer.SqlAlchemyWriter(db_url)

  def test_write_single_column_report_returns_correct_data(
    self, sql_writer, single_column_data, tmp_path
  ):
    sql_writer.write(single_column_data, _TMP_NAME)
    df = pd.read_sql(f'SELECT * FROM {_TMP_NAME}', sql_writer.connection_string)

    assert report.GaarfReport.from_pandas(df) == single_column_data

  def test_write_multi_column_report_with_arrays_returns_concatenated_strings(
    self, sql_writer, sample_data
  ):
    results = [[1, 'two', '3|4']]
    columns = ['column_1', 'column_2', 'column_3']
    expected_report = report.GaarfReport(results, columns)

    sql_writer.array_handling = 'strings'
    sql_writer.write(sample_data, _TMP_NAME)

    df = pd.read_sql(f'SELECT * FROM {_TMP_NAME}', sql_writer.connection_string)

    assert report.GaarfReport.from_pandas(df) == expected_report
