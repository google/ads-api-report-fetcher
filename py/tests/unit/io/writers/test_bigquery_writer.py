from __future__ import annotations

import pytest
from gaarf.io.writers import bigquery_writer
from google.cloud import bigquery


class TestBigQueryWriter:
  @pytest.fixture
  def bq_writer(self):
    return bigquery_writer.BigQueryWriter(project='test', dataset='test')

  def test_bq_get_results_types(self, bq_writer, sample_data):
    result_types = bq_writer._get_result_types(sample_data)
    assert result_types == {
      'column_1': {'field_type': int, 'repeated': False},
      'column_2': {'field_type': str, 'repeated': False},
      'column_3': {'field_type': int, 'repeated': True},
    }

  def test_get_results_types_returns_arrays(self, bq_writer, sample_data):
    result_types = bq_writer._get_result_types(sample_data)
    assert result_types == {
      'column_1': {'field_type': int, 'repeated': False},
      'column_2': {'field_type': str, 'repeated': False},
      'column_3': {'field_type': int, 'repeated': True},
    }

  def test_bq_get_correct_schema(self, bq_writer, sample_data):
    schema = bq_writer._define_schema(sample_data)
    assert schema == [
      bigquery.SchemaField(
        'column_1', 'INT64', 'NULLABLE', None, None, (), None
      ),
      bigquery.SchemaField(
        'column_2', 'STRING', 'NULLABLE', None, None, (), None
      ),
      bigquery.SchemaField(
        'column_3', 'INT64', 'REPEATED', None, None, (), None
      ),
    ]

  def test_bq_get_correct_schema_with_dates(
    self, bq_writer, sample_data_with_dates
  ):
    schema = bq_writer._define_schema(sample_data_with_dates)
    assert schema == [
      bigquery.SchemaField(
        'column_1', 'INT64', 'NULLABLE', None, None, (), None
      ),
      bigquery.SchemaField(
        'datetime', 'DATETIME', 'NULLABLE', None, None, (), None
      ),
      bigquery.SchemaField('date', 'DATE', 'NULLABLE', None, None, (), None),
    ]
