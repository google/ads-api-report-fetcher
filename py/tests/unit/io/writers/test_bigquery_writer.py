# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import pytest
from google.cloud import bigquery

from gaarf.io.writers import bigquery_writer


class TestBigQueryWriter:
  @pytest.fixture
  def bq_writer(self):
    return bigquery_writer.BigQueryWriter(project='test', dataset='test')

  def test_get_results_types_returns_correct_mapping(self, sample_data):
    result_types = bigquery_writer._get_result_types(sample_data)
    assert result_types == {
      'column_1': {'field_type': int, 'repeated': False},
      'column_2': {'field_type': str, 'repeated': False},
      'column_3': {'field_type': int, 'repeated': True},
    }

  def test_define_schema_returns_correct_schema_fields(self, sample_data):
    schema = bigquery_writer._define_schema(sample_data)
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

  def test_define_schema_correctly_handles_dates(self, sample_data_with_dates):
    schema = bigquery_writer._define_schema(sample_data_with_dates)
    assert schema == [
      bigquery.SchemaField(
        'column_1', 'INT64', 'NULLABLE', None, None, (), None
      ),
      bigquery.SchemaField(
        'datetime', 'DATETIME', 'NULLABLE', None, None, (), None
      ),
      bigquery.SchemaField('date', 'DATE', 'NULLABLE', None, None, (), None),
    ]
