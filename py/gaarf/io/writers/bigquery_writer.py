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
"""Module for writing data to BigQuery."""

from __future__ import annotations

try:
  from google.cloud import bigquery
except ImportError as e:
  raise ImportError(
    'Please install google-ads-api-report-fetcher with BigQuery support '
    '- `pip install google-ads-api-report-fetcher[bq]`'
  ) from e

import datetime
import logging

import numpy as np
import pandas as pd
import proto  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore

from gaarf.io import formatter
from gaarf.io.writers.abs_writer import AbsWriter
from gaarf.report import GaarfReport


class BigQueryWriter(AbsWriter):
  def __init__(
    self,
    project: str,
    dataset: str,
    location: str = 'US',
    write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    **kwargs,
  ):
    super().__init__(**kwargs)
    self.project = project
    self.dataset_id = f'{project}.{dataset}'
    self.location = location
    self.write_disposition = write_disposition
    self.client = None

  def __str__(self) -> str:
    return f'[BigQuery] - {self.dataset_id} at {self.location} location.'

  def create_or_get_dataset(self) -> bigquery.Dataset:
    self._init_client()
    try:
      bq_dataset = self.client.get_dataset(self.dataset_id)
    except NotFound:
      bq_dataset = bigquery.Dataset(self.dataset_id)
      bq_dataset.location = self.location
      bq_dataset = self.client.create_dataset(bq_dataset, timeout=30)
    return bq_dataset

  def write(self, report: GaarfReport, destination: str) -> str:
    report = self.format_for_write(report)
    schema = self._define_schema(report)
    destination = formatter.format_extension(destination)
    table = self._create_or_get_table(
      f'{self.dataset_id}.{destination}', schema
    )
    job_config = bigquery.LoadJobConfig(
      write_disposition=self.write_disposition,
      schema=schema,
      source_format='CSV',
    )

    if not report:
      df = pd.DataFrame(
        data=report.results_placeholder, columns=report.column_names
      ).head(0)
    else:
      df = report.to_pandas()
    df = df.replace({np.nan: None})
    logging.debug('Writing %d rows of data to %s', len(df), destination)
    try:
      self.client.load_table_from_dataframe(
        dataframe=df, destination=table, job_config=job_config
      )
      logging.debug('Writing to %s is completed', destination)
    except Exception as e:
      raise ValueError(f'Unable to save data to BigQuery! {str(e)}') from e
    return f'[BigQuery] - at {self.dataset_id}.{destination}'

  @staticmethod
  def _define_schema(report: GaarfReport) -> list[bigquery.SchemaField]:
    result_types = BigQueryWriter._get_result_types(report)
    return BigQueryWriter._get_bq_schema(result_types)

  @staticmethod
  def _get_result_types(report: GaarfReport) -> dict[str, dict]:
    result_types: dict[str, dict] = {}
    column_names = report.column_names
    for row in report.results or report.results_placeholder:
      if set(column_names) == set(result_types.keys()):
        break
      for i, field in enumerate(row):
        if field is None or column_names[i] in result_types:
          continue
        field_type = type(field)
        if field_type in [
          list,
          proto.marshal.collections.repeated.RepeatedComposite,
          proto.marshal.collections.repeated.Repeated,
        ]:
          repeated = True
          if len(field) == 0:
            field_type = str
          else:
            field_type = type(field[0])
        else:
          field_type = type(field)
          repeated = False
        result_types[column_names[i]] = {
          'field_type': field_type,
          'repeated': repeated,
        }
    return result_types

  @staticmethod
  def _get_bq_schema(types) -> list[bigquery.SchemaField]:
    type_mapping = {
      list: 'REPEATED',
      str: 'STRING',
      datetime.datetime: 'DATETIME',
      datetime.date: 'DATE',
      int: 'INT64',
      float: 'FLOAT64',
      bool: 'BOOL',
      proto.marshal.collections.repeated.RepeatedComposite: 'REPEATED',
      proto.marshal.collections.repeated.Repeated: 'REPEATED',
    }

    schema: list[bigquery.SchemaField] = []
    for key, value in types.items():
      field_type = type_mapping.get(value.get('field_type'))
      schema.append(
        bigquery.SchemaField(
          name=key,
          field_type='STRING' if not field_type else field_type,
          mode='REPEATED' if value.get('repeated') else 'NULLABLE',
        )
      )
    return schema

  def _init_client(self) -> None:
    if not self.client:
      self.client = bigquery.Client(self.project)

  def _create_or_get_table(self, table_name: str, schema) -> bigquery.Table:
    self._init_client()
    try:
      table = self.client.get_table(table_name)
    except NotFound:
      table_ref = bigquery.Table(table_name, schema=schema)
      table = self.client.create_table(table_ref)
      table = self.client.get_table(table_name)
    return table
