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
from collections.abc import Sequence

import numpy as np
import pandas as pd
import proto  # type: ignore
from google.cloud import exceptions as google_cloud_exceptions

import gaarf
from gaarf import parsers
from gaarf.io import formatter
from gaarf.io.writers import abs_writer


class BigQueryWriter(abs_writer.AbsWriter):
  """Writes Gaarf Report to BigQuery.

  Attributes:
    project: Id of Google Cloud Project.
    dataset: BigQuery dataset to write data to.
    location: Location of a newly created dataset.
    write_disposition: Option for overwriting data.
  """

  def __init__(
    self,
    project: str,
    dataset: str,
    location: str = 'US',
    write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    **kwargs,
  ):
    """Initializes BigQueryWriter.

    Args:
      project: Id of Google Cloud Project.
      dataset: BigQuery dataset to write data to.
      location: Location of a newly created dataset.
      write_disposition: Option for overwriting data.
      kwargs: Optional keywords arguments.
    """
    super().__init__(**kwargs)
    self.project = project
    self.dataset_id = f'{project}.{dataset}'
    self.location = location
    self.write_disposition = write_disposition

  def __str__(self) -> str:
    return f'[BigQuery] - {self.dataset_id} at {self.location} location.'

  @property
  def client(self) -> bigquery.Client:
    """Instantiated BigQuery client."""
    return bigquery.Client(self.project)

  def create_or_get_dataset(self) -> bigquery.Dataset:
    """Gets existing dataset or create a new one."""
    try:
      bq_dataset = self.client.get_dataset(self.dataset_id)
    except google_cloud_exceptions.NotFound:
      bq_dataset = bigquery.Dataset(self.dataset_id)
      bq_dataset.location = self.location
      bq_dataset = self.client.create_dataset(bq_dataset, timeout=30)
    return bq_dataset

  def write(self, report: gaarf.report.GaarfReport, destination: str) -> str:
    """Writes Gaarf report to a BigQuery table.

    Args:
      report: Gaarf report.
      destination: Name of the table report should be written to.

    Returns:
      Name of the table in `dataset.table` format.
    """
    report = self.format_for_write(report)
    schema = _define_schema(report)
    destination = formatter.format_extension(destination)
    table = self._create_or_get_table(
      f'{self.dataset_id}.{destination}', schema
    )
    job_config = bigquery.LoadJobConfig(
      write_disposition=self.write_disposition,
      schema=schema,
      source_format='CSV',
      max_bad_records=len(report),
    )

    if not report:
      df = pd.DataFrame(
        data=report.results_placeholder, columns=report.column_names
      ).head(0)
    else:
      df = report.to_pandas()
    df = df.replace({np.nan: None})
    logging.debug('Writing %d rows of data to %s', len(df), destination)
    job = self.client.load_table_from_dataframe(
      dataframe=df, destination=table, job_config=job_config
    )
    try:
      job.result()
      logging.debug('Writing to %s is completed', destination)
    except google_cloud_exceptions.BadRequest as e:
      raise ValueError(f'Unable to save data to BigQuery! {str(e)}') from e
    return f'[BigQuery] - at {self.dataset_id}.{destination}'

  def _create_or_get_table(
    self, table_name: str, schema: Sequence[bigquery.SchemaField]
  ) -> bigquery.Table:
    """Gets existing table or create a new one.

    Args:
      table_name: Name of the table in BigQuery.
      schema: Schema of the table if one should be created.

    Returns:
      BigQuery table object.
    """
    try:
      table = self.client.get_table(table_name)
    except google_cloud_exceptions.NotFound:
      table_ref = bigquery.Table(table_name, schema=schema)
      table = self.client.create_table(table_ref)
      table = self.client.get_table(table_name)
    return table


def _define_schema(
  report: gaarf.report.GaarfReport,
) -> list[bigquery.SchemaField]:
  """Infers schema from GaarfReport.

  Args:
    report: GaarfReport to infer schema from.

  Returns:
    Schema fields for a given report.

  """
  result_types = _get_result_types(report)
  return _get_bq_schema(result_types)


def _get_result_types(
  report: gaarf.report.GaarfReport,
) -> dict[str, dict[str, parsers.GoogleAdsRowElement]]:
  """Maps each column of report to BigQuery field type and repeated status.

  Fields types are inferred based on report resuls or results placeholder.

  Args:
    report: GaarfReport to infer field types from.

  Returns:
    Mapping between each column of report and its field type.
  """
  result_types: dict[str, dict[str, parsers.GoogleAdsRowElement]] = {}
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


def _get_bq_schema(
  types: dict[str, dict[str, parsers.GoogleAdsRowElement]],
) -> list[bigquery.SchemaField]:
  """Converts GoogleAds fields types to BigQuery schema fields.

  Args:
    types: Mapping between column names and its field type.

  Returns:
     BigQuery schema fields corresponding to GaarfReport.
  """
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
        field_type=field_type if field_type else 'STRING',
        mode='REPEATED' if value.get('repeated') else 'NULLABLE',
      )
    )
  return schema
