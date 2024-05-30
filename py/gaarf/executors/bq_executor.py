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
"""Module for executing queries in BigQuery."""

from __future__ import annotations

try:
  from google.cloud import bigquery  # type: ignore
except ImportError as e:
  raise ImportError(
    'Please install google-ads-api-report-fetcher with sqlalchemy support '
    '- `pip install google-ads-api-report-fetcher[bq]`'
  ) from e

import logging

import pandas as pd
from google.cloud import exceptions as google_cloud_exceptions

from gaarf import query_post_processor

logger = logging.getLogger(__name__)


class BigQueryExecutorException(Exception):
  """Error when executor fails to run query."""


class BigQueryExecutor(query_post_processor.PostProcessorMixin):
  """Handles query execution in BigQuery.

  Attributes:
      project_id: Google Cloud project id.
      location: BigQuery dataset location.
      client: BigQuery client.
  """

  def __init__(self, project_id: str, location: str | None = None) -> None:
    """Initializes BigQueryExecutor.

    Args:
        project_id: Google Cloud project id.
        location: BigQuery dataset location.
    """
    self.project_id = project_id
    self.location = location

  @property
  def client(self) -> bigquery.Client:
    """Instantiates bigquery client."""
    return bigquery.Client(self.project_id)

  def execute(
    self, script_name: str, query_text: str, params: dict | None = None
  ) -> pd.DataFrame | None:
    """Executes query in BigQuery.

    Args:
        script_name: Script identifier.
        query_text: Query to be executed.
        params: Optional parameters to be replaced in query text.

    Returns:
        DataFrame if query returns some data, None if it creates data in BQ.
    """
    query_text = self.replace_params_template(query_text, params)
    job = self.client.query(query_text)
    try:
      result = job.result()
      logger.debug('%s launched successfully', script_name)
      if result.total_rows:
        return result.to_dataframe()
      return None
    except Exception as e:
      raise BigQueryExecutorException(e) from e

  def create_datasets(self, macros: dict | None) -> None:
    """Creates datasets in BQ based on values in a dict.

    If dict contains keys with 'dataset' in them, then values for such keys
    are treated as dataset names.

    Args:
        macros: Mapping containing data for query execution.
    """
    if macros and (datasets := extract_datasets(macros)):
      for dataset in datasets:
        dataset_id = f'{self.project_id}.{dataset}'
        try:
          self.client.get_dataset(dataset_id)
        except google_cloud_exceptions.NotFound:
          bq_dataset = bigquery.Dataset(dataset_id)
          bq_dataset.location = self.location
          self.client.create_dataset(bq_dataset, timeout=30)
          logger.debug('Created new dataset %s', dataset_id)


def extract_datasets(macros: dict | None) -> list[str]:
  """Finds dataset-related keys based on values in a dict.

  If dict contains keys with 'dataset' in them, then values for such keys
  are treated as dataset names.

  Args:
      macros: Mapping containing data for query execution.

  Returns:
      Possible names of datasets.
  """
  if not macros:
    return []
  return [value for macro, value in macros.items() if 'dataset' in macro]
