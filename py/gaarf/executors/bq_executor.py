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

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

"""Module for executing queries in BigQuery."""

from __future__ import annotations

import logging

import pandas as pd
from garf_executors import bq_executor, execution_context

logger = logging.getLogger(__name__)


class BigQueryExecutor(bq_executor.BigQueryExecutor):
  """Handles query execution in BigQuery."""

  def execute(
    self, script_name: str, query_text: str, params: dict | None = None
  ) -> pd.DataFrame:
    """Executes query in BigQuery.

    Args:
        script_name: Script identifier.
        query_text: Query to be executed.
        params: Optional parameters to be replaced in query text.

    Returns:
        DataFrame if query returns some data otherwise empty DataFrame.
    """
    context = (
      execution_context.ExecutionContext(query_parameters=params)
      if params
      else execution_context.ExecutionContext()
    )
    report = super().execute(
      query=query_text, title=script_name, context=context
    )
    return report.to_pandas()


extract_datasets = bq_executor.extract_datasets
