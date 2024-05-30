# Copyright 2022 Google LLC
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

import warnings

from gaarf.executors import bq_executor


class BigQueryExecutor:
  """Deprecated class for creating BigQueryExecutor."""

  def __new__(
    cls, project_id: str, location: str | None = None
  ) -> bq_executor.BigQueryExecutor:
    warnings.warn(
      'Loading BigQueryExecutor from `gaarf.bq_executor` is '
      'deprecated; Import BigQueryExecutor from '
      '`gaarf.executors.bq_executor` instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return bq_executor.BigQueryExecutor(project_id, location)


def extract_datasets(macros: dict[str, str] | None = None) -> list[str]:
  """Deprecated method for extracting BigQuery datasets."""
  warnings.warn(
    'Loading `extract_datasets` from `gaarf.bq_executor` is '
    'deprecated; Import `extract_datasets` from '
    '`gaarf.executors.bq_executor` instead',
    category=DeprecationWarning,
    stacklevel=2,
  )
  return bq_executor.extract_datasets(macros)
