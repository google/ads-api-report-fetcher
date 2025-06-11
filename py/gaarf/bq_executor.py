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
"""Deprecated module for executing queries in BigQuery."""

from __future__ import annotations

from gaarf import exceptions


class BigQueryExecutor:
  """Deprecated class for creating BigQueryExecutor."""

  def __init__(self, **kwargs: str) -> None:
    raise exceptions.GaarfDeprecationError(
      'Loading BigQueryExecutor from `gaarf.bq_executor` is '
      'deprecated; Import BigQueryExecutor from '
      '`gaarf.executors.bq_executor` instead',
    )


def extract_datasets(macros: dict[str, str] | None = None) -> list[str]:
  """Deprecated method for extracting BigQuery datasets."""
  raise exceptions.GaarfDeprecationError(
    'Loading `extract_datasets` from `gaarf.bq_executor` is '
    'deprecated; Import `extract_datasets` from '
    '`gaarf.executors.bq_executor` instead',
  )
