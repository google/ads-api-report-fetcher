# Copyright 2023 Google LLC
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
"""Deprecated module for executing queries via SqlAlchemy."""

from __future__ import annotations

from gaarf import exceptions


class SqlAlchemyQueryExecutor:
  """Deprecated class for creating SqlAlchemyQueryExecutor."""

  def __init__(self, **kwargs: str) -> None:
    raise exceptions.GaarfDeprecationError(
      'Loading SqlAlchemyQueryExecutor from `gaarf.sql_executor` is '
      'deprecated; Import SqlAlchemyQueryExecutor from '
      '`gaarf.executors.sql_executor` instead',
    )
