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
"""Defines mechanism for executing queries via SqlAlchemy."""

from __future__ import annotations

try:
  import sqlalchemy
except ImportError as e:
  raise ImportError(
    'Please install google-ads-api-report-fetcher with sqlalchemy support '
    '- `pip install google-ads-api-report-fetcher[sqlalchemy]`'
  ) from e

import logging
import re
from typing import Any

import pandas as pd

from gaarf import query_post_processor


class SqlAlchemyQueryExecutor(query_post_processor.PostProcessorMixin):
  """Handles query execution via SqlAlchemy.

  Attributes:
      engine: Initialized Engine object to operated on a given database.
  """

  def __init__(self, engine: sqlalchemy.engine.base.Engine) -> None:
    """Initializes executor with a given engine.

    Args:
        engine: Initialized Engine object to operated on a given database.
    """
    self.engine = engine

  def execute(
    self,
    script_name: str | None,
    query_text: str,
    params: dict[str, Any] | None = None,
  ) -> pd.DataFrame | None:
    """Executes query in a given database via SqlAlchemy.

    Args:
        script_name: Script identifier.
        query_text: Query to be executed.
        params: Optional parameters to be replaced in query text.

    Returns:
        DataFrame if query returns some data, None if data are saved to DB.
    """
    logging.info('Executing script: %s', script_name)
    query_text = self.replace_params_template(query_text, params)
    with self.engine.begin() as conn:
      if re.findall(r'(create|update) ', query_text.lower()):
        conn.connection.executescript(query_text)
        return None
      temp_table_name = f'temp_{script_name}'.replace('.', '_')
      query_text = f'CREATE TABLE {temp_table_name} AS {query_text}'
      conn.connection.executescript(query_text)
      try:
        result = pd.read_sql(f'SELECT * FROM {temp_table_name}', conn)
      finally:
        conn.connection.execute(f'DROP TABLE {temp_table_name}')
      return result
