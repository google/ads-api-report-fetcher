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
"""Module for writing data with SqlAlchemy."""
from __future__ import annotations
try:
    import sqlalchemy
except ImportError as e:
    raise ImportError(
        'Please install google-ads-api-report-fetcher with sqlalchemy support '
        '- `pip install google-ads-api-report-fetcher[sqlalchemy]`') from e

import logging
import pandas as pd

from gaarf.report import GaarfReport
from gaarf.io import formatter
from gaarf.io.writers.abs_writer import AbsWriter


class SqlAlchemyWriter(AbsWriter):

    def __init__(self,
                 connection_string: str,
                 if_exists: str = 'replace',
                 **kwargs):
        super().__init__(**kwargs)
        self.connection_string = connection_string
        self.if_exists = if_exists

    def write(self, report: GaarfReport, destination: str) -> None:
        report = self.format_for_write(report)
        destination = formatter.format_extension(destination)
        if not report:
            df = pd.DataFrame(data=report.results_placeholder,
                              columns=report.column_names).head(0)
        else:
            df = report.to_pandas()
        logging.debug('Writing %d rows of data to %s', len(df), destination)
        engine = self._create_engine()
        with engine.connect() as conn:
            df.to_sql(name=destination,
                      con=conn.connection,
                      index=False,
                      if_exists=self.if_exists)
        logging.debug('Writing to %s is completed', destination)

    def _create_engine(self):
        return sqlalchemy.create_engine(self.connection_string)
