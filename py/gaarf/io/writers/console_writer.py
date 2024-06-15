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
"""Module for writing data with console."""

from __future__ import annotations

import rich
from rich import console, table
from rich import json as rich_json

from gaarf import report as gaarf_report
from gaarf.io.writers import abs_writer


class ConsoleWriter(abs_writer.AbsWriter):
  """Writes reports to standard output.

  Attributes:
    page_size: How many row of report should be written
    type: Type of output ('table', 'json').
  """

  def __init__(
    self, page_size: int = 10, format: str = 'table', **kwargs: str
  ) -> None:
    """Initializes ConsoleWriter.

    Args:
        page_size: How many row of report should be written
        format: Type of output ('table', 'json').
        kwargs: Optional parameter to initialize writer.
    """
    super().__init__(**kwargs)
    self.page_size = int(page_size)
    self.format = format

  def write(self, report: gaarf_report.GaarfReport, destination: str) -> None:
    """Writes Gaarf report to standard output.

    Args:
      report: Gaarf report.
      destination: Base file name report should be written to.
    """
    report = self.format_for_write(report)
    if self.format == 'table':
      output_table = table.Table(
        title=f"showing results for query <{destination.split('/')[-1]}>",
        caption=(
          f'showing rows 1-{min(self.page_size, len(report))} '
          f'out of total {len(report)}'
        ),
        box=rich.box.MARKDOWN,
      )
      for header in report.column_names:
        output_table.add_column(header)
      for i, row in enumerate(report):
        if i < self.page_size:
          output_table.add_row(*[str(field) for field in row])
    elif self.format == 'json':
      output_table = rich_json.JSON(report.to_json())
    console.Console().print(output_table)
