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
from rich.console import Console
from rich.table import Table

from gaarf.io.writers.abs_writer import AbsWriter
from gaarf.report import GaarfReport


class ConsoleWriter(AbsWriter):
  def __init__(self, page_size: int = 10, **kwargs):
    super().__init__(**kwargs)
    self.page_size = int(page_size)

  def write(self, report: GaarfReport, destination: str) -> None:
    report = self.format_for_write(report)
    console = Console()
    table = Table(
      title=f"showing results for query <{destination.split('/')[-1]}>",
      caption=f'showing rows 1-{min(self.page_size, len(report))} out of total {len(report)}',
      box=rich.box.MARKDOWN,
    )
    for header in report.column_names:
      table.add_column(header)
    for i, row in enumerate(report):
      if i < self.page_size:
        table.add_row(*[str(field) for field in row])
    console.print(table)
