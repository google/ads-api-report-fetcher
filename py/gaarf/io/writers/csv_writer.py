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
"""Module for writing data with CSV."""

from __future__ import annotations

import csv
import logging
import os
from typing import Literal

from gaarf.io import formatter
from gaarf.io.writers.abs_writer import AbsWriter
from gaarf.report import GaarfReport


class CsvWriter(AbsWriter):
  def __init__(
    self,
    destination_folder: str = os.getcwd(),
    delimiter: str = ',',
    quotechar: str = '"',
    quoting: Literal[0] = csv.QUOTE_MINIMAL,
    **kwargs,
  ) -> None:
    super().__init__(**kwargs)
    self.destination_folder = destination_folder
    self.delimiter = delimiter
    self.quotechar = quotechar
    self.quoting = quoting

  def __str__(self):
    return (
      f'[CSV] - data are saved to {self.destination_folder} '
      'destination_folder.'
    )

  def write(self, report: GaarfReport, destination: str) -> str:
    report = self.format_for_write(report)
    destination = formatter.format_extension(destination, new_extension='.csv')
    if not os.path.isdir(self.destination_folder):
      os.makedirs(self.destination_folder)
    logging.debug('Writing %d rows of data to %s', len(report), destination)
    with open(
      os.path.join(self.destination_folder, destination),
      encoding='utf-8',
      mode='w',
    ) as file:
      writer = csv.writer(
        file,
        delimiter=self.delimiter,
        quotechar=self.quotechar,
        quoting=self.quoting,
      )
      writer.writerow(report.column_names)
      writer.writerows(report.results)
    logging.debug('Writing to %s is completed', destination)
    return f'[CSV] - at {destination}'
