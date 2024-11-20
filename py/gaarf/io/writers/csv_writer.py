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

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-bare-generic

from __future__ import annotations

import csv
import logging
import os
from typing import Literal, Union

import smart_open

from gaarf.io import formatter
from gaarf.io.writers import file_writer
from gaarf.report import GaarfReport


class CsvWriter(file_writer.FileWriter):
  """Writes Gaarf Report to CSV.

  Attributes:
      destination_folder: Destination where CSV files are stored.
      delimiter: CSV delimiter.
      quotechar: CSV writer quotechar.
      quoting: CSV writer quoting.
  """

  def __init__(
    self,
    destination_folder: Union[str, os.PathLike] = os.getcwd(),
    delimiter: str = ',',
    quotechar: str = '"',
    quoting: Literal[0] = csv.QUOTE_MINIMAL,
    **kwargs,
  ) -> None:
    """Initializes CsvWriter based on a destination_folder.

    Args:
      destination_folder: Destination where CSV files are stored.
      delimiter: CSV delimiter.
      quotechar: CSV writer quotechar.
      quoting: CSV writer quoting.
      kwargs: Optional keyword arguments to initialize writer.
    """
    super().__init__(destination_folder=destination_folder, **kwargs)
    self.delimiter = delimiter
    self.quotechar = quotechar
    self.quoting = quoting

  def __str__(self):
    return (
      f'[CSV] - data are saved to {self.destination_folder} '
      'destination_folder.'
    )

  def write(self, report: GaarfReport, destination: str) -> str:
    """Writes Gaarf report to a CSV file.

    Args:
        report: Gaarf report.
        destination: Base file name report should be written to.

    Returns:
        Full path where data are written.
    """
    report = self.format_for_write(report)
    destination = formatter.format_extension(destination, new_extension='.csv')
    self.create_dir()
    logging.debug('Writing %d rows of data to %s', len(report), destination)
    output_path = os.path.join(self.destination_folder, destination)
    with smart_open.open(
      output_path,
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
    logging.debug('Writing to %s is completed', output_path)
    return f'[CSV] - at {output_path}'
