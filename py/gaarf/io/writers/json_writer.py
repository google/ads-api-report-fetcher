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
"""Module for writing data to JSON."""

from __future__ import annotations

import json
import logging
import os

import gaarf
from gaarf.io import formatter
from gaarf.io.writers import abs_writer


class JsonWriter(abs_writer.AbsWriter):
  """Writes Gaarf Report to JSON.

  Attributes:
      destination_folder: A local folder where JSON files are stored.
  """

  def __init__(
    self, destination_folder: str = os.getcwd(), **kwargs: str
  ) -> None:
    """Initializes JsonWriter based on a destination_folder.

    Args:
        destination_folder: A local folder where JSON files are stored.
    Returns: Description of return.
    """
    super().__init__(**kwargs)
    self.destination_folder = destination_folder

  def write(self, report: gaarf.report.GaarfReport, destination: str) -> str:
    """Writes Gaarf report to a JSON file.

    Args:
        report: Gaarf report.
        destination: Base file name report should be written to.
    """
    report = self.format_for_write(report)
    destination = formatter.format_extension(destination, new_extension='.json')
    if not os.path.isdir(self.destination_folder):
      os.makedirs(self.destination_folder)
    logging.debug('Writing %d rows of data to %s', len(report), destination)
    with open(
      os.path.join(self.destination_folder, destination), 'w', encoding='utf-8'
    ) as f:
      json.dump(report.to_list(row_type='dict'), f)
    logging.debug('Writing to %s is completed', destination)
    return f'[JSON] - at {destination}'
