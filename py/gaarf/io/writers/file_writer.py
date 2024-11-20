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
"""Module for writing data to a file."""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import, g-bare-generic

import os
from typing import Union

from gaarf.io.writers.abs_writer import AbsWriter


class FileWriter(AbsWriter):
  """Writes Gaarf Report to a local or remote file.

  Attributes:
      destination_folder: Destination where output file is stored.
  """

  def __init__(
    self,
    destination_folder: Union[str, os.PathLike] = os.getcwd(),
    **kwargs: str,
  ) -> None:
    """Initializes FileWriter based on destination folder."""
    super().__init__(**kwargs)
    self.destination_folder = str(destination_folder)

  def create_dir(self) -> None:
    """Creates folders if needed or destination is not remote."""
    if (
      not os.path.isdir(self.destination_folder)
      and '://' not in self.destination_folder
    ):
      os.makedirs(self.destination_folder)

  def write(self) -> None:
    return
