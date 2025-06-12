# Copyright 2025 Google LLC
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

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

"""Loads queries to be processed by Gaarf."""

from garf_io import reader

from gaarf import exceptions

FileReader = reader.FileReader
ConsoleReader = reader.ConsoleReader
create_reader = reader.create_reader


class ReaderFactory:
  """Deprecated class for creating readers."""

  def __init__(self):
    raise exceptions.GaarfDeprecationError(
      'ReaderFactory is deprecated; '
      'Create reader with `gaarf.io.reader.create_reader` function instead',
    )
