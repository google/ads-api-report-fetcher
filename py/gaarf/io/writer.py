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

"""Module for defining writer factory."""

from __future__ import annotations

from garf_io import writer

from gaarf import exceptions

create_writer = writer.create_writer


class WriterFactory:
  """Deprecated class for creating concrete writer."""

  def __init__(self) -> None:
    raise exceptions.GaarfDeprecationError(
      'WriterFactory is deprecated, use `gaarf.io.writer.create_writer` '
      'function instead',
    )


# Deprecated writers
class BigQueryWriter:
  """Deprecated class for creating BigQueryWriter."""

  def __init__(self, **kwargs):
    raise exceptions.GaarfDeprecationError(
      'Loading BigQueryWriter from `gaarf.io.writer` is deprecated; '
      'Import BigQueryWriter from `gaarf.io.writers.bigquery_writer` '
      'instead',
    )


class CsvWriter:
  """Deprecated class for creating CsvWriter."""

  def __init__(self, **kwargs):
    raise exceptions.GaarfDeprecationError(
      'Loading CsvWriter from `gaarf.io.writer` is deprecated; '
      'Import CsvWriter from `gaarf.io.writers.csv_writer` instead',
    )


class SqlAlchemyWriter:
  """Deprecated class for creating SqlAlchemyWriter."""

  def __init__(self, **kwargs):
    raise exceptions.GaarfDeprecationError(
      'Loading SqlAlchemyWriter from `gaarf.io.writer` is deprecated; '
      'Import SqlAlchemyWriter from `gaarf.io.writers.sqldb_writer` '
      'instead',
    )


class SheetWriter:
  """Deprecated class for creating SheetWriter."""

  def __init__(self, **kwargs):
    raise exceptions.GaarfDeprecationError(
      'Loading SheetWriter from `gaarf.io.writer` is deprecated; '
      'Import SheetWriter from `gaarf.io.writers.sheets_writer` instead',
    )


class StdoutWriter:
  """Deprecated class for creating StdoutWriter."""

  def __init__(self, **kwargs):
    raise exceptions.GaarfDeprecationError(
      'Loading StdoutWriter from `gaarf.io.writer` is deprecated; '
      'Import ConsoleWriter from `gaarf.io.writers.console_writer` '
      'instead',
    )


class ZeroRowException(Exception):
  """Raised when report has no data."""
