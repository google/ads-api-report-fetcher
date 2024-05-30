# Copyright 2022 Google LLC
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
"""Module for defining writer factory."""

from __future__ import annotations

import csv
import os
import warnings
from importlib import import_module

from gaarf.io.writers import abs_writer


def create_writer(
  writer_option: str, **kwargs: str
) -> type[abs_writer.AbsWriter]:
  """Factory function for creating concrete writer.

  Writer is created based on `writer_option` and possible `kwargs` needed
  to correctly instantiate it.

  Args:
      writer_option: Type of writer.
      kwargs: Any possible arguments needed o instantiate writer.

  Returns:
      Concrete instantiated writer.
  """
  if writer_option in ('bq', 'bigquery'):
    writer_module = import_module('gaarf.io.writers.bigquery_writer')
    return writer_module.BigQueryWriter(**kwargs)
  if writer_option == 'sqldb':
    writer_module = import_module('gaarf.io.writers.sqldb_writer')
    return writer_module.SqlAlchemyWriter(**kwargs)
  if writer_option in ('sheet', 'sheets'):
    writer_module = import_module('gaarf.io.writers.sheets_writer')
    return writer_module.SheetWriter(**kwargs)
  if writer_option == 'console':
    writer_module = import_module('gaarf.io.writers.console_writer')
    return writer_module.ConsoleWriter(**kwargs)
  if writer_option == 'csv':
    writer_module = import_module('gaarf.io.writers.csv_writer')
    return writer_module.CsvWriter(**kwargs)
  if writer_option == 'json':
    writer_module = import_module('gaarf.io.writers.json_writer')
    return writer_module.JsonWriter(**kwargs)
  return import_module('gaarf.io.writers.null_writer').NullWriter(writer_option)


class WriterFactory:
  """Deprecated class for creating concrete writer."""

  def __init__(self) -> None:
    warnings.warn(
      'WritingFactory is deprecated, use `gaarf.io.writer.create_writer` '
      'function instead',
      category=DeprecationWarning,
      stacklevel=2,
    )

  def create_writer(
    self, writer_option: str, **kwargs: str
  ) -> type[abs_writer.AbsWriter]:
    """Factory method for creating concrete writer.

    Writer is created based on `writer_option` and possible `kwargs` needed
    to correctly instantiate it.

    Args:
        writer_option: Type of writer.
        kwargs: Any possible arguments needed o instantiate writer.

    Returns:
        Concrete instantiated writer.
    """
    return create_writer(writer_option, **kwargs)


# Deprecated writers
class BigQueryWriter:
  """Deprecated class for creating BigQueryWriter."""

  def __new__(cls, project: str, dataset: str, location: str = 'US', **kwargs):
    warnings.warn(
      'Loading BigQueryWriter from `gaarf.io.writer` is deprecated; '
      'Import BigQueryWriter from `gaarf.io.writers.bigquery_writer` '
      'instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return create_writer(
      'bq', project=project, dataset=dataset, location=location
    )


class CsvWriter:
  """Deprecated class for creating CsvWriter."""

  def __new__(
    cls,
    destination_folder: str = os.getcwd(),
    delimiter: str = ',',
    quotechar: str = '"',
    quoting=csv.QUOTE_MINIMAL,
    **kwargs,
  ):
    warnings.warn(
      'Loading CsvWriter from `gaarf.io.writer` is deprecated; '
      'Import CsvWriter from `gaarf.io.writers.csv_writer` instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return create_writer(
      'csv',
      destination_folder=destination_folder,
      delimiter=delimiter,
      quotechar=quotechar,
      quoting=quoting,
    )


class SqlAlchemyWriter:
  """Deprecated class for creating SqlAlchemyWriter."""

  def __new__(cls, connection_string, if_exists='replace', **kwargs):
    warnings.warn(
      'Loading SqlAlchemyWriter from `gaarf.io.writer` is deprecated; '
      'Import SqlAlchemyWriter from `gaarf.io.writers.sqldb_writer` '
      'instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return create_writer(
      'sqldb', connection_string=connection_string, if_exists=if_exists
    )


class SheetWriter:
  """Deprecated class for creating SheetWriter."""

  def __new__(
    cls,
    share_with,
    credentials_file,
    spreadsheet_url=None,
    is_append=False,
    **kwargs,
  ):
    warnings.warn(
      'Loading SheetWriter from `gaarf.io.writer` is deprecated; '
      'Import SheetWriter from `gaarf.io.writers.sheets_writer` instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return create_writer(
      'sheet',
      share_with=share_with,
      credentials_file=credentials_file,
      spreadsheet_url=spreadsheet_url,
      is_append=is_append,
    )


class StdoutWriter:
  """Deprecated class for creating StdoutWriter."""

  def __new__(cls, page_size: int = 10, **kwargs):
    warnings.warn(
      'Loading StdoutWriter from `gaarf.io.writer` is deprecated; '
      'Import ConsoleWriter from `gaarf.io.writers.console_writer` '
      'instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return create_writer('console', page_size=page_size)


class ZeroRowException(Exception):
  """Raised when report has no data."""
