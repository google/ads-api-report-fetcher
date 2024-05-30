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
"""Module for formatting Gaarf reports before writing."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Callable, Union, get_args

import proto  # type: ignore
from typing_extensions import TypeAlias

from gaarf.report import GaarfReport

_NESTED_FIELD: TypeAlias = Union[
  list,
  proto.marshal.collections.repeated.RepeatedComposite,
  proto.marshal.collections.repeated.Repeated,
]


class FormattingStrategy:
  """Interface for all formatting strategies applied to GaarfReport."""

  def apply_transformations(self, report: GaarfReport) -> GaarfReport:
    """Applies class transformation to report."""
    raise NotImplementedError

  def _cast_to_enum(self, enum: type[Enum], value: str | Enum) -> Enum:
    """Ensures that strings are always converted to Enums."""
    return enum[value.upper()] if isinstance(value, str) else value


class ArrayHandling(Enum):
  """Specifies acceptable options for ArrayHandlingStrategy."""

  STRINGS = 1
  ARRAYS = 2


class ArrayHandlingStrategy(FormattingStrategy):
  """Handles arrays in the report.

  Arrays can be left as-is or converted to strings with required delimiter.

  Attributes:
      type_: Type of array handling (ARRAYS, STRINGS).
      delimiter: Symbol used as delimiter when type_ is STRINGS.
  """

  def __init__(
    self,
    type_: ArrayHandling | str = ArrayHandling.STRINGS,
    delimiter: str = '|',
  ) -> None:
    """Initializes strategy based on type_ and delimiter.

    Args:
        type_: Type of array handling (ARRAYS, STRINGS).
        delimiter: Symbol used as delimiter when type_ is STRINGS.
    """
    self.type_ = self._cast_to_enum(ArrayHandling, type_)
    self.delimiter = delimiter

  def apply_transformations(self, report: GaarfReport) -> GaarfReport:
    """Replaces arrays in the report."""
    if self.type_ == ArrayHandling.ARRAYS:
      return report

    formatted_rows = self._format_rows(report.results, self._delimiter_join)
    formatted_placeholders = self._format_rows(
      report.results_placeholder, lambda x: ''
    )
    return GaarfReport(
      results=formatted_rows,
      column_names=report.column_names,
      results_placeholder=formatted_placeholders,
    )

  def _format_rows(
    self, rows: list[list], nested_field_handler: Callable
  ) -> list[list]:
    """Formats rows of report based on join_strategy.

    Args:
        rows: Rows on GaarfReport.
        nested_field_handler: Handlers to nested structures.

    Returns:
        Formatted rows.
    """
    formatted_rows = []
    for row in rows:
      formatted_row = []
      for field in row:
        if isinstance(field, get_args(_NESTED_FIELD)):
          field = nested_field_handler(field)
        formatted_row.append(field)
      formatted_rows.append(formatted_row)
    return formatted_rows

  def _delimiter_join(self, field: _NESTED_FIELD) -> str:
    """Helper function to perform join by an instance delimiter.

    Args:
        field: A nested field.

    Returns:
        The same field but concatenated to string.
    """
    return self.delimiter.join([str(element) for element in field])


def format_report_for_writing(
  report: GaarfReport, formatting_strategies: list[FormattingStrategy]
) -> GaarfReport:
  """Applies formatting strategies to report.

  Args:
      report: Report that needs to be formatted.
      formatting_strategies: Strategies to be applied to report.

  Returns:
      New report with updated data.
  """
  for strategy in formatting_strategies:
    report = strategy.apply_transformations(report)
  return report


def format_extension(
  path_object: str, current_extension: str = '.sql', new_extension: str = ''
) -> str:
  """Formats query path to required extension.

  Args:
      path_object: Path to query.
      current_extension: Extension of the query.
      new_extension: Required extension

  Returns:
     Path with an updated extension.
  """
  path_object_name = Path(path_object).name
  if len(path_object_name.split('.')) > 1:
    return path_object_name.replace(current_extension, new_extension)
  return f'{path_object}{new_extension}'
