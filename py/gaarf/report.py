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
"""Simples handling fetched data from Ads API.

Module exposes two classes:
    * GaarfReport - contains all data from Ads API response alongside methods
      for iteration, slicing and converting to/from common structures.
    * GaarfRow - helper class for dealing with iteration over each response
      row in GaarfReport.
"""

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

from __future__ import annotations

import itertools
import json
import warnings
from collections import defaultdict
from collections.abc import MutableSequence, Sequence
from typing import Generator, Literal, get_args

from gaarf import exceptions, parsers, query_editor


class GaarfReport:
  """Provides convenient handler for working with results from Ads API.

  Attributes:
      results: Contains data from Ads API in a form of nested list
      column_names: Maps in each element in sublist of results to name.
      results_placeholder: Optional placeholder values for missing results.
      query_specification: Specification used to get data from Ads API.
      auto_convert_to_scalars: Whether to simplify slicing operations.
  """

  def __init__(
    self,
    results: Sequence[Sequence[parsers.GoogleAdsRowElement]],
    column_names: Sequence[str],
    results_placeholder: Sequence[Sequence[parsers.GoogleAdsRowElement]]
    | None = None,
    query_specification: query_editor.QuerySpecification | None = None,
    auto_convert_to_scalars: bool = True,
  ) -> None:
    """Initializes GaarfReport from API response.

    Args:
        results: Contains data from Ads API in a form of nested list
        column_names: Maps in each element in sublist of results to name.
        results_placeholder: Optional placeholder values for missing results.
        query_specification: Specification used to get data from Ads API.
        auto_convert_to_scalars: Whether to simplify slicing operations.
    """
    self.results = results
    self.column_names = column_names
    self._multi_column_report = len(column_names) > 1
    if results_placeholder:
      self.results_placeholder = list(results_placeholder)
    else:
      self.results_placeholder = []
    self.query_specification = query_specification
    self.auto_convert_to_scalars = auto_convert_to_scalars

  def disable_scalar_conversions(self):
    """Disables auto conversions of scalars of reports slices.

    Ensures that slicing and indexing operations always return GaarfReport or
    GaarfRow instead of underlying sequences and scalars
    """
    self.auto_convert_to_scalars = False

  def enable_scalar_conversions(self):
    """Enables auto conversions of scalars of reports slices.

    Ensures that slicing and indexing operations might return underlying
    sequences and scalars whenever possible.
    """
    self.auto_convert_to_scalars = True

  def to_list(
    self,
    row_type: Literal['list', 'dict', 'scalar'] = 'list',
    flatten: bool = False,
    distinct: bool = False,
  ) -> list[parsers.GoogleAdsRowElement]:
    """Converts report to a list.

    Args:
        row_type: Expected type of element in the list.
        flatten: Whether to have a flattened list (nested by default).
        distinct: Whether to perform item deduplication in the list.

    Returns:
        List of elements based on the report.

    Raises:
        GaarfReportException: When incorrect row_type is specified.
    """
    if flatten:
      warnings.warn(
        '`GaarfReport` will deprecate passing `flatten=True` '
        "to `to_list` method. Use row_type='scalar' instead.",
        category=DeprecationWarning,
        stacklevel=3,
      )
      row_type = 'scalar'
    if row_type == 'list':
      if self._multi_column_report:
        if distinct:
          return list(set(self.results))
        return self.results
      return self.to_list(row_type='scalar')
    if row_type == 'dict':
      results: list[dict] = []
      for row in iter(self):
        results.append(row.to_dict())
      return results
    if row_type == 'scalar':
      results = list(itertools.chain.from_iterable(self.results))
      if distinct:
        results = list(set(results))
      return results
    raise exceptions.GaarfReportException(
      'incorrect row_type specified', row_type
    )

  def to_dict(
    self,
    key_column: str,
    value_column: str | None = None,
    value_column_output: Literal['scalar', 'list'] = 'list',
  ) -> dict[
    str, parsers.GoogleAdsRowElement | list[parsers.GoogleAdsRowElement]
  ]:
    """Converts report to dictionary.

    Args:
        key_column: Column of report to serve as a key.
        value_column: Column of report to serve as a value.
        value_column_output: How value column should be represented.

    Returns:
        Mapping based on report elements.

    Raises:
        GaarfReportException: When incorrect column_name specified.
    """
    if key_column not in self.column_names:
      raise exceptions.GaarfReportException(
        f'column name {key_column} not found in the report'
      )
    if value_column and value_column not in self.column_names:
      raise exceptions.GaarfReportException(
        f'column name {value_column} not found in the report'
      )
    if value_column_output == 'list':
      output: dict = defaultdict(list)
    else:
      output = {}
    key_index = self.column_names.index(key_column)
    if not (key_generator := list(zip(*self.results))):
      return {key_column: None}
    key_generator = key_generator[key_index]
    if value_column:
      value_index = self.column_names.index(value_column)
      value_generator = list(zip(*self.results))[value_index]
    else:
      value_generator = self.results
    for key, value in zip(key_generator, value_generator):
      if not value_column:
        value = dict(zip(self.column_names, value))
      if value_column_output == 'list':
        output[key].append(value)
      else:
        if key in output:
          raise exceptions.GaarfReportException(
            f'Non unique values found for key_column: {key}'
          )
        output[key] = value
    return output

  def to_pandas(self) -> 'pd.DataFrame':
    """Converts report to Pandas dataframe.

    Returns:
        Dataframe from report results and column_names.

    Raises:
        ImportError: if pandas are not installed.
    """
    try:
      import pandas as pd
    except ImportError as e:
      raise ImportError(
        'Please install google-ads-api-report-fetcher with Pandas support '
        '- `pip install google-ads-api-report-fetcher[pandas]`'
      ) from e
    return pd.DataFrame(data=self.results, columns=self.column_names)

  def to_json(self) -> str:
    """Converts report to JSON.

    Returns:
        JSON from report results and column_names.
    """
    return json.dumps(self.to_list(row_type='dict'))

  @classmethod
  def from_json(cls, json_str: str) -> 'GaarfReport':
    """Creates a GaarfReport object from a JSON string.

    Args:
        json_str: JSON string representation of the data.

    Returns:
        GaarfReport.

    Raises:
        TypeError: If any value in the JSON data is not a supported type.
        ValueError: If `data` is a list but not all dictionaries
        have the same keys.
    """
    data = json.loads(json_str)

    def validate_value(value):
      if not isinstance(value, get_args(parsers.GoogleAdsRowElement)):
        raise TypeError(
          f'Unsupported type {type(value)} for value {value}. '
          'Expected types: int, float, str, bool, list, or None.'
        )
      return value

    # Case 1: `data` is a dictionary
    if isinstance(data, dict):
      column_names = list(data.keys())
      if not data.values():
        results = []
      else:
        results = [[validate_value(value) for value in data.values()]]

    # Case 2: `data` is a list of dictionaries, each representing a row
    elif isinstance(data, list):
      column_names = list(data[0].keys()) if data else []
      for row in data:
        if not isinstance(row, dict):
          raise TypeError('All elements in the list must be dictionaries.')
        if list(row.keys()) != column_names:
          raise ValueError(
            'All dictionaries must have consistent keys in the same order.'
          )
      results = [
        [validate_value(value) for value in row.values()] for row in data
      ]
    else:
      raise TypeError(
        'Input JSON must be a dictionary or a list of dictionaries.'
      )
    return cls(results=results, column_names=column_names)

  def get_value(
    self, column_index: int = 0, row_index: int = 0
  ) -> parsers.GoogleAdsRowElement:
    """Extracts data from report as a scalar.

    Raises:
      GaarfReportException: If row or column index are out of bounds.
    """
    if column_index >= len(self.column_names):
      raise exceptions.GaarfReportException(
        'Column %d of report is not found; report contains only %d columns.',
        column_index,
        len(self.column_names) + 1,
      )
    if row_index >= len(self):
      raise exceptions.GaarfReportException(
        'Row %d of report is not found; report contains only %d rows.',
        row_index,
        len(self) + 1,
      )
    return self.results[column_index][row_index]

  def __len__(self):
    """Returns number of rows in the report."""
    return len(self.results)

  def __iter__(self) -> Generator[GaarfRow, None, None] | None:
    """Returns GaarfRow for each element in GaarfReport.

    If report contains results_placeholder return None immediately.

    Yields:
        GaarfRow for each sub-list in the report.

    """
    if self.results_placeholder:
      return None
    for result in self.results:
      yield GaarfRow(result, self.column_names)

  def __bool__(self):
    """Checks whether report results is not empty."""
    return bool(self.results)

  def __str__(self):
    return self.to_pandas().to_string()

  def __getitem__(
    self, key: str | int | slice | MutableSequence[str]
  ) -> GaarfReport | GaarfRow:
    """Gets data from report based on a key.

    Data can be extract from report by rows and columns.
    For single column extraction use `report['column_name']` syntax;
    for multiple columns extract use `report[['column_1', 'column_2']]`.
    For single row extraction use `report[0]` syntax;
    for multiple rows extraction use `report[0:2]` syntax.

    Args:
      key:
        element to get from report. Could be index, slice or column_name(s).

    Returns:
      New GaarfReport or GaarfRow.

    Raises:
        GaarfReportException: When incorrect column_name specified.
    """
    if isinstance(key, (MutableSequence, str)):
      return self._get_columns_slice(key)
    return self._get_rows_slice(key)

  def _get_rows_slice(
    self, key: slice | int
  ) -> GaarfReport | GaarfRow | parsers.GoogleAdsRowElement:
    """Gets one or several rows from the report.

    Args:
        key: Row(s) to get from report. Could be index or slice.

    Returns:
      New GaarfReport or GaarfRow.
    """
    if not self._multi_column_report and self.auto_convert_to_scalars:
      warnings.warn(
        'Getting scalars from single column `GaarfReport` is discouraged and '
        'will be deprecated in future releases of gaarf. To get scalar value '
        'use `get_value()` method instead. '
        'Call `.disable_scalar_conversions()` to return GaarfRow '
        'or GaarfReport.',
        category=FutureWarning,
        stacklevel=3,
      )
      if isinstance(key, slice):
        return [element[0] for element in self.results[key]]
      return self.results[key]
    if isinstance(key, slice):
      return GaarfReport(self.results[key], self.column_names)
    return GaarfRow(self.results[key], self.column_names)

  def _get_columns_slice(self, key: str | MutableSequence[str]) -> GaarfReport:
    """Gets one or several columns from the report.

    Args:
      key: Column(s) to get from the report.

    Returns:
      New GaarfReport or GaarfRow.

    Raises:
        GaarfReportException: When incorrect column_name specified.
    """
    if isinstance(key, str):
      key = [key]
    if set(key).issubset(set(self.column_names)):
      indices = []
      for k in key:
        indices.append(self.column_names.index(k))
      results = []
      for row in self.results:
        rows = []
        for index in indices:
          rows.append(row[index])
        results.append(rows)
      # TODO: propagate placeholders and query specification to new report
      return GaarfReport(results, key)
    non_existing_keys = set(key).intersection(set(self.column_names))
    if len(non_existing_keys) > 1:
      message = (
        f"Columns '{', '.join(list(non_existing_keys))}' "
        'cannot be found in the report'
      )
    message = (
      f"Column '{non_existing_keys.pop()}' " 'cannot be found in the report'
    )
    raise exceptions.GaarfReportException(message)

  def __eq__(self, other) -> bool:
    if not isinstance(other, self.__class__):
      return False
    if sorted(self.column_names) != sorted(other.column_names):
      return False
    if len(self.results) != len(other.results):
      return False
    for self_row, other_row in zip(self, other):
      if self_row.to_dict() != other_row.to_dict():
        return False
    return True

  def __add__(self, other: GaarfReport) -> GaarfReport:
    """Combines two reports into one.

    New report is build from two reports results variable; if either of reports
    has results placeholder it's copied into the new report.

    Args:
        other: Report to be added to existing report.

    Return:
        New GaarfReport with combined data.

    Raises:
        GaarfReportException:
            When columns are different or added instance is not GaarfReport.
    """
    if not isinstance(other, self.__class__):
      raise exceptions.GaarfReportException(
        'Add operation is supported only for GaarfReport'
      )
    if self.column_names != other.column_names:
      raise exceptions.GaarfReportException(
        'column_names should be the same in GaarfReport'
      )
    return GaarfReport(
      results=self.results + other.results,
      column_names=self.column_names,
      results_placeholder=self.results_placeholder
      and other.results_placeholder,
    )

  @classmethod
  def from_pandas(cls, df: 'pd.DataFrame') -> GaarfReport:
    """Builds GaarfReport from pandas dataframe.

    Args:
        df: Pandas dataframe to build report from.

    Returns:
        Report build from dataframe data and columns.

    Raises:
        ImportError: If pandas library not installed.
    """
    try:
      import pandas as pd
    except ImportError as e:
      raise ImportError(
        'Please install google-ads-api-report-fetcher with Pandas support '
        '- `pip install google-ads-api-report-fetcher[pandas]`'
      ) from e
    return cls(results=df.values.tolist(), column_names=list(df.columns.values))


class GaarfRow:
  """Helper class to simplify iteration of GaarfReport.

  Attributes:
      data: ...
      column_names: ...
  """

  def __init__(
    self, data: parsers.GoogleAdsRowElement, column_names: Sequence[str]
  ) -> None:
    """Initializes new GaarfRow.

    data: ...
    column_names: ...
    """
    super().__setattr__('data', data)
    super().__setattr__('column_names', column_names)

  def to_dict(self) -> dict[str, parsers.GoogleAdsRowElement]:
    """Maps column names to corresponding data point."""
    return {x[1]: x[0] for x in zip(self.data, self.column_names)}

  def get_value(self, column_index: int = 0) -> parsers.GoogleAdsRowElement:
    """Extracts data from row as a scalar.

    Raises:
      GaarfReportException: If column index is out of bounds.
    """
    if column_index >= len(self.column_names):
      raise exceptions.GaarfReportException(
        'Column %d of report is not found; report contains only %d columns.',
        column_index,
        len(self.column_names) + 1,
      )
    return self.data[column_index]

  def __getattr__(self, element: str) -> parsers.GoogleAdsRowElement:
    """Gets element from row as an attribute.

    Args:
        element: Name of an attribute.

    Returns:
        Found element.

    Raises:
        AttributeError: If attribute is not in column_names.
    """
    if element in self.column_names:
      return self.data[self.column_names.index(element)]
    raise AttributeError(f'cannot find {element} element!')

  def __getitem__(self, element: str | int) -> parsers.GoogleAdsRowElement:
    """Gets element from row by index.

    Args:
        element: index of value.

    Returns:
        Found element.

    Raises:
        GaarfReportException: If element not found in the position.
    """
    if isinstance(element, int):
      if element < len(self.column_names):
        return self.data[element]
      raise exceptions.GaarfReportException(
        f'cannot find data in position {element}!'
      )
    if isinstance(element, str):
      return self.__getattr__(element)
    raise exceptions.GaarfReportException(f'cannot find {element} element!')

  def __setattr__(self, name: str, value: parsers.GoogleAdsRowElement) -> None:
    """Sets new value for an attribute.

    Args:
        name: Attribute name.
        value: New values of an attribute.
    """
    self.__setitem__(name, value)

  def __setitem__(self, name: str, value: str | int) -> None:
    """Sets new value by index.

    Args:
        name: Column name.
        value: New values of an element.
    """
    if name in self.column_names:
      if len(self.column_names) == len(self.data):
        self.data[self.column_names.index(name)] = value
      else:
        self.data.append(value)
    else:
      self.data.append(value)
      self.column_names.append(name)

  def get(self, item: str) -> parsers.GoogleAdsRowElement:
    """Extracts value as dictionary get operation.

    Args:
        item: Column name of a value to be extracted from the row.

    Returns:
        Found value.
    """
    return self.__getattr__(item)

  def __iter__(self) -> parsers.GoogleAdsRowElement:
    """Yields each element of a row."""
    for field in self.data:
      yield field

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    if self.column_names != other.column_names:
      return False
    return self.data == other.data

  def __repr__(self):
    return f'GaarfRow(\n{self.to_dict()}\n)'
