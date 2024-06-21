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
"""Module defining key query elements for building and parsing GAQL query."""

from __future__ import annotations

import ast
import contextlib
import dataclasses
import datetime
import operator
import re
from typing import Generator

from dateutil import relativedelta

from gaarf import api_clients, exceptions, query_post_processor

VALID_VIRTUAL_COLUMN_OPERATORS = (
  ast.BinOp,
  ast.UnaryOp,
  ast.operator,
  ast.Num,
  ast.Expression,
)


@dataclasses.dataclass(frozen=True)
class VirtualColumn:
  """Represents element in Gaarf query that either calculated or plugged-in.

  Virtual columns allow performing basic manipulation with metrics and
  dimensions (i.e. division or multiplication) as well as adding raw text
  values directly into report.

  Attributes:
    type: Type of virtual column, either build-in or expression.
    value: Value of the field after macro expansion.
    fields: Possible fields participating in calculations.
    substitute_expression: Formatted expression.
  """

  type: str
  value: str
  fields: list[str] | None = None
  substitute_expression: str | None = None


@dataclasses.dataclass
class ExtractedLineElements:
  """Helper class for parsing query lines.

  Attributes:
    fields: All fields extracted from the line.
    alias: Optional alias assign to a field.
    virtual_column: Optional virtual column extracted from query line.
    customizer: Optional values for customizers associated with a field.
  """

  field: str | None
  alias: str | None
  virtual_column: VirtualColumn | None
  customizer: dict[str, str | int]


@dataclasses.dataclass
class ProcessedField:
  """Helper class to store fields with its customizers.

  Attributes:
    field: Extractable field.
    customizer_type: Type of customizer to be applied to the field.
    customizer_value: Value to be used in customizer.
  """

  field: str
  customizer_type: str | None = None
  customizer_value: int | str | None = None


@dataclasses.dataclass
class QueryElements:
  """Contains raw query and parsed elements.

  Attributes:
      query_title: Title of the query that needs to be parsed.
      query_text: Text of the query that needs to be parsed.
      resource_name: Name of Google Ads API reporting resource.
      fields: Ads API fields that need to be fetched.
      column_names: Friendly names for fields which are used when saving data
      customizers: Attributes of fields that need to be be extracted.
      virtual_columns: Attributes of fields that need to be be calculated.
      is_constant_resource: Whether resource considered a constant one.
      is_builtin_query: Whether query is built-in.
  """

  query_title: str
  query_text: str
  resource_name: str
  fields: list[str] | None = None
  column_names: list[str] | None = None
  customizers: dict[str, dict[str, str]] | None = None
  virtual_columns: dict[str, VirtualColumn] | None = None
  is_constant_resource: bool = False
  is_builtin_query: bool = False


class CommonParametersMixin:
  """Helper mixin to inject set of common parameters to all queries."""

  _common_params = {
    'date_iso': lambda: datetime.date.today().strftime('%Y%m%d'),
    'yesterday_iso': lambda: (
      datetime.date.today() - relativedelta.relativedelta(days=1)
    ).strftime('%Y%m%d'),
    'current_date': lambda: datetime.date.today().strftime('%Y-%m-%d'),
    'current_datetime': lambda: datetime.datetime.today().strftime(
      '%Y-%m-%d %H:%M:%S'
    ),
  }

  @property
  def common_params(self):
    """Instantiates common parameters to the current values."""
    return {key: value() for key, value in self._common_params.items()}


class QuerySpecification(
  CommonParametersMixin, query_post_processor.PostProcessorMixin
):
  """Simplifies fetching data from API and it's further parsing..

  Attributes:
    text: Query text.
    title: Query title.
    args: Optional parameters to be dynamically injected into query text.
    api_version: Version of Google Ads API.
  """

  def __init__(
    self,
    text: str,
    title: str | None = None,
    args: dict | None = None,
    api_version: str = api_clients.GOOGLE_ADS_API_VERSION,
  ) -> None:
    """Instantiates QuerySpecification based on text, title and optional args.

    Args:
      text: Query text.
      title: Query title.
      args: Optional parameters to be dynamically injected into query text.
      api_version: Version of Google Ads API.
    """
    self.text = text
    self.title = title
    self.args = args or {}
    self._api_version = api_version

  @property
  def base_client(self):
    """Helper for validating identified query fields."""
    return api_clients.BaseClient(self._api_version)

  @property
  def macros(self) -> dict[str, str]:
    """Returns macros with injected common parameters."""
    common_params = dict(self.common_params)
    if macros := self.args.get('macro'):
      common_params.update(macros)
    return common_params

  @property
  def expanded_query(self) -> str:
    """Applies necessary transformations to query."""
    query_text = self.expand_jinja(self.text, self.args.get('template'))
    query_lines = self._remove_comments_from_query(query_text)
    query_text = ' '.join(query_lines)
    try:
      return query_text.format(**self.macros).strip()
    except KeyError as e:
      raise exceptions.GaarfMacroException(
        f'No value provided for macro {str(e)}.'
      ) from e

  def generate(self) -> QueryElements:
    """Generates necessary query elements based on query text and arguments.

    Returns:
        Various elements parsed from a query (text, fields,
        column_names, etc).

    Raises:
        GaarfResourceException: If query contains invalid resource_name.
        GaarfMacroException: If missing values for one of the query macros.
    """
    resource_name = self._extract_resource_from_query()
    if is_builtin_query := bool(resource_name.startswith('builtin')):
      return QueryElements(
        query_title=resource_name.replace('builtin.', ''),
        query_text=self.expanded_query,
        fields=None,
        column_names=None,
        customizers=None,
        virtual_columns=None,
        resource_name=resource_name,
        is_constant_resource=False,
        is_builtin_query=True,
      )
    if not is_builtin_query and resource_name not in dir(
      self.base_client.google_ads_row
    ):
      raise exceptions.GaarfResourceException(
        f'Invalid resource specified in the query: {resource_name}'
      )
    is_constant_resource = bool(resource_name.endswith('_constant'))
    fields = []
    column_names = []
    customizers = {}
    virtual_columns = {}

    for line in self._extract_query_lines():
      line_elements = self._extract_line_elements(line)
      column_name = line_elements.alias
      if field := line_elements.field:
        fields.append(field)
      column_names.append(column_name)

      if virtual_column := line_elements.virtual_column:
        virtual_columns[column_name] = virtual_column
      if customizer := line_elements.customizer:
        customizers[column_name] = customizer
    return QueryElements(
      query_title=self.title,
      query_text=self._create_gaql_query(fields, virtual_columns),
      fields=fields,
      column_names=column_names,
      customizers=customizers,
      virtual_columns=virtual_columns,
      resource_name=resource_name,
      is_constant_resource=is_constant_resource,
      is_builtin_query=is_builtin_query,
    )

  def _create_gaql_query(
    self,
    fields: list[str],
    virtual_columns: dict[str, VirtualColumn],
  ) -> str:
    """Generate valid GAQL query.

    Based on original Gaarf query text, a set of field and virtual columns
    constructs new GAQL query to be sent to Ads API.

    Args:
        fields:
            All fields that need to be fetched from API.
        virtual_columns:
            Virtual columns that might contain extra fields for fetching.

    Returns:
        Valid GAQL query.
    """
    virtual_fields = [
      field
      for name, column in virtual_columns.items()
      if column.type == 'expression'
      for field in column.fields
    ]
    if virtual_fields:
      fields = fields + virtual_fields
    query_text = (
      f'SELECT {", ".join(fields)} '
      f'FROM {self._extract_resource_from_query()} '
      f'{self._extract_filters()}'
    )
    query_text = self._remove_trailing_comma(query_text)
    query_text = self._unformat_type_field_name(query_text)
    return re.sub(r'\s+', ' ', query_text).strip()

  def _remove_comments_from_query(self, query_text: str) -> list[str]:
    """Removes comments and converts text to lines."""
    result: list[str] = []
    for line in query_text.split('\n'):
      if re.match('^(#|--|//)', line):
        continue
      cleaned_query_line = re.sub(
        ';$', '', re.sub('(--|//).*$', '', line).strip()
      )
      result.append(cleaned_query_line)
    return result

  def _extract_resource_from_query(self) -> str:
    """Finds resource_name in query_text.

    Returns:
      Found resource.

    Raises:
      GaarfResourceException: If resource_name isn't found.
    """
    if resource_name := re.findall(
      r'FROM\s+([\w.]+)', self.expanded_query, flags=re.IGNORECASE
    ):
      return str(resource_name[0]).strip()
    raise exceptions.GaarfResourceException(
      f'No resource found in query: {self.expanded_query}'
    )

  def _extract_query_lines(self) -> Generator[str, None, None]:
    """Helper for extracting fields with aliases from query text.

    Yields:
      Line in query between SELECT and FROM statements.
    """
    selected_rows = re.sub(
      r'\bSELECT\b|FROM .*', '', self.expanded_query, flags=re.IGNORECASE
    ).split(',')
    for row in selected_rows:
      if non_empty_row := row.strip():
        yield non_empty_row

  def _extract_filters(self) -> str:
    if where_statement := re.search(
      ' (WHERE|LIMIT|ORDER BY|PARAMETERS) .+',
      self.expanded_query,
      re.IGNORECASE,
    ):
      return where_statement.group(0)
    return ''

  def _extract_line_elements(self, query_line: str) -> ExtractedLineElements:
    """Parses query line into elements.

    Args:
      query_line: Field name with optional alias.

    Returns:
      Parsed elements (field, alias, virtual_column).
    """
    field, *alias = re.split(' [Aa][Ss] ', query_line)
    processed_field = self._process_field(field)
    field = processed_field.field
    if self._is_valid_google_ads_field(field):
      virtual_column = None
    else:
      virtual_column = self._convert_to_virtual_column(field)
    if alias and processed_field.customizer_type:
      customizer = {
        'type': processed_field.customizer_type,
        'value': processed_field.customizer_value,
      }
    else:
      customizer = {}
    if virtual_column and not alias:
      raise exceptions.GaarfVirtualColumnException(
        'Virtual attributes should be aliased'
      )
    return ExtractedLineElements(
      field=self._format_type_field_name(field)
      if not virtual_column and field
      else None,
      alias=self._normalize_column_name(alias[0] if alias else field),
      virtual_column=virtual_column,
      customizer=customizer,
    )

  def _process_field(self, raw_field: str) -> ProcessedField:
    """Process field to extract possible customizers.

    Args:
        raw_field: Unformatted field string value.

    Returns:
        ProcessedField that contains formatted field with customizers.
    """
    raw_field = raw_field.replace(r'\s+', '').strip()
    if self._is_quoted_string(raw_field):
      return ProcessedField(field=raw_field)
    if len(resources := self._extract_resource_element(raw_field)) > 1:
      field_name, resource_index = resources
      return ProcessedField(
        field=field_name,
        customizer_type='resource_index',
        customizer_value=int(resource_index),
      )

    if len(nested_fields := self._extract_nested_resource(raw_field)) > 1:
      field_name, nested_field = nested_fields
      return ProcessedField(
        field=field_name,
        customizer_type='nested_field',
        customizer_value=nested_field,
      )
    if len(pointers := self._extract_pointer(raw_field)) > 1:
      field_name, pointer = pointers
      return ProcessedField(
        field=field_name, customizer_type='pointer', customizer_value=pointer
      )
    return ProcessedField(field=raw_field)

  def _convert_to_virtual_column(self, field: str) -> VirtualColumn:
    """Converts a field to virtual column."""
    if field.isdigit():
      field = int(field)
    else:
      with contextlib.suppress(ValueError):
        field = float(field)
    if isinstance(field, (int, float)):
      return VirtualColumn(type='built-in', value=field)

    operators = ('/', r'\*', r'\+', ' - ')
    if len(expressions := re.split('|'.join(operators), field)) > 1:
      virtual_column_fields = []
      substitute_expression = field
      for expression in expressions:
        element = expression.strip()
        if self._is_valid_google_ads_field(element):
          virtual_column_fields.append(element)
          substitute_expression = substitute_expression.replace(
            element, f'{{{element}}}'
          )
      return VirtualColumn(
        type='expression',
        value=field.format(**self.macros) if self.macros else field,
        fields=virtual_column_fields,
        substitute_expression=substitute_expression.replace('.', '_'),
      )
    if not self._is_quoted_string(field):
      raise exceptions.GaarfFieldException(
        f"Incorrect field '{field}' in the query '{self.text}'."
      )
    field = field.replace("'", '').replace('"', '')
    field = field.format(**self.macros) if self.macros else field
    return VirtualColumn(type='built-in', value=field)

  def _is_valid_google_ads_field(self, field: str) -> bool:
    """Checks whether field is is a valid Google Ads field."""
    try:
      operator.attrgetter(field)(self.base_client.google_ads_row)
      return True
    except AttributeError:
      return False

  def _extract_resource_element(self, line_elements: str) -> list[str]:
    return re.split('~', line_elements)

  def _extract_pointer(self, line_elements: str) -> list[str]:
    return re.split('->', line_elements)

  def _extract_nested_resource(self, line_elements: str) -> list[str]:
    return re.split(':', line_elements)

  def _format_type_field_name(self, field_name: str) -> str:
    return re.sub(r'\.type', '.type_', field_name)

  def _normalize_column_name(self, column_name: str) -> str:
    return re.sub(r'\.', '_', column_name)

  def _remove_trailing_comma(self, query: str) -> str:
    return re.sub(r',\s+from', ' FROM', query, re.IGNORECASE)

  def _unformat_type_field_name(self, query: str) -> str:
    return re.sub(r'\.type_', '.type', query)

  def _is_quoted_string(self, field_name: str) -> bool:
    if (field_name.startswith("'") and field_name.endswith("'")) or (
      field_name.startswith('"') and field_name.endswith('"')
    ):
      return True
    return False
