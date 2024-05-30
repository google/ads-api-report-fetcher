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
"""Module for defining various parsing strategy for GoogleAdsRow elements.

GoogleAdsRowParser parses a single GoogleAdsRow and applies different parsing
strategies to each element of the row.
"""

from __future__ import annotations

import ast
import importlib
import operator
import re
from collections import abc
from typing import Union, get_args

import proto  # type: ignore
from google import protobuf
from google.ads.googleads import util as googleads_utils
from proto.marshal.collections import repeated
from typing_extensions import Self, TypeAlias

from gaarf import api_clients, exceptions, query_editor

google_ads_service = importlib.import_module(
  f'google.ads.googleads.{api_clients.GOOGLE_ADS_API_VERSION}.'
  'services.types.google_ads_service'
)

GoogleAdsRowElement: TypeAlias = Union[int, float, str, bool, list, None]

_REPEATED: TypeAlias = Union[
  repeated.Repeated,
  protobuf.internal.containers.RepeatedScalarFieldContainer,
]
_REPEATED_COMPOSITE: TypeAlias = Union[
  repeated.RepeatedComposite,
  protobuf.internal.containers.RepeatedCompositeFieldContainer,
]

_NESTED_FIELD: TypeAlias = Union[
  _REPEATED,
  _REPEATED_COMPOSITE,
]


class BaseParser:
  """Base class for defining parsers.

  Attributes:
      _successor: Indicates the previous parser in the chain.
  """

  def __init__(self, successor: type[Self]) -> None:
    self._successor = successor

  def parse(self, element: GoogleAdsRowElement) -> GoogleAdsRowElement:
    """Parses GoogleAdsRow by using a successor parser.

    Args:
        element: An element of a GoogleAdsRow.
    Returns:
        Parsed GoogleAdsRow element.
    """
    if self._successor:
      return self._successor.parse(element)
    return None


class RepeatedParser(BaseParser):
  """Parses repeated.Repeated resources."""

  def parse(self, element: GoogleAdsRowElement) -> GoogleAdsRowElement:
    """Parses only repeated elements from GoogleAdsRow.

    If there a repeated resource, applies transformations to each element;
    otherwise delegates parsing of the element to the next parser
    in the chain.

    Args:
        element: An element of a GoogleAdsRow.
    Returns:
        Parsed GoogleAdsRow element.
    """
    if isinstance(element, get_args(_REPEATED)) and 'customer' in str(element):
      items: list[GoogleAdsRowElement] = []
      for item in element:
        item = ResourceFormatter.get_resource_id(item)
        items.append(ResourceFormatter.clean_resource_id(item))
      return items
    return super().parse(element)


class RepeatedCompositeParser(BaseParser):
  """Parses repeated.RepeatedComposite elements."""

  def parse(self, element):
    """Parses only repeated composite resources from GoogleAdsRow.

    If there a repeated composited resource, applies transformations
    to each element; otherwise delegates parsing of the element
    to the next parser in the chain.

    Args:
        element: An element of a GoogleAdsRow.
    Returns:
        Parsed GoogleAdsRow element.
    """
    if isinstance(element, get_args(_REPEATED_COMPOSITE)):
      items = []
      for item in element:
        item = ResourceFormatter.get_resource(item)
        item = ResourceFormatter.get_resource_id(item)
        items.append(ResourceFormatter.clean_resource_id(item))
      return items
    return super().parse(element)


class AttributeParser(BaseParser):
  """Parses elements that have attributes."""

  def parse(self, element: GoogleAdsRowElement) -> GoogleAdsRowElement:
    """Parses only elements that have attributes.

    If there a repeated composited resource, applies transformations
    to each element; otherwise delegates parsing of the element
    to the next parser in the chain.

    Args:
        element: An element of a GoogleAdsRow.
    Returns:
        Parsed GoogleAdsRow element.
    """
    if hasattr(element, 'name'):
      return element.name
    if hasattr(element, 'text'):
      return element.text
    if hasattr(element, 'asset'):
      return element.asset
    if hasattr(element, 'value'):
      return element.value
    return super().parse(element)


class EmptyMessageParser(BaseParser):
  """Generates placeholder for empty Message objects."""

  def parse(self, element: GoogleAdsRowElement) -> GoogleAdsRowElement:
    """Checks if an element is an empty proto.Message.

    If an element is empty message, returns 'Not set' placeholder;
    otherwise delegates parsing of the element to the next parser
    in the chain.

    Args:
        element: An element of a GoogleAdsRow.
    Returns:
        Parsed GoogleAdsRow element.
    """
    if issubclass(type(element), proto.Message):
      return 'Not set'
    return super().parse(element)


class GoogleAdsRowParser:
  """Performs parsing of a single GoogleAdsRow.

  Attributes:
      fields: Expected fields in GoogleAdsRow.
      customizers: Customizing behaviour performed on a field.
      virtual_columns: Elements that are not directly present in GoogleAdsRow.
      parser: Chain of parsers to execute on a single GoogleAdsRow.
      row_getter: Helper to easily extract fields from GogleAdsRow.
      respect_nulls: Whether or not convert nulls to zeros.
  """

  def __init__(self, query_specification: query_editor.QueryElements) -> None:
    """Initializes GoogleAdsRowParser.

    Args:
        query_specification: All elements forming gaarf query.
    """
    self.fields = query_specification.fields
    self.customizers = query_specification.customizers
    self.virtual_columns = query_specification.virtual_columns
    self.column_names = query_specification.column_names
    self.parser_chain = self._init_parsers_chain()
    self.row_getter = operator.attrgetter(*query_specification.fields)
    # Some segments are automatically converted to 0 when not present
    # For this case we specify attribute `respect_null` which converts
    # such attributes to None rather than 0
    self.respect_nulls = (
      'segments.sk_ad_network_conversion_value' in self.fields
    )

  def _init_parsers_chain(self):
    """Initializes chain of parsers."""
    parser_chain = BaseParser(None)
    for parser in [
      EmptyMessageParser,
      AttributeParser,
      RepeatedCompositeParser,
      RepeatedParser,
    ]:
      new_parser = parser(parser_chain)
      parser_chain = new_parser
    return parser_chain

  def parse_ads_row(
    self, row: google_ads_service.GoogleAdsRow
  ) -> list[GoogleAdsRowElement]:
    """Parses GoogleAdsRow by applying various transformations.

    Args:
        row: A single GoogleAdsRow.

    Returns:
        List of parsed elements.
    """
    parsed_row_elements: list[GoogleAdsRowElement] = []
    extracted_attributes = self._get_attributes_from_row(row, self.row_getter)
    index = 0
    for column in self.column_names:
      if column in self.virtual_columns.keys():
        parsed_element = self._convert_virtual_column(
          row, self.virtual_columns[column]
        )
      else:
        parsed_element = self._parse_row_element(
          extracted_attributes[index], column
        )
        index += 1
      parsed_row_elements.append(parsed_element)
    return parsed_row_elements

  def _parse_row_element(
    self, extracted_attribute: GoogleAdsRowElement, column: str
  ) -> GoogleAdsRowElement:
    """Parses a single element from row.

    Args:
        extracted_attribute: A single element from GoogleAdsRow.
        column: Corresponding name of the element.

    Returns:
        Parsed element.
    """
    if self.customizers:
      extracted_attribute = self._extract_attributes_with_customizer(
        extracted_attribute, column
      )
    if isinstance(extracted_attribute, abc.MutableSequence):
      parsed_element = [
        self.parser_chain.parse(element) or element
        for element in extracted_attribute
      ]
    else:
      parsed_element = (
        self.parser_chain.parse(extracted_attribute) or extracted_attribute
      )

    return parsed_element

  def _extract_attributes_with_customizer(
    self, extracted_attribute: GoogleAdsRowElement, column: str
  ) -> GoogleAdsRowElement:
    """Extracts additional info from row element based on customizers.

    Some GoogleAdsRow objects can be complex and customizers help extract
    specific values from them by using special syntax.

    Args:
        extracted_attribute: A single element from GoogleAdsRow.
        column: Corresponding name of the element.

    Returns:
        Extracted row attribute.
    """
    if caller := self.customizers.get(column):
      if caller.get('type') == 'nested_field':
        extracted_attribute = self._extract_nested_customizer(
          extracted_attribute, caller
        )
      elif caller.get('type') == 'resource_index':
        extracted_attribute = self._get_resource_index(
          extracted_attribute, caller
        )
    return extracted_attribute

  def _extract_nested_customizer(
    self, extracted_attribute, caller: dict[str, str]
  ) -> GoogleAdsRowElement:
    """Extracts additional info from nested resource.

    Some GoogleAdsRow objects are nested and has attributes that can be
    further accessed using special customizer syntax.

    Args:
        extracted_attribute: A single element from GoogleAdsRow.
        caller: Mapping between type of customizer type and its value.

    Returns:
        Extracted row attribute.

    Raises:
        GaarfCustomizerException: When customizer incorrectly specified.
    """
    values_ = caller.get('value').split('.')
    extracted_attribute_ = (
      getattr(extracted_attribute, values_[0])
      if hasattr(extracted_attribute, values_[0])
      else extracted_attribute
    )
    try:
      if isinstance(extracted_attribute, get_args(_NESTED_FIELD)) or isinstance(
        extracted_attribute_, get_args(_NESTED_FIELD)
      ):
        if isinstance(
          extracted_attribute_, (repeated.Repeated, repeated.RepeatedComposite)
        ):
          extracted_attribute = extracted_attribute
        if len(values_) > 1:
          value = values_[1]
        else:
          value = caller.get('value')
        extracted_attribute = list(
          {
            operator.attrgetter(value)(element)
            for element in extracted_attribute_
          }
        )
      else:
        extracted_attribute = operator.attrgetter(caller.get('value'))(
          extracted_attribute
        )
      return extracted_attribute
    except AttributeError as e:
      raise exceptions.GaarfCustomizerException(
        f'customizer "{caller}" is incorrect,\n' f'details: "{e}"'
      )

  def _get_resource_index(
    self, extracted_attribute: GoogleAdsRowElement, caller: dict[str, str]
  ) -> str:
    """Extracts additional info from resource_name.

    Some GoogleAdsRow objects resource_names
    (i.e. customers/1/conversionActions/2~3); with resource_index we can
    access only the last element of this expression ('3').

    Args:
        extracted_attribute: A single element from GoogleAdsRow.
        caller: Mapping between type of customizer type and its value.

    Returns:
        Extracted row attribute.
    """
    if isinstance(extracted_attribute, abc.MutableSequence):
      parsed_element = [
        self.parser_chain.parse(element) or element
        for element in extracted_attribute
      ]
      return [
        self._get_resource_index(attribute, caller)
        for attribute in parsed_element
      ]
    extracted_attribute = re.split('~', extracted_attribute)[
      caller.get('value')
    ]
    return re.split('/', extracted_attribute)[-1]

  def _get_attributes_from_row(
    self, row: google_ads_service.GoogleAdsRow, getter: operator.attrgetter
  ) -> tuple[GoogleAdsRowElement, ...]:
    """Extracts attributes from GoogleAdsRow based on attribute getter.

    Attribute getter contains nested attribute access operations (i.e.
    campaign -> id) which can be easily executed on nested GoogleAdsRow
    object.

    Args:
        row: A single GoogleAdsRow.
        getter: Initialized attribute getter.

    Returns:
        All parsed elements from a single GoogleAdsRow..
    """
    attributes = getter(row)
    if self.respect_nulls:
      row = googleads_utils.convert_proto_plus_to_protobuf(row)
      if row.segments.HasField('sk_ad_network_conversion_value'):
        # Convert to list to perform modification
        attributes = list(attributes)
        # Replace 0 attributes in the row with None
        attributes[
          self.fields.index('segments.sk_ad_network_conversion_value')
        ] = None
        # Convert back to tuple
        attributes = tuple(attributes)
    else:
      attributes = getter(row)
    return attributes if isinstance(attributes, tuple) else (attributes,)

  def _convert_virtual_column(
    self,
    row: google_ads_service.GoogleAdsRow,
    virtual_column: query_editor.VirtualColumn,
  ) -> GoogleAdsRowElement:
    """Convert virtual column definition to a single element.

    Args:
        row: A single GoogleAdsRow.
        virtual_column: Virtual column definition.

    Returns:
        Parsed element.
    """
    if virtual_column.type not in ('built-in', 'expression'):
      raise exceptions.GaarfVirtualColumnException(
        f'Unsupported virtual column type: {virtual_column.type}'
      )
    if virtual_column.type == 'built-in':
      return virtual_column.value
    if virtual_column.type == 'expression':
      virtual_column_getter = operator.attrgetter(*virtual_column.fields)
      virtual_column_values = virtual_column_getter(row)
      try:
        iter(virtual_column_values)
      except TypeError:
        virtual_column_values = (virtual_column_values,)
      virtual_column_replacements = {
        field.replace('.', '_'): value
        for field, value in zip(virtual_column.fields, virtual_column_values)
      }
      try:
        virtual_column_expression = virtual_column.substitute_expression.format(
          **virtual_column_replacements
        )
        tree = ast.parse(virtual_column_expression, mode='eval')
        valid = all(
          isinstance(node, query_editor.VALID_VIRTUAL_COLUMN_OPERATORS)
          for node in ast.walk(tree)
        )
        if valid:
          result = eval(
            compile(tree, filename='', mode='eval'), {'__builtins__': None}
          )
      except TypeError as e:
        raise exceptions.GaarfVirtualColumnException(
          f'cannot parse virtual_column {virtual_column.value}'
        ) from e
      except ZeroDivisionError:
        return 0
      except SyntaxError:
        return virtual_column.value
      return result


class ResourceFormatter:
  """Helper class for formatting resources strings."""

  @staticmethod
  def get_resource(element: str) -> str:
    return re.split(': ', str(element).strip())[1]

  @staticmethod
  def get_resource_id(element: str) -> str:
    return re.split('/', str(element))[-1]

  @staticmethod
  def clean_resource_id(element: str) -> int | str:
    element = re.sub('"', '', str(element))
    try:
      return int(element)
    except ValueError:
      return element
