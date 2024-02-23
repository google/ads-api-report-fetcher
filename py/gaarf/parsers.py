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

import re
from collections import abc
from operator import attrgetter
from typing import Self
from typing import TypeAlias

import proto  # type: ignore
from gaarf.query_editor import QueryElements
from gaarf.query_editor import VirtualColumn
from gaarf.query_editor import VirtualColumnError
from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from google.protobuf.internal.containers import RepeatedScalarFieldContainer
from proto.marshal.collections.repeated import Repeated
from proto.marshal.collections.repeated import RepeatedComposite

GoogleAdsRowElement: TypeAlias = int | float | str | bool | list | None


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
    """Parses Repeated resources."""

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
        if isinstance(
                element,
            (Repeated,
             RepeatedScalarFieldContainer)) and 'customer' in str(element):
            items: list[GoogleAdsRowElement] = []
            for item in element:
                item = ResourceFormatter.get_resource_id(item)
                items.append(ResourceFormatter.clean_resource_id(item))
            return items
        return super().parse(element)


class RepeatedCompositeParser(BaseParser):
    """Parses RepeatedComposite elements."""

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
        if isinstance(element,
                      (RepeatedComposite, RepeatedCompositeFieldContainer)):
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

    def __init__(self, query_specification: QueryElements) -> None:
        """Initializes GoogleAdsRowParser.

        Args:
            query_specification: All elements forming gaarf query.
        """
        self.fields = query_specification.fields
        self.customizers = query_specification.customizers
        self.virtual_columns = query_specification.virtual_columns
        self.column_names = query_specification.column_names
        self.parser_chain = self._init_parsers_chain()
        self.row_getter = attrgetter(*query_specification.fields)
        # Some segments are automatically converted to 0 when not present
        # For this case we specify attribute `respect_null` which converts
        # such attributes to None rather than 0
        self.respect_nulls = ('segments.sk_ad_network_conversion_value'
                              in self.fields)

    def _init_parsers_chain(self):
        """Initializes chain of parsers."""
        parser_chain = BaseParser(None)
        for parser in [
                EmptyMessageParser, AttributeParser, RepeatedCompositeParser,
                RepeatedParser
        ]:
            new_parser = parser(parser_chain)
            parser_chain = new_parser
        return parser_chain

    def parse_ads_row(self, row: 'GoogleAdsRow') -> list[GoogleAdsRowElement]:
        """Parses GoogleAdsRow by applying various transformations.

        Args:
            row: A single GoogleAdsRow.
        Returns:
            List of parsed elements.
        """
        parsed_row_elements: list[GoogleAdsRowElement] = []
        extracted_attributes = self._get_attributes_from_row(
            row, self.row_getter)
        index = 0
        for i, column in enumerate(self.column_names):
            if column in self.virtual_columns.keys():
                parsed_element = self._convert_virtual_column(
                    row, self.virtual_columns[column])
            else:
                extracted_attribute = extracted_attributes[index]
                index += 1
                if self.customizers:
                    if (caller := self.customizers.get(column)):
                        if caller.get('type') == 'nested_field':
                            values_ = caller.get('value').split('.')
                            extracted_attribute_ = getattr(
                                extracted_attribute, values_[0]) if hasattr(
                                    extracted_attribute,
                                    values_[0]) else extracted_attribute
                            try:
                                if isinstance(
                                        extracted_attribute,
                                    (Repeated, RepeatedComposite,
                                     RepeatedScalarFieldContainer,
                                     RepeatedCompositeFieldContainer
                                     )) or isinstance(
                                         extracted_attribute_,
                                         (Repeated, RepeatedComposite,
                                          RepeatedScalarFieldContainer,
                                          RepeatedCompositeFieldContainer)):
                                    if isinstance(
                                            extracted_attribute_,
                                        (Repeated, RepeatedComposite)):
                                        extracted_attribute = extracted_attribute_
                                    if len(values_) > 1:
                                        value = values_[1]
                                    else:
                                        value = caller.get('value')
                                    extracted_attribute = list({
                                        attrgetter(value)(element)
                                        for element in extracted_attribute
                                    })
                                else:
                                    extracted_attribute = attrgetter(
                                        caller.get('value'))(
                                            extracted_attribute)
                            except AttributeError as e:
                                raise ValueError(
                                    f'customizer {caller} is incorrect,\n'
                                    f"details: '{e}',\n"
                                    f"row: '{row}'") from e
                        elif caller.get('type') == 'resource_index':
                            if isinstance(extracted_attribute,
                                          abc.MutableSequence):
                                parsed_element = [
                                    self.parser_chain.parse(element) or element
                                    for element in extracted_attribute
                                ]
                                extracted_attribute = [
                                    self._get_resource_index(
                                        attribute, caller)
                                    for attribute in parsed_element
                                ]
                            else:
                                extracted_attribute = self._get_resource_index(
                                    extracted_attribute, caller)
                if isinstance(extracted_attribute, abc.MutableSequence):
                    parsed_element = [
                        self.parser_chain.parse(element) or element
                        for element in extracted_attribute
                    ]
                else:
                    parsed_element = self.parser_chain.parse(
                        extracted_attribute) or extracted_attribute
            parsed_row_elements.append(parsed_element)
        return parsed_row_elements

    def _get_resource_index(self, extracted_attribute: str
                            | abc.MutableSequence[str], caller: dict) -> str:
        extracted_attribute = re.split(
            '~', extracted_attribute)[caller.get('value')]
        return re.split('/', extracted_attribute)[-1]

    def _get_attributes_from_row(self, row: 'GoogleAdsRow', getter) -> tuple:
        attributes = getter(row)
        if self.respect_nulls:
            # Validate whether field is actually present in a protobuf message
            value = row.segments._pb.HasField('sk_ad_network_conversion_value')
            # If not present
            if not value:
                # Convert to list to perform modification
                attributes = list(attributes)
                # Replace 0 attributes in the row with None
                attributes[self.fields.index(
                    'segments.sk_ad_network_conversion_value')] = None
                # Convert back to tuple
                attributes = tuple(attributes)
        else:
            attributes = getter(row)
        return attributes if isinstance(attributes, tuple) else (attributes, )

    def _convert_virtual_column(
            self, row: 'GoogleAdsRow',
            virtual_column: VirtualColumn) -> GoogleAdsRowElement:
        """Convert virtual column definition to a single element.

        Args:
            row: A single GoogleAdsRow.
            virtual_column: Virtual column definition.
        Returns:
            Parsed element.
        """
        if virtual_column.type not in ('built-in', 'expression'):
            raise ValueError(
                f'Unsupported virtual column type: {virtual_column.type}')
        if virtual_column.type == 'built-in':
            return virtual_column.value
        if virtual_column.type == 'expression':
            virtual_column_getter = attrgetter(*virtual_column.fields)
            virtual_column_values = virtual_column_getter(row)
            try:
                iter(virtual_column_values)
            except TypeError:
                virtual_column_values = (virtual_column_values, )
            virtual_column_replacements = {
                field.replace('.', '_'): value
                for field, value in zip(virtual_column.fields,
                                        virtual_column_values)
            }
            try:
                result = eval(
                    virtual_column.substitute_expression.format(
                        **virtual_column_replacements))
            except TypeError as e:
                raise VirtualColumnError(
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
