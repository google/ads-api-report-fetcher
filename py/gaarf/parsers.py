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

from collections import abc
from typing import Any, Dict, Tuple, Sequence, Union
from operator import attrgetter
import datetime
import re
import proto  # type: ignore
from proto.marshal.collections.repeated import (  # type: ignore
    Repeated, RepeatedComposite)  # type: ignore
from google.protobuf.internal.containers import (
    RepeatedScalarFieldContainer,
    RepeatedCompositeFieldContainer)  # type: ignore

from .query_editor import VirtualAttribute, VirtualAttributeError


class BaseParser:

    def __init__(self, successor):
        self._successor = successor

    def parse(self, request):
        if self._successor:
            return self._successor.parse(request)
        return None


class RepeatedParser(BaseParser):

    def parse(self, request):
        if isinstance(
                request,
            (Repeated,
             RepeatedScalarFieldContainer)) and "customer" in str(request):
            elements = []
            for request_element in request:
                element = ResourceFormatter.get_resource_id(request_element)
                elements.append(ResourceFormatter.clean_resource_id(element))
            return elements
        return super().parse(request)


class RepeatedCompositeParser(BaseParser):

    def parse(self, request):
        if isinstance(request,
                      (RepeatedComposite, RepeatedCompositeFieldContainer)):
            elements = []
            for request_element in request:
                resource = ResourceFormatter.get_resource(request_element)
                element = ResourceFormatter.get_resource_id(resource)
                elements.append(ResourceFormatter.clean_resource_id(element))
            return elements
        return super().parse(request)


class AttributeParser(BaseParser):

    def parse(self, request):
        if hasattr(request, "name"):
            return request.name
        if hasattr(request, "text"):
            return request.text
        if hasattr(request, "value"):
            return request.value
        return super().parse(request)


class EmptyAttributeParser(BaseParser):

    def parse(self, request):
        if issubclass(type(request), proto.Message):
            return "Not set"
        return super().parse(request)


class GoogleAdsRowParser:

    def __init__(self, query_specification, nested_fields=None):
        self.nested_fields = nested_fields
        self.parser = self._init_parsers()
        self.row_getter = attrgetter(*query_specification.fields)
        self.fields = query_specification.fields
        self.customizers = query_specification.customizers
        self.virtual_attributes = query_specification.virtual_attributes
        self.column_names = query_specification.column_names

    def _init_parsers(self):
        parser_chain = BaseParser(None)
        for parser in [
                EmptyAttributeParser, AttributeParser, RepeatedCompositeParser,
                RepeatedParser
        ]:
            new_parser = parser(parser_chain)
            parser_chain = new_parser
        return parser_chain

    def parse(self, request):
        return self.parser.parse(request)

    def parse_ads_row(self, row) -> Sequence[Any]:
        final_rows = []
        extracted_attributes = self._get_attributes_from_row(
            row, self.row_getter)
        index = 0
        virtual_attributes = {}
        for i, column in enumerate(self.column_names):
            if column in self.virtual_attributes.keys():
                parsed_element = self._convert_virtual_attribute(
                    row, self.virtual_attributes[column])
            else:
                extracted_attribute = extracted_attributes[index]
                index += 1
                if self.customizers:
                    if (caller := self.customizers.get(column)):
                        if caller.get("type") == "nested_field":
                            values_ = caller.get("value").split(".")
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
                                        value = caller.get("value")
                                    extracted_attribute = list(
                                        set([
                                            attrgetter(value)(element)
                                            for element in extracted_attribute
                                        ]))
                                else:
                                    extracted_attribute = attrgetter(
                                        caller.get("value"))(
                                            extracted_attribute)
                            except AttributeError as e:
                                raise ValueError(
                                    f"customizer {caller} is incorrect,\ndetails: '{e}',\n"
                                    f"row: '{row}'")
                        elif caller.get("type") == "resource_index":
                            extracted_attribute = re.split(
                                "~", extracted_attribute)[caller.get("value")]
                            extracted_attribute = re.split(
                                "/", extracted_attribute)[-1]
                if isinstance(extracted_attribute, abc.MutableSequence):
                    parsed_element = [
                        self.parser.parse(element) or element
                        for element in extracted_attribute
                    ]
                else:
                    parsed_element = self.parser.parse(
                        extracted_attribute) or extracted_attribute
            final_rows.append(parsed_element)
        return final_rows if len(final_rows) > 1 else final_rows[0]

    def _get_attributes_from_row(self, row, getter) -> Tuple[Any, ...]:
        attributes = getter(row)
        return attributes if isinstance(attributes, tuple) else (attributes, )

    def _convert_virtual_attribute(self, row,
                                   virtual_attribute: VirtualAttribute) -> str:
        if virtual_attribute.type == "built-in":
            return virtual_attribute.value
        elif virtual_attribute.type == "expression":
            virtual_attribute_getter = attrgetter(*virtual_attribute.fields)
            virtual_attribute_values = virtual_attribute_getter(row)
            try:
                iter(virtual_attribute_values)
            except TypeError:
                virtual_attribute_values = (virtual_attribute_values, )
            virtual_attribute_replacements = {
                field.replace(".", "_"): value
                for field, value in zip(virtual_attribute.fields,
                                        virtual_attribute_values)
            }
            try:
                result = eval(
                    virtual_attribute.substitute_expression.format(
                        **virtual_attribute_replacements))
            except ZeroDivisionError:
                return 0
            except TypeError:
                raise VirtualAttributeError(
                    f"cannot parse virtual_attribute {virtual_attribute.value}"
                )
            except Exception as e:
                return virtual_attribute.value
            return result
        else:
            raise ValueError(
                f"Unsupported virtual attribute type: {virtual_attribute_type}"
            )


class ResourceFormatter:

    @staticmethod
    def get_resource(element: str) -> str:
        return re.split(": ", str(element).strip())[1]

    @staticmethod
    def get_resource_id(element: str) -> str:
        return re.split("/", str(element))[-1]

    @staticmethod
    def clean_resource_id(element: str) -> Union[int, str]:
        element = re.sub('"', '', str(element))
        try:
            return int(element)
        except ValueError:
            return element
