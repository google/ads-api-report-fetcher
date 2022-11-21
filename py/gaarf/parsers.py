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

from typing import Any, Tuple, Sequence, Union
from operator import attrgetter
import re
import proto  # type: ignore
from proto.marshal.collections.repeated import (  # type: ignore
    Repeated, RepeatedComposite)  # type: ignore


class BaseParser:

    def __init__(self, successor):
        self._successor = successor

    def parse(self, request):
        if self._successor:
            return self._successor.parse(request)
        return None


class RepeatedParser(BaseParser):

    def parse(self, request):
        if isinstance(request, Repeated) and "customer" in str(request):
            elements = []
            for request_element in request:
                element = ResourceFormatter.get_resource_id(request_element)
                elements.append(ResourceFormatter.clean_resource_id(element))
            return elements
        return super().parse(request)


class RepeatedCompositeParser(BaseParser):

    def parse(self, request):
        if isinstance(request, RepeatedComposite):
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

    def __init__(self, nested_fields=None):
        self.nested_fields = nested_fields
        self.parser = self._init_parsers()

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

    def parse_ads_row(self, row, query_specification) -> Sequence[Any]:
        final_rows = []
        extracted_rows = self._get_attributes_from_row(
            row, query_specification.fields)
        customizers = query_specification.customizers
        for i, extracted_row in enumerate(extracted_rows):
            if customizers:
                if (caller := customizers.get(i)):
                    if caller.get("type") == "nested_field":
                        values_ = caller.get("value").split(".")
                        extracted_row_ = getattr(
                            extracted_row, values_[0]) if hasattr(
                                extracted_row, values_[0]) else extracted_row
                        try:
                            if isinstance(
                                    extracted_row,
                                (Repeated, RepeatedComposite)) or isinstance(
                                    extracted_row_,
                                    (Repeated, RepeatedComposite)):
                                if isinstance(extracted_row_,
                                              (Repeated, RepeatedComposite)):
                                    extracted_row = extracted_row_
                                if len(values_) > 1:
                                    value = values_[1]
                                else:
                                    value = caller.get("value")
                                extracted_row = list(
                                    set([
                                        attrgetter(value)(element)
                                        for element in extracted_row
                                    ]))
                            else:
                                extracted_row = attrgetter(
                                    caller.get("value"))(extracted_row)
                        except AttributeError:
                            raise ValueError(f"{caller} is incorrect")
                    elif caller.get("type") == "resource_index":
                        extracted_row = re.split(
                            "~", extracted_row)[caller.get("value")]
                        extracted_row = re.split("/", extracted_row)[-1]
            if isinstance(extracted_row, list):
                parsed_element = [
                    self.parser.parse(element) or element
                    for element in extracted_row
                ]
            else:
                parsed_element = self.parser.parse(
                    extracted_row) or extracted_row
            final_rows.append(parsed_element)
        return final_rows if len(final_rows) > 1 else final_rows[0]

    def _get_attributes_from_row(self, row, fields) -> Tuple[Any, ...]:
        getter = attrgetter(*fields)
        rows = getter(row)
        return rows if isinstance(rows, tuple) else (rows, )


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
