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

from typing import Any, Sequence
from operator import attrgetter
import proto  #type: ignore
import re
from .utils import ResourceFormatter


class BaseParser:
    def __init__(self, successor):
        self._successor = successor

    def parse(self, request):
        if self._successor:
            return self._successor.parse(request)
        return None


class RepeatedParser(BaseParser):
    def parse(self, request):
        if type(request) == proto.marshal.collections.repeated.Repeated and \
            "customer" in str(request):
            elements = []
            for v in request:
                element = ResourceFormatter.get_resource_id(v)
                elements.append(ResourceFormatter.clean_resource_id(element))
            return elements
        else:
            return super().parse(request)


class RepeatedCompositeParser(BaseParser):
    def parse(self, request):
        if type(request
                ) == proto.marshal.collections.repeated.RepeatedComposite:
            elements = []
            for v in request:
                resource = ResourceFormatter.get_resource(v)
                element = ResourceFormatter.get_resource_id(resource)
                elements.append(ResourceFormatter.clean_resource_id(element))
            return elements
        else:
            return super().parse(request)


class AttributeParser(BaseParser):
    def parse(self, request):
        if hasattr(request, "name"):
            return request.name
        elif hasattr(request, "text"):
            return request.text
        elif hasattr(request, "value"):
            return request.value
        else:
            return super().parse(request)


class EmptyAttributeParser(BaseParser):
    def parse(self, request):
        if issubclass(type(request), proto.Message):
            return "Not set"
        else:
            return super().parse(request)


class GoogleAdsRowParser(BaseParser):
    def __init__(self, nested_fields=None):
        self.nested_fields = nested_fields
        self.parser = self._init_parsers()

    def _init_parsers(self):
        parser_chain = BaseParser(None)
        for parser in EmptyAttributeParser, AttributeParser, RepeatedCompositeParser, RepeatedParser:
            new_parser = parser(parser_chain)
            parser_chain = new_parser
        return parser_chain

    def parse(self, request):
        return self.parser.parse(request)

    def parse_ads_row(self, row, query_specification) -> Sequence[Any]:
        final_rows = []
        extracted_rows = self._get_attributes_from_row(row,
                                                       query_specification)
        customizers = query_specification.customizers
        for i, r in enumerate(extracted_rows):
            if customizers:
                if customizers.get(i):
                    caller = customizers.get(i)
                    if caller.get("type") == "nested_field":
                        try:
                            r = attrgetter(caller.get("value"))(r)
                        except:
                            raise ValueError(f"{caller} is incorrect")
                    elif caller.get("type") == "resource_index":
                        r = re.split("~", r)[caller.get("value")]
            parsed_element = self.parser.parse(r) or r
            final_rows.append(parsed_element)
        return final_rows if len(final_rows) > 1 else final_rows[0]

    def _get_attributes_from_row(self, row, query_specification):
        getter = attrgetter(*query_specification.fields)
        rows = getter(row)
        return rows if isinstance(rows, tuple) else (rows, )
