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
