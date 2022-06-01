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

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import abc
import dataclasses
import re


@dataclasses.dataclass
class QueryElements:
    """Contains raw query and parsed elements.

    Attributes:
        query_title: Title of the query that needs to be parsed.
        query_text: Text of the query that needs to be parsed.
        fields: Ads API fields that need to be feched.
        column_names: friendly names for fields which are used when saving data
        customizers: Attributes of fields that need to be be extracted.
    """
    query_title: str
    query_text: str
    fields: List[str]
    column_names: List[str]
    customizers: Optional[Dict[int, Dict[str, str]]]
    resource_name: str
    is_constant_resource: bool


class QuerySpecification:
    def __init__(self, title: str, text: str, args: Dict[Any, Any] = None):
        self.title = title
        self.text = text
        self.args = args

    def generate(self) -> QueryElements:
        """Reads query from a file and returns different elements of a query.

        Args:
            path: Path to a file with a query.

        Returns:
            Various elements parsed from a query (text, fields, column_names, etc).
        """

        query_lines = self.cleanup_query_text(self.text)
        resource_name = self.extract_resource_from_query(self.text)
        is_constant_resource = True if resource_name.endswith(
            "_constant") else False
        query_text = self.normalize_query(" ".join(query_lines))
        if self.args:
            query_text = query_text.format(**self.args)
        fields = []
        column_names = []
        customizers = {}

        field_index = 0
        query_lines = self.extract_query_lines(" ".join(query_lines))
        for line in query_lines:
            field_elements, alias = self.extract_fields_and_aliases(line)
            field_name, customizer_type, customizer_value = self.extract_fields_and_customizers(
                field_elements)
            field_name = field_name.strip().replace(",", "")
            if customizer_type:
                customizers[field_index] = {
                    "type": customizer_type,
                    "value": customizer_value
                }
            fields.append(self.format_type_field_name(field_name))
            field_index += 1
            column_name = alias.strip().replace(",",
                                                "") if alias else field_name
            column_names.append(self.normalize_column_name(column_name))
        return QueryElements(query_title=self.title,
                             query_text=query_text,
                             fields=fields,
                             column_names=column_names,
                             customizers=customizers,
                             resource_name=resource_name,
                             is_constant_resource=is_constant_resource)

    def cleanup_query_text(self, query: str) -> List[str]:
        query_lines = query.split("\n")
        result = []
        for line in query_lines:
            if re.match("^(#|--|//)", line):
                continue
            result.append(re.sub("(--|//).*$", "", line).strip())
        return result

    def extract_resource_from_query(self, query: str) -> str:
        return str(re.findall(r"FROM\s+(\w+)", query,
                              flags=re.IGNORECASE)[0]).strip()

    def extract_query_lines(self, query_text: str) -> List[str]:
        selected_fields = re.sub(r"\bSELECT\b|FROM .*",
                                 "",
                                 query_text,
                                 flags=re.IGNORECASE).split(",")
        return [field.strip() for field in selected_fields if field != " "]

    def extract_fields_and_aliases(
            self, query_line: str) -> Tuple[str, Optional[str]]:
        field_raw, *alias = re.split(" [Aa][Ss] ", query_line)
        return field_raw, alias[0] if alias else None

    def extract_fields_and_customizers(self, line_elements: str):
        resources = self.extract_resource_element(line_elements)
        pointers = self.extract_pointer(line_elements)
        nested_fields = self.extract_nested_resource(line_elements)
        if len(resources) > 1:
            field_name, resource_index = resources
            return field_name, "resource_index", int(resource_index)
        if len(pointers) > 1:
            field_name, pointer = pointers
            return field_name, "pointer", pointer
        if len(nested_fields) > 1:
            field_name, nested_field = nested_fields
            return field_name, "nested_field", nested_field
        return line_elements, None, None

    def extract_resource_element(self, line_elements: str) -> List[str]:
        return re.split("~", line_elements)

    def extract_pointer(self, line_elements: str) -> List[str]:
        return re.split("->", line_elements)

    def extract_nested_resource(self, line_elements: str) -> List[str]:
        return re.split(":", line_elements)

    def format_type_field_name(self, field_name: str) -> str:
        return re.sub(r"\.type", ".type_", field_name)

    def normalize_column_name(self, column_name: str) -> str:
        return re.sub(r"\.", "_", column_name)

    def normalize_query(self, query: str) -> str:
        query = self._remove_alias(query)
        query = self._remove_pointers(query)
        query = self._remove_nested_fields(query)
        query = self._clean_spaces_tabs_new_lines(query)
        query = self._remove_traling_comma(query)
        return query

    def _remove_alias(self, query: str) -> str:
        return re.sub(r"\s+[Aa][Ss]\s+(\w+)", "", query)

    def _remove_pointers(self, query: str) -> str:
        query = re.sub(r"->(\w+)|->", "", query)
        query = re.sub(r"~(\w+)|->", "", query)
        return query

    def _remove_nested_fields(self, query: str) -> str:
        return re.sub(r":((\w+)\.*){1,}", "", query)

    def _clean_spaces_tabs_new_lines(self, query: str) -> str:
        return re.sub(r"\s+", " ", query).strip()

    def _remove_traling_comma(self, query: str) -> str:
        return re.sub(r",\s+from", " FROM", query, re.IGNORECASE)
