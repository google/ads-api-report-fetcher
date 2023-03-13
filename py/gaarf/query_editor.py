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

from typing import Any, Dict, List, Optional, Tuple
import dataclasses
import re
from .api_clients import BaseClient
from operator import attrgetter


@dataclasses.dataclass(frozen=True)
class VirtualColumn:
    type: str
    value: str
    fields: Optional[List[str]] = None
    substitute_expression: Optional[str] = None


class VirtualColumnError(Exception):
    pass


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
    customizers: Optional[Dict[str, Dict[str, str]]]
    virtual_columns: Optional[Dict[str, VirtualColumn]]
    resource_name: str
    is_constant_resource: bool


class QuerySpecification:

    def __init__(self,
                 text: str,
                 title: str = None,
                 args: Dict[Any, Any] = None,
                 api_version: str = "v12"):
        self.text = text
        self.title = title
        self.args = args
        self.base_client = BaseClient(api_version)

    def generate(self) -> QueryElements:
        """Reads query from a file and returns different elements of a query.

        Args:
            path: Path to a file with a query.

        Returns:
            Various elements parsed from a query (text, fields,
            column_names, etc).
        """

        query_lines = self.cleanup_query_text(self.text)
        resource_name = self.extract_resource_from_query(self.text)
        is_constant_resource = bool(resource_name.endswith("_constant"))
        query_text = " ".join(query_lines)
        if self.args:
            if (macros := self.args.get("macro")):
                query_text = query_text.format(**macros)
        fields = []
        column_names = []
        customizers = {}
        virtual_columns = {}

        query_lines = self.extract_query_lines(" ".join(query_lines))
        for line in query_lines:
            field_name = None
            customizer_type = None
            field_elements, alias, virtual_column = self.extract_fields_and_aliases(
                line)
            if field_elements:
                for field_element in field_elements:
                    field_name, customizer_type, customizer_value = \
                        self.extract_fields_and_customizers(field_element)
                    field_name = field_name.strip().replace(",", "")
                    fields.append(self.format_type_field_name(field_name))
            if not alias and not field_name:
                raise ValueError("Virtual attributes should be aliased")
            else:
                column_name = alias.strip().replace(
                    ",", "") if alias else field_name
                column_names.append(self.normalize_column_name(column_name))

            if virtual_column:
                virtual_columns[column_name] = virtual_column
            if customizer_type:
                customizers[column_name] = {
                    "type": customizer_type,
                    "value": customizer_value
                }
        query_text = self.create_query_text(fields, virtual_columns,
                                            query_text)
        return QueryElements(query_title=self.title,
                             query_text=query_text,
                             fields=fields,
                             column_names=column_names,
                             customizers=customizers,
                             virtual_columns=virtual_columns,
                             resource_name=resource_name,
                             is_constant_resource=is_constant_resource)

    def create_query_text(self, fields: List[str],
                          virtual_columns: Dict[str, VirtualColumn],
                          query_text: str) -> str:
        virtual_fields = [
            field for name, column in virtual_columns.items()
            if column.type == "expression" for field in column.fields
        ]
        if virtual_fields:
            fields = fields + virtual_fields
        query_text = f"SELECT {', '.join(fields)} {self.extract_from_statement(query_text)}"
        query_text = self._remove_traling_comma(query_text)
        query_text = self._unformat_type_field_name(query_text)
        query_text = re.sub("\s+", " ", query_text).strip()
        return query_text

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

    def extract_from_statement(self, query_text: str) -> str:
        return re.search(" FROM .+", query_text, re.IGNORECASE).group(0)

    def extract_fields_and_aliases(
        self, query_line: str
    ) -> Tuple[Optional[List[str]], Optional[str], Optional[VirtualColumn]]:
        fields = []
        virtual_column = None
        field_raw, *alias = re.split(" [Aa][Ss] ", query_line)
        field_raw = field_raw.replace("\s+", "").strip()
        virtual_field, _, _ = self.extract_fields_and_customizers(field_raw)
        try:
            _ = attrgetter(virtual_field)(self.base_client.google_ads_row)
        except AttributeError:
            operators = ("/", "\*", "\+", "-")
            if len(expressions := re.split("|".join(operators),
                                           field_raw)) > 1:
                virtual_column_fields = []
                substitute_expression = virtual_field
                for element in expressions:
                    element = element.strip()
                    try:
                        _ = attrgetter(element)(
                            self.base_client.google_ads_row)
                        virtual_column_fields.append(element)
                        substitute_expression = substitute_expression.replace(
                            element, f"{{{element}}}")
                    except AttributeError:
                        pass
                virtual_column = VirtualColumn(
                    type="expression",
                    value=virtual_field,
                    fields=virtual_column_fields,
                    substitute_expression=substitute_expression.replace(".", "_"))
            else:
                virtual_column = VirtualColumn(type="built-in",
                                                     value=virtual_field)
        if not virtual_column and field_raw:
            fields = [field_raw]
        else:
            fields = None
        return fields, alias[0] if alias else None, virtual_column

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

    def _remove_traling_comma(self, query: str) -> str:
        return re.sub(r",\s+from", " FROM", query, re.IGNORECASE)

    def _unformat_type_field_name(self, query: str) -> str:
        return re.sub(r"\.type_", ".type", query)
