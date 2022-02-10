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

from .formatter import QueryTextFormatter


@dataclasses.dataclass
class QueryElements:
    """Contains raw query and parsed elements.

    Attributes:
        query_text: Text of the query that needs to be parsed.
        fields: Ads API fields that need to be feched.
        column_names: friendly names for fields which are used when saving data
        customizers: Attributes of fields that need to be be extracted.
    """
    query_text: str
    fields: List[str]
    column_names: List[str]
    customizers: Optional[Dict[int, Dict[str, str]]]


def get_query_elements(path: str) -> QueryElements:
    """Reads query from a file and returns different elements of a query.

    Args:
        path: Path to a file with a query.

    Returns:
        Various elements parsed from a query (text, fields, column_names, etc).
    """
    with open(path, "r") as f:
        query_lines_ = f.readlines()

    query_lines = [
        line for line in query_lines_
        if not line.startswith("#") and line.strip() != ""
    ]
    query_text = QueryTextFormatter.format_ads_query("".join(query_lines))
    fields = []
    column_names = []
    customizers = {}

    field_index = 0
    for line in query_lines:
        # exclude SELECT keyword
        if line.upper().startswith("SELECT"):
            continue
        # exclude everything that goes after FROM statement
        if line.upper().startswith("FROM"):
            break
        field_elements, alias = extract_fields_and_aliases(line)
        field_name, customizer_type, customizer_value = query_parser_chain(
            field_elements)
        field_name = field_name.strip().replace(",", "")
        if customizer_type:
            customizers[field_index] = {
                "type": customizer_type,
                "value": customizer_value
            }
        fields.append(format_type_field_name(field_name))
        field_index += 1
        column_name = alias.strip().replace(",", "") if alias else field_name
        column_names.append(column_name)
    return QueryElements(query_text=query_text,
                         fields=fields,
                         column_names=column_names,
                         customizers=customizers)


def extract_fields_and_aliases(query_line: str) -> Tuple[str, Optional[str]]:
    field_raw, *alias = re.split(" [Aa][Ss] ", query_line)
    return field_raw, alias[0] if alias else None


def extract_resource_element(line_elements: str) -> List[str]:
    return re.split("~", line_elements)


def extract_pointer(line_elements: str) -> List[str]:
    return re.split("->", line_elements)


def extract_nested_resource(line_elements: str) -> List[str]:
    return re.split(":", line_elements)


def format_type_field_name(field_name):
    return re.sub("\\.type", ".type_", field_name)


def query_parser_chain(line_elements: str):
    resources = extract_resource_element(line_elements)
    pointers = extract_pointer(line_elements)
    nested_fields = extract_nested_resource(line_elements)
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
