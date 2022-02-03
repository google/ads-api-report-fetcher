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

from typing import Any, Dict, List, Optional, Sequence, Union
import abc
import dataclasses
import re

import utils
from formatter import QueryTextFormatter


@dataclasses.dataclass
class QueryElements:
    """Contains raw query and parsed elements.

    Attributes:
        query_text: Text of the query that needs to be parsed.
        fields: Ads API fields that need to be feched.
        column_names: friendly names for fields which are used when saving data
        pointers: Attributes of fields that need to be be extracted.
        nested_fields: Multiple elements that need to be fetched from a single
            resource.
        resource_indices: Position of element in resource_name.
    """
    query_text: str
    fields: List[str]
    column_names: List[str]
    pointers: Optional[Dict[str, str]]
    nested_fields: Optional[Dict[int, str]]
    resource_indices: Optional[Dict[str, str]]


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
    pointers = {}
    nested_fields = {}
    resource_indices = {}

    field_index = 0
    for line in query_lines:
        # exclude SELECT keyword
        if line.upper().startswith("SELECT"):
            continue
        # exclude everything that goes after FROM statement
        if line.upper().startswith("FROM"):
            break
        # get fields and aliases
        line_elements_raw = re.split(" [Aa][Ss] ", line)
        # extract unselectable fields
        resource_elements = re.split("~", line_elements_raw[0])
        line_elements = re.split("->", line_elements_raw[0])
        fields_with_nested = re.split(":", line_elements[0])
        # if there's `type` element rename it to `type_` in order to fetch it
        if len(resource_elements) == 1:
            field_name = re.sub("\\.type", ".type_",
                                utils.get_element(fields_with_nested, 0))
        else:
            field_name = utils.get_element(resource_elements, 0)
        if len(line_elements) > 1:
            pointers[field_name] = utils.get_element(line_elements, 1)
        if len(fields_with_nested) > 1:
            nested_fields[field_index] = utils.get_element(
                fields_with_nested, 1)
        fields.append(field_name)
        field_index += 1
        column_name = utils.get_element(
            line_elements_raw, 1) if len(line_elements_raw) > 1 else field_name
        if len(resource_elements) > 1:
            resource_indices[field_name] = utils.get_element(
                resource_elements, 1)
        column_names.append(column_name)
    return QueryElements(query_text=query_text,
                         fields=fields,
                         column_names=column_names,
                         pointers=pointers,
                         nested_fields=nested_fields,
                         resource_indices=resource_indices)
