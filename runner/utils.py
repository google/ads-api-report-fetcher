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
# limitations under the License.import proto

from typing import Sequence
import re
import proto  # type: ignore
from google.cloud import bigquery  # type: ignore


class ResourceFormatter:
    @staticmethod
    def get_resource(element):
        return re.split(": ", str(element).strip())[1]

    @staticmethod
    def get_resource_id(element):
        return re.split("/", str(element))[-1]

    @staticmethod
    def clean_resource_id(element):
        element = re.sub('"', '', str(element))
        try:
            return int(element)
        except:
            return element


def get_element(elements, position):
    return f'{elements[position].strip().replace(",", "")}'


def extract_resource(field):
    return re.sub('"', '', re.split(": ", str(field).strip())[1])


def extract_id_from_resource(resource, id, delimeter="~"):
    return re.split(delimeter, resource)[id]


TYPE_MAPPING = {
    list: "REPEATED",
    str: "STRING",
    int: "INT64",
    float: "FLOAT64",
    bool: "BOOL",
    proto.marshal.collections.repeated.RepeatedComposite: "REPEATED",
    proto.marshal.collections.repeated.Repeated: "REPEATED"
}


def get_bq_schema(types):
    schema = []
    for key, value in types.items():
        element_type = TYPE_MAPPING.get(value.get("element_type"))
        schema.append(
            bigquery.SchemaField(
                name=key,
                field_type="STRING" if not element_type else element_type,
                mode="REPEATED" if value.get("repeated") else "NULLABLE"))
    return schema
