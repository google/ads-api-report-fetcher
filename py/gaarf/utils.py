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
