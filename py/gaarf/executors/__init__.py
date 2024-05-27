# Copyright 2024 Google LLC
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
"""Defines simplified import of executors.

Instead of importing `gaarf.executors.ads_executors.AdsQueryExecutor`
import like this `gaarf.executors.AdsQueryExecutor`
"""
from __future__ import annotations

from gaarf.executors.ads_executor import AdsQueryExecutor
from gaarf.executors.bq_executor import BigQueryExecutor
from gaarf.executors.sql_executor import SqlAlchemyQueryExecutor

__all__ = [
    'AdsQueryExecutor',
    'BigQueryExecutor',
    'SqlAlchemyQueryExecutor',
]
