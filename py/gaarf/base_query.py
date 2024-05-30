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
"""Module for defining base class for Gaarf query classes.

Gaarf query classes can inherit from BaseQuery and have a simple way of
being fetched from API.
"""

from __future__ import annotations


class BaseQuery:
  """Base class to inherit all Gaarf queries from.


  Attributes:
      query_text: Contains query text or template.
  """

  query_text = ''

  @property
  def query(self) -> str:
    """Returns expanded query with parameters takes from initialization."""
    return self.query_text.format(**self.__dict__)

  def __str__(self) -> str:
    """Formatted query string representation."""
    return self.query.strip()
