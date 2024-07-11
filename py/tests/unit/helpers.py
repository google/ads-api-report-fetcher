# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Contains helpers classes to simulate Google Ads API response."""

from __future__ import annotations

import dataclasses
from typing import Generator

from gaarf import parsers


@dataclasses.dataclass
class FakeResponse:
  """Simulates Google Ads API response iterator."""

  data: list[list[parsers.GoogleAdsRowElement]]

  def __iter__(self) -> Generator[FakeBatch, None, None]:
    """Yields batch from Google Ads API response."""
    for result in self.data:
      yield FakeBatch(result)


@dataclasses.dataclass
class FakeBatch:
  """Simulates a single batch of data from Google Ads API response.

  Attributes:
    results: Data from API response.
    query_resource_consumption: Resources consumed to serve the query.
  """

  results: list[parsers.GoogleAdsRowElement]
  query_resource_consumption: int = 0


@dataclasses.dataclass
class Customer:
  """Helper to represent customer resource."""

  id: int


@dataclasses.dataclass
class FakeGoogleAdsRowElement:
  """Helper to represent a single row in the batch."""

  customer: Customer
