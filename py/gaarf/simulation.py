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
"""Module for generating simulated GaarfReport based on query text."""

from __future__ import annotations

import dataclasses
import random
import string
from collections.abc import Sequence
from typing import Any

from faker import Faker

from gaarf.api_clients import GOOGLE_ADS_API_VERSION, BaseClient
from gaarf.query_editor import QuerySpecification
from gaarf.query_executor import AdsReportFetcher
from gaarf.report import GaarfReport


@dataclasses.dataclass(frozen=True)
class FakeField:
  type: Any
  name: str


@dataclasses.dataclass
class SimulatorSpecification:
  """Specifies meta information for simulated results."""

  api_version: str = GOOGLE_ADS_API_VERSION
  n_rows: int = 1000
  days_ago: str = '-7d'
  string_length: int = 3
  ignored_enums: tuple[str, ...] = ('REMOVED', 'UNKNOWN', 'UNSPECIFIED')
  allowed_enums: dict[str, Sequence[str]] | None = None
  replacements: dict[str, Any] | None = None


def simulate_data(
  query_text: str,
  query_name: str,
  args: dict,
  api_version: str,
  simulator_specification: SimulatorSpecification,
) -> GaarfReport | None:
  """Simulates GaarfReport based for a given query.

  Args:
      query_text: GAQL query text.
      query_name: Name of the query.
      args: Query parameters to be replaced in query_text.
      api_version: Ads API version to simulate against.
      simulator_specification: Meta information for simulated results.

  Returns:
      Report with simulated data. For `built-in` queries returns None.
  """
  query_specification = QuerySpecification(
    query_text, query_name, args, api_version
  ).generate()
  if query_specification.is_builtin_query:
    return None
  client = BaseClient(api_version)
  report_fetcher = AdsReportFetcher(client)
  inferred_types = client.infer_types(query_specification.fields)
  report = report_fetcher.fetch(query_specification)
  try:
    iter(report.results_placeholder[0])
    results = report.results_placeholder[0]
  except TypeError:
    results = [report.results_placeholder[0]]
  if not results:
    results = [results]
  row_field_types = [
    FakeField(type(field_value), field_name)
    for field_value, field_name in zip(results, query_specification.fields)
  ]
  values = _simulate_values(
    simulator_specification,
    query_specification,
    row_field_types,
    inferred_types,
  )
  return GaarfReport(values, query_specification.column_names)


def _simulate_values(
  simulator_specification: SimulatorSpecification,
  query_specification: QuerySpecification,
  row_field_types: list,
  inferred_types: list,
) -> list[list]:
  """Simulates values for a given query based on API version.

  Args:
      simulator_specification: Meta information for simulated results.
      query_specification: Specification of the query.
      row_field_types: Mapping between type of each row element and its name.
      inferred_types: Allowed types for each field of the query.

  Returns:
      Nested list with simulated values.
  """
  simulation_helper = Faker()
  values = []
  for _ in range(simulator_specification.n_rows):
    row = []
    for i, field in enumerate(row_field_types):
      if len(enums := inferred_types[i].values) > 1:
        if allowed_enums := simulator_specification.allowed_enums:
          if selected_enums := allowed_enums.get(field.name):
            value = random.choice(selected_enums)
          else:
            value = random.choice(list(enums))
        else:
          value = random.choice(
            [
              value
              for value in inferred_types[i].values
              if value not in simulator_specification.ignored_enums
            ]
          )
      elif (
        simulator_specification.replacements
        and field.name in simulator_specification.replacements
      ):
        value = random.choice(simulator_specification.replacements[field.name])
      else:
        value = _generate_random_value(
          field,
          simulator_specification,
          simulation_helper,
          query_specification.column_names[i],
        )
      row.append(value)
    values.append(row)
  return values


def _generate_random_value(
  field: FakeField,
  simulator_specification: SimulatorSpecification,
  simulation_helper: Faker,
  column_name: str,
) -> bool | str | float:
  """Generates random values based on field type and name.

  Args:
      field: A single element of a row.
      simulator_specification: Meta information for simulated results.
      simulation_helper: Class to perform simulation operations.
      column_name: Name of the column that needs to be simulated.

  Returns:
      Simulated value.
  """
  if field.type == str:
    if 'date' in field.name:
      return simulation_helper.date_between(
        simulator_specification.days_ago
      ).strftime('%Y-%m-%d')
    if 'url' in field.name:
      return 'example.com'
    if 'video.id' in field.name or 'video_id' in field.name:
      return '4WXs3sKu41I'
    if 'id' in column_name:
      return str(random.randint(1000000, 1000010))
    return _generate_random_string(simulator_specification.string_length)
  if field.type == int:
    if '.id' in field.name:
      return random.randint(1000000, 1000010)
    if 'micros' in field.name:
      return int(random.randint(0, 1000) * 1e6)
    return random.randint(0, 1000)
  if field.type == float:
    return float(random.randint(0, 1000))
  if field.type == bool:
    return bool(random.getrandbits(1))
  return ''


def _generate_random_string(length: int) -> str:
  'Helper for simulating random string of a given length.' ''
  characters = string.ascii_lowercase
  return ''.join(random.choice(characters) for _ in range(length))
