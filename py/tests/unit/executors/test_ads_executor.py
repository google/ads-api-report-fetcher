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
# limitations under the License
from __future__ import annotations

import json
import os

import pytest

from gaarf import api_clients
from gaarf.executors import ads_executor
from gaarf.io.writers import json_writer
from tests.unit import helpers


class TestAdsQueryExecutor:
  @pytest.fixture
  def test_client(self, mocker, config_path):
    mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
    return api_clients.GoogleAdsApiClient(path_to_config=config_path)

  @pytest.fixture
  def fake_response(self):
    fake_results = [
      [
        helpers.FakeGoogleAdsRowElement(helpers.Customer(1)),
      ],
      [
        helpers.FakeGoogleAdsRowElement(helpers.Customer(2)),
      ],
      [
        helpers.FakeGoogleAdsRowElement(helpers.Customer(3)),
      ],
    ]
    return helpers.FakeResponse(data=fake_results)

  @pytest.fixture
  def executor(self, mocker, test_client, fake_response):
    mocker.patch(
      'gaarf.api_clients.GoogleAdsApiClient.get_response',
      return_value=fake_response,
    )
    return ads_executor.AdsQueryExecutor(test_client)

  @pytest.fixture
  def test_json_writer(self, tmp_path):
    return json_writer.JsonWriter(destination_folder=tmp_path)

  def test_execute_returns_success(self, executor, test_json_writer):
    query_text = 'SELECT customer.id FROM customer'
    expected_result = [
      {'customer_id': 1},
      {'customer_id': 2},
      {'customer_id': 3},
    ]

    executor.execute(
      query_text=query_text,
      query_name='test',
      customer_ids='1234567890',
      writer_client=test_json_writer,
    )
    with open(
      os.path.join(test_json_writer.destination_folder, 'test.json'),
      'r',
      encoding='utf-8',
    ) as f:
      result = json.load(f)

    assert result == expected_result
