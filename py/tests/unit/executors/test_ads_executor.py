from __future__ import annotations

import dataclasses
import json
import os

import pytest

from gaarf import api_clients, parsers
from gaarf.executors import ads_executor
from gaarf.io.writers import json_writer


@dataclasses.dataclass
class FakeResponse:
  data: list[list[parsers.GoogleAdsRowElement]]

  def __iter__(self):
    for result in self.data:
      yield FakeBatch(result)


@dataclasses.dataclass
class FakeBatch:
  results: list[list]


@dataclasses.dataclass
class Customer:
  id: int


@dataclasses.dataclass
class FakeGoogleAdsRowElement:
  customer: Customer


class TestAdsQueryExecutor:
  @pytest.fixture
  def test_client(self, mocker, config_path):
    mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
    return api_clients.GoogleAdsApiClient(path_to_config=config_path)

  @pytest.fixture
  def fake_response(self):
    fake_results = [
      [
        FakeGoogleAdsRowElement(Customer(1)),
      ],
      [
        FakeGoogleAdsRowElement(Customer(2)),
      ],
      [
        FakeGoogleAdsRowElement(Customer(3)),
      ],
    ]
    return FakeResponse(data=fake_results)

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
