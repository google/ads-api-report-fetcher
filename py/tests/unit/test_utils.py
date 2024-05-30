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

import dataclasses

import pytest

from gaarf import api_clients, parsers, report_fetcher, utils


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
class CustomerClient:
  id: int


@dataclasses.dataclass
class FakeGoogleAdsRowElement:
  customer_client: CustomerClient


@pytest.fixture
def test_client(mocker, config_path):
  fake_results = [
    [
      FakeGoogleAdsRowElement(CustomerClient(1)),
    ],
  ]
  fake_response = FakeResponse(data=fake_results)
  mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
  mocker.patch(
    'gaarf.api_clients.GoogleAdsApiClient.get_response',
    return_value=fake_response,
  )
  return api_clients.GoogleAdsApiClient(path_to_config=config_path)


@pytest.fixture
def fake_report_fetcher(mocker, test_client):
  data = ['1']
  mocker.patch(
    'gaarf.report_fetcher.AdsReportFetcher.expand_mcc', return_value=data
  )
  return report_fetcher.AdsReportFetcher(test_client)


def test_calling_get_customer_ids_is_deprecated(test_client):
  with pytest.warns(DeprecationWarning) as w:
    utils.get_customer_ids(test_client, '1')
    assert len(w) == 1
    assert str(w[0].message) == (
      '`get_customer_ids` will be deprecated, '
      'use `AdsReportFetcher.expand_mcc` or '
      '`AdsQueryExecutor.expand_mcc` '
      'methods instead'
    )
