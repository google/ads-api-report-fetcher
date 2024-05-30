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
from __future__ import annotations

import pytest

from gaarf import api_clients, query_executor


@pytest.fixture
def test_client(mocker, config_path):
  mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
  return api_clients.GoogleAdsApiClient(path_to_config=config_path)


class TestAdsReportFetcher:
  def test_instantiating_ads_report_fetcher_Is_deprecated(self, test_client):
    with pytest.warns(DeprecationWarning) as w:
      query_executor.AdsReportFetcher(api_client=test_client)
      assert len(w) == 1
      assert issubclass(w[0].category, DeprecationWarning)


class TestAdsQueryExecutor:
  def test_instantiating_ads_query_executor_Is_deprecated(self, test_client):
    with pytest.warns(DeprecationWarning) as w:
      query_executor.AdsQueryExecutor(api_client=test_client)
      assert len(w) == 1
      assert issubclass(w[0].category, DeprecationWarning)
