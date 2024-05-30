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
"""Module for executing Gaarf queries and writing them to local/remote.


Module defines two major classes:
    * AdsReportFetcher - to perform fetching data from Ads API, parsing it
      and returning GaarfReport.
    * AdsQueryExecutor - to perform fetching data from Ads API in a form of
      GaarfReport and saving it to local/remote storage.
"""

from __future__ import annotations

import warnings
from collections.abc import Sequence

from gaarf import api_clients, executors, report_fetcher


class AdsReportFetcher:
  """Deprecated class for creating AdsReportFetcher."""

  def __new__(
    cls,
    api_client: api_clients.BaseClient,
    customer_ids: Sequence[str] | None = None,
  ) -> report_fetcher.AdsReportFetcher:
    warnings.warn(
      'Loading AdsReportFetcher from `gaarf.query_executor` is '
      'deprecated; Import AdsReportFetcher from `gaarf.report_fetcher` '
      'instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return report_fetcher.AdsReportFetcher(api_client, customer_ids)


class AdsQueryExecutor:
  """Deprecated class for creating AdsQueryExecutor."""

  def __new__(
    cls,
    api_client: api_clients.BaseClient,
  ) -> executors.ads_executor.AdsQueryExecutor:
    warnings.warn(
      'Loading AdsReportFetcher from `gaarf.query_executor` is '
      'deprecated; Import AdsReportFetcher from `gaarf.report_fetcher` '
      'instead',
      category=DeprecationWarning,
      stacklevel=2,
    )
    return executors.ads_executor.AdsQueryExecutor(api_client)
