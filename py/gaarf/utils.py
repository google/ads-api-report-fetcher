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
"""Module for various utility functions."""

from __future__ import annotations

import warnings
from collections.abc import MutableSequence

from gaarf import api_clients, report_fetcher


def get_customer_ids(
  ads_client: api_clients.GoogleAdsApiClient,
  customer_id: str | MutableSequence,
  customer_ids_query: str | None = None,
) -> list[str]:
  """Gets list of customer_ids from an MCC account.

  Args:
      ads_client: GoogleAdsApiClient used for connection.
      customer_id: MCC account_id(s).
      customer_ids_query: GAQL query used to reduce the number of accounts.

  Returns:
      All customer_ids from MCC satisfying the condition.
  """
  warnings.warn(
    '`get_customer_ids` will be deprecated, '
    'use `AdsReportFetcher.expand_mcc` or `AdsQueryExecutor.expand_mcc` '
    'methods instead',
    category=DeprecationWarning,
    stacklevel=3,
  )
  return report_fetcher.AdsReportFetcher(ads_client).expand_mcc(
    customer_id, customer_ids_query
  )
