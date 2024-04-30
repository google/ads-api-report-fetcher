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
# limitations under the License.import proto
"""Module for various utility functions."""
from __future__ import annotations

import warnings
from collections.abc import MutableSequence
from collections.abc import Sequence

from gaarf import api_clients
from gaarf import query_editor
from gaarf import query_executor
from gaarf import report


def get_customer_ids(ads_client: api_clients.GoogleAdsApiClient,
                     customer_id: str | MutableSequence,
                     customer_ids_query: str | None = None) -> Sequence[str]:
    """Gets list of customer_ids from an MCC account.

    Args:
        ads_client: GoogleAdsApiClient used for connection.
        customer_id: MCC account_id(s).
        custom_query: GAQL query used to reduce the number of customer_ids.

    Returns:
        All customer_ids from MCC satisfying the condition.
    """

    query = """
    SELECT customer_client.id FROM customer_client
    WHERE customer_client.manager = FALSE AND customer_client.status = ENABLED
    """
    warnings.warn(
        '`get_customer_ids` will be deprecated, '
        'use `AdsReportFetcher.expand_mcc` or `AdsQueryExecutor.expand_mcc` '
        'methods instead',
        category=DeprecationWarning,
        stacklevel=3)
    query_specification = query_editor.QuerySpecification(query).generate()
    if not isinstance(customer_id, MutableSequence):
        customer_id = customer_id.split(',')
    report_fetcher = query_executor.AdsReportFetcher(ads_client)
    customer_ids = report_fetcher.fetch(query_specification,
                                        customer_id).to_list()
    if customer_ids_query:
        query_specification = query_editor.QuerySpecification(
            customer_ids_query).generate()
        customer_ids = report_fetcher.fetch(query_specification, customer_ids)
        customer_ids = [
            row[0] if isinstance(row, report.GaarfRow) else row
            for row in customer_ids
        ]

    customer_ids = list(
        set([customer_id for customer_id in customer_ids if customer_id != 0]))

    return customer_ids
