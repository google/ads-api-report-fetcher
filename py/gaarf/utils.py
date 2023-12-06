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

from typing import Sequence, Union
from collections.abc import MutableSequence
import re
import warnings

from .api_clients import GoogleAdsApiClient
from .query_editor import QuerySpecification
from .query_executor import AdsReportFetcher
from .report import GaarfRow, GaarfReport


def get_customer_ids(ads_client: GoogleAdsApiClient,
                     customer_id: Union[str, MutableSequence],
                     customer_ids_query: str = None) -> Sequence[str]:
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
    WHERE customer_client.manager = FALSE AND customer_client.status = "ENABLED"
    """
    warnings.warn("`get_customer_ids` will be deprecated, use `AdsReportFetcher.expand_mcc` or `AdsQueryExecutor.expand_mcc` methods instead",
                 category=DeprecationWarning, stacklevel=3)
    query_specification = QuerySpecification(query).generate()
    if not isinstance(customer_id, MutableSequence):
        customer_id = customer_id.split(",")
    report_fetcher = AdsReportFetcher(ads_client)
    customer_ids = report_fetcher.fetch(query_specification, customer_id).to_list()
    if customer_ids_query:
        query_specification = QuerySpecification(customer_ids_query).generate()
        customer_ids = report_fetcher.fetch(query_specification, customer_ids)
        customer_ids = [
            row[0] if isinstance(row, GaarfRow) else row
            for row in customer_ids
        ]

    customer_ids = list(
        set([customer_id for customer_id in customer_ids if customer_id != 0]))

    return customer_ids
