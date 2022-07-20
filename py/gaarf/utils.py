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

from typing import Sequence

from .api_clients import GoogleAdsApiClient
from .query_editor import QuerySpecification
from .query_executor import AdsReportFetcher


def get_customer_ids(ads_client: GoogleAdsApiClient,
                     customer_id: str,
                     customer_ids_query: str = None) -> Sequence[str]:
    """Gets list of customer_ids from an MCC account.

    Args:
        ads_client: GoogleAdsApiClient used for connection.
        customer_id: MCC account_id.
        custom_query: GAQL query used to reduce the number of customer_ids.
    Returns:
        All customer_ids from MCC safisfying the condition.
    """

    query = """
    SELECT customer_client.id FROM customer_client
    WHERE customer_client.manager = FALSE AND customer_client.status = "ENABLED"
    """
    query_specification = QuerySpecification(query).generate()
    report_fetcher = AdsReportFetcher(ads_client, customer_id)
    customer_ids = report_fetcher.fetch(query_specification).to_list()
    if customer_ids_query:
        report_fetcher = AdsReportFetcher(ads_client, customer_ids)
        query_specification = QuerySpecification(customer_ids_query).generate()
        customer_ids = report_fetcher.fetch(query_specification).to_list()
    return list(set(customer_ids))
