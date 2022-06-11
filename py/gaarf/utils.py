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

from .query_editor import QuerySpecification
from .query_executor import AdsReportFetcher


def get_customer_ids(ads_client, customer_id):
    query = """
    SELECT customer_client.id FROM customer_client
    WHERE customer_client.manager = FALSE
    """
    query_specification = QuerySpecification(query).generate()
    report_fetcher = AdsReportFetcher(ads_client)
    return report_fetcher.fetch(query_specification, customer_id)
