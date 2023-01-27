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

from typing import List
import os
import pathlib
from gaarf.utils import get_customer_ids

from .hooks import GaarfHook
from .operators import GaarfOperator, GaarfBqOperator


def get_query_basename(query_path: pathlib.Path) -> str:
    return os.path.basename(query_path)[:-4]


class GaarfMccExpander:

    def __init__(self,
                 google_ads_conn_id: str = "google_ads_default",
                 api_version: str = "v12") -> None:
        self.client = GaarfHook(google_ads_conn_id=google_ads_conn_id,
                                api_version=api_version)

    def expand_seed_customer_id(self, customer_id: str, customer_ids_query: str) -> List[str]:
        return get_customer_ids(self.client.get_client, customer_id, customer_ids_query)


class GaarfExecutor:

    def __init__(self,
                 query_params,
                 customer_ids,
                 writer_client,
                 reader_client,
                 google_ads_conn_id="google_ads_default",
                 api_version="v12"):
        self.query_params = query_params
        self.customer_ids = customer_ids
        self.writer_client = writer_client
        self.reader_client = reader_client
        self.google_ads_conn_id = google_ads_conn_id
        self.api_version = api_version

    def run(self, query):
        return GaarfOperator(task_id=f"fetch_{get_query_basename(query)}",
                             query=str(query),
                             query_params=self.query_params,
                             customer_ids=[self.customer_ids],
                             writer_client=self.writer_client,
                             reader_client=self.reader_client,
                             google_ads_conn_id=self.google_ads_conn_id,
                             api_version=self.api_version)


class GaarfBqExecutor:

    def __init__(self, query_params, reader_client, gcp_conn_id="gcp_conn_id"):
        self.query_params = query_params
        self.reader_client = reader_client
        self.gcp_conn_id = gcp_conn_id

    def run(self, query):
        return GaarfBqOperator(
            task_id=f"postprocess_{get_query_basename(query)}",
            query=query,
            query_params=self.query_params,
            reader_client=self.reader_client,
            gcp_conn_id=self.gcp_conn_id)
