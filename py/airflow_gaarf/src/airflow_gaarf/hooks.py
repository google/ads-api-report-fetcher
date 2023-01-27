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

from typing import Any, Dict

from functools import cached_property

from google.auth.exceptions import GoogleAuthError

from airflow import AirflowException
from airflow.hooks.base import BaseHook
from gaarf.api_clients import GoogleAdsApiClient
from gaarf.bq_executor import BigQueryExecutor


class GaarfHook(BaseHook):

    def __init__(self,
                 google_ads_conn_id: str = "google_ads_default",
                 api_version: str = "v10") -> None:
        super().__init__()
        self.google_ads_conn_id = google_ads_conn_id
        self.api_version = api_version
        self.google_ads_config: Dict[str, Any] = {}

    @cached_property
    def get_client(self) -> GoogleAdsApiClient:
        self._get_config()
        try:
            return GoogleAdsApiClient(path_to_config=None,
                                      config_dict=self.google_ads_config,
                                      version=self.api_version)
        except GoogleAuthError as e:
            self.log.error("Google Auth Error: %s", e)
            raise

    def _get_config(self) -> None:
        conn = self.get_connection(self.google_ads_conn_id)
        if "google_ads_client" not in conn.extra_dejson:
            raise AirflowException(
                "google_ads_client not found in extra field")
        self.google_ads_config = conn.extra_dejson["google_ads_client"]


class GaarfBqHook(BaseHook):

    def __init__(self, gcp_conn_id: str = "gcp_conn_id") -> None:
        super().__init__()
        self.gcp_conn_id = gcp_conn_id
        self.config = Dict[str, Any]

    @cached_property
    def get_bq_executor(self) -> BigQueryExecutor:
        self._get_config()
        try:
            return BigQueryExecutor(project_id=self.config.get("project_id"))
        except GoogleAuthError as e:
            self.log.error("Google Auth Error: %s", e)
            raise

    def _get_config(self) -> None:
        conn = self.get_connection(self.gcp_conn_id)
        if "cloud" not in conn.extra_dejson:
            raise AirflowException("cloud not found in extra field")
        self.config = conn.extra_dejson["cloud"]
