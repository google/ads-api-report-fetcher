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
from google.ads.googleads.client import GoogleAdsClient  #type: ignore
from google.ads.googleads.v9.services.services.google_ads_service.client import GoogleAdsServiceClient  #type: ignore
from pathlib import Path


class BaseClient:
    def get_response(self, entity_id: str, query_text: str):
        pass


class GoogleAdsApiClient(BaseClient):
    default_google_ads_yaml = str(Path.home() / "google-ads.yaml")

    def __init__(self,
                 path_to_config: str = default_google_ads_yaml,
                 version: str = "v9"):
        self.client = GoogleAdsClient.load_from_storage(path=path_to_config,
                                                        version=version)
        self.ads_service = self.client.get_service("GoogleAdsService")

    def get_response(self, entity_id, query_text):
        response = self.ads_service.search_stream(customer_id=entity_id,
                                                  query=query_text)
        return response
