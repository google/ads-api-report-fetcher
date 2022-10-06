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

from typing import Any, Dict, Optional, Sequence

from google.ads.googleads.client import GoogleAdsClient  # type: ignore
from importlib import import_module
from pathlib import Path
from proto.primitives import ProtoType


class BaseClient:

    def get_response(self, entity_id: str, query_text: str):
        pass


class GoogleAdsApiClient(BaseClient):
    default_google_ads_yaml = str(Path.home() / "google-ads.yaml")

    def __init__(self,
                 path_to_config: str = default_google_ads_yaml,
                 config_dict: Dict[str, Any] = None,
                 yaml_str: str = None,
                 version: str = "v11"):
        self.client = self._init_client(path=path_to_config,
                                        config_dict=config_dict,
                                        yaml_str=yaml_str,
                                        version=version)
        self.ads_service = self.client.get_service("GoogleAdsService")
        self.api_version = version

    def get_response(self, entity_id, query_text):
        response = self.ads_service.search_stream(customer_id=entity_id,
                                                  query=query_text)
        return response

    def _init_client(self, path, config_dict, yaml_str,
                     version) -> Optional[GoogleAdsClient]:
        if path:
            return GoogleAdsClient.load_from_storage(path, version)
        if config_dict:
            return GoogleAdsClient.load_from_dict(config_dict, version)
        if yaml_str:
            return GoogleAdsClient.load_from_string(yaml_str, version)
        try:
            return GoogleAdsClient.load_from_env(version)
        except Exception as e:
            raise ValueError("Cannot instantiate GoogleAdsClient")

    def infer_types(self, fields: Sequence[str]) -> Sequence[Any]:
        """Maps API fields to Python primitives."""

        base_module = f"google.ads.googleads.{self.api_version}"
        common_types_module = f"{base_module}.common.types"
        segments = import_module(f"{common_types_module}.segments")
        metrics = import_module(f"{common_types_module}.metrics")

        mapping = {"INT64": int, "FLOAT": float, "DOUBLE": float, "BOOL": bool}
        output = []
        for field in fields:
            resource, *sub_resource, base_field = field.split(".")
            if resource == "metrics":
                result = metrics.Metrics.meta.fields.get(
                    base_field).descriptor.type
            elif resource == "segments":
                # If segment has name segments.something.something
                if sub_resource:
                    target_segment = getattr(
                        segments, f"{clean_resource(sub_resource[-1])}")
                else:
                    target_segment = getattr(segments,
                                             f"{clean_resource(resource)}")
                result = target_segment.meta.fields.get(
                    base_field).descriptor.type
            else:
                resource_module = import_module(
                    f"{base_module}.resources.types.{resource}")

                target_resource = getattr(resource_module,
                                          f"{clean_resource(resource)}")
                try:
                    # If resource has name resource.something.something
                    if sub_resource:
                        target_resource = getattr(
                            target_resource,
                            f"{clean_resource(sub_resource[-1])}")
                except AttributeError:
                    resource_module = import_module(
                        f"{base_module}.resources.types.{sub_resource[0]}")
                    if len(sub_resource) > 1:
                        if hasattr(resource_module,
                                   f"{clean_resource(sub_resource[1])}"):
                            target_resource = getattr(
                                resource_module,
                                f"{clean_resource(sub_resource[-1])}")
                        else:
                            resource_module = import_module(
                                f"{common_types_module}.ad_type_infos")

                            target_resource = getattr(
                                resource_module,
                                f"{clean_resource(sub_resource[1])}Info")
                    else:
                        target_resource = getattr(
                            resource_module,
                            f"{clean_resource(sub_resource[-1])}")
                result = target_resource.meta.fields.get(
                    base_field).descriptor.type

            output.append(mapping.get(ProtoType(result).name, str))
        return output


def clean_resource(resource: str) -> str:
    return resource.title().replace('_', '')
