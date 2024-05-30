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
"""Module for defining client to interact with API."""

from __future__ import annotations

import dataclasses
import importlib
import os
import re
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import Final

import proto
import smart_open
import tenacity
import yaml
from google import protobuf
from google.ads.googleads import client as googleads_client
from google.api_core import exceptions as google_exceptions

GOOGLE_ADS_API_VERSION: Final = googleads_client._DEFAULT_VERSION
google_ads_service = importlib.import_module(
  f'google.ads.googleads.{GOOGLE_ADS_API_VERSION}.'
  'services.types.google_ads_service'
)


@dataclasses.dataclass(frozen=True)
class FieldPossibleValues:
  name: str
  values: set[str]


class BaseClient:
  """Base API client class.

  Attributes:
      api_version:
          Version of Google Ads API to use.
      google_ads_row:
          Proto message that contains all possible fields for a given
          API version.
  """

  _MAPPING = {
    'INT64': int,
    'FLOAT': float,
    'DOUBLE': float,
    'BOOL': bool,
  }

  def __init__(self, version: str = GOOGLE_ADS_API_VERSION):
    """Initializes client based on provided API version.

    Args:
        version: Version of Google Ads API to use.
    """
    self.api_version = (
      str(version) if str(version).startswith('v') else f'v{version}'
    )
    self.google_ads_row = self._get_google_ads_row(self.api_version)

  @property
  def _base_module(self) -> str:
    """Name of Google Ads module for a given API version."""
    return f'google.ads.googleads.{self.api_version}'

  @property
  def _common_types_module(self) -> str:
    """Name of module containing common data types."""
    return f'{self._base_module}.common.types'

  @property
  def _metrics(self) -> ModuleType:
    """Module containing metrics."""
    return importlib.import_module(f'{self._common_types_module}.metrics')

  @property
  def _segments(self) -> ModuleType:
    """Module containing segments."""
    return importlib.import_module(f'{self._common_types_module}.segments')

  def _get_google_ads_row(
    self, api_version: str
  ) -> google_ads_service.GoogleAdsRow:
    """Gets GoogleAdsRow for a given API version.

    Args:
        version: Version of Google Ads API to use.

    Returns:
        GoogleAdsRow proto message for a given API version.
    """
    google_ads_service = importlib.import_module(
      f'{self._base_module}.services.types.google_ads_service'
    )
    return google_ads_service.GoogleAdsRow()

  def infer_types(
    self, field_names: Sequence[str]
  ) -> list[FieldPossibleValues]:
    """Maps API fields to Python primitives.

    Args:
        field_names: Valid field names to be sent to Ads API.

    Returns:
        Possible values for each field.
    """

    output = []
    for field_name in field_names:
      try:
        descriptor = self._get_descriptor(field_name)
        values = self._get_possible_values_for_resource(descriptor)
        field = FieldPossibleValues(name=field_name, values=values)
      except (AttributeError, ModuleNotFoundError):
        field = FieldPossibleValues(
          name=field_name,
          values={
            '',
          },
        )
      output.append(field)
    return output

  def _get_descriptor(
    self, field: str
  ) -> protobuf.descriptor_pb2.FieldDescriptorProto:
    """Gets descriptor for specified field.

    Args:
        field: Valid field name to be sent to Ads API.

    Returns:
        FieldDescriptorProto for specified field.
    """
    resource, *sub_resource, base_field = field.split('.')
    base_field = 'type_' if base_field == 'type' else base_field
    target_resource = self._get_target_resource(resource, sub_resource)
    return target_resource.meta.fields.get(base_field).descriptor

  def _get_target_resource(
    self, resource: str, sub_resource: list[str] | None = None
  ) -> proto.message.Message:
    """Gets Proto message for specified resource and its sub-resources.

    Args:
        resource:
            Google Ads resource (campaign, ad_group, segments, etc.).
        sub_resource:
            Possible sub-resources (date for segments resource).

    Returns:
        Proto describing combination of resource and sub-resource.
    """
    if resource == 'metrics':
      target_resource = self._metrics.Metrics
    elif resource == 'segments':
      # If segment has name segments.something.something
      if sub_resource:
        target_resource = getattr(
          self._segments, f'{clean_resource(sub_resource[-1])}'
        )
      else:
        target_resource = getattr(self._segments, f'{clean_resource(resource)}')
    else:
      resource_module = importlib.import_module(
        f'{self._base_module}.resources.types.{resource}'
      )

      target_resource = getattr(resource_module, f'{clean_resource(resource)}')
      try:
        # If resource has name resource.something.something
        if sub_resource:
          target_resource = getattr(
            target_resource, f'{clean_resource(sub_resource[-1])}'
          )
      except AttributeError:
        try:
          resource_module = importlib.import_module(
            f'{self._base_module}.resources.types.{sub_resource[0]}'
          )
        except ModuleNotFoundError:
          resource_module = importlib.import_module(
            f'{self._common_types_module}.{sub_resource[0]}'
          )
        if len(sub_resource) > 1:
          if hasattr(resource_module, f'{clean_resource(sub_resource[1])}'):
            target_resource = getattr(
              resource_module, f'{clean_resource(sub_resource[-1])}'
            )
          else:
            resource_module = importlib.import_module(
              f'{self._common_types_module}.ad_type_infos'
            )

            target_resource = getattr(
              resource_module, f'{clean_resource(sub_resource[1])}Info'
            )
        else:
          target_resource = getattr(
            resource_module, f'{clean_resource(sub_resource[-1])}'
          )
    return target_resource

  def _get_possible_values_for_resource(
    self, descriptor: protobuf.descriptor_pb2.FieldDescriptorProto
  ) -> set:
    """Identifies possible values for a given descriptor or field_type.

    If descriptor's type is ENUM function gets all possible values for
    this Enum, otherwise the default value for descriptor type is taken
    (0 for int, '' for str, False for bool).

    Args:
        descriptor: FieldDescriptorProto for specified field.

    Returns:
        Possible values for a given descriptor.
    """
    if descriptor.type == 14:  # 14 stands for ENUM
      enum_class, enum = descriptor.type_name.split('.')[-2:]
      file_name = re.sub(r'(?<!^)(?=[A-Z])', '_', enum).lower()
      enum_resource = importlib.import_module(
        f'{self._base_module}.enums.types.{file_name}'
      )
      return {p.name for p in getattr(getattr(enum_resource, enum_class), enum)}

    field_type = self._MAPPING.get(
      proto.primitives.ProtoType(descriptor.type).name, str
    )
    default_value = field_type()
    return {
      default_value,
    }


class GoogleAdsApiClient(BaseClient):
  """Client to interact with Google Ads API.

  Attributes:
      default_google_ads_yaml: Default location for google-ads.yaml file.
      client: GoogleAdsClient to perform stream and mutate operations.
      ads_service: GoogleAdsService to perform stream operations.
  """

  default_google_ads_yaml = str(Path.home() / 'google-ads.yaml')

  def __init__(
    self,
    path_to_config: str | os.PathLike = default_google_ads_yaml,
    config_dict: dict[str, str] | None = None,
    yaml_str: str | None = None,
    version: str = GOOGLE_ADS_API_VERSION,
    use_proto_plus: bool = True,
  ) -> None:
    """Initializes GoogleAdsApiClient based on one of the methods.

    Args:
        path_to_config: Path to google-ads.yaml file.
        config_dict: A dictionary containing authentication details.
        yaml_str: Strings representation of google-ads.yaml.
        version: Ads API version.
        use_proto_plus: Whether to convert Enums to names in response.

    Raises:
        ValueError:
            When GoogleAdsClient cannot be instantiated due to missing
            credentials.
    """
    super().__init__(version)
    self.client = self._init_client(
      path=path_to_config, config_dict=config_dict, yaml_str=yaml_str
    )
    self.client.use_proto_plus = use_proto_plus
    self.ads_service = self.client.get_service('GoogleAdsService')

  @tenacity.retry(
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential(),
    retry=tenacity.retry_if_exception_type(
      google_exceptions.InternalServerError
    ),
    reraise=True,
  )
  def get_response(
    self, entity_id: int, query_text: str, query_title: str | None = None
  ) -> google_ads_service.SearchGoogleAdsStreamResponse:
    """Executes query for a given entity_id (customer_id).

    Args:
        entity_id: Google Ads customer_id.
        query_text: GAQL query text.
        query_title: Optional name of the query.

    Returns:
        SearchGoogleAdsStreamResponse for a given API version.

    Raises:
        google_exceptions.InternalServerError:
            When data cannot be fetched from Ads API.
    """

    return self.ads_service.search_stream(
      customer_id=entity_id, query=query_text
    )

  def _init_client(
    self, path: str, config_dict: dict[str, str], yaml_str: str
  ) -> googleads_client.GoogleAdsClient | None:
    """Initializes GoogleAdsClient based on one of the methods.

    Args:
        path: Path to google-ads.yaml file.
        config_dict: A dictionary containing authentication details.
        yaml_str: Strings representation of google-ads.yaml.

    Returns:
        Instantiated GoogleAdsClient;
        None if instantiation hasn't been done.

    Raises:
        ValueError:
            if google-ads.yaml wasn't found or environment does not
    """
    if config_dict:
      return googleads_client.GoogleAdsClient.load_from_dict(
        config_dict, self.api_version
      )
    if yaml_str:
      return googleads_client.GoogleAdsClient.load_from_string(
        yaml_str, self.api_version
      )
    if path:
      if os.path.isfile(path):
        return googleads_client.GoogleAdsClient.load_from_storage(
          path, self.api_version
        )
      try:
        with smart_open.open(path, 'r', encoding='utf-8') as f:
          google_ads_config_dict = yaml.safe_load(f)
        return googleads_client.GoogleAdsClient.load_from_dict(
          google_ads_config_dict, self.api_version
        )
      except google_exceptions.NotFound:
        raise ValueError(f'File {path} not found')
    try:
      return googleads_client.GoogleAdsClient.load_from_env(self.api_version)
    except ValueError:
      raise ValueError('Cannot instantiate GoogleAdsClient')


def clean_resource(resource: str) -> str:
  """Converts nested resource to a TitleCase format."""
  return resource.title().replace('_', '')
