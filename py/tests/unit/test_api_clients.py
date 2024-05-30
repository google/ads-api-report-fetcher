# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import pytest
import tenacity
from google.api_core import exceptions as google_exceptions

from gaarf import api_clients


class TestBaseClient:
  @pytest.fixture
  def client(self):
    return api_clients.BaseClient()

  @pytest.mark.parametrize(
    'field_name, possible_values',
    [
      (
        'campaign.name',
        {
          '',
        },
      ),
      (
        'campaign.id',
        {
          0,
        },
      ),
      (
        'metrics.clicks',
        {
          0,
        },
      ),
      (
        'metrics.conversions',
        {
          0.0,
        },
      ),
      (
        'ad_group_ad.ad.video_responsive_ad.videos.asset',
        {
          '',
        },
      ),
      (
        'ad_group_ad.policy_summary.policy_topic_entries',
        {
          '',
        },
      ),
    ],
  )
  def test_infer_types_returns_correct_field_possible_values_for_primitives(
    self, client, field_name, possible_values
  ):
    field_possible_values = client.infer_types(
      field_names=[
        field_name,
      ]
    )
    expected_value = [
      api_clients.FieldPossibleValues(name=field_name, values=possible_values),
    ]
    assert field_possible_values == expected_value

  @pytest.mark.parametrize(
    'field_name, possible_values',
    [
      (
        'segments.device',
        {
          'CONNECTED_TV',
          'DESKTOP',
          'MOBILE',
          'OTHER',
          'TABLET',
          'UNKNOWN',
          'UNSPECIFIED',
        },
      ),
      (
        'ad_group_ad.policy_summary.approval_status',
        {
          'APPROVED',
          'APPROVED_LIMITED',
          'AREA_OF_INTEREST_ONLY',
          'DISAPPROVED',
          'UNKNOWN',
          'UNSPECIFIED',
        },
      ),
    ],
  )
  def test_infer_types_returns_correct_field_possible_values_for_enums(
    self, client, field_name, possible_values
  ):
    field_possible_values = client.infer_types(
      field_names=[
        field_name,
      ]
    )
    expected_value = [
      api_clients.FieldPossibleValues(name=field_name, values=possible_values),
    ]
    assert field_possible_values == expected_value

  def test_get_target_resource_for_metrics_returns_metrics(self, client):
    target_resource = client._get_target_resource('metrics')
    assert target_resource == client._metrics.Metrics

  def test_get_target_resource_for_segment_without_subresource_returns_segments(
    self, client
  ):
    target_resource = client._get_target_resource('segments')
    assert target_resource == client._segments.Segments

  def test_get_target_resource_for_segment_with_subresource_returns_segments(
    self, client
  ):
    sub_resource = 'asset_interaction_target'
    target_resource = client._get_target_resource(
      'segments',
      [
        sub_resource,
      ],
    )
    expected_resource = getattr(
      client._segments, api_clients.clean_resource(sub_resource)
    )
    assert target_resource == expected_resource

  @pytest.mark.skip('Implement')
  def test_get_target_resource_for_segment_with_nested_subresource_returns_segments(
    self, client
  ):
    sub_resource = 'discovery_carousel_card_asset'
    target_resource = client._get_target_resource(
      'asset',
      [
        sub_resource,
      ],
    )
    expected_resource = getattr(
      client._segments, api_clients.clean_resource(sub_resource)
    )
    assert target_resource == expected_resource

  def test_get_decriptor_returns_correct_attribute_name(self, client):
    field = 'segments.date'
    descriptor = client._get_descriptor(field)
    assert descriptor.name == 'date'

  @pytest.mark.parametrize(
    'field_name, value',
    [
      (
        'campaign.name',
        '',
      ),
      (
        'metrics.clicks',
        0,
      ),
      (
        'metrics.conversions_value',
        0.0,
      ),
      (
        'ad_group_ad.ad.added_by_google_ads',
        False,
      ),
    ],
  )
  def test_get_possible_values_for_resource_returns_default_value_for_primitives(
    self, client, field_name, value
  ):
    descriptor = client._get_descriptor(field_name)
    default_value = client._get_possible_values_for_resource(descriptor)
    expected_value = {
      value,
    }
    assert default_value == expected_value

  def test_get_possible_values_for_resource_returns_default_value_for_enums(
    self, client
  ):
    descriptor = client._get_descriptor('segments.device')
    default_value = client._get_possible_values_for_resource(descriptor)
    expected_values = {
      'CONNECTED_TV',
      'DESKTOP',
      'MOBILE',
      'OTHER',
      'TABLET',
      'UNKNOWN',
      'UNSPECIFIED',
    }
    assert default_value == expected_values


class TestGoogleAdsApiClient:
  @pytest.fixture
  def test_client(self, mocker, config_path):
    mocker.patch('google.ads.googleads.client.oauth2', return_value=[])
    mocker.patch(
      f'google.ads.googleads.{api_clients.GOOGLE_ADS_API_VERSION}'
      '.services.services.google_ads_service.GoogleAdsServiceClient'
      '.search_stream',
      side_effect=[
        google_exceptions.InternalServerError('test'),
        google_exceptions.InternalServerError('test'),
        google_exceptions.InternalServerError('test'),
      ],
    )
    return api_clients.GoogleAdsApiClient(path_to_config=config_path)

  def test_get_response_raises_internal_service_error_after_3_failed_retries(
    self, test_client, mocker
  ):
    test_client.get_response.retry.wait = tenacity.wait_none()
    with pytest.raises(google_exceptions.InternalServerError):
      test_client.get_response(
        entity_id='1',
        query_text='SELECT customer.id FROM customer',
        query_title='test',
      )
