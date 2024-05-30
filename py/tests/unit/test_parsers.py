# Copyright 2024 Google LLC
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
from __future__ import annotations

import dataclasses
import json

import proto
import pytest
from google.ads.googleads.v16.resources.types import (
  ad_group_ad_asset_view,
  change_event,
)

from gaarf import exceptions, parsers, query_editor


@dataclasses.dataclass
class FakeGoogleAdsRowElement:
  value: int
  text: str
  name: str


@dataclasses.dataclass
class TextAttribute:
  text: str


@dataclasses.dataclass
class NameAttribute:
  name: str


@dataclasses.dataclass
class AssetAttribute:
  name: str


@dataclasses.dataclass
class ValueAttribute:
  value: str


@dataclasses.dataclass
class Metric:
  clicks: int
  impressions: int


@dataclasses.dataclass(frozen=True)
class NestedResource:
  nested_field: str


@dataclasses.dataclass(frozen=True)
class FakeAdsRowMultipleElements:
  campaign_type: NameAttribute
  clicks: int
  resource: str
  value: str
  metrics: Metric
  old_resource: NestedResource
  new_resource: change_event.ChangeEvent.ChangedResource
  policy_summary: ad_group_ad_asset_view.AdGroupAdAssetPolicySummary

  @property
  def _pb(self):
    return self


@dataclasses.dataclass
class FakeQuerySpecification:
  customizers: dict[str, str]
  virtual_columns: dict[str, str]
  fields: list[str]
  column_names: list[str]


@pytest.fixture
def fake_query_specification():
  customizers = {
    'resource': {
      'type': 'resource_index',
      'value': 0,
    },
    'old_resource': {
      'type': 'nested_field',
      'value': 'nested_field',
    },
    'new_resource': {
      'type': 'nested_field',
      'value': 'campaign.target_cpa.target_cpa_micros',
    },
    'policy_summary': {
      'type': 'nested_field',
      'value': 'policy_topic_entries.type',
    },
    'approval_status': {
      'type': 'nested_field',
      'value': 'approval_status',
    },
  }
  virtual_columns = {
    'date': {
      'type': 'built-in',
      'value': 'date',
    }
  }
  return FakeQuerySpecification(
    customizers=customizers,
    virtual_columns=virtual_columns,
    fields=[
      'campaign_type',
      'clicks',
      'resource',
      'value',
      'old_resource',
      'new_resource',
      'policy_summary',
      'policy_summary',
    ],
    column_names=[
      'campaign_type',
      'clicks',
      'resource',
      'value',
      'old_resource',
      'new_resource',
      'policy_summary',
      'approval_status',
    ],
  )


@pytest.fixture
def fake_google_ads_row_element():
  return FakeGoogleAdsRowElement(1, '2', '3')


@pytest.fixture
def google_ads_row_parser(fake_query_specification):
  return parsers.GoogleAdsRowParser(fake_query_specification)


class FakeMessage(proto.Message):
  message = proto.Field(proto.STRING, number=1)


class TestResourceFormatter:
  def test_resource_formatter_get_resource(self):
    resource = parsers.ResourceFormatter.get_resource('name: id')
    assert resource == 'id'

  def test_resource_formatter_get_resource_id(self):
    resource = parsers.ResourceFormatter.get_resource_id(
      'customers/1/resource/2'
    )
    assert resource == '2'

  def test_resource_formatter_clear_resource_id_int(self):
    resource = parsers.ResourceFormatter.clean_resource_id('"1"')
    assert resource == 1

  def test_resource_formatter_clear_resource_id_str(self):
    resource = parsers.ResourceFormatter.clean_resource_id('"value"')
    assert resource == 'value'


class TestParser:
  @pytest.fixture
  def base_parser(self):
    return parsers.BaseParser(None)

  @pytest.fixture
  def attribute_parser(self, base_parser):
    return parsers.AttributeParser(base_parser)

  @pytest.fixture
  def empty_message_parser(self, base_parser):
    return parsers.EmptyMessageParser(base_parser)

  def test_base_parser_parse_returns_none(self, base_parser):
    assert base_parser.parse('') is None

  @pytest.mark.parametrize(
    'element,expected_value',
    [
      (
        NameAttribute('some-name'),
        'some-name',
      ),
      (
        TextAttribute('some-text'),
        'some-text',
      ),
      (
        AssetAttribute('some-asset'),
        'some-asset',
      ),
      (
        ValueAttribute(1),
        1,
      ),
      (
        '',
        None,
      ),
    ],
  )
  def test_attribute_parser_parser_returns_expected_value(
    self, attribute_parser, element, expected_value
  ):
    assert attribute_parser.parse(element) == expected_value

  @pytest.mark.parametrize(
    'element,expected_value',
    [
      (
        FakeMessage(message='test'),
        'Not set',
      ),
      (
        '',
        None,
      ),
    ],
  )
  def test_empty_message_parser_returns_expected_value(
    self, empty_message_parser, element, expected_value
  ):
    assert empty_message_parser.parse(element) == expected_value


class TestGoogleAdsRowParser:
  @pytest.fixture
  def fake_change_event(self):
    return change_event.ChangeEvent.ChangedResource.from_json(
      json.dumps({'campaign': {'target_cpa': {'target_cpa_micros': 1}}})
    )

  @pytest.fixture
  def fake_policy_summary(self):
    return ad_group_ad_asset_view.AdGroupAdAssetPolicySummary.from_json(
      json.dumps(
        {
          'approvalStatus': 'APPROVED',
          'policyTopicEntries': [{'type': 'LIMITED'}],
        }
      )
    )

  @pytest.fixture
  def fake_approval_status(self):
    return ad_group_ad_asset_view.AdGroupAdAssetPolicySummary.from_json(
      json.dumps({'approvalStatus': 'APPROVED'})
    )

  @pytest.fixture
  def fake_ads_row(
    self, fake_change_event, fake_policy_summary, fake_approval_status
  ):
    return FakeAdsRowMultipleElements(
      campaign_type=NameAttribute('SEARCH'),
      clicks=1,
      resource='customers/1/resource/2',
      value=ValueAttribute(1),
      metrics=Metric(clicks=10, impressions=10),
      old_resource=NestedResource('nested_value'),
      new_resource=fake_change_event,
      policy_summary=fake_policy_summary,
    )

  @pytest.fixture
  def fake_expression_virtual_column(self):
    return query_editor.VirtualColumn(
      type='expression',
      value='metrics.clicks / metrics.impressions',
      fields=[
        'metrics.clicks',
        'metrics.impressions',
      ],
      substitute_expression='{metrics_clicks} / {metrics_impressions}',
    )

  def test_google_ads_row_parser_return_last_parser_in_chain(
    self, google_ads_row_parser
  ):
    assert isinstance(
      google_ads_row_parser.parser_chain, parsers.RepeatedParser
    )

  def test_get_attributes_from_row_returns_correct_list(
    self,
    google_ads_row_parser,
    fake_ads_row,
    fake_change_event,
    fake_policy_summary,
  ):
    extracted_rows = google_ads_row_parser._get_attributes_from_row(
      fake_ads_row, google_ads_row_parser.row_getter
    )
    assert extracted_rows == (
      NameAttribute('SEARCH'),
      1,
      'customers/1/resource/2',
      ValueAttribute(1),
      NestedResource('nested_value'),
      fake_change_event,
      fake_policy_summary,
      fake_policy_summary,
    )
    assert google_ads_row_parser.parse_ads_row(fake_ads_row) == [
      'SEARCH',
      1,
      '2',
      1,
      'nested_value',
      1,
      ['LIMITED'],
      'APPROVED',
    ]

  def test_parse_ads_row_extracts_correct_resource_indices_from_array(
    self,
    google_ads_row_parser,
    fake_change_event,
    fake_policy_summary,
    fake_approval_status,
  ):
    fake_ads_row_with_array = FakeAdsRowMultipleElements(
      campaign_type=NameAttribute('SEARCH'),
      clicks=1,
      resource=[
        'customers/1/resource/1',
        'customers/1/resource/2',
      ],
      value=ValueAttribute(1),
      metrics=Metric(clicks=10, impressions=10),
      old_resource=NestedResource('nested_value'),
      new_resource=fake_change_event,
      policy_summary=fake_policy_summary,
    )
    assert google_ads_row_parser.parse_ads_row(fake_ads_row_with_array) == [
      'SEARCH',
      1,
      ['1', '2'],
      1,
      'nested_value',
      1,
      ['LIMITED'],
      'APPROVED',
    ]

  def test_parse_ads_row_extract_correct_resource_indices_from_array_of_attributes(  # pylint: disable=line-too-long
    self,
    google_ads_row_parser,
    fake_change_event,
    fake_policy_summary,
    fake_approval_status,
  ):
    fake_ads_row_with_array = FakeAdsRowMultipleElements(
      campaign_type=NameAttribute('SEARCH'),
      clicks=1,
      resource=[
        AssetAttribute('customers/1/resource/1'),
        AssetAttribute('customers/1/resource/2'),
      ],
      value=ValueAttribute(1),
      metrics=Metric(clicks=10, impressions=10),
      old_resource=NestedResource('nested_value'),
      new_resource=fake_change_event,
      policy_summary=fake_policy_summary,
    )
    assert google_ads_row_parser.parse_ads_row(fake_ads_row_with_array) == [
      'SEARCH',
      1,
      ['1', '2'],
      1,
      'nested_value',
      1,
      ['LIMITED'],
      'APPROVED',
    ]

  def test_convert_virtual_column_returns_correct_value_for_builtin_type(
    self, google_ads_row_parser, fake_ads_row
  ):
    fake_builtin_virtual_column = query_editor.VirtualColumn(
      type='built-in', value='fake_value'
    )
    result = google_ads_row_parser._convert_virtual_column(
      fake_ads_row, fake_builtin_virtual_column
    )
    assert result == 'fake_value'

  def test_convert_virtual_column_returns_correct_value_for_expression(
    self, google_ads_row_parser, fake_ads_row, fake_expression_virtual_column
  ):
    result = google_ads_row_parser._convert_virtual_column(
      fake_ads_row, fake_expression_virtual_column
    )
    assert result == 1.0

  def test_convert_virtual_column_returns_zero_for_expression_with_zero_in_denominator(  # pylint: disable=line-too-long
    self, google_ads_row_parser, fake_ads_row, fake_expression_virtual_column
  ):
    fake_ads_row.metrics.impressions = 0
    result = google_ads_row_parser._convert_virtual_column(
      fake_ads_row, fake_expression_virtual_column
    )
    assert result == 0

  def test_convert_virtual_column_raises_virtual_column_exception_on_incorrect_type(  # pylint: disable=line-too-long
    self, google_ads_row_parser, fake_ads_row, fake_expression_virtual_column
  ):
    fake_ads_row.metrics.impressions = 'str'
    with pytest.raises(exceptions.GaarfVirtualColumnException):
      google_ads_row_parser._convert_virtual_column(
        fake_ads_row, fake_expression_virtual_column
      )

  def test_convert_virtual_column_returns_virtual_column_value_on_incorrect_expression(  # pylint: disable=line-too-long
    self, google_ads_row_parser, fake_ads_row, fake_expression_virtual_column
  ):
    fake_ads_row.metrics.impressions = '0 +'
    result = google_ads_row_parser._convert_virtual_column(
      fake_ads_row, fake_expression_virtual_column
    )
    assert result == fake_expression_virtual_column.value

  def test_convert_virtual_column_raises_virtual_column_exception_on_incorrect_type(  # pylint: disable=line-too-long
    self, google_ads_row_parser, fake_ads_row
  ):
    virtual_column = query_editor.VirtualColumn(
      type='non-existing-type',
      value='metrics.clicks / metrics.impressions',
      fields=[
        'metrics.clicks',
        'metrics.impressions',
      ],
      substitute_expression='{metrics_clicks} / {metrics_impressions}',
    )
    with pytest.raises(exceptions.GaarfVirtualColumnException):
      google_ads_row_parser._convert_virtual_column(
        fake_ads_row, virtual_column
      )

  def test_parse_ads_row_with_extract_protobufs_returns_correct_results(
    self, google_ads_row_parser, fake_ads_row
  ):
    google_ads_row_parser.extract_protobufs = True
    assert google_ads_row_parser.parse_ads_row(fake_ads_row) == [
      'SEARCH',
      1,
      '2',
      1,
      'nested_value',
      1,
      ['LIMITED'],
      'APPROVED',
    ]
