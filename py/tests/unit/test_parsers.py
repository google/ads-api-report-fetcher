from __future__ import annotations

import dataclasses
from typing import Any

import proto
import pytest
from gaarf import parsers
from gaarf import query_editor


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


@dataclasses.dataclass
class FakeAdsRowMultipleElements:
    campaign_type: NameAttribute
    clicks: int
    resource: str
    value: str
    metrics: Metric


@dataclasses.dataclass
class FakeQuerySpecification:
    customizers: dict[str, Any]
    virtual_columns: dict[str, Any]
    fields: list[str]
    column_names: list[str]


@pytest.fixture
def fake_query_specification():
    customizers = {
        'resource': {
            'type': 'resource_index',
            'value': 0,
        },
        'value': {
            'type': 'nested_field',
            'value': 'value',
        }
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
        ],
        column_names=[
            'campaign_type',
            'clicks',
            'resource',
            'value',
        ])


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
            'customers/1/resource/2')
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

    @pytest.mark.parametrize('element,expected_value', [
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
    ])
    def test_attribute_parser_parser_returns_expected_value(
            self, attribute_parser, element, expected_value):
        assert attribute_parser.parse(element) == expected_value

    @pytest.mark.parametrize('element,expected_value', [
        (
            FakeMessage(message='test'),
            'Not set',
        ),
        (
            '',
            None,
        ),
    ])
    def test_empty_message_parser_returns_expected_value(
            self, empty_message_parser, element, expected_value):
        assert empty_message_parser.parse(element) == expected_value


class TestGoogleAdsRowParser:

    @pytest.fixture
    def fake_ads_row(self):
        return FakeAdsRowMultipleElements(
            campaign_type=NameAttribute('SEARCH'),
            clicks=1,
            resource='customers/1/resource/2',
            value=ValueAttribute(1),
            metrics=Metric(clicks=10, impressions=10))

    @pytest.fixture
    def fake_expression_virtual_column(self):
        return query_editor.VirtualColumn(
            type='expression',
            value='metrics.clicks / metrics.impressions',
            fields=[
                'metrics.clicks',
                'metrics.impressions',
            ],
            substitute_expression='{metrics_clicks} / {metrics_impressions}')

    def test_google_ads_row_parser_return_last_parser_in_chain(
            self, google_ads_row_parser):
        assert isinstance(google_ads_row_parser.parser_chain,
                          parsers.RepeatedParser)

    def test_get_attributes_from_row_returns_correct_list(
            self, google_ads_row_parser, fake_ads_row):
        extracted_rows = google_ads_row_parser._get_attributes_from_row(
            fake_ads_row, google_ads_row_parser.row_getter)
        assert extracted_rows == (
            NameAttribute('SEARCH'),
            1,
            'customers/1/resource/2',
            ValueAttribute(1),
        )
        assert google_ads_row_parser.parse_ads_row(fake_ads_row) == [
            'SEARCH', 1, '2', 1
        ]

    def test_parse_ads_row_extracts_correct_resource_indices_from_array(
            self, google_ads_row_parser):
        fake_ads_row_with_array = FakeAdsRowMultipleElements(
            campaign_type=NameAttribute('SEARCH'),
            clicks=1,
            resource=[
                'customers/1/resource/1',
                'customers/1/resource/2',
            ],
            value=ValueAttribute(1),
            metrics=Metric(clicks=10, impressions=10))
        assert google_ads_row_parser.parse_ads_row(fake_ads_row_with_array) == [
            'SEARCH', 1, ['1', '2'], 1
        ]

    def test_parse_ads_row_extract_correct_resource_indices_from_array_of_attributes(  # pylint: disable=line-too-long
            self, google_ads_row_parser):
        fake_ads_row_with_array = FakeAdsRowMultipleElements(
            campaign_type=NameAttribute('SEARCH'),
            clicks=1,
            resource=[
                AssetAttribute('customers/1/resource/1'),
                AssetAttribute('customers/1/resource/2'),
            ],
            value=ValueAttribute(1),
            metrics=Metric(clicks=10, impressions=10))
        assert google_ads_row_parser.parse_ads_row(fake_ads_row_with_array) == [
            'SEARCH', 1, ['1', '2'], 1
        ]

    def test_convert_virtual_column_returns_correct_value_for_builtin_type(
            self, google_ads_row_parser, fake_ads_row):
        fake_builtin_virtual_column = query_editor.VirtualColumn(
            type='built-in', value='fake_value')
        result = google_ads_row_parser._convert_virtual_column(
            fake_ads_row, fake_builtin_virtual_column)
        assert result == 'fake_value'

    def test_convert_virtual_column_returns_correct_value_for_expression(
            self, google_ads_row_parser, fake_ads_row,
            fake_expression_virtual_column):
        result = google_ads_row_parser._convert_virtual_column(
            fake_ads_row, fake_expression_virtual_column)
        assert result == 1.0

    def test_convert_virtual_column_returns_zero_for_expression_with_zero_in_denominator(  # pylint: disable=line-too-long
            self, google_ads_row_parser, fake_ads_row,
            fake_expression_virtual_column):
        fake_ads_row.metrics.impressions = 0
        result = google_ads_row_parser._convert_virtual_column(
            fake_ads_row, fake_expression_virtual_column)
        assert result == 0

    def test_convert_virtual_column_raises_virtual_column_error_on_incorrect_type(  # pylint: disable=line-too-long
            self, google_ads_row_parser, fake_ads_row,
            fake_expression_virtual_column):
        fake_ads_row.metrics.impressions = 'str'
        with pytest.raises(query_editor.VirtualColumnError):
            google_ads_row_parser._convert_virtual_column(
                fake_ads_row, fake_expression_virtual_column)

    def test_convert_virtual_column_returns_virtual_column_value_on_incorrect_expression(  # pylint: disable=line-too-long
            self, google_ads_row_parser, fake_ads_row,
            fake_expression_virtual_column):
        fake_ads_row.metrics.impressions = '0 +'
        result = google_ads_row_parser._convert_virtual_column(
            fake_ads_row, fake_expression_virtual_column)
        assert result == fake_expression_virtual_column.value

    def test_convert_virtual_column_raises_value_error_on_incorrect_type(  # pylint: disable=line-too-long
            self, google_ads_row_parser, fake_ads_row):
        virtual_column = query_editor.VirtualColumn(
            type='non-existing-type',
            value='metrics.clicks / metrics.impressions',
            fields=[
                'metrics.clicks',
                'metrics.impressions',
            ],
            substitute_expression='{metrics_clicks} / {metrics_impressions}')
        with pytest.raises(ValueError):
            google_ads_row_parser._convert_virtual_column(
                fake_ads_row, virtual_column)
