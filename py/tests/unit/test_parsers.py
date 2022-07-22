import pytest
from typing import Any, Dict, List
from dataclasses import dataclass
from proto import Message
import gaarf.parsers as parsers


@dataclass
class FakeGoogleAdsRowElement:
    value: int
    text: str
    name: str


@dataclass
class TextAttribute:
    text: str


@dataclass
class NameAttribute:
    name: str


@dataclass
class ValueAttribute:
    value: str


@dataclass
class FakeAdsRowMultipleElements:
    campaign_type: NameAttribute
    clicks: int
    resource: str
    value: str


@dataclass
class FakeQuerySpecification:
    customizers: Dict[str, Any]
    fields: List[str]


@pytest.fixture
def fake_google_ads_row_element():
    return FakeGoogleAdsRowElement(1, "2", "3")


@pytest.fixture
def google_ads_row_parser():
    return parsers.GoogleAdsRowParser()


def test_google_ads_row_parser_return_last_parser_in_chain(
        google_ads_row_parser):
    assert type(google_ads_row_parser.parser) == parsers.RepeatedParser


def test_google_ads_row_parser_chooses_correct_element_parser(
        google_ads_row_parser, fake_google_ads_row_element):
    assert google_ads_row_parser.parse(fake_google_ads_row_element) == "3"


def test_resource_formatter_get_resource():
    resource = parsers.ResourceFormatter.get_resource("name: id")
    assert resource == "id"


def test_resource_formatter_get_resource_id():
    resource = parsers.ResourceFormatter.get_resource_id(
        "customers/1/resource/2")
    assert resource == "2"


def test_resource_formatter_clear_resource_id_int():
    resource = parsers.ResourceFormatter.clean_resource_id('"1"')
    assert resource == 1


def test_resource_formatter_clear_resource_id_str():
    resource = parsers.ResourceFormatter.clean_resource_id('"value"')
    assert resource == "value"


@pytest.fixture
def base_parser():
    return parsers.BaseParser(None)


def test_base_parser(base_parser):
    assert base_parser.parse("") == None


@pytest.fixture
def text_attribute():
    return TextAttribute("some-text")


@pytest.fixture
def name_attribute():
    return NameAttribute("some-name")


@pytest.fixture
def value_attribute():
    return ValueAttribute(1)


def test_attribute_parser(base_parser, text_attribute, name_attribute,
                          value_attribute):
    attribute_parser = parsers.AttributeParser(base_parser)
    assert attribute_parser.parse("") == None
    assert attribute_parser.parse(name_attribute) == "some-name"
    assert attribute_parser.parse(text_attribute) == "some-text"
    assert attribute_parser.parse(value_attribute) == 1


@pytest.mark.skip("WIP")
def test_empty_attribute_parser(base_parser):
    empty_attribute_parser = parsers.EmptyAttributeParser(base_parser)
    assert empty_attribute_parser.parse("") == None
    message = Message()
    assert empty_attribute_parser.parse(message) == "Not set"


def test_google_ads_row_parser(google_ads_row_parser, text_attribute,
                               name_attribute, value_attribute):
    assert isinstance(google_ads_row_parser.parser, parsers.RepeatedParser)
    assert google_ads_row_parser.parse(name_attribute) == "some-name"
    assert google_ads_row_parser.parse(text_attribute) == "some-text"
    assert google_ads_row_parser.parse(value_attribute) == 1
    assert google_ads_row_parser.parse("some-value") == None


@pytest.fixture
def fake_ads_row():
    return FakeAdsRowMultipleElements(NameAttribute("SEARCH"), 1,
                                      "customers/1/resource/2",
                                      ValueAttribute(1))


@pytest.fixture
def fake_query_specification():
    customizers = {
        2: {
            "type": "resource_index",
            "value": 0
        },
        3: {
            "type": "nested_field",
            "value": "value"
        }
    }
    return FakeQuerySpecification(
        customizers, ["campaign_type", "clicks", "resource", "value"])


def test_get_attributes_from_row(google_ads_row_parser, fake_ads_row,
                                 fake_query_specification):
    extracted_rows = google_ads_row_parser._get_attributes_from_row(
        fake_ads_row, ["campaign_type", "clicks", "resource", "value"])
    assert extracted_rows == (NameAttribute("SEARCH"), 1,
                              "customers/1/resource/2", ValueAttribute(1))
    assert google_ads_row_parser.parse_ads_row(
        fake_ads_row, fake_query_specification) == ["SEARCH", 1, "2", 1]
