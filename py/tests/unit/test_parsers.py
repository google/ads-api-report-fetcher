import pytest
from dataclasses import dataclass
import gaarf.parsers as parsers


@dataclass
class FakeGoogleAdsRowElement:
    value: int
    text: str
    name: str


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
