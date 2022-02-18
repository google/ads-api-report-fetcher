import pytest
import runner.query_editor as query_editor


@pytest.fixture
def ads_query_editor():
    query = """
-- Comment
# Comment
// Comment

SELECT
	customer.id, --customer_id
	campaign.type AS campaign_type, campaign.id:nested AS campaign,
	ad_group.id~1 AS ad_group,
	ad_group_ad.id->asset AS ad,
from ad_group_ad
"""
    return query_editor.AdsQueryEditor(query)


@pytest.fixture
def sample_query(ads_query_editor):
    return ads_query_editor.get_query_elements()


def test_extract_correct_fields(sample_query):
    assert sample_query.fields == [
        "customer.id", "campaign.type_", "campaign.id", "ad_group.id",
        "ad_group_ad.id"
    ]


def test_extract_correct_aliases(sample_query):
    assert sample_query.column_names == [
        "customer_id", "campaign_type", "campaign", "ad_group", "ad"
    ]


def test_extract_correct_text(sample_query):
    assert sample_query.query_text.lower(
    ) == "select customer.id, campaign.type, campaign.id, ad_group.id, ad_group_ad.id from ad_group_ad"


def test_extract_custom_callers(sample_query):
    assert sample_query.customizers == {
        2: {
            "type": "nested_field",
            "value": "nested"
        },
        3: {
            "type": "resource_index",
            "value": 1
        },
        4: {
            "type": "pointer",
            "value": "asset"
        }
    }


def test_format_query(ads_query_editor, sample_query):
    formatted_query = ads_query_editor.extract_query_lines(
        sample_query.query_text)
    assert formatted_query == [
        "customer.id", "campaign.type", "campaign.id", "ad_group.id",
        "ad_group_ad.id"
    ]


def test_extract_correct_resource(sample_query):
    assert sample_query.resource_name == "ad_group_ad"
