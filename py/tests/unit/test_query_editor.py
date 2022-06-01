import pytest
import gaarf.query_editor as query_editor


@pytest.fixture
def query_specification():
    query = """
-- Comment
# Comment
// Comment

SELECT
    customer.id, --customer_id
    campaign.type AS campaign_type, campaign.id:nested AS campaign,
    ad_group.id~1 AS ad_group,
    ad_group_ad.id->asset AS ad,
    campaign.selective_optimization AS selective_optimization,
from ad_group_ad
"""
    return query_editor.QuerySpecification(title="sample_query",
                                           text=query,
                                           args=None)


@pytest.fixture
def sample_query(query_specification):
    return query_specification.generate()


def test_correct_title(sample_query):
    assert sample_query.query_title == "sample_query"


def test_extract_correct_fields(sample_query):
    assert sample_query.fields == [
        "customer.id", "campaign.type_", "campaign.id", "ad_group.id",
        "ad_group_ad.id", "campaign.selective_optimization"
    ]


def test_extract_correct_aliases(sample_query):
    assert sample_query.column_names == [
        "customer_id", "campaign_type", "campaign", "ad_group", "ad",
        "selective_optimization"
    ]


def test_extract_correct_text(sample_query):
    assert sample_query.query_text.lower(
    ) == "select customer.id, campaign.type, campaign.id, ad_group.id, ad_group_ad.id, campaign.selective_optimization from ad_group_ad"


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


def test_format_query(query_specification, sample_query):
    formatted_query = query_specification.extract_query_lines(
        sample_query.query_text)
    assert formatted_query == [
        "customer.id", "campaign.type", "campaign.id", "ad_group.id",
        "ad_group_ad.id", "campaign.selective_optimization"
    ]


def test_extract_correct_resource(sample_query):
    assert sample_query.resource_name == "ad_group_ad"


def test_is_constant_resource(sample_query):
    assert sample_query.is_constant_resource == False
