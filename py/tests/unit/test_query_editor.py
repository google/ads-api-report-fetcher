import pytest
import gaarf.query_editor as query_editor


@pytest.fixture
def query_specification():
    query = """
-- Comment
# Comment
// Comment

SELECT
    '20230101' AS date,
    metrics.clicks / metrics.impressions AS ctr,
    customer.id, --customer_id
    campaign.bidding_strategy_type AS campaign_type, campaign.id:nested AS campaign,
    ad_group.id~1 AS ad_group,
    ad_group_ad.ad.id->asset AS ad,
    metrics.cost_micros * 1e6 AS cost,
    campaign.selective_optimization AS selective_optimization,
from ad_group_ad
"""
    return query_editor.QuerySpecification(title="sample_query",
                                           text=query,
                                           args=None)


@pytest.fixture
def sample_query(query_specification):
    return query_specification.generate()


@pytest.fixture
def incorrect_query_specification():
    query = """
SELECT
    "${custom_field}",
    ad_group.id AS ad_group_id
FROM ad_group_id
"""
    return query_editor.QuerySpecification(title="sample_query",
                                           text=query,
                                           args=None)


def test_correct_title(sample_query):
    assert sample_query.query_title == "sample_query"


def test_extract_correct_fields(sample_query):
    assert sample_query.fields == [
        "customer.id", "campaign.bidding_strategy_type", "campaign.id",
        "ad_group.id", "ad_group_ad.ad.id", "campaign.selective_optimization"
    ]


def test_extract_correct_aliases(sample_query):
    assert sample_query.column_names == [
        "date", "ctr", "customer_id", "campaign_type", "campaign", "ad_group",
        "ad", "cost", "selective_optimization"
    ]


def test_extract_correct_text(sample_query):
    assert sample_query.query_text.lower(
    ) == "select customer.id, campaign.bidding_strategy_type, campaign.id, ad_group.id, ad_group_ad.ad.id, campaign.selective_optimization, metrics.clicks, metrics.impressions, metrics.cost_micros from ad_group_ad"


def test_extract_custom_callers(sample_query):
    assert sample_query.customizers == {
        "campaign": {
            "type": "nested_field",
            "value": "nested"
        },
        "ad_group": {
            "type": "resource_index",
            "value": 1
        },
        "ad": {
            "type": "pointer",
            "value": "asset"
        }
    }


def test_extract_query_lines(query_specification, sample_query):
    extracted_lines = query_specification.extract_query_lines(
        sample_query.query_text)
    assert extracted_lines == [
        "customer.id", "campaign.bidding_strategy_type", "campaign.id",
        "ad_group.id", "ad_group_ad.ad.id", "campaign.selective_optimization",
        "metrics.clicks", "metrics.impressions", "metrics.cost_micros"
    ]


def test_extract_correct_resource(sample_query):
    assert sample_query.resource_name == "ad_group_ad"


def test_is_constant_resource(sample_query):
    assert sample_query.is_constant_resource == False


def test_has_virtual_columns(sample_query):
    assert sample_query.virtual_columns == {
        "date":
        query_editor.VirtualColumn(type="built-in", value="'20230101'"),
        "ctr":
        query_editor.VirtualColumn(
            type="expression",
            value="metrics.clicks / metrics.impressions",
            fields=["metrics.clicks", "metrics.impressions"],
            substitute_expression="{metrics_clicks} / {metrics_impressions}"),
        "cost":
        query_editor.VirtualColumn(
            type="expression",
            value="metrics.cost_micros * 1e6",
            fields=["metrics.cost_micros"],
            substitute_expression="{metrics_cost_micros} * 1e6")
    }


def test_incorrect_specification_raises_value_error(
        incorrect_query_specification):
    with pytest.raises(ValueError):
        incorrect_query_specification.generate()
