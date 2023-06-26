import pytest
import datetime
import gaarf.query_editor as query_editor


@pytest.fixture
def query_specification():
    query = """
-- Comment
# Comment
// Comment

SELECT
    1 AS constant,
    '2023-01-01' AS date,
    '{current_date}' AS current_date,
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


def test_correct_title(sample_query):
    assert sample_query.query_title == "sample_query"


def test_extract_correct_fields(sample_query):
    assert sample_query.fields == [
        "customer.id", "campaign.bidding_strategy_type", "campaign.id",
        "ad_group.id", "ad_group_ad.ad.id", "campaign.selective_optimization"
    ]


def test_extract_correct_aliases(sample_query):
    assert sample_query.column_names == [
        "constant", "date", "current_date", "ctr", "customer_id", "campaign_type",
        "campaign", "ad_group", "ad", "cost", "selective_optimization"
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
        "constant":
        query_editor.VirtualColumn(type="built-in", value=1),
        "date":
        query_editor.VirtualColumn(type="built-in", value="2023-01-01"),
        "current_date":
        query_editor.VirtualColumn(
            type="built-in", value=datetime.date.today().strftime("%Y-%m-%d")),
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


def test_incorrect_specification_raises_macro_error():
    query = "SELECT '${custom_field}', ad_group.id FROM ad_group_id"
    spec = query_editor.QuerySpecification(title="sample_query",
                                           text=query,
                                           args=None)
    with pytest.raises(query_editor.MacroError):
        spec.generate()


def test_incorrect_specification_raises_virtual_column_error():
    query = "SELECT 1, ad_group.id AS ad_group_id FROM ad_group_id"
    spec = query_editor.QuerySpecification(title="sample_query",
                                           text=query,
                                           args=None)
    with pytest.raises(query_editor.VirtualColumnError):
        spec.generate()

def test_incorrect_field_raises_value_error():
    query = "SELECT metric.impressions, ad_group.id FROM ad_group_id"
    spec = query_editor.QuerySpecification(title="sample_query",
                                           text=query,
                                           args=None)
    with pytest.raises(query_editor.FieldError):
        spec.generate()
