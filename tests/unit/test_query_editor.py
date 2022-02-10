import pytest
import runner.query_editor as query_editor


@pytest.fixture
def sample_query():
    return query_editor.get_query_elements("tests/sql/test_query.sql")


def test_extract_correct_aliases(sample_query):
    assert sample_query.column_names == [
        "customer.id", "campaign_type", "campaign", "ad_group", "ad"
    ]


@pytest.mark.skip(reason="WIP")
def test_extract_correct_text(sample_query):
    assert sample_query.query_text == "SELECT customer.id, campaign.id FROM campaign"


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
