import pytest
from gaarf.bq_executor import extract_datasets, expand_jinja


def test_extract_datasets():
    macros = {
        "start_date": ":YYYYMMDD",
        "bq_dataset": "dataset_1",
        "dataset_new": "dataset_2",
        "legacy_dataset_old": "dataset_3",
        "wrong_dts": "dataset_4"
    }

    expected = ["dataset_1", "dataset_2", "dataset_3"]
    datasets = extract_datasets(macros)
    assert datasets == expected


@pytest.fixture
def templated_query():
    return "SELECT field_one, {% for day in cohort_days %} {{day}} AS day_{{day}}, {% endfor %} FROM some_table"


def test_expand_jinja_if_template_parameters_are_provided(templated_query):
    expected_query = "SELECT field_one, 1 AS day_1, 2 AS day_2, FROM some_table"
    rendered_query= expand_jinja(templated_query, cohort_days="1,2")
    assert rendered_query.replace("  ", " ") == expected_query


def test_expand_jinja_if_empty_template_parameter_is_provided(templated_query):
    expected_query = "SELECT field_one, FROM some_table"
    rendered_query= expand_jinja(templated_query, cohort_days=None)
    assert rendered_query.replace("  ", " ") == expected_query


def test_expand_jinja_not_template_parameter_is_provided(templated_query):
    expected_query = "SELECT field_one, FROM some_table"
    rendered_query = expand_jinja(templated_query)
    assert rendered_query.replace("  ", " ") == expected_query
