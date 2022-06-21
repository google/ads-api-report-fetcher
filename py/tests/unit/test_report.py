import pytest
from gaarf.report import GaarfReport, GaarfRow, GaarfIterator


@pytest.fixture
def single_column_report():
    return GaarfReport(results=[[1], [2], [3]], column_names=["campaign_id"])


@pytest.fixture
def multi_column_report():
    return GaarfReport(results=[[1, 2], [2, 3], [3, 4]],
                       column_names=["campaign_id", "ad_group_id"])


def test_single_column_report_return_sequence(single_column_report):
    assert [row for row in single_column_report] == [1, 2, 3]


def test_multi_column_report_returns_gaarf_row(multi_column_report):
    results = [row for row in multi_column_report]
    assert isinstance(results[0], GaarfRow)


def test_multi_column_report_support_iteration_with_gaarf_iterator(multi_column_report):
    assert isinstance(iter(multi_column_report), GaarfIterator)


def test_multi_column_report_get_element_by_id(multi_column_report):
    assert [row[0] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_by_name(multi_column_report):
    assert [row["campaign_id"] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_with_get_method(multi_column_report):
    assert [row.get("campaign_id") for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_as_attribute(multi_column_report):
    assert [row.campaign_id for row in multi_column_report] == [1, 2, 3]
