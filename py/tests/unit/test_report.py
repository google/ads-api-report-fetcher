import pytest
import pandas as pd
from gaarf.report import GaarfReport, GaarfRow, GaarfIterator


@pytest.fixture
def single_element_report():
    return GaarfReport(results=[1], column_names=["campaign_id"])


@pytest.fixture
def single_column_report():
    return GaarfReport(results=[[1], [2], [3]], column_names=["campaign_id"])


@pytest.fixture
def multi_column_report():
    return GaarfReport(results=[[1, 2], [2, 3], [3, 4]],
                       column_names=["campaign_id", "ad_group_id"])


def test_single_element_report_returns_sequence(single_element_report):
    assert [row for row in single_element_report] == [1]


def test_single_column_report_returns_sequence(single_column_report):
    assert [row for row in single_column_report] == [1, 2, 3]


def test_multi_column_report_returns_gaarf_row(multi_column_report):
    results = [row for row in multi_column_report]
    assert isinstance(results[0], GaarfRow)


def test_multi_column_report_support_iteration_with_gaarf_iterator(
        multi_column_report):
    assert isinstance(iter(multi_column_report), GaarfIterator)


def test_multi_column_report_get_element_by_id(multi_column_report):
    assert [row[0] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_by_name(multi_column_report):
    assert [row["campaign_id"] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_with_get_method(multi_column_report):
    assert [row.get("campaign_id") for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_as_attribute(multi_column_report):
    assert [row.campaign_id for row in multi_column_report] == [1, 2, 3]


def test_getitem_raise_index_error(multi_column_report):
    with pytest.raises(IndexError):
        [row[99] for row in multi_column_report] == [1, 2, 3]


def test_get_return_non_value(multi_column_report):
    assert [row.get("missing_value")
            for row in multi_column_report] == [None, None, None]


def test_convert_report_to_pandas(multi_column_report):
    expected = pd.DataFrame(data=[[1, 2], [2, 3], [3, 4]],
                            columns=["campaign_id", "ad_group_id"])
    assert multi_column_report.to_pandas().equals(expected)


def test_get_report_length(multi_column_report):
    assert len(multi_column_report) == 3


def test_report_str(single_element_report):
    assert str(single_element_report) == "[1]"
