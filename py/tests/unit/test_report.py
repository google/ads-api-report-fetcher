import pytest
import pandas as pd
from collections import abc
from gaarf.report import GaarfReport, GaarfRow


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
    assert isinstance(iter(multi_column_report), abc.Iterable)


def test_multi_column_report_get_element_by_id(multi_column_report):
    assert [row[0] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_by_name(multi_column_report):
    assert [row["campaign_id"] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_with_get_method(multi_column_report):
    assert [row.get("campaign_id") for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_as_attribute(multi_column_report):
    assert [row.campaign_id for row in multi_column_report] == [1, 2, 3]


def test_getitem_raise_index_error_for_out_of_index_value(multi_column_report):
    with pytest.raises(IndexError):
        [row[99] for row in multi_column_report] == [1, 2, 3]


def test_get_raises_attribute_error_for_missing_value(multi_column_report):
    with pytest.raises(AttributeError):
        [row.get("missing_value")
         for row in multi_column_report] == [None, None, None]


def test_getattr_raises_attribute_error_for_missing_value(multi_column_report):
    with pytest.raises(AttributeError):
        [getattr(row, "missing_value")
         for row in multi_column_report] == [None, None, None]


def test_hasattr_return_false_for_missing_value(multi_column_report):
    assert [hasattr(row, "missing_value")
            for row in multi_column_report] == [False, False, False]


def test_convert_report_to_pandas(multi_column_report):
    expected = pd.DataFrame(data=[[1, 2], [2, 3], [3, 4]],
                            columns=["campaign_id", "ad_group_id"])
    assert multi_column_report.to_pandas().equals(expected)


def test_get_report_length(multi_column_report):
    assert len(multi_column_report) == 3


def test_report_str(single_element_report):
    assert str(single_element_report) == "[1]"


def test_report_bool(single_element_report):
    assert single_element_report
    single_element_report.is_fake = True
    assert not single_element_report


def test_add_two_reports(multi_column_report):
    added_report = multi_column_report + multi_column_report
    assert len(added_report) == 6


def test_add_report_and_non_report_raises_type_error(multi_column_report):
    with pytest.raises(TypeError):
        multi_column_report + 1


def test_add_non_report_and_report_raises_type_error(multi_column_report):
    with pytest.raises(TypeError):
        1 + multi_column_report


def test_add_reports_with_different_columns_raises_value_error(
        multi_column_report, single_element_report):
    with pytest.raises(ValueError):
        multi_column_report + single_element_report


def test_iteration_over_fake_report_returns_empty_list(multi_column_report):
    multi_column_report.is_fake = True
    results = [row for row in multi_column_report]
    assert results == []


def test_slicing_multi_column_gaarf_report_returns_gaarf_report(
        multi_column_report):
    new_report = multi_column_report[0:2]
    assert new_report == GaarfReport(
        results=[[1, 2], [2, 3]], column_names=["campaign_id", "ad_group_id"])


def test_indexing_multi_column_gaarf_report_by_index_returns_gaarf_report(
        multi_column_report):
    new_report = multi_column_report[0]
    assert new_report == GaarfReport(
        results=[[1, 2]], column_names=["campaign_id", "ad_group_id"])


def test_indexing_multi_column_gaarf_report_by_one_column_returns_gaarf_report(
        multi_column_report):
    new_report = multi_column_report["campaign_id"]
    assert new_report == GaarfReport(results=[[1], [2], [3]],
                                     column_names=["campaign_id"])


def test_indexing_multi_column_gaarf_report_by_several_columns_returns_gaarf_report(
        multi_column_report):
    new_report = multi_column_report[["campaign_id", "ad_group_id"]]
    assert new_report == multi_column_report


def test_indexing_multi_column_gaarf_report_by_non_existing_column_raises_type_error(
        multi_column_report):
    with pytest.raises(TypeError):
        multi_column_report[["campaign_id", "ad_group"]]


def test_slicing_multi_column_gaarf_report_returns_slice(single_column_report):
    new_report = single_column_report[0:2]
    assert new_report == [1, 2]


def test_slicing_multi_column_gaarf_report_returns_element(
        single_column_report):
    new_report = single_column_report[0]
    assert new_report == [1]
