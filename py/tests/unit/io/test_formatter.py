from __future__ import annotations

import pytest
from gaarf.io import formatter
from gaarf.report import GaarfReport


@pytest.fixture
def report_without_arrays():
    return GaarfReport(results=[[1, 2], [2, 3], [3, 4]],
                       column_names=['campaign_id', 'ad_group_id'])


@pytest.fixture
def report_with_arrays():
    return GaarfReport(results=[[1, 2], [2, 3], [3, [4, 5]]],
                       column_names=['campaign_id', 'ad_group_id'])


def test_arrays_remain_arrays(report_with_arrays):
    array_handling_strategy = formatter.ArrayHandlingStrategy(type_='arrays')
    formatted_report = formatter.format_report_for_writing(
        report_with_arrays, [array_handling_strategy])
    assert report_with_arrays == formatted_report


def test_arrays_remain_arrays_in_default_array_handling_strategy(
        report_without_arrays):
    array_handling_strategy = formatter.ArrayHandlingStrategy()
    formatted_report = formatter.format_report_for_writing(
        report_without_arrays, [array_handling_strategy])
    assert report_without_arrays == formatted_report


def test_arrays_converted_to_strings(report_with_arrays):
    array_handling_strategy = formatter.ArrayHandlingStrategy(type_='strings')
    formatted_report = formatter.format_report_for_writing(
        report_with_arrays, [array_handling_strategy])
    expected_report = GaarfReport(results=[[1, 2], [2, 3], [3, '4|5']],
                                  column_names=['campaign_id', 'ad_group_id'])
    assert expected_report == formatted_report


def test_arrays_converted_to_strings_custom_delimiter(report_with_arrays):
    array_handling_strategy = formatter.ArrayHandlingStrategy(type_='strings',
                                                              delimiter='*')
    formatted_report = formatter.format_report_for_writing(
        report_with_arrays, [array_handling_strategy])
    expected_report = GaarfReport(results=[[1, 2], [2, 3], [3, '4*5']],
                                  column_names=['campaign_id', 'ad_group_id'])
    assert expected_report == formatted_report


def test_format_extension():
    default_output = formatter.format_extension('test_query.sql')
    default_output_custom_extension = formatter.format_extension(
        'test_query.txt', '.txt')
    csv_output = formatter.format_extension('test_query.sql',
                                            new_extension='.csv')
    assert default_output == 'test_query'
    assert default_output_custom_extension == 'test_query'
    assert csv_output == 'test_query.csv'
