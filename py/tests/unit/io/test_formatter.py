from __future__ import annotations

import pytest

from gaarf.io import formatter
from gaarf.report import GaarfReport


class TestFormatterWithoutPlaceholders:
  @pytest.fixture
  def report_without_arrays(self):
    return GaarfReport(
      results=[
        [1, 2],
        [2, 3],
        [3, 4],
      ],
      column_names=['campaign_id', 'ad_group_id'],
    )

  @pytest.fixture
  def report_with_arrays(self):
    return GaarfReport(
      results=[
        [1, [2]],
        [2, [3]],
        [3, [4, 5]],
      ],
      column_names=['campaign_id', 'ad_group_id'],
    )

  def test_format_report_for_writing_keeps_arrays_in_report_(
    self, report_with_arrays
  ):
    array_handling_strategy = formatter.ArrayHandlingStrategy(type_='arrays')
    formatted_report = formatter.format_report_for_writing(
      report_with_arrays, [array_handling_strategy]
    )
    assert report_with_arrays == formatted_report

  def test_format_report_for_writing_returns_the_same_report_under_default_array_handling_strategy(  # pylint: disable=line-too-long
    self, report_without_arrays
  ):
    array_handling_strategy = formatter.ArrayHandlingStrategy()
    formatted_report = formatter.format_report_for_writing(
      report_without_arrays, [array_handling_strategy]
    )
    assert report_without_arrays == formatted_report

  def test_format_report_for_writing_converts_arrays_converted_to_strings(
    self, report_with_arrays
  ):
    array_handling_strategy = formatter.ArrayHandlingStrategy(type_='strings')
    formatted_report = formatter.format_report_for_writing(
      report_with_arrays, [array_handling_strategy]
    )
    expected_report = GaarfReport(
      results=[
        [1, '2'],
        [2, '3'],
        [3, '4|5'],
      ],
      column_names=['campaign_id', 'ad_group_id'],
      results_placeholder=[[0, '']],
    )
    assert expected_report == formatted_report

  def test_format_report_for_writing_converts_arrays_converted_to_strings_with_custom_delimiter(  # pylint: disable=line-too-long
    self, report_with_arrays
  ):
    array_handling_strategy = formatter.ArrayHandlingStrategy(
      type_='strings', delimiter='*'
    )
    formatted_report = formatter.format_report_for_writing(
      report_with_arrays, [array_handling_strategy]
    )
    expected_report = GaarfReport(
      results=[
        [1, '2'],
        [2, '3'],
        [3, '4*5'],
      ],
      column_names=['campaign_id', 'ad_group_id'],
    )
    assert expected_report == formatted_report


class TestFormatterWithPlaceholders:
  @pytest.fixture
  def report_without_arrays(self):
    return GaarfReport(
      results=[
        [1, 2],
        [2, 3],
        [3, 4],
      ],
      column_names=['campaign_id', 'ad_group_id'],
      results_placeholder=[[0, 0]],
    )

  @pytest.fixture
  def report_with_arrays(self):
    return GaarfReport(
      results=[
        [1, [2]],
        [2, [3]],
        [3, [4, 5]],
      ],
      column_names=['campaign_id', 'ad_group_id'],
      results_placeholder=[[0, [0]]],
    )

  def test_format_report_for_writing_keeps_placeholders_the_same(
    self, report_with_arrays
  ):
    array_handling_strategy = formatter.ArrayHandlingStrategy(type_='arrays')
    formatted_report = formatter.format_report_for_writing(
      report_with_arrays, [array_handling_strategy]
    )
    assert report_with_arrays.results_placeholder == (
      formatted_report.results_placeholder
    )

  def test_format_report_for_writing_keeps_placeholders_the_same_for_default_array_handling_strategy(  # pylint: disable=line-too-long
    self, report_without_arrays
  ):
    array_handling_strategy = formatter.ArrayHandlingStrategy()
    formatted_report = formatter.format_report_for_writing(
      report_without_arrays, [array_handling_strategy]
    )
    assert report_without_arrays.results_placeholder == (
      formatted_report.results_placeholder
    )

  def test_format_report_for_writing_convert_array_placeholders_to_strings(
    self, report_with_arrays
  ):
    array_handling_strategy = formatter.ArrayHandlingStrategy(type_='strings')
    formatted_report = formatter.format_report_for_writing(
      report_with_arrays, [array_handling_strategy]
    )
    expected_report = GaarfReport(
      results=[
        [1, '2'],
        [2, '3'],
        [3, '4|5'],
      ],
      column_names=['campaign_id', 'ad_group_id'],
      results_placeholder=[[0, '']],
    )
    assert expected_report.results_placeholder == (
      formatted_report.results_placeholder
    )


def test_format_extension_returns_correct_extensions():
  default_output = formatter.format_extension('test_query.sql')
  default_output_custom_extension = formatter.format_extension(
    'test_query.txt', '.txt'
  )
  csv_output = formatter.format_extension(
    'test_query.sql', new_extension='.csv'
  )
  assert default_output == 'test_query'
  assert default_output_custom_extension == 'test_query'
  assert csv_output == 'test_query.csv'
