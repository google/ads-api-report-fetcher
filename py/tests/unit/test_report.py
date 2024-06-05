# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

from collections import abc

import pandas as pd
import pytest

from gaarf import exceptions, report


@pytest.fixture
def single_element_report():
  return report.GaarfReport(results=[[1]], column_names=['campaign_id'])


@pytest.fixture
def single_column_report():
  return report.GaarfReport(
    results=[[1], [1], [3]], column_names=['campaign_id']
  )


@pytest.fixture
def multi_column_report():
  return report.GaarfReport(
    results=[[1, 2], [2, 3], [3, 4]],
    column_names=['campaign_id', 'ad_group_id'],
  )


def test_single_element_report_returns_gaarf_row(single_element_report):
  assert [row[0] for row in single_element_report] == [1]


def test_single_column_report_returns_sequence(single_column_report):
  assert [row[0] for row in single_column_report] == [1, 1, 3]


def test_multi_column_report_returns_gaarf_row(multi_column_report):
  assert isinstance(list(multi_column_report)[0], report.GaarfRow)


def test_multi_column_report_support_iteration_with_gaarf_iterator(
  multi_column_report,
):
  assert isinstance(iter(multi_column_report), abc.Iterable)


def test_multi_column_report_get_element_by_id(multi_column_report):
  assert [row[0] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_by_name(multi_column_report):
  assert [row['campaign_id'] for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_with_get_method(multi_column_report):
  assert [row.get('campaign_id') for row in multi_column_report] == [1, 2, 3]


def test_multi_column_report_get_element_as_attribute(multi_column_report):
  assert [row.campaign_id for row in multi_column_report] == [1, 2, 3]


def test_getitem_raise_index_error_for_out_of_index_value(multi_column_report):
  with pytest.raises(exceptions.GaarfReportException):
    [row[99] for row in multi_column_report] == [1, 2, 3]


def test_get_raises_attribute_error_for_missing_value(multi_column_report):
  with pytest.raises(AttributeError):
    [row.get('missing_value') for row in multi_column_report] == [
      None,
      None,
      None,
    ]


def test_getattr_raises_attribute_error_for_missing_value(multi_column_report):
  with pytest.raises(AttributeError):
    [getattr(row, 'missing_value') for row in multi_column_report] == [
      None,
      None,
      None,
    ]


def test_hasattr_return_false_for_missing_value(multi_column_report):
  assert [hasattr(row, 'missing_value') for row in multi_column_report] == [
    False,
    False,
    False,
  ]


def test_convert_report_to_pandas(multi_column_report):
  expected = pd.DataFrame(
    data=[[1, 2], [2, 3], [3, 4]], columns=['campaign_id', 'ad_group_id']
  )
  assert multi_column_report.to_pandas().equals(expected)


def test_get_report_length(multi_column_report):
  assert len(multi_column_report) == 3


def test_report_bool(single_element_report):
  assert single_element_report
  single_element_report.results = []
  assert not single_element_report


def test_add_two_reports(multi_column_report):
  added_report = multi_column_report + multi_column_report
  assert len(added_report) == 6


def test_add_report_and_non_report_raises_exception(multi_column_report):
  with pytest.raises(exceptions.GaarfReportException):
    multi_column_report + 1


def test_add_non_report_and_report_raises_exception(multi_column_report):
  with pytest.raises(TypeError):
    1 + multi_column_report


def test_add_reports_with_different_columns_raises_exception(
  multi_column_report, single_element_report
):
  with pytest.raises(exceptions.GaarfReportException):
    multi_column_report + single_element_report


def test_slicing_multi_column_gaarf_report_returns_gaarf_report(
  multi_column_report,
):
  new_report = multi_column_report[0:2]
  assert new_report == report.GaarfReport(
    results=[[1, 2], [2, 3]], column_names=['campaign_id', 'ad_group_id']
  )


def test_indexing_multi_column_gaarf_report_by_single_index_returns_gaarf_row(
  multi_column_report,
):
  new_report = multi_column_report[0]
  assert new_report == report.GaarfRow(
    data=[1, 2], column_names=['campaign_id', 'ad_group_id']
  )


def test_indexing_multi_column_gaarf_report_by_multi_index_returns_gaarf_report(
  multi_column_report,
):
  new_report = multi_column_report[0:2]
  assert new_report == report.GaarfReport(
    results=[[1, 2], [2, 3]], column_names=['campaign_id', 'ad_group_id']
  )


def test_indexing_multi_column_gaarf_report_by_one_column_returns_gaarf_report(
  multi_column_report,
):
  new_report = multi_column_report['campaign_id']
  assert new_report == report.GaarfReport(
    results=[[1], [2], [3]], column_names=['campaign_id']
  )


def test_indexing_multi_column_gaarf_report_by_several_columns_returns_gaarf_report(
  multi_column_report,
):
  new_report = multi_column_report[['campaign_id', 'ad_group_id']]
  assert new_report == multi_column_report


def test_indexing_multi_column_gaarf_report_by_non_existing_column_raises_exception(
  multi_column_report,
):
  with pytest.raises(exceptions.GaarfReportException):
    multi_column_report[['campaign_id', 'ad_group']]


def test_slicing_multi_column_gaarf_report_returns_slice(single_column_report):
  new_report = single_column_report[0:2]
  assert new_report == [1, 1]


def test_slicing_multi_column_gaarf_report_returns_element(
  single_column_report,
):
  new_report = single_column_report[0]
  assert new_report == [1]


def test_set_non_existing_item_gaarf_row_get_new_column(multi_column_report):
  row = multi_column_report[0]
  row['campaign_id_new'] = row['campaign_id'] * 100
  assert row == report.GaarfRow(
    data=[1, 2, 100],
    column_names=['campaign_id', 'ad_group_id', 'campaign_id_new'],
  )


def test_set_existing_item_gaarf_row_updates_column(multi_column_report):
  row = multi_column_report[0]
  row['campaign_id'] = row['campaign_id'] * 100
  assert row == report.GaarfRow(
    data=[100, 2], column_names=['campaign_id', 'ad_group_id']
  )


def test_set_non_existing_attribute_gaarf_row_get_new_column(
  multi_column_report,
):
  row = multi_column_report[0]
  row.campaign_id_new = row.campaign_id * 100
  assert row == report.GaarfRow(
    data=[1, 2, 100],
    column_names=['campaign_id', 'ad_group_id', 'campaign_id_new'],
  )


def test_set_non_existing_attribute_gaarf_rows_get_new_columns(
  multi_column_report,
):
  for row in multi_column_report:
    row.campaign_id_new = row.campaign_id * 100
  assert multi_column_report == report.GaarfReport(
    results=[[1, 2, 100], [2, 3, 200], [3, 4, 300]],
    column_names=['campaign_id', 'ad_group_id', 'campaign_id_new'],
  )


def test_set_existing_attribute_gaarf_row_updates_column(multi_column_report):
  row = multi_column_report[0]
  row.campaign_id = row.campaign_id * 100
  assert row == report.GaarfRow(
    data=[100, 2], column_names=['campaign_id', 'ad_group_id']
  )


def test_set_existing_attribute_gaarf_multiple_rows_updates_columns(
  multi_column_report,
):
  for row in multi_column_report:
    row.campaign_id = row.campaign_id * 100
  assert multi_column_report == report.GaarfReport(
    results=[[100, 2], [200, 3], [300, 4]],
    column_names=['campaign_id', 'ad_group_id'],
  )


def test_single_column_report_returns_flattened_list(single_column_report):
  assert single_column_report.to_list() == [1, 1, 3]


def test_single_column_report_returns_distinct_flattened_list(
  single_column_report,
):
  assert single_column_report.to_list(row_type='scalar', distinct=True) == [
    1,
    3,
  ]


def test_single_column_report_returns_distinct_flattened_list_legacy(
  single_column_report,
):
  assert single_column_report.to_list(flatten=True, distinct=True) == [1, 3]


def test_multi_column_report_converted_to_dict_list_values(multi_column_report):
  key_column = 'campaign_id'
  value_column = 'ad_group_id'
  output_dict = multi_column_report.to_dict(
    key_column=key_column, value_column=value_column
  )
  assert output_dict == {1: [2], 2: [3], 3: [4]}


def test_multi_column_report_converted_to_dict_scalar_values(
  multi_column_report,
):
  key_column = 'campaign_id'
  value_column = 'ad_group_id'
  output_dict = multi_column_report.to_dict(
    key_column=key_column,
    value_column=value_column,
    value_column_output='scalar',
  )
  assert output_dict == {1: 2, 2: 3, 3: 4}


def test_multi_column_report_converted_to_dict_raises_exception_on_non_existing_key_column(
  multi_column_report,
):
  key_column = 'missing_column'
  value_column = 'ad_group_id'
  with pytest.raises(exceptions.GaarfReportException):
    output_dict = multi_column_report.to_dict(
      key_column=key_column,
      value_column=value_column,
      value_column_output='scalar',
    )


def test_multi_column_report_converted_to_dict_raises_exception_on_non_existing_value_column(
  multi_column_report,
):
  key_column = 'campaign_id'
  value_column = 'missing_column'
  with pytest.raises(exceptions.GaarfReportException):
    output_dict = multi_column_report.to_dict(
      key_column=key_column,
      value_column=value_column,
      value_column_output='scalar',
    )


def test_multi_column_report_converted_to_dict_with_missing_value_column(
  multi_column_report,
):
  key_column = 'campaign_id'
  output_dict = multi_column_report.to_dict(key_column=key_column)
  assert output_dict == {
    1: [{'campaign_id': 1, 'ad_group_id': 2}],
    2: [{'campaign_id': 2, 'ad_group_id': 3}],
    3: [{'campaign_id': 3, 'ad_group_id': 4}],
  }


def test_multi_column_report_converted_to_dict_without_arguments(
  multi_column_report,
):
  output_dict = multi_column_report.to_list(row_type='dict')
  assert output_dict == [
    {'campaign_id': 1, 'ad_group_id': 2},
    {'campaign_id': 2, 'ad_group_id': 3},
    {'campaign_id': 3, 'ad_group_id': 4},
  ]


def test_to_list_incorrect_row_type_raises_exception(multi_column_report):
  with pytest.raises(exceptions.GaarfReportException):
    multi_column_report.to_list(row_type='tuple')


def test_empty_report_converted_to_dict_with_key_column(multi_column_report):
  key_column = 'campaign_id'
  value_column = 'ad_group_id'
  # clear results of report
  multi_column_report.results = []
  output_dict = multi_column_report.to_dict(
    key_column=key_column,
    value_column=value_column,
    value_column_output='scalar',
  )
  assert output_dict == {key_column: None}


def test_report_with_different_columns_not_equal(multi_column_report):
  assert single_element_report != multi_column_report


def test_report_with_different_data_are_not_equal(multi_column_report):
  new_multi_column_report = report.GaarfReport(
    results=list(multi_column_report.results),
    column_names=list(multi_column_report.column_names),
  )
  new_multi_column_report.results[0] = [10, 10]
  assert new_multi_column_report != multi_column_report


def test_report_with_same_data_are_equal(multi_column_report):
  new_multi_column_report = report.GaarfReport(
    results=list(multi_column_report.results),
    column_names=list(multi_column_report.column_names),
  )
  assert new_multi_column_report == multi_column_report


def test_conversion_from_pandas():
  values = [[1, 2], [3, 4]]
  column_names = ['one', 'two']
  df = pd.DataFrame(data=values, columns=column_names)
  report_from_df = report.GaarfReport.from_pandas(df)
  expected_report = report.GaarfReport(
    results=values, column_names=column_names
  )
  assert report_from_df == expected_report
