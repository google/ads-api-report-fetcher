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

import json

import pytest

from gaarf.io.writers import console_writer

_TMP_NAME = 'test'


class TestConsoleTableWriter:
  @pytest.fixture
  def console_writer(self):
    return console_writer.ConsoleWriter()

  def test_write_single_column_report_returns_correct_data(
    self, capsys, console_writer, single_column_data
  ):
    console_writer.write(single_column_data, _TMP_NAME)
    output = capsys.readouterr().out.strip()
    prepared_output = ' '.join(output.replace('\n', '').split())
    assert f'showing results for query <{_TMP_NAME}>' in prepared_output
    assert 'column_1' in prepared_output
    assert 'showing rows 1-3 out of total 3' in prepared_output

  def test_write_multi_column_report_returns_arrays(
    self, capsys, console_writer, sample_data
  ):
    console_writer.array_handling = 'arrays'
    console_writer.write(sample_data, _TMP_NAME)
    output = capsys.readouterr().out.strip()
    prepared_output = ' '.join(output.replace('\n', '').split())
    assert 'column_1 | column_2 | column_3' in prepared_output
    assert '[3, 4]' in prepared_output

  def test_write_multi_column_report_with_arrays_returns_concatenated_strings(
    self, capsys, console_writer, sample_data
  ):
    console_writer.array_handling = 'strings'
    console_writer.write(sample_data, _TMP_NAME)
    output = capsys.readouterr().out.strip()
    prepared_output = ' '.join(output.replace('\n', '').split())
    assert '3|4' in prepared_output


class TestConsoleJsonWriter:
  @pytest.fixture
  def console_writer(self):
    return console_writer.ConsoleWriter(format='json')

  def test_write_single_column_report_returns_correct_data(
    self, capsys, console_writer, single_column_data
  ):
    console_writer.write(single_column_data, _TMP_NAME)
    output = capsys.readouterr().out.strip()
    prepared_output = ' '.join(output.replace('\n', '').split())
    expected_result = json.loads(single_column_data.to_json())
    res = json.loads(prepared_output.replace("'", '"'))
    assert res == expected_result

  def test_write_multi_column_report_returns_arrays(
    self, capsys, console_writer, sample_data
  ):
    console_writer.array_handling = 'arrays'
    console_writer.write(sample_data, _TMP_NAME)
    output = capsys.readouterr().out.strip()
    prepared_output = ' '.join(output.replace('\n', '').split())
    expected_result = json.loads(sample_data.to_json())
    res = json.loads(prepared_output.replace("'", '"'))
    assert res == expected_result

  def test_write_multi_column_report_with_arrays_returns_concatenated_strings(
    self, capsys, console_writer, sample_data
  ):
    console_writer.array_handling = 'strings'
    console_writer.write(sample_data, _TMP_NAME)
    output = capsys.readouterr().out.strip()
    prepared_output = ' '.join(output.replace('\n', '').split())
    sample_data[0].column_3 = '3|4'
    expected_result = json.loads(sample_data.to_json())
    res = json.loads(prepared_output.replace("'", '"'))
    assert res == expected_result
