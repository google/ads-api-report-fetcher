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

from gaarf.io.writers import json_writer

_TMP_FILENAME = 'test.json'


class TestJsonWriter:
  @pytest.fixture
  def output_folder(self, tmp_path):
    return tmp_path

  @pytest.fixture
  def json_writer(self, output_folder):
    return json_writer.JsonWriter(output_folder)

  def test_write_single_column_report_returns_correct_data(
    self, json_writer, single_column_data, output_folder
  ):
    output = output_folder / _TMP_FILENAME
    expected = [
      {'column_1': 1},
      {'column_1': 2},
      {'column_1': 3},
    ]

    json_writer.write(single_column_data, _TMP_FILENAME)

    with open(output, 'r') as f:
      data = json.load(f)

    assert data == expected

  def test_write_multi_column_report_returns_correct_data(
    self, json_writer, sample_data, output_folder
  ):
    output = output_folder / _TMP_FILENAME
    expected = [
      {
        'column_1': 1,
        'column_2': 'two',
        'column_3': [3, 4],
      },
    ]
    json_writer.array_handling = 'arrays'
    json_writer.write(sample_data, _TMP_FILENAME)

    with open(output, 'r') as f:
      data = json.load(f)

    assert data == expected

  def test_write_multi_column_report_with_arrays_returns_correct_data(
    self, json_writer, sample_data, output_folder
  ):
    output = output_folder / _TMP_FILENAME
    expected = [
      {
        'column_1': 1,
        'column_2': 'two',
        'column_3': '3|4',
      },
    ]
    json_writer.array_handling = 'strings'
    json_writer.write(sample_data, _TMP_FILENAME)

    with open(output, 'r') as f:
      data = json.load(f)

    assert data == expected
