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
