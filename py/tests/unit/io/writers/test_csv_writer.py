from __future__ import annotations

import pytest
from gaarf.io.writers import csv_writer

_TMP_FILENAME = 'test.csv'


class TestCsvWriter:

    @pytest.fixture
    def output_folder(self, tmp_path):
        return tmp_path

    @pytest.fixture
    def csv_writer(self, output_folder):
        return csv_writer.CsvWriter(output_folder)

    def test_write_single_column_report_returns_correct_data(
            self, csv_writer, single_column_data, output_folder):

        output = output_folder / _TMP_FILENAME
        expected = ['column_1', '1', '2', '3']
        csv_writer.write(single_column_data, _TMP_FILENAME)
        with open(output, 'r') as f:
            file = f.readlines()
        assert [row.strip() for row in file] == expected

    def test_write_multi_column_report_returns_correct_data(
            self, csv_writer, sample_data, output_folder):
        output = output_folder / _TMP_FILENAME
        expected = [
            'column_1,column_2,column_3',
            '1,two,"[3, 4]"',
        ]
        csv_writer.array_handling = 'arrays'
        csv_writer.write(sample_data, _TMP_FILENAME)
        with open(output, 'r') as f:
            file = f.readlines()
        assert [row.strip() for row in file] == expected

    def test_write_multi_column_report_with_arrays_returns_correct_data(
            self, csv_writer, sample_data, output_folder):
        output = output_folder / _TMP_FILENAME
        expected = [
            'column_1,column_2,column_3',
            '1,two,3|4',
        ]
        csv_writer.array_handling = 'strings'
        csv_writer.write(sample_data, _TMP_FILENAME)
        with open(output, 'r') as f:
            file = f.readlines()
        assert [row.strip() for row in file] == expected
