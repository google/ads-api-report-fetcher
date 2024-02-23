from __future__ import annotations

from datetime import datetime

import pytest
from gaarf.io import writer
from gaarf.io.writer import create_writer
from gaarf.io.writer import WriterFactory
from gaarf.report import GaarfReport
from google.cloud.bigquery import SchemaField  # type: ignore


@pytest.fixture
def csv_writer():
    return create_writer('csv', destination_folder='/tmp')


@pytest.fixture
def bigquery_writer():
    return create_writer('bq', project='fake-project', dataset='fake-dataset')


@pytest.fixture
def single_column_data():
    results = [[1], [2], [3]]
    columns = ['column_1']
    return GaarfReport(results, columns)


@pytest.fixture
def sample_data():
    results = [[1, 'two', [3, 4]]]
    columns = ['column_1', 'column_2', 'column_3']
    return GaarfReport(results, columns)


@pytest.fixture
def sample_data_with_dates():
    results = [[1, datetime.now(), datetime.now().date()]]
    columns = ['column_1', 'datetime', 'date']
    return GaarfReport(results, columns)


def test_csv_writer_single_column(csv_writer, single_column_data):
    tmp_file = '/tmp/test.csv'
    expected = ['column_1', '1', '2', '3']
    csv_writer.write(single_column_data, 'test.csv')
    with open(tmp_file, 'r') as f:
        file = f.readlines()
    assert [row.strip() for row in file] == expected


def test_csv_writer_multi_column(csv_writer, sample_data):
    tmp_file = '/tmp/test.csv'
    expected = ['column_1,column_2,column_3', '1,two,"[3, 4]"']
    csv_writer.array_handling = 'arrays'
    csv_writer.write(sample_data, 'test.csv')
    with open(tmp_file, 'r') as f:
        file = f.readlines()
    assert [row.strip() for row in file] == expected


def test_csv_writer_multi_column_arrays_converted_to_strings(
        csv_writer, sample_data):
    tmp_file = '/tmp/test.csv'
    expected = ['column_1,column_2,column_3', '1,two,3|4']
    csv_writer.array_handling = 'strings'
    csv_writer.write(sample_data, 'test.csv')
    with open(tmp_file, 'r') as f:
        file = f.readlines()
    assert [row.strip() for row in file] == expected


def test_bq_get_results_types(bigquery_writer, sample_data):
    result_types = bigquery_writer._get_result_types(sample_data)
    assert result_types == {
        'column_1': {
            'field_type': int,
            'repeated': False
        },
        'column_2': {
            'field_type': str,
            'repeated': False
        },
        'column_3': {
            'field_type': int,
            'repeated': True
        }
    }


def test_bq_get_results_types_supports_arrays(bigquery_writer, sample_data):
    result_types = bigquery_writer._get_result_types(sample_data)
    assert result_types == {
        'column_1': {
            'field_type': int,
            'repeated': False
        },
        'column_2': {
            'field_type': str,
            'repeated': False
        },
        'column_3': {
            'field_type': int,
            'repeated': True
        }
    }


def test_bq_get_correct_schema(bigquery_writer, sample_data):
    schema = bigquery_writer._define_schema(sample_data)
    assert schema == [
        SchemaField('column_1', 'INT64', 'NULLABLE', None, None, (), None),
        SchemaField('column_2', 'STRING', 'NULLABLE', None, None, (), None),
        SchemaField('column_3', 'INT64', 'REPEATED', None, None, (), None)
    ]


def test_bq_get_correct_schema_with_dates(bigquery_writer,
                                          sample_data_with_dates):
    schema = bigquery_writer._define_schema(sample_data_with_dates)
    assert schema == [
        SchemaField('column_1', 'INT64', 'NULLABLE', None, None, (), None),
        SchemaField('datetime', 'DATETIME', 'NULLABLE', None, None, (), None),
        SchemaField('date', 'DATE', 'NULLABLE', None, None, (), None)
    ]


def test_writer_factory_inits():
    bq_writer = create_writer('bq',
                              project='fake_project',
                              dataset='fake_dataset')
    csv_writer = create_writer('csv', destination_folder='/fake_folder')
    sheet_writer = create_writer('sheet',
                                 share_with='1@google.com',
                                 credentials_file='home/me/client_secret.json')
    sqlalchemy_writer = create_writer(
        'sqldb', connection_string='protocol://user:password@host:port/db')
    assert bq_writer.dataset_id == 'fake_project.fake_dataset'
    assert csv_writer.destination_folder == '/fake_folder'
    assert sqlalchemy_writer.connection_string == 'protocol://user:password@host:port/db'
    assert sheet_writer.share_with == '1@google.com'
    assert sheet_writer.credentials_file == 'home/me/client_secret.json'


def test_null_writer_raises_unknown_writer_error():
    with pytest.raises(ValueError):
        create_writer('non-existing-option')


class TestDeprecatedWriters:

    def test_instantiating_writer_factory_is_deprected(self):
        with pytest.deprecated_call():
            WriterFactory()

    def test_instantiating_console_writer_is_deprected(self):
        with pytest.warns(DeprecationWarning) as w:
            writer.StdoutWriter()
            assert len(w) == 1
            assert str(w[0].message) == (
                'Loading StdoutWriter from `gaarf.io.writer` is deprecated; '
                'Import ConsoleWriter from `gaarf.io.writers.console_writer` '
                'instead')

    def test_instantiating_csv_writer_is_deprected(self):
        with pytest.warns(DeprecationWarning) as w:
            writer.CsvWriter()
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == (
            'Loading CsvWriter from `gaarf.io.writer` is deprecated; '
            'Import CsvWriter from `gaarf.io.writers.csv_writer` instead')

    def test_instantiating_sqlalchemy_writer_is_deprected(self):
        with pytest.warns(DeprecationWarning) as w:
            writer.SqlAlchemyWriter(connection_string='fake-connection')
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == (
            'Loading SqlAlchemyWriter from `gaarf.io.writer` is deprecated; '
            'Import SqlAlchemyWriter from `gaarf.io.writers.sqldb_writer` '
            'instead')

    def test_instantiating_sheet_writer_is_deprected(self):
        with pytest.warns(DeprecationWarning) as w:
            writer.SheetWriter(share_with='fake-user', credentials_file=None)
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == (
            'Loading SheetWriter from `gaarf.io.writer` is deprecated; '
            'Import SheetWriter from `gaarf.io.writers.sheets_writer` instead')

    def test_instantiating_bigquery_writer_is_deprected(self):
        with pytest.warns(DeprecationWarning) as w:
            writer.BigQueryWriter(project='fake-project', dataset='fake-dataset')
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == (
            'Loading BigQueryWriter from `gaarf.io.writer` is deprecated; '
            'Import BigQueryWriter from `gaarf.io.writers.bigquery_writer` '
            'instead')
