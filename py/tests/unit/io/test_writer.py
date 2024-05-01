from __future__ import annotations

import pytest
from gaarf.io import writer


def test_writer_factory_inits():
    bq_writer = writer.create_writer(
        'bq', project='fake_project', dataset='fake_dataset')
    csv_writer = writer.create_writer('csv', destination_folder='/fake_folder')
    sheet_writer = writer.create_writer(
        'sheet',
        share_with='1@google.com',
        credentials_file='home/me/client_secret.json')
    sqlalchemy_writer = writer.create_writer(
        'sqldb', connection_string='protocol://user:password@host:port/db')
    assert bq_writer.dataset_id == 'fake_project.fake_dataset'
    assert csv_writer.destination_folder == '/fake_folder'
    assert sqlalchemy_writer.connection_string == (
        'protocol://user:password@host:port/db')
    assert sheet_writer.share_with == '1@google.com'
    assert sheet_writer.credentials_file == 'home/me/client_secret.json'


def test_null_writer_raises_unknown_writer_error():
    with pytest.raises(ValueError):
        writer.create_writer('non-existing-option')


class TestDeprecatedWriters:

    def test_instantiating_writer_factory_is_deprected(self):
        with pytest.deprecated_call():
            writer.WriterFactory()

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
                'Loading SqlAlchemyWriter from `gaarf.io.writer` is '
                'deprecated; '
                'Import SqlAlchemyWriter from `gaarf.io.writers.sqldb_writer` '
                'instead')

    def test_instantiating_sheet_writer_is_deprected(self):
        with pytest.warns(DeprecationWarning) as w:
            writer.SheetWriter(share_with='fake-user', credentials_file=None)
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == (
                'Loading SheetWriter from `gaarf.io.writer` is deprecated; '
                'Import SheetWriter from `gaarf.io.writers.sheets_writer` '
                'instead'
            )

    def test_instantiating_bigquery_writer_is_deprected(self):
        with pytest.warns(DeprecationWarning) as w:
            writer.BigQueryWriter(
                project='fake-project', dataset='fake-dataset')
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == (
                'Loading BigQueryWriter from `gaarf.io.writer` is deprecated; '
                'Import BigQueryWriter from `gaarf.io.writers.bigquery_writer` '
                'instead')
