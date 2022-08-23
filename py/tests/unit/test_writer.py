import pytest
from google.cloud.bigquery import SchemaField  # type: ignore
import gaarf.io.writer as writer  # type: ignore
from gaarf.report import GaarfReport


@pytest.fixture
def csv_writer():
    return writer.CsvWriter("/tmp")


@pytest.fixture
def bq_writer():
    return writer.BigQueryWriter("fake_project", "fake_dataset")


@pytest.fixture
def single_column_data():
    results = [1, 2, 3]
    columns = ["column_1"]
    return GaarfReport(results, columns)


@pytest.fixture
def sample_data():
    results = [(1, "two", [3, 4])]
    columns = ["column_1", "column_2", "column_3"]
    return GaarfReport(results, columns)


def test_csv_writer_single_column(csv_writer, single_column_data):
    tmp_file = "/tmp/test.csv"
    expected = ["column_1", "1", "2", "3"]
    csv_writer.write(single_column_data, "test.csv")
    with open(tmp_file, "r") as f:
        file = f.readlines()
    assert [row.strip() for row in file] == expected


def test_csv_writer_multi_column(csv_writer, sample_data):
    tmp_file = "/tmp/test.csv"
    expected = ["column_1,column_2,column_3", '1,two,3|4']
    csv_writer.write(sample_data, "test.csv")
    with open(tmp_file, "r") as f:
        file = f.readlines()
    assert [row.strip() for row in file] == expected


def test_bq_get_results_types(bq_writer, sample_data):
    results, columns = sample_data.results, sample_data.column_names
    result_types = bq_writer._get_result_types(results, columns)
    assert result_types == {
        'column_1': {
            'element_type': int,
            'repeated': False
        },
        'column_2': {
            'element_type': str,
            'repeated': False
        },
        'column_3': {
            'element_type': int,
            'repeated': True
        }
    }


def test_bq_get_correct_schema(bq_writer, sample_data):
    results, columns = sample_data.results, sample_data.column_names
    schema = bq_writer._define_schema(results, columns)
    assert schema == [
        SchemaField('column_1', 'INT64', 'NULLABLE', None, (), None),
        SchemaField('column_2', 'STRING', 'NULLABLE', None, (), None),
        SchemaField('column_3', 'INT64', 'REPEATED', None, (), None)
    ]


def test_format_extension():
    default_output = writer.DestinationFormatter.format_extension(
        "test_query.sql")
    default_output_custom_extension = writer.DestinationFormatter.format_extension(
        "test_query.txt", ".txt")
    csv_output = writer.DestinationFormatter.format_extension(
        "test_query.sql", new_extension=".csv")
    assert default_output == "test_query"
    assert default_output_custom_extension == "test_query"
    assert csv_output == "test_query.csv"


@pytest.fixture
def writer_factory():
    return writer.WriterFactory()


def test_writer_factory_load(writer_factory):
    assert writer_factory.write_options == {
        "bq": writer.BigQueryWriter,
        "csv": writer.CsvWriter,
        "console": writer.StdoutWriter,
        "sqldb": writer.SqlAlchemyWriter
    }


def test_writer_factory_inits(writer_factory):
    bq_writer = writer_factory.create_writer("bq",
                                             project="fake_project",
                                             dataset="fake_dataset")
    csv_writer = writer_factory.create_writer(
        "csv", destination_folder="/fake_folder")
    sqlalchemy_writer = writer_factory.create_writer(
        "sqldb",
        connection_string="protocol://user:password@host:port/db")
    assert bq_writer.dataset_id == "fake_project.fake_dataset"
    assert csv_writer.destination_folder == "/fake_folder"
    assert sqlalchemy_writer.connection_string == "protocol://user:password@host:port/db"


def test_null_writer_raises_unknown_writer_error(writer_factory):
    with pytest.raises(ValueError):
        writer_factory.create_writer("non-existing-option")
