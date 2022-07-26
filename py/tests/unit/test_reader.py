import pytest
import gaarf.io.reader as reader


def test_console_reader():
    console_reader = reader.ConsoleReader()
    expected = "SELECT 1"
    assert expected == console_reader.read(expected)


@pytest.fixture
def reader_factory():
    return reader.ReaderFactory()


def test_reader_factory_load(reader_factory):
    assert reader_factory.reader_options == {
        "file": reader.FileReader,
        "console": reader.ConsoleReader
    }


def test_reader_factory_inits(reader_factory):
    file_reader = reader_factory.create_reader("file")
    console_reader = reader_factory.create_reader("console")
    assert isinstance(file_reader, reader.FileReader)
    assert isinstance(console_reader, reader.ConsoleReader)


def test_null_reader_raises_unknown_reader_error(reader_factory):
    with pytest.raises(ValueError):
        reader_factory.create_reader("non-existing-option")
