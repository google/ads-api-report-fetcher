from typing import Dict
import abc
from smart_open import open  # type: ignore


class AbsReader(abc.ABC):
    @abc.abstractmethod
    def read(self, query_path: str, **kwargs):
        raise NotImplementedError


class FileReader(AbsReader):
    def __init__(self):
        pass

    def read(self, query_path, **kwargs):
        with open(query_path, "r") as f:
            raw_query_text = f.read()
        return raw_query_text


class ConsoleReader(AbsReader):
    def __init__(self):
        pass

    def read(self, query_path, **kwargs):
        return query_path


class NullReader(AbsReader):
    def __init__(self, reader_option, **kwargs):
        raise ValueError(f"{reader_option} is unknown reader type!")

    def read(self):
        print("Unknown reader type!")


class ReaderFactory:
    reader_options: Dict[str, AbsReader] = {}

    def __init__(self):
        self.load_reader_options()

    def load_reader_options(self):
        self.reader_options["file"] = FileReader
        self.reader_options["console"] = ConsoleReader

    def create_reader(self, reader_option: str, **kwargs) -> AbsReader:
        if reader_option in self.reader_options:
            return self.reader_options[reader_option](**kwargs)
        else:
            return NullReader(reader_option)
