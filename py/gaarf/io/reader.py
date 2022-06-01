import abc
from smart_open import open  # type: ignore


class AbsReader(abc.ABC):
    @abc.abstractmethod
    def read(self, query_path, **kwargs):
        raise NotImplementedError


class FileReader(AbsReader):
    def __init__(self):
        pass

    def read(self, query_path, **kwargs):
        with open(query_path, "r") as f:
            raw_query_text = f.read()
        return raw_query_text
