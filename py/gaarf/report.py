from typing import Any, Sequence, Union
import pandas as pd


class GaarfReport:
    def __init__(self, results: Sequence[Any], column_names: Sequence[str]):
        self.results = results
        self.column_names = column_names

    def to_list(self) -> Sequence[Any]:
        return self.results

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(data=self.results, columns=self.column_names)

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        return GaarfIterator(self.results, self.column_names)

    def __str__(self):
        return f"{self.results}"


class GaarfIterator:

    def __init__(self, results, column_names):

        self.results = results
        self.column_names = column_names
        self.single_column_report = len(self.column_names) == 1
        self.index = 0

    def __next__(self):
        try:
            result = self.results[self.index]
            if not isinstance(result, Sequence):
                result = [result]
        except IndexError as e:
            raise StopIteration from e
        self.index += 1
        if self.single_column_report:
            return result[0]
        return GaarfRow(result, self.column_names)


class GaarfRow:
    def __init__(self, data: Sequence[Union[int, float, str]],
                 column_names: Sequence[str]):
        self.data = data
        self.n_elements = len(data)
        self.column_names = column_names

    def __getattr__(self, element: str) -> Any:
        return self.data[self.column_names.index(element)]

    def __getitem__(self, element: Union[str, int]) -> Any:
        if isinstance(element, int) and element < self.n_elements:
            return self.data[element]
        if isinstance(element, str):
            return self.data[self.column_names.index(element)]
        raise IndexError(f"cannot find {element} element!")

    def get(self, item: str) -> Any:
        if item in self.column_names:
            return self.data[self.column_names.index(item)]
        return None
