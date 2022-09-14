# Copyright 2022 Google LLC
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

from typing import Any, Sequence, Union
import pandas as pd


class GaarfReport:

    def __init__(self,
                 results: Sequence[Any],
                 column_names: Sequence[str],
                 is_fake: bool = False):
        self.results = results
        self.column_names = column_names
        self.is_fake = is_fake

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
