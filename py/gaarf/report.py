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
from collections import abc
import operator
import pandas as pd


class GaarfReport:

    def __init__(self,
                 results: Sequence[Any],
                 column_names: Sequence[str],
                 is_fake: bool = False):
        self.results = results
        self.column_names = column_names
        self.multi_column_report = len(column_names) > 1
        self.is_fake = is_fake

    def to_list(self) -> Sequence[Any]:
        return self.results

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(data=self.results, columns=self.column_names)

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        if self.is_fake:
            return None
        for result in self.results:
            if self.multi_column_report:
                yield GaarfRow(result, self.column_names)
            elif isinstance(result, abc.Sequence):
                yield result[0]
            else:
                yield result

    def __bool__(self):
        return not self.is_fake

    def __str__(self):
        return f"{self.results}"

    def __getitem__(self, key):
        cls = type(self)
        if isinstance(key, abc.MutableSequence):
            if set(key).issubset(set(self.column_names)):
                indices = []
                for k in key:
                    indices.append(self.column_names.index(k))
                results = []
                for row in self.results:
                    rows = []
                    for index in indices:
                        rows.append(row[index])
                    results.append(rows)
                return cls(results, key)
            else:
                non_existing_keys = set(key).intersection(set(self.column_names))
                if len(non_existing_keys) > 1:
                    message =  f"Columns '{', '.join(list(non_existing_keys))}' cannot be found in the report"
                message =  f"Column '{non_existing_keys.pop()}' cannot be found in the report"
                raise TypeError(message)
        else:
            if key in self.column_names:
                index = self.column_names.index(key)
                results = [[row[index]] for row in self.results]
                return cls(results, [key])
        if self.multi_column_report:
            if isinstance(key, slice):
                return cls(self.results[key], self.column_names)
            return cls([self.results[key]], self.column_names)
        if isinstance(key, slice):
            return [element[0] for element in self.results[key]]
        index = operator.index(key)
        return self.results[key]

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.column_names != other.column_names:
            return false
        return self.results == other.results

    def __add__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError("Add operation is supported only for GaarfReport")
        if self.column_names != other.column_names:
            raise ValueError("column_names should be the same in GaarfReport")
        return GaarfReport(results=self.results + other.results,
                           column_names=self.column_names)


class GaarfRow:

    def __init__(self, data: Sequence[Union[int, float, str]],
                 column_names: Sequence[str]):
        self.data = data
        try:
            self.n_elements = len(data)
        except TypeError:
            self.n_elements = 1
        self.column_names = column_names

    def __getattr__(self, element: str) -> Any:
        if element in self.column_names:
            return self.data[self.column_names.index(element)]
        raise AttributeError(f"cannot find {element} element!")

    def __getitem__(self, element: Union[str, int]) -> Any:
        if isinstance(element, int) and element < self.n_elements:
            return self.data[element]
        if isinstance(element, str):
            return self.__getattr__(element)
        raise IndexError(f"cannot find {element} element!")

    def get(self, item: str) -> Any:
        return self.__getattr__(item)

    def __repr__(self):
        dict_data = {x[1]: x[0] for x in zip(self.data, self.column_names)}
        return f"GaarfRow(\n{dict_data}\n)"
