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
from __future__ import annotations

import itertools
import operator
import warnings
from collections import defaultdict
from collections.abc import MutableSequence
from collections.abc import Sequence
from typing import Any
from typing import Literal

import pandas as pd
from gaarf import exceptions
from gaarf import query_editor


class GaarfReport:

    def __init__(
        self,
        results: Sequence,
        column_names: Sequence[str],
        results_placeholder: Sequence | None = None,
        query_specification: query_editor.QuerySpecification | None = None
    ) -> None:
        self.results = results
        self.column_names = column_names
        self.multi_column_report = len(column_names) > 1
        if results_placeholder:
            self.results_placeholder = list(results_placeholder)
        else:
            self.results_placeholder = list()
        self.query_specification = query_specification

    def to_list(self,
                row_type: Literal['list', 'dict', 'scalar'] = 'list',
                flatten: bool = False,
                distinct: bool = False) -> Sequence:
        if flatten:
            warnings.warn(
                '`GaarfReport` will deprecate passing `flatten=True` '
                "to `to_list` method. Use row_type='scalar' instead.",
                category=DeprecationWarning,
                stacklevel=3)
            row_type = 'scalar'
        if row_type == 'list':
            if self.multi_column_report:
                if distinct:
                    return list(set(self.results))
                return self.results
            return self.to_list(row_type='scalar')
        if row_type == 'dict':
            results: list[dict] = []
            for row in iter(self):
                results.append(row.to_dict())
            return results
        if row_type == 'scalar':
            results = list(itertools.chain.from_iterable(self.results))
            if distinct:
                results = list(set(results))
            return results
        raise exceptions.GaarfReportException('incorrect row_type specified',
                                              row_type)

    def to_dict(
            self,
            key_column: str,
            value_column: str | None = None,
            value_column_output: Literal['scalar', 'list'] = 'list') -> dict:
        if key_column not in self.column_names:
            raise exceptions.GaarfReportException(
                f'column name {key_column} not found in the report')
        if value_column and value_column not in self.column_names:
            raise exceptions.GaarfReportException(
                f'column name {value_column} not found in the report')
        if value_column_output == 'list':
            output: dict = defaultdict(list)
        else:
            output = {}
        key_index = self.column_names.index(key_column)
        if not (key_generator := list(zip(*self.results))):
            return {key_column: None}
        key_generator = key_generator[key_index]
        if value_column:
            value_index = self.column_names.index(value_column)
            value_generator = list(zip(*self.results))[value_index]
        else:
            value_generator = self.results
        for (key, value) in zip(key_generator, value_generator):
            if not value_column:
                value = dict(zip(self.column_names, value))
            if value_column_output == 'list':
                output[key].append(value)
            else:
                if key in output:
                    raise exceptions.GaarfReportException(
                        f'Non unique values found for key_column: {key}')
                output[key] = value
        return output

    def to_pandas(self) -> pd.DataFrame:
        return pd.DataFrame(data=self.results, columns=self.column_names)

    def __len__(self):
        return len(self.results)

    def __iter__(self):
        if self.results_placeholder:
            return None
        for result in self.results:
            yield GaarfRow(result, self.column_names)

    def __bool__(self):
        return bool(self.results)

    def __str__(self):
        return self.to_pandas().to_string()

    def __getitem__(self, key):
        cls = type(self)
        if isinstance(key, MutableSequence):
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
                non_existing_keys = set(key).intersection(
                    set(self.column_names))
                if len(non_existing_keys) > 1:
                    message = (
                        f"Columns '{', '.join(list(non_existing_keys))}' "
                        'cannot be found in the report')
                message = (f"Column '{non_existing_keys.pop()}' "
                           'cannot be found in the report')
                raise exceptions.GaarfReportException(message)
        else:
            if key in self.column_names:
                index = self.column_names.index(key)
                results = [[row[index]] for row in self.results]
                return cls(results, [key])
        if self.multi_column_report:
            if isinstance(key, slice):
                return cls(self.results[key], self.column_names)
            return GaarfRow(self.results[key], self.column_names)
        if isinstance(key, slice):
            return [element[0] for element in self.results[key]]
        index = operator.index(key)
        return self.results[key]

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.column_names != other.column_names:
            return False
        return self.results == other.results

    def __add__(self, other):
        if not isinstance(other, self.__class__):
            raise exceptions.GaarfReportException(
                'Add operation is supported only for GaarfReport')
        if self.column_names != other.column_names:
            raise exceptions.GaarfReportException(
                'column_names should be the same in GaarfReport')
        return GaarfReport(
            results=self.results + other.results,
            column_names=self.column_names,
            results_placeholder=self.results_placeholder and
            other.results_placeholder)

    @classmethod
    def from_pandas(cls, df: pd.DataFrame):
        return cls(
            results=df.values.tolist(), column_names=list(df.columns.values))


class GaarfRow:

    def __init__(self, data: Sequence[int | float | str],
                 column_names: Sequence[str]):
        super().__setattr__('data', data)
        super().__setattr__('column_names', column_names)

    def to_dict(self) -> dict:
        return {x[1]: x[0] for x in zip(self.data, self.column_names)}

    def __getattr__(self, element: str) -> Any:
        if element in self.column_names:
            return self.data[self.column_names.index(element)]
        raise AttributeError(f'cannot find {element} element!')

    def __getitem__(self, element: str | int) -> Any:
        if isinstance(element, int):
            if element < len(self.column_names):
                return self.data[element]
            raise exceptions.GaarfReportException(
                f'cannot find data in position {element}!')
        if isinstance(element, str):
            return self.__getattr__(element)
        raise exceptions.GaarfReportException(f'cannot find {element} element!')

    def __setattr__(self, name: str, value: str | int) -> None:
        self.__setitem__(name, value)

    def __setitem__(self, name: str, value: str | int) -> None:
        if name in self.column_names:
            if len(self.column_names) == len(self.data):
                self.data[self.column_names.index(name)] = value
            else:
                self.data.append(value)
        else:
            self.data.append(value)
            self.column_names.append(name)

    def get(self, item: str) -> Any:
        return self.__getattr__(item)

    def __iter__(self):
        for field in self.data:
            yield field

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        if self.column_names != other.column_names:
            return False
        return self.data == other.data

    def __repr__(self):
        return f'GaarfRow(\n{self.to_dict()}\n)'
