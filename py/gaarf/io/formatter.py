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

from typing import Any, Sequence
import abc
import proto  # type: ignore


class AbsFormatter(abc.ABC):
    @abc.abstractstaticmethod
    def format(rows: Sequence[Sequence[Any]],
               delimiter: str) -> Sequence[Sequence[Any]]:
        raise NotImplementedError


class ArrayFormatter(AbsFormatter):
    @staticmethod
    def format(rows: Sequence[Sequence[Any]],
               delimiter: str) -> Sequence[Sequence[Any]]:
        formatted_rows = []
        for row in rows:
            formatted_row = []
            for field in row:
                if isinstance(
                        field,
                    (list,
                     proto.marshal.collections.repeated.RepeatedComposite,
                     proto.marshal.collections.repeated.Repeated)):
                    formatted_row.append(
                        delimiter.join([str(element) for element in field]))
                else:
                    formatted_row.append(field)
            formatted_rows.append(formatted_row)
        return formatted_rows


class ResultsFormatter:
    @staticmethod
    def format(results: Sequence[Any]):
        if type(results[0]) in (int, float, str, bool):
            results = [[result] for result in results]
        return results
