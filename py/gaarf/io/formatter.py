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

from typing import Type
import abc
from enum import Enum
from pathlib import Path
import proto  # type: ignore

from gaarf.report import GaarfReport


class FormattingStrategy:

    def apply_transformations(self, report: GaarfReport) -> GaarfReport:
        raise NotImplementedError

    def _cast_to_enum(self, enum: Type[Enum], value: str | Enum) -> Enum:
        return enum[value.upper()] if isinstance(value, str) else value


class ArrayHandling(Enum):
    STRINGS = 1
    ARRAYS = 2


class ArrayHandlingStrategy(FormattingStrategy):

    def __init__(self,
                 type_: ArrayHandling | str = ArrayHandling.STRINGS,
                 delimiter: str = "|") -> None:
        self.type_ = self._cast_to_enum(ArrayHandling, type_)
        self.delimiter = delimiter

    def apply_transformations(self, report: GaarfReport) -> GaarfReport:
        if self.type_ == ArrayHandling.ARRAYS:
            return report
        formatted_rows = []
        for row in report:
            formatted_row = []
            for field in row:
                if isinstance(
                        field,
                    (list,
                     proto.marshal.collections.repeated.RepeatedComposite,
                     proto.marshal.collections.repeated.Repeated)):
                    formatted_row.append(
                        self.delimiter.join(
                            [str(element) for element in field]))
                else:
                    formatted_row.append(field)
            formatted_rows.append(formatted_row)
        return GaarfReport(results=formatted_rows,
                           column_names=report.column_names)


def format_report_for_writing(
        report: GaarfReport,
        formatting_strategies: list[FormattingStrategy]) -> GaarfReport:
    for strategy in formatting_strategies:
        report = strategy.apply_transformations(report)
    return report


def format_extension(path_object: str,
                     current_extension: str = ".sql",
                     new_extension: str = "") -> str:
    path_object_name = Path(path_object).name
    if len(path_object_name.split(".")) > 1:
        return path_object_name.replace(current_extension, new_extension)
    else:
        return f"{path_object}{new_extension}"
