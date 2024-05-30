# Copyright 2024 Google LLC
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
"""Defines an interface for Abstract writer."""

from __future__ import annotations

import abc
import logging
from typing import Literal

import proto  # type: ignore

from gaarf.io import formatter
from gaarf.report import GaarfReport

logger = logging.getLogger(__name__)


class AbsWriter(abc.ABC):
  def __init__(
    self,
    array_handling: Literal['strings', 'arrays'] = 'strings',
    array_separator: str = '|',
    **kwargs,
  ) -> None:
    self.array_handling = array_handling
    self.array_separator = array_separator

  @abc.abstractmethod
  def write(self, report: GaarfReport, destination: str) -> str | None:
    raise NotImplementedError

  def format_for_write(self, report: GaarfReport) -> GaarfReport:
    array_handling_strategy = formatter.ArrayHandlingStrategy(
      type_=self.array_handling, delimiter=self.array_separator
    )
    return formatter.format_report_for_writing(
      report, [array_handling_strategy]
    )
