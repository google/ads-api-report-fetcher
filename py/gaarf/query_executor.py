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

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

"""Deprecated module for executing Gaarf queries."""

from __future__ import annotations

import warnings

from gaarf import exceptions

warnings.warn(
  "The 'gaarf.query_executor' namespace is deprecated. "
  "Please use 'gaarf.report_fetcher' or "
  "'gaarf.executors.ads_executor` instead.",
  DeprecationWarning,
  stacklevel=2,
)


class AdsReportFetcher:
  """Deprecated class for creating AdsReportFetcher."""

  def __init__(self, *args, **kwargs: str) -> None:
    raise exceptions.GaarfDeprecationError(
      'Loading AdsReportFetcher from `gaarf.query_executor` is '
      'deprecated; Import AdsReportFetcher from `gaarf.report_fetcher` '
      'instead',
    )


class AdsQueryExecutor:
  """Deprecated class for creating AdsQueryExecutor."""

  def __init__(self, *args, **kwargs: str) -> None:
    raise exceptions.GaarfDeprecationError(
      'Loading AdsQueryExecutor from `gaarf.query_executor` is '
      'deprecated; Import AdsQueryExecutor from `gaarf.executors.ads_executor` '
      'instead',
    )
