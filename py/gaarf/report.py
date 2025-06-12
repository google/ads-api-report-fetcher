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

# pylint: disable=C0330, g-bad-import-order, g-multiple-import

"""Simplifies handling fetched data from Ads API.

Module exposes two classes:
    * GaarfReport - contains all data from Ads API response alongside methods
      for iteration, slicing and converting to/from common structures.
    * GaarfRow - helper class for dealing with iteration over each response
      row in GaarfReport.
"""

from garf_core import report

GaarfReport = report.GarfReport
GaarfRow = report.GarfRow
