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
# limitations under the License.import proto

from typing import Callable, Dict
import re

from .report import GaarfRow, GaarfReport


def get_ocid_mapping(report_fetcher, accounts) -> GaarfReport:
    query = "SELECT customer.id, metrics.optimization_score_url FROM campaign LIMIT 1"
    mapping = []
    for account in accounts:
        report = report_fetcher.fetch(query, account)
        if report and (ocid := re.findall("ocid=(\w+)", report[0][1])):
            mapping.append((report[0][0], ocid[0]))
        else:
            mapping.append((int(account), "0"))
    return GaarfReport(results=mapping, column_names=("account_id", "ocid"))


BUILTIN_QUERIES: Dict[str, Callable] = {
    "ocid_mapping": get_ocid_mapping
}
