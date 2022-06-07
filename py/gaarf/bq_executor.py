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

import dataclasses
from typing import Any, Dict, Sequence
from google.cloud import bigquery  # type: ignore
import datetime


@dataclasses.dataclass
class BigQueryExecutorParams:
    sql_params: Dict[str, Any]
    macro_params: Dict[str, Any]
    target: str
    write_disposition: str


class BigQueryParamsParser:

    common_macros = {"date_iso": datetime.date.today().strftime("%Y%m%d")}

    def __init__(self, macros: Sequence[Any]):
        self.macros = macros

    def parse(self):
        return self._parse_macros(self.macros)

    def _parse_macros(self, macros: Sequence[Any]) -> BigQueryExecutorParams:
        parsed_macros = {}
        if macros:
            raw_macros = [macro.split("=", maxsplit=1) for macro in macros]
            for macro in raw_macros:
                parsed_macros.update(self._identify_macro_pair(macro))
            parsed_macros.update(self.common_macros)
        return BigQueryExecutorParams(sql_params={},
                                      macro_params=parsed_macros,
                                      target="",
                                      write_disposition="")

    def _identify_macro_pair(self, macro: Sequence[str]) -> Dict[str, Any]:
        macro_idenfifier = "--macro."
        key = macro[0].replace(macro_idenfifier, "")
        if len(macro) == 2:
            return {key: macro[1]}
        raise ValueError(f"macro {key} is invalid,"
                         "--macro.key=value is the correct format")


class BigQueryExecutor:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project_id)

    def execute(self, script_name: str, query_text: str,
                params: BigQueryExecutorParams) -> None:
        formatted_query = query_text.format(**params.macro_params)
        job = self.client.query(formatted_query)
        try:
            job.result()
            print(f"{script_name} launched successfully")
        except Exception as e:
            print(f"Error launching {script_name} query!" f"{str(e)}")
