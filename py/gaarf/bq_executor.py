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

from google.cloud import bigquery  # type: ignore
from jinja2 import Template

from .cli.utils import ExecutorParams


class BigQueryExecutor:
    def __init__(self, project_id: str):
        self.client = bigquery.Client(project_id)

    def execute(self, script_name: str, query_text: str,
                params: ExecutorParams) -> None:
        query_text = self._expand_jinja(query_text,
                                        **params.template_params)
        formatted_query = query_text.format(**params.macro_params)
        job = self.client.query(formatted_query)
        try:
            job.result()
            print(f"{script_name} launched successfully")
        except Exception as e:
            print(f"Error launching {script_name} query!" f"{str(e)}")

    def _expand_jinja(self, query_text, **template_params):
        for key, value in template_params.items():
            if len(splitted_param := value.split(",")) > 1:
                template_params[key] = splitted_param
        template = Template(query_text)
        return template.render(template_params)
