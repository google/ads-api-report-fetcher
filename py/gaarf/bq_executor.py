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

from typing import Any, Dict, List, Optional, Union
from google.cloud import bigquery  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore
from jinja2 import Template


class BigQueryExecutor:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project_id)

    def execute(self, script_name: str, query_text: str,
                params: Optional[Dict[str, Any]]) -> None:
        print(params)
        if params:
            if (templates := params.get("template")):
                query_text = self._expand_jinja(query_text,
                                                **templates)
            if (macros := params.get("macro")):
                query_text = query_text.format(**macros)
        job = self.client.query(query_text)
        try:
            job.result()
            print(f"{script_name} launched successfully")
        except Exception as e:
            print(f"Error launching {script_name} query!" f"{str(e)}")

    def create_datasets(self, datasets: Union[str, List[str]]) -> None:
        if isinstance(datasets, str):
            datasets = [datasets]
        for dataset in datasets:
            dataset_id = f"{self.project_id}.{dataset}"
            try:
                bq_dataset = self.client.get_dataset(dataset_id)
            except NotFound:
                bq_dataset = bigquery.Dataset(dataset_id)
                bq_dataset = self.client.create_dataset(bq_dataset, timeout=30)

    def _expand_jinja(self, query_text, **template_params):
        for key, value in template_params.items():
            if len(splitted_param := value.split(",")) > 1:
                template_params[key] = splitted_param
            else:
                template_params[key] = [value]
        template = Template(query_text)
        return template.render(template_params)
