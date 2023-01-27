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

from typing import Union

import os
from datetime import datetime
import pathlib
import yaml

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from gaarf.io.writer import WriterFactory
from gaarf.io.reader import FileReader

from airflow_gaarf.utils import GaarfExecutor, GaarfBqExecutor, GaarfMccExpander

solution_directory = r"absolute-path-to-solution-directory"
path_to_config = r"config.yaml"
dag_name = "04_gaarf_solutions_directory"

google_ads_queries = pathlib.Path(solution_directory + "google_ads_queries")
bq_queries = pathlib.Path(solution_directory + "bq_queries")

with open(path_to_config, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

gaarf = config.get("gaarf")
output_option = gaarf.get("output")
writer_client = WriterFactory().create_writer(output_option,
                                              **gaarf.get(output_option))
reader_client = FileReader()

accounts = GaarfMccExpander().expand_seed_customer_id(
    customer_id=gaarf.get("account"),
    customer_ids_query=gaarf.get("customer_ids_query"))

query_params = gaarf.get("params")

bq_query_params = config.get("gaarf-bq").get("params")

gaarf_executor = GaarfExecutor(query_params, accounts, writer_client,
                               reader_client)
bq_executor = GaarfBqExecutor(bq_query_params, reader_client)

with DAG(dag_name,
         schedule_interval=None,
         start_date=datetime(2021, 1, 1),
         catchup=False) as dag:

    def run_gaarf(base_group_id: str,
                  queries_path: pathlib.Path,
                  executor: Union[GaarfExecutor, GaarfBqExecutor],
                  start: DummyOperator = None,
                  recursive: bool = False,
                  tooltip: str = ""):
        if not start:
            start = DummyOperator(task_id=f"start_{base_group_id}")
        end = DummyOperator(task_id=f"{base_group_id}_done")
        with TaskGroup(group_id=base_group_id, tooltip=tooltip):
            for query in queries_path.glob("*"):
                if query.is_dir():
                    if recursive:
                        with TaskGroup(group_id=os.path.basename(query)):
                            for query in query.glob("*"):
                                execute_query = executor.run(query)
                                start >> execute_query >> end
                else:
                    execute_query = executor.run(query)
                    start >> execute_query >> end
        return end

    end_fetching = run_gaarf("fetching",
                             google_ads_queries,
                             gaarf_executor,
                             recursive=True)
    end_output_tables = run_gaarf("generate_output_tables", bq_queries,
                                  bq_executor, end_fetching)
