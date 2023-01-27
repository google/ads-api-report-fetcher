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

from datetime import datetime

from airflow.models import DAG

from gaarf.io.writer import SqlAlchemyWriter
from gaarf.io.reader import FileReader

from airflow_gaarf.operators import GaarfOperator
from airflow_gaarf.utils import GaarfMccExpander


dag_name = "03_gaarf_read_file_write_to_sqlalchemy"
ACCOUNT_ID="YOUR_MCC_ID"
accounts = GaarfMccExpander(
    google_ads_conn_id="google_ads_default",
    api_version="v12").expand_seed_customer_id(ACCOUNT_ID)

path_to_query="example_query.sql"

DB_CONNECTION_STRING="'postgresql+psycopg2://user:password@hostname/database_name'"

db_writer = SqlAlchemyWriter(connection_string=DB_CONNECTION_STRING)

with DAG(dag_name,
         schedule_interval=None,
         start_date=datetime(2021, 1, 1),
         catchup=False) as dag:

    task = GaarfOperator(task_id="fetch",
                         query=path_to_query,
                         query_params={},
                         customer_ids=accounts,
                         writer_client=db_writer,
                         reader_client=FileReader(),
                         google_ads_conn_id="google_ads_default",
                         api_version="v12")

