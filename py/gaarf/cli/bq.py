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

import os
import argparse
from concurrent import futures
from gaarf.io import reader  # type: ignore
from gaarf.bq_executor import BigQueryExecutor
from .utils import ParamsParser, ExecutorParamsParser, ConfigSaver


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("--project", dest="project")
    parser.add_argument("--target", dest="dataset")
    parser.add_argument("--save-config", dest="save_config", action="store_true")
    parser.add_argument("--no-save-config", dest="save_config", action="store_false")
    parser.set_defaults(save_config=False)
    args = parser.parse_known_args()
    main_args = args[0]
    query_args = args[1]

    params = ParamsParser(["macro", "sql", "template"]).parse(query_args)

    if main_args.save_config:
        config = ConfigSaver("config.yaml")
        config.save(main_args, params, "gaarf-bq")

    bq_executor = BigQueryExecutor(main_args.project)
    query_params = ExecutorParamsParser(params).parse()
    reader_client = reader.FileReader()

    with futures.ThreadPoolExecutor() as executor:
        future_to_query = {
            executor.submit(bq_executor.execute, query,
                            reader_client.read(query), query_params): query
            for query in main_args.query
        }
        for future in futures.as_completed(future_to_query):
            query = future_to_query[future]
            try:
                future.result()
                print(f"{query} executed successfully")
            except Exception as e:
                print(f"{query} generated an exception: {e}")


if __name__ == "__main__":
    main()
