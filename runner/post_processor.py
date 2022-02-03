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

from concurrent import futures
import os
import argparse
from itertools import repeat
from google.cloud import bigquery  #type: ignore

parser = argparse.ArgumentParser()
parser.add_argument("query", nargs="+")
parser.add_argument("--bq_project", dest="project")
parser.add_argument("--bq_dataset", dest="dataset")
args = parser.parse_args()

bq_client = bigquery.Client()


def run_post_processing_query(path: str, bq_client: bigquery.Client,
                              project: str, dataset: str) -> None:
    with open(path, "r") as f:
        query = f.read()
    formatted_query = query.format(bq_project=project, bq_dataset=dataset)
    job = bq_client.query(formatted_query)
    try:
        result = job.result()
        print(f"{path} launched successfully")
    except Exception as e:
        print(f"Error launching {path} query!" f"{str(e)}")


with futures.ThreadPoolExecutor() as executor:
    future_to_query = {
        executor.submit(run_post_processing_query, query, bq_client,
                        args.project, args.dataset): query
        for query in args.query
    }
    for future in futures.as_completed(future_to_query):
        query = future_to_query[future]
        try:
            result = future.result()
            print(f"{query} executed successfully")
        except Exception as e:
            print(f"{query} generated an exception: {e}")
