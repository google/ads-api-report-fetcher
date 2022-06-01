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
from typing import Any, Dict, List
import sys
import argparse
from pathlib import Path
import logging
import traceback

from google.ads.googleads.errors import GoogleAdsException  # type: ignore

from . import api_clients
from .io import writer, reader  # type: ignore
from .query_executor import AdsQueryExecutor


def process_query(query: str,
                  customer_ids: List[str],
                  api_client: api_clients.BaseClient,
                  reader_client: reader.AbsReader,
                  writer_client: writer.AbsWriter,
                  args: Dict[Any, Any] = None) -> None:
    """Reads query, extract results and stores them in a specified location.

    Attributes:
        query: Path to a file that contains query text.
        customer_ids: All accounts for which query will be executed.
        api_client: Client used to perform authentication to Ads API.
        reader_client: Client responsible for reading data from local storage
        writer_client: Client responsible for writing data to local/remote
            location.
        args: Arguments that need to be passed to the query
    """

    query_text = reader_client.read(query)
    query_executor = AdsQueryExecutor(api_client, query_text, query, args)
    results = query_executor.execute(customer_ids)
    if len(results) > 0:
        writer_client.write(results,
                            query_executor.query_specification.query_title,
                            query_executor.query_specification.column_names)
    else:
        raise writer.ZeroRowException


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("--account", dest="customer_id")
    parser.add_argument("--output", dest="save", default="csv")
    parser.add_argument("--csv.destination-folder", dest="destination_folder")
    parser.add_argument("--bq.project", dest="project")
    parser.add_argument("--bq.dataset", dest="dataset")
    parser.add_argument("--sql.start_date", dest="start_date")
    parser.add_argument("--sql.end_date", dest="end_date")
    parser.add_argument("--ads-config",
                        dest="config",
                        default=str(Path.home() / "google-ads.yaml"))
    parser.add_argument("--api-version", dest="api_version", default=10)
    parser.add_argument("--log",
                        "--loglevel",
                        dest="loglevel",
                        default="warning")
    args = parser.parse_args()

    logging.basicConfig(
        format="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        stream=sys.stdout,
        level=args.loglevel.upper(),
        datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger("google.ads.googleads.client").setLevel(logging.WARNING)

    ads_client = api_clients.GoogleAdsApiClient(path_to_config=args.config,
                                                version=f"v{args.api_version}")

    writer_factory = writer.WriterFactory()
    writer_client = writer_factory.create_writer(args.save, **vars(args))
    reader_client = reader.FileReader()

    query_executor = AdsQueryExecutor(
        ads_client, """
        SELECT customer_client.id FROM customer_client
        WHERE customer_client.manager = FALSE
        """, None)
    customer_ids = query_executor.execute(args.customer_id)

    logging.info("Total number of customer_ids is %d, accounts=[%s]",
                 len(customer_ids), ",".join(map(str, customer_ids)))

    with futures.ThreadPoolExecutor() as executor:
        future_to_query = {
            executor.submit(process_query, query, customer_ids, ads_client,
                            reader_client, writer_client, vars(args)): query
            for query in args.query
        }
        for future in futures.as_completed(future_to_query):
            query = future_to_query[future]
            try:
                future.result()
                print(f"{query} executed successfully")
            except writer.ZeroRowException:
                print(f"""
                    {query} generated ZeroRowException,
                    please check WHERE statements.
                    """)
            except GoogleAdsException as ex:
                print(f'"{query}" failed with status '
                      f'"{ex.error.code().name}" and includes'
                      'the following errors:')
                for error in ex.failure.errors:
                    print(f'\tError with message "{error.message}".')
                    if error.location:
                        for field in error.location.field_path_elements:
                            print(
                                f"\t\tOn field: {field.field_name}"
                            )
            except Exception as e:
                traceback.print_tb(e.__traceback__)
                print(f"{query} generated an exception: {str(e)}")


if __name__ == "__main__":
    main()
