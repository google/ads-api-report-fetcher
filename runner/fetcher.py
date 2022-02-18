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
from typing import Any, Callable, Dict, Sequence
import sys
import argparse
from pathlib import Path
import logging

from google.ads.googleads.v9.services.services.google_ads_service.client import GoogleAdsServiceClient  #type: ignore
from google.ads.googleads.errors import GoogleAdsException  #type: ignore
from operator import attrgetter

from . import parsers
from . import writer
from . import api_handler
from . import api_clients
from .query_editor import AdsQueryEditor


def process_query(query: str, customer_ids: Dict[str, str],
                  api_client: GoogleAdsServiceClient,
                  parser: parsers.BaseParser, writer_client: writer.AbsWriter,
                  args: argparse.Namespace) -> None:
    """Reads query, extract results and stores them in a specified location.

    Attributes:
        query: Path to a file that contains query text.
        customer_ids: All accounts for which query will be executed.
        api_client: Client used to perform authentication to Ads API.
        parser: Parser responsible for extracting elements from each row of
            Ads API response.
        writer_client: Client responsible for writing data to local/remote
            location.
        args: Arguments that need to be passed to a query
    """

    with open(query, "r") as f:
        raw_query_text = f.read()
    query_elements = AdsQueryEditor(raw_query_text).get_query_elements()
    print(query_elements.resource_name)
    query_text = query_elements.query_text.format(start_date=args.start_date,
                                                  end_date=args.end_date)
    getter = attrgetter(*query_elements.fields)
    total_results = []
    for customer_id in customer_ids:
        response = api_client.search_stream(customer_id=customer_id,
                                            query=query_text)
        for batch in response:
            results = [
                api_handler.parse_ads_row(row, getter, parser,
                                          query_elements.customizers)
                for row in batch.results
            ]
            logging.info(
                "[Query: %s] customer_id: %s, nrows - %d, size in bytes - %d",
                query, customer_id, len(results), sys.getsizeof(results))
            total_results.extend(results)

        if query_elements.resource_name.endswith("_constant"):
            print("Running only once")
            break

    logging.info("[Query: %s] total size in bytes - %d", query,
                 sys.getsizeof(total_results))
    if len(total_results) > 0:
        output = writer_client.write(total_results, query,
                                     query_elements.column_names)
    else:
        raise writer.ZeroRowException


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("--customer_id", dest="customer_id")
    parser.add_argument("--save", dest="save", default="csv")
    parser.add_argument("--destination-folder", dest="destination_folder")
    parser.add_argument("--bq_project", dest="project")
    parser.add_argument("--bq_dataset", dest="dataset")
    parser.add_argument("--start_date", dest="start_date")
    parser.add_argument("--end_date", dest="end_date")
    parser.add_argument("-c",
                        "--path-to-api-config",
                        dest="config",
                        default=str(Path.home() / "google-ads.yaml"))
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

    ga_service = api_clients.GoogleAdsApiClient(
        path_to_config=args.config).get_client()

    google_ads_row_parser = parsers.GoogleAdsRowParser()

    writer_factory = writer.WriterFactory()
    writer_client = writer_factory.create_writer(args.save, **vars(args))

    customer_ids = api_handler.get_customer_ids(ga_service, args.customer_id)
    logging.info("Total number of customer_ids is %d, accounts=[%s]",
                 len(customer_ids), ",".join(customer_ids))

    with futures.ThreadPoolExecutor() as executor:
        future_to_query = {
            executor.submit(process_query, query, customer_ids, ga_service,
                            google_ads_row_parser, writer_client, args): query
            for query in args.query
        }
        for future in futures.as_completed(future_to_query):
            query = future_to_query[future]
            try:
                result = future.result()
                print(f"{query} executed successfully")
            except writer.ZeroRowException:
                print(
                    f"{query} generated ZeroRowException, please check WHERE statements."
                )
            except GoogleAdsException as ex:
                print(
                    f'"{query}" failed with status '
                    f'"{ex.error.code().name}" and includes the following errors:'
                )
                for error in ex.failure.errors:
                    print(f'\tError with message "{error.message}".')
                    if error.location:
                        for field_path_element in error.location.field_path_elements:
                            print(
                                f"\t\tOn field: {field_path_element.field_name}"
                            )
            except Exception as e:
                print(f"{query} generated an exception: {str(e)}")


if __name__ == "__main__":
    main()
