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

from typing import Any, Dict, List, Sequence, Union
from google.ads.googleads.errors import GoogleAdsException  # type: ignore

from . import parsers
from . import api_clients
from .query_editor import QuerySpecification, QueryElements
from .report import GaarfReport
from .io import writer, reader  # type: ignore


class AdsReportFetcher:
    def __init__(self, api_client: api_clients.BaseClient,
                 customer_ids: Union[List[str], str]):
        self.api_client = api_client
        self.customer_ids = [
            customer_ids
        ] if not isinstance(customer_ids, list) else customer_ids

    def fetch(
        self,
        query_specification: Union[str, QueryElements],
    ) -> GaarfReport:
        total_results = []
        if not isinstance(query_specification, QueryElements):
            query_specification = QuerySpecification(
                str(query_specification)).generate()

        for customer_id in self.customer_ids:
            try:
                results = self._parse_ads_response(query_specification,
                                                   customer_id)
                total_results.extend(results)
                if query_specification.is_constant_resource:
                    print("Running only once")
                    break
            except GoogleAdsException as e:
                print("Cannot execute query for "
                      f"{query_specification.query_title} "
                      f"for customer_id {customer_id}")
                print(e)
        return GaarfReport(results=total_results,
                           column_names=query_specification.column_names)

    def _parse_ads_response(self, query_specification: QueryElements,
                            customer_id: str) -> Sequence[Any]:
        parser = parsers.GoogleAdsRowParser()
        total_results = []
        response = self.api_client.get_response(
            entity_id=str(customer_id),
            query_text=query_specification.query_text)
        for batch in response:
            results = [
                parser.parse_ads_row(row, query_specification)
                for row in batch.results
            ]
            total_results.extend(results)
        return total_results


class AdsQueryExecutor:
    def __init__(self, api_client: api_clients.BaseClient):
        """
        api_client: Client used to perform authentication to Ads API.
        """
        self.api_client = api_client

    def execute(self,
                query: str,
                customer_ids: Union[List[str], str],
                reader_client: reader.AbsReader,
                writer_client: writer.AbsWriter,
                args: Dict[Any, Any] = None) -> None:
        """Reads query, extract results and stores them in a specified location.

        Attributes:
            query: Path to a file that contains query text.
            customer_ids: All accounts for which query will be executed.
            reader_client: Client responsible for reading data from local storage
            writer_client: Client responsible for writing data to local/remote
                location.
            args: Arguments that need to be passed to the query
        """

        query_text = reader_client.read(query)
        query_specification = QuerySpecification(query_text, query,
                                                 args).generate()
        report_fetcher = AdsReportFetcher(self.api_client, customer_ids)
        results = report_fetcher.fetch(query_specification)
        if len(results) > 0:
            writer_client.write(results, query_specification.query_title)
        else:
            raise writer.ZeroRowException
