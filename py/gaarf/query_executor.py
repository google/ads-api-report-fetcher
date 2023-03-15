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
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
from concurrent import futures
from enum import Enum, auto
import itertools
from google.ads.googleads.errors import GoogleAdsException  # type: ignore
import logging

from . import parsers
from . import api_clients
from .query_editor import QuerySpecification, QueryElements
from .report import GaarfReport
from .io import writer, reader  # type: ignore

logger = logging.getLogger(__name__)


class OptimizeStrategy(Enum):
    NONE = auto()
    BATCH = auto()
    PROTOBUF = auto()
    BATCH_PROTOBUF = auto()


class AdsReportFetcher:
    """Class responsible for getting data from Ads API.

    Attributes:
        api_client: a client used for connecting to Ads API.
        customer_ids: account_id(s) from which data will be fetches.
    """

    def __init__(self, api_client: api_clients.BaseClient,
                 customer_ids: Union[List[str], str]) -> None:

        self.api_client = api_client
        self.customer_ids = [
            customer_ids
        ] if not isinstance(customer_ids, list) else customer_ids

    def fetch(self,
              query_specification: Union[str, QueryElements],
              optimize_strategy: str = "NONE") -> GaarfReport:
        """Fetches data from Ads API based on query_specification.

        Args:
            query_specification: query text that will be passed to Ads API
                alongside column_names, customizers and virtual columns.
            optimize_strategy: strategy for speeding up query execution
                ("NONE", "PROTOBUF", "BATCH", "BATCH_PROTOBUF").

        Returns:
            GaarfReport with results of query execution.
        """

        total_results: List[List[Tuple[Any]]] = []
        is_fake_report = False
        if not isinstance(query_specification, QueryElements):
            query_specification = QuerySpecification(
                text=str(query_specification),
                api_version=self.api_client.api_version).generate()

        parser = parsers.GoogleAdsRowParser(query_specification)
        for customer_id in self.customer_ids:
            logger.debug("Running query %s for customer_id %s",
                         query_specification.query_title, customer_id)
            try:
                results = self._parse_ads_response(
                    query_specification, customer_id, parser,
                    getattr(OptimizeStrategy, optimize_strategy))
                total_results.extend(results)
                if query_specification.is_constant_resource:
                    logger.debug("Constant resource query: running only once")
                    break
            except GoogleAdsException as e:
                logger.error("Cannot execute query %s for %s",
                             query_specification.query_title, customer_id)
                logger.error(str(e))
        if not total_results:
            row = self.api_client.google_ads_row
            results = [parser.parse_ads_row(row)]
            total_results.extend(results)
            is_fake_report = True
            logger.warning(
                "Query %s generated zero results, using placeholders to infer schema",
                query_specification.query_title)
        return GaarfReport(results=total_results,
                           column_names=query_specification.column_names,
                           is_fake=is_fake_report)

    def _parse_ads_response(
        self,
        query_specification: QueryElements,
        customer_id: str,
        parser: parsers.GoogleAdsRowParser,
        optimize_strategy: OptimizeStrategy = OptimizeStrategy.NONE
    ) -> List[List[Tuple[Any]]]:
        """Parses response returned from Ads API request.

        Args:
            query_specification: query text that will be passed to Ads API
                alongside column_names, customizers and virtual columns.
            customer_id: account for which data should be requested.
            parser: instance of parser class that transforms each row from
                request into desired format.
            optimize_strategy: strategy for speeding up query execution
                ("NONE", "PROTOBUF", "BATCH", "BATCH_PROTOBUF").

        Returns:
            Parsed rows for the whole response.
        """
        total_results: List[List[Tuple[Any]]] = []
        logger.debug("Getting response for query %s for customer_id %s",
                     query_specification.query_title, customer_id)
        response = self.api_client.get_response(
            entity_id=str(customer_id),
            query_text=query_specification.query_text)
        if optimize_strategy in (OptimizeStrategy.BATCH,
                                 OptimizeStrategy.BATCH_PROTOBUF):
            logger.warning("Running gaarf in an optimized mode")
            logger.warning("Optimize strategy is %s", optimize_strategy.name)
            if optimize_strategy == OptimizeStrategy.BATCH_PROTOBUF:
                rows = [row._pb for batch in response for row in batch.results]
            else:
                rows = [row for batch in response for row in batch.results]
            parsed_batches = []
            with futures.ThreadPoolExecutor() as executor:
                BATCH_SIZE = 10000
                batches = list(range(0, len(rows), BATCH_SIZE))
                future_to_batch = {}
                for i, batch_index in enumerate(batches):
                    try:
                        from_, to = batch_index, batches[i + 1]
                        future_to_batch[executor.submit(
                            self._parse_batch, parser,
                            itertools.islice(rows, from_,
                                             to))] = itertools.islice(
                                                 rows, from_, to)
                    except IndexError:
                        future_to_batch[executor.submit(
                            self._parse_batch, parser,
                            itertools.islice(rows, batch_index,
                                             len(rows)))] = itertools.islice(
                                                 rows, batch_index, len(rows))

                for i, future in enumerate(
                        futures.as_completed(future_to_batch), start=1):
                    batch = future_to_batch[future]
                    logger.debug(
                        "Parsed batch %d for query %s for customer_id %s", i,
                        query_specification.query_title, customer_id)
                    parsed_batch = future.result()
                    parsed_batches.append(parsed_batch)
            total_results = [
                items for batch in parsed_batches for items in batch
            ]

        else:
            logger.debug(
                "Iterating over response for query %s for customer_id %s",
                query_specification.query_title, customer_id)
            for batch in response:
                logger.debug("Parsing batch for query %s for customer_id %s",
                             query_specification.query_title, customer_id)
                if optimize_strategy == OptimizeStrategy.PROTOBUF:
                    logger.warning("Running gaarf in an optimized mode")
                    logger.warning("Optimize strategy is %s",
                                   optimize_strategy.name)
                    rows = (row._pb for row in batch.results)
                else:
                    rows = (row for row in batch.results)
                results = self._parse_batch(parser, rows)
                total_results.extend(results)
        return total_results

    def _parse_batch(
            self, parser,
            batch: Sequence["GoogleAdsRow"]) -> List[List[Tuple[Any]]]:
        """Parse reach row from batch of Ads API response.

        Args:
            parser: instance of parser class that transforms each row from
                request into desired format.
            batch: sequence of GoogleAdsRow that needs to be parsed.
        Returns:
            Parsed rows for a batch.
        """

        return [parser.parse_ads_row(row) for row in batch]


class AdsQueryExecutor:
    """Class responsible for getting data from Ads API and writing it to
        local/remote storage.

    Attributes:
        api_client: a client used for connecting to Ads API.
    """

    def __init__(self, api_client: api_clients.BaseClient):
        self.api_client = api_client

    def execute(self,
                query_text: str,
                query_name: str,
                customer_ids: Union[List[str], str],
                writer_client: writer.AbsWriter = writer.StdoutWriter(),
                args: Optional[Dict[str, Any]] = None,
                optimize_performance: str = "NONE") -> None:
        """Reads query, extract results and stores them in a specified location.

        Args:
            query_text: Text for the query.
            customer_ids: All accounts for which query will be executed.
            writer_client: Client responsible for writing data to local/remote
                location.
            args: Arguments that need to be passed to the query
            optimize_strategy: strategy for speeding up query execution
                ("NONE", "PROTOBUF", "BATCH", "BATCH_PROTOBUF")
        """

        query_specification = QuerySpecification(
            query_text, query_name, args,
            self.api_client.api_version).generate()
        report_fetcher = AdsReportFetcher(self.api_client, customer_ids)
        results = report_fetcher.fetch(query_specification,
                                       optimize_performance)
        logger.debug("Start writing data for query %s via %s writer",
                     query_specification.query_title, type(writer_client))
        writer_client.write(results, query_specification.query_title)
        logger.debug("Finish writing data for query %s via %s writer",
                     query_specification.query_title, type(writer_client))
