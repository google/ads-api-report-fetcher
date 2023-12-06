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
from collections.abc import MutableSequence
from concurrent import futures
from enum import Enum, auto
import itertools
from google.ads.googleads.errors import GoogleAdsException  # type: ignore
import logging
from google.api_core import exceptions
from tenacity import Retrying, RetryError, retry_if_exception_type, stop_after_attempt, wait_exponential
import warnings

from . import parsers
from . import api_clients
from . import builtin_queries
from .query_editor import QuerySpecification, QueryElements
from .report import GaarfReport, GaarfRow
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
    """

    def __init__(self,
                 api_client: api_clients.BaseClient,
                 customer_ids: Optional[Any] = None) -> None:
        self.api_client = api_client
        if customer_ids:
            warnings.warn(
                "`AdsReportFetcher` will deprecate passing `customer_ids` to `__init__` method. "
                "Consider passing list of customer_ids to `AdsReportFetcher.fetch` method",
                category=DeprecationWarning,
                stacklevel=3)
            self.customer_ids = [
                customer_ids
            ] if not isinstance(customer_ids, list) else customer_ids

    def expand_mcc(self,
                   customer_ids: Union[str, MutableSequence],
                   customer_ids_query: Optional[str] = None) -> List[str]:
        return self._get_customer_ids(customer_ids, customer_ids_query)

    def fetch(self,
              query_specification: Union[str, QueryElements],
              customer_ids: Optional[Union[List[str], str]] = None,
              customer_ids_query: Optional[str] = None,
              expand_mcc: bool = False,
              args: Optional[Dict[str, Any]] = None,
              optimize_strategy: str = "NONE") -> GaarfReport:
        """Fetches data from Ads API based on query_specification.

        Args:
            query_specification: Query text that will be passed to Ads API
                alongside column_names, customizers and virtual columns.
            customer_ids: Account(s) for from which data should be fetched.
            custom_query: GAQL query used to reduce the number of customer_ids.
            expand_mcc: Whether to perform expansion of root customer_ids
                into leaf accounts.
            args: Arguments that need to be passed to the query
            optimize_strategy: strategy for speeding up query execution
                ("NONE", "PROTOBUF", "BATCH", "BATCH_PROTOBUF").

        Returns:
            GaarfReport with results of query execution.
        """

        if isinstance(self.api_client, api_clients.GoogleAdsApiClient):
            if not customer_ids:
                warnings.warn(
                    "`AdsReportFetcher` will require passing `customer_ids` to `fetch` method.",
                    category=DeprecationWarning,
                    stacklevel=3)
                if hasattr(self, "customer_ids"):
                    if not self.customer_ids:
                        raise ValueError(
                            "Please specify add `customer_ids` to `fetch` method"
                        )
                    customer_ids = self.customer_ids
            else:
                if expand_mcc:
                    customer_ids = self.expand_mcc(customer_ids,
                                                   customer_ids_query)
                customer_ids = [
                    customer_ids
                ] if not isinstance(customer_ids, list) else customer_ids
        else:
            customer_ids = []
        total_results: List[List[Tuple[Any]]] = []
        is_fake_report = False
        if not isinstance(query_specification, QueryElements):
            query_specification = QuerySpecification(
                text=str(query_specification),
                args=args,
                api_version=self.api_client.api_version).generate()

        if query_specification.is_builtin_query:
            if not (report := builtin_queries.BUILTIN_QUERIES.get(
                    query_specification.query_title)):
                raise ValueError(
                    f"Cannot find the built-in query '{query_specification.title}'"
                )
            return report(self, accounts=customer_ids)
        parser = parsers.GoogleAdsRowParser(query_specification)
        for customer_id in customer_ids:
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
                raise
        if not total_results:
            row = self.api_client.google_ads_row
            results = [parser.parse_ads_row(row)]
            total_results.extend(results)
            is_fake_report = True
            if not isinstance(self.api_client, api_clients.BaseClient):
                logger.warning(
                    "Query %s generated zero results, using placeholders to infer schema",
                    query_specification.query_title)
        return GaarfReport(results=total_results,
                           column_names=query_specification.column_names,
                           is_fake=is_fake_report,
                           query_specification=query_specification)

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
        try:
            for attempt in Retrying(retry=retry_if_exception_type(
                    exceptions.InternalServerError),
                                    stop=stop_after_attempt(3),
                                    wait=wait_exponential()):
                with attempt:
                    response = self.api_client.get_response(
                        entity_id=str(customer_id),
                        query_text=query_specification.query_text)
        except RetryError as retry_failure:
            logger.error("Cannot fetch data from API for query '%s' %d times",
                         query_specification.query_title,
                         retry_failure.last_attempt.attempt_number)
            raise exceptions.InternalServerError
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

    def _get_customer_ids(
            self,
            seed_customer_ids: Union[str, MutableSequence],
            customer_ids_query: Optional[str] = None) -> List[str]:
        """Gets list of customer_ids from an MCC account.

        Args:
            customer_ids: MCC account_id(s).
            custom_query: GAQL query used to reduce the number of customer_ids.
        Returns:
            All customer_ids from MCC satisfying the condition.
        """

        query = """
        SELECT customer_client.id FROM customer_client
        WHERE customer_client.manager = FALSE AND customer_client.status = "ENABLED"
        """
        query_specification = QuerySpecification(query).generate()
        if not isinstance(seed_customer_ids, MutableSequence):
            seed_customer_ids = seed_customer_ids.split(",")
        child_customer_ids = self.fetch(query_specification,
                                        seed_customer_ids).to_list()
        if customer_ids_query:
            query_specification = QuerySpecification(
                customer_ids_query).generate()
            child_customer_ids = self.fetch(query_specification,
                                            child_customer_ids)
            child_customer_ids = [
                row[0] if isinstance(row, GaarfRow) else row
                for row in child_customer_ids
            ]

        child_customer_ids = list(
            set([
                customer_id for customer_id in child_customer_ids
                if customer_id != 0
            ]))

        return child_customer_ids


class AdsQueryExecutor:
    """Class responsible for getting data from Ads API and writing it to
        local/remote storage.

    Attributes:
        api_client: a client used for connecting to Ads API.
    """

    def __init__(self, api_client: api_clients.BaseClient):
        self.report_fetcher = AdsReportFetcher(api_client)

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
            self.report_fetcher.api_client.api_version).generate()
        results = self.report_fetcher.fetch(
            query_specification=query_specification,
            customer_ids=customer_ids,
            optimize_strategy=optimize_performance)
        logger.debug("Start writing data for query %s via %s writer",
                     query_specification.query_title, type(writer_client))
        writer_client.write(results, query_specification.query_title)
        logger.debug("Finish writing data for query %s via %s writer",
                     query_specification.query_title, type(writer_client))

    def expand_mcc(self,
                   customer_ids: Union[str, MutableSequence],
                   customer_ids_query: Optional[str] = None) -> List[str]:
        return self.report_fetcher._get_customer_ids(customer_ids,
                                                     customer_ids_query)
