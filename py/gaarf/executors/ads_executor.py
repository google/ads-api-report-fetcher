# Copyright 2024 Google LLC
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
"""Module for executing Gaarf queries and writing them to local/remote.

AdsQueryExecutor performs fetching data from Ads API in a form of
GaarfReport and saving it to local/remote storage.
"""

from __future__ import annotations

import logging
from collections.abc import MutableSequence

from gaarf import api_clients, query_editor, report_fetcher
from gaarf.io.writers import abs_writer, console_writer

logger = logging.getLogger(__name__)


class AdsQueryExecutor:
  """Class responsible for getting data from Ads API and writing it to
      local/remote storage.

  Attributes:
      api_client: a client used for connecting to Ads API.
  """

  def __init__(self, api_client: api_clients.BaseClient) -> None:
    """Initializes QueryExecutor.

    Args:
        api_client: a client used for connecting to Ads API.
    """
    self.api_client = api_client

  @property
  def report_fetcher(self) -> report_fetcher.AdsReportFetcher:
    """Initializes AdsReportFetcher to get data from Ads API."""
    return report_fetcher.AdsReportFetcher(self.api_client)

  def execute(
    self,
    query_text: str,
    query_name: str,
    customer_ids: list[str] | str,
    writer_client: abs_writer.AbsWriter = console_writer.ConsoleWriter(),
    args: dict[str, str] | None = None,
    optimize_performance: str = 'NONE',
  ) -> None:
    """Reads query, extract results and stores them in a specified location.

    Args:
        query_text: Text for the query.
        query_name: Identifier of a query..
        customer_ids: All accounts for which query will be executed.
        writer_client: Client responsible for writing data to local/remote
            location.
        args: Arguments that need to be passed to the query
        optimize_performance: strategy for speeding up query execution
            ("NONE", "PROTOBUF", "BATCH", "BATCH_PROTOBUF")
    """
    query_specification = query_editor.QuerySpecification(
      query_text, query_name, args, self.report_fetcher.api_client.api_version
    ).generate()
    results = self.report_fetcher.fetch(
      query_specification=query_specification,
      customer_ids=customer_ids,
      optimize_strategy=optimize_performance,
    )
    logger.debug(
      'Start writing data for query %s via %s writer',
      query_specification.query_title,
      type(writer_client),
    )
    writer_client.write(results, query_specification.query_title)
    logger.debug(
      'Finish writing data for query %s via %s writer',
      query_specification.query_title,
      type(writer_client),
    )

  def expand_mcc(
    self,
    customer_ids: str | MutableSequence,
    customer_ids_query: str | None = None,
  ) -> list[str]:
    """Performs Manager account(s) expansion to child accounts.

    Args:
        customer_ids:
            Manager account(s) to be expanded.
        customer_ids_query:
            Gaarf query to limit the expansion only to accounts
            satisfying the condition.

    Returns:
        All child accounts under provided customer_ids.
    """
    return self.report_fetcher.expand_mcc(customer_ids, customer_ids_query)
