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

from typing import Any, Dict, List

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
