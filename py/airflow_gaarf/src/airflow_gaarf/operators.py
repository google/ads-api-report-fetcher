from typing import Dict, List, Sequence

from airflow.models import BaseOperator  # type: ignore
from airflow.utils.context import Context

from gaarf.utils import get_customer_ids
from gaarf.query_executor import AdsQueryExecutor  # type: ignore
from gaarf.cli.utils import ExecutorParamsParser  # type: ignore
from gaarf.io.writer import AbsWriter  # type: ignore
from gaarf.io.reader import AbsReader  # type: ignore

from .hooks import GaarfHook, GaarfBqHook  # type: ignore

class GaarfOperator(BaseOperator):

    template_files: Sequence[str] = ("query", "customer_ids", "query_params")

    def __init__(self,
                 query: str,
                 query_params: Dict[str, str],
                 customer_ids: List[str],
                 writer_client: AbsWriter,
                 reader_client: AbsReader,
                 google_ads_conn_id: str,
                 api_version: str,
                 page_size: int = 10000,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.query_params = query_params
        self.customer_ids = customer_ids  #TODO: rename to seed customer_ids
        self.writer_client = writer_client
        self.reader_client = reader_client
        self.google_ads_conn_id = google_ads_conn_id
        self.api_version = api_version
        self.page_size = page_size

    def execute(self, context: 'Context') -> None:
        client = GaarfHook(
            google_ads_conn_id=self.google_ads_conn_id,
            api_version=self.api_version)
        self.customer_ids = get_customer_ids(client.get_client, self.customer_ids)
        executor = AdsQueryExecutor(client.get_client)
        executor.execute(self.query, self.customer_ids, self.reader_client,
                         self.writer_client, self.query_params)


class GaarfBqOperator(BaseOperator):

    template_files: Sequence[str] = ("query", "query_params")

    def __init__(self,
                 query: str,
                 query_params: Dict[str, str],
                 reader_client: AbsReader,
                 gcp_conn_id: str,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.query_params = query_params
        self.reader_client = reader_client
        self.gcp_conn_id = gcp_conn_id


    def execute(self, context: 'Context') -> None:
        bq_executor = GaarfBqHook(self.gcp_conn_id).get_bq_executor
        query_params = ExecutorParamsParser(self.query_params).parse()
        bq_executor.execute(self.query, self.reader_client.read(self.query), query_params)
