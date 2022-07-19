import os
import pathlib

from .operators import GaarfOperator, GaarfBqOperator


def get_query_basename(query_path: pathlib.Path) -> str:
    return os.path.basename(query_path)[:-4]


class GaarfExecutor:

    def __init__(self,
                 query_params,
                 customer_ids,
                 writer_client,
                 reader_client,
                 google_ads_conn_id="google_ads_default",
                 api_version="v10"):
        self.query_params = query_params
        self.customer_ids = customer_ids
        self.writer_client = writer_client
        self.reader_client = reader_client
        self.google_ads_conn_id = google_ads_conn_id
        self.api_version = api_version

    def run(self, query):
        return GaarfOperator(task_id=f"fetch_{get_query_basename(query)}",
                             query=str(query),
                             query_params=self.query_params,
                             customer_ids=[self.customer_ids],
                             writer_client=self.writer_client,
                             reader_client=self.reader_client,
                             google_ads_conn_id=self.google_ads_conn_id,
                             api_version=self.api_version)


class GaarfBqExecutor:

    def __init__(self, query_params, reader_client, gcp_conn_id="gcp_conn_id"):
        self.query_params = query_params
        self.reader_client = reader_client
        self.gcp_conn_id = gcp_conn_id

    def run(self, query):
        return GaarfBqOperator(
            task_id=f"postprocess_{get_query_basename(query)}",
            query=query,
            query_params=self.query_params,
            reader_client=self.reader_client,
            gcp_conn_id=self.gcp_conn_id)
