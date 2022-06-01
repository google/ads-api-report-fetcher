from . import parsers
from .query_editor import QuerySpecification


class AdsQueryExecutor:
    def __init__(self, api_client, text, title=None, args=None):
        self.query_specification = QuerySpecification(title, text,
                                                      args).generate()
        self.api_client = api_client
        self.parser = parsers.GoogleAdsRowParser()

    def execute(self, customer_ids):
        total_results = []
        if not isinstance(customer_ids, list):
            customer_ids = [
                customer_ids,
            ]
        for customer_id in customer_ids:
            results = self._parse_ads_response(customer_id)
            total_results.extend(results)
            if self.query_specification.is_constant_resource:
                print("Running only once")
                break
        return total_results

    def _parse_ads_response(self, customer_id):
        total_results = []
        response = self.api_client.get_response(
            entity_id=str(customer_id),
            query_text=self.query_specification.query_text)
        for batch in response:
            results = [
                self.parser.parse_ads_row(row, self.query_specification)
                for row in batch.results
            ]
            total_results.extend(results)
        return total_results
