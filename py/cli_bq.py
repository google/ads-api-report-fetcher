import argparse
from concurrent import futures
from gaarf.io import reader  # type: ignore
from gaarf.bq_executor import BigQueryExecutor, BigQueryParamsParser


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("--project", dest="project")
    parser.add_argument("--target", dest="dataset")
    args = parser.parse_known_args()
    main_args = args[0]
    query_args = args[1]

    bq_executor = BigQueryExecutor(main_args.project)
    query_params = BigQueryParamsParser(query_args).parse()
    reader_client = reader.FileReader()

    with futures.ThreadPoolExecutor() as executor:
        future_to_query = {
            executor.submit(bq_executor.execute, query,
                            reader_client.read(query), query_params): query
            for query in main_args.query
        }
        for future in futures.as_completed(future_to_query):
            query = future_to_query[future]
            try:
                future.result()
                print(f"{query} executed successfully")
            except Exception as e:
                print(f"{query} generated an exception: {e}")


if __name__ == "__main__":
    main()
