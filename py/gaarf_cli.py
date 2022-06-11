from concurrent import futures
import sys
import argparse
from pathlib import Path
import logging
import traceback

from google.ads.googleads.errors import GoogleAdsException  # type: ignore

from gaarf import api_clients, utils, query_executor
from gaarf.io import writer, reader  # type: ignore


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("--account", dest="customer_id")
    parser.add_argument("--output", dest="save", default="csv")
    parser.add_argument("--csv.destination-folder", dest="destination_folder")
    parser.add_argument("--bq.project", dest="project")
    parser.add_argument("--bq.dataset", dest="dataset")
    parser.add_argument("--macro.start_date", dest="start_date")
    parser.add_argument("--macro.end_date", dest="end_date")
    parser.add_argument("--ads-config",
                        dest="config",
                        default=str(Path.home() / "google-ads.yaml"))
    parser.add_argument("--api-version", dest="api_version", default=10)
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

    ads_client = api_clients.GoogleAdsApiClient(path_to_config=args.config,
                                                version=f"v{args.api_version}")

    writer_factory = writer.WriterFactory()
    writer_client = writer_factory.create_writer(args.save, **vars(args))
    reader_client = reader.FileReader()
    customer_ids = utils.get_customer_ids(ads_client, args.customer_id)
    ads_query_executor = query_executor.AdsQueryExecutor(ads_client)

    logging.info("Total number of customer_ids is %d, accounts=[%s]",
                 len(customer_ids), ",".join(map(str, customer_ids)))

    with futures.ThreadPoolExecutor() as executor:
        future_to_query = {
            executor.submit(ads_query_executor.execute, query, customer_ids,
                            reader_client, writer_client, vars(args)): query
            for query in args.query
        }
        for future in futures.as_completed(future_to_query):
            query = future_to_query[future]
            try:
                future.result()
                print(f"{query} executed successfully")
            except writer.ZeroRowException:
                print(f"""
                    {query} generated ZeroRowException,
                    please check WHERE statements.
                    """)
            except GoogleAdsException as ex:
                print(f'"{query}" failed with status '
                      f'"{ex.error.code().name}" and includes'
                      'the following errors:')
                for error in ex.failure.errors:
                    print(f'\tError with message "{error.message}".')
                    if error.location:
                        for field in error.location.field_path_elements:
                            print(
                                f"\t\tOn field: {field.field_name}"
                            )
            except Exception as e:
                traceback.print_tb(e.__traceback__)
                print(f"{query} generated an exception: {str(e)}")


if __name__ == "__main__":
    main()
