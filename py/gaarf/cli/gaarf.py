from concurrent import futures
import sys
import argparse
from pathlib import Path
import logging
import traceback

from google.ads.googleads.errors import GoogleAdsException  # type: ignore

from gaarf import api_clients, utils, query_executor
from gaarf.io import writer, reader  # type: ignore
from .utils import ParamsParser, ExecutorParamsParser, WriterParamsParser, ConfigSaver


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("--account", dest="customer_id")
    parser.add_argument("--output", dest="save", default="csv")
    parser.add_argument("--ads-config",
                        dest="config",
                        default=str(Path.home() / "google-ads.yaml"))
    parser.add_argument("--api-version", dest="api_version", default=10)
    parser.add_argument("--log",
                        "--loglevel",
                        dest="loglevel",
                        default="warning")
    parser.add_argument("--save-config", dest="save_config", action="store_true")
    parser.add_argument("--no-save-config", dest="save_config", action="store_false")
    parser.set_defaults(save_config=False)
    args = parser.parse_known_args()
    main_args = args[0]
    query_args = args[1]

    params = ParamsParser(["macro", main_args.save]).parse(query_args)
    query_params = ExecutorParamsParser(params).parse()
    writer_params = WriterParamsParser(params).parse(main_args.save)

    if main_args.save_config:
        config = ConfigSaver("config.yaml")
        config.save(main_args, params, "gaarf")

    logging.basicConfig(
        format="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        stream=sys.stdout,
        level=main_args.loglevel.upper(),
        datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger("google.ads.googleads.client").setLevel(logging.WARNING)

    ads_client = api_clients.GoogleAdsApiClient(path_to_config=main_args.config,
                                                version=f"v{main_args.api_version}")

    writer_factory = writer.WriterFactory()
    writer_client = writer_factory.create_writer(main_args.save, **writer_params)
    reader_client = reader.FileReader()
    customer_ids = utils.get_customer_ids(ads_client, main_args.customer_id)
    ads_query_executor = query_executor.AdsQueryExecutor(ads_client)

    logging.info("Total number of customer_ids is %d, accounts=[%s]",
                 len(customer_ids), ",".join(map(str, customer_ids)))

    with futures.ThreadPoolExecutor() as executor:
        future_to_query = {
            executor.submit(ads_query_executor.execute, query, customer_ids,
                            reader_client, writer_client, query_params.macro_params): query
            for query in main_args.query
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
