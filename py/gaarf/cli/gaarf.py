# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict

from concurrent import futures
import argparse
from pathlib import Path
import logging
from rich.logging import RichHandler
from smart_open import open
import yaml

from gaarf import api_clients, utils, query_executor
from gaarf.io import writer, reader  # type: ignore
from .utils import GaarfConfigBuilder, ConfigSaver, initialize_runtime_parameters, gaarf_runner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("-c", "--config", dest="gaarf_config", default=None)
    parser.add_argument("--account", dest="customer_id", default=None)
    parser.add_argument("--output", dest="save", default="console")
    parser.add_argument("--input", dest="input", default="file")
    parser.add_argument("--ads-config",
                        dest="config",
                        default=str(Path.home() / "google-ads.yaml"))
    parser.add_argument("--api-version", dest="api_version", default=12)
    parser.add_argument("--log", "--loglevel", dest="loglevel", default="info")
    parser.add_argument("--customer-ids-query",
                        dest="customer_ids_query",
                        default=None)
    parser.add_argument("--customer-ids-query-file",
                        dest="customer_ids_query_file",
                        default=None)
    parser.add_argument("--save-config",
                        dest="save_config",
                        action="store_true")
    parser.add_argument("--no-save-config",
                        dest="save_config",
                        action="store_false")
    parser.add_argument("--config-destination",
                        dest="save_config_dest",
                        default="config.yaml")
    parser.add_argument("--parallel-queries",
                        dest="parallel_queries",
                        action="store_true")
    parser.add_argument("--no-parallel-queries",
                        dest="parallel_queries",
                        action="store_false")
    parser.add_argument("--optimize-performance",
                        dest="optimize_performance",
                        default="NONE")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true")
    parser.set_defaults(save_config=False)
    parser.set_defaults(parallel_queries=True)
    parser.set_defaults(dry_run=False)
    args = parser.parse_known_args()
    main_args = args[0]

    logging.basicConfig(format="%(message)s",
                        level=main_args.loglevel.upper(),
                        datefmt="%Y-%m-%d %H:%M:%S",
                        handlers=[RichHandler(rich_tracebacks=True)])
    logging.getLogger("google.ads.googleads.client").setLevel(logging.WARNING)
    logging.getLogger("smart_open.smart_open_lib").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    with open(main_args.config, "r", encoding="utf-8") as f:
        google_ads_config_dict = yaml.safe_load(f)

    config = GaarfConfigBuilder(args).build()
    if account := main_args.customer_id:
        config.account = account
    if not config.account:
        if mcc := google_ads_config_dict.get("login_customer_id"):
            config.account = str(mcc)
        else:
            raise ValueError(
                "No account found, please specify via --account CLI flag"
                "or add as login_customer_id in google-ads.yaml")
    logger.debug("config: %s", config)

    if main_args.save_config and not main_args.gaarf_config:
        ConfigSaver(main_args.save_config_dest).save(config)
    if main_args.dry_run:
        exit()

    config = initialize_runtime_parameters(config)
    logger.debug("initialized config: %s", config)

    ads_client = api_clients.GoogleAdsApiClient(
        config_dict=google_ads_config_dict, version=f"v{config.api_version}")
    reader_factory = reader.ReaderFactory()
    reader_client = reader_factory.create_reader(main_args.input)

    if config.customer_ids_query:
        customer_ids_query = config.customer_ids_query
    elif config.customer_ids_query_file:
        file_reader = reader_factory.create_reader("file")
        customer_ids_query = file_reader.read(config.customer_ids_query_file)
    else:
        customer_ids_query = None

    customer_ids = utils.get_customer_ids(ads_client, config.account,
                                          customer_ids_query)
    if customer_ids:
        writer_client = writer.WriterFactory().create_writer(
            config.output, **config.writer_params)
        if config.output == "bq":
            _ = writer_client.create_or_get_dataset()
        ads_query_executor = query_executor.AdsQueryExecutor(ads_client)

        logger.info("Total number of customer_ids is %d, accounts=[%s]",
                    len(customer_ids), ",".join(map(str, customer_ids)))

        if main_args.parallel_queries:
            logger.info("Running queries in parallel")
            with futures.ThreadPoolExecutor() as executor:
                future_to_query = {
                    executor.submit(ads_query_executor.execute,
                                    reader_client.read(query), query,
                                    customer_ids, writer_client, config.params,
                                    main_args.optimize_performance): query
                    for query in main_args.query
                }
                for future in futures.as_completed(future_to_query):
                    query = future_to_query[future]
                    gaarf_runner(query, future.result, logger)
        else:
            logger.info("Running queries sequentially")
            for query in main_args.query:
                callback = ads_query_executor.execute(
                    reader_client.read(query), query, customer_ids,
                    writer_client, config.params,
                    main_args._optimize_performance)
                gaarf_runner(query, callback, logger)
    else:
        logger.warning(
            "Not a single under MCC %s is found that satisfies the following customer_id query: '%s'",
            config.account, customer_ids_query)


if __name__ == "__main__":
    main()
