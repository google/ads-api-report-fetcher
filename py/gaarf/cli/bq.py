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

import os
import argparse
from concurrent import futures
import logging
import sys

from gaarf.io import reader  # type: ignore
from gaarf.bq_executor import BigQueryExecutor
from .utils import GaarfBqConfigBuilder, ConfigSaver, initialize_runtime_parameters


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", nargs="+")
    parser.add_argument("-c", "--config", dest="gaarf_config", default=None)
    parser.add_argument("--project", dest="project")
    parser.add_argument("--target", dest="dataset")
    parser.add_argument("--save-config", dest="save_config", action="store_true")
    parser.add_argument("--no-save-config", dest="save_config", action="store_false")
    parser.add_argument("--config-destination", dest="save_config_dest", default="config.yaml")
    parser.add_argument("--log",
                        "--loglevel",
                        dest="loglevel",
                        default="info")
    parser.set_defaults(save_config=False)
    args = parser.parse_known_args()
    main_args = args[0]

    logging.basicConfig(
        format="[%(asctime)s][%(name)s][%(levelname)s] %(message)s",
        stream=sys.stdout,
        level=main_args.loglevel.upper(),
        datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger("smart_open.smart_open_lib").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)

    config = GaarfBqConfigBuilder(args).build()
    logger.debug("config: %s", config)
    if main_args.save_config and not main_args.gaarf_config:
        ConfigSaver(main_args.save_config_dest).save(config)

    config = initialize_runtime_parameters(config)
    logger.debug("initialized config: %s", config)

    bq_executor = BigQueryExecutor(config.project)
    bq_executor.create_datasets(config.params.get("macro"))

    reader_client = reader.FileReader()

    with futures.ThreadPoolExecutor() as executor:
        future_to_query = {
            executor.submit(bq_executor.execute, query,
                            reader_client.read(query), config.params): query
            for query in main_args.query
        }
        for future in futures.as_completed(future_to_query):
            query = future_to_query[future]
            try:
                logger.debug("starting query %s", query)
                future.result()
                logger.info("%s executed successfully", query)
            except Exception as e:
                logger.error("%s generated an exception: %s", query, str(e))


if __name__ == "__main__":
    main()
