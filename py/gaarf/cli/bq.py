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
"""Module for defing `gaarf-bq` CLI utility.

`gaarf-bq` allows to execute BigQuery queries based on Gaarf config.
"""

from __future__ import annotations

import argparse
import functools
from concurrent import futures

from gaarf import bq_executor
from gaarf.cli import utils
from gaarf.io import reader  # type: ignore


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('query', nargs='+')
  parser.add_argument('-c', '--config', dest='gaarf_config', default=None)
  parser.add_argument('--project', dest='project')
  parser.add_argument(
    '--dataset-location', dest='dataset_location', default=None
  )
  parser.add_argument('--save-config', dest='save_config', action='store_true')
  parser.add_argument(
    '--no-save-config', dest='save_config', action='store_false'
  )
  parser.add_argument(
    '--config-destination', dest='save_config_dest', default='config.yaml'
  )
  parser.add_argument('--log', '--loglevel', dest='loglevel', default='info')
  parser.add_argument('--logger', dest='logger', default='local')
  parser.add_argument('--dry-run', dest='dry_run', action='store_true')
  parser.add_argument(
    '--parallel-queries', dest='parallel_queries', action='store_true'
  )
  parser.add_argument(
    '--no-parallel-queries', dest='parallel_queries', action='store_false'
  )
  parser.add_argument(
    '--parallel-threshold', dest='parallel_threshold', default=None
  )
  parser.set_defaults(save_config=False)
  parser.set_defaults(dry_run=False)
  parser.set_defaults(parallel_queries=True)
  args = parser.parse_known_args()
  main_args = args[0]

  logger = utils.init_logging(
    loglevel=main_args.loglevel.upper(), logger_type=main_args.logger
  )
  config = utils.ConfigBuilder('gaarf-bq').build(vars(main_args), args[1])
  logger.debug('config: %s', config)
  if main_args.save_config and not main_args.gaarf_config:
    utils.ConfigSaver(main_args.save_config_dest).save(config)
  if main_args.dry_run:
    exit()

  config = utils.initialize_runtime_parameters(config)
  logger.debug('initialized config: %s', config)

  bigquery_executor = bq_executor.BigQueryExecutor(
    project_id=config.project, location=config.dataset_location
  )
  bigquery_executor.create_datasets(config.params.get('macro'))

  reader_client = reader.FileReader()

  if main_args.parallel_queries:
    logger.info('Running queries in parallel')
    with futures.ThreadPoolExecutor(
      max_workers=main_args.parallel_threshold
    ) as executor:
      future_to_query = {
        executor.submit(
          bigquery_executor.execute,
          query,
          reader_client.read(query),
          config.params,
        ): query
        for query in sorted(main_args.query)
      }
      for future in futures.as_completed(future_to_query):
        query = future_to_query[future]
        utils.postprocessor_runner(query, future.result, logger)
  else:
    logger.info('Running queries sequentially')
    for query in sorted(main_args.query):
      callback = functools.partial(
        executor.execute, query, reader_client.read(query), config.params
      )
      utils.postprocessor_runner(query, callback, logger)


if __name__ == '__main__':
  main()
