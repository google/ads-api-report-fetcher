/**
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  BigQueryExecutor,
  BigQueryExecutorOptions,
  getMemoryUsage,
} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework';
import express from 'express';
import {getProject, getScript, startPeriodicMemoryLogging} from './utils.js';
import {createLogger, ILogger} from './logger.js';

async function main_bq_unsafe(
  req: express.Request,
  res: express.Response,
  projectId: string,
  logger: ILogger
) {
  const options: BigQueryExecutorOptions = {
    datasetLocation: <string>req.query.dataset_location,
  };
  const {queryText, scriptName} = await getScript(req, logger);
  const executor = new BigQueryExecutor(<string>projectId, options);

  const body = req.body || {};
  const sqlParams = body.sql;
  const macroParams = body.macro;

  const result = await executor.execute(scriptName, queryText, {
    sqlParams,
    macroParams,
  });
  if (result && result.length) {
    res.json({rowCount: result.length});
  } else {
    res.sendStatus(200);
  }
}

export const main_bq: HttpFunction = async (
  req: express.Request,
  res: express.Response
) => {
  const dumpMemory = !!(req.query.dump_memory || process.env.DUMP_MEMORY);
  const projectId = await getProject();
  const logger = createLogger(
    req,
    projectId,
    process.env.K_SERVICE || 'gaarf-bq'
  );
  logger.info('request', {body: req.body, query: req.query});
  let dispose;
  if (dumpMemory) {
    logger.info(getMemoryUsage('Start'));
    dispose = startPeriodicMemoryLogging(logger, 60_000);
  }

  try {
    await main_bq_unsafe(req, res, projectId, logger);
  } catch (e) {
    console.error(e);
    logger.error(e.message, {error: e});
    res.status(500).send(e.message).end();
  } finally {
    if (dumpMemory) {
      if (dispose) dispose();
      logger.info(getMemoryUsage('End'));
    }
  }
};
