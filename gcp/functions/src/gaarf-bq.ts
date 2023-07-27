/**
 * Copyright 2022 Google LLC
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
import {BigQueryExecutor} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';
import {getProject, getScript} from './utils';
import {BigQueryExecutorOptions} from 'google-ads-api-report-fetcher/src/lib/bq-executor';
import {createLogger, ILogger} from './logger';

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
  const projectId = await getProject();
  const logger = createLogger(
    req,
    projectId,
    process.env.K_SERVICE || 'gaarf-bq'
  );
  await logger.info('request', {body: req.body, query: req.query});

  try {
    await main_bq_unsafe(req, res, projectId, logger);
  } catch (e) {
    console.log(e);
    await logger.error(e.message, {error: e});
    res.status(500).send(e.message).end();
  }
};
