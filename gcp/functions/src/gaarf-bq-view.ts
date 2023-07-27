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
import path from 'node:path';
import {BigQueryExecutor} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';
import {BigQueryExecutorOptions} from 'google-ads-api-report-fetcher/src/lib/bq-executor';
import {createLogger, ILogger} from './logger';
import {getProject} from './utils';

async function main_bq_view_unsafe(
  req: express.Request,
  res: express.Response,
  projectId: string,
  logger: ILogger
) {
  console.log(req.body);
  console.log(req.query);
  const body = req.body || {};
  const datasetId = req.query.dataset || body.dataset;
  const accounts = body.accounts;

  let tableId = body.table || req.query.table;
  // note: projectId isn't mandatory (should be detected from ADC)
  const options: BigQueryExecutorOptions = {
    datasetLocation: req.query.dataset_location || body.dataset_location,
  };
  const executor = new BigQueryExecutor(<string>projectId, options);
  const scriptPath = body.script_path || req.query.script_path;
  if (scriptPath && !tableId) {
    tableId = path.basename(<string>scriptPath).split('.sql')[0];
  }
  await logger.info(
    `Creating an unified view ${datasetId}.${tableId} for ${accounts.length} accounts`
  );
  await executor.createUnifiedView(datasetId, tableId, accounts);
  res.sendStatus(200);
  res.end();
}

export const main_bq_view: HttpFunction = async (
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
    await main_bq_view_unsafe(req, res, projectId, logger);
  } catch (e) {
    console.log(e);
    await logger.error(e.message, {error: e});
    res.status(500).send(e.message).end();
  }
};
