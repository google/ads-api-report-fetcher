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
import {getScript} from './utils';
import {BigQueryExecutorOptions} from 'google-ads-api-report-fetcher/src/lib/bq-executor';

export const main_bq: HttpFunction = async (
  req: express.Request,
  res: express.Response
) => {
  console.log(req.body);
  console.log(req.query);

  const projectId = req.query.project_id || process.env.PROJECT_ID;
  // note: projectId isn't mandatory (should be detected from ADC)

  const options: BigQueryExecutorOptions = {
    datasetLocation: <string>req.query.dataset_location,
  };
  const {queryText, scriptName} = await getScript(req);
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
};
