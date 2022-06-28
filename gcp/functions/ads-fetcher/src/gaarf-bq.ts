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
import {BigQueryExecutor, getFileContent} from 'google-ads-api-report-fetcher';
import path from 'path';

import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';

export const main_bq: HttpFunction =
    async (req: express.Request, res: express.Response) => {
  console.log(req.query);

  let scriptPath = req.query.script_path;
  if (!scriptPath)
    throw new Error(
        `Ads script path is not specified in script_path query argument`);
  let projectId = req.query.project_id || process.env.PROJECT_ID;
  if (!projectId)
    throw new Error(
        `Project id is not specified in either 'project_id' query argument or PROJECT_ID envvar`);
  let target = <string>req.query.target;

  let body = req.body || {};
  let sqlParams = body.sql;
  let macroParams = body.macros;
  let queryText = await getFileContent(<string>scriptPath);
  let scriptName = path.basename(<string>scriptPath).split('.sql')[0];

  let executor = new BigQueryExecutor(<string>projectId);

  console.log(`Executing BQ-query from ${scriptPath}`);
  let result = await executor.execute(
      scriptName, queryText, {sqlParams, macroParams, target});
  if (result && result.length) {
    res.send({rowCount: result.length});
  } else {
    res.sendStatus(200);
  }
}
