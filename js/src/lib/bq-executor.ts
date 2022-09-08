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
import {BigQuery, Dataset, Query, SimpleQueryRowsResponse, Table, TableOptions} from '@google-cloud/bigquery';
import bigquery from '@google-cloud/bigquery/build/src/types';

import logger from './logger';
import {substituteMacros} from './utils';

export var OAUTH_SCOPES = [
  'https://www.googleapis.com/auth/cloud-platform',
  'https://www.googleapis.com/auth/cloud-platform.read-only',
  'https://www.googleapis.com/auth/bigquery',
  'https://www.googleapis.com/auth/bigquery.readonly',
];

export interface BigQueryExecutorOptions {
  datasetLocation?: string;
  keyFilePath?: string;
}
export interface BigQueryExecutorParams {
  sqlParams?: Record<string, any>;
  macroParams?: Record<string, any>;
  writeDisposition?: string;
}
export class BigQueryExecutor {
  bigquery: BigQuery;
  datasetLocation?: string;

  constructor(projectId?: string|undefined, options?: BigQueryExecutorOptions) {
    this.bigquery = new BigQuery({
      projectId: projectId,
      scopes: OAUTH_SCOPES,
      keyFilename: options?.keyFilePath
    });
    this.datasetLocation = options?.datasetLocation;
  }
  async execute(
      scriptName: string, queryText: string,
      params?: BigQueryExecutorParams): Promise<any[]> {
    if (params?.macroParams) {
      for (const macro of Object.keys(params.macroParams)) {
        if (macro.includes('dataset')) {
          // all macros containing the word 'dataset' we treat as a dataset's name
          const value = params.macroParams[macro];
          if (value) {
            await this.getDataset(value);
          }
        }
      }
    }
    let res = substituteMacros(queryText, params?.macroParams);
    if (res.unknown_params.length) {
      throw new Error(
          `The following parameters used in '${
              scriptName}' query were not specified: ${res.unknown_params}`);
    }
    let query: Query = {
      query: res.queryText
    };
    // NOTE: we can support DML scripts as well, but there is no clear reason for this
    // but if we do then it can be like this:
    //if (dataset && !meta.ddl) {
    // query.destination = dataset.table(meta.table || scriptName);
    // query.createDisposition = 'CREATE_IF_NEEDED';
    // query.writeDisposition = params?.writeDisposition || 'WRITE_TRUNCATE';
    //}
    try {
      let [values] = await this.bigquery.query(query);
      logger.info(`Query '${scriptName}' executed successfully`);
      return values;
    } catch (e) {
      logger.error(`Query '${scriptName}' failed to execute: ${e}`);
      throw e;
    }
  }

  private async getDataset(datasetId: string): Promise<Dataset> {
    let dataset: Dataset;
    const options: bigquery.IDataset = {
      location: this.datasetLocation,
    };
    try {
      dataset = this.bigquery.dataset(datasetId, options);
      await dataset.get({autoCreate: true});
    } catch (e) {
      logger.error(`Failed to get or create the dataset '${datasetId}'`);
      throw e;
    }
    return dataset;
  }
}
