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

export var OAUTH_SCOPES = [
  'https://www.googleapis.com/auth/cloud-platform',
  'https://www.googleapis.com/auth/cloud-platform.read-only',
  'https://www.googleapis.com/auth/bigquery',
  'https://www.googleapis.com/auth/bigquery.readonly',
];

export interface BigQueryExecutorOptions {
  datasetLocation?: string;
}
export interface BigQueryExecutorParams {
  sqlParams?: Record<string, any>;
  macroParams?: Record<string, any>;
  target?: string;
  writeDisposition?: string;
}
export class BigQueryExecutor {
  bigquery: BigQuery;
  datasetLocation?: string;

  constructor(projectId: string, options?: BigQueryExecutorOptions) {
    this.bigquery = new BigQuery({
      projectId: projectId,
      scopes: OAUTH_SCOPES,
      // TODO: keyFilename: argv.keyFile
    });
    this.datasetLocation = options?.datasetLocation;
  }

  substituteMacros(queryText: string, macros: Record<string, any>): string {
    for (let pair of Object.entries(macros)) {
      queryText = queryText.replaceAll(`{${pair[0]}}`, pair[1]);
    }
    return queryText;
  }

  async execute(
      scriptName: string, queryText: string,
      params?: BigQueryExecutorParams): Promise<any[]> {
    let dataset: Dataset|undefined;
    if (params?.target) {
      if (params!.target.includes('.')) {
        let idx = params!.target.indexOf('.');
        if (idx > 0) throw new Error('Not yet supported');
      }
      dataset = await this.getDataset(params!.target);
    }
    let query: Query = {
      query: params?.macroParams ?
          this.substituteMacros(queryText, params?.macroParams) :
          queryText,
    };
    if (dataset) {
      query.destination = dataset.table(scriptName);
      query.createDisposition = 'CREATE_IF_NEEDED';
      // TODO: support WRITE_APPEND (if target='dataset.table' or specify
      // disposition explicitly)
      query.writeDisposition = params?.writeDisposition || 'WRITE_TRUNCATE';
      //query.location = 'US';
    }
    try {
      let [values] = await this.bigquery.query(query);
      console.log(`Query '${scriptName}' executed successfully (${values.length} rows)`);
      if (dataset && values.length) {
        // write down query's results into a table in BQ
        let table = query.destination;
        const MAX_ROWS = 50_000;
        for (let i = 0, j = values.length; i < j; i += MAX_ROWS) {
          let rowsChunk = values.slice(i, i + MAX_ROWS);
          await table!.insert(rowsChunk, {});
          console.log(`\tInserted ${rowsChunk.length} rows`);
        }
      }
      return values;
    } catch (e) {
      console.log(`Query '${scriptName}' failed to execute: ${e}`);
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
      console.log(`Failed to get or create the dataset '${datasetId}'`);
      throw e;
    }
    return dataset;
  }
}
