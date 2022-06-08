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

import _ from 'lodash';

import {AdsQueryEditor} from './ads-query-editor';
import {AdsRowParser} from './ads-row-parser';
import {IGoogleAdsApiClient} from './api-client';
import {IResultWriter, QueryElements, QueryResult} from './types';

export interface AdsQueryExecutorOptions {
  skipConstants?: boolean|undefined;
}
export class AdsQueryExecutor {
  client: IGoogleAdsApiClient;
  editor: AdsQueryEditor;
  parser: AdsRowParser;

  constructor(client: IGoogleAdsApiClient) {
    this.client = client;
    this.editor = new AdsQueryEditor();
    this.parser = new AdsRowParser();
  }

  async execute(
      scriptName: string, queryText: string, customers: string[],
      macros: Record<string, any>, writer: IResultWriter,
      options?: AdsQueryExecutorOptions) {
    let skipConstants = !!options?.skipConstants;
    let query = this.editor.parseQuery(queryText, macros);
    let isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      console.log(`Skipping constant resource ${query.resource.name}`);
      return;
    }
    await writer.beginScript(scriptName, query);
    for (let customerId of customers) {
      console.log(`Processing customer ${customerId}`);
      // TODO: should we parallelirize?
      let result = await this.executeOne(query, customerId, writer);
      // if resource has '_constant' in its name, break the loop over customers
      // (it doesn't depend on them)
      if (isConstResource) {
        console.log(
            'Detected constant resource script (breaking loop over customers)')
        break;
      }
    }
    await writer.endScript(customers);
  }

  async *
      executeGen(
          scriptName: string, queryText: string, customers: string[],
          params: Record<string, any>, writer: IResultWriter,
          options?: AdsQueryExecutorOptions):
          AsyncGenerator<QueryResult, void, QueryResult> {
    let skipConstants = !!options?.skipConstants;
    let query = this.editor.parseQuery(queryText, params);
    let isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      console.log(`Skipping constant resource ${query.resource.name}`);
      return;
    }
    await writer.beginScript(scriptName, query);
    for (let customerId of customers) {
      console.log(`Processing customer ${customerId}`);
      let result = await this.executeOne(query, customerId, writer);
      yield result;
      // if resource has '_constant' in its name, break the loop over customers
      // (it doesn't depend on them)
      if (skipConstants) {
        console.log(
            'Detected constant resource script (breaking loop over customers)')
        break;
      }
    }
    await writer.endScript(customers);
  }

  async executeOne(
      query: QueryElements, customerId: string,
      writer: IResultWriter): Promise<QueryResult> {
    await writer.beginCustomer(customerId);
    let parsedRows: any[] = [];
    let rows = await this.client.executeQuery(query.queryText, customerId);
    for (let row of rows) {
      // TODO: use ConsoleWriter instead
      // console.log('raw row:');
      // console.log(row);
      let parsedRow = this.parser.parseRow(row, query);
      // console.log('parsed row:');
      // console.log(parsedRow);
      parsedRows.push(parsedRow);
      writer.addRow(parsedRow);
    }
    console.log(`\tgot ${rows.length} rows`);
    await writer.endCustomer();

    return {rawRows: rows, rows: parsedRows, query};
  }
}
