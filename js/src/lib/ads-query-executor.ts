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

import {IGoogleAdsApiClient} from './ads-api-client';
import {AdsQueryEditor} from './ads-query-editor';
import {AdsRowParser} from './ads-row-parser';
import {NullWriter} from './csv-writer';
import {IResultWriter, QueryElements, QueryResult} from './types';

export interface AdsQueryExecutorOptions {
  /** Do not execute script for constant resources */
  skipConstants?: boolean | undefined;
  /** synchronous execution -
   * each script will be executed for all customers synchronously,
   * otherwise in parallel */
  sync?: boolean;
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

  parseQuery(queryText: string, macros?: Record<string, any>) {
    return this.editor.parseQuery(queryText, macros);
  }

  async execute(
      scriptName: string, queryText: string, customers: string[],
      macros: Record<string, any>, writer: IResultWriter|undefined,
      options?: AdsQueryExecutorOptions) {
    let skipConstants = !!options?.skipConstants;
    let sync = !!options?.sync;
    if (sync) console.log(`Running in synchronous mode`);
    let query = this.parseQuery(queryText, macros);
    let isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      console.log(`Skipping constant resource ${query.resource.name}`);
      return;
    }

    if (writer) await writer.beginScript(scriptName, query);
    let tasks: Array<Promise<void>> = [];
    for (let customerId of customers) {
      try {
        if (sync) {
          await this.executeOne(query, customerId, writer);
        }
        else {
          let task = this.executeOne(query, customerId, writer);
          tasks.push(task);
        }
      } catch (e) {
        console.log(`An error occured during executing script '${
            scriptName}' for ${customerId} customer: ${e.message || e}`);
        // we're swallowing the exception
      }
      // if resource has '_constant' in its name, break the loop over customers
      // (it doesn't depend on them)
      if (isConstResource) {
        console.log(
            'Detected constant resource script (breaking loop over customers)')
        break;
      }
    }

    if (!sync) {
      let results = await Promise.allSettled(tasks);
      for (let result of results) {
        if (result.status == 'rejected') {
          let customerId = result.reason.customerId;
          console.log(`An error occured during executing script '${
              scriptName}' for ${customerId} customer: ${result.reason.message || result.reason}`);
        }
      }
    }

    if (writer) await writer.endScript();
  }

  /**
   * Analogue to `execute` method but with an ability to get result for each customer
   * (`execute` can only be used with a writer)
   * @example
   *
   * @param scriptName name of the script
   * @param queryText parsed Ads query
   * @param customers a list of customers to process
   * @param macros macros (arbitrary key-value pairs to substitute into query)
   * @param options execution options
   * @returns an async generator to iterate through to get results for each customer
   */
  async *
      executeGen(
          scriptName: string, queryText: string, customers: string[],
          macros?: Record<string, any>,
          options?: AdsQueryExecutorOptions):
          AsyncGenerator<QueryResult, void, QueryResult|void> {
    let skipConstants = !!options?.skipConstants;
    let query = this.parseQuery(queryText, macros);
    let isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      console.log(`Skipping constant resource ${query.resource.name}`);
      return;
    }
    for (let customerId of customers) {
      console.log(`Processing customer ${customerId}`);
      let result = await this.executeOne(query, customerId);
      yield result;
      // if resource has '_constant' in its name, break the loop over customers
      // (it doesn't depend on them)
      if (skipConstants) {
        console.log(
            'Detected constant resource script (breaking loop over customers)')
        break;
      }
    }
  }

  async executeOne(query: QueryElements, customerId: string):
      Promise<QueryResult>;
  async executeOne(
      query: QueryElements, customerId: string,
      writer?: IResultWriter): Promise<void>;
  /**
   * Executes a query for a customer.
   * Please note that if you use the method directly you should call methods
   * `beginScript` and `endScript` on your writer instance.
   * @param query parsed Ads query (GAQL)
   * @param customerId customer id
   * @param writer output writer, can be ommited (if you need QueryResult)
   * @returns void if you supplied a writer, otherwise (no writer) a QueryResult
   */
  async executeOne(
      query: QueryElements, customerId: string,
      writer?: IResultWriter|undefined): Promise<QueryResult|void> {
    if (!customerId) throw new Error(`customerId should be specified`);
      let empty_result = !!writer;
    if (!writer) {
      writer = new NullWriter();
    }
    console.log(`Processing customer ${customerId}`);
    try {
      await writer.beginCustomer(customerId);
      let parsedRows: any[] = [];
      let rows = await this.client.executeQuery(query.queryText, customerId);
      for (let row of rows) {
        // TODO: use logging instead
        // console.log('raw row:');
        // console.log(row);
        let parsedRow = this.parser.parseRow(row, query);
        // console.log('parsed row:');
        // console.log(parsedRow);
        if (!empty_result) {
          parsedRows.push(parsedRow);
        }
        writer.addRow(customerId, parsedRow, row);
      }
      console.log(`\t[${customerId}] got ${rows.length} rows`);
      await writer.endCustomer(customerId);

      if (empty_result) return;
      return {rawRows: rows, rows: parsedRows, query};
    }
    catch (e) {
      e.customerId = customerId;
      throw e;
    }
  }
}
