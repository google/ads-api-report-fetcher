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
import {AdsQueryEditor,AdsApiVersion} from "./ads-query-editor";
import {AdsRowParser} from './ads-row-parser';
import logger from './logger';
import {IResultWriter, QueryElements, QueryResult} from './types';
import { dumpMemory, getElapsed } from './utils';
export {AdsApiVersion};

export interface AdsQueryExecutorOptions {
  /** Do not execute script for constant resources */
  skipConstants?: boolean | undefined;
  /**
   * execution mode: parallel (default) or synchronous -
   * each script will be executed for all customers synchronously,
   * otherwise (by default) - in parallel
   */
  parallelAccounts?: boolean;
  dumpQuery?: boolean;
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

  /**
   * Executes a query for a list of customers.
   * Please note that if you use the method directly you should call methods
   * `beginScript` and `endScript` on your writer instance.
   * @param scriptName name of a script (can be use as target table name)
   * @param queryText Ads query text (GAQL)
   * @param customers customer ids
   * @param macros macro values to substritute into the query
   * @param writer output writer, can be ommited
   * @param options additional execution options
   * @returns a map from customer-id to row counts
   */
  async execute(
    scriptName: string,
    queryText: string,
    customers: string[],
    macros: Record<string, any>,
    writer?: IResultWriter | undefined,
    options?: AdsQueryExecutorOptions
  ): Promise<Record<string, number>> {
    let skipConstants = !!options?.skipConstants;
    let sync = options?.parallelAccounts === false || customers.length === 1;
    if (sync)
      logger.verbose(`Running in synchronous mode`, { scriptName });
    let query = this.parseQuery(queryText, macros);
    let isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      logger.verbose(`Skipping constant resource '${query.resource.name}'`, {
        scriptName,
      });
      return {};
    }
    if (options?.dumpQuery) {
      logger.info(`Script text to execute:\n` + query.queryText);
    }
    if (writer) await writer.beginScript(scriptName, query);
    let tasks: Array<Promise<QueryResult>> = [];
    let result_map: Record<string, number> = {}; // customer-id to row count mapping for return

    if (isConstResource) {
      // if resource has '_constant' in its name it doesn't depend on customers,
      // so it's enough to execute it only once
      let cid1 = customers[0];
      let res = await this.executeOne(query, cid1, writer, scriptName);
      result_map[cid1] = res.rowCount;
      logger.debug(
        "Detected constant resource script (breaking loop over customers)",
        { scriptName, customerId: cid1 }
      );
      sync = true;
    } else {
      // non-constant
      if (!sync) {
        // parallel mode - we're limiting the level of concurrency with limit
        tasks = customers.map((customerId) => {
          return this.executeOne(query, customerId, writer, scriptName);
          // TODO: return limit(() => this.executeOne(query, customerId, writer, scriptName));
        });
      } else {
        for (let customerId of customers) {
          let res = await this.executeOne(
            query,
            customerId,
            writer,
            scriptName
          );
          result_map[customerId] = res.rowCount;
        }
      }
    }

    if (!sync) {
      logger.debug(`Waiting for all tasks (${tasks.length}) to complete`);
      let results = await Promise.allSettled(tasks);
      for (let result of results) {
        if (result.status == "rejected") {
          // NOTE: we logged the error in executeOne
          throw result.reason;
        } else {
          let customerId = result.value.customerId;
          result_map[customerId] = result.value.rowCount;
        }
      }
    }

    if (writer) await writer.endScript();
    logger.debug(`[${scriptName}] Memory (script completed):\n` + dumpMemory());

    return result_map;
  }

  /**
   * Analogue to `execute` method but with an ability to get result for each
   * customer
   * (`execute` can only be used with a writer)
   * @example
   *
   * @param scriptName name of the script
   * @param queryText parsed Ads query
   * @param customers a list of customers to process
   * @param macros macros (arbitrary key-value pairs to substitute into query)
   * @param options execution options
   * @returns an async generator to iterate through to get results for each
   *     customer
   */
  async *executeGen(
    scriptName: string,
    queryText: string,
    customers: string[],
    macros?: Record<string, any>,
    options?: AdsQueryExecutorOptions
  ): AsyncGenerator<QueryResult, void, QueryResult | void> {
    let skipConstants = !!options?.skipConstants;
    let query = this.parseQuery(queryText, macros);
    let isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      logger.verbose(`Skipping constant resource '${query.resource.name}'`, {
        scriptName: scriptName,
      });
      return;
    }
    for (let customerId of customers) {
      logger.info(`Processing customer ${customerId}`, {
        scriptName: scriptName,
      });
      let result = await this.executeOne(query, customerId, undefined, scriptName);
      yield result;
      // if resource has '_constant' in its name, break the loop over customers
      // (it doesn't depend on them)
      if (skipConstants) {
        logger.debug(
          "Detected constant resource script (breaking loop over customers)",
          { scriptName, customerId }
        );
        break;
      }
    }
  }

  /**
   * Executes a query for a customer.
   * Please note that if you use the method directly you should call methods
   * `beginScript` and `endScript` on your writer instance.
   * @param query parsed Ads query (GAQL)
   * @param customerId customer id
   * @param writer output writer, can be ommited (if you need QueryResult with data)
   * @returns QueryResult, but `rows` and `rawRows` fields will be empty if you supplied a writer
   */
  async executeOne(
    query: QueryElements,
    customerId: string,
    writer?: IResultWriter | undefined,
    scriptName?: string | undefined
  ): Promise<QueryResult> {
    if (!customerId) throw new Error(`customerId should be specified`);
    logger.verbose(`Starting processing customer ${customerId}`, {
      scriptName,
      customerId,
    });
    if (logger.isLevelEnabled("debug")) {
      logger.debug(
        `[${customerId}] Memory (before customer):\n` + dumpMemory()
      );
    }
    let started = new Date();
    try {
      if (writer) await writer.beginCustomer(customerId);
      logger.debug(`Executing query: ${query.queryText}`, {
        scriptName,
        customerId,
      });
      let result = await this.executeQueryAndParse(query, customerId, writer);
      logger.info(
        `Query executed and parsed. ${
          result.rowCount
        } rows. Elapsed: ${getElapsed(started)}`,
        {
          scriptName,
          customerId,
        }
      );
      if (writer) await writer.endCustomer(customerId);
      if (logger.isDebugEnabled()) {
        logger.debug(
          `[${customerId}] Memory (customer completed):\n` + dumpMemory()
        );
      }
      logger.info(
        `Customer processing completed. Elapsed: ${getElapsed(started)}`,
        {
          scriptName,
          customerId,
        }
      );
      return result;
    } catch (e) {
      logger.error(
        `An error occured during executing script '${scriptName}':`,
        {
          scriptName,
          customerId,
        }
      );
      logger.error(e);
      e.customerId = customerId;
      // NOTE: there could be legit reasons for the query to fail (e.g. customer is disabled),
      // but swalling the exception here will possible cause other issue in writer,
      // particularly in BigQueryWriter.endScript we'll trying to create a view
      // for customer - based tables,
      // and if query failed for all customers the view creation will also fail.
      throw e;
    }
  }

  protected async executeQueryAndParse(
    query: QueryElements,
    customerId: string,
    writer?: IResultWriter | undefined
  ) {
    let stream = this.client.executeQueryStream(query.queryText, customerId);
    let rowCount = 0;
    let rawRows: any[] = [];
    let parsedRows: any[] = [];
    for await (const row of stream) {
      let parsedRow = this.parser.parseRow(row, query);
      rowCount++;
      // NOTE: to descrease memory consumption we won't accumulate data if a writer was supplied
      if (writer) {
        await writer.addRow(customerId, parsedRow, row);
      } else {
        rawRows.push(row);
        parsedRows.push(parsedRow);
      }
    }
    return { rawRows, rows: parsedRows, query, customerId, rowCount };
  }

  async getCustomerIds(
    ids: string[],
    customer_ids_query: string
  ): Promise<string[]> {
    let query = this.parseQuery(customer_ids_query);
    let accounts: Set<string> = new Set();
    let idx = 0;
    for (let id of ids) {
      let result = await this.executeQueryAndParse(query, id);
      logger.verbose(
        `#${idx}: Fetched ${result.rowCount} rows for ${id} account`
      );
      if (result.rowCount > 0) {
        for (let row of result.rows!) {
          accounts.add(row[0]);
        }
      }
      idx++;
      // TODO: purge Customer objects in IGoogleAdsApiClient
    }
    return Array.from(accounts);
  }
}
