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

import {IGoogleAdsApiClient} from './ads-api-client.js';
import {AdsApiVersion, IAdsQueryEditor} from './ads-query-editor.js';
import {IAdsRowParser} from './ads-row-parser.js';
import {getLogger} from './logger.js';
import {
  IResultWriter,
  QueryElements,
  QueryObjectResult,
  QueryResult,
} from './types.js';
import {executeWithRetry, getElapsed, getMemoryUsage} from './utils.js';
import {mapLimit} from 'async';
export {AdsApiVersion};

export interface AdsQueryExecutorOptions {
  /** Do not execute script for constant resources */
  skipConstants?: boolean | undefined;
  /**
   * Execution mode: parallel (default) or synchronous -
   * whether a script will be executed for all account in parallel or synchronously
   */
  parallelAccounts?: boolean;
  /**
   * A level of parallelism (if `parallelAccounts=true`) - maximum number
   * of requests executing in parallel.
   */
  parallelThreshold?: number;
  dumpQuery?: boolean;
}

/**
 * Parameter for query parsing (parseQuery method)
 */
export interface AdsQueryParams {
  /**
   * Macros map used in the query in the `{param}` format.
   */
  macros?: Record<string, string>;
  /**
   * Params for template processing
   */
  templateParams?: Record<string, string>;
}

/**
 * The main component for Adq query execution.
 */
export class AdsQueryExecutor {
  static DEFAULT_PARALLEL_THRESHOLD = 16;
  static DEFAULT_RETRY_COUNT = 5;

  client: IGoogleAdsApiClient;
  editor: IAdsQueryEditor;
  parser: IAdsRowParser;
  logger;
  maxRetryCount;

  constructor(client: IGoogleAdsApiClient) {
    this.client = client;
    this.editor = client.getQueryEditor();
    this.parser = client.getRowParser();
    this.logger = getLogger();
    this.maxRetryCount = AdsQueryExecutor.DEFAULT_RETRY_COUNT;
  }

  parseQuery(queryText: string, scriptName?: string, params?: AdsQueryParams) {
    try {
      return this.editor.parseQuery(
        queryText,
        params?.macros,
        params?.templateParams
      );
    } catch (e) {
      e.message = (scriptName ? scriptName + ': ' : '') + e.message;
      throw e;
    }
  }

  /**
   * Executes a query for a list of customers.
   * Please note that if you use the method directly you should call methods
   * `beginScript` and `endScript` on your writer instance.
   * @param scriptName name of a script (can be use as target table name)
   * @param queryText Ads query text (GAQL)
   * @param customers customer ids
   * @param params macro values to substitute into the query
   * @param writer output writer, can be omitted
   * @param options additional execution options
   * @returns a map from customer-id to row counts
   */
  async execute(
    scriptName: string,
    queryText: string,
    customers: string[],
    params: AdsQueryParams,
    writer?: IResultWriter | undefined,
    options?: AdsQueryExecutorOptions
  ): Promise<Record<string, number>> {
    const skipConstants = !!options?.skipConstants;
    let sync = options?.parallelAccounts === false || customers.length === 1;
    const threshold =
      options?.parallelThreshold || AdsQueryExecutor.DEFAULT_PARALLEL_THRESHOLD;
    if (customers.length > 1) {
      this.logger.verbose(
        `Executing (API ${AdsApiVersion}) '${scriptName}' query for ${
          customers.length
        } accounts in ${sync ? 'synchronous' : 'parallel'} mode`,
        {scriptName}
      );
    } else {
      this.logger.verbose(
        `Executing (API ${AdsApiVersion}) '${scriptName}' query for single account (${customers[0]})`
      );
    }

    const query = this.parseQuery(queryText, scriptName, params);
    const isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      this.logger.verbose(
        `Skipping constant resource '${query.resource.name}'`,
        {
          scriptName,
        }
      );
      return {};
    }
    if (options?.dumpQuery) {
      this.logger.info('Script text to execute:\n' + query.queryText);
    }
    if (writer) await writer.beginScript(scriptName, query);
    const result_map: Record<string, number> = {}; // customer-id to row count mapping for return

    if (isConstResource) {
      // if resource has '_constant' in its name it doesn't depend on customers,
      // so it's enough to execute it only once
      const cid1 = customers[0];
      const res = await this.executeOne(query, cid1, writer, scriptName);
      result_map[cid1] = res.rowCount;
      this.logger.debug(
        'Detected constant resource script (breaking loop over customers)',
        {scriptName, customerId: cid1}
      );
      sync = true;
    } else {
      // non-constant
      if (!sync) {
        // parallel mode - we're limiting the level of concurrency with limit
        this.logger.debug(
          `Concurrently processing customers (throttle: ${threshold}): ${customers}`
        );
        const results = await mapLimit(
          customers,
          threshold,
          async (customerId: string) => {
            return this.executeOne(query, customerId, writer, scriptName);
          }
        );
        for (const result of results) {
          result_map[result.customerId] = result.rowCount;
        }
      } else {
        for (const customerId of customers) {
          const res = await this.executeOne(
            query,
            customerId,
            writer,
            scriptName
          );
          result_map[customerId] = res.rowCount;
        }
      }
    }

    if (writer) await writer.endScript();
    if (process.env.DUMP_MEMORY) {
      this.logger.debug(getMemoryUsage('Script completed'));
    }

    return result_map;
  }

  /**
   * Analogue to `execute` method but with an ability to get result for each
   * customer
   * (`execute` can only be used with a writer)
   * @param scriptName name of the script
   * @param queryText parsed Ads query
   * @param customers a list of customers to process
   * @param params macros (arbitrary key-value pairs to substitute into query)
   * @param options execution options
   * @returns an async generator to iterate through to get results for each
   *     customer
   */
  async *executeGen(
    scriptName: string,
    queryText: string,
    customers: string[],
    params?: AdsQueryParams,
    options?: AdsQueryExecutorOptions
  ): AsyncGenerator<QueryResult, void, QueryResult | void> {
    const skipConstants = !!options?.skipConstants;
    const query = this.parseQuery(queryText, scriptName, params);
    const isConstResource = query.resource.isConstant;
    if (skipConstants && isConstResource) {
      this.logger.verbose(
        `Skipping constant resource '${query.resource.name}'`,
        {
          scriptName: scriptName,
        }
      );
      return;
    }
    for (const customerId of customers) {
      this.logger.info(`Processing customer ${customerId}`, {
        scriptName: scriptName,
      });
      const result = await this.executeOne(
        query,
        customerId,
        undefined,
        scriptName
      );
      yield result;
      // if resource has '_constant' in its name, break the loop over customers
      // (it doesn't depend on them)
      if (skipConstants) {
        this.logger.debug(
          'Detected constant resource script (breaking loop over customers)',
          {scriptName, customerId}
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
   * @param writer output writer, can be omitted (if you need QueryResult with data)
   * @returns QueryResult, but `rows` and `rawRows` fields will be empty if you supplied a writer
   */
  async executeOne(
    query: QueryElements,
    customerId: string,
    writer?: IResultWriter | undefined,
    scriptName?: string | undefined
  ): Promise<QueryResult> {
    if (!customerId) throw new Error('customerId should be specified');
    this.logger.verbose(`Starting processing customer ${customerId}`, {
      scriptName,
      customerId,
    });
    if (process.env.DUMP_MEMORY) {
      this.logger.debug(getMemoryUsage('Begin customer'));
    }
    const started = new Date();
    try {
      if (writer) await writer.beginCustomer(customerId);
      this.logger.debug(`Executing query: ${query.queryText}`, {
        scriptName,
        customerId,
      });
      const result = await this.executeQueryAndParse(query, customerId, writer);
      this.logger.info(
        `Query executed and parsed. ${
          result.rowCount
        } rows. Elapsed: ${getElapsed(started)}`,
        {
          scriptName,
          customerId,
        }
      );
      if (writer) await writer.endCustomer(customerId);
      if (process.env.DUMP_MEMORY) {
        this.logger.debug(getMemoryUsage('Customer completed'));
      }
      this.logger.info(
        `Customer processing completed. Elapsed: ${getElapsed(started)}`,
        {
          scriptName,
          customerId,
        }
      );
      return result;
    } catch (e) {
      if (!e.logged) {
        console.error(e);
        this.logger.error(
          `An error occurred during executing script '${scriptName}':${
            e.message || e
          }`,
          {
            scriptName,
            customerId,
            error: e,
          }
        );
      }
      e.customerId = customerId;
      // NOTE: there could be legit reasons for the query to fail (e.g. customer is disabled),
      // but swelling the exception here will possible cause other issue in writer,
      // particularly in BigQueryWriter.endScript we'll trying to create a view
      // for customer - based tables,
      // and if query failed for all customers the view creation will also fail.
      throw e;
    }
  }

  protected executeNativeQuery(query: QueryElements, customerId: string) {
    if (query.executor) {
      return query.executor.execute(query, customerId, this);
    } else {
      const stream = this.client.executeQueryStream(
        query.queryText,
        customerId
      );
      return stream;
    }
  }

  async executeQueryAndParseToObjects(
    query: QueryElements,
    customerId: string
  ): Promise<QueryObjectResult> {
    const rows = await this.client.executeQuery(query.queryText, customerId);
    let rowCount = 0;
    const parsedRows: Record<string, unknown>[] = [];
    // NOTE: as we're iterating over an AsyncGenerator any error if happens
    // will be thrown on iterating not on creating of the generator
    for (const row of rows) {
      const parsedRow = <Record<string, unknown>>(
        this.parser.parseRow(row, query, true)
      );
      parsedRows.push(parsedRow);
      rowCount++;
    }
    return {rows: parsedRows, query, customerId, rowCount};
  }

  /**
   * Execute a query (via QueryStream) and process each rows with AdsRowParsed.
   * If a writer provided it will be call for each row.
   * @param query A parsed query (by AdsQueryEditor)
   * @param customerId customer id
   * @param writer optional writer
   * @returns if no writer provided a results with rows and parsed rows,
   *          otherwise only rowCount returned
   */
  async executeQueryAndParse(
    query: QueryElements,
    customerId: string,
    writer?: IResultWriter | undefined
  ): Promise<QueryResult> {
    return executeWithRetry(
      async () => {
        const dumpRawRow = process.env.GAARF_DUMP_API_ROW;
        const dumpRow = process.env.GAARF_DUMP_ROW;
        const stream = this.executeNativeQuery(query, customerId);
        if (process.env.DUMP_MEMORY) {
          this.logger.debug(getMemoryUsage('Query executed'));
        }
        let rowCount = 0;
        const rawRows: Record<string, unknown>[] = [];
        const parsedRows: unknown[][] = [];
        // NOTE: as we're iterating over an AsyncGenerator any error if happens
        // will be thrown on iterating not on creating of the generator
        for await (const row of stream) {
          if (dumpRawRow) {
            console.log(row);
          }
          const parsedRow = <unknown[]>this.parser.parseRow(row, query, false);
          if (dumpRow) {
            console.log(parsedRow);
          }
          rowCount++;
          // NOTE: to decrease memory consumption we won't accumulate data if a writer was supplied
          if (writer) {
            await writer.addRow(customerId, parsedRow, row);
            if (process.env.DUMP_MEMORY && rowCount % 10 === 0) {
              this.logger.debug(getMemoryUsage('10 rows processed'));
            }
          } else {
            rawRows.push(row);
            parsedRows.push(parsedRow);
          }
        }
        return {rawRows, rows: parsedRows, query, customerId, rowCount};
      },
      (error, attempt) => {
        const retry = attempt <= this.maxRetryCount && error.retryable;
        this.logger.verbose(
          retry
            ? `Retrying on transient error, attempt ${attempt}, error: ${error}`
            : `Breaking on ${
                error.retryable ? 'retriable' : 'non-retriable'
              } error, attempt ${attempt}, error: ${error}`,
          {customerId, query}
        );
        return retry;
      },
      {
        baseDelayMs: 100,
        delayStrategy: 'linear',
      }
    );
  }
}
