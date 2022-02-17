import _ from 'lodash';

import {AdsQueryEditor} from './ads-query-editor';
import {AdsRowParser} from './ads-row-parser';
import {IGoogleAdsApiClient} from './api-client';
import {IResultWriter, QueryElements, QueryResult} from './types';

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
      params: Record<string, any>, writer: IResultWriter) {
    let query = this.editor.parseQuery(queryText, params);
    await writer.beginScript(scriptName, query);
    for (let customerId of customers) {
      console.log(`Processing customer ${customerId}`);
      // TODO: should we parallelirize?
      let result = await this.executeOne(query, customerId, writer);
    }
    await writer.endScript();
  }

  async *
      executeGen(
          scriptName: string, queryText: string, customers: string[],
          params: Record<string, any>,
          writer: IResultWriter): AsyncGenerator<any[], void, any[]> {
    let query = this.editor.parseQuery(queryText, params);
    await writer.beginScript(scriptName, query);
    for (let customerId of customers) {
      console.log(`Processing customer ${customerId}`);
      let result = await this.executeOne(query, customerId, writer);
      yield result.rows;
    }
    await writer.endScript();
  }

  async executeOne(
      query: QueryElements, customerId: string,
      writer: IResultWriter): Promise<QueryResult> {
    await writer.beginCustomer(customerId);
    let parsedRows: any[] = [];
    let rows = await this.client.executeQuery(query.queryText, customerId);
    for (let row of rows) {
      console.log('raw row:');
      console.log(row);
      let parsedRow = this.parser.parseRow(row, query);
      // console.log('parsed row:');
      // console.log(parsedRow);
      parsedRows.push(parsedRow);
      writer.addRow(parsedRow);
    }
    await writer.endCustomer();

    return {rawRows: rows, rows: parsedRows, query};
  }
}
