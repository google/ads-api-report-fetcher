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

import {BigQuery, Dataset, Table, TableOptions} from '@google-cloud/bigquery';
import bigquery from '@google-cloud/bigquery/build/src/types';
import fs from 'fs';
import _ from 'lodash';

import {FieldType, FieldTypeKind, IResultWriter, isEnumType, QueryElements, QueryResult} from './types';

const MAX_ROWS = 50000;

export var OAUTH_SCOPES = [
  'https://www.googleapis.com/auth/cloud-platform',
  'https://www.googleapis.com/auth/cloud-platform.read-only',
  'https://www.googleapis.com/auth/bigquery',
  'https://www.googleapis.com/auth/bigquery.readonly',
];

export interface BigQueryWriterOptions {
  tableTemplate?: string|undefined;
  datasetLocation?: string|undefined;
  dumpSchema?: boolean;
  keepData?: boolean;
  noUnionView?: boolean;
}

export class BigQueryWriter implements IResultWriter {
  bigquery: BigQuery;
  datasetId: string;
  datasetLocation?: string;
  customers: string[];
  schema: bigquery.ITableSchema|undefined;
  tableId: string|undefined;
  dataset: Dataset|undefined;
  rowsByCustomer: Record<string, any[][]>;
  query: QueryElements|undefined;
  tableTemplate: string|undefined;
  dumpSchema: boolean;
  keepData: boolean;
  noUnionView: boolean;

  constructor(
      projectId: string, dataset: string, options?: BigQueryWriterOptions) {
    this.bigquery = new BigQuery({
      projectId: projectId,
      scopes: OAUTH_SCOPES,
      // TODO: keyFilename: argv.keyFile
    });
    this.datasetId = dataset;
    this.datasetLocation = options?.datasetLocation;
    this.tableTemplate = options?.tableTemplate;
    this.dumpSchema = options?.dumpSchema || false;
    this.keepData = options?.keepData || false;
    this.noUnionView = options?.noUnionView || false;
    this.customers = [];
    this.rowsByCustomer = {};
  }

  async beginScript(scriptName: string, query: QueryElements): Promise<void> {
    if (!scriptName)
      throw new Error(`scriptName (used as name for table) was not specified`);
    // a script's results go to a separate table with same name as a file
    if (this.tableTemplate) {
      this.tableId = this.tableTemplate.replace(/\{script\}/i, scriptName);
      // TODO: support "{account}" (though it'll have to be moved to
      // beginCustomer)
    } else {
      this.tableId = scriptName;
    }
    this.dataset = await this.getDataset();
    this.query = query;
    let schema: bigquery.ITableSchema = this.createSchema(query);
    this.schema = schema;
    if (this.dumpSchema) {
      console.log(schema);
      let schemaJson = JSON.stringify(schema, undefined, 2)
      fs.writeFileSync(scriptName + '.json', schemaJson);
    }
    if (!this.query.resource.isConstant) {
      await this.dataset!.table(this.tableId).delete({ignoreNotFound: true});
    }
  }

  private async getDataset(): Promise<Dataset> {
    let dataset: Dataset;
    const options: bigquery.IDataset = {
      location: this.datasetLocation,
    };
    try {
      dataset = this.bigquery.dataset(this.datasetId, options);
      await dataset.get({autoCreate: true});
    } catch (e) {
      console.log(`Failed to get or create the dataset ${this.datasetId}`);
      throw e;
    }
    return dataset;
  }

  async endScript(): Promise<void> {
    if (!this.tableId) {
      throw new Error(`No table id is set. Did you call beginScript method?`);
    }
    if (!this.query) {
      throw new Error(`No query is set. Did you call beginScript method?`);
    }
    if (!this.query.resource.isConstant && !this.noUnionView) {
      /*
      Create a view to union all customer tables (if not disabled excplicitly):
      CREATE OR REPLACE VIEW `dataset.resource` AS
        SELECT * FROM `dataset.resource_*`;
      Unfortunately BQ always creates a based empty table for templated
      (customer) table, so we have to drop it first.
      */
      await this.dataset!.table(this.tableId).delete({ignoreNotFound: true});
      await this.dataset!.query({
        query: `CREATE OR REPLACE VIEW \`${this.datasetId}.${
            this.tableId}\` AS SELECT * FROM \`${this.datasetId}.${
            this.tableId}_*\` WHERE _TABLE_SUFFIX in (${
            this.customers.map(s => '\'' + s + '\'').join(',')})`
      });
      console.log(`Created a union view '${this.datasetId}.${this.tableId}'`);
    }
    this.tableId = undefined;
    this.query = undefined;
    if (!this.keepData) {
      this.customers = [];
      this.rowsByCustomer = {};
    }
  }

  beginCustomer(customerId: string): Promise<void> | void {
    if (this.rowsByCustomer[customerId]) {
      throw new Error(`Customer id ${customerId} already exist`);
    }
    this.customers.push(customerId);
    this.rowsByCustomer[customerId] = [];
  }

  async endCustomer(customerId: string): Promise<void> {
    if (!this.tableId) {
      throw new Error(`No table id is set. Did you call beginScript method?`);
    }
    if (!customerId) {
      throw new Error(`No customer id is specified`);
    }
    // NOTE: for constant resources we don't use templated tables (table per
    // customer)
    let tableFullName = this.query?.resource.isConstant ?
        this.tableId :
        this.tableId + '_' + customerId;

    //  remove customer's table (to make sure you have only fresh data)
    try {
      console.log(`\t[${customerId}] Removing table '${tableFullName}'`);
      await this.dataset!.table(tableFullName).delete({ignoreNotFound: true});
    } catch (e) {
      console.log(
          `[${customerId}] Deletion of table '${tableFullName}' failed: ${e}`);
      throw e;
    }
    let rows = this.rowsByCustomer[customerId];
    if (rows.length > 0) {
      // upload data to BQ
      try {
        // insert rows by chunks (there's a limit for insert)
        let table = this.dataset!.table(this.tableId);
        for (let i = 0, j = rows.length; i < j; i += MAX_ROWS) {
          let rowsChunk = rows.slice(i, i + MAX_ROWS);
          let rows2insert = rowsChunk.map(row => {
            let rowObj: Record<string, any> = {};
            for (let i = 0; i < row.length; i++) {
              let colName = this.query!.columnNames[i];
              const colType = this.query!.columnTypes[i];
              let val = row[i];
              if (colType.repeated) {
                if (!val) {
                  // repeated field can't access nulls
                  val = [];
                } else if (val.length) {
                  // there could be structs (or arrays, or their combinations)
                  // in the array, and they will be rejected
                  for (let j = 0; j < val.length; j++) {
                    let subval = val[j];
                    if (_.isArray(subval) || _.isObjectLike(subval)) {
                      val[j] = JSON.stringify(subval);
                    }
                  }
                }
              } else if (colType.kind === FieldTypeKind.struct) {
                // we don't support structs at the moment
                if (val) {
                  val = JSON.stringify(val);
                }
              }
              rowObj[colName] = val;
            }
            return rowObj;
          });
          let templateSuffix = undefined;
          if (!this.query?.resource.isConstant) {
            // we'll create table as
            templateSuffix = '_' + customerId;
          }
          await table!.insert(rows2insert, {
            templateSuffix: templateSuffix,
            schema: this.schema,
          });
          console.log(`\t[${customerId}] Inserted ${rowsChunk.length} rows`);
        }
      } catch (e) {
        console.log(`[${customerId}] Failed to insert rows into '${
            tableFullName}' table`);
        if (e.name === 'PartialFailureError') {
          // Some rows failed to insert, while others may have succeeded.
          const max_errors_to_show = 10;
          let msgDetail = e.errors.length > max_errors_to_show ?
              `showing first ${max_errors_to_show} errors of ${
                  e.errors.length})` :
              e.errors.length + ' error(s)';
          console.log(`Some rows failed to insert (${msgDetail}):`);
          // show first 10 rows with errors
          for (let i = 0; i < Math.min(e.errors.length, 10); i++) {
            let err = e.errors[i];
            console.log(`#${i} row: `);
            console.log(err.row);
            console.log(`Error: ${err.errors[0].message}`);
          }
        } else if (e.code === 404) {
          // ApiError: "Table 162551664177:dataset.table not found"
          // This is unexpected but theriotically can happen (and did) due to eventually consistency of BigQuery
          console.log(`ERROR: Table ${tableFullName} not found.`);
          const table = this.dataset!.table(this.tableId);
          let exists = await table.exists();
          console.log(`Table exists: ${exists}`);
        }
        throw e;
      }
      console.log(`\t[${customerId}] ${rows.length} rows inserted into '${
          tableFullName}' table`);
    } else {
      // no rows found for the customer, as so no table was created,
      // create an empty one, so we could use it for a union view
      try {
        await this.dataset!.createTable(tableFullName, {schema: this.schema});
        console.log(`\t[${customerId}] Created empty table '${tableFullName}'`);
      } catch (e) {
        console.log(
            `\tCreation of empty table '${tableFullName}' failed: ${e}`);
        throw e;
      }
    }
    if (!this.keepData) this.rowsByCustomer[customerId] = [];
  }

  createSchema(query: QueryElements): bigquery.ITableSchema {
    let schema: bigquery.ITableSchema = {fields: []};
    for (let i = 0; i < query.fields.length; i++) {
      let colName = query.columnNames[i];
      let colType = query.columnTypes[i];
      let field: bigquery.ITableFieldSchema = {
        mode: colType.repeated ? 'REPEATED' : 'NULLABLE',
        name: colName.replace(/\./g, '_'),
        type: this.getBigQueryFieldType(colType)
      };
      // STRING, BYTES, INTEGER, INT64 (same as INTEGER), FLOAT, FLOAT64 (same
      // as FLOAT), NUMERIC, BIGNUMERIC, BOOLEAN, BOOL (same as BOOLEAN),
      // TIMESTAMP, DATE, TIME, DATETIME, INTERVAL, RECORD (where RECORD
      // indicates that the field contains a nested schema) or STRUCT (same as
      // RECORD).
      schema.fields!.push(field);
    }
    return schema;
  }

  private getBigQueryFieldType(colType: FieldType): string|undefined {
    if (_.isString(colType.type)) {
      switch (colType.type.toLowerCase()) {
        case 'int32':
          return 'INT64';
        case 'double':
          return 'FLOAT';
      }
      return colType.type;
    }
    if (isEnumType(colType.type)) {
      return 'STRING';
    }
    // TODO: any other means STRUCT, but do we really need structs in BQ?
    return 'STRING';
  }

  addRow(customerId: string, parsedRow: any[]): void {
    if (!parsedRow || parsedRow.length == 0) return;
    this.rowsByCustomer[customerId].push(parsedRow);
  }
}
