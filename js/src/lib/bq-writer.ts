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
}

export class BigQueryWriter implements IResultWriter {
  bigquery: BigQuery;
  datasetId: string;
  datasetLocation?: string;
  customerId: string|undefined;
  schema: bigquery.ITableSchema|undefined;
  tableId: string|undefined;
  dataset: Dataset|undefined;
  // table: Table|undefined;
  rows: any[][] = [];
  query: QueryElements|undefined;
  tableTemplate?: string;
  dumpSchema?: boolean;

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
    this.dumpSchema = options?.dumpSchema;
  }

  async beginScript(scriptName: string, query: QueryElements): Promise<void> {
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

  async endScript(customers: string[]): Promise<void> {
    if (!this.query?.resource.isConstant) {
      /*
      Create a view to union all customer tables:
      CREATE OR REPLACE VIEW `dataset.resource` AS
        SELECT * FROM `dataset.resource_*`;
      Unfortunately BQ always creates a based empty table for templated
      (customer) table, so we have to drop it first.
      */
      await this.dataset!.table(this.tableId!).delete({ignoreNotFound: true});
      await this.dataset!.query({
        query: `CREATE OR REPLACE VIEW \`${this.datasetId}.${
            this.tableId}\` AS SELECT * FROM \`${this.datasetId}.${
            this.tableId}_*\` WHERE _TABLE_SUFFIX in (${customers.map(s => "'"+s+"'").join(',')})`
      });
      console.log(`Created a union view '${this.datasetId}.${this.tableId}'`);
    }
    this.tableId = undefined;
    // this.table = undefined;
    this.query = undefined;
  }

  beginCustomer(customerId: string): Promise<void>|void {
    this.customerId = customerId;
    this.rows = [];
  }

  async endCustomer(): Promise<void> {
    // let started = new Date();

    //  remove customer's table (to make sure you have only fresh data)
    // NOTE: for constant resources we don't use templated table (table per customer)
    let tableFullName = this.query?.resource.isConstant ?
        this.tableId! :
        this.tableId! + '_' + this.customerId;

    try {
      console.log(`\tRemoving table '${tableFullName}'`);
      await this.dataset!.table(tableFullName).delete({ignoreNotFound: true});
    } catch (e) {
      console.log(`Deletion of table '${tableFullName}' failed: ${e}`);
      throw e;
    }

    if (this.rows.length > 0) {
      // upload data to BQ
      try {
        // insert rows by chunks (there's a limit for insert)
        let table = this.dataset!.table(this.tableId!);
        for (let i = 0, j = this.rows.length; i < j; i += MAX_ROWS) {
          let rowsChunk = this.rows.slice(i, i + MAX_ROWS);
          let rows = rowsChunk.map(row => {
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
            templateSuffix = '_' + this.customerId;
          }
          await table!.insert(rows, {
            templateSuffix: templateSuffix,
            schema: this.schema,
          });
          console.log(`\tInserted ${rowsChunk.length} rows`);
        }
      } catch (e) {
        console.log(`Failed to insert rows into '${this.datasetId}.${
            this.tableId}' table`);
        if (e.name === 'PartialFailureError') {
          // Some rows failed to insert, while others may have succeeded.
          const max_errors_to_show = 10;
          let msgDetail = e.errors.length > max_errors_to_show ?
              `showing first ${max_errors_to_show} errors of ${
                  e.errors.length})` :
              e.errors.length + ' error(s)';
          console.log(`Some rows failed to insert (${msgDetail}):`);
          for (let i = 0; i < Math.min(e.errors.length, 10); i++) {
            let err = e.errors[i];
            console.log(`#${i} row: `);
            console.log(err.row);
            console.log(`error: ${err.errors[0].message}`);
          }
        } else if (e.code === 404) {
          // ApiError: "Table 162551664177:dataset.table not found"
          console.log(`ERROR: Table ${this.tableId} not found. WHY?`);
          const table = this.dataset!.table(this.tableId!);
          let exists = await table.exists();
          console.log(`Table exists: ${exists}`);
        }
        throw e;
      }
      console.log(
          `${this.rows.length} rows inserted into '${tableFullName}' table`);
    } else {
      // no rows found for the customer, as so no table was created, create an
      // empty one
      try {
        await this.dataset!.createTable(tableFullName, { schema: this.schema });
        console.log(`\Created empty table '${tableFullName}'`);
      } catch (e) {
        console.log(`\tCreation of empty table '${tableFullName}' failed: ${e}`);
        throw e;
      }
    }
    this.customerId = undefined;
    // TODO: get elapsed seconds: let delta = new Date() - started;
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

  addRow(parsedRow: any[]): void {
    if (!parsedRow || parsedRow.length == 0) return;
    this.rows.push(parsedRow);
  }
}
