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
  tableId: string|undefined;
  dataset: Dataset|undefined;
  table: Table|undefined;
  rows: any[][] = [];
  query: QueryElements|undefined;
  tableTemplate?: string;
  dumpSchema?: boolean;

  constructor(
      projectId: string, dataset: string, options?: BigQueryWriterOptions) {
    this.bigquery = new BigQuery({
      projectId: projectId,
      scopes: OAUTH_SCOPES,
      // keyFilename: argv.keyFile
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
    let schema = this.createSchema(query);
    this.table = await this.createTable(schema);
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

  private async createTable(schema: bigquery.ITableSchema): Promise<Table> {
    try {
      const table = this.dataset!.table(this.tableId!);
      await table.delete({ignoreNotFound: true});
    } catch (e) {
      console.log(`Failed to delete the table ${this.tableId}`);
      throw e;
    }
    let [table] = await this.dataset!.createTable(this.tableId!, {schema});
    return table
  }

  endScript(): Promise<void>|void {
    this.tableId = undefined;
    this.table = undefined;
    this.query = undefined;
  }

  beginCustomer(customerId: string): Promise<void>|void {
    this.rows = [];
  }

  async endCustomer(): Promise<void> {
    if (this.rows.length > 0) {
      // upload data to BQ
      try {
        // insert rows by chunks (there's a limit for insert)
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
          await this.table?.insert(rows, {});
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
          // ApiError: Table 162551664177:adsapi_yt_js.campaign not found.
        }
        throw e;
      }
      console.log(`${this.rows.length} rows inserted into '${this.datasetId}.${
          this.tableId}' table`);
    }
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
    // TODO: all other means STRUCT, but do we really need structs in BQ?
    return 'STRING';
  }

  addRow(parsedRow: any[]): void {
    if (!parsedRow || parsedRow.length == 0) return;
    this.rows.push(parsedRow);
  }
}
