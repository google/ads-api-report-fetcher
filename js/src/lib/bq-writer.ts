/**
 * Copyright 2023 Google LLC
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

import { BigQuery, Dataset, Table, TableOptions } from "@google-cloud/bigquery";
import bigquery from "@google-cloud/bigquery/build/src/types";
import fs from "node:fs";
import fs_async from 'node:fs/promises';
import path from "node:path";
import _ from "lodash";

import {getLogger} from "./logger";
import {
  FieldType,
  FieldTypeKind,
  IResultWriter,
  isEnumType,
  QueryElements,
  QueryResult,
} from "./types";
import { substituteMacros } from "./utils";
import { getDataset, OAUTH_SCOPES } from "./bq-common";


const MAX_ROWS = 50000;

export interface BigQueryWriterOptions {
  datasetLocation?: string | undefined;
  noUnionView?: boolean;
  tableTemplate?: string | undefined;
  dumpSchema?: boolean;
  dumpData?: boolean;
  keepData?: boolean;
  keyFilePath?: string;
  insertMethod?: BigQueryInsertMethod;
  arrayHandling?: BigQueryArrayHandling;
  arraySeparator?: string | undefined;
}
export enum BigQueryInsertMethod {
  insertAll,
  loadTable,
}
export enum BigQueryArrayHandling {
  strings = 'strings',
  arrays = 'arrays'
}

export class BigQueryWriter implements IResultWriter {
  bigquery: BigQuery;
  datasetId: string;
  datasetLocation?: string;
  customers: string[];
  schema: bigquery.ITableSchema | undefined;
  tableId: string | undefined;
  dataset: Dataset | undefined;
  rowsByCustomer: Record<string, any[][]>;
  rowCountsByCustomer: Record<string, number>;
  streamsByCustomer: Record<string, fs.WriteStream>;
  query: QueryElements | undefined;
  tableTemplate: string | undefined;
  dumpSchema: boolean;
  dumpData: boolean;
  keepData: boolean;
  noUnionView: boolean;
  insertMethod: BigQueryInsertMethod;
  logger;
  arrayHandling: BigQueryArrayHandling;
  arraySeparator: string;

  constructor(
    projectId: string,
    dataset: string,
    options?: BigQueryWriterOptions
  ) {
    const datasetLocation = options?.datasetLocation || "us";
    this.bigquery = new BigQuery({
      projectId: projectId,
      scopes: OAUTH_SCOPES,
      keyFilename: options?.keyFilePath,
      location: datasetLocation,
    });
    this.datasetId = dataset;
    this.datasetLocation = datasetLocation;
    this.tableTemplate = options?.tableTemplate;
    this.dumpSchema = options?.dumpSchema || false;
    this.dumpData = options?.dumpData || false;
    this.keepData = options?.keepData || false;
    this.noUnionView = options?.noUnionView || false;
    this.insertMethod = options?.insertMethod || BigQueryInsertMethod.loadTable;
    this.arrayHandling = options?.arrayHandling || BigQueryArrayHandling.arrays;
    this.arraySeparator = options?.arraySeparator || '|';
    this.customers = [];
    this.rowsByCustomer = {};
    this.rowCountsByCustomer = {};
    this.streamsByCustomer = {};
    this.logger = getLogger();
  }

  async beginScript(scriptName: string, query: QueryElements): Promise<void> {
    if (!scriptName)
      throw new Error(`scriptName (used as name for table) was not specified`);
    // a script's results go to a separate table with same name as a file
    if (this.tableTemplate) {
      this.tableId = substituteMacros(this.tableTemplate, { scriptName }).text;
    } else {
      this.tableId = scriptName;
    }
    this.dataset = await getDataset(
      this.bigquery,
      this.datasetId,
      this.datasetLocation
    );
    this.query = query;
    let schema: bigquery.ITableSchema = this.createSchema(query);
    this.schema = schema;
    if (this.dumpSchema) {
      this.logger.debug(JSON.stringify(schema, null, 2));
      let schemaJson = JSON.stringify(schema, undefined, 2);
      await fs_async.writeFile(scriptName + ".json", schemaJson);
    }
  }

  beginCustomer(customerId: string): Promise<void> | void {
    if (this.rowsByCustomer[customerId]) {
      throw new Error(`Customer id ${customerId} already exist`);
    }
    this.customers.push(customerId);
    this.rowsByCustomer[customerId] = [];
    this.rowCountsByCustomer[customerId] = 0;

    if (this.insertMethod === BigQueryInsertMethod.loadTable) {
      let tableFullName = this.getTableFullname(customerId);
      let filepath = this.getDataFilepath(tableFullName);
      if (fs.existsSync(filepath)) {
        fs.rmSync(filepath);
      }
      this.streamsByCustomer[customerId] = fs.createWriteStream(filepath);
    }
  }

  protected getTableFullname(customerId: string): string {
    if (!this.tableId)
      throw new Error(
        `tableId is not set (probably beginScript method was not called)`
      );
    let tableFullName = this.query?.resource.isConstant
      ? this.tableId
      : this.tableId + "_" + customerId;
    return tableFullName;
  }

  protected getDataFilepath(tableFullName: string) {
    let filepath = `.${tableFullName}.json`;
    if (process.env.K_SERVICE) {
      // we're in GCloud - file system is readonly, the only writable place is /tmp
      // TODO: use streaming uploads (https://cloud.google.com/storage/docs/streaming-uploads)
      filepath = path.join("/tmp", filepath);
    }
    return filepath;
  }

  async loadRows(customerId: string, tableFullName: string) {
    let filepath = this.getDataFilepath(tableFullName);
    let rowCount = this.rowCountsByCustomer[customerId];
    this.logger.verbose(
      `Loading ${rowCount} rows into '${this.datasetId}.${tableFullName}' table`,
      {
        customerId: customerId,
        scriptName: this.tableId,
      }
    );
    let table = this.dataset!.table(tableFullName);
    await table.load(filepath, {
      schema: this.schema,
      sourceFormat: "NEWLINE_DELIMITED_JSON",
      writeDisposition: "WRITE_TRUNCATE",
    });
    this.logger.info(
      `${rowCount} rows loaded into '${this.datasetId}.${tableFullName}' table`,
      {
        customerId: customerId,
        scriptName: this.tableId,
      }
    );
  }

  prepareRow(row: any[]) {
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
        if (this.arrayHandling === BigQueryArrayHandling.strings) {
          val = val.join(this.arraySeparator);
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
  }

  async insertRows(rows: any[][], customerId: string, tableFullName: string) {
    const tableId = this.tableId!;
    try {
      // insert rows by chunks (there's a limit for insert)
      let table = this.dataset!.table(tableId);
      for (let i = 0, j = rows.length; i < j; i += MAX_ROWS) {
        let rowsChunk = rows.slice(i, i + MAX_ROWS);
        let rows2insert = rowsChunk.map((row) => this.prepareRow(row));
        let templateSuffix = undefined;
        if (!this.query?.resource.isConstant) {
          templateSuffix = "_" + customerId;
        }
        this.logger.verbose(`Inserting ${rowsChunk.length} rows`, {
          customerId: customerId,
        });
        await table!.insert(rows2insert, {
          templateSuffix: templateSuffix,
          schema: this.schema,
        });
      }
    } catch (e) {
      this.logger.debug(e);
      this.logger.error(`Failed to insert rows into '${tableFullName}' table`, {
        customerId: customerId,
      });
      if (e.name === "PartialFailureError") {
        // Some rows failed to insert, while others may have succeeded.
        const max_errors_to_show = 10;
        let msgDetail =
          e.errors.length > max_errors_to_show
            ? `showing first ${max_errors_to_show} errors of ${e.errors.length})`
            : e.errors.length + " error(s)";
        this.logger.warn(`Some rows failed to insert (${msgDetail}):`, {
          customerId: customerId,
        });
        // show first 10 rows with errors
        for (let i = 0; i < Math.min(e.errors.length, 10); i++) {
          let err = e.errors[i];
          this.logger.warn(
            `#${i} row:\n${JSON.stringify(err.row, null, 2)}\nError: ${
              err.errors[0].message
            }`,
            { customerId: customerId }
          );
        }
      } else if (e.code === 404) {
        // ApiError: "Table 162551664177:dataset.table not found"
        // This is unexpected but theriotically can happen (and did) due to eventually consistency of BigQuery
        console.error(`Table ${tableFullName} not found.`, {
          customerId: customerId,
        });
        const table = this.dataset!.table(tableId);
        let exists = await table.exists();
        console.warn(`Table ${tableFullName} existence check: ${exists}`);
      }
      throw e;
    }
    this.logger.info(
      `${rows.length} rows inserted into '${tableFullName}' table`,
      {
        customerId: customerId,
      }
    );
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
    let tableFullName = this.getTableFullname(customerId);

    //  remove customer's table (to make sure you have only fresh data)
    try {
      this.logger.debug(`Removing table '${tableFullName}'`, {
        customerId: customerId,
        scriptName: this.tableId,
      });
      await this.dataset!.table(tableFullName).delete({ ignoreNotFound: true });
    } catch (e) {
      this.logger.error(`Deletion of table '${tableFullName}' failed: ${e}`, {
        customerId: customerId,
        scriptName: this.tableId,
      });
      throw e;
    }
    if (this.insertMethod === BigQueryInsertMethod.loadTable) {
      let stream = this.streamsByCustomer[customerId];
      stream.end();
    }
    let rowCount = this.rowCountsByCustomer[customerId];
    if (rowCount > 0) {
      // upload data to BQ: we support two methods: via insertAll and loadTable,
      // the later is default one as much more faster (but it requires dumping data on the disk)
      if (this.insertMethod === BigQueryInsertMethod.insertAll) {
        let rows = this.rowsByCustomer[customerId];
        await this.insertRows(rows, customerId, tableFullName);
      } else {
        await this.loadRows(customerId, tableFullName);
      }
    } else {
      // no rows found for the customer, as so no table was created,
      // create an empty one, so we could use it for a union view
      try {
        await this.dataset!.createTable(tableFullName, { schema: this.schema });
        this.logger.verbose(`Created empty table '${tableFullName}'`, {
          customerId: customerId,
          scriptName: this.tableId,
        });
      } catch (e) {
        this.logger.error(
          `\tCreation of empty table '${tableFullName}' failed: ${e}`
        );
        throw e;
      }
    }
    if (
      !this.dumpData &&
      this.insertMethod === BigQueryInsertMethod.loadTable
    ) {
      let filepath = this.getDataFilepath(tableFullName);
      if (fs.existsSync(filepath)) {
        this.logger.verbose(
          `Removing data file ${filepath}, dumpData=${this.dumpData}`
        );
        fs.rmSync(filepath);
      }
    }
    if (!this.keepData) {
      this.rowsByCustomer[customerId] = [];
    }
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
      await this.dataset!.table(this.tableId).delete({ ignoreNotFound: true });
      const table_fq = `${this.datasetId}.${this.tableId}`;
      try {
        // here there's a potential problem. If wildcard expression (resource_*)
        // catches another view the DML-query will fail with error:
        // 'Views cannot be queried through prefix. First view projectid:datasetid.viewname.'

        const query = `CREATE OR REPLACE VIEW \`${table_fq}\` AS SELECT * FROM \`${table_fq}_*\` WHERE _TABLE_SUFFIX in (${this.customers
          .map((s) => "'" + s + "'")
          .join(",")})`;
        this.logger.debug(query);
        await this.dataset!.query({
          query: query,
        });
      }
      catch (e) {
        this.logger.error(
          `An error occured during creating the unified view (${table_fq}): ${e.message}`
        );
        if (e.message.includes("Views cannot be queried through prefix")) {
          this.logger.warn(`You have to rename the script ${this.tableId} to a name so the wildcard expression ${this.tableId}_* would not catch other views`);
        }
        throw e;
      }
      this.logger.info(`Created a union view '${table_fq}'`, {
        scriptName: this.tableId,
      });
    }
    this.tableId = undefined;
    this.query = undefined;
    if (!this.keepData) {
      this.customers = [];
      this.rowsByCustomer = {};
    }
    this.streamsByCustomer = {};
  }

  createSchema(query: QueryElements): bigquery.ITableSchema {
    let schema: bigquery.ITableSchema = { fields: [] };
    for (let column of query.columns) {
      let field: bigquery.ITableFieldSchema = {
        mode:
          column.type.repeated &&
          this.arrayHandling === BigQueryArrayHandling.arrays
            ? "REPEATED"
            : "NULLABLE",
        name: column.name.replace(/\./g, "_"),
        type: this.getBigQueryFieldType(column.type),
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

  private getBigQueryFieldType(colType: FieldType): string | undefined {
    if (this.arrayHandling === BigQueryArrayHandling.strings && colType.repeated) return "STRING";
    if (_.isString(colType.type)) {
      switch (colType.type.toLowerCase()) {
        case "int32":
        case "int64":
          return "INT64";
        case "double":
        case "float":
          return "FLOAT";
      }
      return colType.type;
    }
    if (isEnumType(colType.type)) {
      return "STRING";
    }
    // TODO: any other means STRUCT, but do we really need structs in BQ?
    return "STRING";
  }

  addRow(customerId: string, parsedRow: any[]): void {
    if (!parsedRow || parsedRow.length == 0) return;
    if (this.insertMethod === BigQueryInsertMethod.loadTable) {
      // dump the row object to a file
      let row_obj: any = this.prepareRow(parsedRow);
      let fsStream = this.streamsByCustomer[customerId];
      fsStream.write(JSON.stringify(row_obj));
      fsStream.write("\n");
    } else {
      this.rowsByCustomer[customerId].push(parsedRow);
    }
    this.rowCountsByCustomer[customerId] += 1;
  }
}
