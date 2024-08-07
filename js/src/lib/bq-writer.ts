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
import fs_async from "node:fs/promises";
import fs from "node:fs";
import * as stream from "node:stream";
import _ from "lodash";

import {
  ArrayHandling,
  FieldType,
  FieldTypeKind,
  isEnumType,
  QueryElements,
} from "./types";
import { delay, substituteMacros } from "./utils";
import { getDataset, OAUTH_SCOPES } from "./bq-common";
import { BigQueryExecutor, BigQueryExecutorOptions } from "./bq-executor";
import { FileWriterBase } from "./file-writers";

const MAX_ROWS = 50000;

/**
 * Options for BigQueryWriter.
 */
export interface BigQueryWriterOptions {
  outputPath?: string | undefined;
  datasetLocation?: string | undefined;
  noUnionView?: boolean;
  tableTemplate?: string | undefined;
  dumpSchema?: boolean;
  dumpData?: boolean;
  keyFilePath?: string;
  /**
   * Insert method. Be default using `load` from json files.
   */
  insertMethod?: BigQueryInsertMethod;
  /**
   * How to process arrys (as is or convert to strings).
   */
  arrayHandling?: ArrayHandling;
  /**
   * String separator for arrays' values if arrayHandlings=strings.
   */
  arraySeparator?: string | undefined;
}

/**
 * Modes how to insert rows into a table.
 */
export enum BigQueryInsertMethod {
  /**
   * Using `insert` with rows in memory.
   */
  insertAll,
  /**
   * Using `load` with rows in json files.
   */
  loadTable,
}

/**
 * Writer to BigQuery.
 */
export class BigQueryWriter extends FileWriterBase {
  bigquery: BigQuery;
  bqExecutor: BigQueryExecutor;
  datasetId: string;
  datasetLocation?: string;
  customers: string[];
  schema: bigquery.ITableSchema | undefined;
  tableId: string | undefined;
  dataset: Dataset | undefined;
  rowsByCustomer: Record<string, any[][]>;
  tableTemplate: string | undefined;
  dumpSchema: boolean;
  dumpData: boolean;
  noUnionView: boolean;
  insertMethod: BigQueryInsertMethod;
  arrayHandling: ArrayHandling;
  arraySeparator: string;

  constructor(
    projectId: string,
    dataset: string,
    options?: BigQueryWriterOptions
  ) {
    super({ filePerCustomer: true, outputPath: options?.outputPath });
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
    this.noUnionView = options?.noUnionView || false;
    this.insertMethod = options?.insertMethod || BigQueryInsertMethod.loadTable;
    this.arrayHandling = options?.arrayHandling || ArrayHandling.arrays;
    this.arraySeparator = options?.arraySeparator || "|";
    this.customers = [];
    this.rowsByCustomer = {};

    let bqExecutorOptions: BigQueryExecutorOptions = {
      datasetLocation: datasetLocation,
      bigqueryClient: this.bigquery,
    };
    this.bqExecutor = new BigQueryExecutor(projectId, bqExecutorOptions);
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
      await fs_async.writeFile(scriptName + "_schema.json", schemaJson);
    }
  }

  async beginCustomer(customerId: string): Promise<void> {
    if (this.rowCountsByCustomer[customerId] !== undefined) {
      throw new Error(`Customer id ${customerId} already exist`);
    }
    this.customers.push(customerId);
    this.rowCountsByCustomer[customerId] = 0;

    if (this.insertMethod === BigQueryInsertMethod.loadTable) {
      let tableFullName = this.getTableFullname(customerId);
      let filepath = this.getDataFilePath(`.${tableFullName}.json`);
      const stream = this.createOutput(filepath);
      if (this.useFilePerCustomer()) {
        this.streamsByCustomer[customerId] = stream;
      } else {
        this.streamsByCustomer[""] = stream;
      }
      this.logger.verbose(`Temp output is ${stream.path}`);
      await stream.deleteFile();
    } else {
      this.rowsByCustomer[customerId] = [];
    }
  }

  private getTableFullname(customerId: string): string {
    if (!this.tableId)
      throw new Error(
        `tableId is not set (probably beginScript method was not called)`
      );
    let tableFullName = this.query?.resource.isConstant
      ? this.tableId
      : this.tableId + "_" + customerId;
    return tableFullName;
  }

  private async loadRows(customerId: string, tableFullName: string) {
    let rowCount = this.rowCountsByCustomer[customerId];
    this.logger.verbose(
      `Loading ${rowCount} rows into '${this.datasetId}.${tableFullName}' table`,
      {
        customerId: customerId,
        scriptName: this.tableId,
      }
    );
    let table = this.dataset!.table(tableFullName);
    const output = this.getOutput(customerId);
    const [job] = await table.load(
      output.isGCS ? output.getStorageFile!() : output.path,
      {
        schema: this.schema,
        sourceFormat: "NEWLINE_DELIMITED_JSON",
        writeDisposition: "WRITE_TRUNCATE",
      }
    );
    const errors = job.status?.errors;
    if (errors && errors.length > 0) {
      throw errors;
    }

    this.logger.info(
      `${rowCount} rows loaded into '${this.datasetId}.${tableFullName}' table`,
      {
        customerId: customerId,
        scriptName: this.tableId,
      }
    );
  }

  private serializeRow(row: any[]) {
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
        if (this.arrayHandling === ArrayHandling.strings) {
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

  private async insertRows(
    rows: any[][],
    customerId: string,
    tableFullName: string
  ) {
    const tableId = this.tableId!;
    try {
      // insert rows by chunks (there's a limit for insert)
      let table = this.dataset!.table(tableId);
      for (let i = 0, j = rows.length; i < j; i += MAX_ROWS) {
        let rowsChunk = rows.slice(i, i + MAX_ROWS);
        let templateSuffix = undefined;
        if (!this.query?.resource.isConstant) {
          templateSuffix = "_" + customerId;
        }
        this.logger.verbose(`Inserting ${rowsChunk.length} rows`, {
          customerId: customerId,
        });
        await table!.insert(rowsChunk, {
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
        // This is unexpected but theriotically can happen (and did)
        // due to eventually consistency of BigQuery
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
    let rowCount = this.rowCountsByCustomer[customerId];
    if (this.insertMethod === BigQueryInsertMethod.loadTable) {
      // finalize the output stream
      const output = this.getOutput(customerId);
      await this.closeStream(output);
      if (rowCount > 0) {
        this.logger.debug(
          `Data prepared in '${output.path}', closed: ${output.stream.closed}`,
          {
            customerId: customerId,
            scriptName: this.tableId,
          }
        );
        if (!fs.existsSync(output.path)) {
          console.log(
            `File ${output.path} exists: ${fs.existsSync(output.path)}`
          );
        }
      }
    }

    if (rowCount > 0) {
      // upload data to BQ: we support two methods: via insertAll and loadTable,
      // the later is default one as much more faster (but it requires dumping data on the disk)
      if (this.insertMethod === BigQueryInsertMethod.insertAll) {
        let rows = this.rowsByCustomer[customerId];
        await this.insertRows(rows, customerId, tableFullName);
        this.rowsByCustomer[customerId] = [];
      } else {
        await this.loadRows(customerId, tableFullName);
      }
    } else {
      // no rows found for the customer, as so no table was created,
      // create an empty one, so we could use it for a union view
      try {
        // it might a case when creatTables fails because the previous delete
        // hasn't been propagated
        await this.ensureTableCreated(tableFullName);
        this.logger.verbose(`Created empty table '${tableFullName}'`, {
          customerId: customerId,
          scriptName: this.tableId,
        });
      } catch (e) {
        this.logger.error(
          `Creation of empty table '${tableFullName}' failed: ${e}`
        );
        throw e;
      }
    }
    if (
      !this.dumpData &&
      this.insertMethod === BigQueryInsertMethod.loadTable
    ) {
      const output = this.getOutput(customerId);
      this.logger.verbose(`Removing data file ${output.path}`);
      await output.deleteFile();
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
      */
      const table_fq = await this.bqExecutor.createUnifiedView(
        this.datasetId,
        this.tableId,
        this.customers
      );

      this.logger.info(`Created a union view '${table_fq}'`, {
        scriptName: this.tableId,
      });
    }
    this.tableId = undefined;
    this.query = undefined;
    this.customers = [];
    if (this.rowsByCustomer) this.rowsByCustomer = {};
    if (this.streamsByCustomer) this.streamsByCustomer = {};
    this.rowCountsByCustomer = {};
  }

  private createSchema(query: QueryElements): bigquery.ITableSchema {
    let schema: bigquery.ITableSchema = { fields: [] };
    for (let column of query.columns) {
      let field: bigquery.ITableFieldSchema = {
        mode:
          column.type.repeated && this.arrayHandling === ArrayHandling.arrays
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
    if (this.arrayHandling === ArrayHandling.strings && colType.repeated)
      return "STRING";
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

  async addRow(customerId: string, parsedRow: any[]): Promise<void> {
    if (!parsedRow || parsedRow.length == 0) return;
    let rowObj: any = this.serializeRow(parsedRow);
    if (this.insertMethod === BigQueryInsertMethod.loadTable) {
      // dump the row object to a file
      const content = JSON.stringify(rowObj) + "\n";
      await this.writeContent(customerId, content);
    } else {
      this.rowsByCustomer[customerId].push(rowObj);
    }
    this.rowCountsByCustomer[customerId] += 1;
  }

  private async ensureTableCreated(tableFullName: string, maxRetries = 5) {
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        await this.dataset!.createTable(tableFullName, { schema: this.schema });
        return;
      } catch (e) {
        if (e.code === 409) {
          // 409 - ApiError: Already Exists
          // probably the table still hasn't been deleted, wait a bit
          await delay(200);
          continue;
        }
        throw e;
      }
    }
    throw new Error(
      `Failed to create a table ${tableFullName} after ${maxRetries} attempts`
    );
  }
}
