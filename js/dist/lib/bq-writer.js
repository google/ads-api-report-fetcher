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
import { BigQuery, } from '@google-cloud/bigquery';
import fs_async from 'node:fs/promises';
import { isObjectLike, isArray, isString } from 'lodash-es';
import { ArrayHandling, FieldTypeKind, isEnumType, } from './types.js';
import { delay, substituteMacros } from './utils.js';
import { getDataset, OAUTH_SCOPES } from './bq-common.js';
import { BigQueryExecutor } from './bq-executor.js';
import { FileWriterBase } from './file-writers.js';
const MAX_ROWS = 50000;
/**
 * Modes how to insert rows into a table.
 */
export var BigQueryInsertMethod;
(function (BigQueryInsertMethod) {
    /**
     * Using `insert` with rows in memory.
     */
    BigQueryInsertMethod["insertAll"] = "insert";
    /**
     * Using `load` with rows in json files.
     */
    BigQueryInsertMethod["loadTable"] = "load";
})(BigQueryInsertMethod || (BigQueryInsertMethod = {}));
/**
 * Writer to BigQuery.
 */
export class BigQueryWriter extends FileWriterBase {
    constructor(projectId, dataset, options) {
        super({ filePerCustomer: true, outputPath: options === null || options === void 0 ? void 0 : options.outputPath });
        const datasetLocation = (options === null || options === void 0 ? void 0 : options.datasetLocation) || 'us';
        this.bigquery = new BigQuery({
            projectId: projectId,
            scopes: OAUTH_SCOPES,
            keyFilename: options === null || options === void 0 ? void 0 : options.keyFilePath,
            location: datasetLocation,
        });
        this.datasetId = dataset;
        this.datasetLocation = datasetLocation;
        this.tableTemplate = options === null || options === void 0 ? void 0 : options.tableTemplate;
        this.dumpSchema = (options === null || options === void 0 ? void 0 : options.dumpSchema) || false;
        this.dumpData = (options === null || options === void 0 ? void 0 : options.dumpData) || false;
        this.noUnionView = (options === null || options === void 0 ? void 0 : options.noUnionView) || false;
        this.insertMethod = (options === null || options === void 0 ? void 0 : options.insertMethod) || BigQueryInsertMethod.loadTable;
        this.arrayHandling = (options === null || options === void 0 ? void 0 : options.arrayHandling) || ArrayHandling.arrays;
        this.arraySeparator = (options === null || options === void 0 ? void 0 : options.arraySeparator) || '|';
        this.customers = [];
        this.rowsByCustomer = {};
        const bqExecutorOptions = {
            datasetLocation: datasetLocation,
            bigqueryClient: this.bigquery,
        };
        this.bqExecutor = new BigQueryExecutor(projectId, bqExecutorOptions);
    }
    async beginScript(scriptName, query) {
        if (!scriptName)
            throw new Error('scriptName (used as name for table) was not specified');
        // a script's results go to a separate table with same name as a file
        if (this.tableTemplate) {
            this.tableId = substituteMacros(this.tableTemplate, { scriptName }).text;
        }
        else {
            this.tableId = scriptName;
        }
        this.dataset = await getDataset(this.bigquery, this.datasetId, this.datasetLocation);
        this.query = query;
        const schema = this.createSchema(query);
        this.schema = schema;
        if (this.dumpSchema) {
            this.logger.debug(JSON.stringify(schema, null, 2));
            const schemaJson = JSON.stringify(schema, undefined, 2);
            await fs_async.writeFile(scriptName + '_schema.json', schemaJson);
        }
    }
    async beginCustomer(customerId) {
        if (this.rowCountsByCustomer[customerId] !== undefined) {
            throw new Error(`Customer id ${customerId} already exist`);
        }
        this.customers.push(customerId);
        this.rowCountsByCustomer[customerId] = 0;
        if (this.insertMethod === BigQueryInsertMethod.loadTable) {
            const tableFullName = this.getTableFullname(customerId);
            const filepath = this.getDataFilePath(`.${tableFullName}.json`);
            const stream = this.createOutput(filepath);
            if (this.useFilePerCustomer()) {
                this.streamsByCustomer[customerId] = stream;
            }
            else {
                this.streamsByCustomer[''] = stream;
            }
            this.logger.verbose(`Temp output is ${stream.path}`);
            await stream.deleteFile();
        }
        else {
            this.rowsByCustomer[customerId] = [];
        }
    }
    getTableFullname(customerId) {
        var _a;
        if (!this.tableId)
            throw new Error('tableId is not set (probably beginScript method was not called)');
        const tableFullName = ((_a = this.query) === null || _a === void 0 ? void 0 : _a.resource.isConstant)
            ? this.tableId
            : this.tableId + '_' + customerId;
        return tableFullName;
    }
    async loadRows(customerId, tableFullName) {
        var _a;
        const rowCount = this.rowCountsByCustomer[customerId];
        this.logger.verbose(`Loading ${rowCount} rows into '${this.datasetId}.${tableFullName}' table`, {
            customerId: customerId,
            scriptName: this.tableId,
        });
        const table = this.dataset.table(tableFullName);
        const output = this.getOutput(customerId);
        const [job] = await table.load(output.isGCS ? output.getStorageFile() : output.path, {
            schema: this.schema,
            sourceFormat: 'NEWLINE_DELIMITED_JSON',
            writeDisposition: 'WRITE_TRUNCATE',
        });
        const errors = (_a = job.status) === null || _a === void 0 ? void 0 : _a.errors;
        if (errors && errors.length > 0) {
            throw errors;
        }
        this.logger.info(`${rowCount} rows loaded into '${this.datasetId}.${tableFullName}' table`, {
            customerId: customerId,
            scriptName: this.tableId,
        });
    }
    serializeRow(row) {
        const rowObj = {};
        for (let i = 0; i < row.length; i++) {
            const colName = this.query.columnNames[i];
            const colType = this.query.columnTypes[i];
            let val = row[i];
            if (colType.repeated) {
                if (!val) {
                    // repeated field can't access nulls
                    val = [];
                }
                else if (isArray(val)) {
                    // there could be structs (or arrays, or their combinations)
                    // in the array, and they will be rejected
                    for (let j = 0; j < val.length; j++) {
                        const subval = val[j];
                        if (isArray(subval) || isObjectLike(subval)) {
                            val[j] = JSON.stringify(subval);
                        }
                    }
                }
                if (isArray(val) && this.arrayHandling === ArrayHandling.strings) {
                    val = val.join(this.arraySeparator);
                }
            }
            else if (colType.kind === FieldTypeKind.struct) {
                // we don't support structs at the moment
                if (val) {
                    val = JSON.stringify(val);
                }
            }
            rowObj[colName] = val;
        }
        return rowObj;
    }
    async insertRows(rows, customerId, tableFullName) {
        var _a;
        const tableId = this.tableId;
        try {
            // insert rows by chunks (there's a limit for insert)
            const table = this.dataset.table(tableId);
            for (let i = 0, j = rows.length; i < j; i += MAX_ROWS) {
                const rowsChunk = rows.slice(i, i + MAX_ROWS);
                let templateSuffix = undefined;
                if (!((_a = this.query) === null || _a === void 0 ? void 0 : _a.resource.isConstant)) {
                    templateSuffix = '_' + customerId;
                }
                this.logger.verbose(`Inserting ${rowsChunk.length} rows`, {
                    customerId: customerId,
                });
                await table.insert(rowsChunk, {
                    templateSuffix: templateSuffix,
                    schema: this.schema,
                });
            }
        }
        catch (e) {
            this.logger.debug(e);
            this.logger.error(`Failed to insert rows into '${tableFullName}' table`, {
                customerId: customerId,
            });
            if (e.name === 'PartialFailureError') {
                // Some rows failed to insert, while others may have succeeded.
                const max_errors_to_show = 10;
                const msgDetail = e.errors.length > max_errors_to_show
                    ? `showing first ${max_errors_to_show} errors of ${e.errors.length})`
                    : e.errors.length + ' error(s)';
                this.logger.warn(`Some rows failed to insert (${msgDetail}):`, {
                    customerId: customerId,
                });
                // show first 10 rows with errors
                for (let i = 0; i < Math.min(e.errors.length, 10); i++) {
                    const err = e.errors[i];
                    this.logger.warn(`#${i} row:\n${JSON.stringify(err.row, null, 2)}\nError: ${err.errors[0].message}`, { customerId: customerId });
                }
            }
            else if (e.code === 404) {
                // ApiError: "Table 162551664177:dataset.table not found"
                // This is unexpected but theoretically can happen (and did)
                // due to eventually consistency of BigQuery
                console.error(`Table ${tableFullName} not found.`, {
                    customerId: customerId,
                });
                const table = this.dataset.table(tableId);
                const exists = await table.exists();
                console.warn(`Table ${tableFullName} existence check: ${exists}`);
            }
            throw e;
        }
        this.logger.info(`${rows.length} rows inserted into '${tableFullName}' table`, {
            customerId: customerId,
        });
    }
    async endCustomer(customerId) {
        if (!this.tableId) {
            throw new Error('No table id is set. Did you call beginScript method?');
        }
        if (!customerId) {
            throw new Error('No customer id is specified');
        }
        // NOTE: for constant resources we don't use templated tables (table per
        // customer)
        const tableFullName = this.getTableFullname(customerId);
        //  remove customer's table (to make sure you have only fresh data)
        try {
            this.logger.debug(`Removing table '${tableFullName}'`, {
                customerId: customerId,
                scriptName: this.tableId,
            });
            await this.dataset.table(tableFullName).delete({ ignoreNotFound: true });
        }
        catch (e) {
            this.logger.error(`Deletion of table '${tableFullName}' failed: ${e}`, {
                customerId: customerId,
                scriptName: this.tableId,
            });
            throw e;
        }
        const rowCount = this.rowCountsByCustomer[customerId];
        if (this.insertMethod === BigQueryInsertMethod.loadTable) {
            // finalize the output stream
            const output = this.getOutput(customerId);
            await this.closeStream(output);
            if (rowCount > 0) {
                this.logger.debug(`Data prepared in '${output.path}', closed: ${output.stream.closed}`, {
                    customerId: customerId,
                    scriptName: this.tableId,
                });
            }
        }
        if (rowCount > 0) {
            // upload data to BQ: we support two methods: via insertAll and loadTable,
            // the later is default one as much more faster (but it requires dumping data on the disk)
            if (this.insertMethod === BigQueryInsertMethod.insertAll) {
                const rows = this.rowsByCustomer[customerId];
                await this.insertRows(rows, customerId, tableFullName);
                this.rowsByCustomer[customerId] = [];
            }
            else {
                await this.loadRows(customerId, tableFullName);
            }
        }
        else {
            // no rows found for the customer, as so no table was created,
            // create an empty one, so we could use it for a union view
            try {
                // it might a case when createTables fails because the previous delete
                // hasn't been propagated
                await this.ensureTableCreated(tableFullName);
                this.logger.verbose(`Created empty table '${tableFullName}'`, {
                    customerId: customerId,
                    scriptName: this.tableId,
                });
            }
            catch (e) {
                this.logger.error(`Creation of empty table '${tableFullName}' failed: ${e}`);
                throw e;
            }
        }
        if (!this.dumpData &&
            this.insertMethod === BigQueryInsertMethod.loadTable) {
            const output = this.getOutput(customerId);
            this.logger.verbose(`Removing data file ${output.path}`);
            await output.deleteFile();
        }
    }
    async endScript() {
        if (!this.tableId) {
            throw new Error('No table id is set. Did you call beginScript method?');
        }
        if (!this.query) {
            throw new Error('No query is set. Did you call beginScript method?');
        }
        if (!this.query.resource.isConstant && !this.noUnionView) {
            /*
            Create a view to union all customer tables (if not disabled explicitly):
            CREATE OR REPLACE VIEW `dataset.resource` AS
              SELECT * FROM `dataset.resource_*`;
            */
            const table_fq = await this.bqExecutor.createUnifiedView(this.datasetId, this.tableId, this.customers);
            this.logger.info(`Created a union view '${table_fq}'`, {
                scriptName: this.tableId,
            });
        }
        this.tableId = undefined;
        this.query = undefined;
        this.customers = [];
        if (this.rowsByCustomer)
            this.rowsByCustomer = {};
        if (this.streamsByCustomer)
            this.streamsByCustomer = {};
        this.rowCountsByCustomer = {};
    }
    createSchema(query) {
        const schema = { fields: [] };
        for (const column of query.columns) {
            const field = {
                mode: column.type.repeated && this.arrayHandling === ArrayHandling.arrays
                    ? 'REPEATED'
                    : 'NULLABLE',
                name: column.name.replace(/\./g, '_'),
                type: this.getBigQueryFieldType(column.type),
            };
            // STRING, BYTES, INTEGER, INT64 (same as INTEGER), FLOAT, FLOAT64 (same
            // as FLOAT), NUMERIC, BIGNUMERIC, BOOLEAN, BOOL (same as BOOLEAN),
            // TIMESTAMP, DATE, TIME, DATETIME, INTERVAL, RECORD (where RECORD
            // indicates that the field contains a nested schema) or STRUCT (same as
            // RECORD).
            schema.fields.push(field);
        }
        return schema;
    }
    getBigQueryFieldType(colType) {
        if (this.arrayHandling === ArrayHandling.strings && colType.repeated)
            return 'STRING';
        if (isString(colType.type)) {
            switch (colType.type.toLowerCase()) {
                case 'int32':
                case 'int64':
                    return 'INT64';
                case 'double':
                case 'float':
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
    async addRow(customerId, parsedRow) {
        if (!parsedRow || parsedRow.length === 0)
            return;
        const rowObj = this.serializeRow(parsedRow);
        if (this.insertMethod === BigQueryInsertMethod.loadTable) {
            // dump the row object to a file
            const content = JSON.stringify(rowObj) + '\n';
            await this.writeContent(customerId, content);
        }
        else {
            this.rowsByCustomer[customerId].push(rowObj);
        }
        this.rowCountsByCustomer[customerId] += 1;
    }
    async ensureTableCreated(tableFullName, maxRetries = 5) {
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                await this.dataset.createTable(tableFullName, { schema: this.schema });
                return;
            }
            catch (e) {
                if (e.code === 409) {
                    // 409 - ApiError: Already Exists
                    // probably the table still hasn't been deleted, wait a bit
                    await delay(200);
                    continue;
                }
                throw e;
            }
        }
        throw new Error(`Failed to create a table ${tableFullName} after ${maxRetries} attempts`);
    }
}
//# sourceMappingURL=bq-writer.js.map