"use strict";
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BigQueryWriter = exports.BigQueryArrayHandling = exports.BigQueryInsertMethod = void 0;
const bigquery_1 = require("@google-cloud/bigquery");
const node_fs_1 = __importDefault(require("node:fs"));
const promises_1 = __importDefault(require("node:fs/promises"));
const node_path_1 = __importDefault(require("node:path"));
const lodash_1 = __importDefault(require("lodash"));
const logger_1 = require("./logger");
const types_1 = require("./types");
const utils_1 = require("./utils");
const bq_common_1 = require("./bq-common");
const MAX_ROWS = 50000;
var BigQueryInsertMethod;
(function (BigQueryInsertMethod) {
    BigQueryInsertMethod[BigQueryInsertMethod["insertAll"] = 0] = "insertAll";
    BigQueryInsertMethod[BigQueryInsertMethod["loadTable"] = 1] = "loadTable";
})(BigQueryInsertMethod = exports.BigQueryInsertMethod || (exports.BigQueryInsertMethod = {}));
var BigQueryArrayHandling;
(function (BigQueryArrayHandling) {
    BigQueryArrayHandling["strings"] = "strings";
    BigQueryArrayHandling["arrays"] = "arrays";
})(BigQueryArrayHandling = exports.BigQueryArrayHandling || (exports.BigQueryArrayHandling = {}));
class BigQueryWriter {
    constructor(projectId, dataset, options) {
        const datasetLocation = (options === null || options === void 0 ? void 0 : options.datasetLocation) || "us";
        this.bigquery = new bigquery_1.BigQuery({
            projectId: projectId,
            scopes: bq_common_1.OAUTH_SCOPES,
            keyFilename: options === null || options === void 0 ? void 0 : options.keyFilePath,
            location: datasetLocation,
        });
        this.datasetId = dataset;
        this.datasetLocation = datasetLocation;
        this.tableTemplate = options === null || options === void 0 ? void 0 : options.tableTemplate;
        this.dumpSchema = (options === null || options === void 0 ? void 0 : options.dumpSchema) || false;
        this.dumpData = (options === null || options === void 0 ? void 0 : options.dumpData) || false;
        this.keepData = (options === null || options === void 0 ? void 0 : options.keepData) || false;
        this.noUnionView = (options === null || options === void 0 ? void 0 : options.noUnionView) || false;
        this.insertMethod = (options === null || options === void 0 ? void 0 : options.insertMethod) || BigQueryInsertMethod.loadTable;
        this.arrayHandling = (options === null || options === void 0 ? void 0 : options.arrayHandling) || BigQueryArrayHandling.arrays;
        this.arraySeparator = (options === null || options === void 0 ? void 0 : options.arraySeparator) || '|';
        this.customers = [];
        this.rowsByCustomer = {};
        this.rowCountsByCustomer = {};
        this.streamsByCustomer = {};
        this.logger = (0, logger_1.getLogger)();
    }
    async beginScript(scriptName, query) {
        if (!scriptName)
            throw new Error(`scriptName (used as name for table) was not specified`);
        // a script's results go to a separate table with same name as a file
        if (this.tableTemplate) {
            this.tableId = (0, utils_1.substituteMacros)(this.tableTemplate, { scriptName }).text;
        }
        else {
            this.tableId = scriptName;
        }
        this.dataset = await (0, bq_common_1.getDataset)(this.bigquery, this.datasetId, this.datasetLocation);
        this.query = query;
        let schema = this.createSchema(query);
        this.schema = schema;
        if (this.dumpSchema) {
            this.logger.debug(JSON.stringify(schema, null, 2));
            let schemaJson = JSON.stringify(schema, undefined, 2);
            await promises_1.default.writeFile(scriptName + ".json", schemaJson);
        }
    }
    beginCustomer(customerId) {
        if (this.rowsByCustomer[customerId]) {
            throw new Error(`Customer id ${customerId} already exist`);
        }
        this.customers.push(customerId);
        this.rowsByCustomer[customerId] = [];
        this.rowCountsByCustomer[customerId] = 0;
        if (this.insertMethod === BigQueryInsertMethod.loadTable) {
            let tableFullName = this.getTableFullname(customerId);
            let filepath = this.getDataFilepath(tableFullName);
            if (node_fs_1.default.existsSync(filepath)) {
                node_fs_1.default.rmSync(filepath);
            }
            this.streamsByCustomer[customerId] = node_fs_1.default.createWriteStream(filepath);
        }
    }
    getTableFullname(customerId) {
        var _a;
        if (!this.tableId)
            throw new Error(`tableId is not set (probably beginScript method was not called)`);
        let tableFullName = ((_a = this.query) === null || _a === void 0 ? void 0 : _a.resource.isConstant)
            ? this.tableId
            : this.tableId + "_" + customerId;
        return tableFullName;
    }
    getDataFilepath(tableFullName) {
        let filepath = `.${tableFullName}.json`;
        if (process.env.K_SERVICE) {
            // we're in GCloud - file system is readonly, the only writable place is /tmp
            // TODO: use streaming uploads (https://cloud.google.com/storage/docs/streaming-uploads)
            filepath = node_path_1.default.join("/tmp", filepath);
        }
        return filepath;
    }
    async loadRows(customerId, tableFullName) {
        let filepath = this.getDataFilepath(tableFullName);
        let rowCount = this.rowCountsByCustomer[customerId];
        this.logger.verbose(`Loading ${rowCount} rows into '${this.datasetId}.${tableFullName}' table`, {
            customerId: customerId,
            scriptName: this.tableId,
        });
        let table = this.dataset.table(tableFullName);
        await table.load(filepath, {
            schema: this.schema,
            sourceFormat: "NEWLINE_DELIMITED_JSON",
            writeDisposition: "WRITE_TRUNCATE",
        });
        this.logger.info(`${rowCount} rows loaded into '${this.datasetId}.${tableFullName}' table`, {
            customerId: customerId,
            scriptName: this.tableId,
        });
    }
    prepareRow(row) {
        let rowObj = {};
        for (let i = 0; i < row.length; i++) {
            let colName = this.query.columnNames[i];
            const colType = this.query.columnTypes[i];
            let val = row[i];
            if (colType.repeated) {
                if (!val) {
                    // repeated field can't access nulls
                    val = [];
                }
                else if (val.length) {
                    // there could be structs (or arrays, or their combinations)
                    // in the array, and they will be rejected
                    for (let j = 0; j < val.length; j++) {
                        let subval = val[j];
                        if (lodash_1.default.isArray(subval) || lodash_1.default.isObjectLike(subval)) {
                            val[j] = JSON.stringify(subval);
                        }
                    }
                }
                if (this.arrayHandling === BigQueryArrayHandling.strings) {
                    val = val.join(this.arraySeparator);
                }
            }
            else if (colType.kind === types_1.FieldTypeKind.struct) {
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
            let table = this.dataset.table(tableId);
            for (let i = 0, j = rows.length; i < j; i += MAX_ROWS) {
                let rowsChunk = rows.slice(i, i + MAX_ROWS);
                let rows2insert = rowsChunk.map((row) => this.prepareRow(row));
                let templateSuffix = undefined;
                if (!((_a = this.query) === null || _a === void 0 ? void 0 : _a.resource.isConstant)) {
                    templateSuffix = "_" + customerId;
                }
                this.logger.verbose(`Inserting ${rowsChunk.length} rows`, {
                    customerId: customerId,
                });
                await table.insert(rows2insert, {
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
            if (e.name === "PartialFailureError") {
                // Some rows failed to insert, while others may have succeeded.
                const max_errors_to_show = 10;
                let msgDetail = e.errors.length > max_errors_to_show
                    ? `showing first ${max_errors_to_show} errors of ${e.errors.length})`
                    : e.errors.length + " error(s)";
                this.logger.warn(`Some rows failed to insert (${msgDetail}):`, {
                    customerId: customerId,
                });
                // show first 10 rows with errors
                for (let i = 0; i < Math.min(e.errors.length, 10); i++) {
                    let err = e.errors[i];
                    this.logger.warn(`#${i} row:\n${JSON.stringify(err.row, null, 2)}\nError: ${err.errors[0].message}`, { customerId: customerId });
                }
            }
            else if (e.code === 404) {
                // ApiError: "Table 162551664177:dataset.table not found"
                // This is unexpected but theriotically can happen (and did) due to eventually consistency of BigQuery
                console.error(`Table ${tableFullName} not found.`, {
                    customerId: customerId,
                });
                const table = this.dataset.table(tableId);
                let exists = await table.exists();
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
            await this.dataset.table(tableFullName).delete({ ignoreNotFound: true });
        }
        catch (e) {
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
            }
            else {
                await this.loadRows(customerId, tableFullName);
            }
        }
        else {
            // no rows found for the customer, as so no table was created,
            // create an empty one, so we could use it for a union view
            try {
                await this.dataset.createTable(tableFullName, { schema: this.schema });
                this.logger.verbose(`Created empty table '${tableFullName}'`, {
                    customerId: customerId,
                    scriptName: this.tableId,
                });
            }
            catch (e) {
                this.logger.error(`\tCreation of empty table '${tableFullName}' failed: ${e}`);
                throw e;
            }
        }
        if (!this.dumpData &&
            this.insertMethod === BigQueryInsertMethod.loadTable) {
            let filepath = this.getDataFilepath(tableFullName);
            if (node_fs_1.default.existsSync(filepath)) {
                this.logger.verbose(`Removing data file ${filepath}, dumpData=${this.dumpData}`);
                node_fs_1.default.rmSync(filepath);
            }
        }
        if (!this.keepData) {
            this.rowsByCustomer[customerId] = [];
        }
    }
    async endScript() {
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
            await this.dataset.table(this.tableId).delete({ ignoreNotFound: true });
            const table_fq = `${this.datasetId}.${this.tableId}`;
            try {
                // here there's a potential problem. If wildcard expression (resource_*)
                // catches another view the DML-query will fail with error:
                // 'Views cannot be queried through prefix. First view projectid:datasetid.viewname.'
                const query = `CREATE OR REPLACE VIEW \`${table_fq}\` AS SELECT * FROM \`${table_fq}_*\` WHERE _TABLE_SUFFIX in (${this.customers
                    .map((s) => "'" + s + "'")
                    .join(",")})`;
                this.logger.debug(query);
                await this.dataset.query({
                    query: query,
                });
            }
            catch (e) {
                this.logger.error(`An error occured during creating the unified view (${table_fq}): ${e.message}`);
                if (e.message.includes("Views cannot be queried through prefix")) {
                    this.logger.warn(`You have to rename the script ${this.tableId} to a name so the wildcard expression ${this.tableId}_* would catch other views `);
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
    createSchema(query) {
        let schema = { fields: [] };
        for (let column of query.columns) {
            let field = {
                mode: column.type.repeated &&
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
            schema.fields.push(field);
        }
        return schema;
    }
    getBigQueryFieldType(colType) {
        if (this.arrayHandling === BigQueryArrayHandling.strings && colType.repeated)
            return "STRING";
        if (lodash_1.default.isString(colType.type)) {
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
        if ((0, types_1.isEnumType)(colType.type)) {
            return "STRING";
        }
        // TODO: any other means STRUCT, but do we really need structs in BQ?
        return "STRING";
    }
    addRow(customerId, parsedRow) {
        if (!parsedRow || parsedRow.length == 0)
            return;
        if (this.insertMethod === BigQueryInsertMethod.loadTable) {
            // dump the row object to a file
            let row_obj = this.prepareRow(parsedRow);
            let fsStream = this.streamsByCustomer[customerId];
            fsStream.write(JSON.stringify(row_obj));
            fsStream.write("\n");
        }
        else {
            this.rowsByCustomer[customerId].push(parsedRow);
        }
        this.rowCountsByCustomer[customerId] += 1;
    }
}
exports.BigQueryWriter = BigQueryWriter;
//# sourceMappingURL=bq-writer.js.map