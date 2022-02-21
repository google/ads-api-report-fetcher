"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NullWriter = exports.CsvWriter = void 0;
const sync_1 = require("csv-stringify/sync");
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
class CsvWriter {
    constructor(options) {
        this.appending = false;
        this.customerRows = 0;
        this.rows = [];
        this.destination = options === null || options === void 0 ? void 0 : options.destinationFolder;
    }
    beginScript(scriptName, query) {
        this.appending = false;
        this.query = query;
        let filename = scriptName + '.csv';
        if (this.destination) {
            if (!fs_1.default.existsSync(this.destination)) {
                fs_1.default.mkdirSync(this.destination, { recursive: true });
            }
            filename = path_1.default.join(this.destination, filename);
        }
        this.filename = filename;
        if (fs_1.default.existsSync(this.filename)) {
            fs_1.default.rmSync(this.filename);
        }
    }
    endScript() {
        this.filename = undefined;
    }
    beginCustomer(customerId) {
        this.rows = [];
    }
    endCustomer() {
        if (!this.rows.length) {
            return;
        }
        let csvOptions = {
            header: !this.appending,
            quoted: false,
            columns: this.query.columnNames,
            cast: {
                boolean: (value, context) => value ? 'true' : 'false'
            }
        };
        let csv = (0, sync_1.stringify)(this.rows, csvOptions);
        fs_1.default.writeFileSync(this.filename, csv, { encoding: 'utf-8', flag: this.appending ? 'a' : 'w' });
        if (this.rows.length > 0) {
            console.log((this.appending ? 'Updated ' : 'Created ') + this.filename +
                ` with ${this.rows.length} rows`);
        }
        this.appending = true;
        this.rows = [];
    }
    addRow(parsedRow) {
        if (!parsedRow || parsedRow.length == 0)
            return;
        this.rows.push(parsedRow);
    }
}
exports.CsvWriter = CsvWriter;
class NullWriter {
    beginScript(scriptName, query) { }
    endScript() { }
    beginCustomer(customerId) { }
    endCustomer() { }
    addRow(parsedRow) { }
}
exports.NullWriter = NullWriter;
//# sourceMappingURL=csv-writer.js.map