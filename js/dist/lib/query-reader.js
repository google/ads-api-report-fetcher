"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConsoleQueryReader = exports.FileQueryReader = void 0;
const path_1 = __importDefault(require("path"));
const chalk_1 = __importDefault(require("chalk"));
const file_utils_1 = require("./file-utils");
const logger_1 = require("./logger");
class FileQueryReader {
    constructor(scripts) {
        this.scripts = scripts || [];
        this.logger = (0, logger_1.getLogger)();
    }
    async *[Symbol.asyncIterator]() {
        for (const script of this.scripts) {
            let queryText = await (0, file_utils_1.getFileContent)(script);
            this.logger.info(`Processing query from ${chalk_1.default.gray(script)}`);
            let scriptName = path_1.default.basename(script).split(".sql")[0];
            const item = { name: scriptName, text: queryText };
            yield item;
        }
    }
}
exports.FileQueryReader = FileQueryReader;
class ConsoleQueryReader {
    constructor(scripts) {
        this.scripts = scripts || [];
        this.logger = (0, logger_1.getLogger)();
    }
    async *[Symbol.asyncIterator]() {
        let i = 0;
        for (let script of this.scripts) {
            i++;
            let scriptName = "query" + i;
            let match = script.match(/^([\d\w]+)\:/);
            if (match && match.length > 1) {
                scriptName = match[1];
                script = script.substring(scriptName.length + 1);
            }
            this.logger.info(`Processing inline query ${scriptName}:\n ${chalk_1.default.gray(script)}`);
            const item = { name: scriptName, text: script };
            yield item;
        }
    }
}
exports.ConsoleQueryReader = ConsoleQueryReader;
//# sourceMappingURL=query-reader.js.map