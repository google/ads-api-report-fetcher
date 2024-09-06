"use strict";
/*
 Copyright 2024 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConsoleQueryReader = exports.FileQueryReader = void 0;
const path_1 = __importDefault(require("path"));
const chalk_1 = __importDefault(require("chalk"));
const file_utils_1 = require("./file-utils");
const logger_1 = require("./logger");
const glob_1 = require("glob");
class FileQueryReader {
    constructor(scripts) {
        this.scripts = [];
        if (scripts && scripts.length) {
            for (let script of scripts) {
                if (script.includes('*') || script.includes('**')) {
                    const expanded_files = (0, glob_1.globSync)(script);
                    this.scripts.push(...expanded_files);
                }
                else {
                    script = script.trim();
                    if (script)
                        this.scripts.push(script);
                }
            }
        }
        this.logger = (0, logger_1.getLogger)();
    }
    async *[Symbol.asyncIterator]() {
        for (const script of this.scripts) {
            const queryText = await (0, file_utils_1.getFileContent)(script);
            this.logger.info(`Processing query from ${chalk_1.default.gray(script)}`);
            const scriptName = path_1.default.basename(script).split('.sql')[0];
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
            let scriptName = 'query' + i;
            const match = script.match(/^([\d\w]+):/);
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