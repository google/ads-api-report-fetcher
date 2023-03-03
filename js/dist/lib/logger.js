"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.logger = exports.LOG_LEVEL = void 0;
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
const winston_1 = __importDefault(require("winston"));
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;
const { format } = winston_1.default;
/** Default log level */
exports.LOG_LEVEL = argv.loglevel || process.env.LOG_LEVEL ||
    (process.env.NODE_ENV === 'production' ? 'info' : 'verbose');
const colors = {
    error: 'red',
    warn: 'yellow',
    info: 'white',
    verbose: 'gray',
    debug: 'grey',
};
function wrap(str) {
    return str ? ' [' + str + ']' : '';
}
const formats = [];
if (process.stdout.isTTY) {
    formats.push(format.colorize({ all: true }));
    winston_1.default.addColors(colors);
}
formats.push(format.printf((info) => `${info.timestamp}${wrap(info.scriptName)}${wrap(info.customerId)}: ${info.message}`));
const transports = [];
transports.push(new winston_1.default.transports.Console({
    format: format.combine(...formats)
}));
exports.logger = winston_1.default.createLogger({
    level: exports.LOG_LEVEL,
    format: format.combine(format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss:SSS' })),
    transports
});
//# sourceMappingURL=logger.js.map