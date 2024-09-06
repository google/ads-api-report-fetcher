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
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AdsRowParser = exports.ParseResultMode = void 0;
const google_ads_api_1 = require("google-ads-api");
const lodash_1 = __importStar(require("lodash"));
const types_1 = require("./types");
const utils_1 = require("./utils");
const CAMEL_TO_SNAKE_REGEX = /[A-Z]/g;
/**
 * How to parse results from Ads API in `IAdsRowParser.parseRow`.
 */
var ParseResultMode;
(function (ParseResultMode) {
    /**
     * Return results as an array.
     */
    ParseResultMode[ParseResultMode["Array"] = 1] = "Array";
    /**
     * Return results as an object.
     */
    ParseResultMode[ParseResultMode["Object"] = 2] = "Object";
})(ParseResultMode = exports.ParseResultMode || (exports.ParseResultMode = {}));
class AdsRowParser {
    constructor(apiType) {
        this.apiType = apiType;
    }
    normalizeName(name) {
        if (this.apiType === types_1.ApiType.REST) {
            return name.replace(CAMEL_TO_SNAKE_REGEX, (letter) => `_${letter.toLowerCase()}`);
        }
        return name;
    }
    parseRow(row, query, objectMode = false) {
        // flatten the tree of object into a flat obejct with all properties
        const rowValues = {};
        for (const field of Object.keys(row)) {
            const item = row[field];
            rowValues[this.normalizeName(field)] = item;
            (0, utils_1.traverseObject)(item, (name, value, path) => {
                const field_full = path.join('.');
                rowValues[this.normalizeName(field_full)] = value;
            }, [field]);
        }
        // process customizers and flatten row object into array
        const rowValuesArr = [];
        for (let i = 0; i < query.columns.length; i++) {
            const column = query.columns[i];
            let value;
            if (column.customizer) {
                value = this.getValueWithCustomizer(rowValues, column, column.customizer, query);
            }
            else {
                value = rowValues[column.expression];
            }
            if (objectMode) {
                rowValues[column.name] = value;
            }
            else {
                rowValuesArr.push(value);
            }
        }
        // post process enum (convert number to enum field names) and structs
        if (this.apiType === types_1.ApiType.gRPC) {
            // NOTE: gRPC API returns enums as numbers, while REST API returns strings
            for (let i = 0; i < query.columns.length; i++) {
                const column = query.columns[i];
                const value = objectMode
                    ? rowValues[column.name]
                    : rowValuesArr[i];
                const colType = column.type;
                if (colType.kind === types_1.FieldTypeKind.enum &&
                    colType.repeated &&
                    lodash_1.default.isArray(value)) {
                    for (let j = 0; j < value.length; j++) {
                        const subval = value[j];
                        if (lodash_1.default.isNumber(subval)) {
                            const enumType = google_ads_api_1.enums[colType.typeName];
                            if (enumType) {
                                value[j] = enumType[subval];
                            }
                        }
                    }
                }
                else if (colType.kind === types_1.FieldTypeKind.enum) {
                    if (lodash_1.default.isNumber(value)) {
                        const enumType = google_ads_api_1.enums[colType.typeName];
                        if (enumType) {
                            if (objectMode) {
                                rowValues[column.name] = enumType[value];
                            }
                            else {
                                rowValuesArr[i] = enumType[value];
                            }
                        }
                    }
                }
                else if (colType.kind === types_1.FieldTypeKind.struct) {
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    if (value && value.toJSON) {
                        if (objectMode) {
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            rowValues[column.name] = value.toJSON();
                        }
                        else {
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            rowValuesArr[i] = value.toJSON();
                        }
                    }
                }
            }
        }
        return objectMode ? rowValues : rowValuesArr;
    }
    getValueWithCustomizer(row, column, customizer, query) {
        let value;
        if (customizer.type === types_1.CustomizerType.VirtualColumn) {
            try {
                value = customizer.evaluator.evaluate(row);
            }
            catch (e) {
                if (e.message.includes('TypeError: Cannot read properties of null')) {
                    value = null;
                }
            }
        }
        else {
            value = row[column.expression];
            if (!value)
                return value;
            if (customizer.type === types_1.CustomizerType.Function) {
                const func = query.functions[customizer.function];
                value = func(value);
            }
            else {
                // for other customizers we support arrays specifically
                if (lodash_1.default.isArray(value)) {
                    const new_value = [];
                    for (let j = 0; j < value.length; j++) {
                        new_value[j] = this.parseScalarValue(value[j], customizer);
                    }
                    value = new_value;
                }
                else {
                    value = this.parseScalarValue(value, customizer);
                }
            }
        }
        return value;
    }
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    parseScalarValue(value, customizer) {
        if (customizer.type === types_1.CustomizerType.NestedField) {
            value = (0, utils_1.navigateObject)(value, customizer.selector);
        }
        else if (customizer.type === types_1.CustomizerType.ResourceIndex) {
            // the value from query's result we expect to be a string
            if (!(0, lodash_1.isString)(value)) {
                throw new Error(`Unexpected value type ${typeof value} ('${value}') for column with ResourceIndex customizer`);
            }
            value = value.split('~')[customizer.index];
            if (value) {
                if (customizer.index === 0) {
                    // e.g. customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}
                    // here index=0 ("~0") corresponds ad_group_id
                    // and index=1 ("~1") - ad_id
                    value = value.match(/[^/]+\/(\d+)$/)[1];
                }
                value = (0, utils_1.tryParseNumber)(value);
            }
        }
        return value;
    }
}
exports.AdsRowParser = AdsRowParser;
//# sourceMappingURL=ads-row-parser.js.map