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
exports.AdsRowParser = void 0;
const google_ads_api_1 = require("google-ads-api");
const lodash_1 = __importDefault(require("lodash"));
const types_1 = require("./types");
const utils_1 = require("./utils");
class AdsRowParser {
    parseRow(row, query) {
        // flatten the tree of object into a flat obejct with all properties
        let row_values = {};
        for (let field of Object.keys(row)) {
            let item = row[field];
            row_values[field] = item;
            (0, utils_1.traverseObject)(item, (name, value, path, object) => {
                let field_full = path.join(".");
                row_values[field_full] = value;
            }, [field]);
        }
        // process customizers and flatten row object into array
        let row_values_arr = [];
        for (let i = 0; i < query.columns.length; i++) {
            let column = query.columns[i];
            let value;
            if (column.customizer) {
                value = this.getValueWithCustomizer(row_values, column, column.customizer, query);
            }
            else {
                value = row_values[column.expression];
            }
            row_values_arr.push(value);
        }
        // post process enum (convert number to enum field names) and structs
        for (let i = 0; i < query.columns.length; i++) {
            let column = query.columns[i];
            let value = row_values_arr[i];
            //let colType = query.columnTypes[i];
            let colType = column.type;
            if (colType.kind === types_1.FieldTypeKind.enum &&
                colType.repeated &&
                lodash_1.default.isArray(value)) {
                for (let j = 0; j < value.length; j++) {
                    let subval = value[j];
                    if (lodash_1.default.isNumber(subval)) {
                        let enumType = google_ads_api_1.enums[colType.typeName];
                        if (enumType) {
                            value[j] = enumType[subval];
                        }
                    }
                }
            }
            else if (colType.kind === types_1.FieldTypeKind.enum) {
                if (lodash_1.default.isNumber(value)) {
                    let enumType = google_ads_api_1.enums[colType.typeName];
                    if (enumType) {
                        row_values_arr[i] = enumType[value];
                    }
                }
            }
            else if (colType.kind === types_1.FieldTypeKind.struct) {
                if (value && value.toJSON) {
                    row_values_arr[i] = value.toJSON();
                }
            }
        }
        return row_values_arr;
    }
    getValueWithCustomizer(row, column, customizer, query) {
        let value;
        if (customizer.type === types_1.CustomizerType.VirtualColumn) {
            try {
                value = customizer.evaluator.evaluate(row);
            }
            catch (e) {
                if (e.message.includes("TypeError: Cannot read properties of null")) {
                    value = null;
                }
            }
        }
        else {
            value = row[column.expression];
            if (!value)
                return value;
            if (customizer.type === types_1.CustomizerType.Function) {
                let func = query.functions[customizer.function];
                value = func(value);
            }
            else {
                // for other customers we support arrays specifically
                if (lodash_1.default.isArray(value)) {
                    let new_value = [];
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
    parseScalarValue(value, customizer) {
        if (customizer.type === types_1.CustomizerType.NestedField) {
            value = (0, utils_1.navigateObject)(value, customizer.selector);
        }
        else if (customizer.type === types_1.CustomizerType.ResourceIndex) {
            value = value.split("~")[customizer.index];
            if (value) {
                if (customizer.index === 0) {
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