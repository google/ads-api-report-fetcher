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
import { enums } from 'google-ads-api';
import { isNumber, isArray, isString } from 'lodash-es';
import { ApiType, CustomizerType, FieldTypeKind, } from './types.js';
import { navigateObject, traverseObject, tryParseNumber } from './utils.js';
const CAMEL_TO_SNAKE_REGEX = /[A-Z]/g;
/**
 * How to parse results from Ads API in `IAdsRowParser.parseRow`.
 */
export var ParseResultMode;
(function (ParseResultMode) {
    /**
     * Return results as an array.
     */
    ParseResultMode[ParseResultMode["Array"] = 1] = "Array";
    /**
     * Return results as an object.
     */
    ParseResultMode[ParseResultMode["Object"] = 2] = "Object";
})(ParseResultMode || (ParseResultMode = {}));
export class AdsRowParser {
    constructor(apiType) {
        this.apiType = apiType;
    }
    normalizeName(name) {
        if (this.apiType === ApiType.REST) {
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
            traverseObject(item, (name, value, path) => {
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
        if (this.apiType === ApiType.gRPC) {
            // NOTE: gRPC API returns enums as numbers, while REST API returns strings
            for (let i = 0; i < query.columns.length; i++) {
                const column = query.columns[i];
                const value = objectMode
                    ? rowValues[column.name]
                    : rowValuesArr[i];
                const colType = column.type;
                if (colType.kind === FieldTypeKind.enum &&
                    colType.repeated &&
                    isArray(value)) {
                    for (let j = 0; j < value.length; j++) {
                        const subval = value[j];
                        if (isNumber(subval)) {
                            const enumType = enums[colType.typeName];
                            if (enumType) {
                                value[j] = enumType[subval];
                            }
                        }
                    }
                }
                else if (colType.kind === FieldTypeKind.enum) {
                    if (isNumber(value)) {
                        const enumType = enums[colType.typeName];
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
                else if (colType.kind === FieldTypeKind.struct) {
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
        if (customizer.type === CustomizerType.VirtualColumn) {
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
            if (customizer.type === CustomizerType.Function) {
                const func = query.functions[customizer.function];
                value = func(value);
            }
            else {
                // for other customizers we support arrays specifically
                if (isArray(value)) {
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
        if (customizer.type === CustomizerType.NestedField) {
            value = navigateObject(value, customizer.selector);
        }
        else if (customizer.type === CustomizerType.ResourceIndex) {
            // the value from query's result we expect to be a string
            if (!isString(value)) {
                // we fetched a struct, let's try to find a suitable field with resource
                let resourceVal = '';
                if (value['name'] && isString(value['name'])) {
                    resourceVal = value['name'];
                }
                else if (value['text'] && isString(value['text'])) {
                    resourceVal = value['text'];
                }
                else if (value['asset'] && isString(value['asset'])) {
                    resourceVal = value['asset'];
                }
                else if (value['value'] && isString(value['value'])) {
                    resourceVal = value['value'];
                }
                if (resourceVal) {
                    value = resourceVal;
                }
                else {
                    throw new Error(`Unexpected value for ResourceIndex source: ${JSON.stringify(value)}.` +
                        'We expect either a string or a struct with fields name/text/asset/value');
                }
            }
            value = value.split('~')[customizer.index];
            if (value) {
                if (customizer.index === 0) {
                    // e.g. customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}
                    // here index=0 ("~0") corresponds ad_group_id
                    // and index=1 ("~1") - ad_id
                    value = value.match(/[^/]+\/(\d+)$/)[1];
                }
                value = tryParseNumber(value);
            }
        }
        return value;
    }
}
//# sourceMappingURL=ads-row-parser.js.map