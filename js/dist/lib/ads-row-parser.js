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
import { isArray, isString, forIn, isPlainObject } from 'lodash-es';
import { CustomizerType } from './types.js';
import { navigateObject, tryParseNumber } from './utils.js';
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
export function transformObject(
// eslint-disable-next-line @typescript-eslint/no-explicit-any
obj, convertName) {
    // result object containing both structured and flattened fields
    const result = {};
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    function traverse(currentObj, parentPath = []) {
        if (isArray(currentObj)) {
            return currentObj.map(item => traverse(item, parentPath));
        }
        if (!isPlainObject(currentObj)) {
            if (currentObj && typeof currentObj === 'object' && currentObj.toJSON) {
                return traverse(currentObj.toJSON(), parentPath);
            }
            return currentObj;
        }
        const transformedObj = {};
        forIn(currentObj, (value, key) => {
            const keyNew = convertName ? convertName(key) : key;
            const currentPath = [...parentPath, keyNew];
            // Transform nested value before assigning
            const transformedValue = traverse(value, currentPath);
            // Add flattened field to result
            const flattenedKey = currentPath.join('.');
            result[flattenedKey] = transformedValue;
            transformedObj[keyNew] = transformedValue;
        });
        return transformedObj;
    }
    // Transform the root object and merge it into result
    const transformed = traverse(obj);
    Object.assign(result, transformed);
    return result;
}
export class AdsRowParser {
    constructor(logger) {
        this.logger = logger;
    }
    parseRow(row, query, objectMode = false) {
        // flatten the tree of object into a flat object with all properties
        const rowValues = transformObject(row, (name) => name.replace(CAMEL_TO_SNAKE_REGEX, (letter) => `_${letter.toLowerCase()}`));
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
                else {
                    this.logger.warn(`Evaluation of expression for column ${column.name} failed: ${e.message}, expression: ${column.expression}`);
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