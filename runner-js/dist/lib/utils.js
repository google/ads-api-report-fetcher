"use strict";
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.tryParseNumber = exports.navigateObject = exports.traverseObject = void 0;
const lodash_1 = __importDefault(require("lodash"));
function traverseObject(object, visitor, path) {
    path = path || [];
    return lodash_1.default.forIn(object, function (value, name) {
        path.push(name);
        if (lodash_1.default.isPlainObject(value)) {
            visitor(name, value, path, object);
            traverseObject(value, visitor, path);
        }
        else if (value === null || value === undefined || lodash_1.default.isString(value) ||
            lodash_1.default.isNumber(value) || lodash_1.default.isBoolean(value)) {
            visitor(name, value, path, object);
        }
        else if (lodash_1.default.isArray(value)) {
            // TODO: empty arrays, arrays of primities
            visitor(name, value, path, object);
            // for (const idx in value) {
            //   path.push(idx);
            //   traverseObject(value[idx], visitor, path);
            //   path.pop();
            // }
        }
        else if (value.toJSON) {
            value = value.toJSON();
            visitor(name, value, path, object);
            traverseObject(value, visitor, path);
        }
        path.pop();
    });
}
exports.traverseObject = traverseObject;
/**
 * Navigation a property chain on an object.
 * @param object an object
 * @param path a chain of property/field path (e.g. field1.field2)
 * @returns a value from the chain
 */
function navigateObject(object, path) {
    let ctx = object;
    for (let name of path.split('.')) {
        ctx = ctx[name];
        if (!ctx)
            return ctx;
    }
    return ctx;
}
exports.navigateObject = navigateObject;
/**
 * Parses numbers from strings
 * @param str a string containing a number
 * @returns a finite number (never returns NaN) or undefined
 */
function tryParseNumber(str) {
    if (lodash_1.default.isFinite(str))
        return str;
    if (lodash_1.default.isString(str) && str.length > 0) {
        let num = Number(str);
        return isNaN(num) ? undefined : num;
    }
}
exports.tryParseNumber = tryParseNumber;
//# sourceMappingURL=utils.js.map