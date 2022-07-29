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
exports.getElapsed = exports.substituteMacros = exports.formatDateISO = exports.tryParseNumber = exports.navigateObject = exports.traverseObject = void 0;
const lodash_1 = __importDefault(require("lodash"));
const math_engine_1 = require("./math-engine");
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
/**
 * Format a date in ISO format - YYYYMMDD or YYYY-MM-DD if delimiter is "-"
 */
function formatDateISO(dt, delimiter = '') {
    let month = dt.getMonth() + 1;
    let day = dt.getDate();
    let iso = dt.getFullYear() + delimiter +
        (month < 10 ? '0' + month.toString() : month.toString()) + delimiter +
        (day < 10 ? '0' + day : day);
    return iso;
}
exports.formatDateISO = formatDateISO;
function processMacroValue(value) {
    return value;
}
/**
 * Substitute macros into the text, and evalutes expressions (in ${} blocks).
 * @param queryText a text (query) to process
 * @param macros an object with key-values to substitute
 * @returns same text with substituted macros and executed expressions
 */
function substituteMacros(queryText, macros) {
    let unknown_params = {};
    if (macros) {
        Object.entries(macros).map(pair => {
            let value = pair[1];
            if (value && lodash_1.default.isString(value) && value.startsWith(':YYYY')) {
                macros[pair[0]] = processMacroValue(value);
            }
        });
    }
    macros = macros || {};
    // notes on the regexp:
    //  "(?<!\$)" - is a lookbehind expression (catch the following exp if it's
    //  not precended with '$'), with that it we're capturing {smth} expressions
    //  and not ${smth} expressions
    queryText = queryText.replace(/(?<!\$)\{([^}]+)\}/g, (ss, name) => {
        if (!macros.hasOwnProperty(name)) {
            unknown_params[name] = true;
            return ss;
        }
        return macros[name];
    });
    // now process expressions with built-in functions in ${..} blocks
    queryText = queryText.replace(/\$\{([^}]*)\}/g, (ss, expr) => {
        if (!expr.trim())
            return '';
        return (0, math_engine_1.math_parse)(expr).evaluate(macros);
    });
    return { queryText, unknown_params: Object.keys(unknown_params) };
}
exports.substituteMacros = substituteMacros;
function prepend(value, num) {
    let value_str = value.toString();
    num = num || 2;
    if (value_str.length < num) {
        while (value_str.length < num) {
            value_str = '0' + value_str;
        }
    }
    return value_str;
}
function getElapsed(started, now) {
    // NOTE: as we've already imported @js-joda it seems logic to use it for
    // calculating duration and formating. Unfortunetely it doesn't seem to
    // support formating of duration in a way we need (hh:mm:ss)
    //let from = LocalDateTime.from(nativeJs(started));
    //let to = LocalDateTime.from(nativeJs(now || new Date()));
    //Duration.between(from, to).toString() - return 'PT..' string
    let ms = ((now ? now.valueOf() : Date.now()) - started.valueOf());
    let seconds = ms / 1000;
    ms = Math.floor(ms % 1000);
    let minutes = seconds / 60;
    seconds = Math.floor(seconds % 60);
    let hours = minutes / 60;
    minutes = Math.floor(minutes % 60);
    hours = Math.floor(hours % 24);
    return prepend(hours) + ':' + prepend(minutes) + ':' + prepend(seconds) +
        '.' + prepend(ms, 3);
}
exports.getElapsed = getElapsed;
//# sourceMappingURL=utils.js.map