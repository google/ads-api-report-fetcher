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

import date_add from 'date-fns/add'
import _ from 'lodash';

import {math_parse} from './math-engine';

export function traverseObject(
    object: any,
    visitor: (name: string, value: any, path: string[], object: Object) => void,
    path: string[]): boolean {
  path = path || [];
  return _.forIn(object, function(value: any, name: string) {
    path.push(name);
    if (_.isPlainObject(value)) {
      visitor(name, value, path, object);
      traverseObject(value, visitor, path);
    } else if (
        value === null || value === undefined || _.isString(value) ||
        _.isNumber(value) || _.isBoolean(value)) {
      visitor(name, value, path, object);
    } else if (_.isArray(value)) {
      // TODO: empty arrays, arrays of primities
      visitor(name, value, path, object);
      // for (const idx in value) {
      //   path.push(idx);
      //   traverseObject(value[idx], visitor, path);
      //   path.pop();
      // }
    } else if (value.toJSON) {
      value = value.toJSON();
      visitor(name, value, path, object);
      traverseObject(value, visitor, path);
    }
    path.pop();
  });
}

/**
 * Navigation a property chain on an object.
 * @param object an object
 * @param path a chain of property/field path (e.g. field1.field2)
 * @returns a value from the chain
 */
export function navigateObject(object: any, path: string) {
  let ctx = object;
  for (let name of path.split('.')) {
    ctx = ctx[name];
    if (!ctx) return ctx;
  }
  return ctx;
}

/**
 * Parses numbers from strings
 * @param str a string containing a number
 * @returns a finite number (never returns NaN) or undefined
 */
export function tryParseNumber(str: any): number|undefined {
  if (_.isFinite(str)) return <number>str;
  if (_.isString(str) && str.length > 0) {
    let num = Number(str);
    return isNaN(num) ? undefined : num;
  }
}

/**
 * Format a date in ISO format - YYYYMMDD or YYYY-MM-DD if delimiter is "-"
 */
export function formatDateISO(dt: Date, delimiter: string = ''): string {
  let month = dt.getMonth() + 1;
  let day = dt.getDate();
  let iso = dt.getFullYear() + delimiter +
      (month < 10 ? '0' + month.toString() : month.toString()) + delimiter +
      (day < 10 ? '0' + day : day);
  return iso;
}

function convert_date(name: string, value: string): string {
  let [pattern, delta, ...other] = value.split('-');
  if (!pattern || other.length) {
    throw new Error(`Macro ${name} has incorrect format, expected :YYYYMMDD-1, or :YYYYMM-1, or :YYYY-1 `);
  }
  if (!delta) {
    // simple case ":YYYYMMDD"
    return formatDateISO(new Date(), '-');
  }
  let ago = +delta;
  pattern = pattern.trim().toUpperCase();
  let duration: Duration;
  if (pattern === ':YYYYMMDD') {
    duration = { days: -ago };
  }
  else if (pattern === ':YYYYMM') {
    duration = {months: -ago};
  }
  else if (pattern === ':YYYY') {
    duration = {years: -ago};
  }
  else {
    throw new Error(`Macro ${
        name} has incorrect format, expected :YYYYMMDD-1, or :YYYYMM-1, or :YYYY-1 `);
  }
  let dt = date_add(new Date(), duration);
  return formatDateISO(dt, '-');
}

/**
 * Substitute macros into the text, and evalutes expressions (in ${} blocks).
 * @param queryText a text (query) to process
 * @param macros an object with key-values to substitute
 * @returns same text with substituted macros and executed expressions
 */
export function substituteMacros(
    queryText: string, macros?: Record<string, any>):
    {queryText: string, unknown_params: string[]} {
  let unknown_params: Record<string, boolean> = {};
  // Support for macro's values containing special syntax for dynamic dates:
  // ':YYYYMMDD-N', ':YYYYMM-N', ':YYYY-N', where N is a number of days/months/yaer respectedly
  if (macros) {
    Object.entries(macros).map(pair => {
      let value = <string>pair[1];
      if (value && _.isString(value) && value.startsWith(':YYYY')) {
        let key = pair[0];
        macros![key] = convert_date(key, value);
      }
    });
  }

  macros = macros || {};
  // notes on the regexp:
  //  "(?<!\$)" - is a lookbehind expression (catch the following exp if it's
  //  not precended with '$'), with that we're capturing {smth} expressions
  //  and not ${smth} expressions
  queryText = queryText.replace(/(?<!\$)\{([^}]+)\}/g, (ss, name) => {
    if (!macros!.hasOwnProperty(name)) {
      unknown_params[name] = true;
      return ss;
    }

    return macros![name];
  });
  // now process expressions with built-in functions in ${..} blocks
  queryText = queryText.replace(/\$\{([^}]*)\}/g, (ss, expr) => {
    if (!expr.trim()) return '';
    return math_parse(expr).evaluate(macros);
  });
  return {queryText, unknown_params: Object.keys(unknown_params)};
}

function prepend(value: number, num?: number): string {
  let value_str = value.toString();
  num = num || 2;
  if (value_str.length < num) {
    while (value_str.length < num) {
      value_str = '0' + value_str;
    }
  }
  return value_str;
}
export function getElapsed(started: Date, now?: Date): string {
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
