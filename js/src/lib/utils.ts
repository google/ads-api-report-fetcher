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

import _ from 'lodash';

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
 *
 * @returns Return current date as YYYYMMDD
 */
export function getCurrentDateISO(): string {
  let now = new Date();
  let month = now.getMonth() + 1;
  let day = now.getDate();
  let iso = now.getFullYear() +
      (month < 10 ? '0' + month.toString() : month.toString()) +
      (day < 10 ? '0' + day : day);
  return iso;
}

export const MACRO_DATE_ISO = 'date_iso';

export function substituteMacros(
    queryText: string, macros?: Record<string, any>):
    {queryText: string, unknown_params: string[]} {
  macros = macros || {};
  let unknown_params: string[] = [];
  queryText = queryText.replace(/\{([^}]+)\}/g, (ss, name) => {
    if (name === MACRO_DATE_ISO && !macros![MACRO_DATE_ISO]) {
      return getCurrentDateISO();
    }
    if (!macros!.hasOwnProperty(name)) {
      unknown_params.push(name);
      return ss;
    }

    return macros![name];
  });

  return {queryText, unknown_params};
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
