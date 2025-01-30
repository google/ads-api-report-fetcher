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
import fs from 'node:fs';
import {add as date_add, format, Duration} from 'date-fns';
import {
  forIn,
  isPlainObject,
  isString,
  isBoolean,
  isNumber,
  isFinite,
  isArray,
} from 'lodash-es';
import nunjucks from 'nunjucks';

import {math_parse} from './math-engine.js';

export function traverseObject(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  object: any,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  visitor: (name: string, value: any, path: string[], object: Object) => void,
  path: string[]
): boolean {
  path = path || [];
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return forIn(object, (value: any, name: string) => {
    path.push(name);
    if (isPlainObject(value)) {
      visitor(name, value, path, object);
      traverseObject(value, visitor, path);
    } else if (
      value === null ||
      value === undefined ||
      isString(value) ||
      isNumber(value) ||
      isBoolean(value)
    ) {
      visitor(name, value, path, object);
    } else if (isArray(value)) {
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
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function navigateObject(object: any, path: string) {
  let ctx = object;
  for (const name of path.split('.')) {
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
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function tryParseNumber(str: any): number | undefined {
  if (isFinite(str)) return str as number;
  if (isString(str) && str.length > 0) {
    const num = Number(str);
    return isNaN(num) ? undefined : num;
  }
}

/**
 * Format a date in ISO format - YYYYMMDD or YYYY-MM-DD if delimiter is "-"
 * @deprecated use `format` from `date-fns` package instead
 */
export function formatDateISO(dt: Date, delimiter = ''): string {
  const month = dt.getMonth() + 1;
  const day = dt.getDate();
  const iso =
    dt.getFullYear() +
    delimiter +
    (month < 10 ? '0' + month.toString() : month.toString()) +
    delimiter +
    (day < 10 ? '0' + day : day);
  return iso;
}

function convert_date(name: string, value: string): string {
  // eslint-disable-next-line prefer-const
  let [pattern, delta, ...other] = value.split('-');
  if (!pattern || other.length) {
    throw new Error(
      `Macro ${name} has incorrect format, expected :YYYYMMDD-1, or :YYYYMM-1, or :YYYY-1 `
    );
  }
  if (!delta) {
    // simple case ":YYYYMMDD"
    return format(new Date(), 'yyyy-MM-dd');
  }
  const ago = +delta;
  pattern = pattern.trim().toUpperCase();
  let duration: Duration;
  if (pattern === ':YYYYMMDD') {
    duration = {days: -ago};
  } else if (pattern === ':YYYYMM') {
    duration = {months: -ago};
  } else if (pattern === ':YYYY') {
    duration = {years: -ago};
  } else {
    throw new Error(
      `Macro ${name} has incorrect format, expected :YYYYMMDD-1, or :YYYYMM-1, or :YYYY-1 `
    );
  }
  const dt = date_add(new Date(), duration);
  return format(dt, 'yyyy-MM-dd');
}

/**
 * Substitute macros into the text, and evaluates expressions (in ${} blocks).
 * @param text a text (query) to process
 * @param macros an object with key-values to substitute
 * @returns same text with substituted macros and executed expressions
 */
export function substituteMacros(
  text: string,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  macros?: Record<string, string | number>
): {text: string; unknown_params: string[]} {
  const unknown_params: Record<string, boolean> = {};
  // Support for macro's values containing special syntax for dynamic dates:
  // ':YYYYMMDD-N', ':YYYYMM-N', ':YYYY-N', where N is a number of days/months/year respectedly
  if (macros) {
    Object.entries(macros).map(pair => {
      const value = <string>pair[1];
      if (value && isString(value) && value.startsWith(':YYYY')) {
        const key = pair[0];
        macros![key] = convert_date(key, value);
      }
    });
  }

  macros = macros || {};
  // add "magic" macros for Python version compatibility
  if (!macros['date_iso']) {
    macros['date_iso'] = format(new Date(), 'yyyyMMdd');
  }
  if (!macros['yesterday_iso']) {
    const date = new Date();
    date.setDate(date.getDate() - 1);
    macros['yesterday_iso'] = format(date, 'yyyyMMdd');
  }
  if (!macros['current_date']) {
    macros['current_date'] = format(new Date(), 'yyyy-MM-dd');
  }
  if (!macros['current_datetime']) {
    macros['current_datetime'] = format(new Date(), 'yyyy-MM-dd HH:mm:ss');
  }

  // notes on the regexp:
  //  "(?<!\$)" - is a lookbehind expression (catch the following exp if it's
  //  not prepended with '$'), with that we're capturing {smth} expressions
  //  and not ${smth} expressions
  text = text.replace(/(?<!\$)\{([^}]+)\}/g, (ss, name) => {
    if (!Object.prototype.hasOwnProperty.call(macros!, name)) {
      unknown_params[name] = true;
      return ss;
    }
    // NOTE: we don't care here about type, it'll degraded to string anyway
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return macros![name] as any;
  });
  // now process expressions with built-in functions in ${..} blocks
  text = text.replace(/\$\{([^}]*)\}/g, (ss, expr) => {
    if (!expr.trim()) return '';
    return math_parse(expr).evaluate(macros);
  });
  return {text: text, unknown_params: Object.keys(unknown_params)};
}

export function renderTemplate(
  template: string,
  params: Record<string, unknown>
) {
  //nunjucks.configure("views", { autoescape: true });
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value && typeof value === 'string') {
        params[key] = value.split(',');
      }
    }
  }
  return nunjucks.renderString(template, params);
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
  // calculating duration and formatting. Unfortunately it doesn't seem to
  // support formatting of duration in a way we need (hh:mm:ss)
  //let from = LocalDateTime.from(nativeJs(started));
  //let to = LocalDateTime.from(nativeJs(now || new Date()));
  //Duration.between(from, to).toString() - return 'PT..' string

  let ms = (now ? now.valueOf() : Date.now()) - started.valueOf();
  let seconds = ms / 1000;
  ms = Math.floor(ms % 1000);
  let minutes = seconds / 60;
  seconds = Math.floor(seconds % 60);
  let hours = minutes / 60;
  minutes = Math.floor(minutes % 60);
  hours = Math.floor(hours % 24);

  return (
    prepend(hours) +
    ':' +
    prepend(minutes) +
    ':' +
    prepend(seconds) +
    '.' +
    prepend(ms, 3)
  );
}

/**
 * Return a directory size.
 * @param path a local path
 * @returns size in megabytes
 */
export function getDirectorySize(path: string): number | undefined {
  let totalSize = 0;
  if (fs.existsSync(path)) {
    const files = fs.readdirSync(path);
    for (const file of files) {
      const stats = fs.statSync(`${path}/${file}`);
      totalSize += stats.size;
    }
    return Math.round(totalSize / 1024 / 1024); // Convert to MB
  }
  return undefined;
}

/**
 * Construct a string with memory usage dump.
 * @param phase arbitrary string to describe a moment
 * @returns formatted info
 */
export function getMemoryUsage(phase: string): string {
  const used = process.memoryUsage();
  // NOTE: Additionally v8.getHeapStatistics() can be used
  let memUsage = '';
  for (const key in used) {
    memUsage += `${key} ${
      Math.round((used[<keyof typeof used>key] / 1024 / 1024) * 100) / 100
    } MB\n`;
  }
  let extra = '';
  if (process.env.K_SERVICE) {
    const tmpSize = getDirectorySize('/tmp');
    if (Number.isInteger(tmpSize)) {
      extra = `/tmp Directory Size: ${tmpSize} MB`;
    }
  }
  return `${phase} - Memory Usage: ${memUsage}\n${extra}`;
}

export interface RetryOptions {
  baseDelayMs?: number;
  delayStrategy?:
    | null
    | undefined
    | false
    | 'constant'
    | 'linear'
    | 'exponential';
}

/**
 *
 * @param fn Any operation to execute
 * @param checkToRetry A callback to determine if the operation should be retried
 * @param baseDelayMs Initial delay in milliseconds to wait before retrying the operation
 * @returns A result of the operation
 */
export function executeWithRetry<T>(
  fn: () => T,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  checkToRetry: (error: any, attempt: number) => boolean,
  options?: RetryOptions
): Promise<T> {
  let attempt = 1;

  const execute: () => Promise<T> = async () => {
    try {
      return await fn();
    } catch (error) {
      if (!checkToRetry(error, attempt)) {
        throw error;
      }
      // retrying
      options = options || {};
      if (options.delayStrategy) {
        let delayMs = 0;
        const baseDelayMs = options.baseDelayMs || 1000;
        switch (options.delayStrategy) {
          case 'constant':
            delayMs = baseDelayMs;
            break;
          case 'linear':
            delayMs = baseDelayMs * attempt;
            break;
          case 'exponential':
            delayMs = baseDelayMs * 2 ** attempt;
            break;
          default:
            throw new Error('Unknown delayStrategy ');
        }
        console.log(`Retry attempt ${attempt} after ${delayMs}ms`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }

      attempt++;
      return execute();
    }
  };

  return execute();
}

/**
 * Return a waitable Promise for a delay.
 * @param ms number of milliseconds to wait
 */
export function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
