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

import {enums} from 'google-ads-api';
import {isNumber, isArray, isString, forIn, isPlainObject} from 'lodash-es';

import {
  ApiType,
  Column,
  Customizer,
  CustomizerType,
  FieldTypeKind,
  QueryElements,
} from './types.js';
import {navigateObject, tryParseNumber} from './utils.js';
import {ILogger} from './logger.js';

export interface IAdsRowParser {
  /**
   * Parse a row returned by a GoogleAds API client.
   * @param row A single row returned by IGoogleAdsApiClient.executeQuery
   * @param query A query
   * @param objectMode True to use object mode, i.e. return result as an object
   *        instead of an array.
   */
  parseRow(
    row: Record<string, unknown>,
    query: QueryElements,
    objectMode?: boolean
  ): unknown[] | Record<string, unknown>;
}

const CAMEL_TO_SNAKE_REGEX = /[A-Z]/g;

/**
 * How to parse results from Ads API in `IAdsRowParser.parseRow`.
 */
export enum ParseResultMode {
  /**
   * Return results as an array.
   */
  Array = 1,
  /**
   * Return results as an object.
   */
  Object = 2,
}

export function transformObject(
  obj: any,
  convertName?: (name: string) => string
): Record<string, unknown> {
  // result object containing both structured and flattened fields
  const result: Record<string, unknown> = {};

  function traverse(currentObj: any, parentPath: string[] = []): any {
    if (isArray(currentObj)) {
      return currentObj.map(item => traverse(item, parentPath));
    }

    if (!isPlainObject(currentObj)) {
      if (currentObj && typeof currentObj === 'object' && currentObj.toJSON) {
        return traverse(currentObj.toJSON(), parentPath);
      }
      return currentObj;
    }

    const transformedObj: Record<string, unknown> = {};

    forIn(currentObj, (value, key) => {
      const keyNew = convertName ? convertName(key) : key;
      const currentPath = [...parentPath, keyNew];

      // Add flattened field to result
      const flattenedKey = currentPath.join('.');
      result[flattenedKey] = value;

      // Transform nested value
      transformedObj[keyNew] = traverse(value, currentPath);
    });

    return transformedObj;
  }

  // Transform the root object and merge it into result
  const transformed = traverse(obj);
  Object.assign(result, transformed);

  return result;
}

export class AdsRowParser implements IAdsRowParser {
  constructor(
    private apiType: ApiType,
    private logger: ILogger
  ) {}

  parseRow(
    row: Record<string, unknown>,
    query: QueryElements,
    objectMode = false
  ) {
    // flatten the tree of object into a flat object with all properties
    const rowValues: Record<string, unknown> = transformObject(
      row,
      this.apiType === ApiType.REST
        ? (name: string) =>
            name.replace(
              CAMEL_TO_SNAKE_REGEX,
              (letter: string) => `_${letter.toLowerCase()}`
            )
        : undefined
    );

    // process customizers and flatten row object into array
    const rowValuesArr = [];
    for (let i = 0; i < query.columns.length; i++) {
      const column = query.columns[i];
      let value;
      if (column.customizer) {
        value = this.getValueWithCustomizer(
          rowValues,
          column,
          column.customizer,
          query
        );
      } else {
        value = rowValues[column.expression];
      }

      if (objectMode) {
        rowValues[column.name] = value;
      } else {
        rowValuesArr.push(value);
      }
    }

    // post process enum (convert number to enum field names) and structs
    if (this.apiType === ApiType.gRPC) {
      // NOTE: gRPC API returns enums as numbers, while REST API returns strings
      for (let i = 0; i < query.columns.length; i++) {
        const column = query.columns[i];
        const value: unknown = objectMode
          ? rowValues[column.name]
          : rowValuesArr[i];
        const colType = column.type;
        if (
          colType.kind === FieldTypeKind.enum &&
          colType.repeated &&
          isArray(value)
        ) {
          for (let j = 0; j < value.length; j++) {
            const subval = value[j];
            if (isNumber(subval)) {
              const enumType = (
                enums as Record<string, Record<number, string>>
              )[colType.typeName];
              if (enumType) {
                value[j] = enumType[subval];
              }
            }
          }
        } else if (colType.kind === FieldTypeKind.enum) {
          if (isNumber(value)) {
            const enumType = (enums as Record<string, Record<number, string>>)[
              colType.typeName
            ];
            if (enumType) {
              if (objectMode) {
                rowValues[column.name] = enumType[value];
              } else {
                rowValuesArr[i] = enumType[value];
              }
            }
          }
        } else if (colType.kind === FieldTypeKind.struct) {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          if (value && (value as any).toJSON) {
            if (objectMode) {
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              rowValues[column.name] = (value as any).toJSON();
            } else {
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              rowValuesArr[i] = (value as any).toJSON();
            }
          }
        }
      }
    }
    return objectMode ? rowValues : rowValuesArr;
  }

  protected getValueWithCustomizer(
    row: Record<string, unknown>,
    column: Column,
    customizer: Customizer,
    query: QueryElements
  ): unknown {
    let value;
    if (customizer.type === CustomizerType.VirtualColumn) {
      try {
        value = customizer.evaluator.evaluate(row);
      } catch (e) {
        if (e.message.includes('TypeError: Cannot read properties of null')) {
          value = null;
        } else {
          this.logger.warn(
            `Evaluation of expression for column ${column.name} failed: ${e.message}, expression: ${column.expression}`
          );
        }
      }
    } else {
      value = row[column.expression];
      if (!value) return value;
      if (customizer.type === CustomizerType.Function) {
        const func = query.functions[customizer.function];
        value = func(value);
      } else {
        // for other customizers we support arrays specifically
        if (isArray(value)) {
          const new_value = [];
          for (let j = 0; j < value.length; j++) {
            new_value[j] = this.parseScalarValue(value[j], customizer);
          }
          value = new_value;
        } else {
          value = this.parseScalarValue(value, customizer);
        }
      }
    }
    return value;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected parseScalarValue(value: any, customizer: Customizer) {
    if (customizer.type === CustomizerType.NestedField) {
      value = navigateObject(value, customizer.selector);
    } else if (customizer.type === CustomizerType.ResourceIndex) {
      // the value from query's result we expect to be a string
      if (!isString(value)) {
        // we fetched a struct, let's try to find a suitable field with resource
        let resourceVal = '';
        if (value['name'] && isString(value['name'])) {
          resourceVal = value['name'];
        } else if (value['text'] && isString(value['text'])) {
          resourceVal = value['text'];
        } else if (value['asset'] && isString(value['asset'])) {
          resourceVal = value['asset'];
        } else if (value['value'] && isString(value['value'])) {
          resourceVal = value['value'];
        }
        if (resourceVal) {
          value = resourceVal;
        } else {
          throw new Error(
            `Unexpected value for ResourceIndex source: ${JSON.stringify(value)}.` +
              'We expect either a string or a struct with fields name/text/asset/value'
          );
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
