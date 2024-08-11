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

import { enums } from "google-ads-api";
import _ from "lodash";
import { math_parse } from "./math-engine";

import {
  ApiType,
  Column,
  Customizer,
  CustomizerType,
  FieldTypeKind,
  QueryElements,
} from "./types";
import { navigateObject, traverseObject, tryParseNumber } from "./utils";

export interface IAdsRowParser {
  parseRow(
    row: any,
    query: QueryElements,
    objectMode?: boolean
  ): any[] | Record<string, any>;
}

const CAMEL_TO_SNAKE_REGEX = /[A-Z]/g;

/**
 * How to parse results from Ads API in `IAdsRowParser.parseRowz`.
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

export class AdsRowParser implements IAdsRowParser {
  constructor(private apiType: ApiType) {}

  private normalizeName(name: string): string {
    if (this.apiType === ApiType.REST) {
      return name.replace(
        CAMEL_TO_SNAKE_REGEX,
        (letter: string) => `_${letter.toLowerCase()}`
      );
    }
    return name;
  }

  parseRow(row: any, query: QueryElements, objectMode = false) {
    // flatten the tree of object into a flat obejct with all properties
    let rowValues: Record<string, any> = {};
    for (let field of Object.keys(row)) {
      let item = row[field];
      rowValues[this.normalizeName(field)] = item;
      traverseObject(
        item,
        (name, value, path, object) => {
          let field_full = path.join(".");
          rowValues[this.normalizeName(field_full)] = value;
        },
        [field]
      );
    }
    // process customizers and flatten row object into array
    let rowValuesArr = [];
    for (let i = 0; i < query.columns.length; i++) {
      let column = query.columns[i];
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
        let column = query.columns[i];
        let value: any = objectMode ? rowValues[column.name] : rowValuesArr[i];
        let colType = column.type;
        if (
          colType.kind === FieldTypeKind.enum &&
          colType.repeated &&
          _.isArray(value)
        ) {
          for (let j = 0; j < value.length; j++) {
            let subval = value[j];
            if (_.isNumber(subval)) {
              let enumType = (<any>enums)[colType.typeName];
              if (enumType) {
                value[j] = enumType[subval];
              }
            }
          }
        } else if (colType.kind === FieldTypeKind.enum) {
          if (_.isNumber(value)) {
            let enumType = (<any>enums)[colType.typeName];
            if (enumType) {
              if (objectMode) {
                rowValues[column.name] = enumType[value];
              } else {
                rowValuesArr[i] = enumType[value];
              }
            }
          }
        } else if (colType.kind === FieldTypeKind.struct) {
          if (value && value.toJSON) {
            if (objectMode) {
              rowValues[column.name] = value.toJSON();
            } else {
              rowValuesArr[i] = value.toJSON();
            }
          }
        }
      }
    }
    if (objectMode) {
      return rowValues;
    } else {
      return rowValuesArr;
    }
  }

  protected getValueWithCustomizer(
    row: any,
    column: Column,
    customizer: Customizer,
    query: QueryElements
  ): any {
    let value;
    if (customizer.type === CustomizerType.VirtualColumn) {
      try {
        value = customizer.evaluator.evaluate(row);
      } catch (e) {
        if (e.message.includes("TypeError: Cannot read properties of null")) {
          value = null;
        }
      }
    } else {
      value = row[column.expression];
      if (!value) return value;
      if (customizer.type === CustomizerType.Function) {
        let func = query.functions[customizer.function];
        value = func(value);
      } else {
        // for other customers we support arrays specifically
        if (_.isArray(value)) {
          let new_value = [];
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

  protected parseScalarValue(value: any, customizer: Customizer) {
    if (customizer.type === CustomizerType.NestedField) {
      value = navigateObject(value, customizer.selector);
    } else if (customizer.type === CustomizerType.ResourceIndex) {
      value = value.split("~")[customizer.index];
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
