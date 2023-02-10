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

import { enums } from 'google-ads-api';
import _ from 'lodash';
import { math_parse } from "./math-engine";

import {Column, Customizer, CustomizerType, FieldTypeKind, QueryElements} from './types';
import {navigateObject, traverseObject, tryParseNumber} from './utils';


export class AdsRowParser {
  parseRow(row: any, query: QueryElements) {
    // flatten the tree of object into a flat obejct with all properties
    let row_values: Record<string, any> = {};
    for (let field of Object.keys(row)) {
      let item = row[field];
      row_values[field] = item;
      traverseObject(
        item,
        (name, value, path, object) => {
          let field_full = path.join(".");
          row_values[field_full] = value;
        },
        [field]
      );
    }
    // process customizers and flatten row object into array
    let row_values_arr = [];
    for (let i = 0; i < query.columns.length; i++) {
      let column = query.columns[i];
      let value;
      if (column.customizer) {
        value = this.getValueWithCustomizer(
          row_values,
          column,
          column.customizer,
          query
        );
      } else {
        value = row_values[column.expression];
      }
      row_values_arr.push(value);
    }

    // post process enum (convert number to enum field names) and structs
    for (let i = 0; i < query.columns.length; i++) {
      let column = query.columns[i];
      let value: any = row_values_arr[i];
      //let colType = query.columnTypes[i];
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
            row_values_arr[i] = enumType[value];
          }
        }
      } else if (colType.kind === FieldTypeKind.struct) {
        if (value && value.toJSON) {
          row_values_arr[i] = value.toJSON();
        }
      }
    }

    return row_values_arr;
  }

  getValueWithCustomizer(
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
          value = value.match(/[^/]+\/(\d+)$/)[1];
        }
        value = tryParseNumber(value);
      }
    }
    return value;
  }
}
