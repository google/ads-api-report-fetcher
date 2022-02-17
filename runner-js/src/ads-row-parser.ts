import {enums, fields} from 'google-ads-api';
import _ from 'lodash';

import {FieldTypeKind} from '.';
import {Customizer, CustomizerType, QueryElements} from './types'
import {navigateObject, traverseObject} from './utils';


export class AdsRowParser {
  parseRow(row: any, query: QueryElements) {
    // flatten the tree of object into a flat array of values
    let row_values: Record<string, any> = {};
    for (let field of Object.keys(row)) {
      let item = row[field];
      traverseObject(item, (name, value, path, object) => {
        let field_full = path.join('.');
        row_values[field_full] = value;
      }, [field]);
    }
    console.log('Flattened row:');
    console.log(row_values);

    // process customizers
    let row_values_arr = [];
    for (let i = 0; i < query.fields.length; i++) {
      let field = query.fields[i];
      let customizer = query.customizers[i];
      let value = row_values[field];
      if (customizer && customizer.type === CustomizerType.Function) {
        value = this.getValueWithCustomizer(value, customizer, query);
      }
      else if (value && customizer) {
        if (_.isArray(value)) {
          let new_value = [];
          for (let j = 0; j < value.length; j++) {
            new_value[j] =
                this.getValueWithCustomizer(value[j], customizer, query);
          }
          value = new_value;
        } else {
          value = this.getValueWithCustomizer(value, customizer, query);
        }
      }
      row_values_arr.push(value);
    }

    // parse numbers as enum's field names
    for (let i = 0; i < query.fields.length; i++) {
      let field = query.fields[i];
      let value: any = row_values_arr[i];
      let colType = query.columnTypes[i];
      if (colType.kind === FieldTypeKind.enum && colType.repeated &&
          _.isArray(value)) {
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

    // for (let i = 0; i < query.fields.length; i++) {
    //   let field = query.fields[i];
    //   let value: any = row_values_arr[i];
    //   if (_.isNumber(value)) {
    //     let enumTypeName = (<any>fields.enumFields)[field];
    //     if (enumTypeName) {
    //       let enumType = (<any>enums)[enumTypeName];
    //       if (enumType) {
    //         row_values_arr[i] = enumType[value];
    //       }
    //     }
    //   }
    // }
    return row_values_arr;
  }

  getValueWithCustomizer(
      value: any, customizer: Customizer, query: QueryElements): any {
    if (!value) return value;
    if (customizer.type === CustomizerType.NestedField) {
      value = navigateObject(value, customizer.value);
    } else if (customizer.type === CustomizerType.ResourceIndex) {
      value = value.split('~')[customizer.value];
    } else if (customizer.type === CustomizerType.Function) {
      let func = query.functions[customizer.value];
      // TODO: move the check to ads-query-editor
      if (!func)
        throw new Error(
            `InvalidQuerySyntax: unknown function ${customizer.value}`);
      value = func(value);
    }
    // else if (customizer.type === CustomizerType.Pointer) {
    //   value = navigateObject(value, customizer.value);
    // }
    return value;
  }
}
