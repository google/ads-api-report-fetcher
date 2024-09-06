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

import _ from 'lodash';
// eslint-disable-next-line n/no-extraneous-require
const ads_protos = require('google-ads-node/build/protos/protos.json');
import {getLogger} from './logger';
import {
  Customizer,
  CustomizerType,
  Column,
  FieldType,
  FieldTypeKind,
  isEnumType,
  ProtoTypeMeta,
  QueryElements,
  ResourceInfo,
  ApiType,
} from './types';
import {substituteMacros} from './utils';
import {math_parse} from './math-engine';
import {BuiltinQueryProcessor} from './builtins';

const protoRoot = ads_protos.nested.google.nested.ads.nested.googleads.nested;
const protoVer = Object.keys(protoRoot)[0]; // e.g. "v9"
export const AdsApiVersion = protoVer;
const protoRowType = protoRoot[protoVer].nested.services.nested.GoogleAdsRow;
const protoResources = protoRoot[protoVer].nested.resources.nested;
const protoEnums = protoRoot[protoVer].nested.enums.nested;
const protoCommonTypes = protoRoot[protoVer].nested.common.nested;

class InvalidQuerySyntax extends Error {}

export interface IAdsQueryEditor {
  parseQuery(query: string, macros?: Record<string, string>): QueryElements;
}

export class AdsQueryEditor implements IAdsQueryEditor {
  logger = getLogger();
  builtinQueryProcessor: BuiltinQueryProcessor;
  apiType: ApiType;
  apiVersion: string;

  constructor(apiType: ApiType, apiVersion: string) {
    this.apiType = apiType;
    this.apiVersion = apiVersion;
    this.builtinQueryProcessor = new BuiltinQueryProcessor(this);
  }

  /**
   * Remove comments and empty lines, normilize newlines,
   * i.e. remove insugnificat elements
   */
  private cleanupQueryText(query: string): string {
    const queryLines = [];
    for (let line of query.split('\n')) {
      // lines that start with '#' are treated as comments
      if (line.startsWith('#') || line.trim() === '') {
        continue;
      }
      // remove comments, we support '--' and '//' as comment line starters
      line = line.replace(/(--|\/\/)(.*)/g, '').trim();
      if (line.length > 0) queryLines.push(line);
    }
    query = queryLines.join('\n\r');
    // remove block comments /* */
    query = query.replaceAll(/\/\*([\s\S]*?)\*\//g, '');
    // remove non-single whitespaces
    query = '' + query.replace(/\s{2,}/g, ' ');
    // remove trailing semicolon
    query = query.trim().replace(/;$/gm, '');
    return query;
  }

  private parseFunctions(query: string): Record<string, Function> {
    const match = query.match(/FUNCTIONS (.*)/i);
    const functions: Record<string, Function> = {};
    if (match && match.length > 1) {
      const code = match[1];
      const iter = code.matchAll(
        /function\s+([^(]+)\s*\(\s*([^)]+)\s*\)\s*\{/gi
      );
      for (const funcBlock of iter) {
        const funcName = funcBlock[1];
        const argName = funcBlock[2];
        const idx = funcBlock[0].length;
        let brackets = 1;
        for (let i = idx; i < code.length; i++) {
          if (code[i] === '{') brackets++;
          else if (code[i] === '}') brackets--;
          if (brackets === 0) {
            // found the closing '}' of the function body, cut off the body w/o
            // enclosing {}
            const funcBody = code.slice(idx, i - 1);
            try {
              functions[funcName] = new Function(argName, funcBody);
            } catch (e) {
              this.logger.error(
                `InvalidQuerySyntax: failed to parse '${funcName}' function's body:\n ${e}`
              );
              throw e;
            }
            break;
          }
        }
      }
    }
    return functions;
  }

  parseQuery(query: string, macros?: Record<string, string>): QueryElements {
    query = this.cleanupQueryText(query);

    // parse and remove functions
    const functions = this.parseFunctions(query);
    query = this.removeFunctions(query);

    // substibute parameters and detect unspecified ones
    const res = substituteMacros(query, macros);
    if (res.unknown_params.length) {
      throw new Error(
        'The following parameters used in query and were not specified: ' +
          res.unknown_params
      );
    }
    query = res.text;
    const columnsPlaceholder = '$COLUMNS$';
    let queryNative = this.normalizeQuery(query, columnsPlaceholder);

    const raw_select_fields: string[] = [];
    // parse query metadata (resource type)
    const match = query.match(/ FROM ([^\s]+)/i);
    if (!match || !match.length)
      throw new Error('Could not parse resource from the query');
    let resourceName = match[1];
    let resourceTypeFrom: ProtoTypeMeta & {name: string};
    if (resourceName.startsWith('builtin.')) {
      // it's a builtin query, but it still can query an Ads resource
      resourceName = resourceName.substring('builtin.'.length);
      return this.builtinQueryProcessor.parse(resourceName, query);
    } else {
      resourceTypeFrom = this.getResource(resourceName);
    }
    const resourceInfo: ResourceInfo = {
      name: resourceName,
      typeName: resourceTypeFrom.name,
      typeMeta: resourceTypeFrom,
      isConstant: resourceName.endsWith('_constant'),
    };

    const selectFields = query
      .replace(/(^\s*SELECT)|(\s*FROM .*)/gi, '')
      .split(',')
      .filter(field => {
        return field.length > 0;
      });

    let field_index = 0;
    const fields: Column[] = [];
    const column_names: string[] = [];
    let expandWildcardAt = -1;
    for (const item of selectFields) {
      const pair = item.trim().toLowerCase().split(/ as /);
      const select_expr = pair[0];
      const alias = pair[1]; // can be undefined
      const parsedExpr = this.parseExpression(select_expr);
      if (!parsedExpr.field || !parsedExpr.field.trim()) {
        throw new InvalidQuerySyntax(
          `empty select field at index ${field_index}`
        );
      }

      // initialize column alias
      let column_name = alias || parsedExpr.field.replaceAll(/\./g, '_');
      if (!alias && column_name.startsWith(resourceName + '_')) {
        // cut off the current resource name from auto-generated column name
        column_name = column_name.substring(resourceName.length + 1);
      }
      column_name = column_name.replaceAll(/[ ]/g, '');
      // check for uniquniess
      if (column_names.includes(column_name)) {
        throw new InvalidQuerySyntax(
          `duplicating column name ${column_name} at index ${field_index}`
        );
      }
      column_names.push(column_name);

      // now decide on how the current column should be mapped to native query
      const select_expr_parsed = parsedExpr.field.trim();
      let fieldType: FieldType;
      if (select_expr_parsed === '*') {
        if (expandWildcardAt > -1) {
          throw new InvalidQuerySyntax(
            `duplicating wildcard '*' expression encountered at index ${field_index}`
          );
        }
        expandWildcardAt = field_index;
        continue;
      } else if (parsedExpr.customizer) {
        raw_select_fields.push(select_expr_parsed);
        const nameParts = select_expr_parsed.split('.');
        const curType = this.getResource(nameParts[0]);
        fieldType = this.getFieldType(curType, nameParts.slice(1));
        if (parsedExpr.customizer.type === CustomizerType.NestedField) {
          // we expect a field with nested_field customizer should ends with a
          // type (not primitive, not enum) i.e. ProtoTypeMeta
          if (_.isString(fieldType.type)) {
            throw new Error(
              `InvalidQuery: field ${column_name} contains nested field accessor (:) but selected field's type is primitive (${fieldType.typeName})`
            );
          }
          if (isEnumType(fieldType.type)) {
            throw new Error(
              `InvalidQuery: field ${column_name} contains nested field accessor (:) but selected field's type enum (${fieldType.typeName})`
            );
          }
          const repeated = fieldType.repeated;
          fieldType = this.getFieldType(
            fieldType.type,
            parsedExpr.customizer.selector.split('.')
          );
          fieldType.repeated = repeated || fieldType.repeated;
        } else if (
          parsedExpr.customizer.type === CustomizerType.ResourceIndex
        ) {
          fieldType.typeName = 'int64';
          fieldType.type = 'int64';
          fieldType.kind = FieldTypeKind.primitive;
        } else if (parsedExpr.customizer.type === CustomizerType.Function) {
          const func = functions[parsedExpr.customizer.function];
          if (!func) {
            throw new Error(
              `InvalidQuerySyntax: unknown function reference '${parsedExpr.customizer.function}' in experession '${select_expr}'`
            );
          }
          // expect that function's return type is always string
          // TODO: we could explicitly tell the type in query, e.g.
          // "field:$fun<int> AS field"
          fieldType.type = 'string';
          fieldType.typeName = 'string';
          fieldType.kind = FieldTypeKind.primitive;
          // TODO: we could support functions that return arrays or scalar
          // but how to tell it in a query ? e.g. field:$fun<int,string[]>
          // Currently all columns with functions are treated as scalar for output
          fieldType.repeated = false;
        }
      } else {
        // non-customizer column
        const field_regexp = /^[\w]+(\.[\w]+)+$/i;
        const field_match = field_regexp.exec(select_expr_parsed);
        if (field_match && field_match[0] === select_expr_parsed) {
          // looks like a field accessor
          raw_select_fields.push(select_expr_parsed);
        } else {
          // everything else should be an expression
          // it can be either a constant (number/string) or an expression of fields, or combinations,
          // we should parse all fields from the expression and add them into raw query for selecting
          const parsed_expression = math_parse(select_expr_parsed);
          let field: Column;
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          if ((parsed_expression as any).isConstantNode) {
            // constant expression
            const value = parsed_expression.evaluate();
            const value_type = _.isInteger(value)
              ? 'int64'
              : _.isNumber(value)
                ? 'double'
                : 'string';
            field = {
              name: column_name,
              customizer: {
                type: CustomizerType.VirtualColumn,
                evaluator: {evaluate: () => value},
              },
              expression: select_expr_parsed,
              type: {
                kind: FieldTypeKind.primitive,
                type: value_type,
                typeName: value_type,
              },
            };
          } else {
            const raw_expr_fields: string[] = [];
            parsed_expression.forEach((node /*, path, parent*/) => {
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              if ((node as any).isAccessorNode) {
                raw_expr_fields.push(node.toString());
              }
            });
            raw_select_fields.push(...raw_expr_fields);
            field = {
              name: column_name,
              customizer: {
                type: CustomizerType.VirtualColumn,
                evaluator: parsed_expression.compile(),
              },
              expression: select_expr_parsed,
              type: {
                kind: FieldTypeKind.primitive,
                // TODO: detect expression type
                type: 'string',
                typeName: 'string',
              },
            };
          }
          fields.push(field);
          continue;
        }
      }

      const field: Column = {
        name: column_name,
        customizer: parsedExpr.customizer,
        expression: select_expr_parsed,
        type: this.getColumnType(
          column_name,
          select_expr_parsed,
          parsedExpr.customizer
        ),
      };
      fields.push(field);

      field_index++;
    }
    if (expandWildcardAt > -1) {
      // expand wildcard expression '*' to fields that weren't specified easier
      // TODO: currently expanding only to scalar primitive fields (no nested types or enum)
      const new_fields = [];
      for (const fieldName of Object.keys(resourceInfo.typeMeta.fields)) {
        const field = resourceInfo.typeMeta.fields[fieldName];
        if (
          field.rule !== 'repeated' &&
          !column_names.includes(fieldName) &&
          (this.primitiveTypes.includes(field.type) ||
            field.type.match('google\\.ads.googleads.\\w+.enums'))
        ) {
          //"google.ads.googleads.v14.enums.CustomerStatusEnum.CustomerStatus"
          raw_select_fields.push(resourceInfo.name + '.' + fieldName);
          const column: Column = {
            name: fieldName,
            expression: resourceInfo.name + '.' + fieldName,
            type: this.getColumnType(
              fieldName,
              resourceInfo.name + '.' + fieldName
            ),
            // {
            //   type: field.type,
            //   typeName: field.type,
            //   kind: field.type.startsWith("google.ads.googleads.")
            //     ? FieldTypeKind.enum : FieldTypeKind.primitive,
            // },
          };
          new_fields.push(column);
        }
      }
      fields.splice(expandWildcardAt, 0, ...new_fields);
    }
    queryNative = queryNative.replace(
      '$COLUMNS$',
      raw_select_fields.join(', ')
    );
    return new QueryElements(queryNative, fields, resourceInfo, functions);
  }

  resourcesMap: Record<string, ProtoTypeMeta & {name: string}> = {};

  primitiveTypes = ['string', 'int64', 'int32', 'float', 'double', 'bool'];

  protected getColumnType(
    columnName: string,
    columnExpression: string,
    customizer?: Customizer
  ) {
    const nameParts = columnExpression.split('.');
    const curType = this.getResource(nameParts[0]);
    let fieldType = this.getFieldType(curType, nameParts.slice(1));
    if (customizer) {
      if (customizer.type === CustomizerType.NestedField) {
        // we expect a field with nested_field customizer should ends with a
        // type (not primitive, not enum) i.e. ProtoTypeMeta
        if (_.isString(fieldType.type)) {
          throw new Error(
            `InvalidQuery: field ${columnName} contains nested field accessor (:) but selected field's type is primitive (${fieldType.typeName})`
          );
        }
        if (isEnumType(fieldType.type)) {
          throw new Error(
            `InvalidQuery: field ${columnName} contains nested field accessor (:) but selected field's type enum (${fieldType.typeName})`
          );
        }
        const repeated = fieldType.repeated;
        fieldType = this.getFieldType(
          fieldType.type,
          customizer.selector.split('.')
        );
        fieldType.repeated = repeated || fieldType.repeated;
      } else if (customizer.type === CustomizerType.ResourceIndex) {
        fieldType.typeName = 'int64';
        fieldType.type = 'int64';
        fieldType.kind = FieldTypeKind.primitive;
      } else if (customizer.type === CustomizerType.Function) {
        // expect that function's return type is always string
        // TODO: we could explicitly tell the type in query, e.g.
        // "field:$fun<int> AS field"
        fieldType.type = 'string';
        fieldType.typeName = 'string';
        fieldType.kind = FieldTypeKind.primitive;
        // TODO: we could support functions that return arrays or scalar
        // but how to tell it in a query ? e.g. field:$fun<int,string[]>
        // Currently all columns with functions are treated as scalar
        fieldType.repeated = false;
      }
    }
    return fieldType;
  }

  protected getFieldType(type: ProtoTypeMeta, nameParts: string[]): FieldType {
    if (!nameParts || !nameParts.length)
      throw new Error('ArgumentException: namePart is empty');
    if (!type) throw new Error('ArgumentException: type was not specified');
    const rootType = type.name;
    for (let i = 0; i < nameParts.length; i++) {
      const isLastPart = i === nameParts.length - 1;
      let fieldType: FieldType;
      const field = type.fields[nameParts[i]];
      if (!field) {
        if (isLastPart) {
          // it's an unknown field (probably from a next version)
          fieldType = {
            repeated: false,
            type: 'string',
            typeName: 'string',
            kind: FieldTypeKind.primitive,
          };
          return fieldType;
        }
        throw new Error(
          `Resource or type '${type.name}' does not have field '${
            nameParts[i]
          }' (initial property chain is ${nameParts.join(
            '.'
          )} and resource is '${rootType}')`
        );
      }
      const repeated = field.rule === 'repeated';
      if (repeated && !isLastPart) {
        throw new Error(
          `InternalError: repeated field '${
            nameParts[i]
          }' in the middle of prop chain '${nameParts.join('.')}'`
        );
      }
      const fieldTypeName = field.type;
      // is it a primitive type?
      if (this.primitiveTypes.includes(fieldTypeName)) {
        fieldType = {
          repeated,
          type: fieldTypeName,
          typeName: fieldTypeName,
          kind: FieldTypeKind.primitive,
        };
        // field with primitive type can be only at the end of property chain
        if (!isLastPart) {
          throw new Error(
            `InternalError: field '${
              nameParts[i]
            }' in prop chain '${nameParts.join(
              '.'
            )}' has primitive type ${fieldTypeName}`
          );
        }
        return fieldType;
      }
      // is it a link to common type or enum
      else if (
        fieldTypeName.startsWith(`google.ads.googleads.${protoVer}.enums.`)
      ) {
        // google.ads.googleads.v9.enums
        // e.g. "google.ads.googleads.v9.enums.CriterionTypeEnum.CriterionType"
        const match = fieldTypeName.match(
          /google\.ads\.googleads\.v[\d]+\.enums\.([^.]+)\.([^.]+)/i
        );
        if (!match || match.length < 3) {
          throw new Error(`Could parse enum type reference ${fieldTypeName}`);
        }
        const enumType = protoEnums[match[1]].nested[match[2]];
        enumType['name'] = match[2];
        fieldType = {
          repeated,
          type: enumType,
          typeName: match[2],
          kind: FieldTypeKind.enum,
        };
        // field with primitive type can be only at the end of property chain
        if (!isLastPart) {
          throw new Error(
            `InternalError: field '${
              nameParts[i]
            }' in prop chain '${nameParts.join(
              '.'
            )}' has enum type ${fieldTypeName}`
          );
        }
        return fieldType;
      } else if (
        fieldTypeName.startsWith(`google.ads.googleads.${protoVer}.common.`)
      ) {
        // google.ads.googleads.v9.common
        const match = fieldTypeName.match(
          /google\.ads\.googleads\.v[\d]+\.common\.([^.]+)/i
        );
        if (!match || match.length < 2) {
          throw new Error(`Could parse common type reference ${fieldTypeName}`);
        }
        const commonType = protoCommonTypes[match[1]];
        commonType['name'] = match[1];

        fieldType = {
          repeated,
          type: commonType,
          typeName: match[1],
          kind: FieldTypeKind.struct,
        };
      } else {
        // then it's either another resource or a nested type
        if (type.nested && type.nested[fieldTypeName]) {
          fieldType = {
            repeated,
            type: type.nested[fieldTypeName],
            typeName: fieldTypeName,
            kind: FieldTypeKind.struct,
          };
        } else if (protoResources[fieldTypeName]) {
          fieldType = {
            repeated,
            type: protoResources[fieldTypeName],
            typeName: fieldTypeName,
            kind: FieldTypeKind.struct,
          };
        } else if (protoCommonTypes[fieldTypeName]) {
          // yes, some fields refer to common types by a full name but some by a
          // short one
          fieldType = {
            repeated,
            type: protoCommonTypes[fieldTypeName],
            typeName: fieldTypeName,
            kind: FieldTypeKind.struct,
          };
        } else {
          throw new Error(
            `InternalError: could not find a type proto for ${fieldTypeName} (field ${nameParts})`
          );
        }
      }
      type = <ProtoTypeMeta>fieldType.type;
      if (isLastPart) return fieldType;
    }
    throw new Error('InternalError');
  }

  getResource(fieldName: string): ProtoTypeMeta & {name: string} {
    let resourceType = this.resourcesMap[fieldName];
    if (resourceType) return resourceType;
    const resource = protoRowType.fields[fieldName];
    if (!resource)
      throw new Error(
        `Could not find resource '${fieldName}' in protobuf schema`
      );
    // resource.type will be a full name like
    // "google.ads.googleads.v9.resources.AdGroup" or
    // "google.ads.googleads.v9.common.Metrics"
    // we need to get the last part and find such a resource in protos
    const nameParts = resource.type.split('.');
    const resourceTypeName = nameParts[nameParts.length - 1];
    if (
      resource.type.startsWith(`google.ads.googleads.${protoVer}.resources.`)
    ) {
      resourceType = protoResources[resourceTypeName];
    } else if (
      resource.type.startsWith(`google.ads.googleads.${protoVer}.common.`)
    ) {
      resourceType = protoCommonTypes[resourceTypeName];
    }
    if (!resourceType) {
      throw new Error(`InternalError: could find resource ${resourceTypeName}`);
    }
    this.resourcesMap[fieldName] = resourceType;
    resourceType['name'] = resourceTypeName;
    return resourceType;
  }

  protected parseExpression(selectExpr: string): {
    field: string;
    customizer?: Customizer;
  } {
    // remove index (resource~N)
    const resources = selectExpr.split('~');
    if (resources.length > 1) {
      if (!_.isInteger(+resources[1])) {
        throw new Error(
          `Expression '${selectExpr}' contains indexed access ('~') but argument isn't a number`
        );
      }
      return {
        field: resources[0],
        customizer: {
          type: CustomizerType.ResourceIndex,
          index: +resources[1],
        },
      };
    }
    // nested resource accessor
    const nestedFields = selectExpr.split(':');
    if (nestedFields.length > 1) {
      const value = nestedFields[1];
      if (!value) {
        throw new Error(
          `Expression '${selectExpr}' contains nested path (':') but path is empty`
        );
      }
      if (value.startsWith('$')) {
        // the value is a function
        return {
          field: nestedFields[0],
          customizer: {
            type: CustomizerType.Function,
            function: value.slice(1),
          },
        };
      }
      return {
        field: nestedFields[0],
        customizer: {type: CustomizerType.NestedField, selector: value},
      };
    }
    // otherwise it's a column or an expression using columns
    return {field: selectExpr};
  }

  /** Remove all extensions from the query and return Ads API compatible query */
  protected normalizeQuery(query: string, columnsPlaceholder: string): string {
    // remove everything between SELECT and FROM, we'll add them back later
    query = query.replace(
      /SELECT\s.+\sFROM/gi,
      `SELECT ${columnsPlaceholder} FROM`
    );
    return query;
  }

  protected removeFunctions(query: string) {
    return query.replace(/FUNCTIONS .*/gi, '');
  }
}
