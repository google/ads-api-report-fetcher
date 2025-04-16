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
import { isInteger, isNumber, isString } from 'lodash-es';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
// eslint-disable-next-line n/no-extraneous-require
const ads_protos = require('google-ads-node/build/protos/protos.json');
import { getLogger } from './logger.js';
import { CustomizerType, FieldTypeKind, isEnumType, QueryElements, } from './types.js';
import { assertIsError, renderTemplate, substituteMacros } from './utils.js';
import { extractFieldAccesses, mathjs } from './math-engine.js';
import { BuiltinQueryProcessor } from './builtins.js';
import { parse } from './parser.js';
const protoRoot = ads_protos.nested.google.nested.ads.nested.googleads.nested;
const protoVer = Object.keys(protoRoot)[0]; // e.g. "v9"
export const AdsApiVersion = protoVer;
const protoRowType = protoRoot[protoVer].nested.services.nested.GoogleAdsRow;
const protoResources = protoRoot[protoVer].nested.resources.nested;
const protoEnums = protoRoot[protoVer].nested.enums.nested;
const protoCommonTypes = protoRoot[protoVer].nested.common.nested;
class InvalidQuerySyntax extends Error {
}
export class AdsQueryEditor {
    constructor(apiType, apiVersion) {
        this.logger = getLogger();
        this.resourcesMap = {};
        this.primitiveTypes = ['string', 'int64', 'int32', 'float', 'double', 'bool'];
        this.apiType = apiType;
        this.apiVersion = apiVersion;
        this.builtinQueryProcessor = new BuiltinQueryProcessor(this);
    }
    parseFunctions(query) {
        const functions = {};
        const code = query;
        const iter = query.matchAll(/function\s+([^(]+)\s*\(\s*([^)]+)\s*\)\s*\{/gi);
        for (const funcBlock of iter) {
            const funcName = funcBlock[1];
            const argName = funcBlock[2];
            const idx = funcBlock[0].length;
            let brackets = 1;
            for (let i = idx; i < code.length; i++) {
                if (code[i] === '{')
                    brackets++;
                else if (code[i] === '}')
                    brackets--;
                if (brackets === 0) {
                    // found the closing '}' of the function body, cut off the body w/o
                    // enclosing {}
                    const funcBody = code.slice(idx, i - 1);
                    try {
                        functions[funcName] = new Function(argName, funcBody);
                    }
                    catch (e) {
                        this.logger.error(`InvalidQuerySyntax: failed to parse '${funcName}' function's body:\n ${e}`);
                        throw e;
                    }
                    break;
                }
            }
        }
        //}
        return functions;
    }
    compileAst(query, selectFields) {
        let text = 'SELECT ';
        if (selectFields) {
            text += selectFields.join(', ');
        }
        else {
            for (const f of query.select.fields) {
                text += f.expression.selector;
                if (f.alias) {
                    text += ' AS ' + f.alias;
                }
                text += ',';
            }
            text = text.substring(0, text.length - 1);
        }
        text += ' FROM ' + query.from.resource;
        if (query.where) {
            text += ' WHERE ' + query.where.clause;
        }
        if (query.orderBy) {
            text += ' ORDER BY ' + query.orderBy.clause;
        }
        if (query.limit) {
            text += ' LIMIT ' + query.limit.value;
        }
        if (query.parameters) {
            text += ' PARAMETERS ' + query.parameters.clause;
        }
        return text;
    }
    parseQuery(query, macros, templateParams) {
        var _a;
        let ast = parse(query);
        let queryNormalized = this.compileAst(ast);
        const functions = this.parseFunctions(((_a = ast.functions) === null || _a === void 0 ? void 0 : _a.clause) || '');
        mathjs.import(functions, { override: true });
        if (templateParams) {
            query = renderTemplate(query, templateParams);
        }
        // substitute parameters and detect unspecified ones
        const res = substituteMacros(queryNormalized, macros);
        if (res.unknown_params.length) {
            throw new Error('The following parameters used in query and were not specified: ' +
                res.unknown_params +
                (macros
                    ? ', all passed macros: ' + Object.keys(macros)
                    : ', no macros were passed'));
        }
        queryNormalized = res.text;
        // reparse query again with substituted macro
        ast = parse(queryNormalized);
        let raw_select_fields = [];
        const selectFields = ast.select.fields.map(f => {
            return {
                selector: f.expression.selector,
                type: f.expression.type,
                alias: f.alias,
            };
        });
        let resourceName = ast.from.resource;
        let resourceTypeFrom;
        if (resourceName.startsWith('builtin.')) {
            // it's a builtin query, but it still can query an Ads resource
            resourceName = resourceName.substring('builtin.'.length);
            return this.builtinQueryProcessor.parse(resourceName, query);
        }
        else {
            resourceTypeFrom = this.getResource(resourceName);
        }
        const resourceInfo = {
            name: resourceName,
            typeName: resourceTypeFrom.name,
            typeMeta: resourceTypeFrom,
            isConstant: resourceName.endsWith('_constant'),
        };
        let field_index = 0;
        const fields = [];
        const column_names = [];
        let expandWildcardAt = -1;
        for (const selectField of selectFields) {
            const parsedExpr = this.parseExpression(selectField.selector);
            // initialize column alias
            let column_name = selectField.alias || parsedExpr.field.replaceAll(/\./g, '_');
            if (!selectField.alias && column_name.startsWith(resourceName + '_')) {
                // cut off the current resource name from auto-generated column name
                column_name = column_name.substring(resourceName.length + 1);
            }
            column_name = column_name.replaceAll(/[ ]/g, '');
            // check for uniqueness
            if (column_names.includes(column_name)) {
                throw new InvalidQuerySyntax(`duplicating column name ${column_name} at index ${field_index}`);
            }
            column_names.push(column_name);
            // now decide on how the current column should be mapped to native query
            let select_expr_parsed = parsedExpr.field.trim();
            let fieldType;
            if (select_expr_parsed === '*') {
                if (expandWildcardAt > -1) {
                    throw new InvalidQuerySyntax(`duplicating wildcard '*' expression encountered at index ${field_index}`);
                }
                expandWildcardAt = field_index;
                continue;
            }
            else if (parsedExpr.customizer) {
                raw_select_fields.push(select_expr_parsed);
                const nameParts = select_expr_parsed.split('.');
                const curType = this.getResource(nameParts[0]);
                fieldType = this.getFieldType(curType, nameParts.slice(1));
                if (parsedExpr.customizer.type === CustomizerType.NestedField) {
                    // we expect a field with nested_field customizer should ends with a
                    // type (not primitive, not enum) i.e. ProtoTypeMeta
                    if (isString(fieldType.type)) {
                        throw new Error(`InvalidQuery: field ${column_name} contains nested field accessor (:) but selected field's type is primitive (${fieldType.typeName})`);
                    }
                    if (isEnumType(fieldType.type)) {
                        throw new Error(`InvalidQuery: field ${column_name} contains nested field accessor (:) but selected field's type enum (${fieldType.typeName})`);
                    }
                    const repeated = fieldType.repeated;
                    fieldType = this.getFieldType(fieldType.type, parsedExpr.customizer.selector.split('.'));
                    fieldType.repeated = repeated || fieldType.repeated;
                }
                else if (parsedExpr.customizer.type === CustomizerType.ResourceIndex) {
                    fieldType.typeName = 'int64';
                    fieldType.type = 'int64';
                    fieldType.kind = FieldTypeKind.primitive;
                }
                else if (parsedExpr.customizer.type === CustomizerType.Function) {
                    const func = functions[parsedExpr.customizer.function];
                    if (!func) {
                        throw new Error(`InvalidQuerySyntax: unknown function reference '${parsedExpr.customizer.function}' in expression '${selectField.selector}'`);
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
            }
            else {
                // non-customizer column
                const field_regexp = /^[\w]+(\.[\w]+)+$/i;
                const field_match = field_regexp.exec(select_expr_parsed);
                if (field_match && field_match[0] === select_expr_parsed) {
                    // looks like a field accessor
                    raw_select_fields.push(select_expr_parsed);
                }
                else {
                    // everything else should be an expression
                    // we should parse all field accessors from the expression and
                    // add them into raw query for selecting.
                    let parsed_expression;
                    try {
                        parsed_expression = mathjs.parse(select_expr_parsed);
                    }
                    catch (e) {
                        console.warn(`Failed to parse: ${select_expr_parsed}, at index ${field_index}`);
                        throw e;
                    }
                    let field = undefined;
                    const raw_accessors = extractFieldAccesses(parsed_expression);
                    if (raw_accessors.length) {
                        for (const f of raw_accessors) {
                            // support for nested fields (resource.field1:field2)
                            const pairs = f.split(':');
                            if (pairs.length > 1) {
                                raw_select_fields.push(pairs[0]);
                                select_expr_parsed = select_expr_parsed.replaceAll(pairs.join(':'), pairs.join('.'));
                                parsed_expression = mathjs.parse(select_expr_parsed);
                            }
                            else {
                                raw_select_fields.push(f);
                            }
                        }
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
                    else {
                        // if no field accesses then it's a constant expression
                        let value;
                        try {
                            value = parsed_expression.evaluate();
                        }
                        catch (e) {
                            assertIsError(e);
                            if (e.message.match(`Undefined symbol (metrics)|(segments)|(${resourceName.toLowerCase()})`)) {
                                // TODO: actually there could be other resources in the query
                                // We should extract the name of the symbol and check if it's a resource
                                // it's something that we couldn't parse, but it's a constant,
                                // so we'll keep as virt columns just without auto-adding
                                // accessed fields (they have to be fetched explicitly)
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
                            else {
                                throw new Error(`Failed to evaluate column (at index ${field_index}) expression "${select_expr_parsed}": ${e.message}`);
                            }
                        }
                        if (!field) {
                            const value_type = isInteger(value)
                                ? 'int64'
                                : isNumber(value)
                                    ? 'double'
                                    : 'string';
                            field = {
                                name: column_name,
                                customizer: {
                                    type: CustomizerType.VirtualColumn,
                                    evaluator: { evaluate: () => value },
                                },
                                expression: select_expr_parsed,
                                type: {
                                    kind: FieldTypeKind.primitive,
                                    type: value_type,
                                    typeName: value_type,
                                },
                            };
                        }
                    }
                    fields.push(field);
                    continue;
                }
            }
            const field = {
                name: column_name,
                customizer: parsedExpr.customizer,
                expression: select_expr_parsed,
                type: this.getColumnType(column_name, select_expr_parsed, parsedExpr.customizer),
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
                if (field.rule !== 'repeated' &&
                    !column_names.includes(fieldName) &&
                    (this.primitiveTypes.includes(field.type) ||
                        field.type.match('google\\.ads.googleads.\\w+.enums'))) {
                    //"google.ads.googleads.v14.enums.CustomerStatusEnum.CustomerStatus"
                    raw_select_fields.push(resourceInfo.name + '.' + fieldName);
                    const column = {
                        name: fieldName,
                        expression: resourceInfo.name + '.' + fieldName,
                        type: this.getColumnType(fieldName, resourceInfo.name + '.' + fieldName),
                    };
                    new_fields.push(column);
                }
            }
            fields.splice(expandWildcardAt, 0, ...new_fields);
        }
        // remove duplicates:
        raw_select_fields = [...new Set(raw_select_fields)];
        // now we have a list of accessors which are supposed to be resource fields,
        // but if a field is a structure (Message in protos) then an accessor can
        // have addition parts which are not selectable - for example:
        // campaign.final_urls is a MESSAGE common.CustomParameter
        // SELECT query can only have 'campaign.final_urls' column, but expressions
        // can be deeper: campaign.final_urls.key
        //
        raw_select_fields.join(', ');
        const queryNative = this.compileAst(ast, raw_select_fields);
        return new QueryElements(queryNative, fields, resourceInfo, functions);
    }
    getColumnType(columnName, columnExpression, customizer) {
        const nameParts = columnExpression.split('.');
        const curType = this.getResource(nameParts[0]);
        let fieldType = this.getFieldType(curType, nameParts.slice(1));
        if (customizer) {
            if (customizer.type === CustomizerType.NestedField) {
                // we expect a field with nested_field customizer should ends with a
                // type (not primitive, not enum) i.e. ProtoTypeMeta
                if (isString(fieldType.type)) {
                    throw new Error(`InvalidQuery: field ${columnName} contains nested field accessor (:) but selected field's type is primitive (${fieldType.typeName})`);
                }
                if (isEnumType(fieldType.type)) {
                    throw new Error(`InvalidQuery: field ${columnName} contains nested field accessor (:) but selected field's type enum (${fieldType.typeName})`);
                }
                const repeated = fieldType.repeated;
                fieldType = this.getFieldType(fieldType.type, customizer.selector.split('.'));
                fieldType.repeated = repeated || fieldType.repeated;
            }
            else if (customizer.type === CustomizerType.ResourceIndex) {
                fieldType.typeName = 'int64';
                fieldType.type = 'int64';
                fieldType.kind = FieldTypeKind.primitive;
            }
            else if (customizer.type === CustomizerType.Function) {
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
    getFieldType(type, nameParts) {
        if (!nameParts || !nameParts.length)
            throw new Error('ArgumentException: namePart is empty');
        if (!type)
            throw new Error('ArgumentException: type was not specified');
        const rootType = type.name;
        for (let i = 0; i < nameParts.length; i++) {
            const isLastPart = i === nameParts.length - 1;
            let fieldType;
            const field = type.fields[nameParts[i]];
            if (!field) {
                if (isLastPart) {
                    // it's an unknown field (probably from a future version)
                    fieldType = {
                        repeated: false,
                        type: 'string',
                        typeName: 'string',
                        kind: FieldTypeKind.primitive,
                    };
                    return fieldType;
                }
                throw new Error(`Resource or type '${type.name}' does not have field '${nameParts[i]}' (initial property chain is ${nameParts.join('.')} and resource is '${rootType}')`);
            }
            const repeated = field.rule === 'repeated';
            if (repeated && !isLastPart) {
                throw new Error(`InternalError: repeated field '${nameParts[i]}' in the middle of prop chain '${nameParts.join('.')}'`);
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
                    throw new Error(`InternalError: field '${nameParts[i]}' in prop chain '${nameParts.join('.')}' has primitive type ${fieldTypeName}`);
                }
                return fieldType;
            }
            // is it a link to common type or enum
            else if (fieldTypeName.startsWith(`google.ads.googleads.${protoVer}.enums.`)) {
                // google.ads.googleads.v9.enums
                // e.g. "google.ads.googleads.v9.enums.CriterionTypeEnum.CriterionType"
                const match = fieldTypeName.match(/google\.ads\.googleads\.v[\d]+\.enums\.([^.]+)\.([^.]+)/i);
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
                    throw new Error(`InternalError: field '${nameParts[i]}' in prop chain '${nameParts.join('.')}' has enum type ${fieldTypeName}`);
                }
                return fieldType;
            }
            else if (fieldTypeName.startsWith(`google.ads.googleads.${protoVer}.common.`)) {
                // google.ads.googleads.v9.common
                const match = fieldTypeName.match(/google\.ads\.googleads\.v[\d]+\.common\.([^.]+)/i);
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
            }
            else {
                // then it's either another resource or a nested type
                if (type.nested && type.nested[fieldTypeName]) {
                    fieldType = {
                        repeated,
                        type: type.nested[fieldTypeName],
                        typeName: fieldTypeName,
                        kind: FieldTypeKind.struct,
                    };
                }
                else if (protoResources[fieldTypeName]) {
                    fieldType = {
                        repeated,
                        type: protoResources[fieldTypeName],
                        typeName: fieldTypeName,
                        kind: FieldTypeKind.struct,
                    };
                }
                else if (protoCommonTypes[fieldTypeName]) {
                    // yes, some fields refer to common types by a full name but some by a
                    // short one
                    fieldType = {
                        repeated,
                        type: protoCommonTypes[fieldTypeName],
                        typeName: fieldTypeName,
                        kind: FieldTypeKind.struct,
                    };
                }
                else {
                    throw new Error(`InternalError: could not find a type proto for ${fieldTypeName} (field ${nameParts})`);
                }
            }
            type = fieldType.type;
            if (isLastPart)
                return fieldType;
        }
        throw new Error('InternalError');
    }
    getResource(fieldName) {
        let resourceType = this.resourcesMap[fieldName];
        if (resourceType)
            return resourceType;
        const resource = protoRowType.fields[fieldName];
        if (!resource)
            throw new Error(`Could not find resource '${fieldName}' in protobuf schema`);
        // resource.type will be a full name like
        // "google.ads.googleads.v9.resources.AdGroup" or
        // "google.ads.googleads.v9.common.Metrics"
        // we need to get the last part and find such a resource in protos
        const nameParts = resource.type.split('.');
        const resourceTypeName = nameParts[nameParts.length - 1];
        if (resource.type.startsWith(`google.ads.googleads.${protoVer}.resources.`)) {
            resourceType = protoResources[resourceTypeName];
        }
        else if (resource.type.startsWith(`google.ads.googleads.${protoVer}.common.`)) {
            resourceType = protoCommonTypes[resourceTypeName];
        }
        if (!resourceType) {
            throw new Error(`InternalError: could find resource ${resourceTypeName}`);
        }
        this.resourcesMap[fieldName] = resourceType;
        resourceType['name'] = resourceTypeName;
        return resourceType;
    }
    parseExpression(selectExpr) {
        // a normal field: resource.field.may_be_another.may_be_yet_another
        // const field_accessor_re = /[\w]+(\.[\w]+)+/i;
        // Resource Indexes: resource.field.may_be_another~N, where N - digit
        const field_with_resource_index_re = /^([\w.]+[\w])~(\d+)$/i;
        // Nested Field: resource.field.may_be_another:nested.may_be_another
        const field_with_nested_accessor_re = /^([\w.]+[\w]):(\$?[\w.]+[\w])$/i;
        // TODO: support nested customizers, e.g.:
        //    res.field1:field2~1
        //    res.array_field1:field2.array_field3:field3
        // resource index (resource~N)
        let result = field_with_resource_index_re.exec(selectExpr);
        if (result) {
            const index = Number(result[2]);
            if (Number.isNaN(index) ||
                !Number.isFinite(index) ||
                !Number.isInteger(index)) {
                throw new Error(`Expression '${selectExpr}' contains indexed resource access ('~') but argument isn't an integer (${result[2]})`);
            }
            return {
                field: result[1],
                customizer: {
                    type: CustomizerType.ResourceIndex,
                    index,
                },
            };
        }
        // nested resource accessor
        result = field_with_nested_accessor_re.exec(selectExpr);
        if (result) {
            const value = result[2];
            if (value.startsWith('$')) {
                // the value is a function
                return {
                    field: result[1],
                    customizer: {
                        type: CustomizerType.Function,
                        function: value.slice(1),
                    },
                };
            }
            return {
                field: result[1],
                customizer: { type: CustomizerType.NestedField, selector: value },
            };
        }
        // otherwise it's a column or an expression using columns
        return { field: selectExpr };
    }
}
//# sourceMappingURL=ads-query-editor.js.map