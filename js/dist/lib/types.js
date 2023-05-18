"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueryElements = exports.isEnumType = exports.FieldTypeKind = exports.CustomizerType = void 0;
var CustomizerType;
(function (CustomizerType) {
    CustomizerType["ResourceIndex"] = "ResourceIndex";
    CustomizerType["NestedField"] = "NestedField";
    CustomizerType["Function"] = "Function";
    CustomizerType["VirtualColumn"] = "VirtualColumn";
})(CustomizerType = exports.CustomizerType || (exports.CustomizerType = {}));
var FieldTypeKind;
(function (FieldTypeKind) {
    FieldTypeKind[FieldTypeKind["primitive"] = 0] = "primitive";
    FieldTypeKind[FieldTypeKind["enum"] = 1] = "enum";
    FieldTypeKind[FieldTypeKind["struct"] = 2] = "struct";
})(FieldTypeKind = exports.FieldTypeKind || (exports.FieldTypeKind = {}));
function isEnumType(type) {
    return !!type.values;
}
exports.isEnumType = isEnumType;
class QueryElements {
    constructor(query, columns, resource, functions) {
        this.queryText = "";
        this.queryText = query;
        this.columns = columns;
        this.resource = resource;
        this.functions = functions;
    }
    get columnNames() {
        return this.columns.map(col => col.name);
    }
    get columnTypes() {
        return this.columns.map(col => col.type);
    }
}
exports.QueryElements = QueryElements;
//# sourceMappingURL=types.js.map