"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.QueryElements = exports.isEnumType = exports.FieldTypeKind = exports.CustomizerType = void 0;
var CustomizerType;
(function (CustomizerType) {
    CustomizerType["ResourceIndex"] = "ResourceIndex";
    CustomizerType["NestedField"] = "NestedField";
    CustomizerType["Function"] = "Function";
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
    constructor(query, fields, column_names, customizers, resource, columnTypes, functions) {
        this.queryText = '';
        this.columnNames = [];
        this.queryText = query;
        this.fields = fields;
        this.columnNames = column_names;
        this.customizers = customizers;
        this.resource = resource;
        this.columnTypes = columnTypes;
        this.functions = functions;
    }
}
exports.QueryElements = QueryElements;
//# sourceMappingURL=types.js.map