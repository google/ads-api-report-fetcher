"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ArrayHandling = exports.QueryElements = exports.ApiType = exports.isEnumType = exports.FieldTypeKind = exports.CustomizerType = void 0;
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
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function isEnumType(type) {
    return !!type.values;
}
exports.isEnumType = isEnumType;
var ApiType;
(function (ApiType) {
    ApiType["gRPC"] = "gRPC";
    ApiType["REST"] = "REST";
})(ApiType = exports.ApiType || (exports.ApiType = {}));
class QueryElements {
    constructor(query, columns, resource, functions) {
        this.queryText = '';
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
var ArrayHandling;
(function (ArrayHandling) {
    ArrayHandling["strings"] = "strings";
    ArrayHandling["arrays"] = "arrays";
})(ArrayHandling = exports.ArrayHandling || (exports.ArrayHandling = {}));
//# sourceMappingURL=types.js.map