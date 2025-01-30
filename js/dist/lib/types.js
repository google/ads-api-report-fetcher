export var CustomizerType;
(function (CustomizerType) {
    CustomizerType["ResourceIndex"] = "ResourceIndex";
    CustomizerType["NestedField"] = "NestedField";
    CustomizerType["Function"] = "Function";
    CustomizerType["VirtualColumn"] = "VirtualColumn";
})(CustomizerType || (CustomizerType = {}));
export var FieldTypeKind;
(function (FieldTypeKind) {
    FieldTypeKind[FieldTypeKind["primitive"] = 0] = "primitive";
    FieldTypeKind[FieldTypeKind["enum"] = 1] = "enum";
    FieldTypeKind[FieldTypeKind["struct"] = 2] = "struct";
})(FieldTypeKind || (FieldTypeKind = {}));
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function isEnumType(type) {
    return !!type.values;
}
export var ApiType;
(function (ApiType) {
    ApiType["gRPC"] = "gRPC";
    ApiType["REST"] = "REST";
})(ApiType || (ApiType = {}));
export class QueryElements {
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
export var ArrayHandling;
(function (ArrayHandling) {
    ArrayHandling["strings"] = "strings";
    ArrayHandling["arrays"] = "arrays";
})(ArrayHandling || (ArrayHandling = {}));
//# sourceMappingURL=types.js.map