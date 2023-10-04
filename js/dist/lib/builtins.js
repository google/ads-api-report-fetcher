"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BuiltinQueryProcessor = void 0;
const types_1 = require("./types");
class BuiltinQueryProcessor {
    constructor(queryEditor) {
        this.queryEditor = queryEditor;
    }
    parse(name, query) {
        if (name === "ocid_mapping" || name === "ocid") {
            let queryNew = "SELECT customer.id, metrics.optimization_score_url FROM campaign LIMIT 1";
            let fields = [
                {
                    name: "customer_id",
                    expression: "customer_id",
                    type: {
                        kind: types_1.FieldTypeKind.primitive,
                        type: "int64",
                        typeName: "int64",
                    },
                },
                {
                    name: "ocid",
                    expression: "ocid",
                    type: {
                        kind: types_1.FieldTypeKind.primitive,
                        type: "string",
                        typeName: "string",
                    },
                },
            ];
            let resourceTypeFrom = this.queryEditor.getResource("campaign");
            let resourceInfo = {
                name: "ocid",
                typeName: resourceTypeFrom.name,
                typeMeta: resourceTypeFrom,
                isConstant: false,
            };
            let query = new types_1.QueryElements(queryNew, fields, resourceInfo, {});
            query.executor = this;
            return query;
        }
        throw new Error(`Could not find a builtin resource '${name}'`);
    }
    async *execute(client, query, customerId) {
        var _a, _b;
        if (query.resource.name === "ocid") {
            let queryReal = "SELECT customer.id, metrics.optimization_score_url FROM campaign LIMIT 1";
            // we need to parse result so we wrap generator
            let stream = client.executeQueryStream(queryReal, customerId);
            for await (const row of stream) {
                let new_row = {
                    customer_id: (_a = row.customer) === null || _a === void 0 ? void 0 : _a.id,
                    ocid: (_b = row.metrics) === null || _b === void 0 ? void 0 : _b.optimization_score_url,
                };
                if (new_row.ocid) {
                    let ocid = new_row.ocid.match("ocid=(\\w+)");
                    if (ocid === null || ocid === void 0 ? void 0 : ocid.length) {
                        new_row.ocid = ocid[1];
                    }
                }
                yield new_row;
            }
            return;
        }
        throw new Error("Unknown builtin query: " + query.resource.name);
    }
}
exports.BuiltinQueryProcessor = BuiltinQueryProcessor;
//# sourceMappingURL=builtins.js.map