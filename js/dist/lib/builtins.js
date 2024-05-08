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
            let queryNew = "SELECT customer.id, metrics.optimization_score_url FROM customer LIMIT 1";
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
            let resourceTypeFrom = this.queryEditor.getResource("customer");
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
        /*
        // TODO: WIP
        if (
          name === "account_hierarchy_flattened" ||
          name === "account_hierarchy"
        ) {
          let resourceTypeFrom = this.queryEditor.getResource("customer");
          let resourceInfo = {
            name: "account_hierarchy_flattened",
            typeName: resourceTypeFrom.name,
            typeMeta: resourceTypeFrom,
            isConstant: false,
          };
          let fields = [
            {
              name: "mcc_id",
              expression: "mcc_id",
              type: {
                kind: FieldTypeKind.primitive,
                type: "int64",
                typeName: "int64",
              },
            },
            {
              name: "mcc_name",
              expression: "mcc_name",
              type: {
                kind: FieldTypeKind.primitive,
                type: "string",
                typeName: "string",
              },
            },
          ];
          let query = new QueryElements("", fields, resourceInfo, {});
          query.executor = this;
          return query;
        }
        */
        throw new Error(`Could not find a builtin resource '${name}'`);
    }
    async *execute(query, customerId, executor) {
        var _a, _b;
        if (query.resource.name === "ocid") {
            let queryReal = "SELECT customer.id, metrics.optimization_score_url FROM customer LIMIT 1";
            // we need to parse result so we wrap generator
            let stream = executor.client.executeQueryStream(queryReal, customerId);
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
        /*
        else if (query.resource.name === "account_hierarchy_flattened") {
          // TODO: WIP
          const seed_account_query = `
          SELECT
              customer_client.level,
              customer_client.manager,
              customer_client.id
          FROM customer_client
          WHERE customer_client.level <= 1`;
    
          const nested_account_query = `
          SELECT
              customer.id AS mcc_id,
              customer.descriptive_name AS mcc_name,
              {level} AS level,
              customer_client_link.client_customer~0 AS account_id
          FROM customer_client_link`;
          let stream = executor.client.executeQueryStream(
            seed_account_query,
            customerId
          );
          const level_mapping: Map<string, string[]> = new Map();
          for await (const row of stream) {
            if (row.customer_client?.manager) {
              const key = row.customer_client.level!.toString();
              let ids = level_mapping.get(key) || [];
              ids.push(row.customer_client.id!.toString());
              level_mapping.set(key, ids);
            }
          }
          console.log(level_mapping);
          let results = [];
          for (const [level, cids] of level_mapping.entries()) {
            const queryText = nested_account_query.replaceAll("{level}", level);
            for (const cid of cids) {
              const query = executor.parseQuery(queryText);
              //const rows = await executor.client.executeQuery(query, cid);
              const result = await executor.executeOne(query, cid);
              if (result)
                for (const row of result.rows!) {
                  results.push({
                    mcc_id: row.mcc_id,
                    mcc_name: row.mcc_name,
                  });
                }
            }
          }
          for (const i of results) {
            yield i;
          }
          return;
        }
        */
        throw new Error("Unknown builtin query: " + query.resource.name);
    }
}
exports.BuiltinQueryProcessor = BuiltinQueryProcessor;
//# sourceMappingURL=builtins.js.map