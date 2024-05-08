import { FieldTypeKind, IQueryExecutor, QueryElements } from "./types";
import { AdsQueryEditor } from "./ads-query-editor";
import { AdsQueryExecutor } from "./ads-query-executor";

export class BuiltinQueryProcessor implements IQueryExecutor {
  constructor(public queryEditor: AdsQueryEditor) {}

  parse(name: string, query: string) {
    if (name === "ocid_mapping" || name === "ocid") {
      let queryNew =
        "SELECT customer.id, metrics.optimization_score_url FROM customer LIMIT 1";
      let fields = [
        {
          name: "customer_id",
          expression: "customer_id",
          type: {
            kind: FieldTypeKind.primitive,
            type: "int64",
            typeName: "int64",
          },
        },
        {
          name: "ocid",
          expression: "ocid",
          type: {
            kind: FieldTypeKind.primitive,
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
      let query = new QueryElements(queryNew, fields, resourceInfo, {});
      query.executor = this;
      return query;
    }
    throw new Error(`Could not find a builtin resource '${name}'`);
  }

  async *execute(
    query: QueryElements,
    customerId: string,
    executor: AdsQueryExecutor
  ): AsyncGenerator<any> {
    if (query.resource.name === "ocid") {
      let queryReal =
        "SELECT customer.id, metrics.optimization_score_url FROM customer LIMIT 1";
      // we need to parse result so we wrap generator
      let stream = executor.client.executeQueryStream(queryReal, customerId);
      for await (const row of stream) {
        let new_row = {
          customer_id: row.customer?.id,
          ocid: row.metrics?.optimization_score_url,
        };
        if (new_row.ocid) {
          let ocid = new_row.ocid.match("ocid=(\\w+)");
          if (ocid?.length) {
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
