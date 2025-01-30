/*
 Copyright 2025 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import {FieldTypeKind, IQueryExecutor, QueryElements} from './types.js';
import {AdsQueryEditor} from './ads-query-editor.js';
import {AdsQueryExecutor} from './ads-query-executor.js';

export class BuiltinQueryProcessor implements IQueryExecutor {
  constructor(public queryEditor: AdsQueryEditor) {}

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  parse(name: string, query: string) {
    if (name === 'ocid_mapping' || name === 'ocid') {
      const queryNew =
        'SELECT customer.id, metrics.optimization_score_url FROM customer LIMIT 1';
      const fields = [
        {
          name: 'customer_id',
          expression: 'customer_id',
          type: {
            kind: FieldTypeKind.primitive,
            type: 'int64',
            typeName: 'int64',
          },
        },
        {
          name: 'ocid',
          expression: 'ocid',
          type: {
            kind: FieldTypeKind.primitive,
            type: 'string',
            typeName: 'string',
          },
        },
      ];
      const resourceTypeFrom = this.queryEditor.getResource('customer');
      const resourceInfo = {
        name: 'ocid',
        typeName: resourceTypeFrom.name,
        typeMeta: resourceTypeFrom,
        isConstant: false,
      };
      const query = new QueryElements(queryNew, fields, resourceInfo, {});
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

  async *execute(
    query: QueryElements,
    customerId: string,
    executor: AdsQueryExecutor
  ): AsyncGenerator<Record<string, unknown>> {
    if (query.resource.name === 'ocid') {
      const queryRealText =
        'SELECT customer.id, metrics.optimization_score_url as url FROM customer LIMIT 1';
      // we need to parse result so we wrap generator
      const queryReal = executor.editor.parseQuery(queryRealText);
      const result = await executor.executeQueryAndParseToObjects(
        queryReal,
        customerId
      );
      if (result.rows)
        for (const row of result.rows) {
          const new_row = {
            customer_id: row['id'],
            ocid: row['url'],
          };
          if (new_row.ocid) {
            const ocid = new_row.ocid.match('ocid=(\\w+)');
            if (ocid?.length) {
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
    throw new Error('Unknown builtin query: ' + query.resource.name);
  }
}
