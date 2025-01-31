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

import {isArray} from 'lodash-es';
import yaml from 'js-yaml';
import {GoogleAdsApiConfig, IGoogleAdsApiClient} from './ads-api-client.js';
import {getFileContent} from './file-utils.js';
import {AdsQueryExecutor} from './ads-query-executor.js';

/**
 * Return a normalized list of customer ids
 * @param customerId a customer id or a list of ids via comma
 * @param adsConfig a config
 * @returns a customer id
 */
export function parseCustomerIds(
  customerId: string | undefined,
  adsConfig: GoogleAdsApiConfig
) {
  let customerIds: string[] | undefined;
  if (!customerId) {
    // CID/account wasn't provided explicitly, we'll use customer_id field from ads-config (it can be absent)
    if (adsConfig.customer_id) {
      if (isArray(adsConfig.customer_id)) {
        customerIds = adsConfig.customer_id;
      } else {
        customerIds = [adsConfig.customer_id];
      }
    }
  } else {
    // NOTE: argv.account is CLI arg, it can only be a string
    if (customerId.includes(',')) {
      customerIds = customerId.split(',');
    } else {
      customerIds = [customerId];
    }
  }
  if (!customerIds && adsConfig.login_customer_id) {
    // last chance if no CID was provided is to use login_customer_id
    customerIds = [adsConfig.login_customer_id];
  }

  if (customerIds && customerIds.length) {
    for (let i = 0; i < customerIds.length; i++) {
      customerIds[i] = customerIds[i].toString().replaceAll('-', '');
    }
  }
  return customerIds;
}

/**
 * Load Ads credentials from a file (json or yaml)
 * @param configFilepath a path to config
 * @returns Ads credentials
 */
export async function loadAdsConfigFromFile(
  configFilepath: string
): Promise<GoogleAdsApiConfig> {
  try {
    const content = await getFileContent(configFilepath);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const doc: any = configFilepath.endsWith('.json')
      ? JSON.parse(content)
      : yaml.load(content);

    return {
      developer_token: doc['developer_token'],
      client_id: doc['client_id'],
      client_secret: doc['client_secret'],
      refresh_token: doc['refresh_token'],
      login_customer_id: doc['login_customer_id']?.toString(),
      customer_id: doc['customer_id']?.toString(),
      json_key_file_path: doc['json_key_file_path'],
    };
  } catch (e) {
    throw new Error(
      `Failed to load Ads API configuration from ${configFilepath}: ${e}`
    );
  }
}

export interface CustomerInfo {
  id: string;
  name: string | null;
  is_mcc: boolean;
  status: string;
  children: CustomerInfo[];
}

/**
 * Bulid a hierarchy with account structure starting from a cid.
 * @param adsClient Ads client
 * @param customerId a seed customer id
 * @returns a hierarchy of CustomerInfo
 */
export async function getCustomerInfo(
  adsClient: IGoogleAdsApiClient,
  customerId: string
): Promise<CustomerInfo> {
  const queryText = `SELECT
      customer_client.id,
      customer_client.level,
      customer_client.status,
      customer_client.manager
    FROM customer_client
    WHERE
      customer_client.level <= 1
      AND customer_client.status = ENABLED
      AND customer_client.hidden = FALSE
    ORDER BY customer_client.level`;
  //
  const queryText2 = 'SELECT customer.descriptive_name FROM customer';
  let customer: CustomerInfo | undefined = undefined;
  const query = adsClient.getQueryEditor().parseQuery(queryText);
  const query2 = adsClient.getQueryEditor().parseQuery(queryText2);
  const executor = new AdsQueryExecutor(adsClient);
  const result = await executor.executeQueryAndParseToObjects(
    query,
    customerId
  );
  for (const row of result.rows!) {
    const cid = row['id'].toString();
    if (row['level'].toString() === '0') {
      // the current account itself
      const descriptiveName =
        row['status'] === 'ENABLED'
          ? (await executor.executeQueryAndParse(query2, cid)).rows![0]
          : null;
      customer = {
        id: cid,
        name: descriptiveName,
        is_mcc: false,
        status: row['status'],
        children: [],
      };
    } else {
      customer!.children.push(await getCustomerInfo(adsClient, cid));
      customer!.is_mcc = true;
    }
  }
  return customer!;
}

/**
 * Get all nested non-MCC account for the specified one(s).
 * If the specified one is a leaf account (non-MCC) then it will be returned
 * @param customerId A customer account (CID)
 * @returns a list of child account (at all levels)
 */
export async function getCustomerIds(
  adsClient: IGoogleAdsApiClient,
  customerId: string | string[]
): Promise<string[]> {
  const queryText = `SELECT
      customer_client.id as cid
    FROM customer_client
    WHERE
      customer_client.status = ENABLED
      AND customer_client.hidden = FALSE
      AND customer_client.manager = FALSE`;
  if (typeof customerId === 'string') {
    customerId = [customerId];
  }
  const all_ids = [];
  const executor = new AdsQueryExecutor(adsClient);
  const query = adsClient.getQueryEditor().parseQuery(queryText);
  for (const cid of customerId) {
    const res = await executor.executeQueryAndParse(query, cid);
    const ids = res!.rows!.map(row => row[0].toString());
    all_ids.push(...ids);
  }
  return all_ids;
}

/**
 * Filter customers with a query.
 * @param adsClient Ads client
 * @param ids a list of customer ids to filter
 * @param customer_ids_query a query
 * @returns a filtered list of customer ids
 */
export async function filterCustomerIds(
  adsClient: IGoogleAdsApiClient,
  ids: string[],
  customer_ids_query: string
): Promise<string[]> {
  const query = adsClient.getQueryEditor().parseQuery(customer_ids_query);
  const accounts: Set<string> = new Set();
  const executor = new AdsQueryExecutor(adsClient);
  for (const id of ids) {
    const result = await executor.executeQueryAndParse(query, id);
    if (result.rowCount > 0) {
      for (const row of result.rows!) {
        accounts.add(row[0]);
      }
    }
    // TODO: purge Customer objects in IGoogleAdsApiClient
  }
  return Array.from(accounts);
}
