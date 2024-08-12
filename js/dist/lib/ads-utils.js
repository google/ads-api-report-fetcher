"use strict";
/*
 Copyright 2024 Google LLC

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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.filterCustomerIds = exports.getCustomerIds = exports.getCustomerInfo = exports.loadAdsConfigFromFile = exports.parseCustomerIds = void 0;
const file_utils_1 = require("./file-utils");
const lodash_1 = __importDefault(require("lodash"));
const js_yaml_1 = __importDefault(require("js-yaml"));
const ads_query_executor_1 = require("./ads-query-executor");
/**
 * Return a normalized list of customer ids
 * @param customerId a customer id or a list of ids via comma
 * @param adsConfig a config
 * @returns a customer id
 */
function parseCustomerIds(customerId, adsConfig) {
    let customerIds;
    if (!customerId) {
        // CID/account wasn't provided explicitly, we'll use customer_id field from ads-config (it can be absent)
        if (adsConfig.customer_id) {
            if (lodash_1.default.isArray(adsConfig.customer_id)) {
                customerIds = adsConfig.customer_id;
            }
            else {
                customerIds = [adsConfig.customer_id];
            }
        }
    }
    else {
        // NOTE: argv.account is CLI arg, it can only be a string
        if (customerId.includes(",")) {
            customerIds = customerId.split(",");
        }
        else {
            customerIds = [customerId];
        }
    }
    if (!customerIds && adsConfig.login_customer_id) {
        // last chance if no CID was provided is to use login_customer_id
        customerIds = [adsConfig.login_customer_id];
    }
    if (customerIds && customerIds.length) {
        for (let i = 0; i < customerIds.length; i++) {
            customerIds[i] = customerIds[i].toString().replaceAll("-", "");
        }
    }
    return customerIds;
}
exports.parseCustomerIds = parseCustomerIds;
/**
 * Load Ads credentials from a file (json or yaml)
 * @param configFilepath a path to config
 * @returns Ads credentials
 */
async function loadAdsConfigFromFile(configFilepath) {
    var _a, _b;
    try {
        const content = await (0, file_utils_1.getFileContent)(configFilepath);
        const doc = configFilepath.endsWith(".json")
            ? JSON.parse(content)
            : js_yaml_1.default.load(content);
        return {
            developer_token: doc["developer_token"],
            client_id: doc["client_id"],
            client_secret: doc["client_secret"],
            refresh_token: doc["refresh_token"],
            login_customer_id: (_a = doc["login_customer_id"]) === null || _a === void 0 ? void 0 : _a.toString(),
            customer_id: (_b = doc["customer_id"]) === null || _b === void 0 ? void 0 : _b.toString(),
        };
    }
    catch (e) {
        throw new Error(`Failed to load Ads API configuration from ${configFilepath}: ${e}`);
    }
}
exports.loadAdsConfigFromFile = loadAdsConfigFromFile;
/**
 * Bulid a hierarchy with account structure starting from a cid.
 * @param adsClient Ads client
 * @param customerId a seed customer id
 * @returns a hierarchy of CustomerInfo
 */
async function getCustomerInfo(adsClient, customerId) {
    const queryText = `SELECT
      customer_client.id,
      customer_client.level,
      customer_client.status,
      customer_client.manager
    FROM customer_client
    WHERE
      customer_client.level <= 1
      AND customer_client.status = "ENABLED"
    ORDER BY customer_client.level`;
    //
    const queryText2 = `SELECT customer.descriptive_name FROM customer`;
    let customer = undefined;
    const query = adsClient.getQueryEditor().parseQuery(queryText);
    const query2 = adsClient.getQueryEditor().parseQuery(queryText2);
    const executor = new ads_query_executor_1.AdsQueryExecutor(adsClient);
    const result = await executor.executeQueryAndParseToObjects(query, customerId);
    for (const row of result.rows) {
        const cid = row["id"].toString();
        if (row["level"].toString() === '0') {
            // the current account itself
            const descriptiveName = row["status"] === "ENABLED"
                ? (await executor.executeQueryAndParse(query2, cid)).rows[0]
                : null;
            customer = {
                id: cid,
                name: descriptiveName,
                is_mcc: false,
                status: row["status"],
                children: [],
            };
        }
        else {
            customer.children.push(await getCustomerInfo(adsClient, cid));
            customer.is_mcc = true;
        }
    }
    return customer;
}
exports.getCustomerInfo = getCustomerInfo;
/**
 * Get all nested non-MCC account for the specified one(s).
 * If the specified one is a leaf account (non-MCC) then it will be returned
 * @param customerId A customer account (CID)
 * @returns a list of child account (at all levels)
 */
async function getCustomerIds(adsClient, customerId) {
    const queryText = `SELECT
      customer_client.id as cid
    FROM customer_client
    WHERE
      customer_client.status = "ENABLED" AND
      customer_client.manager = False`;
    if (typeof customerId === "string") {
        customerId = [customerId];
    }
    let all_ids = [];
    const executor = new ads_query_executor_1.AdsQueryExecutor(adsClient);
    const query = adsClient.getQueryEditor().parseQuery(queryText);
    for (const cid of customerId) {
        const res = await executor.executeQueryAndParse(query, cid);
        let ids = res.rows.map((row) => row[0].toString());
        all_ids.push(...ids);
    }
    return all_ids;
}
exports.getCustomerIds = getCustomerIds;
/**
 * Filter customers with a query.
 * @param adsClient Ads client
 * @param ids a list of customer ids to filter
 * @param customer_ids_query a query
 * @returns a filtered list of customer ids
 */
async function filterCustomerIds(adsClient, ids, customer_ids_query) {
    let query = adsClient.getQueryEditor().parseQuery(customer_ids_query);
    let accounts = new Set();
    let idx = 0;
    const executor = new ads_query_executor_1.AdsQueryExecutor(adsClient);
    for (let id of ids) {
        let result = await executor.executeQueryAndParse(query, id);
        if (result.rowCount > 0) {
            for (let row of result.rows) {
                accounts.add(row[0]);
            }
        }
        idx++;
        // TODO: purge Customer objects in IGoogleAdsApiClient
    }
    return Array.from(accounts);
}
exports.filterCustomerIds = filterCustomerIds;
//# sourceMappingURL=ads-utils.js.map