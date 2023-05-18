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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.loadAdsConfigFromFile = exports.GoogleAdsApiClient = void 0;
const google_ads_api_1 = require("google-ads-api");
const js_yaml_1 = __importDefault(require("js-yaml"));
const file_utils_1 = require("./file-utils");
const logger_1 = require("./logger");
class GoogleAdsApiClient {
    constructor(adsConfig, customerId) {
        if (!adsConfig) {
            throw new Error("GoogleAdsApiConfig instance was not passed");
        }
        customerId = customerId || adsConfig.customer_id;
        if (!customerId) {
            throw new Error(`No customer id was specified`);
        }
        customerId = customerId === null || customerId === void 0 ? void 0 : customerId.toString();
        this.ads_cfg = adsConfig;
        this.client = new google_ads_api_1.GoogleAdsApi({
            client_id: adsConfig.client_id,
            client_secret: adsConfig.client_secret,
            developer_token: adsConfig.developer_token,
        });
        this.customers = {};
        this.customers[customerId] = this.client.Customer({
            customer_id: customerId,
            login_customer_id: adsConfig.login_customer_id,
            refresh_token: adsConfig.refresh_token,
        });
        // also put the customer as the default one
        this.customers[""] = this.customers[customerId];
        this.root_cid = customerId;
        this.logger = (0, logger_1.getLogger)();
    }
    getCustomer(customerId) {
        let customer;
        if (!customerId) {
            customer = this.customers[""];
        }
        else {
            customer = this.customers[customerId];
            if (!customer) {
                customer = this.client.Customer({
                    customer_id: customerId,
                    login_customer_id: this.ads_cfg.login_customer_id,
                    refresh_token: this.ads_cfg.refresh_token,
                });
                this.customers[customerId] = customer;
            }
        }
        return customer;
    }
    handleGoogleAdsError(error, query) {
        if (error.errors)
            this.logger.error(`An error occured on executing query: ${query}\nError: ` +
                JSON.stringify(error.errors[0], null, 2));
    }
    async executeQuery(query, customerId) {
        const customer = this.getCustomer(customerId);
        try {
            return await customer.query(query);
        }
        catch (e) {
            this.handleGoogleAdsError(e, query);
            throw e;
        }
    }
    executeQueryStream(query, customerId) {
        const customer = this.getCustomer(customerId);
        try {
            return customer.queryStream(query);
        }
        catch (e) {
            this.handleGoogleAdsError(e, query);
            throw e;
        }
    }
    async getCustomerIds() {
        const query = `SELECT
          customer_client.id
        FROM customer_client
        WHERE
          customer_client.status = "ENABLED" AND
          customer_client.manager = False`;
        let rows = await this.executeQuery(query);
        let ids = rows.map((row) => row.customer_client.id);
        return ids;
    }
}
exports.GoogleAdsApiClient = GoogleAdsApiClient;
async function loadAdsConfigFromFile(configFilepath, customerId) {
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
            login_customer_id: (_a = (doc["login_customer_id"] || customerId)) === null || _a === void 0 ? void 0 : _a.toString(),
            customer_id: (_b = (customerId ||
                doc["customer_id"] ||
                doc["login_customer_id"])) === null || _b === void 0 ? void 0 : _b.toString(),
        };
    }
    catch (e) {
        throw new Error(`Failed to load Ads API configuration from ${configFilepath}: ${e}`);
    }
}
exports.loadAdsConfigFromFile = loadAdsConfigFromFile;
//# sourceMappingURL=ads-api-client.js.map