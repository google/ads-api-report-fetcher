"use strict";
/**
 * Copyright 2022 Google LLC
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.GoogleAdsApiClient = void 0;
const google_ads_api_1 = require("google-ads-api");
class GoogleAdsApiClient {
    constructor(adsConfig, customerId) {
        var _a;
        if (!adsConfig) {
            throw new Error('GoogleAdsApiConfig instance was not passed');
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
            developer_token: adsConfig.developer_token
        });
        this.customers = {};
        this.customers[customerId] = this.client.Customer({
            customer_id: customerId,
            login_customer_id: adsConfig.login_customer_id,
            refresh_token: adsConfig.refresh_token
        });
        // also put the customer as the default one
        this.customers[''] = this.customers[customerId];
        this.isChildCustomer =
            customerId !== ((_a = adsConfig.login_customer_id) === null || _a === void 0 ? void 0 : _a.toString());
    }
    async executeQuery(query, customerId) {
        let customer;
        if (!customerId) {
            customer = this.customers[''];
        }
        else {
            customer = this.customers[customerId];
            if (!customer) {
                customer = this.client.Customer({
                    customer_id: customerId,
                    login_customer_id: this.ads_cfg.login_customer_id,
                    refresh_token: this.ads_cfg.refresh_token
                });
                this.customers[customerId] = customer;
            }
        }
        try {
            return await customer.query(query);
        }
        catch (e) {
            let error = e;
            if (error.errors)
                console.log(`An error occured on executing query: ` +
                    JSON.stringify(error.errors[0], null, 2));
            throw e;
        }
    }
    async getCustomerIds() {
        var _a;
        if (this.isChildCustomer) {
            return Object.keys(this.customers).filter(k => !!k);
        }
        const query_customer_ids = `SELECT
          customer_client.id,
          customer_client.manager
        FROM customer_client`;
        let rows = await this.executeQuery(query_customer_ids);
        let ids = [];
        for (let row of rows) {
            if (row.customer_client && !((_a = row.customer_client) === null || _a === void 0 ? void 0 : _a.manager)) {
                ids.push(row.customer_client.id);
            }
        }
        return ids;
    }
}
exports.GoogleAdsApiClient = GoogleAdsApiClient;
//# sourceMappingURL=ads-api-client.js.map