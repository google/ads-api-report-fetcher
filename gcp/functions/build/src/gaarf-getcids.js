"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.main_getcids = void 0;
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
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const utils_1 = require("./utils");
const logger_1 = require("./logger");
const DEFAULT_BATCH_SIZE = 500;
async function main_getcids_unsafe(req, res, logger) {
    // prepare Ads API parameters
    const adsConfig = await (0, utils_1.getAdsConfig)(req);
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { refresh_token, ...ads_config_wo_token } = adsConfig;
    await logger.info('Ads API config', ads_config_wo_token);
    let customerIds = (0, google_ads_api_report_fetcher_1.parseCustomerIds)(req.query.customer_id, adsConfig);
    if (!customerIds || customerIds.length === 0) {
        throw new Error("Customer id is not specified in either 'customer_id' query argument or google-ads.yaml");
    }
    if (!adsConfig.login_customer_id && customerIds && customerIds.length === 1) {
        adsConfig.login_customer_id = customerIds[0];
    }
    const adsClient = new google_ads_api_report_fetcher_1.GoogleAdsApiClient(adsConfig);
    customerIds = await adsClient.getCustomerIds(customerIds);
    let customer_ids_query = '';
    if (req.body && req.body.customer_ids_query) {
        customer_ids_query = req.body.customer_ids_query;
    }
    else if (req.query.customer_ids_query) {
        customer_ids_query = await (0, google_ads_api_report_fetcher_1.getFileContent)(req.query.customer_ids_query);
    }
    if (customer_ids_query) {
        await logger.info(`Fetching customer id using custom query: ${customer_ids_query}`);
        const executor = new google_ads_api_report_fetcher_1.AdsQueryExecutor(adsClient);
        customerIds = await executor.getCustomerIds(customerIds, customer_ids_query);
        await logger.info(`Loaded ${customerIds.length} accounts`);
    }
    customerIds = customerIds || [];
    customerIds.sort();
    // now we have a final list of accounts (customerIds)
    let batchSize = DEFAULT_BATCH_SIZE;
    if (req.query.customer_ids_batchsize) {
        batchSize = Number(req.query.customer_ids_batchsize);
        if (isNaN(batchSize)) {
            throw new Error('customer_ids_batchsize should be a number');
        }
    }
    if (req.query.customer_ids_offset) {
        // extract a subset of CIDs if offset is specified
        const offset = Number(req.query.customer_ids_offset);
        if (isNaN(offset)) {
            throw new Error('customer_ids_offset should be a number');
        }
        const cids_length = customerIds.length;
        customerIds = customerIds.slice(offset, offset + batchSize);
        if (cids_length !== customerIds.length) {
            await logger.info(`Reshaped customer ids array from ${cids_length} to ${customerIds.length} items`);
        }
    }
    if (req.query.flatten) {
        res.json(customerIds);
        res.end();
    }
    else {
        // otherwise, by default we'll return CIDs grouped in batches:
        //    [1, 2, ..., 10_000] => [ [1, 2, ..., 5_000], [5_001, 10_000], ]
        const customerIdsBatched = (0, utils_1.splitIntoChunks)(customerIds, batchSize);
        res.json({
            batchCount: customerIdsBatched.length,
            batchSize: batchSize,
            accounts: customerIdsBatched,
        });
        res.end();
    }
}
const main_getcids = async (req, res) => {
    const projectId = await (0, utils_1.getProject)();
    const logger = (0, logger_1.createLogger)(req, projectId, process.env.K_SERVICE || 'gaarf-getcids');
    await logger.info('request', { body: req.body, query: req.query });
    try {
        await main_getcids_unsafe(req, res, logger);
    }
    catch (e) {
        await logger.error(e.message, { error: e });
        res.status(500).send(e.message).end();
    }
};
exports.main_getcids = main_getcids;
//# sourceMappingURL=gaarf-getcids.js.map