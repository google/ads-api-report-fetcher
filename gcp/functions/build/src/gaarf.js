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
exports.main = void 0;
/**
 * Cloud Function 'gaarf' - executes Ads query (suplied either via body or as gcs path) and writes data to BigQuery
 * arguments:
 *  - (required) ads config - different sources are supported, see `getAdsConfig` fucntion
 *  - (required) bq_dataset - (can be taken from envvar DATASET) output BQ dataset id
 *  - bq_project_id - BigQuery project id, be default the current project is used
 *  - customer_id - Ads customer id (a.k.a. CID), can be taken from google-ads.yaml if specified
 *  - single_customer - true for skipping loading of subaccount, assuming the supplied CID is a leaf one (not MCC)
 *  - bq_dataset_location - BigQuery dataset location ('us' or 'europe'), optional, by default 'us' is used
 */
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const utils_1 = require("./utils");
const logger_1 = require("./logger");
async function main_unsafe(req, res, projectId, logger) {
    var _a, _b;
    // prepare Ads API parameters
    const adsConfig = await (0, utils_1.getAdsConfig)(req);
    const { refresh_token, ...ads_config_wo_token } = adsConfig;
    await logger.info('Ads API config', ads_config_wo_token);
    projectId =
        req.query.bq_project_id || process.env.PROJECT_ID || projectId;
    const dataset = req.query.bq_dataset || process.env.DATASET;
    if (!dataset)
        throw new Error("Dataset is not specified in either 'bq_dataset' query argument or DATASET envvar");
    const customerId = req.query.customer_id || adsConfig.customer_id;
    if (!customerId)
        throw new Error("Customer id is not specified in either 'customer_id' query argument or google-ads.yaml");
    const ads_client = new google_ads_api_report_fetcher_1.GoogleAdsApiClient(adsConfig, customerId);
    // TODO: support CsvWriter and output path to GCS
    // (csv.destination_folder=gs://bucket/path)
    const singleCustomer = req.query.single_customer;
    const body = req.body || {};
    const macroParams = body.macro;
    const bq_writer_options = {
        datasetLocation: req.query.bq_dataset_location,
        arrayHandling: (_a = body.bq_writer_options) === null || _a === void 0 ? void 0 : _a.array_handling,
        arraySeparator: (_b = body.bq_writer_options) === null || _b === void 0 ? void 0 : _b.array_separator,
    };
    const { queryText, scriptName } = await (0, utils_1.getScript)(req, logger);
    let customers;
    if (singleCustomer) {
        await logger.info(`[${scriptName}] Executing for a single customer id: ${customerId}`, { scriptName, customerId });
        customers = [customerId];
        bq_writer_options.noUnionView = true;
    }
    else {
        await logger.info(`[${scriptName}] Fetching customer ids`, {
            customerId,
            scriptName,
        });
        customers = await ads_client.getCustomerIds();
        await logger.info(`[${scriptName}] Customers to process (${customers.length})`, {
            customerId,
            scriptName,
            customers,
        });
    }
    const executor = new google_ads_api_report_fetcher_1.AdsQueryExecutor(ads_client);
    const writer = new google_ads_api_report_fetcher_1.BigQueryWriter(projectId, dataset, bq_writer_options);
    const result = await executor.execute(scriptName, queryText, customers, macroParams, writer);
    await logger.info('Cloud Function gaarf compeleted', {
        customerId,
        scriptName,
        result,
    });
    // we're returning a map of customer to number of rows
    res.json(result);
    res.end();
}
const main = async (req, res) => {
    const projectId = await (0, utils_1.getProject)();
    console.log(`Project: ${projectId}`);
    const logger = (0, logger_1.createLogger)(req, projectId, process.env.K_SERVICE || 'gaarf');
    await logger.info('request', { body: req.body, query: req.query });
    try {
        await main_unsafe(req, res, projectId, logger);
    }
    catch (e) {
        await logger.error(e.message, e);
        res.status(500).send(e.message).end();
    }
};
exports.main = main;
//# sourceMappingURL=gaarf.js.map