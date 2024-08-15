"use strict";
/**
 * Copyright 2024 Google LLC
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
 * Cloud Function 'gaarf' - executes Ads query (suplied either via body or as a GCS path) and writes data to BigQuery (or other writer)
 * arguments:
 *  - (required) ads config - different sources are supported, see `getAdsConfig` function
 *  - writer - writer to use: "bq", "json", "csv". By default - "bq" (BigQuery)
 *  - bq_dataset - (can be taken from envvar DATASET) output BQ dataset id
 *  - bq_project_id - BigQuery project id, be default the current project is used
 *  - customer_id - Ads customer id (a.k.a. CID), can be taken from google-ads.yaml if specified
 *  - expand_mcc - true to expand account in `customer_id` argument. By default (if fale) it also disables creating union views.
 *  - bq_dataset_location - BigQuery dataset location ('us' or 'europe'), optional, by default 'us' is used
 *  - output_path - output path for interim data (for BigQueryWriter) or generated data (Csv/Json writers)
 */
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const utils_1 = require("./utils");
const logger_1 = require("./logger");
function getQueryWriter(req, projectId) {
    var _a, _b, _c, _d, _e, _f;
    const body = req.body || {};
    if (!req.query.writer || req.query.writer === 'bq') {
        const bqWriterOptions = {
            datasetLocation: req.query.bq_dataset_location,
            arrayHandling: (_a = body.writer_options) === null || _a === void 0 ? void 0 : _a.array_handling,
            arraySeparator: (_b = body.writer_options) === null || _b === void 0 ? void 0 : _b.array_separator,
            outputPath: req.query.output_path,
            noUnionView: true,
        };
        if (req.query.expand_mcc) {
            bqWriterOptions.noUnionView = false;
        }
        const dataset = req.query.bq_dataset || process.env.DATASET;
        if (!dataset)
            throw new Error("Dataset is not specified in either 'bq_dataset' query argument or DATASET envvar");
        const writer = new google_ads_api_report_fetcher_1.BigQueryWriter(projectId, dataset, bqWriterOptions);
        return writer;
    }
    if (req.query.writer === 'csv') {
        const options = {
            quoted: (_c = body.writer_options) === null || _c === void 0 ? void 0 : _c.quoted,
            arraySeparator: (_d = body.writer_options) === null || _d === void 0 ? void 0 : _d.array_separator,
            outputPath: req.query.output_path || `gs://${projectId}/tmp`,
        };
        return new google_ads_api_report_fetcher_1.CsvWriter(options);
    }
    if (req.query.writer === 'json') {
        const options = {
            format: (_e = body.writer_options) === null || _e === void 0 ? void 0 : _e.format,
            valueFormat: (_f = body.writer_options) === null || _f === void 0 ? void 0 : _f.value_format,
            outputPath: req.query.output_path || `gs://${projectId}/tmp`,
        };
        return new google_ads_api_report_fetcher_1.JsonWriter(options);
    }
}
async function main_unsafe(req, res, projectId, logger, functionName) {
    // prepare Ads API parameters
    const adsConfig = await (0, utils_1.getAdsConfig)(req);
    projectId =
        req.query.bq_project_id || process.env.PROJECT_ID || projectId;
    const customerId = req.query.customer_id || adsConfig.customer_id;
    if (!customerId)
        throw new Error("Customer id is not specified in either 'customer_id' query argument or google-ads.yaml");
    if (!adsConfig.login_customer_id) {
        adsConfig.login_customer_id = customerId;
    }
    let adsClient;
    if (req.query.api === 'rest') {
        const apiVersion = req.query.apiVersion;
        adsClient = new google_ads_api_report_fetcher_1.GoogleAdsRestApiClient(adsConfig, apiVersion);
    }
    else {
        adsClient = new google_ads_api_report_fetcher_1.GoogleAdsRpcApiClient(adsConfig);
    }
    const { queryText, scriptName } = await (0, utils_1.getScript)(req, logger);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
    const { refresh_token, developer_token, ...ads_config_wo_token } = (adsConfig);
    ads_config_wo_token['ApiVersion'] = adsClient.apiVersion;
    logger.info(`Running Cloud Function ${functionName}, Ads API ${adsClient.apiType} ${adsClient.apiVersion}, ${req.query.expand_mcc
        ? 'with MCC expansion (MCC=' + customerId + ')'
        : 'CID=' + customerId}, see Ads API config in metadata field`, {
        adsConfig: ads_config_wo_token,
        scriptName,
        customerId,
        request: { body: req.body, query: req.query },
    });
    let customers;
    if (req.query.expand_mcc) {
        customers = await (0, google_ads_api_report_fetcher_1.getCustomerIds)(adsClient, customerId);
        logger.info(`[${scriptName}] Customers to process (${customers.length})`, {
            customerId,
            scriptName,
            customers,
        });
    }
    else {
        customers = [customerId];
    }
    const executor = new google_ads_api_report_fetcher_1.AdsQueryExecutor(adsClient);
    const writer = getQueryWriter(req, projectId);
    const result = await executor.execute(scriptName, queryText, customers, req.body.macro, writer);
    logger.info(`Cloud Function ${functionName} compeleted`, {
        customerId,
        scriptName,
        result,
    });
    // we're returning a map of customer to number of rows
    res.json(result);
    res.end();
}
const main = async (req, res) => {
    const dumpMemory = !!(req.query.dump_memory || process.env.DUMP_MEMORY);
    const projectId = await (0, utils_1.getProject)();
    const functionName = process.env.K_SERVICE || 'gaarf';
    const logger = (0, logger_1.createLogger)(req, projectId, functionName);
    let dispose;
    if (dumpMemory) {
        logger.info((0, google_ads_api_report_fetcher_1.getMemoryUsage)('Start'));
        dispose = (0, utils_1.startPeriodicMemoryLogging)(logger, 60000);
    }
    try {
        await main_unsafe(req, res, projectId, logger, functionName);
    }
    catch (e) {
        console.error(e);
        logger.error(e.message, {
            error: e,
            body: req.body,
            query: req.query,
        });
        res.status(500).send(e.message).end();
    }
    finally {
        if (dumpMemory) {
            if (dispose)
                dispose();
            logger.info((0, google_ads_api_report_fetcher_1.getMemoryUsage)('End'));
        }
    }
};
exports.main = main;
//# sourceMappingURL=gaarf.js.map