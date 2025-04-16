/**
 * Copyright 2025 Google LLC
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
/**
 * Cloud Function 'gaarf' - executes Ads query (supplied either via body or as a GCS path) and writes data to BigQuery (or other writer)
 * arguments:
 *  - (required) ads config - different sources are supported, see `getAdsConfig` function
 *  - writer - writer to use: "bq", "json", "csv". By default - "bq" (BigQuery)
 *  - bq_dataset - (can be taken from envvar DATASET) output BQ dataset id
 *  - bq_project_id - BigQuery project id, be default the current project is used
 *  - customer_id - Ads customer id (a.k.a. CID), can be taken from google-ads.yaml if specified
 *  - expand_mcc - true to expand account in `customer_id` argument. By default (if false) it also disables creating union views.
 *  - bq_dataset_location - BigQuery dataset location ('us' or 'europe'), optional, by default 'us' is used
 *  - output_path - output path for interim data (for BigQueryWriter) or generated data (Csv/Json writers)
 */
import { AdsQueryExecutor, BigQueryWriter, GoogleAdsRpcApiClient, getMemoryUsage, getCustomerIds, GoogleAdsRestApiClient, CsvWriter, JsonWriter, } from 'google-ads-api-report-fetcher';
import { getAdsConfig, getProject, getScript, startPeriodicMemoryLogging, } from './utils.js';
import { createLogger } from './logger.js';
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
        const writer = new BigQueryWriter(projectId, dataset, bqWriterOptions);
        return writer;
    }
    if (req.query.writer === 'csv') {
        const options = {
            quoted: (_c = body.writer_options) === null || _c === void 0 ? void 0 : _c.quoted,
            arraySeparator: (_d = body.writer_options) === null || _d === void 0 ? void 0 : _d.array_separator,
            outputPath: req.query.output_path || `gs://${projectId}/tmp`,
        };
        return new CsvWriter(options);
    }
    if (req.query.writer === 'json') {
        const options = {
            format: (_e = body.writer_options) === null || _e === void 0 ? void 0 : _e.format,
            valueFormat: (_f = body.writer_options) === null || _f === void 0 ? void 0 : _f.value_format,
            outputPath: req.query.output_path || `gs://${projectId}/tmp`,
        };
        return new JsonWriter(options);
    }
}
async function main_unsafe(req, res, projectId, logger, functionName) {
    var _a;
    // prepare Ads API parameters
    const adsConfig = await getAdsConfig(req);
    projectId =
        req.query.bq_project_id || process.env.PROJECT_ID || projectId;
    const customerId = req.query.customer_id || adsConfig.customer_id;
    if (!customerId)
        throw new Error("Customer id is not specified in either 'customer_id' query argument or google-ads.yaml");
    if (!adsConfig.login_customer_id) {
        adsConfig.login_customer_id = (req.query.root_cid || customerId);
    }
    let adsClient;
    if (((_a = req.query.api) === null || _a === void 0 ? void 0 : _a.toLocaleLowerCase()) === 'rest') {
        const apiVersion = req.query.api_version;
        adsClient = new GoogleAdsRestApiClient(adsConfig, apiVersion);
    }
    else {
        adsClient = new GoogleAdsRpcApiClient(adsConfig);
    }
    const { queryText, scriptName } = await getScript(req, logger);
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
        customers = await getCustomerIds(adsClient, customerId);
        logger.info(`[${scriptName}] Customers to process (${customers.length})`, {
            customerId,
            scriptName,
            customers,
        });
    }
    else {
        customers = [customerId];
    }
    const executor = new AdsQueryExecutor(adsClient);
    const writer = getQueryWriter(req, projectId);
    logger.info(`Starting executing script via Gaarf (${adsClient.apiType})`, {
        customers,
        scriptName,
        queryText,
        macro: req.body.macro,
        templateParams: req.body.templateParams,
    });
    const result = await executor.execute(scriptName, queryText, customers, { macros: req.body.macro, templateParams: req.body.templateParams }, writer);
    logger.info(`Cloud Function ${functionName} completed`, {
        customerId,
        scriptName,
        result,
    });
    // we're returning a map of customer to number of rows
    res.json(result);
    res.end();
}
export const main = async (req, res) => {
    const dumpMemory = !!(req.query.dump_memory || process.env.DUMP_MEMORY);
    const projectId = await getProject();
    const functionName = process.env.K_SERVICE || 'gaarf';
    const logger = createLogger(req, projectId, functionName);
    logger.info('request', { body: req.body, query: req.query });
    let dispose;
    if (dumpMemory) {
        logger.info(getMemoryUsage('Start'));
        dispose = startPeriodicMemoryLogging(logger, 60000);
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
            logger.info(getMemoryUsage('End'));
        }
    }
};
//# sourceMappingURL=gaarf.js.map