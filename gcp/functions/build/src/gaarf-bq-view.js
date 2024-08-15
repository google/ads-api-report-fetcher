"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.main_bq_view = void 0;
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
const node_path_1 = __importDefault(require("node:path"));
const google_ads_api_report_fetcher_1 = require("google-ads-api-report-fetcher");
const logger_1 = require("./logger");
const utils_1 = require("./utils");
async function main_bq_view_unsafe(req, res, projectId, logger) {
    console.log(req.body);
    console.log(req.query);
    const body = req.body || {};
    const datasetId = req.query.dataset || body.dataset;
    const accounts = body.accounts;
    let tableId = body.table || req.query.table;
    // note: projectId isn't mandatory (should be detected from ADC)
    const options = {
        datasetLocation: req.query.dataset_location || body.dataset_location,
    };
    const executor = new google_ads_api_report_fetcher_1.BigQueryExecutor(projectId, options);
    const scriptPath = body.script_path || req.query.script_path;
    if (scriptPath && !tableId) {
        tableId = node_path_1.default.basename(scriptPath).split('.sql')[0];
    }
    await logger.info(`Creating an unified view ${datasetId}.${tableId} for ${accounts.length} accounts`);
    await executor.createUnifiedView(datasetId, tableId, accounts);
    res.sendStatus(200);
    res.end();
}
const main_bq_view = async (req, res) => {
    const dumpMemory = !!(req.query.dump_memory || process.env.DUMP_MEMORY);
    const projectId = await (0, utils_1.getProject)();
    const logger = (0, logger_1.createLogger)(req, projectId, process.env.K_SERVICE || 'gaarf-bq');
    logger.info('request', { body: req.body, query: req.query });
    let dispose;
    if (dumpMemory) {
        logger.info((0, google_ads_api_report_fetcher_1.getMemoryUsage)('Start'));
        dispose = (0, utils_1.startPeriodicMemoryLogging)(logger, 60000);
    }
    try {
        await main_bq_view_unsafe(req, res, projectId, logger);
    }
    catch (e) {
        console.error(e);
        logger.error(e.message, { error: e });
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
exports.main_bq_view = main_bq_view;
//# sourceMappingURL=gaarf-bq-view.js.map