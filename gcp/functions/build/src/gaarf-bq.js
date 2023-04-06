"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.main_bq = void 0;
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
async function main_bq_unsafe(req, res, projectId, logger) {
    const options = {
        datasetLocation: req.query.dataset_location,
    };
    const { queryText, scriptName } = await (0, utils_1.getScript)(req, logger);
    const executor = new google_ads_api_report_fetcher_1.BigQueryExecutor(projectId, options);
    const body = req.body || {};
    const sqlParams = body.sql;
    const macroParams = body.macro;
    const result = await executor.execute(scriptName, queryText, {
        sqlParams,
        macroParams,
    });
    if (result && result.length) {
        res.json({ rowCount: result.length });
    }
    else {
        res.sendStatus(200);
    }
}
const main_bq = async (req, res) => {
    const projectId = await (0, utils_1.getProject)();
    const logger = (0, logger_1.createLogger)(req, projectId, process.env.K_SERVICE || 'gaarf-bq');
    await logger.info('request', { body: req.body, query: req.query });
    try {
        await main_bq_unsafe(req, res, projectId, logger);
    }
    catch (e) {
        await logger.error(e.message, e);
        res.status(500).send(e.message).end();
    }
};
exports.main_bq = main_bq;
//# sourceMappingURL=gaarf-bq.js.map