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
const main_bq = async (req, res) => {
    console.log(req.body);
    console.log(req.query);
    const projectId = req.query.project_id || process.env.PROJECT_ID;
    // note: projectId isn't mandatory (should be detected from ADC)
    const body = req.body || {};
    const sqlParams = body.sql;
    const macroParams = body.macro;
    const { queryText, scriptName } = await (0, utils_1.getScript)(req);
    const options = {
        datasetLocation: req.query.dataset_location,
    };
    const executor = new google_ads_api_report_fetcher_1.BigQueryExecutor(projectId, options);
    const result = await executor.execute(scriptName, queryText, {
        sqlParams,
        macroParams,
    });
    if (result && result.length) {
        res.send({ rowCount: result.length });
    }
    else {
        res.sendStatus(200);
    }
};
exports.main_bq = main_bq;
//# sourceMappingURL=gaarf-bq.js.map