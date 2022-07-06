"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
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
const path_1 = __importDefault(require("path"));
const main_bq = async (req, res) => {
    console.log(req.query);
    let scriptPath = req.query.script_path;
    if (!scriptPath)
        throw new Error(`Ads script path is not specified in script_path query argument`);
    let projectId = req.query.project_id || process.env.PROJECT_ID;
    if (!projectId)
        throw new Error(`Project id is not specified in either 'project_id' query argument or PROJECT_ID envvar`);
    let target = req.query.target;
    let body = req.body || {};
    let sqlParams = body.sql;
    let macroParams = body.macros;
    let queryText = await (0, google_ads_api_report_fetcher_1.getFileContent)(scriptPath);
    let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
    let executor = new google_ads_api_report_fetcher_1.BigQueryExecutor(projectId);
    console.log(`Executing BQ-query from ${scriptPath}`);
    let result = await executor.execute(scriptName, queryText, { sqlParams, macroParams, target });
    if (result && result.length) {
        res.send({ rowCount: result.length });
    }
    else {
        res.sendStatus(200);
    }
};
exports.main_bq = main_bq;
//# sourceMappingURL=gaarf-bq.js.map