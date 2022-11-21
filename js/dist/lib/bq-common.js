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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getDataset = exports.OAUTH_SCOPES = void 0;
const logger_1 = __importDefault(require("./logger"));
exports.OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/cloud-platform.read-only",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/bigquery.readonly",
];
async function getDataset(bigquery, datasetId, datasetLocation) {
    let dataset;
    const options = {
        location: datasetLocation,
    };
    try {
        dataset = bigquery.dataset(datasetId, options);
        await dataset.get({ autoCreate: true });
    }
    catch (e) {
        logger_1.default.error(`Failed to get or create the dataset ${datasetId}`);
        throw e;
    }
    return dataset;
}
exports.getDataset = getDataset;
//# sourceMappingURL=bq-common.js.map