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
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CsvWriter = exports.BigQueryWriter = exports.BigQueryExecutor = exports.AdsQueryExecutor = exports.AdsQueryEditor = exports.loadAdsConfigYaml = exports.GoogleAdsApiClient = void 0;
var ads_api_client_1 = require("./lib/ads-api-client");
Object.defineProperty(exports, "GoogleAdsApiClient", { enumerable: true, get: function () { return ads_api_client_1.GoogleAdsApiClient; } });
Object.defineProperty(exports, "loadAdsConfigYaml", { enumerable: true, get: function () { return ads_api_client_1.loadAdsConfigYaml; } });
var ads_query_editor_1 = require("./lib/ads-query-editor");
Object.defineProperty(exports, "AdsQueryEditor", { enumerable: true, get: function () { return ads_query_editor_1.AdsQueryEditor; } });
var ads_query_executor_1 = require("./lib/ads-query-executor");
Object.defineProperty(exports, "AdsQueryExecutor", { enumerable: true, get: function () { return ads_query_executor_1.AdsQueryExecutor; } });
var bq_executor_1 = require("./lib/bq-executor");
Object.defineProperty(exports, "BigQueryExecutor", { enumerable: true, get: function () { return bq_executor_1.BigQueryExecutor; } });
var bq_writer_1 = require("./lib/bq-writer");
Object.defineProperty(exports, "BigQueryWriter", { enumerable: true, get: function () { return bq_writer_1.BigQueryWriter; } });
var csv_writer_1 = require("./lib/csv-writer");
Object.defineProperty(exports, "CsvWriter", { enumerable: true, get: function () { return csv_writer_1.CsvWriter; } });
__exportStar(require("./lib/file-utils"), exports);
__exportStar(require("./lib/google-cloud"), exports);
__exportStar(require("./lib/types"), exports);
//# sourceMappingURL=index.js.map