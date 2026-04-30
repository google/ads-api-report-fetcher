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
import { AdsQueryEditor } from './ads-query-editor.js';
import { AdsRowParser } from './ads-row-parser.js';
import { getLogger } from './logger.js';
export class GoogleAdsError extends Error {
    constructor(message) {
        super(message || 'Unknown error on calling Google Ads API occurred');
        this.logged = false;
        this.retryable = false;
    }
}
/**
 * Base class for Google Ads API clients.
 */
export class GoogleAdsApiClientBase {
    constructor(adsConfig, schema) {
        if (!adsConfig) {
            throw new Error('GoogleAdsApiConfig instance was not passed');
        }
        this.adsConfig = adsConfig;
        this.logger = getLogger();
        this.apiVersion = schema.version;
        this.schema = schema;
    }
    getQueryEditor() {
        return new AdsQueryEditor(this.schema);
    }
    getRowParser() {
        return new AdsRowParser(this.logger);
    }
}
//# sourceMappingURL=ads-api-client-base.js.map