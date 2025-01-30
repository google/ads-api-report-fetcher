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
export * from './lib/ads-api-client.js';
export * from './lib/ads-query-editor.js';
export * from './lib/ads-query-executor.js';
export * from './lib/bq-executor.js';
export * from './lib/bq-writer.js';
export * from './lib/bq-common.js';
export * from './lib/file-writers.js';
export * from './lib/file-utils.js';
export * from './lib/google-cloud.js';
export * from './lib/types.js';
export * from './lib/logger.js';
export * from './lib/logger-factory.js';
export * from './lib/utils.js';
export * from './lib/ads-utils.js';
// for backward-compatibility
export { GoogleAdsRpcApiClient as GoogleAdsApiClient } from './lib/ads-api-client.js';
export { loadAdsConfigFromFile as loadAdsConfigYaml } from './lib/ads-utils.js';
//# sourceMappingURL=index.js.map