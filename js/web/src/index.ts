/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Browser process shim for safe runtime execution
 */
if (typeof globalThis !== 'undefined' && typeof (globalThis as any).process === 'undefined') {
  (globalThis as any).process = {
    env: {},
    nextTick: (fn: (...args: any[]) => void, ...args: any[]) => setTimeout(() => fn(...args), 0),
    memoryUsage: () => ({ heapUsed: 0, heapTotal: 0, external: 0, rss: 0 }),
    stdout: { isTTY: false, columns: 80 },
  };
}

// Core API Client
export {
  GoogleAdsApiClientBase,
  GoogleAdsError,
} from '../../src/lib/ads-api-client-base.js';
export type {
  GoogleAdsApiConfig,
  IGoogleAdsApiClient,
} from '../../src/lib/ads-api-client-base.js';
export {GoogleAdsApiClient} from '../../src/lib/ads-api-client-rest.js';

// Query Editor & Executor
export {AdsQueryEditor} from '../../src/lib/ads-query-editor.js';
export type {IAdsQueryEditor} from '../../src/lib/ads-query-editor.js';
export {AdsQueryExecutor} from '../../src/lib/ads-query-executor.js';
export type {
  AdsQueryParams,
  AdsQueryExecutorOptions,
} from '../../src/lib/ads-query-executor.js';
export {AdsRowParser, ParseResultMode} from '../../src/lib/ads-row-parser.js';
export type {IAdsRowParser} from '../../src/lib/ads-row-parser.js';

// Schema
export {
  AdsApiSchemaRest,
  AdsApiDefaultVersion,
} from '../../src/lib/ads-api-schema-base.js';
export type {
  IAdsApiSchema,
  ISchemaLoader,
} from '../../src/lib/ads-api-schema-base.js';
export {
  WebSchemaLoader,
  BundledSchemaLoader,
} from './lib/ads-api-schema-loader-web.js';
export type {WebSchemaLoaderOptions} from './lib/ads-api-schema-loader-web.js';

// Types & Customizers
export {
  QueryElements,
  CustomizerType,
  FieldTypeKind,
  ArrayHandling,
  isEnumType,
} from '../../src/lib/types.js';
export type {
  QueryResult,
  QueryObjectResult,
  Column,
  FieldType,
  Customizer,
  CustomizerResourceIndex,
  CustomizerSelector,
  CustomizerFunction,
  CustomizerVirtualColumn,
  ResourceInfo,
  IResultWriter,
  InputQuery,
  IQueryReader,
  ProtoFieldMeta,
  ProtoTypeMeta,
  ProtoEnumMeta,
} from '../../src/lib/types.js';

// Utilities
export {
  substituteMacros,
  renderTemplate,
  executeWithRetry,
  formatDateISO,
  snakeToCamelCase,
  camelToSnakeCase,
  navigateObject,
  traverseObject,
  tryParseNumber,
  delay,
  getElapsed,
} from '../../src/lib/utils.js';

export {
  getCustomerIds,
  getCustomerInfo,
  parseCustomerIds,
  filterCustomerIds,
} from '../../src/lib/ads-utils.js';
export type {CustomerInfo} from '../../src/lib/ads-utils.js';

// Logging
export {getLogger, setLogListener} from './lib/stubs/logger.js';
export type {ILogger, LogListener} from './lib/stubs/logger.js';

// Builtins
export {BuiltinQueryProcessor} from '../../src/lib/builtins.js';

// Parser
export {parse, SyntaxError} from '../../src/lib/parser.js';

// Math Engine
export {mathjs} from '../../src/lib/math-engine.js';
