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

import {AdsQueryEditor, IAdsQueryEditor} from './ads-query-editor.js';
import {AdsRowParser, IAdsRowParser} from './ads-row-parser.js';
import {
  IAdsApiSchema,
  AdsApiSchemaRest,
  AdsApiDefaultVersion,
} from './ads-api-schema-base.js';
import {getLogger, ILogger} from './logger.js';

/**
 * Google Ads API abstraction.
 */
export interface IGoogleAdsApiClient {
  get apiVersion(): string;
  getQueryEditor(): IAdsQueryEditor;
  getRowParser(): IAdsRowParser;
  executeQueryStream(
    query: string,
    customerId: string,
  ): AsyncGenerator<Record<string, unknown>>;
  executeQuery(
    query: string,
    customerId: string,
  ): Promise<Array<Record<string, unknown>>>;
}

export type GoogleAdsApiConfig = {
  client_id?: string;
  client_secret?: string;
  developer_token: string;
  refresh_token?: string;
  login_customer_id?: string;
  linked_customer_id?: string;
  customer_id?: string[] | string;
  json_key_file_path?: string;
};

export class GoogleAdsError extends Error {
  query?: string;
  account?: string;
  retryable: boolean;
  logged = false;

  constructor(message: string | null | undefined) {
    super(message || 'Unknown error on calling Google Ads API occurred');
    this.retryable = false;
  }
}

/**
 * Base class for Google Ads API clients.
 */
export abstract class GoogleAdsApiClientBase implements IGoogleAdsApiClient {
  adsConfig: GoogleAdsApiConfig;
  apiVersion: string;
  logger: ILogger;
  schema: IAdsApiSchema;

  constructor(adsConfig: GoogleAdsApiConfig, schema: IAdsApiSchema) {
    if (!adsConfig) {
      throw new Error('GoogleAdsApiConfig instance was not passed');
    }
    this.adsConfig = adsConfig;
    this.logger = getLogger();
    this.apiVersion = schema.version;
    this.schema = schema;
  }

  getQueryEditor(): IAdsQueryEditor {
    return new AdsQueryEditor(this.schema);
  }

  getRowParser(): IAdsRowParser {
    return new AdsRowParser(this.logger);
  }

  abstract executeQueryStream(
    query: string,
    customerId: string,
  ): AsyncGenerator<Record<string, unknown>>;

  abstract executeQuery(
    query: string,
    customerId: string,
  ): Promise<Array<Record<string, unknown>>>;
}
