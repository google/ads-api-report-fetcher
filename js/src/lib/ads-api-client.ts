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

import {Customer, errors, GoogleAdsApi} from 'google-ads-api';
import {GoogleAuth} from 'google-auth-library';
import {createRequire} from 'module';
const require = createRequire(import.meta.url);
// eslint-disable-next-line n/no-extraneous-require
const ads_protos = require('google-ads-node/build/protos/protos.json');
const protoRoot = ads_protos.nested.google.nested.ads.nested.googleads.nested;
const protoVer = Object.keys(protoRoot)[0]; // e.g. "v9"
import {executeWithRetry} from './utils.js';
import {getLogger, ILogger} from './logger.js';
import axios from 'axios';
import {ApiType} from './types.js';
import {AdsQueryEditor, IAdsQueryEditor} from './ads-query-editor.js';
import {AdsRowParser, IAdsRowParser} from './ads-row-parser.js';

/**
 * Google Ads API abstraction.
 */
export interface IGoogleAdsApiClient {
  /**
   * API enpoint type (REST or RPC).
   */
  get apiType(): ApiType;

  /**
   * Current API version.
   */
  get apiVersion(): string;

  /**
   * Return a query editor to parse query before execution.
   */
  getQueryEditor(): IAdsQueryEditor;

  /**
   * Return a row parser to parse API's response
   */
  getRowParser(): IAdsRowParser;

  /**
   * Execute a native GAQL query using streaming API.
   * @param query GAQL query (native)
   * @param customerId customer id
   */
  executeQueryStream(
    query: string,
    customerId: string
  ): AsyncGenerator<Record<string, unknown>>;

  /**
   * Execute a native GAQL query.
   * Result returned from API (depends on a client used) as is.
   * @param query GAQL query (native)
   * @param customerId customer id
   */
  executeQuery(
    query: string,
    customerId: string
  ): Promise<Record<string, unknown>[]>;
}

export type GoogleAdsApiConfig = {
  // ClientOptions:
  client_id?: string;
  client_secret?: string;
  developer_token: string;
  // CustomerOptions:
  refresh_token?: string;
  login_customer_id?: string;
  linked_customer_id?: string;
  customer_id?: string[] | string;
  json_key_file_path?: string;
};

export class GoogleAdsError extends Error {
  failure: errors.GoogleAdsFailure;
  query?: string;
  account?: string;
  retryable: boolean;
  logged = false;

  constructor(
    message: string | null | undefined,
    failure: errors.GoogleAdsFailure
  ) {
    super(message || 'Unknown error on calling Google Ads API occurred');
    this.failure = failure;
    this.retryable = false;
  }
}

/**
 * Base class for Google Ads API clients.
 */
export abstract class GoogleAdsApiClientBase implements IGoogleAdsApiClient {
  adsConfig: GoogleAdsApiConfig;
  _apiType: ApiType;
  apiVersion: string;
  logger: ILogger;

  constructor(
    adsConfig: GoogleAdsApiConfig,
    apiType: ApiType,
    apiVersion?: string
  ) {
    if (!adsConfig) {
      throw new Error('GoogleAdsApiConfig instance was not passed');
    }
    this.adsConfig = adsConfig;
    this._apiType = apiType;
    this.logger = getLogger();
    if (apiVersion && !apiVersion.startsWith('v')) {
      apiVersion = 'v' + apiVersion;
    }
    this.apiVersion = apiVersion || protoVer;
  }

  get apiType() {
    return this._apiType;
  }

  getQueryEditor(): IAdsQueryEditor {
    return new AdsQueryEditor(this.apiType, this.apiVersion);
  }

  getRowParser(): IAdsRowParser {
    return new AdsRowParser(this.apiType, this.logger);
  }

  abstract executeQueryStream(
    query: string,
    customerId: string
  ): AsyncGenerator<Record<string, unknown>>;

  abstract executeQuery(
    query: string,
    customerId: string
  ): Promise<Record<string, unknown>[]>;
}

/**
 * Google Ads API client using gRPC API (library opteo/google-ads-api).
 */
export class GoogleAdsRpcApiClient
  extends GoogleAdsApiClientBase
  implements IGoogleAdsApiClient
{
  client: GoogleAdsApi;
  customers: Record<string, Customer>;

  constructor(adsConfig: GoogleAdsApiConfig) {
    super(adsConfig, ApiType.gRPC);
    this.client = new GoogleAdsApi({
      client_id: adsConfig.client_id!,
      client_secret: adsConfig.client_secret!,
      developer_token: adsConfig.developer_token,
    });
    this.customers = {};
  }

  protected getCustomer(customerId: string): Customer {
    let customer: Customer;
    if (!customerId) {
      throw new Error('Customer id should be specified ');
    }
    customer = this.customers[customerId];
    if (!customer) {
      customer = this.client.Customer({
        customer_id: customerId,
        login_customer_id: this.adsConfig.login_customer_id,
        refresh_token: this.adsConfig.refresh_token!,
      });
      this.customers[customerId] = customer;
    }
    return customer;
  }

  protected handleGoogleAdsError(
    error: errors.GoogleAdsFailure | Error,
    customerId: string,
    query?: string
  ) {
    try {
      console.error(error);
      this.logger.error(
        `An error occurred on executing query (cid: ${customerId}): ${query}\nRaw error: ` +
          JSON.stringify(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (error as any).toJSON
              ? (<errors.GoogleAdsFailure>error).toJSON()
              : error,
            null,
            2
          ),
        {customerId, query}
      );
    } catch (e) {
      // a very unfortunate situation
      console.error(e);
      this.logger.error(
        `An error occurred on executing query and on logging it afterwards: ${query}\n.Logging error: ${e}`,
        {customerId, query, originalError: error}
      );
    }
    if (error instanceof errors.GoogleAdsFailure && error.errors) {
      const message = error.errors.length
        ? error.errors[0].message
        : // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (error as any).message || 'Unknown GoogleAdsFailure error';
      const ex = new GoogleAdsError(message, error);
      if (error.errors.length) {
        if (
          error.errors[0].error_code?.internal_error ||
          error.errors[0].error_code?.quota_error
        ) {
          ex.retryable = true;
        }
      } else {
        // it's an unknown error (no `errors` collection), it happens sometime
        // we'll treat such errors as retyable
        ex.retryable = true;
      }
      ex.account = customerId;
      ex.query = query;
      ex.logged = true;
      this.logger.debug(
        `API error parsed into GoogleAdsFailure: ${message}, error_code: ${
          error.errors ? error.errors[0]?.error_code : ''
        })`,
        {customerId, query}
      );
      return ex;
    } else {
      // it could be an error from gRPC
      // we expect an Error instance with interface of ServiceError from @grpc/grpc-js library
      // We used to handle only a subset of error by error code
      // (see status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html)
      // particularly 14 (unavailable), 13 (internal server error),
      // 8(RESOURCE_EXHAUSTED), 4 (DEADLINE_EXCEEDED)
      // But there was always something new that we didn't expect, so
      // in the end it seems much safer to treat any error not from the API
      // server as transient and allow retrying.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (error as any).retryable = true;
      return error;
    }
  }

  async executeQuery(
    query: string,
    customerId: string
  ): Promise<Record<string, unknown>[]> {
    const customer = this.getCustomer(customerId);
    return executeWithRetry(
      async () => {
        try {
          // actual type is services.IGoogleAdsRow[]
          return await customer.query(query);
        } catch (e) {
          throw (
            this.handleGoogleAdsError(
              <errors.GoogleAdsFailure>e,
              customerId,
              query
            ) || e
          );
        }
      },
      (error, attempt) => {
        const retry = attempt <= 3 && error.retryable;
        this.logger.verbose(
          retry
            ? `Retrying on transient error, attempt ${attempt}, error: ${error}`
            : `Breaking on ${
                error.retryable ? 'retriable' : 'non-retriable'
              } error, attempt ${attempt}, error: ${error}`,
          {customerId, query}
        );
        return retry;
      },
      {
        baseDelayMs: 100,
        delayStrategy: 'linear',
      }
    );
  }

  async *executeQueryStream(query: string, customerId: string) {
    try {
      const customer = this.getCustomer(customerId);
      // As we return an AsyncGenerator here we can't use executeWithRetry,
      // instead usages of the method should be wrapped with executeWithRetry
      // NOTE: we're iterating over the stream instead of returning it
      // for the sake of error handling
      const stream = customer.queryStream(query);
      for await (const row of stream) {
        yield row as Record<string, unknown>;
      }
    } catch (e) {
      throw (
        this.handleGoogleAdsError(
          <errors.GoogleAdsFailure>e,
          customerId,
          query
        ) || e
      );
    }
  }
}

interface SearchResponse {
  results: Record<string, unknown>[];
  nextPageToken?: string;
  totalResultsCount: string;
}

interface TokenResponse {
  access_token: string;
  expires_in: number;
}

/**
 * Google Ads API client using REST API.
 */
export class GoogleAdsRestApiClient
  extends GoogleAdsApiClientBase
  implements IGoogleAdsApiClient
{
  baseUrl: string;
  private currentToken: string | null = null;
  private tokenExpiration = 0;
  private readonly refreshInterval = 300000; // 5 minutes
  private authClient: GoogleAuth | null = null;

  constructor(adsConfig: GoogleAdsApiConfig, apiVersion?: string) {
    super(adsConfig, ApiType.REST, apiVersion);
    this.baseUrl = `https://googleads.googleapis.com/${this.apiVersion}/`;
    if (this.adsConfig.json_key_file_path || !this.adsConfig.refresh_token) {
      this.authClient = new GoogleAuth({
        keyFile: this.adsConfig.json_key_file_path,
        scopes: 'https://www.googleapis.com/auth/adwords',
      });
    }
  }

  protected async refreshAccessToken(
    clientId: string,
    clientSecret: string,
    refreshToken: string
  ): Promise<TokenResponse> {
    const tokenUrl = 'https://www.googleapis.com/oauth2/v3/token';
    const data = {
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: 'refresh_token',
    };

    try {
      const response = await axios.post<TokenResponse>(tokenUrl, data);
      return {
        access_token: response.data.access_token,
        expires_in: response.data.expires_in || 3600,
      };
    } catch (error) {
      if (axios.isAxiosError(error)) {
        throw new Error(
          `Failed to refresh token: ${error.response?.status}, ${JSON.stringify(error.response?.data)}`
        );
      }
      throw error;
    }
  }

  protected async getValidToken(): Promise<string> {
    if (this.authClient) {
      // working under a service account
      const accessToken = await this.authClient.getAccessToken();
      return accessToken as string;
    }
    if (
      this.currentToken === null ||
      Date.now() >= this.tokenExpiration - this.refreshInterval
    ) {
      // working under a user account (with refreshToken)
      // Refresh if within 5 minutes of expiration
      const {access_token, expires_in} = await this.refreshAccessToken(
        this.adsConfig.client_id!,
        this.adsConfig.client_secret!,
        this.adsConfig.refresh_token!
      );
      this.currentToken = access_token;
      this.tokenExpiration = Date.now() + expires_in * 1000;
    }

    return this.currentToken;
  }

  async executeQuery(
    query: string,
    customerId: string
  ): Promise<Record<string, unknown>[]> {
    this.logger.debug(`Executing GAQL query: ${query}`);
    const url = `${this.baseUrl}customers/${customerId}/googleAds:search`;
    const headers: Record<string, string> = await this.createHeaders();
    const payload: Record<string, unknown> = {
      query,
    };
    let results;
    do {
      const data = await executeWithRetry(
        async () => {
          try {
            return await this.sendApiRequest<SearchResponse>(
              url,
              payload,
              headers
            );
          } catch (e) {
            throw this.handleGoogleAdsError(e, customerId, query) || e;
          }
        },
        (error, attempt) => {
          const retry = attempt <= 3 && error.retryable;
          this.logger.verbose(
            retry
              ? `Retrying on transient error, attempt ${attempt}, error: ${error}`
              : `Breaking on ${
                  error.retryable ? 'retriable' : 'non-retriable'
                } error, attempt ${attempt}, error: ${error}`,
            {customerId, query}
          );
          return retry;
        },
        {
          baseDelayMs: 100,
          delayStrategy: 'linear',
        }
      );
      if (data?.results) {
        if (!results) {
          results = data.results;
        } else {
          results = results.concat(data.results);
        }
      }
      if (data?.nextPageToken) {
        payload.pageToken = data.nextPageToken;
        continue;
      }
      break;
      // eslint-disable-next-line no-constant-condition
    } while (true);

    return results || [];
  }

  protected async createHeaders() {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${await this.getValidToken()}`,
      'developer-token': this.adsConfig.developer_token,
      'Content-Type': 'application/json',
    };
    if (this.authClient) {
      headers['x-goog-user-project'] = await this.authClient.getProjectId();
    }
    if (this.adsConfig.login_customer_id) {
      headers['login-customer-id'] = this.adsConfig.login_customer_id;
    }
    return headers;
  }

  async *executeQueryStream(
    query: string,
    customerId: string
  ): AsyncGenerator<Record<string, unknown>> {
    this.logger.debug(`Executing GAQL query: ${query}`);
    const url = `${this.baseUrl}customers/${customerId}/googleAds:search`;
    const headers: Record<string, string> = await this.createHeaders();
    const payload: Record<string, unknown> = {
      query,
    };
    do {
      // The current implementation is using batched 'search' method,
      // simply iterating over results. Ideally we should use 'searchStream' method
      // with axios' responseType: 'stream' and parse results w/o buffering.
      // Additionally there's a difference how executeQueryStream and executeQuery
      // are used. The former is called by AdsQueryExecuter wrapped in executeWithRetry,
      // while the latter is expected to implement retry on its own.
      try {
        const data = await this.sendApiRequest<SearchResponse>(
          url,
          payload,
          headers
        );
        if (data?.results) {
          for (const row of data.results) {
            yield row;
          }
        }
        if (data?.nextPageToken) {
          payload.pageToken = data.nextPageToken;
          continue;
        }
        break;
      } catch (e) {
        throw this.handleGoogleAdsError(e, customerId, query) || e;
      }
      // eslint-disable-next-line no-constant-condition
    } while (true);
  }

  protected async sendApiRequest<T>(
    url: string,
    data: unknown,
    headers: Record<string, string>
  ): Promise<T> {
    try {
      const response = await axios.post<T>(url, data, {
        headers,
      });
      return response.data;
    } catch (error) {
      if (error.response && error.response.data) {
        const ex = error.response.data.length
          ? error.response.data[0]?.error
          : error.response.data.error;
        if (ex) throw ex;
      }
      throw error;
    }
  }

  protected handleGoogleAdsError(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    error: any,
    customerId: string,
    query?: string
  ) {
    try {
      console.error(error);
      this.logger.error(
        `An error occurred on executing query (cid: ${customerId}): ${query}\nRaw error: ` +
          JSON.stringify(error, null, 2),
        {customerId, query}
      );
    } catch (e) {
      // a very unfortunate situation
      console.error(e);
      this.logger.error(
        `An error occurred on executing query and on logging it afterwards: ${query}\n.Raw error: ${e}, logging error:${e}`
      );
    }
    const failure =
      error.details && error.details.length ? error.details[0] : null;
    if (!failure) {
      this.logger.debug('Could not parse API error into GoogleAdsFailure');
      error.logged = true;
      error.retryable = true;
      return error;
    }

    let message = error.message || 'Unknown Google Ads API error';
    if (error.status) {
      message = error.status + ': ' + message;
    }

    if (failure.errors && failure.errors.length) {
      message += ': ' + failure.errors[0].message;
    }
    const ex = new GoogleAdsError(message, failure);
    const transientStatusCodes = [408, 429, 500, 502, 503, 504];
    if (error.code && transientStatusCodes.includes(error.code)) {
      ex.retryable = true;
    }
    if (failure.errors.length) {
      if (
        failure.errors[0].errorCode?.internalError ||
        failure.errors[0].errorCode?.quotaError
      ) {
        ex.retryable = true;
      }
    } else {
      // it's an unknown error (no `errors` collection), it happens sometimes
      // we'll treat such errors as retryable
      ex.retryable = true;
    }
    ex.account = customerId;
    ex.query = query;
    ex.logged = true;
    this.logger.debug(
      `API error parsed into GoogleAdsFailure: ${ex.message}, error_code: ${
        error.errors ? error.errors[0]?.errorCode : ''
      })`,
      {customerId, query}
    );

    return ex;
  }
}
