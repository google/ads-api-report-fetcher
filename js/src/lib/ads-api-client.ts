/**
 * Copyright 2023 Google LLC
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

import {
  ClientOptions,
  Customer,
  CustomerOptions,
  errors,
  GoogleAdsApi,
  services,
  enums,
} from "google-ads-api";
const ads_protos = require("google-ads-node/build/protos/protos.json");
const protoRoot = ads_protos.nested.google.nested.ads.nested.googleads.nested;
const protoVer = Object.keys(protoRoot)[0]; // e.g. "v9"
import _ from "lodash";
import { executeWithRetry, getElapsed } from "./utils";
import { getLogger } from "./logger";
import axios, { AxiosError } from "axios";
import { ApiType } from "./types";
import { AdsQueryEditor, IAdsQueryEditor } from "./ads-query-editor";
import { AdsRowParser, IAdsRowParser } from "./ads-row-parser";

export interface IGoogleAdsApiClient {
  get apiType(): ApiType;

  get apiVersion(): string;

  getQueryEditor(): IAdsQueryEditor;

  getRowParser(): IAdsRowParser;

  executeQueryStream(
    query: string,
    customerId: string
  ): AsyncGenerator<services.IGoogleAdsRow>;

  /**
   * Execute a native GAQL query.
   * Result returned from API (depends on a client used) as is.
   * @param query GAQL query (native)
   * @param customerId customer id
   */
  executeQuery(
    query: string,
    customerId: string
  ): Promise<any[]>;
}

interface TokenResponse {
  access_token: string;
  expires_in: number;
}

export type GoogleAdsApiConfig = {
  // ClientOptions:
  client_id: string;
  client_secret: string;
  developer_token: string;
  // CustomerOptions:
  refresh_token: string;
  login_customer_id?: string;
  linked_customer_id?: string;
  customer_id?: string[] | string;
};

export class GoogleAdsError extends Error {
  failure: errors.GoogleAdsFailure;
  query?: string;
  account?: string;
  retryable: boolean;
  logged: boolean = false;

  constructor(
    message: string | null | undefined,
    failure: errors.GoogleAdsFailure
  ) {
    super(message || "Unknown error on calling Google Ads API occurred");
    this.failure = failure;
    this.retryable = false;
  }
}

export abstract class GoogleAdsApiClientBase implements IGoogleAdsApiClient {
  _apiType: ApiType;
  apiVersion: string;
  logger;

  constructor(apiType: ApiType, apiVersion?: string) {
    this._apiType = apiType;
    this.logger = getLogger();
    if (apiVersion && !apiVersion.startsWith("v")) {
      apiVersion = "v" + apiVersion;
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
    return new AdsRowParser(this.apiType);
  }

  abstract executeQueryStream(
    query: string,
    customerId: string
  ): AsyncGenerator<services.IGoogleAdsRow, any, unknown>;

  abstract executeQuery(
    query: string,
    customerId: string
  ): Promise<services.IGoogleAdsRow[]>;
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
  adsConfig: GoogleAdsApiConfig;

  constructor(adsConfig: GoogleAdsApiConfig) {
    super(ApiType.gRPC);
    if (!adsConfig) {
      throw new Error("GoogleAdsApiConfig instance was not passed");
    }
    this.adsConfig = adsConfig;
    this.client = new GoogleAdsApi({
      client_id: adsConfig.client_id,
      client_secret: adsConfig.client_secret,
      developer_token: adsConfig.developer_token,
    });
    this.customers = {};
  }

  protected getCustomer(customerId: string): Customer {
    let customer: Customer;
    if (!customerId) {
      throw new Error("Customer id should be specified ");
    }
    customer = this.customers[customerId];
    if (!customer) {
      customer = this.client.Customer({
        customer_id: customerId, // child
        login_customer_id: this.adsConfig.login_customer_id, // MCC
        refresh_token: this.adsConfig.refresh_token,
      });
      this.customers[customerId] = customer;
    }
    return customer;
  }

  public handleGoogleAdsError(
    error: errors.GoogleAdsFailure | Error,
    customerId: string,
    query?: string
  ) {
    try {
      console.error(error);
      this.logger.error(
        `An error occured on executing query (cid: ${customerId}): ${query}\nRaw error: ` +
          JSON.stringify(error, null, 2)
      );
    } catch (e) {
      // a very unfortunate situation
      console.log(e);
      this.logger.error(
        `An error occured on executing query and on logging it afterwards: ${query}\n.Raw error: ${e}, logging error:${e}`
      );
    }
    if (error instanceof errors.GoogleAdsFailure && error.errors) {
      const message = error.errors.length
        ? error.errors[0].message
        : (<any>error).message || "Unknown GoogleAdsFailure error";
      let ex = new GoogleAdsError(message, error);
      if (error.errors.length) {
        if (
          error.errors[0].error_code?.internal_error ||
          error.errors[0].error_code?.quota_error
        ) {
          ex.retryable = true;
        }
      }
      ex.account = customerId;
      ex.query = query;
      ex.logged = true;
      return ex;
    } else {
      // it could be an error from gRPC
      // we expect an Error instance with interface of ServiceError from @grpc/grpc-js library
      // see status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
      if (
        (<any>error).code === 14 /* UNAVAILABLE */ ||
        (<any>error).details === "The service is currently unavailable" ||
        (<any>error).code === 8 /* RESOURCE_EXHAUSTED */ ||
        (<any>error).code === 4 /* DEADLINE_EXCEEDED */ ||
        (<any>error).code === 13 /* INTERNAL */
      ) {
        (<any>error).retryable = true;
      }
    }
  }

  async executeQuery(
    query: string,
    customerId: string
  ): Promise<services.IGoogleAdsRow[]> {
    const customer = this.getCustomer(customerId);
    return executeWithRetry(
      async () => {
        try {
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
        return attempt <= 3 && error.retryable;
      },
      {
        baseDelayMs: 100,
        delayStrategy: "linear",
      }
    );
  }

  async *executeQueryStream(query: string, customerId: string) {
    const customer = this.getCustomer(customerId);
    try {
      // As we return an AsyncGenerator here we can't use executeWithRetry,
      // instead usages of the method should be wrapped with executeWithRetry
      // NOTE: we're iterating over the stream instead of returning it
      // for the sake of error handling
      const stream = customer.queryStream(query);
      for await (const row of stream) {
        yield row;
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
  results: any[];
  nextPageToken?: string;
  totalResultsCount: string;
}

/**
 * Google Ads API client using REST API.
 */
export class GoogleAdsRestApiClient
  extends GoogleAdsApiClientBase
  implements IGoogleAdsApiClient
{
  adsConfig: GoogleAdsApiConfig;
  baseUrl: string;
  private currentToken: string | null = null;
  private tokenExpiration = 0;

  constructor(adsConfig: GoogleAdsApiConfig, apiVersion?: string) {
    super(ApiType.REST, apiVersion);
    if (!adsConfig) {
      throw new Error("GoogleAdsApiConfig instance was not passed");
    }
    this.adsConfig = adsConfig;
    this.baseUrl = `https://googleads.googleapis.com/${this.apiVersion}/`;
  }

  protected async refreshAccessToken(
    clientId: string,
    clientSecret: string,
    refreshToken: string
  ): Promise<TokenResponse> {
    const tokenUrl = "https://www.googleapis.com/oauth2/v3/token";
    const data = {
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
      grant_type: "refresh_token",
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
          `Failed to refresh token: ${error.response?.status}, ${error.response?.data}`
        );
      }
      throw error;
    }
  }

  protected async getValidToken(): Promise<string> {
    if (
      this.currentToken === null ||
      Date.now() >= this.tokenExpiration - 300000
    ) {
      // Refresh if within 5 minutes of expiration
      const { access_token, expires_in } = await this.refreshAccessToken(
        this.adsConfig.client_id,
        this.adsConfig.client_secret,
        this.adsConfig.refresh_token
      );
      this.currentToken = access_token;
      this.tokenExpiration = Date.now() + expires_in * 1000;
    }

    return this.currentToken;
  }

  async executeQuery(query: string, customerId: string): Promise<any[]> {
    this.logger.debug(`Executing GAQL query: ${query}`);
    const url = `${this.baseUrl}customers/${customerId}/googleAds:search`;
    const headers: Record<string, string> = await this.createHeaders();
    const payload: Record<string, any> = {
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
          return attempt <= 3 && error.retryable;
        },
        {
          baseDelayMs: 100,
          delayStrategy: "linear",
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
      "developer-token": this.adsConfig.developer_token,
      "Content-Type": "application/json",
    };
    if (this.adsConfig.login_customer_id) {
      headers["login-customer-id"] = this.adsConfig.login_customer_id;
    }
    return headers;
  }

  async *executeQueryStream(
    query: string,
    customerId: string
  ): AsyncGenerator<services.IGoogleAdsRow, any, unknown> {
    this.logger.debug(`Executing GAQL query: ${query}`);
    const url = `${this.baseUrl}customers/${customerId}/googleAds:search`;
    const headers: Record<string, string> = await this.createHeaders();
    const payload: Record<string, any> = {
      query,
    };
    do {
      // The current implementation is using batched 'search' method,
      // simply iterating over results. Ideally we should use 'seachStream' method
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
    data: any,
    headers: any
  ): Promise<T> {
    try {
      const response = await axios.post<T>(url, data, {
        headers,
      });
      return response.data;
    } catch (error) {
      if (error.response && error.response.data) {
        if (error.response.data.length) {
          // for searchStream method
          const ex = error.response.data[0].error;
          if (ex) throw ex;
        } else {
          // for search method
          const ex = error.response.data.error;
          if (ex) throw ex;
        }
      }
      throw error;
    }
  }

  public handleGoogleAdsError(error: any, customerId: string, query?: string) {
    try {
      console.error(error);
      this.logger.error(
        `An error occured on executing query (cid: ${customerId}): ${query}\nRaw error: ` +
          JSON.stringify(error, null, 2)
      );
    } catch (e) {
      // a very unfortunate situation
      console.log(e);
      this.logger.error(
        `An error occured on executing query and on logging it afterwards: ${query}\n.Raw error: ${e}, logging error:${e}`
      );
    }
    /*
      code: 400
      details: (2) [{…}, {…}]
      message: 'Request contains an invalid argument.'
      status: 'INVALID_ARGUMENT'
     */
    let message = error.message || "Unknown Google Ads API error";
    if (error.status) {
      message = error.status + ": " + message;
    }
    if (error.details && error.details.length && error.details[0].errors) {
      message +=
        ": " + error.details.length && error.details[0].errors[0].message;
    }
    let ex = new GoogleAdsError(message, error);
    // TODO: detect generic errors like internal_error/quota_error
    const transientStatusCodes = [429, 500, 502, 503, 504];
    if (error.code && transientStatusCodes.includes(error.code)) {
      ex.retryable = true;
    }
    return ex;
    // if (error && error.errors) {
    //   const message = error.errors.length
    //     ? error.errors[0].message
    //     : (<any>error).message || "Unknown GoogleAdsFailure error";
    //   let ex = new GoogleAdsError(message, error);
    //   if (error.errors.length) {
    //     if (
    //       error.errors[0].error_code?.internal_error ||
    //       error.errors[0].error_code?.quota_error
    //     ) {
    //       ex.retryable = true;
    //     }
    //   }
    //   ex.account = customerId;
    //   ex.query = query;
    //   ex.logged = true;
    //   return ex;
    // } else {
    //   // it could be an error from gRPC
    //   // we expect an Error instance with interface of ServiceError from @grpc/grpc-js library
    //   // see status codes: https://grpc.github.io/grpc/core/md_doc_statuscodes.html
    //   if (
    //     (<any>error).code === 14 /* UNAVAILABLE */ ||
    //     (<any>error).details === "The service is currently unavailable" ||
    //     (<any>error).code === 8 /* RESOURCE_EXHAUSTED */ ||
    //     (<any>error).code === 4 /* DEADLINE_EXCEEDED */ ||
    //     (<any>error).code === 13 /* INTERNAL */
    //   ) {
    //     (<any>error).retryable = true;
    //   }
    // }
  }
}
