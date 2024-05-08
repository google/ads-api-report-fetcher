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
  enums
} from "google-ads-api";
import yaml from "js-yaml";
import _ from "lodash";
import { getFileContent } from "./file-utils";
import { executeWithRetry } from './utils';
import { getLogger } from "./logger";

export interface IGoogleAdsApiClient {
  executeQueryStream(
    query: string,
    customerId: string
  ): AsyncGenerator<services.IGoogleAdsRow>;
  executeQuery(
    query: string,
    customerId: string
  ): Promise<services.IGoogleAdsRow[]>;
  getCustomerIds(customerId: string): Promise<string[]>;
}

// export type GoogleAdsApiConfig = CustomerOptions&ClientOptions;
export type GoogleAdsApiConfig = {
  // ClientOptions:
  client_id: string;
  client_secret: string;
  developer_token: string;
  // CustomerOptions:
  refresh_token: string;
  login_customer_id?: string;
  linked_customer_id?: string;
  customer_id?: string[]|string;
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
    super(message || "Unknow error on calling Google Ads API occurred");
    this.failure = failure;
    this.retryable = false;
    if (failure.errors[0].error_code?.internal_error) {
      this.retryable = true;
    }
  }
}

export class GoogleAdsApiClient implements IGoogleAdsApiClient {
  client: GoogleAdsApi;
  customers: Record<string, Customer>;
  ads_cfg: GoogleAdsApiConfig;
  logger;

  constructor(adsConfig: GoogleAdsApiConfig) {
    if (!adsConfig) {
      throw new Error("GoogleAdsApiConfig instance was not passed");
    }
    this.ads_cfg = adsConfig;
    this.client = new GoogleAdsApi({
      client_id: adsConfig.client_id,
      client_secret: adsConfig.client_secret,
      developer_token: adsConfig.developer_token,
    });
    this.customers = {};
    this.logger = getLogger();
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
        login_customer_id: this.ads_cfg.login_customer_id, // MCC
        refresh_token: this.ads_cfg.refresh_token,
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
      this.logger.error(
        `An error occured on executing query: ${query}\nRaw error: ` +
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
      let ex = new GoogleAdsError(error.errors[0].message, error);
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
        (<any>error).code === 4 /* DEADLINE_EXCEEDED */
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

  /**
   * Get all nested non-MCC account for the specified one.
   * If the specified one is a leaf account (non-MCC) then it will be returned
   * @param customerId A customer account (CID)
   * @returns a list of child account (at all levels)
   */
  async getCustomerIds(customerId: string | string[]): Promise<string[]> {
    const query = `SELECT
          customer_client.id
        FROM customer_client
        WHERE
          customer_client.status = "ENABLED" AND
          customer_client.manager = False`;
    if (typeof customerId === "string") {
      customerId = [customerId];
    }
    let all_ids = [];
    for (const cid of customerId) {
      let rows = await this.executeQuery(query, cid);
      let ids = rows.map((row) => row.customer_client!.id!.toString());
      all_ids.push(...ids);
    }
    return all_ids;
  }

  async getCustomerInfo(customerId: string): Promise<CustomerInfo> {
    const query = `SELECT
      customer_client.id,
      customer_client.level,
      customer_client.status,
      customer_client.manager
    FROM customer_client
    WHERE
      customer_client.level <= 1
      AND customer_client.status = "ENABLED"
    ORDER BY customer_client.level`;
    //
    const query2 = `SELECT customer.descriptive_name FROM customer`;
    let rows = await this.executeQuery(query, customerId);
    let customer: CustomerInfo | undefined = undefined;
    for (const row of rows) {
      const cid = row.customer_client!.id!.toString();
      if (row.customer_client?.level === 0) {
        // the current account itself
        /* Status:
          UNSPECIFIED = 0,
          UNKNOWN = 1,
          ENABLED = 2,
          CANCELED = 3,
          SUSPENDED = 4,
          CLOSED = 5
         */
        const response =
          row.customer_client!.status === 2
            ? await this.executeQuery(query2, cid)
            : null;
        customer = {
          id: cid,
          name: response ? response[0].customer!.descriptive_name! : null,
          is_mcc: false,
          status: <string>enums.CustomerStatus[row.customer_client!.status!],
          children: [],
        };
      } else {
        customer!.children.push(await this.getCustomerInfo(cid));
        customer!.is_mcc = true;
      }
    }
    return customer!;
  }
}

export function parseCustomerIds(customerId: string|undefined, adsConfig: GoogleAdsApiConfig) {
  let customerIds: string[] | undefined;
  if (!customerId) {
    // CID/account wasn't provided explicitly, we'll use customer_id field from ads-config (it can be absent)
    if (adsConfig.customer_id) {
      if (_.isArray(adsConfig.customer_id)) {
        customerIds = adsConfig.customer_id;
      } else {
        customerIds = [adsConfig.customer_id];
      }
    }
  } else {
    // NOTE: argv.account is CLI arg, it can only be a string
    if (customerId.includes(",")) {
      customerIds = customerId.split(",");
    } else {
      customerIds = [customerId];
    }
  }
  if (!customerIds && adsConfig.login_customer_id) {
    // last chance if no CID was provided is to use login_customer_id
    customerIds = [adsConfig.login_customer_id];
  }

  if (customerIds && customerIds.length) {
    for (let i = 0; i < customerIds.length; i++) {
      customerIds[i] = customerIds[i].toString().replaceAll('-', '');
    }
  }
  return customerIds;
}

export async function loadAdsConfigFromFile(configFilepath: string): Promise<GoogleAdsApiConfig> {
  try {
    const content = await getFileContent(configFilepath);
    const doc = configFilepath.endsWith(".json")
      ? <any>JSON.parse(content)
      : <any>yaml.load(content);

    return {
      developer_token: doc["developer_token"],
      client_id: doc["client_id"],
      client_secret: doc["client_secret"],
      refresh_token: doc["refresh_token"],
      login_customer_id: (doc["login_customer_id"])?.toString(),
      customer_id: doc["customer_id"]?.toString(),
    };
  } catch (e) {
    throw new Error(
      `Failed to load Ads API configuration from ${configFilepath}: ${e}`
    );
  }
}

export interface CustomerInfo {
  id: string,
  name: string | null,
  is_mcc: boolean,
  status: string,
  children: CustomerInfo[]
}
