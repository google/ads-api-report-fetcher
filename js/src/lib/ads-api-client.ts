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
} from "google-ads-api";
import yaml from "js-yaml";
import _ from "lodash";
import {getFileContent} from "./file-utils";
import { getLogger } from "./logger";

export interface IGoogleAdsApiClient {
  executeQueryStream(
    query: string,
    customerId?: string | undefined | null
  ): AsyncGenerator<any[]>;
  executeQuery(
    query: string,
    customerId?: string | undefined | null
  ): Promise<any[]>;
  getCustomerIds(): Promise<string[]>;
}

// export type GoogleAdsApiConfig = CustomerOptions&ClientOptions;
export type GoogleAdsApiConfig = {
  // ClientOptions:
  client_id: string;
  client_secret: string;
  developer_token: string;
  // CustomerOptions:
  customer_id?: string;
  refresh_token: string;
  login_customer_id?: string;
  linked_customer_id?: string;
};

export class GoogleAdsApiClient implements IGoogleAdsApiClient {
  client: GoogleAdsApi;
  customers: Record<string, Customer>;
  ads_cfg: GoogleAdsApiConfig;
  root_cid: string;
  logger;

  constructor(adsConfig: GoogleAdsApiConfig, customerId?: string | undefined) {
    if (!adsConfig) {
      throw new Error("GoogleAdsApiConfig instance was not passed");
    }
    customerId = customerId || adsConfig.customer_id;
    if (!customerId) {
      throw new Error(`No customer id was specified`);
    }
    customerId = customerId?.toString();
    this.ads_cfg = adsConfig;
    this.client = new GoogleAdsApi({
      client_id: adsConfig.client_id,
      client_secret: adsConfig.client_secret,
      developer_token: adsConfig.developer_token,
    });
    this.customers = {};
    this.customers[customerId] = this.client.Customer({
      customer_id: customerId, // child
      login_customer_id: adsConfig.login_customer_id, // MCC
      refresh_token: adsConfig.refresh_token,
    });
    // also put the customer as the default one
    this.customers[""] = this.customers[customerId];
    this.root_cid = customerId;
    this.logger = getLogger();
  }

  protected getCustomer(customerId: string | undefined | null): Customer {
    let customer: Customer;
    if (!customerId) {
      customer = this.customers[""];
    } else {
      customer = this.customers[customerId];
      if (!customer) {
        customer = this.client.Customer({
          customer_id: customerId, // child
          login_customer_id: this.ads_cfg.login_customer_id, // MCC
          refresh_token: this.ads_cfg.refresh_token,
        });
        this.customers[customerId] = customer;
      }
    }
    return customer;
  }

  protected handleGoogleAdsError(
    error: errors.GoogleAdsFailure,
    query: string
  ) {
    if (error.errors)
      this.logger.error(
        `An error occured on executing query: ${query}\nError: ` +
          JSON.stringify(error.errors[0], null, 2)
      );
  }

  async executeQuery(
    query: string,
    customerId?: string | null | undefined
  ): Promise<any[]> {
    const customer = this.getCustomer(customerId);
    try {
      return await customer.query(query);
    } catch (e) {
      this.handleGoogleAdsError(<errors.GoogleAdsFailure>e, query);
      throw e;
    }
  }

  executeQueryStream(
    query: string,
    customerId?: string | undefined | null
  ): AsyncGenerator<any[]> {
    const customer = this.getCustomer(customerId);
    try {
      return customer.queryStream(query);
    } catch (e) {
      this.handleGoogleAdsError(<errors.GoogleAdsFailure>e, query);
      throw e;
    }
  }

  async getCustomerIds(): Promise<string[]> {
    const query =
      `SELECT
          customer_client.id
        FROM customer_client
        WHERE
          customer_client.status = "ENABLED" AND
          customer_client.manager = False`;

    let rows = await this.executeQuery(query);
    let ids = rows.map((row) => row.customer_client.id!);
    return ids;
  }
}

export async function loadAdsConfigFromFile(
  configFilepath: string,
  customerId?: string | undefined
): Promise<GoogleAdsApiConfig> {
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
      login_customer_id: (doc["login_customer_id"] || customerId)?.toString(),
      customer_id: (
        customerId ||
        doc["customer_id"] ||
        doc["login_customer_id"]
      )?.toString(),
    };
  } catch (e) {
    throw new Error(
      `Failed to load Ads API configuration from ${configFilepath}: ${e}`
    );
  }
}
