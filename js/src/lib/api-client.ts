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

import fs from 'fs';
import {ClientOptions, Customer, CustomerOptions, errors, GoogleAdsApi} from 'google-ads-api';
import _ from 'lodash';

export interface IGoogleAdsApiClient {
  executeQuery(query: string, customerId?: string|undefined|null):
      Promise<any[]>;
  getCustomerIds(): Promise<string[]>
}

export type GoogleAdsApiConfig = CustomerOptions&ClientOptions;

export class GoogleAdsApiClient implements IGoogleAdsApiClient {
  client: GoogleAdsApi;
  customers: Record<string, Customer>;
  ads_cfg: GoogleAdsApiConfig;
  isChildCustomer: boolean;

  constructor(adsConfig: GoogleAdsApiConfig, customerId?: string|undefined) {
    customerId = customerId || adsConfig.customer_id;
    if (!customerId) {
      throw new Error(`No customer id was specified`);
    }
    customerId = customerId?.toString();
    this.ads_cfg = adsConfig;
    this.client = new GoogleAdsApi({
      client_id: adsConfig.client_id,
      client_secret: adsConfig.client_secret,
      developer_token: adsConfig.developer_token
    });
    this.customers = {};
    this.customers[customerId] = this.client.Customer({
      customer_id: customerId,                         // child
      login_customer_id: adsConfig.login_customer_id,  // MCC
      refresh_token: adsConfig.refresh_token
    });
    // also put the customer as the default one
    this.customers[''] = this.customers[customerId];
    this.isChildCustomer =
        customerId !== adsConfig.login_customer_id?.toString()
  }

  async executeQuery(query: string, customerId?: string|undefined|null):
      Promise<any[]> {
    let customer: Customer;
    if (!customerId) {
      customer = this.customers[''];
    } else {
      customer = this.customers[customerId];
      if (!customer) {
        customer = this.client.Customer({
          customer_id: customerId,                            // child
          login_customer_id: this.ads_cfg.login_customer_id,  // MCC
          refresh_token: this.ads_cfg.refresh_token
        });
        this.customers[customerId] = customer;
      }
    }
    try {
      return await customer.query(query);
    } catch (e) {
      let error = <errors.GoogleAdsFailure>e;
      if (error.errors)
        console.log(
            `An error occured on executing query: ` +
            JSON.stringify(error.errors[0], null, 2));
      throw e;
    }
  }

  async getCustomerIds(): Promise<string[]> {
    if (this.isChildCustomer) {
      return Object.keys(this.customers).filter(k => !!k);
    }
    const query_customer_ids = `SELECT
          customer_client.id,
          customer_client.manager
        FROM customer_client`;

    let rows = await this.executeQuery(query_customer_ids);
    let ids = [];
    for (let row of rows) {
      if (row.customer_client && !row.customer_client?.manager) {
        ids.push(row.customer_client.id!);
      }
    }
    return ids;
  }
}
