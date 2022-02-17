import fs from 'fs';
import {ClientOptions, Customer, CustomerOptions, errors, GoogleAdsApi } from 'google-ads-api';
import yaml from 'js-yaml';
import _ from 'lodash';

export interface IGoogleAdsApiClient {
  executeQuery(query: string, customerId?: string|undefined|null): Promise<any[]>;
  getCustomerIds(): Promise<string[]>
}

export class GoogleAdsApiClient implements IGoogleAdsApiClient {
  client: GoogleAdsApi;
  customers: Record<string, Customer>;
  ads_cfg: CustomerOptions&ClientOptions;

  constructor(
      config: string|CustomerOptions&ClientOptions, customerId: string) {
    let ads_cfg: CustomerOptions&ClientOptions;
    if (_.isString(config)) {
      ads_cfg = this.loadConfig(config, customerId);
    } else {
      ads_cfg = config;
    }
    // let ads_cfg = this.loadConfig(config, customerId);
    if (!ads_cfg.customer_id) {
      throw new Error(`No customer id was specified`);
    }
    this.ads_cfg = ads_cfg;
    this.client = new GoogleAdsApi({
      client_id: ads_cfg.client_id,
      client_secret: ads_cfg.client_secret,
      developer_token: ads_cfg.developer_token
    });
    this.customers = {};
    this.customers[customerId] = this.client.Customer({
      customer_id: ads_cfg.customer_id,              //'3532705401', // child
      login_customer_id: ads_cfg.login_customer_id,  //'6368728866', // MCC
      refresh_token: ads_cfg.refresh_token
    });
    // also put the customer as the default one
    this.customers[''] = this.customers[customerId];
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
          customer_id: customerId,  //'3532705401', // child
          login_customer_id:
              this.ads_cfg.login_customer_id,  //'6368728866', // MCC
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
            JSON.stringify(error.errors[0]));
      throw e;
    }
  }

  async getCustomerIds(): Promise<string[]> {
    // customer_client.descriptive_name,
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

  loadConfig(config_file_path: string, customerId: string): CustomerOptions
      &ClientOptions {
    try {
      if (!fs.existsSync(config_file_path))
        throw new Error(`Config file ${config_file_path} does not exist`);

      const doc = <any>yaml.load(fs.readFileSync(config_file_path, 'utf8'));
      console.log(doc);
      return {
        developer_token: doc['developer_token'],
        client_id: doc['client_id'],
        client_secret: doc['client_secret'],
        refresh_token: doc['refresh_token'],
        login_customer_id: doc['login_customer_id'],
        customer_id: customerId || doc['customer_id']
      };
    } catch (e) {
      console.log(
          'Failed to load Ads API configuration from ' + config_file_path);
      throw e;
    }
  }
}
