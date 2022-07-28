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
import {AdsQueryExecutor, BigQueryWriter, GoogleAdsApiClient, loadAdsConfigYaml} from 'google-ads-api-report-fetcher';

import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';
import {GoogleAdsApiConfig} from 'google-ads-api-report-fetcher/src/lib/ads-api-client';
import {getScript} from './utils';

export const main: HttpFunction =
    async (req: express.Request, res: express.Response) => {
  console.log(req.body);
  console.log(req.query);

  // prepare Ads API parameters
  let adsConfig: GoogleAdsApiConfig;
  let adsConfigFile = process.env.ADS_CONFIG || 'google-ads.yaml';
  if (fs.existsSync(adsConfigFile)) {
    adsConfig = loadAdsConfigYaml(adsConfigFile, <string>req.query.customer_id);
  } else {
    adsConfig = <GoogleAdsApiConfig> {
      developer_token: <string>process.env.DEVELOPER_TOKEN,
          login_customer_id: <string>process.env.LOGIN_CUSTOMER_ID,
          client_id: <string>process.env.CLIENT_ID,
          client_secret: <string>process.env.CLIENT_SECRET,
          refresh_token: <string>process.env.REFRESH_TOKEN
    }
  }
  console.log('Ads API config:');
  console.log(adsConfig);
  if (!adsConfig.developer_token || !adsConfig.refresh_token) {
    throw new Error(`Ads API configuration is not complete.`);
  }

  let projectId = req.query.bq_project_id || process.env.PROJECT_ID;
  if (!projectId)
    throw new Error(
        `Project id is not specified in either 'bq_project_id' query argument or PROJECT_ID envvar`);
  let dataset = req.query.bq_dataset || process.env.DATASET;
  if (!dataset)
    throw new Error(
        `Dataset is not specified in either 'bq_dataset' query argument or DATASET envvar`);
  let customerId = req.query.customer_id || adsConfig.customer_id;
  if (!customerId)
    throw new Error(
        `Customer id is not specified in either 'customer_id' query argument or google-ads.yaml`);

  let ads_client = new GoogleAdsApiClient(adsConfig, <string>customerId);
  let executor = new AdsQueryExecutor(ads_client);
  let writer =
      new BigQueryWriter(<string>projectId, <string>dataset, {keepData: true});
  // TODO: support CsvWriter and output path to GCS
  // (csv.destination_folder=gs://bucket/path)

  let singleCustomer = req.query.single_customer;
  let body = req.body || {};
  let macroParams = body.macro;

  let {queryText, scriptName} = await getScript(req);
  let customers: string[];
  if (singleCustomer) {
    console.log('Executing for a single customer ids: ' + customerId);
    customers = [<string>customerId];
  } else {
    console.log('Fetching customer ids');
    customers = await ads_client.getCustomerIds();
    console.log(`Customers to process (${customers.length}):`);
    console.log(customers);
  }
  await executor.execute(scriptName, queryText, customers, macroParams, writer);

  console.log(`[${scriptName}][${customerId}] Cloud Function compeleted`);
  if (req.query.get_data) {
    res.send(writer.rowsByCustomer);
  } else {
    // we're returning a map of customer to number of rows
    let result = Object.entries(writer.rowsByCustomer).map(p => {
      return {[p[0]]: p[1].length};
    });
    res.send(result);
  }
};
