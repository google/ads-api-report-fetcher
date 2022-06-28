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
import {AdsQueryExecutor, BigQueryWriter, getFileContent, GoogleAdsApiClient, QueryResult} from 'google-ads-api-report-fetcher';
import yaml from 'js-yaml';
import path from 'path';

import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';
import {GoogleAdsApiConfig} from 'google-ads-api-report-fetcher/src/lib/ads-api-client';

export const main: HttpFunction =
    async (req: express.Request, res: express.Response) => {
  console.log(req.query);

  // prepare Ads API parameters
  let adsConfig: GoogleAdsApiConfig;
  let adsConfigFile = process.env.ADS_CONFIG || 'google-ads.yaml';
  if (fs.existsSync(adsConfigFile)) {
    adsConfig = <GoogleAdsApiConfig>yaml.load(
        fs.readFileSync(adsConfigFile, {encoding: 'utf-8'}));
  } else {
    adsConfig = <GoogleAdsApiConfig> {
      developer_token: <string>process.env.developer_token,
          login_customer_id: <string>process.env.login_customer_id,
          client_id: <string>process.env.client_id,
          client_secret: <string>process.env.client_secret,
          refresh_token: <string>process.env.refresh_token
    }
  }
  console.log('Ads API config:');
  console.log(adsConfig);
  if (!adsConfig.developer_token || !adsConfig.refresh_token) {
    throw new Error(`Ads API configuration is not complete.`);
  }

  let scriptPath = req.query.script_path;
  if (!scriptPath)
    throw new Error(
        `Ads script path is not specified in script_path query argument`);
  let projectId = req.query.project_id || process.env.PROJECT_ID;
  if (!projectId)
    throw new Error(
        `Project id is not specified in either 'project_id' query argument or PROJECT_ID envvar`);
  let dataset = req.query.dataset || process.env.DATASET;
  if (!dataset)
    throw new Error(
        `Dataset is not specified in either 'dataset' query argument or DATASET envvar`);
  let customerId = req.query.customer_id;
  if (!customerId)
    throw new Error(
        `Customer id is not specified in 'customer_id' query argument`);

  let ads_client = new GoogleAdsApiClient(adsConfig, <string>customerId);
  let executor = new AdsQueryExecutor(ads_client);
  let writer =
      new BigQueryWriter(<string>projectId, <string>dataset, {keepData: true});

  let singleCustomer = req.query.single_customer;
  let macros = req.body;

  let queryText = await getFileContent(<string>scriptPath);
  console.log(`Executing Ads-query from ${scriptPath}`);
  let scriptName = path.basename(<string>scriptPath).split('.sql')[0];

  if (singleCustomer) {
    let query = executor.parseQuery(queryText, macros);
    await writer.beginScript(scriptName, query);
    await executor.executeOne(query, <string>customerId, writer);
    await writer.endScript();
  } else {
    console.log('Fetching customer ids');
    let customers = await ads_client.getCustomerIds();
    console.log(`Customers to process (${customers.length}):`);
    console.log(customers);

    await executor.execute(scriptName, queryText, customers, macros, writer);
  }
  let result = Object.entries(writer.rowsByCustomer).map(p => {
    return {[p[0]]: p[1].length};
  });
  res.send(result);
};
