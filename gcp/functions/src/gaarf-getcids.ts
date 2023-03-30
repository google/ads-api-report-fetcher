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
import {
  AdsQueryExecutor,
  getFileContent,
  GoogleAdsApiClient,
  GoogleAdsApiConfig,
} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';
import {getAdsConfig, getProject} from './utils';
import {createLogger, ILogger} from './logger';

async function main_getcids_unsafe(
  req: express.Request,
  res: express.Response,
  logger: ILogger
) {
  // prepare Ads API parameters
  const adsConfig: GoogleAdsApiConfig = await getAdsConfig(req);
  const {refresh_token, ...ads_config_wo_token} = adsConfig;
  await logger.info('Ads API config', ads_config_wo_token);

  const customerId = req.query.customer_id || adsConfig.customer_id;
  if (!customerId)
    throw new Error(
      "Customer id is not specified in either 'customer_id' query argument or google-ads.yaml"
    );

  const ads_client = new GoogleAdsApiClient(adsConfig, <string>customerId);
  let accounts = await ads_client.getCustomerIds();
  let customer_ids_query = '';
  if (req.body && req.body.customer_ids_query) {
    customer_ids_query = <string>req.body.customer_ids_query;
  } else if (req.query.customer_ids_query) {
    customer_ids_query = await getFileContent(
      <string>req.query.customer_ids_query
    );
  }
  if (customer_ids_query) {
    await logger.info(
      `Fetching customer id using custom query: ${customer_ids_query}`
    );
    const executor = new AdsQueryExecutor(ads_client);
    accounts = await executor.getCustomerIds(accounts, customer_ids_query);
  }

  res.json(accounts);
  res.end();
}

export const main_getcids: HttpFunction = async (
  req: express.Request,
  res: express.Response
) => {
  const projectId = await getProject();
  const logger = createLogger(
    req,
    projectId,
    process.env.K_SERVICE || 'gaarf-getcids'
  );
  await logger.info('request', {body: req.body, query: req.query});

  try {
    await main_getcids_unsafe(req, res, logger);
  } catch (e) {
    await logger.error(e.message, e);
    res.status(500).send(e.message).end();
  }
};
