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
  parseCustomerIds,
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

  let customerIds = parseCustomerIds(<string>req.query.customer_id, adsConfig);

  if (!customerIds || customerIds.length === 0) {
    throw new Error(
      "Customer id is not specified in either 'customer_id' query argument or google-ads.yaml"
    );
  }
  if (!adsConfig.login_customer_id && customerIds && customerIds.length === 1) {
    adsConfig.login_customer_id = customerIds[0];
  }
  const adsClient = new GoogleAdsApiClient(adsConfig);
  customerIds = await adsClient.getCustomerIds(customerIds);
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
    const executor = new AdsQueryExecutor(adsClient);
    customerIds = await executor.getCustomerIds(
      customerIds,
      customer_ids_query
    );
  }
  if (customerIds.length) {
    customerIds.sort();

    // extract a subset of CIDs if offset/batch are specified
    let offset = 0;
    if (req.query.customer_ids_offset) {
      offset = parseInt(<string>req.query.customer_ids_offset);
      if (isNaN(offset)) {
        throw new Error('customer_ids_offset should be a number');
      }
    }
    let batchsize = 0;
    if (req.query.customer_ids_batchsize) {
      batchsize = parseInt(<string>req.query.customer_ids_batchsize);
      if (isNaN(offset)) {
        throw new Error('customer_ids_batchsize should be a number');
      }
    }
    const cids_length = customerIds.length;
    if (batchsize > 0) {
      customerIds = customerIds.slice(offset, offset + batchsize);
    } else if (offset > 0) {
      customerIds = customerIds.slice(offset);
    }
    if (cids_length !== customerIds.length) {
      await logger.info(
        `Reshaped cids array from ${cids_length} to ${customerIds.length} items`
      );
    }
  }

  res.json(customerIds);
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
    await logger.error(e.message, {error: e});
    res.status(500).send(e.message).end();
  }
};
