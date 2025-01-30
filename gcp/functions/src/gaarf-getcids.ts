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
/**
 * Cloud Function 'gaarf-getids' - executes Ads query (suplied either via body or as a GCS path) and writes data to BigQuery
 * arguments:
* `ads_config_path` - a path to Ads config, same as for gaarf
* `customer_id` - a seed customer id (CID), without '-';
   can be specified in google-ads.yaml as well, if so then can be omitted
* `customer_ids_query` - custom Ads query to filter customer accounts expanded from `customer_id`,
   same as same-name argument for gaarf cli tool. Query's first column should be a customer id (CID).
* `customer_ids_ignore` - a list of customer ids to exclude from the result
* `customer_ids_batchsize` - a size of batches into which account ids list will be split.
* `customer_ids_offset` - an offset in the customer ids list resulted from the seed CIDs and
   optional query in `customer_ids_query`, it allows to implement an external batching.
* `flatten` - flatten the list of customer ids. If `customer_ids_offset` is provided then
   the list will be a subset of CIDs otherwise it will be the whole list of accounts,
   ignoring batching (regadless of the customer_ids_batchsize's value).
 **/
import {
  getFileContent,
  GoogleAdsRestApiClient,
  GoogleAdsApiConfig,
  parseCustomerIds,
  getMemoryUsage,
  IGoogleAdsApiClient,
  GoogleAdsRpcApiClient,
  getCustomerIds,
  filterCustomerIds,
} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework';
import express from 'express';
import {
  getAdsConfig,
  getProject,
  splitIntoChunks,
  startPeriodicMemoryLogging,
} from './utils.js';
import {createLogger, ILogger} from './logger.js';

const DEFAULT_BATCH_SIZE = 500;

async function main_getcids_unsafe(
  req: express.Request,
  res: express.Response,
  logger: ILogger
) {
  // prepare Ads API parameters
  const adsConfig: GoogleAdsApiConfig = await getAdsConfig(req);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const {refresh_token, ...ads_config_wo_token} = adsConfig;
  logger.info('Ads API config', ads_config_wo_token);

  let customerIds = parseCustomerIds(<string>req.query.customer_id, adsConfig);
  let customerIdsIgnore: string[] = [];
  if (req.query.customer_ids_ignore) {
    const customerIdsIgnoreQS = <string>req.query.customer_ids_ignore;
    if (customerIdsIgnoreQS.includes(',')) {
      customerIdsIgnore = customerIdsIgnoreQS.split(',');
    } else {
      customerIdsIgnore = [customerIdsIgnoreQS];
    }
    customerIdsIgnore = customerIdsIgnore.map(v => v.trim());
  }
  if (!customerIds || customerIds.length === 0) {
    throw new Error(
      "Customer id is not specified in either 'customer_id' query argument or google-ads.yaml"
    );
  }
  if (!adsConfig.login_customer_id && customerIds && customerIds.length === 1) {
    adsConfig.login_customer_id = customerIds[0];
  }
  let adsClient: IGoogleAdsApiClient;
  if (req.query.api === 'rest') {
    const apiVersion = <string>req.query.apiVersion;
    adsClient = new GoogleAdsRestApiClient(adsConfig, apiVersion);
  } else {
    adsClient = new GoogleAdsRpcApiClient(adsConfig);
  }

  customerIds = await getCustomerIds(adsClient, customerIds);
  let customer_ids_query = '';
  if (req.body && req.body.customer_ids_query) {
    customer_ids_query = <string>req.body.customer_ids_query;
  } else if (req.query.customer_ids_query) {
    customer_ids_query = await getFileContent(
      <string>req.query.customer_ids_query
    );
  }
  if (customer_ids_query) {
    logger.info(
      `Fetching customer id using custom query: ${customer_ids_query}`
    );
    customerIds = await filterCustomerIds(
      adsClient,
      customerIds,
      customer_ids_query
    );
    logger.info(`Loaded ${customerIds.length} accounts`);
  }
  customerIds = customerIds || [];
  customerIds.sort();

  // now we have a final list of accounts (customerIds)
  let batchSize = DEFAULT_BATCH_SIZE;
  if (req.query.customer_ids_batchsize) {
    batchSize = Number(<string>req.query.customer_ids_batchsize);
    if (isNaN(batchSize)) {
      throw new Error('customer_ids_batchsize should be a number');
    }
  }
  if (req.query.customer_ids_offset) {
    // extract a subset of CIDs if offset is specified
    const offset = Number(<string>req.query.customer_ids_offset);
    if (isNaN(offset)) {
      throw new Error('customer_ids_offset should be a number');
    }
    const cids_length = customerIds.length;
    customerIds = customerIds.slice(offset, offset + batchSize);
    if (cids_length !== customerIds.length) {
      logger.info(
        `Reshaped customer ids array from ${cids_length} to ${customerIds.length} items`
      );
    }
  }

  customerIds = customerIds.filter(cid => customerIdsIgnore.indexOf(cid) < 0);

  if (req.query.flatten) {
    res.json(customerIds);
    res.end();
  } else {
    // otherwise, by default we'll return CIDs grouped in batches:
    //    [1, 2, ..., 10_000] => [ [1, 2, ..., 5_000], [5_001, 10_000], ]
    const customerIdsBatched = splitIntoChunks(customerIds, batchSize);
    res.json({
      batchCount: customerIdsBatched.length,
      batchSize: batchSize,
      accounts: customerIdsBatched,
    });
    res.end();
  }
}

export const main_getcids: HttpFunction = async (
  req: express.Request,
  res: express.Response
) => {
  const dumpMemory = !!(req.query.dump_memory || process.env.DUMP_MEMORY);
  const projectId = await getProject();
  const logger = createLogger(
    req,
    projectId,
    process.env.K_SERVICE || 'gaarf-getcids'
  );
  logger.info('request', {body: req.body, query: req.query});
  let dispose;
  if (dumpMemory) {
    logger.info(getMemoryUsage('Start'));
    dispose = startPeriodicMemoryLogging(logger, 60_000);
  }

  try {
    await main_getcids_unsafe(req, res, logger);
  } catch (e) {
    console.error(e);
    logger.error(e.message, {error: e});
    res.status(500).send(e.message).end();
  } finally {
    if (dumpMemory) {
      if (dispose) dispose();
      logger.info(getMemoryUsage('End'));
    }
  }
};
