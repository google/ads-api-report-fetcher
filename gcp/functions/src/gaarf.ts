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

/**
 * Cloud Function 'gaarf' - executes Ads query (suplied either via body or as gcs path) and writes data to BigQuery
 * arguments:
 *  - (required) ads config - different sources are supported, see `getAdsConfig` fucntion
 *  - (required) bq_dataset - (can be taken from envvar DATASET) output BQ dataset id
 *  - bq_project_id - BigQuery project id, be default the current project is used
 *  - customer_id - Ads customer id (a.k.a. CID), can be taken from google-ads.yaml if specified
 *  - single_customer - true for skipping loading of subaccount, assuming the supplied CID is a leaf one (not MCC)
 *  - bq_dataset_location - BigQuery dataset location ('us' or 'europe'), optional, by default 'us' is used
 */
import {
  AdsQueryExecutor,
  AdsApiVersion,
  BigQueryWriter,
  BigQueryWriterOptions,
  GoogleAdsApiClient,
} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';
import {GoogleAdsApiConfig} from 'google-ads-api-report-fetcher/src/lib/ads-api-client';
import {getAdsConfig, getProject, getScript} from './utils';
import {ILogger, createLogger} from './logger';

async function main_unsafe(
  req: express.Request,
  res: express.Response,
  projectId: string,
  logger: ILogger,
  functionName: string
) {
  // prepare Ads API parameters
  const adsConfig: GoogleAdsApiConfig = await getAdsConfig(req);
  projectId =
    <string>req.query.bq_project_id || process.env.PROJECT_ID || projectId;

  const dataset = req.query.bq_dataset || process.env.DATASET;
  if (!dataset)
    throw new Error(
      "Dataset is not specified in either 'bq_dataset' query argument or DATASET envvar"
    );
  const customerId = req.query.customer_id || adsConfig.customer_id;
  if (!customerId)
    throw new Error(
      "Customer id is not specified in either 'customer_id' query argument or google-ads.yaml"
    );
  if (!adsConfig.login_customer_id) {
    adsConfig.login_customer_id = <string>customerId;
  }

  const ads_client = new GoogleAdsApiClient(adsConfig);
  // TODO: support CsvWriter and output path to GCS
  // (csv.destination_folder=gs://bucket/path)

  const singleCustomer = req.query.single_customer;
  const body = req.body || {};
  const macroParams = body.macro;
  const bq_writer_options: BigQueryWriterOptions = {
    datasetLocation: <string>req.query.bq_dataset_location,
    arrayHandling: body.bq_writer_options?.array_handling,
    arraySeparator: body.bq_writer_options?.array_separator,
  };

  const {queryText, scriptName} = await getScript(req, logger);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
  const {refresh_token, developer_token, ...ads_config_wo_token} = <any>(
    adsConfig
  );
  ads_config_wo_token['ApiVersion'] = AdsApiVersion;
  await logger.info(
    `Running Cloud Function ${functionName}, Ads API ${AdsApiVersion}, ${
      singleCustomer
        ? 'without MCC expansion (CID=' + customerId + ')'
        : 'with MCC expansion (MCC=' + customerId + ')'
    }, see Ads API config in metadata field`,
    {
      adsConfig: ads_config_wo_token,
      scriptName,
      customerId,
      request: {body: req.body, query: req.query},
    }
  );

  let customers: string[];
  if (singleCustomer) {
    customers = [<string>customerId];
    bq_writer_options.noUnionView = true;
  } else {
    customers = await ads_client.getCustomerIds(<string>customerId);
    await logger.info(
      `[${scriptName}] Customers to process (${customers.length})`,
      {
        customerId,
        scriptName,
        customers,
      }
    );
  }

  const executor = new AdsQueryExecutor(ads_client);
  const writer = new BigQueryWriter(
    <string>projectId,
    <string>dataset,
    bq_writer_options
  );

  const result = await executor.execute(
    scriptName,
    queryText,
    customers,
    macroParams,
    writer
  );

  await logger.info(`Cloud Function ${functionName} compeleted`, {
    customerId,
    scriptName,
    result,
  });
  // we're returning a map of customer to number of rows
  res.json(result);
  res.end();
}

export const main: HttpFunction = async (
  req: express.Request,
  res: express.Response
) => {
  const projectId = await getProject();
  const functionName = process.env.K_SERVICE || 'gaarf';
  const logger = createLogger(req, projectId, functionName);

  try {
    await main_unsafe(req, res, projectId, logger, functionName);
  } catch (e) {
    console.log(e);
    await logger.error(e.message, {error: e, body: req.body, query: req.query});
    res.status(500).send(e.message).end();
  }
};
