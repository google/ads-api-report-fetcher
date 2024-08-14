/**
 * Copyright 2024 Google LLC
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
 * Cloud Function 'gaarf' - executes Ads query (suplied either via body or as a GCS path) and writes data to BigQuery (or other writer)
 * arguments:
 *  - (required) ads config - different sources are supported, see `getAdsConfig` function
 *  - writer - writer to use: "bq", "json", "csv". By default - "bq" (BigQuery)
 *  - bq_dataset - (can be taken from envvar DATASET) output BQ dataset id
 *  - bq_project_id - BigQuery project id, be default the current project is used
 *  - customer_id - Ads customer id (a.k.a. CID), can be taken from google-ads.yaml if specified
 *  - expand_mcc - true to expand account in `customer_id` argument. By default (if fale) it also disables creating union views.
 *  - bq_dataset_location - BigQuery dataset location ('us' or 'europe'), optional, by default 'us' is used
 *  - output_path - output path for interim data (for BigQueryWriter) or generated data (Csv/Json writers)
 */
import {
  AdsQueryExecutor,
  BigQueryWriter,
  BigQueryWriterOptions,
  GoogleAdsRpcApiClient,
  getMemoryUsage,
  getCustomerIds,
  GoogleAdsApiConfig,
  GoogleAdsRestApiClient,
  IGoogleAdsApiClient,
  CsvWriter,
  CsvWriterOptions,
  JsonWriter,
  JsonWriterOptions,
} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework/build/src/functions';
import express from 'express';
import {
  getAdsConfig,
  getProject,
  getScript,
  setLogLevel,
  startPeriodicMemoryLogging,
} from './utils';
import {ILogger, createLogger} from './logger';

function getQueryWriter(req: express.Request, projectId: string) {
  const body = req.body || {};

  if (!req.query.writer || req.query.writer === 'bq') {
    const bqWriterOptions: BigQueryWriterOptions = {
      datasetLocation: <string>req.query.bq_dataset_location,
      arrayHandling: body.writer_options?.array_handling,
      arraySeparator: body.writer_options?.array_separator,
      outputPath: <string>req.query.output_path,
      noUnionView: true,
    };
    if (req.query.expand_mcc) {
      bqWriterOptions.noUnionView = false;
    }
    const dataset = req.query.bq_dataset || process.env.DATASET;
    if (!dataset)
      throw new Error(
        "Dataset is not specified in either 'bq_dataset' query argument or DATASET envvar"
      );
    const writer = new BigQueryWriter(
      <string>projectId,
      <string>dataset,
      bqWriterOptions
    );
    return writer;
  }
  if (req.query.writer === 'csv') {
    const options: CsvWriterOptions = {
      quoted: body.writer_options?.quoted,
      arraySeparator: body.writer_options?.array_separator,
      outputPath: <string>req.query.output_path || `gs://${projectId}/tmp`,
    };
    return new CsvWriter(options);
  }
  if (req.query.writer === 'json') {
    const options: JsonWriterOptions = {
      format: body.writer_options?.format,
      valueFormat: body.writer_options?.value_format,
      outputPath: <string>req.query.output_path || `gs://${projectId}/tmp`,
    };
    return new JsonWriter(options);
  }
}

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

  const customerId = req.query.customer_id || adsConfig.customer_id;
  if (!customerId)
    throw new Error(
      "Customer id is not specified in either 'customer_id' query argument or google-ads.yaml"
    );
  if (!adsConfig.login_customer_id) {
    adsConfig.login_customer_id = <string>customerId;
  }

  let adsClient: IGoogleAdsApiClient;
  if (req.query.api === 'rest') {
    const apiVersion = <string>req.query.apiVersion;
    adsClient = new GoogleAdsRestApiClient(adsConfig, apiVersion);
  } else {
    adsClient = new GoogleAdsRpcApiClient(adsConfig);
  }

  const {queryText, scriptName} = await getScript(req, logger);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
  const {refresh_token, developer_token, ...ads_config_wo_token} = <any>(
    adsConfig
  );
  ads_config_wo_token['ApiVersion'] = adsClient.apiVersion;
  await logger.info(
    `Running Cloud Function ${functionName}, Ads API ${adsClient.apiType} ${
      adsClient.apiVersion
    }, ${
      req.query.expand_mcc
        ? 'with MCC expansion (MCC=' + customerId + ')'
        : 'CID=' + customerId
    }, see Ads API config in metadata field`,
    {
      adsConfig: ads_config_wo_token,
      scriptName,
      customerId,
      request: {body: req.body, query: req.query},
    }
  );

  let customers: string[];
  if (req.query.expand_mcc) {
    customers = await getCustomerIds(adsClient, <string>customerId);
    await logger.info(
      `[${scriptName}] Customers to process (${customers.length})`,
      {
        customerId,
        scriptName,
        customers,
      }
    );
  } else {
    customers = [<string>customerId];
  }

  const executor = new AdsQueryExecutor(adsClient);
  const writer = getQueryWriter(req, projectId);

  const result = await executor.execute(
    scriptName,
    queryText,
    customers,
    req.body.macro,
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
  setLogLevel(req);
  const dumpMemory = !!(req.query.dump_memory || process.env.DUMP_MEMORY);
  const projectId = await getProject();
  const functionName = process.env.K_SERVICE || 'gaarf';
  const logger = createLogger(req, projectId, functionName);
  let dispose;
  if (dumpMemory) {
    logger.info(getMemoryUsage('Start'));
    dispose = startPeriodicMemoryLogging(logger, 60_000);
  }

  try {
    await main_unsafe(req, res, projectId, logger, functionName);
  } catch (e) {
    console.error(e);
    await logger.error(e.message, {
      error: e,
      body: req.body,
      query: req.query,
    });
    res.status(500).send(e.message).end();
  } finally {
    if (dumpMemory) {
      if (dispose) dispose();
      logger.info(getMemoryUsage('End'));
    }
  }
};
