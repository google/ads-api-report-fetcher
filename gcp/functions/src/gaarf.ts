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
 * Cloud Function 'gaarf' - executes Ads query (supplied either via body or as a GCS path) and writes data to BigQuery (or other writer)
 * arguments:
 *  - (required) ads_config - different sources are supported, see `getAdsConfig` function
 *  - writer - writer to use: "bq", "json", "csv". By default - "bq" (BigQuery)
 *  - writer_options - writer options (object)
 *  - bq_dataset - (can be taken from envvar DATASET) output BQ dataset id
 *  - bq_project_id - BigQuery project id, be default the current project is used
 *  - customer_id - Ads customer id (a.k.a. CID), can be taken from google-ads.yaml if specified
 *  - expand_mcc - true to expand account in `customer_id` argument. By default (if false) it also disables creating union views.
 *  - bq_dataset_location - BigQuery dataset location ('us' or 'europe'), optional, by default 'us' is used
 *  - output_path - output path for interim data (for BigQueryWriter) or generated data (Csv/Json writers)
 *  - schema_dir - path for Gaarf schema location
 */
import {
  AdsQueryExecutor,
  BigQueryWriter,
  BigQueryWriterOptions,
  getMemoryUsage,
  getCustomerIds,
  GoogleAdsApiConfig,
  GoogleAdsApiClient,
  IGoogleAdsApiClient,
  CsvWriter,
  CsvWriterOptions,
  JsonWriter,
  JsonWriterOptions,
} from 'google-ads-api-report-fetcher';
import type {HttpFunction} from '@google-cloud/functions-framework';
import express from 'express';
import {
  getAdsConfig,
  getProject,
  getScript,
  loadAdsConfig,
  loadScript,
  startPeriodicMemoryLogging,
} from './utils.js';
import {ILogger, createLogger} from './logger.js';

export interface GaarfTaskArgs {
  scriptPath?: string;
  script?: {query: string; name: string};
  adsConfigPath?: string;
  adsConfig?: Record<string, any>;
  customerId: string;
  rootCid?: string;
  apiVersion?: string;
  projectId?: string;
  writer?: string;
  bqDataset?: string;
  bqDatasetLocation?: string;
  outputPath?: string;
  expandMcc?: boolean;
  macros?: Record<string, any>;
  templateParams?: Record<string, any>;
  writerOptions?: Record<string, any>;
  schemaDir?: string;
  callbackUrl?: string;
}

function getQueryWriter(args: GaarfTaskArgs, projectId: string) {
  const writerType = args.writer || 'bq';

  if (!writerType || writerType === 'bq' || writerType === 'bigquery') {
    const bqWriterOptions: BigQueryWriterOptions = {
      datasetLocation: args.bqDatasetLocation,
      arrayHandling: args.writerOptions?.array_handling,
      arraySeparator: args.writerOptions?.array_separator,
      outputPath: args.outputPath,
      noUnionView: true,
    };
    if (args.expandMcc) {
      bqWriterOptions.noUnionView = false;
    }
    const dataset = args.bqDataset || process.env.DATASET;
    if (!dataset)
      throw new Error(
        "Dataset is not specified in either 'bq_dataset' argument or DATASET envvar"
      );
    const writer = new BigQueryWriter(
      <string>projectId,
      <string>dataset,
      bqWriterOptions
    );
    return writer;
  }
  if (writerType === 'csv') {
    const options: CsvWriterOptions = {
      quoted: args.writerOptions?.quoted,
      arraySeparator: args.writerOptions?.array_separator,
      outputPath: args.outputPath || `gs://${projectId}/tmp`,
    };
    return new CsvWriter(options);
  }
  if (writerType === 'json') {
    const options: JsonWriterOptions = {
      format: args.writerOptions?.format,
      valueFormat: args.writerOptions?.value_format,
      outputPath: args.outputPath || `gs://${projectId}/tmp`,
    };
    return new JsonWriter(options);
  }
}

export async function executeGaarfQuery(
  args: GaarfTaskArgs,
  logger: ILogger,
  functionName = 'gaarf'
): Promise<Record<string, number>> {
  // prepare Ads API parameters
  const adsConfig: GoogleAdsApiConfig = await loadAdsConfig(
    args.adsConfigPath,
    args.adsConfig
  );
  const projectId =
    args.projectId || process.env.PROJECT_ID || (await getProject());

  if (args.schemaDir) {
    process.env.GAARF_SCHEMA_DIR = args.schemaDir;
  }

  const customerId = args.customerId || adsConfig.customer_id;
  if (!customerId)
    throw new Error(
      "Customer id is not specified in either 'customer_id' argument or google-ads.yaml"
    );
  if (!adsConfig.login_customer_id) {
    adsConfig.login_customer_id = (args.rootCid || customerId) as string;
  }

  const apiVersion = args.apiVersion;
  const adsClient = new GoogleAdsApiClient(adsConfig, apiVersion);

  const {queryText, scriptName} = await loadScript(
    args.scriptPath,
    args.script,
    logger
  );
  // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-unused-vars
  const {refresh_token, developer_token, ...ads_config_wo_token} = <any>(
    adsConfig
  );
  ads_config_wo_token['ApiVersion'] = adsClient.apiVersion;
  logger.info(
    `Running ${functionName}, Ads API ${adsClient.apiVersion}, ${
      args.expandMcc
        ? 'with MCC expansion (MCC=' + customerId + ')'
        : 'CID=' + customerId
    }, see Ads API config in metadata field`,
    {
      adsConfig: ads_config_wo_token,
      scriptName,
      customerId,
      args,
    }
  );

  let customers: string[];
  if (args.expandMcc) {
    customers = await getCustomerIds(adsClient, <string>customerId);
    logger.info(`[${scriptName}] Customers to process (${customers.length})`, {
      customerId,
      scriptName,
      customers,
    });
  } else {
    customers = [<string>customerId];
  }

  const executor = new AdsQueryExecutor(adsClient);
  const writer = getQueryWriter(args, projectId);

  // NOTE: 'macro' is deprecated but still used
  const macros = args.macros;
  logger.info(`Starting executing script via Gaarf`, {
    customers,
    scriptName,
    queryText,
    macro: macros,
    templateParams: args.templateParams,
  });

  const result = await executor.execute(
    scriptName,
    queryText,
    customers,
    {macros: macros, templateParams: args.templateParams},
    writer
  );

  logger.info(`${functionName} completed`, {
    customerId,
    scriptName,
    result,
  });

  return result;
}

async function main_unsafe(
  req: express.Request,
  res: express.Response,
  projectId: string,
  logger: ILogger,
  functionName: string
) {
  const taskArgs: GaarfTaskArgs = {
    scriptPath: <string>req.query.script_path,
    script: req.body?.script,
    adsConfigPath: <string>req.query.ads_config_path,
    adsConfig: req.body?.ads_config,
    customerId: <string>(req.query.customer_id || ''),
    rootCid: <string>req.query.root_cid,
    apiVersion: <string>req.query.api_version,
    projectId: <string>req.query.bq_project_id || projectId,
    writer: <string>req.query.writer,
    bqDataset: <string>req.query.bq_dataset,
    bqDatasetLocation: <string>req.query.bq_dataset_location,
    outputPath: <string>req.query.output_path,
    expandMcc: !!req.query.expand_mcc,
    schemaDir: <string>req.query.schema_dir,
    macros: req.body?.macros || req.body?.macro,
    templateParams: req.body?.template_params,
    writerOptions: req.body?.writer_options,
  };

  const result = await executeGaarfQuery(taskArgs, logger, functionName);
  res.json(result);
  res.end();
}

export const main: HttpFunction = async (
  req: express.Request,
  res: express.Response
) => {
  const dumpMemory = !!(req.query.dump_memory || process.env.DUMP_MEMORY);
  const projectId = await getProject();
  const functionName = process.env.K_SERVICE || 'gaarf';
  const logger = createLogger(req, projectId, functionName);
  logger.info('request', {body: req.body, query: req.query});
  let dispose;
  if (dumpMemory) {
    logger.info(getMemoryUsage('Start'));
    dispose = startPeriodicMemoryLogging(logger, 60_000);
  }

  try {
    await main_unsafe(req, res, projectId, logger, functionName);
  } catch (e: any) {
    console.error(e);
    logger.error(e.message, {
      error: e,
      body: req.body,
      query: req.query,
    });
    const status = e.status || e.statusCode || e.code;
    const httpStatus = status === 429 || status === 503 ? status : 400;
    res.status(httpStatus).send(e.message).end();
  } finally {
    if (dumpMemory) {
      if (dispose) dispose();
      logger.info(getMemoryUsage('End'));
    }
  }
};
