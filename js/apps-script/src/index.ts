/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Copyright 2026 Google LLC
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

import {GoogleAdsAppsScriptClient} from './lib/ads-api-client-appsscript';
import {SheetsWriterAppsScript} from './lib/sheets-writer-appsscript';
import {AdsQueryExecutor} from '../../src/lib/ads-query-executor';
import {
  GoogleAdsApiConfig,
  IGoogleAdsApiClient,
} from '../../src/lib/ads-api-client-base';
import {getCustomerIds} from '../../src/lib/ads-utils';
import {BundledSchemaLoader} from './lib/ads-api-schema-loader-bundled';

GoogleAdsAppsScriptClient;
SheetsWriterAppsScript;

/** Creates an Ads client instance. */
async function createAdsClient(
  adsConfig: GoogleAdsApiConfig,
  useInternalProxy = false,
) {
  const client = new GoogleAdsAppsScriptClient(adsConfig, useInternalProxy);
  return client;
}

/** Expands Customer ID (if needed). */
async function expandCustomerId(
  customerId: string,
  client: IGoogleAdsApiClient,
) {
  const customers = await getCustomerIds(client, customerId);
  return customers;
}

/** Executes GAQL query using AdsQueryExecutor. */
async function executeGaarfQuery(
  queryText: string,
  customerIds: string[],
  client: IGoogleAdsApiClient,
) {
  const executor = new AdsQueryExecutor(client);
  const writer = new SheetsWriterAppsScript();

  const result = await executor.execute(
    '', //scriptName,
    queryText,
    customerIds,
    {}, // no macros for now
    writer,
    {parallelAccounts: false},
  );

  return result;
}

/** Reads configuration from the Settings sheet. */
function readSettings(): Record<string, string> {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName('Settings');
  if (!sheet) {
    console.log("Sheet 'Settings' not found");
    return {};
  }
  const data = sheet.getDataRange().getValues();
  const settings: Record<string, string> = {};
  for (const row of data) {
    if (row[0]) {
      settings[row[0].toString()] = row[1] ? row[1].toString() : '';
    }
  }
  return settings;
}

/** Expand Customer ID (if needed) and execute GAQL query. */
async function executeGaarfFromSidebar(args: {
  customerId?: string;
  query: string;
}) {
  console.log(`Executing executeGaarfFromSidebar with args ${args}`);
  const settings = readSettings();
  const cid = args.customerId || settings['CID'];
  const useProxy = settings['ADS_INTERNAL_PROXY'] === 'true';

  if (!cid) {
    throw new Error(
      'Customer ID (CID) not found in arguments or Settings sheet.',
    );
  }

  const adsConfig: GoogleAdsApiConfig = {
    developer_token: settings['ADS_DEV_TOKEN'],
    login_customer_id: settings['MCC'],
  };

  const client = await createAdsClient(adsConfig, useProxy);
  let customerIds: string[];
  if (cid && cid.toString().includes(',')) {
    customerIds = cid.split(',');
  } else {
    customerIds = await expandCustomerId(cid, client);
  }
  console.log(`cid ${cid} was expanded to`, customerIds);
  const result = await executeGaarfQuery(args.query, customerIds, client);
  return result;
}

/** Saves query and Customer ID to document properties. */
function saveClientValues(values: Record<string, any>): void {
  const properties = PropertiesService.getDocumentProperties();
  properties.setProperty('query', values['query']);
  properties.setProperty('cid', values['cid']);
}

/** Restores query and Customer ID from document properties. */
function restoreClientValues() {
  const properties = PropertiesService.getDocumentProperties();
  return {
    query: properties.getProperty('query'),
    cid: properties.getProperty('cid'),
  };
}

/**
 * The only function that is needed to be called in client project
 * @sample onOpen('lib') where lib is the name under which the library is imported in client project
 */
function onOpen(var_name: string) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.addMenu('Gaarf', [
    {
      name: 'Open sidebar',
      functionName: var_name ? var_name + '.open_sidebar' : 'open_sidebar',
    },
    {
      name: 'Open Syntax Help',
      functionName: var_name ? var_name + '.openDocsDialog' : 'openDocsDialog',
    },
  ]);
}

/** Opens the sidebar with the client UI. */
function open_sidebar() {
  const html = HtmlService.createTemplateFromFile('static/sidebar')
    .evaluate()
    .setTitle('Gaarf');

  SpreadsheetApp.getUi().showSidebar(html);
}

/** Returns the current API version. */
function getApiVersion() {
  const loader = new BundledSchemaLoader();
  return loader.getLatestVersion();
}

/** Opens a modal dialog with rendered documentation. */
function openDocsDialog() {
  const html = HtmlService.createTemplateFromFile('static/docs_dialog')
    .evaluate()
    .setWidth(900)
    .setHeight(650)
    .setTitle('Gaarf Syntax Documentation');

  SpreadsheetApp.getUi().showModalDialog(html, 'Gaarf Syntax Documentation');
}

/** Fetches markdown documentation content from GitHub. */
function getDocsMarkdown(): string {
  try {
    const url =
      'https://raw.githubusercontent.com/google/ads-api-report-fetcher/main/docs/how-to-write-queries.md';
    const response = UrlFetchApp.fetch(url, {muteHttpExceptions: true});
    if (response.getResponseCode() === 200) {
      return response.getContentText();
    }
  } catch (e) {
    console.error('Failed to fetch docs markdown via UrlFetchApp', e);
  }
  return '';
}

const globalScope: any = this;

function onClientCall(func: string, arg: any) {
  console.log(`Calling ${func}`);
  if (globalScope && globalScope[func]) {
    return globalScope[func](arg);
  }
  throw new Error('Unknown function ' + func);
}
