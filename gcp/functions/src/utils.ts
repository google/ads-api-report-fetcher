import express from 'express';
import {
  getFileContent,
  GoogleAdsApiConfig,
  loadAdsConfigYaml,
} from 'google-ads-api-report-fetcher';
import path from 'node:path';
import fs from 'node:fs';

export async function getScript(
  req: express.Request
): Promise<{queryText: string; scriptName: string}> {
  const scriptPath = req.query.script_path;
  const body = req.body || {};
  let queryText: string | undefined;
  let scriptName: string | undefined;
  if (body.script) {
    queryText = body.script.query;
    scriptName = body.script.name;
    console.log('Executing inline query from request');
  } else if (scriptPath) {
    queryText = await getFileContent(<string>scriptPath);
    scriptName = path.basename(<string>scriptPath).split('.sql')[0];
    console.log(`Executing query from '${scriptPath}'`);
  }
  if (!queryText)
    throw new Error(
      'Script was not specified in either script_path query argument or body.query'
    );
  if (!scriptName) throw new Error('Could not determine script name');
  return {queryText, scriptName};
}

export async function getAdsConfig(
  req: express.Request
): Promise<GoogleAdsApiConfig> {
  let adsConfig: GoogleAdsApiConfig;
  const adsConfigFile =
    <string>req.query.ads_config_path || process.env.ADS_CONFIG;
  if (adsConfigFile) {
    adsConfig = await loadAdsConfigYaml(
      adsConfigFile,
      <string>req.query.customer_id
    );
  } else {
    adsConfig = <GoogleAdsApiConfig>{
      developer_token: <string>process.env.DEVELOPER_TOKEN,
      login_customer_id: <string>process.env.LOGIN_CUSTOMER_ID,
      client_id: <string>process.env.CLIENT_ID,
      client_secret: <string>process.env.CLIENT_SECRET,
      refresh_token: <string>process.env.REFRESH_TOKEN,
    };
  }
  if (!adsConfig && fs.existsSync('google-ads.yaml')) {
    adsConfig = await loadAdsConfigYaml(
      'google-ads.yaml',
      <string>req.query.customer_id
    );
  }
  return adsConfig;
}
